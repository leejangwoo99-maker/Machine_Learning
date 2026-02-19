# -*- coding: utf-8 -*-
"""
backend5_mastersample_daemon.py
-------------------------------------------------
Backend-5: Mastersample All OK daemon (incremental PK, 5s loop)

요구사항 반영
- 무한루프 5초
- DB 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹) + [RETRY] 5초마다
- 실행 중 DB 끊김 발생 시에도 무한 재접속 후 계속 진행
- 백엔드별 상시 연결 1개 고정(풀 최소화: pool_size=1, max_overflow=0)
- work_mem 폭증 방지: 세션마다 SET work_mem = '4MB' 고정
- PK 증분 기준: (end_day, end_time_norm) (정규화 HH:MI:SS 반올림 half-up)
- 중복 방지 캐시: seen_pk = set[(end_day, end_time_norm, md5(contents))]
- 재실행/윈도우 변경 시 bootstrap(현재 윈도우 start~now full scan) 후 UPSERT
- 기존 데이터 삭제/초기화 금지 (DELETE/TRUNCATE 금지)
- 결과 저장:
  - day  -> i_daily_report.e_mastersample_test_day_daily
  - night-> i_daily_report.e_mastersample_test_night_daily
  - UNIQUE key: prod_day 단일
  - 컬럼: prod_day, shift_type, "Mastersample", first_time(HH:MI:SS text), updated_at(timestamptz)
- 변경사항(20): 윈도우 경계
  - day   : [D] 08:20:00 ~ 20:19:59
  - night : [D] 20:20:00 ~ 23:59:59 + [D+1] 00:00:00 ~ 08:19:59
- 변경사항(21): window 내 Mastersample='O'가 되면 DB 조회 중단(추가 체크 불필요),
  다만 모니터링 목적 updated_at은 5초마다 계속 갱신(heartbeat upsert)
- 안전체크 2개:
  (S2) Mastersample='O'이면 first_time이 반드시 not null
  (S4 정책) 위 위반 시 ERROR 로그 후 강등/종료 없이 즉시 bootstrap 재시도(soft reboot)

[추가] 데몬 헬스 로그 DB 저장
- 로그 스키마/테이블: k_demon_heath_check."5_log" (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 순서: end_day, end_time, info, contents (단건 dataframe 형태로 구성 후 insert)
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Set, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# =========================
# Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

DB_RETRY_INTERVAL_SEC = 5
LOOP_INTERVAL_SEC = 5

WORK_MEM = "4MB"  # 고정

SRC_SCHEMA = "d1_machine_log"
SRC_TABLE = "Main_machine_log"  # 대소문자 고정 (따옴표로 감싸서 사용)

SAVE_SCHEMA = "i_daily_report"
SAVE_DAY_TABLE = "e_mastersample_test_day_daily"
SAVE_NIGHT_TABLE = "e_mastersample_test_night_daily"

LIKE_ALL_OK = "%Mastersample All OK%"

DAY_START = dtime(8, 20, 0)
DAY_END = dtime(20, 19, 59)
NIGHT_START = dtime(20, 20, 0)
NIGHT_END = dtime(8, 19, 59)  # next day

# Health log target
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "5_log"

# DB log runtime flags
_DB_LOG_ENGINE: Optional[Engine] = None
_DB_LOG_READY: bool = False


# =========================
# Logging (console + DB)
# =========================
def _log_db_insert(info: str, contents: str) -> None:
    """
    DB 로그 저장(실패해도 본 로직 영향 없음)
    컬럼 순서: end_day, end_time, info, contents
    """
    global _DB_LOG_ENGINE, _DB_LOG_READY
    if (not _DB_LOG_READY) or (_DB_LOG_ENGINE is None):
        return

    now = datetime.now(KST)
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "").lower(),
        "contents": str(contents),
    }

    # 요구사항: dataframe화하여 저장
    df = pd.DataFrame([[row["end_day"], row["end_time"], row["info"], row["contents"]]],
                      columns=["end_day", "end_time", "info", "contents"])

    insert_sql = f"""
    INSERT INTO "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (end_day, end_time, info, contents)
    VALUES (:end_day, :end_time, :info, :contents);
    """

    try:
        with _DB_LOG_ENGINE.begin() as conn:
            conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
            conn.execute(text(insert_sql), df.iloc[0].to_dict())
    except Exception:
        # 로그 저장 실패는 조용히 무시(데몬 본 기능 우선)
        pass


def log(level: str, msg: str) -> None:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}", flush=True)
    _log_db_insert(level.lower(), msg)


# =========================
# DB Engine
# =========================
def make_engine() -> Engine:
    db_config = {
        "host": os.getenv("PG_HOST", "100.105.75.47"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "dbname": os.getenv("PG_DBNAME", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", ""),
    }
    url = (
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    )
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )


def connect_with_retry(engine: Engine) -> None:
    """DB 연결 될 때까지 무한 재시도 + work_mem 세팅"""
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
                conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
            log("INFO", f"DB connected (work_mem={WORK_MEM})")
            return
        except Exception as e:
            # 아직 DB 로그 준비 전일 수 있으므로 콘솔 중심으로 동작
            print(
                f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} [RETRY] "
                f"DB connect failed: {type(e).__name__}: {e} | retry in {DB_RETRY_INTERVAL_SEC}s",
                flush=True,
            )
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# Window
# =========================
@dataclass(frozen=True)
class WindowKey:
    prod_day: str
    shift_type: str  # 'day' | 'night'


@dataclass(frozen=True)
class WindowRange:
    key: WindowKey
    start_dt: datetime
    end_dt: datetime  # "now"


def yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def shift_window_by_now(now: datetime) -> WindowRange:
    """
    now(KST) 기준으로 현재 윈도우(key + start~now) 계산
    """
    assert now.tzinfo is not None

    t = now.timetz().replace(tzinfo=None)
    today = now.date()

    if DAY_START <= t <= DAY_END:
        key = WindowKey(prod_day=yyyymmdd(today), shift_type="day")
        start_dt = datetime.combine(today, DAY_START, tzinfo=KST)
        return WindowRange(key=key, start_dt=start_dt, end_dt=now)

    if t >= NIGHT_START:
        prod = today
    else:
        prod = today - timedelta(days=1)

    key = WindowKey(prod_day=yyyymmdd(prod), shift_type="night")
    start_dt = datetime.combine(prod, NIGHT_START, tzinfo=KST)
    return WindowRange(key=key, start_dt=start_dt, end_dt=now)


# =========================
# Schema/Table init
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))


def ensure_save_table(engine: Engine, schema: str, table: str) -> None:
    ddl = f"""
CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
  prod_day    text PRIMARY KEY,
  shift_type  text,
  "Mastersample" text,
  first_time  text,
  updated_at  timestamptz
);
"""
    with engine.begin() as conn:
        conn.execute(text(ddl))


def ensure_health_log_table(engine: Engine) -> None:
    ensure_schema(engine, HEALTH_SCHEMA)
    ddl = f"""
CREATE TABLE IF NOT EXISTS "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (
  end_day  text,
  end_time text,
  info     text,
  contents text
);
"""
    with engine.begin() as conn:
        conn.execute(text(ddl))


# =========================
# Source FQN (quoted)
# =========================
def source_fqn() -> str:
    return f'"{SRC_SCHEMA}"."{SRC_TABLE}"'


# =========================
# SQL: normalize half-up + window expr
# =========================
def build_window_expr(shift_type: str) -> str:
    if shift_type == "day":
        return f"""
( end_day = :prod_day
  AND rounded_time_text >= '{DAY_START.strftime("%H:%M:%S")}'
  AND rounded_time_text <= '{DAY_END.strftime("%H:%M:%S")}'
)
"""
    if shift_type == "night":
        return f"""
(
    ( end_day = :prod_day AND rounded_time_text >= '{NIGHT_START.strftime("%H:%M:%S")}' )
 OR ( end_day = :next_day AND rounded_time_text <= '{NIGHT_END.strftime("%H:%M:%S")}' )
)
"""
    raise ValueError("shift_type must be 'day' or 'night'")


def normalize_cte_sql() -> str:
    fqn = source_fqn()
    return f"""
WITH norm AS (
  SELECT
    end_day,
    end_time,
    contents,
    to_timestamp(end_day || ' ' || substring(end_time from 1 for 8), 'YYYYMMDD HH24:MI:SS')::timestamp AS base_ts,
    (substring(end_time from 9))::numeric AS frac_num
  FROM {fqn}
),
norm2 AS (
  SELECT
    end_day,
    end_time,
    contents,
    (base_ts + CASE WHEN frac_num >= 0.5 THEN interval '1 second' ELSE interval '0 second' END) AS rounded_ts,
    to_char((base_ts + CASE WHEN frac_num >= 0.5 THEN interval '1 second' ELSE interval '0 second' END)::time, 'HH24:MI:SS') AS rounded_time_text,
    md5(contents) AS contents_md5,
    (contents LIKE :like_all_ok) AS is_all_ok
  FROM norm
)
"""


# =========================
# Bootstrap / Incremental
# =========================
@dataclass
class State:
    window: Optional[WindowKey] = None
    mastersample: str = "X"
    first_time: Optional[str] = None
    last_pk: Optional[Tuple[str, str]] = None  # (end_day, end_time_norm)
    seen_pk: Set[Tuple[str, str, str]] = None  # (end_day, end_time_norm, md5(contents))
    locked_o: bool = False
    need_recover: bool = False


def init_state() -> State:
    return State(seen_pk=set())


def bootstrap(engine: Engine, wk: WindowKey) -> Tuple[str, Optional[str], Tuple[str, str], Set[Tuple[str, str, str]]]:
    prod_day = wk.prod_day
    next_day = yyyymmdd(parse_yyyymmdd(prod_day) + timedelta(days=1))
    window_expr = build_window_expr(wk.shift_type)

    sql = normalize_cte_sql() + f"""
, win AS (
  SELECT end_day, rounded_time_text, contents_md5, is_all_ok, rounded_ts
  FROM norm2
  WHERE {window_expr}
),
hits AS (
  SELECT rounded_ts
  FROM win
  WHERE is_all_ok = TRUE
)
SELECT
  CASE WHEN EXISTS(SELECT 1 FROM hits) THEN 'O' ELSE 'X' END AS mastersample,
  (SELECT MIN(rounded_ts) FROM hits) AS first_ts,
  (SELECT end_day FROM win ORDER BY end_day DESC, rounded_time_text DESC LIMIT 1) AS max_day,
  (SELECT rounded_time_text FROM win ORDER BY end_day DESC, rounded_time_text DESC LIMIT 1) AS max_time
;
"""
    sql_seen = normalize_cte_sql() + f"""
SELECT end_day, rounded_time_text, contents_md5
FROM norm2
WHERE {window_expr}
;
"""

    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        row = conn.execute(
            text(sql),
            {"prod_day": prod_day, "next_day": next_day, "like_all_ok": LIKE_ALL_OK},
        ).mappings().first()

        rows_seen = conn.execute(
            text(sql_seen),
            {"prod_day": prod_day, "next_day": next_day, "like_all_ok": LIKE_ALL_OK},
        ).mappings().all()

    mastersample = row["mastersample"]
    first_ts = row["first_ts"]
    first_time = None if first_ts is None else first_ts.strftime("%H:%M:%S")

    max_day = row["max_day"]
    max_time = row["max_time"]
    if not max_day or not max_time:
        max_day = prod_day
        max_time = "00:00:00"

    seen_set: Set[Tuple[str, str, str]] = set()
    for r in rows_seen:
        seen_set.add((r["end_day"], r["rounded_time_text"], r["contents_md5"]))

    return mastersample, first_time, (max_day, max_time), seen_set


def fetch_incremental(engine: Engine, wk: WindowKey, last_pk: Tuple[str, str]) -> List[Dict]:
    prod_day = wk.prod_day
    next_day = yyyymmdd(parse_yyyymmdd(prod_day) + timedelta(days=1))
    window_expr = build_window_expr(wk.shift_type)

    last_day, last_time = last_pk

    pk_expr = """
(
  (end_day > :last_day)
  OR (end_day = :last_day AND rounded_time_text > :last_time)
)
"""

    sql = normalize_cte_sql() + f"""
SELECT
  end_day,
  rounded_time_text AS end_time_norm,
  contents_md5,
  is_all_ok
FROM norm2
WHERE {window_expr}
  AND {pk_expr}
ORDER BY end_day ASC, rounded_time_text ASC, contents_md5 ASC
;
"""
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        rows = conn.execute(
            text(sql),
            {
                "prod_day": prod_day,
                "next_day": next_day,
                "like_all_ok": LIKE_ALL_OK,
                "last_day": last_day,
                "last_time": last_time,
            },
        ).mappings().all()

    return [dict(r) for r in rows]


# =========================
# UPSERT
# =========================
def save_fqn_day() -> str:
    return f'"{SAVE_SCHEMA}"."{SAVE_DAY_TABLE}"'


def save_fqn_night() -> str:
    return f'"{SAVE_SCHEMA}"."{SAVE_NIGHT_TABLE}"'


def upsert_result(engine: Engine, wk: WindowKey, mastersample: str, first_time: Optional[str], updated_at: datetime) -> None:
    fqn = save_fqn_day() if wk.shift_type == "day" else save_fqn_night()

    sql = f"""
INSERT INTO {fqn} (prod_day, shift_type, "Mastersample", first_time, updated_at)
VALUES (:prod_day, :shift_type, :mastersample, :first_time, :updated_at)
ON CONFLICT (prod_day) DO UPDATE
SET
  shift_type = COALESCE(EXCLUDED.shift_type, {fqn}.shift_type),
  "Mastersample" = COALESCE(EXCLUDED."Mastersample", {fqn}."Mastersample"),
  first_time = CASE
                WHEN EXCLUDED."Mastersample" = 'X' THEN NULL
                ELSE COALESCE(EXCLUDED.first_time, {fqn}.first_time)
              END,
  updated_at = EXCLUDED.updated_at
;
"""
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        conn.execute(
            text(sql),
            {
                "prod_day": wk.prod_day,
                "shift_type": wk.shift_type,
                "mastersample": mastersample,
                "first_time": first_time,
                "updated_at": updated_at,
            },
        )


def heartbeat_update(engine: Engine, wk: WindowKey, updated_at: datetime) -> None:
    fqn = save_fqn_day() if wk.shift_type == "day" else save_fqn_night()

    sql = f"""
INSERT INTO {fqn} (prod_day, shift_type, "Mastersample", first_time, updated_at)
VALUES (:prod_day, :shift_type, NULL, NULL, :updated_at)
ON CONFLICT (prod_day) DO UPDATE
SET updated_at = EXCLUDED.updated_at
;
"""
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        conn.execute(
            text(sql),
            {
                "prod_day": wk.prod_day,
                "shift_type": wk.shift_type,
                "updated_at": updated_at,
            },
        )


# =========================
# Safety checks
# =========================
def safety_check_S2_S4(state: State) -> None:
    if state.mastersample == "O" and not state.first_time:
        log("ERROR", "SAFETY(S2/S4) violated: Mastersample='O' but first_time is NULL. Will soft-recover by bootstrap.")
        state.need_recover = True


# =========================
# Main loop
# =========================
def main() -> None:
    global _DB_LOG_ENGINE, _DB_LOG_READY

    log("BOOT", "backend5 mastersample daemon starting")

    engine = make_engine()
    connect_with_retry(engine)

    # ensure save schema/tables
    ensure_schema(engine, SAVE_SCHEMA)
    ensure_save_table(engine, SAVE_SCHEMA, SAVE_DAY_TABLE)
    ensure_save_table(engine, SAVE_SCHEMA, SAVE_NIGHT_TABLE)

    # ensure health log table + enable db log sink
    ensure_health_log_table(engine)
    _DB_LOG_ENGINE = engine
    _DB_LOG_READY = True
    log("INFO", f'health log target ready: "{HEALTH_SCHEMA}"."{HEALTH_TABLE}"')

    state = init_state()

    while True:
        t0 = time_mod.time()
        now = datetime.now(KST)

        wr = shift_window_by_now(now)
        wk = wr.key

        try:
            with engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
                conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        except Exception as e:
            # 재연결 중에도 로그 DB 저장이 실패할 수 있으니 우선 콘솔 + 재시도
            _DB_LOG_READY = False
            log("RETRY", f"DB ping failed: {type(e).__name__}: {e} | reconnecting...")
            connect_with_retry(engine)
            # reconnect 성공 시 로그 sink 재활성화
            try:
                ensure_health_log_table(engine)
                _DB_LOG_ENGINE = engine
                _DB_LOG_READY = True
                log("INFO", "health log sink re-enabled after reconnect")
            except Exception as ee:
                print(
                    f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} [error] "
                    f"health log sink enable failed: {type(ee).__name__}: {ee}",
                    flush=True,
                )

            elapsed = time_mod.time() - t0
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                log("SLEEP", f"loop sleep {sleep_sec:.2f}s")
                time_mod.sleep(sleep_sec)
            continue

        window_changed = (state.window != wk)
        if window_changed:
            log("INFO", f"[WINDOW] changed => prod_day={wk.prod_day} shift={wk.shift_type} start={wr.start_dt} now={wr.end_dt}")
            state.window = wk
            state.mastersample = "X"
            state.first_time = None
            state.last_pk = None
            state.seen_pk = set()
            state.locked_o = False
            state.need_recover = False

            log("INFO", "[BOOTSTRAP] start (window full-scan)")
            ms, ft, last_pk, seen = bootstrap(engine, wk)
            state.mastersample = ms
            state.first_time = ft
            state.last_pk = last_pk
            state.seen_pk = seen
            log("INFO", f"[BOOTSTRAP] done | Mastersample={ms}, first_time={ft}, last_pk={last_pk}, seen={len(seen)}")

            upsert_result(engine, wk, state.mastersample, state.first_time, now)
            log("INFO", "[UPSERT] bootstrap result saved")

            if state.mastersample == "O":
                state.locked_o = True
                log("INFO", "[LOCK] Mastersample=O => stop DB fetch until window ends (heartbeat only)")

            safety_check_S2_S4(state)

        elif state.need_recover:
            log("INFO", "[RECOVER] soft reboot by bootstrap (due to safety violation)")
            state.need_recover = False
            state.locked_o = False

            log("INFO", "[BOOTSTRAP] start (recover)")
            ms, ft, last_pk, seen = bootstrap(engine, wk)
            state.mastersample = ms
            state.first_time = ft
            state.last_pk = last_pk
            state.seen_pk = seen
            log("INFO", f"[BOOTSTRAP] done(recover) | Mastersample={ms}, first_time={ft}, last_pk={last_pk}, seen={len(seen)}")

            upsert_result(engine, wk, state.mastersample, state.first_time, now)
            log("INFO", "[UPSERT] recover result saved")

            if state.mastersample == "O":
                state.locked_o = True
                log("INFO", "[LOCK] Mastersample=O => stop DB fetch until window ends (heartbeat only)")

            safety_check_S2_S4(state)

        else:
            if state.locked_o:
                log("INFO", "[HEARTBEAT] Mastersample=O (no fetch)")
                heartbeat_update(engine, wk, now)
                safety_check_S2_S4(state)
            else:
                if state.last_pk is None:
                    state.last_pk = (wk.prod_day, "00:00:00")

                log("INFO", f"[FETCH] start | last_pk={state.last_pk}")
                rows = fetch_incremental(engine, wk, state.last_pk)
                log("INFO", f"[FETCH] done | fetched_rows={len(rows)}")

                if rows:
                    max_day, max_time = state.last_pk
                    new_unique = 0
                    found_all_ok_time: Optional[str] = None

                    for r in rows:
                        end_day = r["end_day"]
                        end_time_norm = r["end_time_norm"]
                        md5v = r["contents_md5"]
                        pk3 = (end_day, end_time_norm, md5v)

                        if (end_day > max_day) or (end_day == max_day and end_time_norm > max_time):
                            max_day, max_time = end_day, end_time_norm

                        if pk3 in state.seen_pk:
                            continue
                        state.seen_pk.add(pk3)
                        new_unique += 1

                        if r["is_all_ok"] and state.mastersample != "O":
                            if found_all_ok_time is None:
                                found_all_ok_time = end_time_norm

                    state.last_pk = (max_day, max_time)
                    log("INFO", f"[PK] advanced => last_pk={state.last_pk} | new_unique={new_unique} | seen={len(state.seen_pk)}")

                    if found_all_ok_time is not None:
                        state.mastersample = "O"
                        state.first_time = found_all_ok_time
                        log("INFO", f"[STATE] Mastersample becomes O | first_time={state.first_time}")

                    upsert_result(engine, wk, state.mastersample, state.first_time, now)
                    log("INFO", "[UPSERT] incremental result saved")

                    if state.mastersample == "O":
                        state.locked_o = True
                        log("INFO", "[LOCK] Mastersample=O => stop DB fetch until window ends (heartbeat only)")

                    safety_check_S2_S4(state)

        elapsed = time_mod.time() - t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            log("SLEEP", f"loop sleep {sleep_sec:.2f}s")
            time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    main()
