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
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

DB_RETRY_INTERVAL_SEC = 5
LOOP_INTERVAL_SEC = 5

WORK_MEM = "4MB"  # 고정

SRC_SCHEMA = "d1_machine_log"
SRC_TABLE  = "Main_machine_log"  # 대소문자 고정 (따옴표로 감싸서 사용)

SAVE_SCHEMA = "i_daily_report"
SAVE_DAY_TABLE = "e_mastersample_test_day_daily"
SAVE_NIGHT_TABLE = "e_mastersample_test_night_daily"

LIKE_ALL_OK = "%Mastersample All OK%"

DAY_START = dtime(8, 20, 0)
DAY_END   = dtime(20, 19, 59)
NIGHT_START = dtime(20, 20, 0)
NIGHT_END   = dtime(8, 19, 59)  # next day

# =========================
# Logging
# =========================
def log(level: str, msg: str) -> None:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}", flush=True)

# =========================
# DB Engine
# =========================
def make_engine() -> Engine:
    db_config = {
        "host": os.getenv("PG_HOST", "100.105.75.47"),
        "port": int(os.getenv("PG_PORT", "5432")),
        "dbname": os.getenv("PG_DBNAME", "postgres"),
        "user": os.getenv("PG_USER", "postgres"),
        "password": os.getenv("PG_PASSWORD", "leejangwoo1!"),
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
            log("RETRY", f"DB connect failed: {type(e).__name__}: {e} | retry in {DB_RETRY_INTERVAL_SEC}s")
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
    end_dt: datetime  # "now" (for logging / bootstrap 설명용)

def yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

def shift_window_by_now(now: datetime) -> WindowRange:
    """
    now(KST) 기준으로 현재 윈도우(key + start~now) 계산
    - day:   today 08:20:00 ~ now (if now in day band)
    - night: (if now >=20:20) prod_day=today, start=today 20:20
             (if now <08:20)  prod_day=yesterday, start=yesterday 20:20
    """
    assert now.tzinfo is not None

    t = now.timetz().replace(tzinfo=None)  # naive time for compare
    today = now.date()

    if DAY_START <= t <= DAY_END:
        key = WindowKey(prod_day=yyyymmdd(today), shift_type="day")
        start_dt = datetime.combine(today, DAY_START, tzinfo=KST)
        return WindowRange(key=key, start_dt=start_dt, end_dt=now)

    # night
    if t >= NIGHT_START:
        prod = today
    else:
        # t < 08:20:00 -> belongs to previous night's window
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
    """
    저장 테이블:
      prod_day text PRIMARY KEY,
      shift_type text,
      "Mastersample" text,
      first_time text,
      updated_at timestamptz
    """
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

# =========================
# Source FQN (quoted)
# =========================
def source_fqn() -> str:
    return f'"{SRC_SCHEMA}"."{SRC_TABLE}"'

# =========================
# SQL: normalize half-up + window expr
# =========================
def build_window_expr(shift_type: str) -> str:
    """
    norm2 columns: end_day(text), rounded_time_text(text 'HH:MI:SS')
    """
    if shift_type == "day":
        return f"""
( end_day = :prod_day
  AND rounded_time_text >= '{DAY_START.strftime("%H:%M:%S")}'
  AND rounded_time_text <= '{DAY_END.strftime("%H:%M:%S")}'
)
"""
    if shift_type == "night":
        # prod_day night spans prod_day 20:20~23:59:59 and next_day 00:00~08:19:59
        return f"""
(
    ( end_day = :prod_day AND rounded_time_text >= '{NIGHT_START.strftime("%H:%M:%S")}' )
 OR ( end_day = :next_day AND rounded_time_text <= '{NIGHT_END.strftime("%H:%M:%S")}' )
)
"""
    raise ValueError("shift_type must be 'day' or 'night'")

def normalize_cte_sql() -> str:
    """
    end_time(text 'HH:MI:SS.xx') -> rounded_time_text('HH:MI:SS') half-up
    - base_ts: end_day + substring(end_time,1,8)
    - frac_num: substring(end_time from 9) => '.xx' numeric
    - rounded_ts: base_ts + 1 sec if frac>=0.5
    """
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
    first_time: Optional[str] = None  # HH:MI:SS text
    last_pk: Optional[Tuple[str, str]] = None  # (end_day, end_time_norm)
    seen_pk: Set[Tuple[str, str, str]] = None  # (end_day, end_time_norm, md5(contents))
    locked_o: bool = False  # O가 나오면 window 종료까지 DB 조회 중단
    need_recover: bool = False  # S4 soft reboot flag

def init_state() -> State:
    return State(seen_pk=set())

def bootstrap(engine: Engine, wk: WindowKey) -> Tuple[str, Optional[str], Tuple[str, str], Set[Tuple[str, str, str]]]:
    """
    현재 윈도우 전체를 1회 스캔하여:
    - mastersample (O/X)
    - first_time (All OK 최초 발생 HH:MI:SS)
    - last_pk (윈도우 내 최대 (end_day, rounded_time_text))
    - seen_pk set (윈도우 내 모든 row의 (end_day, rounded_time_text, md5(contents)))
    """
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
    first_ts = row["first_ts"]  # datetime or None
    first_time = None if first_ts is None else first_ts.strftime("%H:%M:%S")

    max_day = row["max_day"]
    max_time = row["max_time"]
    # window에 row가 0개면 last_pk 초기값을 (prod_day, '00:00:00')로 둔다
    if not max_day or not max_time:
        max_day = prod_day
        max_time = "00:00:00"

    seen_set: Set[Tuple[str, str, str]] = set()
    for r in rows_seen:
        seen_set.add((r["end_day"], r["rounded_time_text"], r["contents_md5"]))

    return mastersample, first_time, (max_day, max_time), seen_set

def fetch_incremental(engine: Engine, wk: WindowKey, last_pk: Tuple[str, str]) -> List[Dict]:
    """
    last_pk 이후 신규 row만 fetch (end_day, end_time_norm) 기준 증분
    반환 row: end_day, end_time_norm, contents_md5, is_all_ok
    """
    prod_day = wk.prod_day
    next_day = yyyymmdd(parse_yyyymmdd(prod_day) + timedelta(days=1))
    window_expr = build_window_expr(wk.shift_type)

    last_day, last_time = last_pk

    # (end_day > last_day) OR (end_day = last_day AND end_time_norm > last_time)
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
    """
    일반 UPSERT (계산 결과 갱신)
    - X면 first_time은 NULL로 overwrite 허용
    - NULL 덮어쓰기 방지: 일부 컬럼은 COALESCE, 단 first_time은 X일 때 NULL 허용
    """
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
                "first_time": first_time,  # O면 HH:MI:SS, X면 None
                "updated_at": updated_at,
            },
        )

def heartbeat_update(engine: Engine, wk: WindowKey, updated_at: datetime) -> None:
    """
    O 상태에서 DB 조회 없이 5초마다 updated_at만 갱신(모니터링 목적)
    - 다른 컬럼은 건드리지 않음
    """
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
    """
    (S2) Mastersample='O'이면 first_time not null
    (S4) 위반 시 ERROR 로그 + 강등/종료 금지 + soft reboot(bootstrap 재시도) 플래그
    """
    if state.mastersample == "O" and not state.first_time:
        log("ERROR", "SAFETY(S2/S4) violated: Mastersample='O' but first_time is NULL. Will soft-recover by bootstrap.")
        state.need_recover = True  # 다음 loop에서 bootstrap 강제

# =========================
# Main loop
# =========================
def main() -> None:
    log("BOOT", "backend5 mastersample daemon starting")

    engine = make_engine()
    connect_with_retry(engine)

    # ensure save schema/tables
    ensure_schema(engine, SAVE_SCHEMA)
    ensure_save_table(engine, SAVE_SCHEMA, SAVE_DAY_TABLE)
    ensure_save_table(engine, SAVE_SCHEMA, SAVE_NIGHT_TABLE)

    state = init_state()

    while True:
        t0 = time_mod.time()
        now = datetime.now(KST)

        # window 계산
        wr = shift_window_by_now(now)
        wk = wr.key

        # DB 재연결 감지/복구: 각 loop에서 간단 ping (끊기면 connect_with_retry로 복구)
        try:
            with engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
                conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        except Exception as e:
            log("RETRY", f"DB ping failed: {type(e).__name__}: {e} | reconnecting...")
            connect_with_retry(engine)
            # 다음 loop로 (현재 loop는 종료)
            elapsed = time_mod.time() - t0
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            time_mod.sleep(sleep_sec)
            continue

        # 윈도우 변경 or 재시작(처음) or S4 soft recover
        window_changed = (state.window != wk)
        if window_changed:
            log("INFO", f"[WINDOW] changed => prod_day={wk.prod_day} shift={wk.shift_type} start={wr.start_dt} now={wr.end_dt}")
            # window reset
            state.window = wk
            state.mastersample = "X"
            state.first_time = None
            state.last_pk = None
            state.seen_pk = set()
            state.locked_o = False
            state.need_recover = False

            # bootstrap
            log("INFO", "[BOOTSTRAP] start (window full-scan)")
            ms, ft, last_pk, seen = bootstrap(engine, wk)
            state.mastersample = ms
            state.first_time = ft
            state.last_pk = last_pk
            state.seen_pk = seen
            log("INFO", f"[BOOTSTRAP] done | Mastersample={ms}, first_time={ft}, last_pk={last_pk}, seen={len(seen)}")

            # UPSERT 결과
            upsert_result(engine, wk, state.mastersample, state.first_time, now)
            log("INFO", "[UPSERT] bootstrap result saved")

            # O면 조회 중단(lock)
            if state.mastersample == "O":
                state.locked_o = True
                log("INFO", "[LOCK] Mastersample=O => stop DB fetch until window ends (heartbeat only)")

            # safety check
            safety_check_S2_S4(state)

        elif state.need_recover:
            # S4: soft reboot => bootstrap 재시도
            log("INFO", "[RECOVER] soft reboot by bootstrap (due to safety violation)")
            state.need_recover = False
            # unlock temporarily
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
            # 정상 loop
            if state.locked_o:
                # DB 조회 중단, updated_at만 heartbeat
                log("INFO", "[HEARTBEAT] Mastersample=O (no fetch)")
                heartbeat_update(engine, wk, now)
                safety_check_S2_S4(state)  # 계속 체크
            else:
                # incremental fetch
                if state.last_pk is None:
                    # 이론상 window_changed에서 bootstrap 했으므로 여기 안 들어와야 함.
                    state.last_pk = (wk.prod_day, "00:00:00")

                log("INFO", f"[FETCH] start | last_pk={state.last_pk}")
                rows = fetch_incremental(engine, wk, state.last_pk)
                log("INFO", f"[FETCH] done | fetched_rows={len(rows)}")

                if rows:
                    # dedup + last_pk advance + O 판단
                    # last_pk 갱신은 "fetched rows 전체" 기준으로 max를 잡는다(중복이라도 PK는 전진시켜야 함)
                    max_day, max_time = state.last_pk

                    # 신규 유효 rows(중복 제거)
                    new_unique = 0
                    found_all_ok_time: Optional[str] = None

                    for r in rows:
                        end_day = r["end_day"]
                        end_time_norm = r["end_time_norm"]
                        md5v = r["contents_md5"]
                        pk3 = (end_day, end_time_norm, md5v)

                        # last_pk advance
                        if (end_day > max_day) or (end_day == max_day and end_time_norm > max_time):
                            max_day, max_time = end_day, end_time_norm

                        if pk3 in state.seen_pk:
                            continue
                        state.seen_pk.add(pk3)
                        new_unique += 1

                        # All OK 발견
                        if r["is_all_ok"] and state.mastersample != "O":
                            # earliest in this batch (ordered ASC)
                            if found_all_ok_time is None:
                                found_all_ok_time = end_time_norm

                    # last_pk update
                    state.last_pk = (max_day, max_time)
                    log("INFO", f"[PK] advanced => last_pk={state.last_pk} | new_unique={new_unique} | seen={len(state.seen_pk)}")

                    # 상태 업데이트
                    if found_all_ok_time is not None:
                        state.mastersample = "O"
                        state.first_time = found_all_ok_time
                        log("INFO", f"[STATE] Mastersample becomes O | first_time={state.first_time}")

                    # UPSERT 결과 (X든 O든 현재 상태 저장)
                    upsert_result(engine, wk, state.mastersample, state.first_time, now)
                    log("INFO", "[UPSERT] incremental result saved")

                    # O가 됐으면 lock
                    if state.mastersample == "O":
                        state.locked_o = True
                        log("INFO", "[LOCK] Mastersample=O => stop DB fetch until window ends (heartbeat only)")

                    # safety check
                    safety_check_S2_S4(state)

                else:
                    # 신규 row 없으면: 모니터링 updated_at은? (요구사항에 명시 없지만 운영상 유용)
                    # 여기서는 "O일 때만 updated_at 계속"을 강제했으니,
                    # X 상태에서는 불필요한 write 줄이기 위해 no-op.
                    pass

        # loop pacing
        elapsed = time_mod.time() - t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        time_mod.sleep(sleep_sec)

if __name__ == "__main__":
    main()
