# -*- coding: utf-8 -*-
"""
backend5_mastersample_daemon.py  (A안: always-on + window-gated + auto-backoff)
-------------------------------------------------
Backend-5: Mastersample All OK daemon (incremental PK)

요구사항 반영
- 무한루프 5초(기본) + (추가) fetched_rows=0이 5회 연속이면 10초로 자동 백오프
- 윈도우 밖에서는 다음 윈도우 시작까지 대기(항상 실행)
- DB 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹) + [RETRY] 5초마다
- 실행 중 DB 끊김 발생 시에도 무한 재접속 후 계속
- 상시 연결 1개 고정(pool_size=1, max_overflow=0)
- 세션마다 SET work_mem = '4MB'
- PK 증분 기준: (end_day, end_time_norm) (HH:MI:SS half-up round)
- 중복 방지: seen_pk=set[(end_day, end_time_norm, md5(contents))]
- 재실행/윈도우 변경 시 bootstrap(윈도우 start~now full scan) 후 UPSERT
- DELETE/TRUNCATE 금지
- 결과 저장:
  - day  -> i_daily_report.e_mastersample_test_day_daily
  - night-> i_daily_report.e_mastersample_test_night_daily
  - UNIQUE key: prod_day 단일
  - 컬럼: prod_day, shift_type, "Mastersample", first_time(HH:MI:SS text), updated_at(timestamptz)
- 윈도우 경계:
  - day   : [D] 08:20:00 ~ 20:19:59
  - night : [D] 20:20:00 ~ 23:59:59 + [D+1] 00:00:00 ~ 08:19:59
- window 내 Mastersample='O'가 되면 DB 조회 중단(추가 체크 불필요),
  다만 updated_at(heartbeat)은 계속 갱신
- Safety:
  (S2) Mastersample='O'이면 first_time not null
  (S4) 위반 시 ERROR 로그 후 즉시 bootstrap 재시도(soft reboot)

[추가] 데몬 헬스 로그 DB 저장
- 스키마/테이블: k_demon_heath_check."5_log" (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장: end_day, end_time, info, contents (단건 dataframe -> insert)
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

# ✅ NEW: auto backoff
BACKOFF_EMPTY_STREAK = 5
BACKOFF_SLEEP_SEC = 10

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
# Time helpers
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def combine_kst(d: date, t: dtime) -> datetime:
    return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, tzinfo=KST)


def seconds_until(a: datetime, b: datetime) -> float:
    return max(0.0, (b - a).total_seconds())


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

    now = now_kst()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "").lower(),
        "contents": str(contents),
    }

    df = pd.DataFrame(
        [[row["end_day"], row["end_time"], row["info"], row["contents"]]],
        columns=["end_day", "end_time", "info", "contents"],
    )

    insert_sql = f"""
    INSERT INTO "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (end_day, end_time, info, contents)
    VALUES (:end_day, :end_time, :info, :contents);
    """

    try:
        with _DB_LOG_ENGINE.begin() as conn:
            conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
            conn.execute(text(insert_sql), df.iloc[0].to_dict())
    except Exception:
        pass


def _emit(level_tag: str, info: str, msg: str, persist: bool = True) -> None:
    print(f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} [{level_tag}] {msg}", flush=True)
    if persist:
        _log_db_insert(info.lower(), msg)


def log_boot(msg: str) -> None:
    _emit("BOOT", "boot", msg, True)


def log_info(msg: str) -> None:
    _emit("INFO", "info", msg, True)


def log_retry(msg: str) -> None:
    _emit("RETRY", "down", msg, True)


def log_warn(msg: str) -> None:
    _emit("WARN", "warn", msg, True)


def log_error(msg: str) -> None:
    _emit("ERROR", "error", msg, True)


def log_sleep(msg: str) -> None:
    _emit("INFO", "sleep", msg, True)


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


def ensure_db_ready(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("SELECT 1;"))
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))


def connect_forever(engine: Engine) -> None:
    while True:
        try:
            ensure_db_ready(engine)
            log_info(f"DB connected (work_mem={WORK_MEM})")
            return
        except Exception as e:
            # 아직 DB log sink 준비 전일 수 있음
            print(
                f"{now_kst().strftime('%Y-%m-%d %H:%M:%S')} [RETRY] "
                f"DB connect failed: {type(e).__name__}: {e} | retry in {DB_RETRY_INTERVAL_SEC}s",
                flush=True,
            )
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


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


def source_fqn() -> str:
    return f'"{SRC_SCHEMA}"."{SRC_TABLE}"'


def save_fqn(shift_type: str) -> str:
    if shift_type == "day":
        return f'"{SAVE_SCHEMA}"."{SAVE_DAY_TABLE}"'
    return f'"{SAVE_SCHEMA}"."{SAVE_NIGHT_TABLE}"'


# =========================
# Window gating (ACTIVE / IDLE)
# =========================
@dataclass(frozen=True)
class WindowKey:
    prod_day: str
    shift_type: str  # day | night


@dataclass(frozen=True)
class WindowCtx:
    active: bool
    key: Optional[WindowKey]
    start_dt: Optional[datetime]
    end_dt: Optional[datetime]
    next_wake_dt: datetime  # IDLE이면 다음 윈도우 시작 시각(= wake)


def get_window_state(now: datetime) -> WindowCtx:
    """
    항상 실행.
    - ACTIVE: day(08:20~20:19:59) or night(20:20~익일 08:19:59)
    - IDLE: 경계/오차 edge를 안전하게 처리하기 위한 fallback
    """
    now = now.astimezone(KST)
    today = now.date()

    day_start_dt = combine_kst(today, DAY_START)
    day_end_dt = combine_kst(today, DAY_END)
    night_start_dt = combine_kst(today, NIGHT_START)
    night_end_dt = combine_kst(today + timedelta(days=1), NIGHT_END)

    # today day
    if day_start_dt <= now <= day_end_dt:
        k = WindowKey(prod_day=yyyymmdd(today), shift_type="day")
        return WindowCtx(True, k, day_start_dt, day_end_dt, now)

    # today night (20:20~)
    if now >= night_start_dt:
        k = WindowKey(prod_day=yyyymmdd(today), shift_type="night")
        return WindowCtx(True, k, night_start_dt, night_end_dt, now)

    # early morning => yesterday night
    yday = today - timedelta(days=1)
    y_night_start_dt = combine_kst(yday, NIGHT_START)
    y_night_end_dt = combine_kst(today, NIGHT_END)
    if now <= y_night_end_dt:
        k = WindowKey(prod_day=yyyymmdd(yday), shift_type="night")
        return WindowCtx(True, k, y_night_start_dt, y_night_end_dt, now)

    # fallback idle
    next_wake = day_start_dt if now < day_start_dt else night_start_dt
    return WindowCtx(False, None, None, None, next_wake)


# =========================
# SQL: normalize half-up + window filter
# (end_time이 "HH:MI:SS.xxx" 형태라고 가정)
# =========================
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
    to_char((base_ts + CASE WHEN frac_num >= 0.5 THEN interval '1 second' ELSE interval '0 second' END)::time, 'HH24:MI:SS') AS end_time_norm,
    md5(contents) AS contents_md5,
    (contents LIKE :like_all_ok) AS is_all_ok
  FROM norm
)
"""


def build_window_filter_sql(wk: WindowKey) -> Tuple[str, Dict[str, str]]:
    prod = parse_yyyymmdd(wk.prod_day)
    next_day = yyyymmdd(prod + timedelta(days=1))

    if wk.shift_type == "day":
        where_sql = """
        (end_day = :prod_day AND end_time_norm >= '08:20:00' AND end_time_norm <= '20:19:59')
        """
        params = {"prod_day": wk.prod_day, "next_day": next_day}
        return where_sql, params

    # night
    where_sql = """
    (
      (end_day = :prod_day AND end_time_norm >= '20:20:00' AND end_time_norm <= '23:59:59')
      OR
      (end_day = :next_day AND end_time_norm >= '00:00:00' AND end_time_norm <= '08:19:59')
    )
    """
    params = {"prod_day": wk.prod_day, "next_day": next_day}
    return where_sql, params


# =========================
# State
# =========================
@dataclass
class State:
    window: Optional[WindowKey] = None
    mastersample: str = "X"
    first_time: Optional[str] = None
    last_pk: Optional[Tuple[str, str]] = None  # (end_day, end_time_norm)
    seen_pk: Set[Tuple[str, str, str]] = None  # (end_day, end_time_norm, md5)
    locked_o: bool = False
    need_recover: bool = False

    # ✅ NEW: auto backoff
    empty_fetch_streak: int = 0
    sleep_target_sec: int = LOOP_INTERVAL_SEC


def init_state() -> State:
    return State(seen_pk=set())


def reset_backoff(st: State) -> None:
    st.empty_fetch_streak = 0
    st.sleep_target_sec = LOOP_INTERVAL_SEC


def apply_backoff_by_empty_fetch(st: State, fetched_rows: int) -> None:
    """
    fetched_rows=0이 5회 연속이면 sleep_target_sec=10
    한번이라도 데이터가 들어오면 즉시 5초로 복귀
    """
    if fetched_rows <= 0:
        st.empty_fetch_streak += 1
        if st.empty_fetch_streak >= BACKOFF_EMPTY_STREAK:
            st.sleep_target_sec = BACKOFF_SLEEP_SEC
        else:
            st.sleep_target_sec = LOOP_INTERVAL_SEC
    else:
        reset_backoff(st)


# =========================
# DB ops
# =========================
def upsert_result(engine: Engine, wk: WindowKey, mastersample: str, first_time: Optional[str], updated_at: datetime) -> None:
    fqn = save_fqn(wk.shift_type)
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
    fqn = save_fqn(wk.shift_type)
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


def safety_check_S2_S4(st: State) -> None:
    if st.mastersample == "O" and not st.first_time:
        log_error("SAFETY(S2/S4) violated: Mastersample='O' but first_time is NULL. soft reboot by bootstrap.")
        st.need_recover = True


def bootstrap(engine: Engine, wk: WindowKey) -> Tuple[str, Optional[str], Tuple[str, str], Set[Tuple[str, str, str]]]:
    where_sql, params = build_window_filter_sql(wk)

    sql = normalize_cte_sql() + f"""
, win AS (
  SELECT end_day, end_time_norm, contents_md5, is_all_ok, rounded_ts
  FROM norm2
  WHERE {where_sql}
),
hits AS (
  SELECT rounded_ts
  FROM win
  WHERE is_all_ok = TRUE
)
SELECT
  CASE WHEN EXISTS(SELECT 1 FROM hits) THEN 'O' ELSE 'X' END AS mastersample,
  (SELECT MIN(rounded_ts) FROM hits) AS first_ts,
  (SELECT end_day FROM win ORDER BY end_day DESC, end_time_norm DESC LIMIT 1) AS max_day,
  (SELECT end_time_norm FROM win ORDER BY end_day DESC, end_time_norm DESC LIMIT 1) AS max_time
;
"""

    sql_seen = normalize_cte_sql() + f"""
SELECT end_day, end_time_norm, contents_md5
FROM norm2
WHERE {where_sql}
;
"""

    p = dict(params)
    p["like_all_ok"] = LIKE_ALL_OK

    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        row = conn.execute(text(sql), p).mappings().first()
        rows_seen = conn.execute(text(sql_seen), p).mappings().all()

    mastersample = row["mastersample"]
    first_ts = row["first_ts"]
    first_time = None if first_ts is None else first_ts.strftime("%H:%M:%S")

    max_day = row["max_day"] or wk.prod_day
    max_time = row["max_time"] or "00:00:00"

    seen_set: Set[Tuple[str, str, str]] = set()
    for r in rows_seen:
        seen_set.add((r["end_day"], r["end_time_norm"], r["contents_md5"]))

    return mastersample, first_time, (max_day, max_time), seen_set


def fetch_incremental(engine: Engine, wk: WindowKey, last_pk: Tuple[str, str]) -> List[Dict]:
    where_sql, params = build_window_filter_sql(wk)
    last_day, last_time = last_pk

    pk_expr = """
(
  (end_day > :last_day)
  OR (end_day = :last_day AND end_time_norm > :last_time)
)
"""

    sql = normalize_cte_sql() + f"""
SELECT
  end_day,
  end_time_norm,
  contents_md5,
  is_all_ok
FROM norm2
WHERE {where_sql}
  AND {pk_expr}
ORDER BY end_day ASC, end_time_norm ASC, contents_md5 ASC
;
"""

    p = dict(params)
    p.update({"like_all_ok": LIKE_ALL_OK, "last_day": last_day, "last_time": last_time})

    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        rows = conn.execute(text(sql), p).mappings().all()

    return [dict(r) for r in rows]


# =========================
# Main
# =========================
def main() -> None:
    global _DB_LOG_ENGINE, _DB_LOG_READY

    log_boot("backend5 mastersample daemon starting")

    engine = make_engine()

    # DB 연결 무한 재시도
    while True:
        try:
            connect_forever(engine)

            # 테이블 준비
            ensure_schema(engine, SAVE_SCHEMA)
            ensure_save_table(engine, SAVE_SCHEMA, SAVE_DAY_TABLE)
            ensure_save_table(engine, SAVE_SCHEMA, SAVE_NIGHT_TABLE)

            ensure_health_log_table(engine)
            _DB_LOG_ENGINE = engine
            _DB_LOG_READY = True
            log_info(f'health log target ready: "{HEALTH_SCHEMA}"."{HEALTH_TABLE}"')
            break
        except Exception as e:
            _DB_LOG_READY = False
            log_retry(f"DB prepare failed: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before retry")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = make_engine()

    st = init_state()

    while True:
        loop_t0 = time_mod.time()

        try:
            now = now_kst()
            wctx = get_window_state(now)

            # === 윈도우 밖이면 대기 ===
            if not wctx.active or wctx.key is None:
                # IDLE 상태에서는 backoff 개념이 무의미하므로 기본 5초로만 쪼개 대기
                sleep_sec = min(LOOP_INTERVAL_SEC, seconds_until(now, wctx.next_wake_dt))
                log_sleep(f"[IDLE] outside window. next_wake={wctx.next_wake_dt} sleep={sleep_sec:.2f}s")
                time_mod.sleep(sleep_sec)
                continue

            wk = wctx.key

            # ping
            ensure_db_ready(engine)

            window_changed = (st.window != wk)

            if window_changed:
                log_info(f"[WINDOW] changed => prod_day={wk.prod_day} shift={wk.shift_type} (active)")
                st.window = wk
                st.mastersample = "X"
                st.first_time = None
                st.last_pk = None
                st.seen_pk = set()
                st.locked_o = False
                st.need_recover = False
                reset_backoff(st)  # ✅ NEW

                log_info("[BOOTSTRAP] start (window full-scan)")
                ms, ft, last_pk, seen = bootstrap(engine, wk)
                st.mastersample = ms
                st.first_time = ft
                st.last_pk = last_pk
                st.seen_pk = seen
                log_info(f"[BOOTSTRAP] done | Mastersample={ms}, first_time={ft}, last_pk={last_pk}, seen={len(seen)}")

                upsert_result(engine, wk, st.mastersample, st.first_time, now)
                log_info("[UPSERT] bootstrap result saved")

                if st.mastersample == "O":
                    st.locked_o = True
                    log_info("[LOCK] Mastersample=O => stop fetch until window ends (heartbeat only)")

                safety_check_S2_S4(st)

            elif st.need_recover:
                log_info("[RECOVER] soft reboot by bootstrap (safety)")
                st.need_recover = False
                st.locked_o = False
                reset_backoff(st)  # ✅ NEW

                ms, ft, last_pk, seen = bootstrap(engine, wk)
                st.mastersample = ms
                st.first_time = ft
                st.last_pk = last_pk
                st.seen_pk = seen
                log_info(f"[BOOTSTRAP] done(recover) | Mastersample={ms}, first_time={ft}, last_pk={last_pk}, seen={len(seen)}")

                upsert_result(engine, wk, st.mastersample, st.first_time, now)
                log_info("[UPSERT] recover result saved")

                if st.mastersample == "O":
                    st.locked_o = True
                    log_info("[LOCK] Mastersample=O => stop fetch until window ends (heartbeat only)")

                safety_check_S2_S4(st)

            else:
                if st.locked_o:
                    # lock 상태에서는 fetch 자체가 없으므로 backoff 카운트는 건드리지 않음
                    heartbeat_update(engine, wk, now)
                    log_info("[HEARTBEAT] Mastersample=O (no fetch)")
                    safety_check_S2_S4(st)
                else:
                    if st.last_pk is None:
                        st.last_pk = (wk.prod_day, "00:00:00")

                    log_info(f"[FETCH] start | last_pk={st.last_pk}")
                    rows = fetch_incremental(engine, wk, st.last_pk)
                    fetched_n = len(rows)
                    log_info(f"[FETCH] done | fetched_rows={fetched_n}")

                    # ✅ NEW: auto backoff
                    apply_backoff_by_empty_fetch(st, fetched_n)
                    if st.sleep_target_sec != LOOP_INTERVAL_SEC:
                        log_info(f"[BACKOFF] empty_fetch_streak={st.empty_fetch_streak} => sleep_target={st.sleep_target_sec}s")
                    else:
                        # streak가 쌓이는 구간도 로그로 보고 싶으면 주석 해제
                        # log_info(f"[BACKOFF] empty_fetch_streak={st.empty_fetch_streak} sleep_target={st.sleep_target_sec}s")
                        pass

                    if rows:
                        max_day, max_time = st.last_pk
                        new_unique = 0
                        found_all_ok_time: Optional[str] = None

                        for r in rows:
                            end_day = r["end_day"]
                            end_time_norm = r["end_time_norm"]
                            md5v = r["contents_md5"]
                            pk3 = (end_day, end_time_norm, md5v)

                            if (end_day > max_day) or (end_day == max_day and end_time_norm > max_time):
                                max_day, max_time = end_day, end_time_norm

                            if pk3 in st.seen_pk:
                                continue
                            st.seen_pk.add(pk3)
                            new_unique += 1

                            if r["is_all_ok"] and st.mastersample != "O" and found_all_ok_time is None:
                                found_all_ok_time = end_time_norm

                        st.last_pk = (max_day, max_time)
                        log_info(f"[PK] advanced => last_pk={st.last_pk} | new_unique={new_unique} | seen={len(st.seen_pk)}")

                        if found_all_ok_time is not None:
                            st.mastersample = "O"
                            st.first_time = found_all_ok_time
                            log_info(f"[STATE] Mastersample becomes O | first_time={st.first_time}")

                        upsert_result(engine, wk, st.mastersample, st.first_time, now)
                        log_info("[UPSERT] incremental result saved")

                        if st.mastersample == "O":
                            st.locked_o = True
                            log_info("[LOCK] Mastersample=O => stop fetch until window ends (heartbeat only)")

                        safety_check_S2_S4(st)
                    else:
                        # 데이터 없어도 heartbeat은 찍어두면 모니터링에 좋음
                        heartbeat_update(engine, wk, now)

        except (OperationalError, DBAPIError) as e:
            _DB_LOG_READY = False
            log_retry(f"DB error: {type(e).__name__}: {e}")

            # 재연결 무한 루프
            while True:
                try:
                    log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine = make_engine()
                    connect_forever(engine)

                    ensure_health_log_table(engine)
                    _DB_LOG_ENGINE = engine
                    _DB_LOG_READY = True
                    log_info("DB reconnected + health log sink re-enabled")

                    # 재연결 후 현재 윈도우로 강제 bootstrap
                    st.window = None
                    reset_backoff(st)
                    break
                except Exception as e2:
                    log_retry(f"reconnect failed: {type(e2).__name__}: {e2}")
                    continue

        except Exception as e:
            log_error(f"Unhandled error: {type(e).__name__}: {e}")

        # pacing (✅ NEW: st.sleep_target_sec 사용)
        elapsed = time_mod.time() - loop_t0
        sleep_sec = max(0.0, float(st.sleep_target_sec) - elapsed)
        if sleep_sec > 0:
            time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    main()