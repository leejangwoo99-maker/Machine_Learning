# -*- coding: utf-8 -*-
"""
backend5_mastersample_daemon_TEST_20260128_DAY.py
-------------------------------------------------
TEST FIXED WINDOW:
- prod_day = 20260128 (fixed)
- shift    = day (fixed)
- window   = 2026-01-28 08:20:00 ~ 2026-01-28 20:19:59 (KST band filter)
- loop     = infinite, 5s interval
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Set, List, Dict

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
# TEST: Force window
# =========================
FORCE_MODE = True
FORCE_PROD_DAY = "20260128"
FORCE_SHIFT_TYPE = "day"  # 'day' or 'night'

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
# Helpers
# =========================
def yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

# =========================
# Window (key + info)
# =========================
@dataclass(frozen=True)
class WindowKey:
    prod_day: str
    shift_type: str  # 'day' | 'night'

@dataclass(frozen=True)
class WindowRange:
    key: WindowKey
    start_dt: datetime
    end_dt: datetime

def shift_window_by_now(now: datetime) -> WindowRange:
    """
    원래 자동전환용 함수. (테스트에서는 FORCE_MODE=True로 우회)
    """
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

def forced_window(now: datetime) -> WindowRange:
    pd = parse_yyyymmdd(FORCE_PROD_DAY)
    if FORCE_SHIFT_TYPE == "day":
        start_dt = datetime.combine(pd, DAY_START, tzinfo=KST)
        return WindowRange(key=WindowKey(FORCE_PROD_DAY, "day"), start_dt=start_dt, end_dt=now)
    if FORCE_SHIFT_TYPE == "night":
        start_dt = datetime.combine(pd, NIGHT_START, tzinfo=KST)
        return WindowRange(key=WindowKey(FORCE_PROD_DAY, "night"), start_dt=start_dt, end_dt=now)
    raise ValueError("FORCE_SHIFT_TYPE must be 'day' or 'night'")

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

# =========================
# FQN
# =========================
def source_fqn() -> str:
    return f'"{SRC_SCHEMA}"."{SRC_TABLE}"'

def save_fqn_day() -> str:
    return f'"{SAVE_SCHEMA}"."{SAVE_DAY_TABLE}"'

def save_fqn_night() -> str:
    return f'"{SAVE_SCHEMA}"."{SAVE_NIGHT_TABLE}"'

# =========================
# SQL builders
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
    last_pk: Optional[Tuple[str, str]] = None
    seen_pk: Set[Tuple[str, str, str]] = None
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
    log("BOOT", "backend5 mastersample daemon starting (TEST FIXED WINDOW)")

    engine = make_engine()
    connect_with_retry(engine)

    ensure_schema(engine, SAVE_SCHEMA)
    ensure_save_table(engine, SAVE_SCHEMA, SAVE_DAY_TABLE)
    ensure_save_table(engine, SAVE_SCHEMA, SAVE_NIGHT_TABLE)

    state = init_state()

    # 첫 loop에서 강제 윈도우 bootstrap 하도록 window None 유지
    while True:
        t0 = time_mod.time()
        now = datetime.now(KST)

        # === window 결정 ===
        wr = forced_window(now) if FORCE_MODE else shift_window_by_now(now)
        wk = wr.key

        # ping + work_mem
        try:
            with engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
                conn.execute(text(f"SET work_mem TO '{WORK_MEM}';"))
        except Exception as e:
            log("RETRY", f"DB ping failed: {type(e).__name__}: {e} | reconnecting...")
            connect_with_retry(engine)
            elapsed = time_mod.time() - t0
            time_mod.sleep(max(0.0, LOOP_INTERVAL_SEC - elapsed))
            continue

        window_changed = (state.window != wk)

        if window_changed:
            log(
                "INFO",
                f"[WINDOW] set => prod_day={wk.prod_day} shift={wk.shift_type} "
                f"start={wr.start_dt} now={wr.end_dt} (FORCE_MODE={FORCE_MODE})"
            )
            # reset
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
        time_mod.sleep(max(0.0, LOOP_INTERVAL_SEC - elapsed))

if __name__ == "__main__":
    main()
