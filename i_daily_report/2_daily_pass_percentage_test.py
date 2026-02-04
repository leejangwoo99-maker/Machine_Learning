# -*- coding: utf-8 -*-
"""
backend_station_daily_percentage_daemon.py (TEST FIXED w/ SHIFT END CAP + GoodFile only)
--------------------------------------------------------------------------------------
Station PASS percentage daemon (incremental PK, 5s loop) - fixed window for test

✅ TEST 고정:
- prod_day = 20260128
- shift_type = day
- window_start = 2026-01-28 08:30:00 (KST)
- window_end = min(now(KST), shift_end)

✅ 추가 조건:
- goodorbad = 'GoodFile' 만 집계 대상

그 외 요구사항 유지:
- DF 콘솔 출력 없음
- 무한 루프 5초
- DB 실패/끊김 무한 재시도
- pool_size=1
- work_mem 설정
- 증분 PK=(end_day,end_time,barcode_information)
- 신규 row 있을 때만 UPSERT (단, BOOT 직후 1회 저장은 예외)
"""

from __future__ import annotations

import os
import time as time_mod
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / Const
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5
FETCH_LIMIT = 5000

PG_WORK_MEM = os.getenv("PG_WORK_MEM", "64MB")

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"]

SAVE_SCHEMA = "i_daily_report"
DAY_TABLE   = "b_station_day_daily_percentage"
NIGHT_TABLE = "b_station_night_daily_percentage"
KEY_COLS    = ["prod_day", "shift_type"]

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"
SRC_FQN    = f"{SRC_SCHEMA}.{SRC_TABLE}"

# ✅ 조건: GoodFile만
GOODORBAD_ALLOW = "GoodFile"

# =========================
# [TEST FIXED] Window target
# =========================
TEST_PROD_DAY = "20260128"
TEST_SHIFT_TYPE = "day"   # 'day' or 'night'

# =========================
# DB Config
# =========================
DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "leejangwoo1!"),
}

# =========================
# Logging
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)

def ts() -> str:
    return now_kst().strftime("%Y-%m-%d %H:%M:%S")

def log_boot(msg: str) -> None:
    print(f"{ts()} [BOOT] {msg}")

def log_info(msg: str) -> None:
    print(f"{ts()} [INFO] {msg}")

def log_retry(msg: str) -> None:
    print(f"{ts()} [RETRY] {msg}")

# =========================
# Engine / Session
# =========================
def make_engine() -> Engine:
    user = urllib.parse.quote_plus(DB_CONFIG["user"])
    pw   = urllib.parse.quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    db   = DB_CONFIG["dbname"]
    url  = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_recycle=1800,
    )

def set_session_params(conn) -> None:
    conn.execute(text(f"SET work_mem = '{PG_WORK_MEM}'"))
    conn.execute(text("SET client_encoding = 'UTF8'"))

def connect_with_retry() -> Engine:
    while True:
        try:
            engine = make_engine()
            with engine.begin() as conn:
                set_session_params(conn)
                conn.execute(text("SELECT 1")).scalar()
            return engine
        except Exception as e:
            log_retry(f"DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

# =========================
# Window (TEST FIXED w/ CAP)
# =========================
@dataclass(frozen=True)
class Window:
    prod_day: str
    shift_type: str
    start_dt: datetime
    end_dt: datetime

def fixed_test_window(now: datetime) -> Window:
    """
    [TEST FIXED]
    prod_day/shift 고정 + end=min(now, shift_end)로 캡

    - day   : start=08:30:00, end=20:29:59
    - night : start=20:30:00, end=다음날 08:29:59
    """
    d0 = datetime.strptime(TEST_PROD_DAY, "%Y%m%d").replace(tzinfo=KST)

    if TEST_SHIFT_TYPE == "day":
        start = d0.replace(hour=8, minute=30, second=0, microsecond=0)
        shift_end = d0.replace(hour=20, minute=29, second=59, microsecond=0)
        end = min(now, shift_end)
        return Window(TEST_PROD_DAY, "day", start, end)

    if TEST_SHIFT_TYPE == "night":
        start = d0.replace(hour=20, minute=30, second=0, microsecond=0)
        d1 = d0 + timedelta(days=1)
        shift_end = d1.replace(hour=8, minute=29, second=59, microsecond=0)
        end = min(now, shift_end)
        return Window(TEST_PROD_DAY, "night", start, end)

    raise ValueError("TEST_SHIFT_TYPE must be 'day' or 'night'")

# =========================
# PK helpers
# =========================
PK = Tuple[str, str, str]  # (end_day, end_time, barcode_information)

def pk_str(pk: Optional[PK]) -> str:
    if not pk:
        return "None"
    return f"({pk[0]}, {pk[1]}, {pk[2]})"

# =========================
# Queries (✅ goodorbad='GoodFile' 필터 추가)
# =========================
BOOTSTRAP_AGG_SQL = f"""
WITH base AS (
  SELECT station, result
  FROM {SRC_FQN}
  WHERE
    station = ANY(:stations)
    AND result IN ('PASS','FAIL')
    AND goodorbad = :goodorbad
    AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')
        BETWEEN :start_dt AND :end_dt
)
SELECT
  station,
  SUM(CASE WHEN result='PASS' THEN 1 ELSE 0 END)::int AS pass_cnt,
  COUNT(*)::int AS total_cnt
FROM base
GROUP BY station
ORDER BY station
"""

BOOTSTRAP_LASTPK_SQL = f"""
SELECT end_day, end_time, barcode_information
FROM {SRC_FQN}
WHERE
  station = ANY(:stations)
  AND result IN ('PASS','FAIL')
  AND goodorbad = :goodorbad
  AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')
      BETWEEN :start_dt AND :end_dt
ORDER BY end_day DESC, end_time DESC, barcode_information DESC
LIMIT 1
"""

INCR_FETCH_SQL = f"""
SELECT end_day, end_time, barcode_information, station, result
FROM {SRC_FQN}
WHERE
  station = ANY(:stations)
  AND result IN ('PASS','FAIL')
  AND goodorbad = :goodorbad
  AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')
      BETWEEN :start_dt AND :end_dt
  AND (end_day, end_time, barcode_information) > (:pk_day, :pk_time, :pk_barcode)
ORDER BY end_day ASC, end_time ASC, barcode_information ASC
LIMIT :limit
"""

# =========================
# Aggregation / DF
# =========================
def fmt_cell(pass_cnt: int, total_cnt: int) -> str:
    pct = (pass_cnt / total_cnt * 100.0) if total_cnt > 0 else 0.0
    return f"PASS: {pass_cnt}, total: {total_cnt}, PASS_pct:{pct:.2f}"

def build_df(prod_day: str, shift_type: str, agg: Dict[str, Dict[str, int]]) -> pd.DataFrame:
    v_pass  = agg["Vision1"]["pass"]  + agg["Vision2"]["pass"]
    v_total = agg["Vision1"]["total"] + agg["Vision2"]["total"]

    out = {
        "prod_day": prod_day,
        "shift_type": shift_type,
        "FCT1": fmt_cell(agg["FCT1"]["pass"], agg["FCT1"]["total"]),
        "FCT2": fmt_cell(agg["FCT2"]["pass"], agg["FCT2"]["total"]),
        "FCT3": fmt_cell(agg["FCT3"]["pass"], agg["FCT3"]["total"]),
        "FCT4": fmt_cell(agg["FCT4"]["pass"], agg["FCT4"]["total"]),
        "Vision1": fmt_cell(agg["Vision1"]["pass"], agg["Vision1"]["total"]),
        "Vision2": fmt_cell(agg["Vision2"]["pass"], agg["Vision2"]["total"]),
        "FCT>Vision Total": fmt_cell(v_pass, v_total),
        "updated_at": now_kst(),
    }
    return pd.DataFrame([out])

# =========================
# Save (schema/table ensure + safe UPSERT)
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        set_session_params(conn)
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def ensure_table(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> None:
    cols = list(df.columns)
    with engine.begin() as conn:
        set_session_params(conn)

        exists = conn.execute(text("""
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = :schema AND table_name = :table
            );
        """), {"schema": schema, "table": table}).scalar()

        if not exists:
            ddl_cols = []
            for c in cols:
                if c == "updated_at":
                    ddl_cols.append(f'"{c}" timestamptz')
                else:
                    ddl_cols.append(f'"{c}" text')

            ddl_cols_sql = ",\n  ".join(ddl_cols)
            uq_sql = ", ".join([f'"{c}"' for c in key_cols])

            conn.execute(text(f"""
                CREATE TABLE {schema}.{table} (
                  {ddl_cols_sql},
                  CONSTRAINT uq_{table}_key UNIQUE ({uq_sql})
                );
            """))
        else:
            existing = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """), {"schema": schema, "table": table}).fetchall()
            existing_cols = {r[0] for r in existing}

            for c in cols:
                if c in existing_cols:
                    continue
                col_type = "timestamptz" if c == "updated_at" else "text"
                conn.execute(text(f'ALTER TABLE {schema}.{table} ADD COLUMN "{c}" {col_type};'))

def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> None:
    assert len(df) == 1
    row0 = df.iloc[0].to_dict()
    cols = list(df.columns)

    bind_map = {c: f"v{i}" for i, c in enumerate(cols)}
    params = {bind_map[c]: row0.get(c) for c in cols}

    insert_cols_sql = ", ".join([f'"{c}"' for c in cols])
    values_sql = ", ".join([f":{bind_map[c]}" for c in cols])
    conflict_cols_sql = ", ".join([f'"{c}"' for c in key_cols])

    set_parts = []
    for c in cols:
        if c in key_cols:
            continue
        if c == "updated_at":
            set_parts.append(f'"{c}" = EXCLUDED."{c}"')
        else:
            set_parts.append(f'"{c}" = COALESCE(EXCLUDED."{c}", {schema}.{table}."{c}")')
    set_sql = ", ".join(set_parts)

    sql = f"""
        INSERT INTO {schema}.{table} ({insert_cols_sql})
        VALUES ({values_sql})
        ON CONFLICT ({conflict_cols_sql})
        DO UPDATE SET {set_sql};
    """

    with engine.begin() as conn:
        set_session_params(conn)
        conn.execute(text(sql), params)

# =========================
# Runtime state
# =========================
@dataclass
class State:
    window: Window
    last_pk: Optional[PK]
    agg: Dict[str, Dict[str, int]]

def empty_agg() -> Dict[str, Dict[str, int]]:
    return {s: {"pass": 0, "total": 0} for s in STATIONS}

# =========================
# Core steps
# =========================
def bootstrap(engine: Engine, w: Window) -> Tuple[Dict[str, Dict[str, int]], Optional[PK]]:
    params = {
        "stations": STATIONS,
        "goodorbad": GOODORBAD_ALLOW,
        "start_dt": w.start_dt.replace(tzinfo=None),
        "end_dt": w.end_dt.replace(tzinfo=None),
    }

    agg = empty_agg()
    with engine.begin() as conn:
        set_session_params(conn)
        rows = conn.execute(text(BOOTSTRAP_AGG_SQL), params).mappings().all()

        for r in rows:
            s = r.get("station")
            if s in agg:
                agg[s]["pass"] = int(r.get("pass_cnt") or 0)
                agg[s]["total"] = int(r.get("total_cnt") or 0)

        last_row = conn.execute(text(BOOTSTRAP_LASTPK_SQL), params).mappings().first()

    last_pk: Optional[PK] = None
    if last_row:
        last_pk = (str(last_row["end_day"]), str(last_row["end_time"]), str(last_row["barcode_information"]))

    return agg, last_pk

def fetch_incremental(engine: Engine, w: Window, last_pk: PK) -> Tuple[List[dict], Optional[PK]]:
    params = {
        "stations": STATIONS,
        "goodorbad": GOODORBAD_ALLOW,
        "start_dt": w.start_dt.replace(tzinfo=None),
        "end_dt": w.end_dt.replace(tzinfo=None),
        "pk_day": last_pk[0],
        "pk_time": last_pk[1],
        "pk_barcode": last_pk[2],
        "limit": FETCH_LIMIT,
    }

    with engine.begin() as conn:
        set_session_params(conn)
        rows = conn.execute(text(INCR_FETCH_SQL), params).mappings().all()

    if not rows:
        return [], None

    rlast = rows[-1]
    new_last_pk: PK = (str(rlast["end_day"]), str(rlast["end_time"]), str(rlast["barcode_information"]))
    return list(rows), new_last_pk

def apply_rows(agg: Dict[str, Dict[str, int]], rows: List[dict]) -> None:
    for r in rows:
        s = r.get("station")
        if s not in agg:
            continue
        result = r.get("result")
        agg[s]["total"] += 1
        if result == "PASS":
            agg[s]["pass"] += 1

def target_table(shift_type: str) -> str:
    return DAY_TABLE if shift_type == "day" else NIGHT_TABLE

# =========================
# Main loop
# =========================
def main() -> None:
    log_boot(f"daemon starting (TEST FIXED prod_day={TEST_PROD_DAY} shift={TEST_SHIFT_TYPE}, goodorbad={GOODORBAD_ALLOW})")

    engine = connect_with_retry()
    log_info(f"DB connected (work_mem={PG_WORK_MEM})")

    ensure_schema(engine, SAVE_SCHEMA)

    w = fixed_test_window(now_kst())
    agg, last_pk = bootstrap(engine, w)
    state = State(window=w, last_pk=last_pk, agg=agg)

    tbl = target_table(state.window.shift_type)

    # BOOT 직후 1회 저장
    df0 = build_df(state.window.prod_day, state.window.shift_type, state.agg)
    ensure_table(engine, SAVE_SCHEMA, tbl, df0, KEY_COLS)
    upsert_df(engine, SAVE_SCHEMA, tbl, df0, KEY_COLS)
    log_info(f"[UPSERT] bootstrap saved => {SAVE_SCHEMA}.{tbl} prod_day={state.window.prod_day} shift={state.window.shift_type}")
    log_info(f"[WINDOW] start={state.window.start_dt} end={state.window.end_dt} (capped)")
    log_info(f"[LAST_PK] {pk_str(state.last_pk)}")

    while True:
        loop_start = time_mod.time()

        try:
            now = now_kst()
            state.window = fixed_test_window(now)

            if state.last_pk is None:
                log_info("[LAST_PK] None => bootstrap rebuild (no rows yet)")
                agg, last_pk = bootstrap(engine, state.window)
                state.agg = agg
                state.last_pk = last_pk
                log_info(f"[LAST_PK] {pk_str(state.last_pk)}")
            else:
                log_info(f"[LAST_PK] {pk_str(state.last_pk)}")
                new_rows, new_last_pk = fetch_incremental(engine, state.window, state.last_pk)

                if new_rows:
                    log_info(f"[FETCH] rows={len(new_rows)}")
                    apply_rows(state.agg, new_rows)
                    state.last_pk = new_last_pk

                    df = build_df(state.window.prod_day, state.window.shift_type, state.agg)
                    ensure_table(engine, SAVE_SCHEMA, tbl, df, KEY_COLS)
                    upsert_df(engine, SAVE_SCHEMA, tbl, df, KEY_COLS)
                    log_info(f"[UPSERT] done => {SAVE_SCHEMA}.{tbl} prod_day={state.window.prod_day} shift={state.window.shift_type} last_pk={pk_str(state.last_pk)}")
                else:
                    log_info("[FETCH] rows=0 (no update)")

        except (OperationalError, DBAPIError) as e:
            log_retry(f"DB error: {type(e).__name__}: {e}")
            try:
                engine.dispose()
            except Exception:
                pass
            engine = connect_with_retry()
            log_info("DB reconnected")
        except Exception as e:
            log_retry(f"Unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_start
        time_mod.sleep(max(0.0, LOOP_INTERVAL_SEC - elapsed))

if __name__ == "__main__":
    main()
