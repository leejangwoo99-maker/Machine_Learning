# -*- coding: utf-8 -*-
"""
backend3_fail_step_repeat_daemon_TEST_20260128_DAY.py
----------------------------------------------------
TEST 고정 버전:
- prod_day = 20260128 (고정)
- shift    = day (고정)
- window   = 2026-01-28 08:30:00 ~ 2026-01-28 20:29:59 (KST, 고정)

나머지 요구사항(무한루프/5초/재접속/seen_pk/증분/업서트/삭제금지) 유지
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from typing import Dict, Tuple, List, Optional, Set

import pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# =========================
# ✅ TEST 고정 설정
# =========================
TEST_PROD_DAY = "20260128"
TEST_SHIFT = "day"  # 'day' only for this test
TEST_START_DT = datetime(2026, 1, 28, 8, 30, 0, tzinfo=KST)
TEST_END_DT   = datetime(2026, 1, 28, 20, 29, 59, tzinfo=KST)

# =========================
# DB Config
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# =========================
# Source / Save Tables
# =========================
SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"
REMARK_SCHEMA = "g_production_film"
REMARK_TABLE = "remark_info"

SAVE_SCHEMA = "i_daily_report"

DAY_T1 = "c_1time_step_decription_day_daily"
DAY_T2 = "c_2time_step_decription_day_daily"
DAY_T3 = "c_3time_over_step_decription_day_daily"

NIGHT_T1 = "c_1time_step_decription_night_daily"
NIGHT_T2 = "c_2time_step_decription_night_daily"
NIGHT_T3 = "c_3time_over_step_decription_night_daily"

COL_1 = "1회 FAIL_step_description"
COL_2 = "2회 반복_FAIL_step_description"
COL_3 = "3회 이상 반복_FAIL_step_description"

# =========================
# Logging
# =========================
def log_boot(msg: str) -> None:
    print(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} [BOOT] {msg}", flush=True)

def log_info(msg: str) -> None:
    print(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} [INFO] {msg}", flush=True)

def log_retry(msg: str) -> None:
    print(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} [RETRY] {msg}", flush=True)

def log_warn(msg: str) -> None:
    print(f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')} [WARN] {msg}", flush=True)

# =========================
# Engine / Session
# =========================
def make_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
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
        conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))

# =========================
# Window (TEST 고정)
# =========================
@dataclass(frozen=True)
class Window:
    prod_day: str
    shift: str
    start_dt: datetime
    now_dt: datetime  # 여기서는 "window_end"로 사용 (고정 end)

def now_kst() -> datetime:
    return datetime.now(KST)

def get_window_fixed() -> Window:
    """
    ✅ TEST 고정 윈도우 반환
    now_dt는 TEST_END_DT로 고정 (데이터를 끝까지 다 읽기)
    """
    return Window(prod_day=TEST_PROD_DAY, shift=TEST_SHIFT, start_dt=TEST_START_DT, now_dt=TEST_END_DT)

def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def time_norm(dt: datetime) -> str:
    return dt.strftime("%H%M%S")

def build_segments(w: Window) -> List[Tuple[str, str, str]]:
    """
    day 고정: 08:30:00 ~ 20:29:59 (고정 end)
    """
    if w.shift != "day":
        raise RuntimeError("TEST_SHIFT is day only in this test script")
    return [(w.prod_day, "083000", time_norm(w.now_dt))]

# =========================
# SQL (FAIL row fetch)
# =========================
def build_where_segments(segments: List[Tuple[str, str, str]]) -> Tuple[str, Dict[str, str]]:
    parts = []
    params: Dict[str, str] = {}
    for i, (d, s, e) in enumerate(segments):
        parts.append(f"(ft.end_day = :d{i} AND LPAD(ft.end_time, 6, '0') BETWEEN :s{i} AND :e{i})")
        params[f"d{i}"] = d
        params[f"s{i}"] = s
        params[f"e{i}"] = e
    return " OR ".join(parts), params

def fetch_fail_rows(engine: Engine, w: Window, last_pk: Optional[Tuple[str, str, str]]) -> pd.DataFrame:
    segments = build_segments(w)
    seg_sql, seg_params = build_where_segments(segments)

    inc_sql = ""
    inc_params: Dict[str, str] = {}
    if last_pk is not None:
        # ✅ 증분 조건 (>=) + seen_pk로 dedup
        inc_sql = "AND (ft.end_day, LPAD(ft.end_time, 6, '0'), ft.barcode_information) >= (:lday, :ltime, :lbarcode)"
        inc_params = {"lday": last_pk[0], "ltime": last_pk[1], "lbarcode": last_pk[2]}

    sql = f"""
    SELECT
        COALESCE(ri.pn, 'UNKNOWN') AS pn,
        ft.barcode_information,
        ft.step_description,
        ft.end_day,
        LPAD(ft.end_time, 6, '0') AS end_time_norm
    FROM {SRC_SCHEMA}.{SRC_TABLE} ft
    LEFT JOIN {REMARK_SCHEMA}.{REMARK_TABLE} ri
        ON ri.barcode_information = SUBSTRING(ft.barcode_information FROM 18 FOR 1)
    WHERE
        ft.result = 'FAIL'
        AND ({seg_sql})
        {inc_sql}
    ORDER BY
        ft.end_day ASC,
        LPAD(ft.end_time, 6, '0') ASC,
        ft.barcode_information ASC,
        ft.step_description ASC
    ;
    """

    params = {}
    params.update(seg_params)
    params.update(inc_params)

    with engine.connect() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
        return pd.read_sql(text(sql), conn, params=params)

# =========================
# In-memory aggregation (+ seen_pk dedup)
# =========================
PairKey = Tuple[str, str, str]  # (pn, barcode_information, step_description)
SeenKey = Tuple[str, str, str, str]  # (end_day, end_time_norm, barcode_information, step_description)

def apply_new_rows(pair_counts: Dict[PairKey, int], seen_pk: Set[SeenKey], df_new: pd.DataFrame) -> int:
    applied = 0
    for r in df_new.itertuples(index=False):
        end_day = str(r.end_day)
        end_time_norm = str(r.end_time_norm)
        barcode = str(r.barcode_information)
        step = str(r.step_description)
        sk: SeenKey = (end_day, end_time_norm, barcode, step)
        if sk in seen_pk:
            continue
        seen_pk.add(sk)

        pn = str(r.pn)
        pk: PairKey = (pn, barcode, step)
        pair_counts[pk] = pair_counts.get(pk, 0) + 1
        applied += 1
    return applied

def summarize_bucket_dfs(prod_day: str, shift: str, pair_counts: Dict[PairKey, int]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    b1: Dict[Tuple[str, str], int] = {}
    b2: Dict[Tuple[str, str], int] = {}
    b3: Dict[Tuple[str, str], int] = {}

    for (pn, _barcode, step), c in pair_counts.items():
        k = (pn, step)
        if c == 1:
            b1[k] = b1.get(k, 0) + 1
        elif c == 2:
            b2[k] = b2.get(k, 0) + 1
        else:
            b3[k] = b3.get(k, 0) + 1

    updated_at = datetime.now(KST)

    def to_df(m: Dict[Tuple[str, str], int], step_col: str) -> pd.DataFrame:
        if not m:
            return pd.DataFrame(columns=["prod_day", "shift_type", "pn", step_col, "count", "updated_at"])
        rows = []
        for (pn, step), cnt in m.items():
            rows.append({
                "prod_day": prod_day,
                "shift_type": shift,
                "pn": pn,
                step_col: step,
                "count": cnt,
                "updated_at": updated_at,
            })
        df = pd.DataFrame(rows)
        return df.sort_values(["count", "pn"], ascending=[False, True]).reset_index(drop=True)

    return (to_df(b1, COL_1), to_df(b2, COL_2), to_df(b3, COL_3))

# =========================
# Save (schema/table/upsert)  [TEXT 유지]
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def ensure_table(engine: Engine, schema: str, table: str, columns: List[str], key_cols: List[str]) -> None:
    ddl_cols = []
    for c in columns:
        if c == "updated_at":
            ddl_cols.append(f'"{c}" timestamptz')
        else:
            ddl_cols.append(f'"{c}" text')
    ddl_cols_sql = ",\n  ".join(ddl_cols)
    key_sql = ", ".join([f'"{c}"' for c in key_cols])

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      {ddl_cols_sql},
      CONSTRAINT {table}__uk UNIQUE ({key_sql})
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))

def normalize_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if c == "updated_at":
            continue
        out[c] = out[c].astype("string")
    return out.where(pd.notnull(out), None)

def key_cols_for(df: pd.DataFrame) -> List[str]:
    step_cols = [c for c in df.columns if str(c).endswith("step_description")]
    step_col = step_cols[0]
    return ["prod_day", "shift_type", "pn", step_col]

def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> None:
    if df.empty:
        log_info(f"[UPSERT] skip empty -> {schema}.{table}")
        return

    df = normalize_df_for_db(df)
    cols = list(df.columns)
    pkeys = [f"v{i}" for i in range(len(cols))]

    col_sql = ", ".join([f'"{c}"' for c in cols])
    val_sql = ", ".join([f":{k}" for k in pkeys])
    key_sql = ", ".join([f'"{c}"' for c in key_cols])
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in key_cols])

    sql = f"""
    INSERT INTO {schema}.{table} ({col_sql})
    VALUES ({val_sql})
    ON CONFLICT ({key_sql})
    DO UPDATE SET
      {set_sql}
    ;
    """

    recs = df.to_dict(orient="records")
    params = [{k: r[c] for c, k in zip(cols, pkeys)} for r in recs]

    with engine.begin() as conn:
        conn.execute(text(sql), params)

    log_info(f"[UPSERT] {len(df)} rows -> {schema}.{table}")

def save_bucket_tables(engine: Engine, w: Window, df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> None:
    ensure_schema(engine, SAVE_SCHEMA)

    # day 고정 테스트라서 day 테이블만
    targets = [(DAY_T1, df1), (DAY_T2, df2), (DAY_T3, df3)]

    for table, df in targets:
        if df.empty:
            log_info(f"[SAVE] {SAVE_SCHEMA}.{table} empty -> skip")
            continue
        keys = key_cols_for(df)
        ensure_table(engine, SAVE_SCHEMA, table, list(df.columns), keys)
        upsert_df(engine, SAVE_SCHEMA, table, df, keys)

# =========================
# Bootstrap / Loop
# =========================
def bootstrap(engine: Engine, w: Window) -> Tuple[Dict[PairKey, int], Set[SeenKey], Optional[Tuple[str, str, str]]]:
    log_info(f"[BOOTSTRAP] TEST fixed window: prod_day={w.prod_day} shift={w.shift} start={w.start_dt} end={w.now_dt}")

    df_all = fetch_fail_rows(engine, w, last_pk=None)
    log_info(f"[BOOTSTRAP] fetched_rows={len(df_all)}")

    pair_counts: Dict[PairKey, int] = {}
    seen_pk: Set[SeenKey] = set()

    applied = apply_new_rows(pair_counts, seen_pk, df_all)
    log_info(f"[BOOTSTRAP] applied_rows(after_dedup)={applied}")

    last_pk = None
    if len(df_all):
        tail = df_all.iloc[-1]
        last_pk = (str(tail["end_day"]), str(tail["end_time_norm"]), str(tail["barcode_information"]))
        log_info(f"[LAST_PK] bootstrap last_pk={last_pk}")

    df1, df2, df3 = summarize_bucket_dfs(w.prod_day, w.shift, pair_counts)
    log_info(f"[BOOTSTRAP] bucket_rows: 1회={len(df1)} 2회={len(df2)} 3회+={len(df3)}")
    save_bucket_tables(engine, w, df1, df2, df3)

    return pair_counts, seen_pk, last_pk

def main() -> None:
    log_boot("backend3 fail-step repeat daemon TEST(20260128/day) starting")

    engine = make_engine()

    # DB 연결 무한 재시도
    while True:
        try:
            ensure_db_ready(engine)
            log_info(f"DB connected (work_mem={WORK_MEM})")
            break
        except Exception as e:
            log_retry(f"DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    current_window = get_window_fixed()

    # bootstrap
    while True:
        try:
            pair_counts, seen_pk, last_pk = bootstrap(engine, current_window)
            break
        except (OperationalError, DBAPIError) as e:
            log_retry(f"bootstrap DB error: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = make_engine()
        except Exception as e:
            log_retry(f"bootstrap unhandled: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    # main loop (고정 윈도우에서 증분 fetch)
    while True:
        loop_start = time_mod.time()
        try:
            log_info(f"[FETCH] last_pk={last_pk}")
            df_new = fetch_fail_rows(engine, current_window, last_pk=last_pk)
            log_info(f"[FETCH] fetched_rows={len(df_new)}")

            if len(df_new) > 0:
                applied = apply_new_rows(pair_counts, seen_pk, df_new)
                log_info(f"[FETCH] applied_rows(after_dedup)={applied}")

                tail = df_new.iloc[-1]
                last_pk = (str(tail["end_day"]), str(tail["end_time_norm"]), str(tail["barcode_information"]))
                log_info(f"[LAST_PK] updated last_pk={last_pk}")

                if applied > 0:
                    df1, df2, df3 = summarize_bucket_dfs(current_window.prod_day, current_window.shift, pair_counts)
                    log_info(f"[AGG] bucket_rows: 1회={len(df1)} 2회={len(df2)} 3회+={len(df3)}")
                    save_bucket_tables(engine, current_window, df1, df2, df3)
                else:
                    log_info("[AGG] no new unique rows -> skip upsert")
            else:
                log_info("[AGG] no new rows -> skip upsert")

        except (OperationalError, DBAPIError) as e:
            log_retry(f"DB error: {type(e).__name__}: {e}")
            while True:
                try:
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine = make_engine()
                    ensure_db_ready(engine)
                    log_info("DB reconnected")
                    # 재연결 후 안전하게 bootstrap
                    pair_counts, seen_pk, last_pk = bootstrap(engine, current_window)
                    break
                except Exception as e2:
                    log_retry(f"reconnect failed: {type(e2).__name__}: {e2}")
                    continue

        except Exception as e:
            log_warn(f"Unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_start
        time_mod.sleep(max(0.0, LOOP_INTERVAL_SEC - elapsed))

if __name__ == "__main__":
    main()
