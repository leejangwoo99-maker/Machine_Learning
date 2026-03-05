# -*- coding: utf-8 -*-
"""
e1_2_fct_runtime_ct_daemon.py  (backend3-style connection)

요구 반영 (backend3와 동일 접속 방식):
- ✅ DATA 엔진 / LOG 엔진 분리 (pool_size=1, max_overflow=0)
- ✅ work_mem은 세션에서 SET (connect_args/options 제거)
- ✅ DB 접속 실패/끊김 시 무한 재접속(블로킹)
- ✅ 루프마다 엔진 ping/재생성/OK 로그 반복 출력 금지 (끊길 때만 재연결)
- ✅ FETCH_LIMIT=2000

기능 유지:
- a2_fct_table.fct_table에서 iqz step(PD/Non-PD) run_time 수집
- LATEST/HIST/OUTLIER UPSERT
- state cursor(e1_FCT_ct.e1_2_state)로 월 전체 재스캔 방지
- health log: k_demon_heath_check.e1_2_log
"""

from __future__ import annotations

import os
import time as time_mod
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import List, Optional, Dict, Tuple

import numpy as np
import pandas as pd
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

import plotly.graph_objects as go
import plotly.io as pio


# =========================
# TZ / Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

# ✅ fetch_limit 과부하 방지
FETCH_LIMIT = 2000

# work_mem 폭증 방지 (환경변수로 조정)
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# recompute throttle
RECOMPUTE_MIN_INTERVAL_SEC = int(os.getenv("E1_2_RECOMPUTE_MIN_SEC", "15"))

# =========================
# DB Config
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": os.getenv("PGPASSWORD", ""),
}

def _make_url() -> str:
    # backend3 스타일: 옵션/파라미터 최소화
    # 비번에 특수문자가 있을 수 있으니 URL-encode
    pwd = urllib.parse.quote_plus(DB_CONFIG["password"])
    return (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{pwd}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


# =========================
# Source / Target / Log / State
# =========================
SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"

TARGET_SCHEMA = "e1_FCT_ct"
TBL_RUN_TIME_LATEST = "fct_run_time_ct"
TBL_RUN_TIME_HIST = "fct_run_time_ct_hist"
TBL_OUTLIER_LIST = "fct_upper_outlier_ct_list"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e1_2_log"

STATE_SCHEMA = TARGET_SCHEMA
STATE_TABLE = "e1_2_state"
STATE_FQN = f'"{STATE_SCHEMA}".{STATE_TABLE}'
STATE_KEY = "e1_2_fct_runtime_ct"

STATIONS_ALL = ["FCT1", "FCT2", "FCT3", "FCT4"]

# ✅ 엔진 분리
_DATA_ENGINE: Optional[Engine] = None
_LOG_ENGINE: Optional[Engine] = None


# =========================
# Logging (console + DB)
# =========================
def _ts_kst() -> datetime:
    return datetime.now(KST)

def _fmt_now() -> str:
    return _ts_kst().strftime("%Y-%m-%d %H:%M:%S")

def _emit(level_tag: str, info: str, msg: str, persist: bool = True) -> None:
    print(f"{_fmt_now()} [{level_tag}] {msg}", flush=True)
    if not persist:
        return
    global _LOG_ENGINE
    if _LOG_ENGINE is None:
        return
    try:
        now = _ts_kst()
        row = {
            "end_day": now.strftime("%Y%m%d"),
            "end_time": now.strftime("%H:%M:%S"),
            "info": (info or "info").lower(),
            "contents": str(msg)[:4000],
        }
        sql = text(f"""
            INSERT INTO "{LOG_SCHEMA}".{LOG_TABLE}
            (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """)
        with _LOG_ENGINE.begin() as conn:
            conn.execute(sql, [row])
    except Exception as e:
        # 로그 저장 실패가 본체 동작을 막지 않게 (backend3 스타일)
        print(f"{_fmt_now()} [WARN] health-log insert failed: {type(e).__name__}: {e}", flush=True)

def log_boot(msg: str) -> None:
    _emit("BOOT", "boot", msg, persist=True)

def log_info(msg: str) -> None:
    _emit("INFO", "info", msg, persist=True)

def log_retry(msg: str) -> None:
    _emit("RETRY", "down", msg, persist=True)

def log_warn(msg: str) -> None:
    _emit("WARN", "warn", msg, persist=True)

def log_error(msg: str) -> None:
    _emit("WARN", "error", msg, persist=True)

def log_sleep(msg: str) -> None:
    _emit("INFO", "sleep", msg, persist=True)


# =========================
# DB identity logger (디버그)
# =========================
def log_db_identity(engine: Engine) -> None:
    sql = """
    SELECT
      current_database()       AS db,
      current_user             AS usr,
      inet_server_addr()::text AS server_ip,
      inet_server_port()       AS server_port,
      inet_client_addr()::text AS client_ip
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()
    log_info(
        f"[DB_ID] db={row['db']} usr={row['usr']} "
        f"server={row['server_ip']}:{row['server_port']} client={row['client_ip']}"
    )


# =========================
# Engine / Session (backend3 스타일)
# =========================
def make_data_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
    return create_engine(
        _make_url(),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

def make_log_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
    return create_engine(
        _make_url(),
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

def ensure_db_ready(engine: Engine) -> None:
    # backend3처럼 세션 SET 방식
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))

def ensure_log_table(engine: Engine) -> None:
    # LOG 엔진으로만 생성/보장
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA}".{LOG_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                end_day   TEXT NOT NULL,
                end_time  TEXT NOT NULL,
                info      TEXT NOT NULL,
                contents  TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE}_day_time
            ON "{LOG_SCHEMA}".{LOG_TABLE} (end_day, end_time);
        """))

def ensure_state_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{STATE_SCHEMA}";'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {STATE_FQN} (
                key           TEXT PRIMARY KEY,
                run_month     TEXT,
                last_end_ts   TIMESTAMPTZ,
                updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))

def ensure_target_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";'))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (
                id BIGSERIAL PRIMARY KEY,
                station                TEXT NOT NULL,
                remark                 TEXT NOT NULL,
                month                  TEXT NOT NULL,
                sample_amount          INTEGER,
                run_time_lower_outlier TEXT,
                q1                     DOUBLE PRECISION,
                median                 DOUBLE PRECISION,
                q3                     DOUBLE PRECISION,
                run_time_upper_outlier TEXT,
                del_out_run_time_av    DOUBLE PRECISION,
                plotly_json            JSONB,
                created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_RUN_TIME_LATEST}_key
            ON "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (station, remark, month);
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (
                id BIGSERIAL PRIMARY KEY,
                snapshot_day           TEXT NOT NULL,
                snapshot_ts            TIMESTAMPTZ NOT NULL DEFAULT now(),
                station                TEXT NOT NULL,
                remark                 TEXT NOT NULL,
                month                  TEXT NOT NULL,
                sample_amount          INTEGER,
                run_time_lower_outlier TEXT,
                q1                     DOUBLE PRECISION,
                median                 DOUBLE PRECISION,
                q3                     DOUBLE PRECISION,
                run_time_upper_outlier TEXT,
                del_out_run_time_av    DOUBLE PRECISION,
                plotly_json            JSONB
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_RUN_TIME_HIST}_day_key
            ON "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (snapshot_day, station, remark, month);
        """))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (
                id BIGSERIAL PRIMARY KEY,
                station             TEXT NOT NULL,
                remark              TEXT NOT NULL,
                barcode_information TEXT NOT NULL,
                end_day             TEXT NOT NULL,
                end_time            TEXT NOT NULL,
                run_time            DOUBLE PRECISION,
                month               TEXT,
                upper_threshold     DOUBLE PRECISION,
                created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_OUTLIER_LIST}_key
            ON "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (station, remark, barcode_information, end_day, end_time);
        """))


# =========================
# State
# =========================
def current_yyyymm(now: Optional[datetime] = None) -> str:
    now = now or _ts_kst()
    return now.strftime("%Y%m")

def today_yyyymmdd() -> str:
    return _ts_kst().strftime("%Y%m%d")

def month_start_ts(run_month: str) -> datetime:
    y = int(run_month[:4])
    m = int(run_month[4:6])
    return datetime(y, m, 1, 0, 0, 0, tzinfo=KST)

def load_state(engine: Engine) -> Tuple[Optional[str], Optional[datetime]]:
    sql = text(f"SELECT run_month, last_end_ts FROM {STATE_FQN} WHERE key=:k")
    with engine.connect() as conn:
        row = conn.execute(sql, {"k": STATE_KEY}).mappings().first()
    if not row:
        return None, None
    return (row["run_month"], row["last_end_ts"])

def save_state(engine: Engine, run_month: str, last_end_ts: Optional[datetime]) -> None:
    sql = text(f"""
        INSERT INTO {STATE_FQN} (key, run_month, last_end_ts)
        VALUES (:k, :m, :ts)
        ON CONFLICT (key) DO UPDATE SET
          run_month   = EXCLUDED.run_month,
          last_end_ts = EXCLUDED.last_end_ts,
          updated_at  = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql, {"k": STATE_KEY, "m": run_month, "ts": last_end_ts})


# =========================
# Fetch
# =========================
def fetch_incremental(engine: Engine, stations: List[str], run_month: str, cursor_ts: datetime) -> pd.DataFrame:
    q = text(f"""
    SELECT
        file_path,
        station,
        remark,
        barcode_information,
        step_description,
        result,
        regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') AS end_day,
        end_time,
        run_time,
        to_timestamp(
            regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g')
            || ' ' || split_part(end_time::text, '.', 1),
            'YYYYMMDD HH24:MI:SS'
        ) AS end_ts
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE
        station = ANY(:stations)
        AND barcode_information LIKE 'B%%'
        AND result <> 'FAIL'
        AND (
            (remark = 'PD' AND step_description = '1.36 Test iqz(uA)')
            OR
            (remark = 'Non-PD' AND step_description = '1.32 Test iqz(uA)')
        )
        AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
        AND to_timestamp(
            regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g')
            || ' ' || split_part(end_time::text, '.', 1),
            'YYYYMMDD HH24:MI:SS'
        ) >= :cursor_ts
    ORDER BY end_ts ASC, file_path ASC
    LIMIT :limit
    """)
    with engine.connect() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
        df = pd.read_sql_query(q, conn, params={
            "stations": stations,
            "run_month": run_month,
            "cursor_ts": cursor_ts,
            "limit": FETCH_LIMIT,
        })

    if df is None or df.empty:
        return pd.DataFrame()

    df["file_path"] = df["file_path"].astype(str).str.strip()
    df = df[df["file_path"].notna() & (df["file_path"] != "")].copy()
    df["end_day"] = df["end_day"].astype(str).str.zfill(8)
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    df = df.dropna(subset=["run_time", "end_ts"]).reset_index(drop=True)
    df["month"] = df["end_day"].str.slice(0, 6)
    return df


# =========================
# Analysis
# =========================
def _round2(x):
    if x is None:
        return None
    try:
        if np.isnan(x):
            return None
    except Exception:
        pass
    return round(float(x), 2)

def _outlier_range_str(outlier_values: np.ndarray):
    if outlier_values.size == 0:
        return None
    mn = round(float(np.min(outlier_values)), 2)
    mx = round(float(np.max(outlier_values)), 2)
    return f"{mn}~{mx}"

def _make_plotly_box_json(values: np.ndarray, name: str):
    fig = go.Figure(data=[go.Box(y=values.tolist(), name=name, boxpoints=False)])
    return pio.to_json(fig, validate=False)

def boxplot_summary_from_values(vals: np.ndarray, name: str) -> dict:
    n = int(vals.size)
    q1 = float(np.percentile(vals, 25))
    median = float(np.percentile(vals, 50))
    q3 = float(np.percentile(vals, 75))
    iqr = q3 - q1

    lower_th = q1 - 1.5 * iqr
    upper_th = q3 + 1.5 * iqr

    lower_outliers = vals[vals < lower_th]
    upper_outliers = vals[vals > upper_th]

    inliers = vals[(vals >= lower_th) & (vals <= upper_th)]
    del_out_mean = float(np.mean(inliers)) if inliers.size > 0 else None

    return {
        "sample_amount": n,
        "run_time_lower_outlier": _outlier_range_str(lower_outliers),
        "q1": _round2(q1),
        "median": _round2(median),
        "q3": _round2(q3),
        "run_time_upper_outlier": _outlier_range_str(upper_outliers),
        "del_out_run_time_av": _round2(del_out_mean),
        "plotly_json": _make_plotly_box_json(vals, name=name),
    }

def build_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "station", "remark", "month", "sample_amount",
            "run_time_lower_outlier", "q1", "median", "q3",
            "run_time_upper_outlier", "del_out_run_time_av", "plotly_json"
        ])

    rows = []
    for (station, remark, month), g in df.groupby(["station", "remark", "month"], sort=True):
        vals = g["run_time"].astype(float).to_numpy()
        d = boxplot_summary_from_values(vals, name=f"{station}_{remark}_{month}")
        d.update({"station": station, "remark": remark, "month": month})
        rows.append(d)

    return pd.DataFrame(rows).sort_values(["station", "remark", "month"]).reset_index(drop=True)

def build_upper_outlier_df(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty or summary_df is None or summary_df.empty:
        return pd.DataFrame()

    upper_rows = []
    for _, row in summary_df.iterrows():
        station = row["station"]
        remark = row["remark"]
        month = row["month"]

        g = df_raw[(df_raw["station"] == station) & (df_raw["remark"] == remark) & (df_raw["month"] == month)].copy()
        if g.empty:
            continue

        vals = g["run_time"].astype(float).to_numpy()
        q1 = np.percentile(vals, 25)
        q3 = np.percentile(vals, 75)
        iqr = q3 - q1
        upper_th = q3 + 1.5 * iqr

        out_df = g[g["run_time"] > upper_th].copy()
        if out_df.empty:
            continue

        out_df["upper_threshold"] = round(float(upper_th), 2)
        upper_rows.append(out_df)

    if not upper_rows:
        return pd.DataFrame()

    out = pd.concat(upper_rows, ignore_index=True)
    out = out[["station", "remark", "barcode_information", "end_day", "end_time", "run_time", "month", "upper_threshold"]]
    return out.sort_values(["station", "remark", "month", "end_day", "end_time"]).reset_index(drop=True)


# =========================
# DB Save
# =========================
def upsert_latest(engine: Engine, summary_df: pd.DataFrame) -> None:
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None)
    rows = df.to_dict("records")

    sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json,
        created_at, updated_at
    )
    VALUES (
        :station, :remark, :month,
        :sample_amount,
        :run_time_lower_outlier,
        :q1, :median, :q3,
        :run_time_upper_outlier,
        :del_out_run_time_av,
        CAST(:plotly_json AS jsonb),
        now(), now()
    )
    ON CONFLICT (station, remark, month)
    DO UPDATE SET
        sample_amount          = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1                     = EXCLUDED.q1,
        median                 = EXCLUDED.median,
        q3                     = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av    = EXCLUDED.del_out_run_time_av,
        plotly_json            = EXCLUDED.plotly_json,
        updated_at             = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

def upsert_hist(engine: Engine, summary_df: pd.DataFrame, snapshot_day: str) -> None:
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    df.insert(0, "snapshot_day", snapshot_day)
    rows = df.to_dict("records")

    sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (
        snapshot_day, snapshot_ts,
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json
    )
    VALUES (
        :snapshot_day, now(),
        :station, :remark, :month,
        :sample_amount,
        :run_time_lower_outlier,
        :q1, :median, :q3,
        :run_time_upper_outlier,
        :del_out_run_time_av,
        CAST(:plotly_json AS jsonb)
    )
    ON CONFLICT (snapshot_day, station, remark, month)
    DO UPDATE SET
        snapshot_ts            = now(),
        sample_amount          = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1                     = EXCLUDED.q1,
        median                 = EXCLUDED.median,
        q3                     = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av    = EXCLUDED.del_out_run_time_av,
        plotly_json            = EXCLUDED.plotly_json
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)

def insert_outliers(engine: Engine, outlier_df: pd.DataFrame) -> None:
    if outlier_df is None or outlier_df.empty:
        return

    df = outlier_df.where(pd.notnull(outlier_df), None)
    rows = df.to_dict("records")

    sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (
        station, remark, barcode_information, end_day, end_time,
        run_time, month, upper_threshold
    )
    VALUES (
        :station, :remark, :barcode_information, :end_day, :end_time,
        :run_time, :month, :upper_threshold
    )
    ON CONFLICT (station, remark, barcode_information, end_day, end_time)
    DO NOTHING
    """)
    with engine.begin() as conn:
        conn.execute(sql, rows)


# =========================
# Main
# =========================
def connect_blocking() -> Tuple[Engine, Engine]:
    """
    backend3 스타일:
    - DATA/LOG 엔진 생성
    - work_mem SET
    - LOG 테이블 보장
    - 실패 시 5초마다 무한 재시도
    """
    global _DATA_ENGINE, _LOG_ENGINE

    data_engine = make_data_engine()
    log_engine = make_log_engine()

    while True:
        try:
            ensure_db_ready(data_engine)
            ensure_db_ready(log_engine)
            ensure_log_table(log_engine)

            _DATA_ENGINE = data_engine
            _LOG_ENGINE = log_engine

            log_info(f"DB connected (work_mem={WORK_MEM})")
            log_db_identity(data_engine)
            log_info(f'health log ready -> "{LOG_SCHEMA}".{LOG_TABLE}')
            return data_engine, log_engine
        except Exception as e:
            _DATA_ENGINE = None
            _LOG_ENGINE = None
            log_retry(f"DB connect failed: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            # 엔진 재생성 (풀 꼬임 방지)
            data_engine = make_data_engine()
            log_engine = make_log_engine()


def main() -> None:
    log_boot("e1_2 fct_runtime_ct daemon starting (backend3-style connect)")

    data_engine, _ = connect_blocking()

    # DDL ensure (DATA 엔진)
    while True:
        try:
            ensure_state_table(data_engine)
            ensure_target_tables(data_engine)
            log_info(f'state table ready: {STATE_FQN}')
            log_info(f'target tables ready: "{TARGET_SCHEMA}".*')
            break
        except (OperationalError, DBAPIError) as e:
            log_retry(f"DDL DB error: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            data_engine, _ = connect_blocking()

    run_month = current_yyyymm()
    st_month, st_ts = (None, None)
    try:
        st_month, st_ts = load_state(data_engine)
    except Exception:
        pass

    if st_month != run_month:
        cursor_ts = month_start_ts(run_month)
        try:
            save_state(data_engine, run_month, cursor_ts)
        except Exception:
            pass
        log_info(f"[STATE] init cursor for {run_month}: {cursor_ts}")
    else:
        cursor_ts = st_ts or month_start_ts(run_month)
        log_info(f"[STATE] loaded cursor: month={st_month} ts={cursor_ts}")

    cache_df: Optional[pd.DataFrame] = None
    last_recompute_ts = 0.0

    while True:
        loop_start = time_mod.time()
        try:
            now = _ts_kst()
            cur_month = current_yyyymm(now)
            if cur_month != run_month:
                log_info(f"[ROLLOVER] month {run_month} -> {cur_month} | reset cache/state")
                run_month = cur_month
                cache_df = None
                cursor_ts = month_start_ts(run_month)
                save_state(data_engine, run_month, cursor_ts)

            df_new = fetch_incremental(data_engine, STATIONS_ALL, run_month, cursor_ts)

            if df_new is None or df_new.empty:
                log_info("no new rows")
            else:
                if cache_df is None or cache_df.empty:
                    cache_df = df_new.copy()
                else:
                    cache_df = pd.concat([cache_df, df_new], ignore_index=True)
                    cache_df = cache_df.drop_duplicates(subset=["file_path"], keep="last").reset_index(drop=True)

                max_ts = pd.to_datetime(df_new["end_ts"]).max()
                if pd.notna(max_ts):
                    cursor_ts = (max_ts.to_pydatetime() - timedelta(seconds=1))
                    save_state(data_engine, run_month, cursor_ts)

                log_info(f"new={len(df_new)} cache={len(cache_df)} cursor={cursor_ts}")

                now_ts = time_mod.time()
                if (now_ts - last_recompute_ts) >= RECOMPUTE_MIN_INTERVAL_SEC:
                    snapshot_day = today_yyyymmdd()
                    summary_df = build_summary_df(cache_df)
                    outlier_df = build_upper_outlier_df(cache_df, summary_df)

                    upsert_latest(data_engine, summary_df)
                    upsert_hist(data_engine, summary_df, snapshot_day=snapshot_day)
                    insert_outliers(data_engine, outlier_df)

                    last_recompute_ts = now_ts
                    log_info(f"[UPSERT] done | groups={len(summary_df)} outliers={len(outlier_df)}")
                else:
                    log_info(f"[SKIP] recompute throttled ({RECOMPUTE_MIN_INTERVAL_SEC}s)")

        except (OperationalError, DBAPIError) as e:
            log_retry(f"DB error: {type(e).__name__}: {e}")

            # 재연결 후 이어서 진행 (backend3 스타일)
            while True:
                try:
                    log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    data_engine, _ = connect_blocking()

                    # 끊겼던 사이 스키마/테이블 보장
                    ensure_state_table(data_engine)
                    ensure_target_tables(data_engine)

                    log_info("DB reconnected")
                    log_db_identity(data_engine)
                    break
                except Exception as e2:
                    log_retry(f"reconnect failed: {type(e2).__name__}: {e2}")
                    continue

        except Exception as e:
            log_error(f"Unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            log_sleep(f"loop sleep {sleep_sec:.2f}s")
            time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    main()