# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인 - current-month incremental + periodic UPSERT
- MP=1 고정
- LATEST/HIST 저장
- id/sequence 자동 보정
- 테이블 스키마 자동 동기화(ALTER TABLE ... ADD COLUMN IF NOT EXISTS)

추가:
- ✅ 로그파일 TEE (콘솔+파일 동시 기록)
- ✅ "서비스 상주" 모드:
  * 프로세스 종료하지 않고 계속 실행
  * 매일 08:22 / 20:22 각각 하루 1회 실행
  * 슬롯 실행 완료 후 다음 슬롯까지 대기(하트비트 출력)
"""

import os
import sys
import warnings
import urllib.parse
from datetime import datetime, date, time as dtime, timedelta
import time as time_mod
import atexit
import traceback

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError

import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0-0) LOG TEE (콘솔 + 파일)
# =========================
def _safe_makedirs(path: str):
    try:
        os.makedirs(path, exist_ok=True)
    except Exception:
        pass

def _default_log_path() -> str:
    p = (os.getenv("VISION_OPCT_LOG") or "").strip()
    if p:
        return p
    if os.name == "nt":
        return r"C:\AptivAgent\logs\vision_opct.log"
    return os.path.join(os.getcwd(), "logs", "vision_opct.log")

LOG_PATH = _default_log_path()
_safe_makedirs(os.path.dirname(LOG_PATH) or ".")

class _TeeStream:
    def __init__(self, path: str, stream):
        self.path = path
        self.stream = stream
        self.f = open(path, "a", encoding="utf-8", buffering=1)

    def write(self, s):
        try:
            self.stream.write(s)
        except Exception:
            pass
        try:
            self.f.write(s)
        except Exception:
            pass

    def flush(self):
        try:
            self.stream.flush()
        except Exception:
            pass
        try:
            self.f.flush()
        except Exception:
            pass

    def close(self):
        try:
            self.f.close()
        except Exception:
            pass

try:
    sys.stdout = _TeeStream(LOG_PATH, sys.__stdout__)
    sys.stderr = _TeeStream(LOG_PATH, sys.__stderr__)
except Exception:
    pass

def _log_banner(kind: str):
    try:
        print(
            f"\n[{kind}] {datetime.now():%Y-%m-%d %H:%M:%S} | pid={os.getpid()} | log={LOG_PATH}",
            flush=True,
        )
    except Exception:
        pass

@atexit.register
def _on_exit():
    _log_banner("EXIT")

def _excepthook(exc_type, exc_value, exc_tb):
    try:
        _log_banner("UNHANDLED_EXCEPTION")
        traceback.print_exception(exc_type, exc_value, exc_tb)
    finally:
        try:
            sys.__excepthook__(exc_type, exc_value, exc_tb)
        except Exception:
            pass

try:
    sys.excepthook = _excepthook
except Exception:
    pass

_log_banner("BOOT")


# =========================
# Thread 제한
# =========================
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

warnings.filterwarnings(
    "ignore",
    message=r"KMeans is known to have a memory leak on Windows with MKL.*"
)

# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e2_vision_ct"
TBL_LATEST = "vision_op_ct"
TBL_HIST   = "vision_op_ct_hist"

OPCT_MAX_SEC = 600
ONLY_RUN_MIN_LEN = 10

STATIONS_ALL = ["Vision1", "Vision2"]

# 스케줄 실행 시간
RUN_TIME_1 = dtime(8, 22, 0)
RUN_TIME_2 = dtime(20, 22, 0)

# 상주 루프 하트비트/슬립
IDLE_HEARTBEAT = True
HEARTBEAT_EVERY_SEC = int(os.getenv("HEARTBEAT_EVERY_SEC", "60"))  # 60초마다 하트비트

TIMING_LOG = True

DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = 5
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")

def current_yyyymm(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now()
    return now.strftime("%Y%m")

def _is_conn_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError)):
        return True
    try:
        if isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            return True
    except Exception:
        pass

    msg = (str(e) or "").lower()
    keys = [
        "server closed the connection",
        "terminating connection",
        "connection not open",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
        "connection already closed",
    ]
    return any(k in msg for k in keys)

def _slot_dt_for(day: date, t: dtime) -> datetime:
    return datetime(day.year, day.month, day.day, t.hour, t.minute, t.second)

def _next_slot_target(now: datetime, ran_1_day: date | None, ran_2_day: date | None) -> datetime:
    """
    다음 실행 타겟 시간 반환.
    - 오늘 RUN_TIME_1/2 중 아직 안 한 것 우선
    - 오늘 다 했으면 내일 RUN_TIME_1
    """
    today = now.date()
    t1 = _slot_dt_for(today, RUN_TIME_1)
    t2 = _slot_dt_for(today, RUN_TIME_2)

    candidates = []
    if ran_1_day != today:
        candidates.append(t1)
    if ran_2_day != today:
        candidates.append(t2)

    # 후보가 없으면 내일 08:22
    if not candidates:
        tomorrow = today + timedelta(days=1)
        return _slot_dt_for(tomorrow, RUN_TIME_1)

    # 아직 안한 것 중 "가장 가까운 미래"를 우선
    # 단, 이미 시간이 지난 슬롯이라도 아직 실행 안 했으면 즉시 실행되도록 now 이하도 허용
    return min(candidates)

def _sleep_with_heartbeat_until(target_dt: datetime, ran_1: bool, ran_2: bool):
    """
    target_dt까지 슬립. HEARTBEAT_EVERY_SEC마다 상태 출력.
    """
    last_hb = 0.0
    while True:
        now = datetime.now()
        if now >= target_dt:
            return
        now_ts = time_mod.time()
        if IDLE_HEARTBEAT and (now_ts - last_hb >= HEARTBEAT_EVERY_SEC):
            last_hb = now_ts
            log(f"[WAIT] now={now:%Y-%m-%d %H:%M:%S} | next={target_dt:%Y-%m-%d %H:%M:%S} | ran_1={ran_1} ran_2={ran_2}")
        # 너무 길게 안 자고, 깨어나서 시간 체크 반복
        remaining = (target_dt - now).total_seconds()
        time_mod.sleep(min(max(remaining, 1.0), 10.0))


# =========================
# 1-1) DB: 상시 연결 1개 + 무한 재시도 + work_mem 제한
# =========================
_ENGINE = None
_PG_CONN = None

def _build_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        f"?connect_timeout={CONNECT_TIMEOUT_SEC}"
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args={
            "connect_timeout": CONNECT_TIMEOUT_SEC,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "vision_opct_service",
        },
    )

def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None

def get_engine_blocking():
    global _ENGINE
    while True:
        try:
            if _ENGINE is None:
                _ENGINE = _build_engine(DB_CONFIG)
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            return _ENGINE
        except Exception as e:
            log(f"[DB][RETRY] engine connect failed: {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

def _pg_conn_is_alive(conn) -> bool:
    try:
        if conn is None:
            return False
        if getattr(conn, "closed", 1) != 0:
            return False
        with conn.cursor() as cur:
            cur.execute("SELECT 1;")
        return True
    except Exception:
        return False

def get_conn_pg_blocking():
    global _PG_CONN
    while True:
        try:
            if not _pg_conn_is_alive(_PG_CONN):
                try:
                    if _PG_CONN is not None:
                        _PG_CONN.close()
                except Exception:
                    pass

                _PG_CONN = psycopg2.connect(
                    host=DB_CONFIG["host"],
                    port=DB_CONFIG["port"],
                    dbname=DB_CONFIG["dbname"],
                    user=DB_CONFIG["user"],
                    password=DB_CONFIG["password"],
                    connect_timeout=CONNECT_TIMEOUT_SEC,
                    keepalives=PG_KEEPALIVES,
                    keepalives_idle=PG_KEEPALIVES_IDLE,
                    keepalives_interval=PG_KEEPALIVES_INTERVAL,
                    keepalives_count=PG_KEEPALIVES_COUNT,
                    application_name="vision_opct_service",
                )
                _PG_CONN.autocommit = False
                with _PG_CONN.cursor() as cur:
                    cur.execute("SET work_mem TO %s;", (WORK_MEM,))
                _PG_CONN.commit()
            return _PG_CONN
        except Exception as e:
            log(f"[DB][RETRY] psycopg2 connect failed: {type(e).__name__}: {repr(e)}")
            try:
                if _PG_CONN is not None:
                    _PG_CONN.close()
            except Exception:
                pass
            _PG_CONN = None
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

def close_db():
    global _ENGINE, _PG_CONN
    try:
        if _PG_CONN is not None and getattr(_PG_CONN, "closed", 1) == 0:
            try:
                _PG_CONN.commit()
            except Exception:
                pass
            _PG_CONN.close()
    except Exception:
        pass
    _PG_CONN = None
    _dispose_engine()


# =========================
# 2) 테이블/인덱스/ID 보정 + 스키마 동기화 (무한 재시도)
# =========================
def ensure_tables_and_indexes():
    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_LATEST} (
                        id BIGINT,
                        station              TEXT NOT NULL,
                        remark               TEXT NOT NULL,
                        month                TEXT NOT NULL,
                        sample_amount        INTEGER,
                        op_ct_lower_outlier  TEXT,
                        q1                   DOUBLE PRECISION,
                        median               DOUBLE PRECISION,
                        q3                   DOUBLE PRECISION,
                        op_ct_upper_outlier  TEXT,
                        del_out_op_ct_av     DOUBLE PRECISION,
                        plotly_json          JSONB,
                        created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
                        updated_at           TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                """)
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_HIST} (
                        id BIGINT,
                        snapshot_day         TEXT NOT NULL,
                        snapshot_ts          TIMESTAMPTZ NOT NULL DEFAULT now(),
                        station              TEXT NOT NULL,
                        remark               TEXT NOT NULL,
                        month                TEXT NOT NULL,
                        sample_amount        INTEGER,
                        op_ct_lower_outlier  TEXT,
                        q1                   DOUBLE PRECISION,
                        median               DOUBLE PRECISION,
                        q3                   DOUBLE PRECISION,
                        op_ct_upper_outlier  TEXT,
                        del_out_op_ct_av     DOUBLE PRECISION,
                        plotly_json          JSONB
                    );
                """)

                # 컬럼 동기화
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;""")
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;""")
                cur.execute(f"""
                    UPDATE "{TARGET_SCHEMA}".{TBL_LATEST}
                    SET created_at = COALESCE(created_at, now()),
                        updated_at = COALESCE(updated_at, now())
                    WHERE created_at IS NULL OR updated_at IS NULL;
                """)
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ALTER COLUMN created_at SET DEFAULT now();""")
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ALTER COLUMN updated_at SET DEFAULT now();""")
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ALTER COLUMN created_at SET NOT NULL;""")
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ALTER COLUMN updated_at SET NOT NULL;""")

                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS snapshot_ts TIMESTAMPTZ;""")
                cur.execute(f"""
                    UPDATE "{TARGET_SCHEMA}".{TBL_HIST}
                    SET snapshot_ts = COALESCE(snapshot_ts, now())
                    WHERE snapshot_ts IS NULL;
                """)
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ALTER COLUMN snapshot_ts SET DEFAULT now();""")
                cur.execute(f"""ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ALTER COLUMN snapshot_ts SET NOT NULL;""")

                cur.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_LATEST}_key
                    ON "{TARGET_SCHEMA}".{TBL_LATEST} (station, remark, month);
                """)
                cur.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_HIST}_day_key
                    ON "{TARGET_SCHEMA}".{TBL_HIST} (snapshot_day, station, remark, month);
                """)

            conn.commit()
            break

        except Exception as e:
            log(f"[DB][RETRY] ensure_tables_and_indexes failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    fix_id_sequence(TARGET_SCHEMA, TBL_LATEST, "vision_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_HIST,   "vision_op_ct_hist_id_seq")

    log(f'[OK] target tables ensured in schema "{TARGET_SCHEMA}"')

def fix_id_sequence(schema: str, table: str, seq_name: str):
    do_sql = f"""
    DO $$
    DECLARE
      v_schema TEXT := {schema!r};
      v_table  TEXT := {table!r};
      v_seq    TEXT := {seq_name!r};
      v_full_table TEXT := quote_ident(v_schema) || '.' || quote_ident(v_table);
      v_full_seq   TEXT := quote_ident(v_schema) || '.' || quote_ident(v_seq);
      v_max BIGINT;
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = v_schema
          AND table_name = v_table
          AND column_name = 'id'
      ) THEN
        EXECUTE 'ALTER TABLE ' || v_full_table || ' ADD COLUMN id BIGINT';
      END IF;

      IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname = v_schema
          AND c.relname = v_seq
      ) THEN
        EXECUTE 'CREATE SEQUENCE ' || v_full_seq;
      END IF;

      EXECUTE 'ALTER TABLE ' || v_full_table ||
              ' ALTER COLUMN id SET DEFAULT nextval(''' || v_full_seq || ''')';

      EXECUTE 'ALTER SEQUENCE ' || v_full_seq ||
              ' OWNED BY ' || v_full_table || '.id';

      EXECUTE 'UPDATE ' || v_full_table || ' SET id = nextval(''' || v_full_seq || ''') WHERE id IS NULL';

      EXECUTE 'SELECT COALESCE(MAX(id), 0) FROM ' || v_full_table INTO v_max;
      EXECUTE 'SELECT setval(''' || v_full_seq || ''', ' || (v_max + 1) || ', false)';
    END $$;
    """
    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                cur.execute(do_sql)
            conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] fix_id_sequence({schema}.{table}) failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 3) 통계/plotly 유틸
# =========================
def boxplot_stats(values: np.ndarray) -> dict:
    values = values.astype(float)
    q1 = float(np.percentile(values, 25))
    med = float(np.percentile(values, 50))
    q3 = float(np.percentile(values, 75))
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    inlier = values[(values >= lower) & (values <= upper)]
    del_out_mean = float(np.mean(inlier)) if len(inlier) else np.nan
    return {
        "q1": q1,
        "median": med,
        "q3": q3,
        "lower": float(lower),
        "upper": float(upper),
        "del_out_mean": del_out_mean,
        "sample_amount": int(len(values)),
    }

def make_plotly_box_json(values: np.ndarray, title: str) -> str:
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), boxpoints=False, name=title))
    fig.update_layout(title=title, showlegend=False)
    return fig.to_json(validate=False)

def fmt_range(a: float, b: float) -> str:
    return f"{a:.2f}~{b:.2f}"


# =========================
# 4) only_run 판정/분석/summarize
# =========================
def mark_only_runs(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out is None or out.empty:
        out = pd.DataFrame(columns=list(df.columns) if df is not None else [])
        out["run_id"] = []
        out["run_len"] = []
        out["is_vision_only_run"] = []
        return out

    out = out.sort_values(["end_dt"], kind="mergesort").reset_index(drop=True)
    out["run_id"] = (out["station"] != out["station"].shift(1)).cumsum()
    run_sizes = out.groupby("run_id")["station"].size().rename("run_len")
    out = out.merge(run_sizes, on="run_id", how="left")

    is_vision_station = out["station"].isin(["Vision1", "Vision2"])
    out["is_vision_only_run"] = is_vision_station & (out["run_len"] >= ONLY_RUN_MIN_LEN)
    return out

def build_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.sort_values(["station", "remark", "end_dt"], kind="mergesort").copy()
    d["op_ct"] = d.groupby(["station", "remark"])["end_dt"].diff().dt.total_seconds()

    df_an = d.dropna(subset=["op_ct"]).copy()
    df_an = df_an[df_an["op_ct"] <= OPCT_MAX_SEC].copy()
    return df_an

def summarize(df_an: pd.DataFrame) -> pd.DataFrame:
    if df_an is None or df_an.empty:
        return pd.DataFrame()

    df_normal = df_an[df_an["is_vision_only_run"] == False].copy()
    df_only   = df_an[df_an["is_vision_only_run"] == True].copy()

    tasks: list[tuple[str, str, str, np.ndarray]] = []

    if not df_normal.empty:
        dn = df_normal.copy()
        dn["station_out"] = dn["station"]
        for (st, rk, mo), g in dn.groupby(["station_out", "remark", "month"], sort=True):
            vals = g["op_ct"].dropna().astype(float).to_numpy()
            if vals.size:
                tasks.append((st, rk, str(mo), vals))

    if not df_only.empty:
        do = df_only.copy()
        do["station_out"] = do["station"]
        mask_v = do["station"].isin(["Vision1", "Vision2"])
        do.loc[mask_v, "station_out"] = do.loc[mask_v, "station"].map({
            "Vision1": "Vision1_only",
            "Vision2": "Vision2_only",
        })

        for (st, rk, mo), g in do.groupby(["station_out", "remark", "month"], sort=True):
            vals = g["op_ct"].dropna().astype(float).to_numpy()
            if vals.size:
                tasks.append((st, rk, str(mo), vals))

    if not tasks:
        return pd.DataFrame()

    rows = []
    for st, rk, mo, vals in tasks:
        stats = boxplot_stats(vals)
        plotly_json = make_plotly_box_json(vals, title=f"{st}-{rk}-{mo}")

        q1 = round(stats["q1"], 2)
        med = round(stats["median"], 2)
        q3 = round(stats["q3"], 2)
        lower = round(stats["lower"], 2)
        upper = round(stats["upper"], 2)
        del_out_mean = round(stats["del_out_mean"], 2) if not np.isnan(stats["del_out_mean"]) else np.nan

        rows.append({
            "station": st,
            "remark": rk,
            "month": str(mo),
            "sample_amount": stats["sample_amount"],
            "op_ct_lower_outlier": fmt_range(lower, q1),
            "q1": q1,
            "median": med,
            "q3": q3,
            "op_ct_upper_outlier": fmt_range(q3, upper),
            "del_out_op_ct_av": del_out_mean,
            "plotly_json": plotly_json,
        })

    return (
        pd.DataFrame(rows)
        .sort_values(["remark", "station", "month"])
        .reset_index(drop=True)
    )


# =========================
# 5) 저장 (끊김 감지 + 무한 재시도)
# =========================
def _execute_values_retry(sql_text: str, rows: list, template: str, page_size: int = 2000):
    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                if rows:
                    execute_values(cur, sql_text, rows, template=template, page_size=page_size)
            conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] execute_values failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            if _is_conn_error(e):
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                global _PG_CONN
                _PG_CONN = None
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

def upsert_latest(summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    cols = [
        "station", "remark", "month",
        "sample_amount",
        "op_ct_lower_outlier",
        "q1", "median", "q3",
        "op_ct_upper_outlier",
        "del_out_op_ct_av",
        "plotly_json",
    ]
    df = df[cols]
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_LATEST} (
        station, remark, month,
        sample_amount,
        op_ct_lower_outlier,
        q1, median, q3,
        op_ct_upper_outlier,
        del_out_op_ct_av,
        plotly_json,
        created_at, updated_at
    )
    VALUES %s
    ON CONFLICT (station, remark, month)
    DO UPDATE SET
        sample_amount       = EXCLUDED.sample_amount,
        op_ct_lower_outlier = EXCLUDED.op_ct_lower_outlier,
        q1                  = EXCLUDED.q1,
        median              = EXCLUDED.median,
        q3                  = EXCLUDED.q3,
        op_ct_upper_outlier = EXCLUDED.op_ct_upper_outlier,
        del_out_op_ct_av    = EXCLUDED.del_out_op_ct_av,
        plotly_json         = EXCLUDED.plotly_json,
        updated_at          = now();
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now(), now())"
    _execute_values_retry(insert_sql, rows, template=template, page_size=2000)

def upsert_hist_daily(summary_df: pd.DataFrame, snapshot_day: str):
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    cols = [
        "station", "remark", "month",
        "sample_amount",
        "op_ct_lower_outlier",
        "q1", "median", "q3",
        "op_ct_upper_outlier",
        "del_out_op_ct_av",
        "plotly_json",
    ]
    df = df[cols]
    df.insert(0, "snapshot_day", snapshot_day)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_HIST} (
        snapshot_day, snapshot_ts,
        station, remark, month,
        sample_amount,
        op_ct_lower_outlier,
        q1, median, q3,
        op_ct_upper_outlier,
        del_out_op_ct_av,
        plotly_json
    )
    VALUES %s
    ON CONFLICT (snapshot_day, station, remark, month)
    DO UPDATE SET
        snapshot_ts         = now(),
        sample_amount       = EXCLUDED.sample_amount,
        op_ct_lower_outlier = EXCLUDED.op_ct_lower_outlier,
        q1                  = EXCLUDED.q1,
        median              = EXCLUDED.median,
        q3                  = EXCLUDED.q3,
        op_ct_upper_outlier = EXCLUDED.op_ct_upper_outlier,
        del_out_op_ct_av    = EXCLUDED.del_out_op_ct_av,
        plotly_json         = EXCLUDED.plotly_json;
    """
    template = "(%s, now(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)"
    _execute_values_retry(insert_sql, rows, template=template, page_size=2000)


# =========================
# 6) 1회 실행 파이프라인
# =========================
def run_pipeline_once(label: str):
    log(f"[RUN] {label} | START")

    eng = get_engine_blocking()
    get_conn_pg_blocking()

    run_month = current_yyyymm()
    stations = STATIONS_ALL

    sql_query = f"""
    SELECT station, remark, end_day, end_time, result, goodorbad
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE station = ANY(:stations)
      AND remark IN ('PD','Non-PD')
      AND COALESCE(result,'') <> 'FAIL'
      AND COALESCE(goodorbad,'') <> 'BadFile'
      AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
    ORDER BY end_day ASC, end_time ASC
    """

    t0 = time_mod.time()
    while True:
        try:
            with eng.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                df = pd.read_sql(
                    text(sql_query),
                    conn,
                    params={"stations": stations, "run_month": run_month},
                )
            break
        except Exception as e:
            log(f"[DB][RETRY] fetch failed: {type(e).__name__}: {repr(e)}")
            if _is_conn_error(e):
                _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            eng = get_engine_blocking()

    t_fetch = time_mod.time() - t0
    if TIMING_LOG:
        log(f"[T] fetch_sec={t_fetch:.2f} | rows={0 if df is None else len(df)}")

    if df is None or df.empty:
        log(f"[RUN] {label} | no rows in month={run_month} -> SKIP")
        return

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    df["end_time_str"] = df["end_time"].astype(str).str.strip()
    df["end_ts"] = pd.to_datetime(df["end_day"] + " " + df["end_time_str"], errors="coerce", format="mixed")
    df = df.dropna(subset=["end_ts"]).copy()
    if df.empty:
        log(f"[RUN] {label} | all rows dropped by end_ts parse -> SKIP")
        return

    t1 = time_mod.time()
    cache_df = df[["station", "remark", "end_day", "end_time_str", "end_ts"]].copy()
    cache_df = cache_df.sort_values(["station", "remark", "end_ts"], kind="mergesort").reset_index(drop=True)
    cache_df["op_ct"] = cache_df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    cache_df["month"] = cache_df["end_ts"].dt.strftime("%Y%m")

    df_for_analysis = cache_df.rename(columns={"end_ts": "end_dt"}).copy()

    df_marked = mark_only_runs(df_for_analysis)
    df_an = build_analysis_df(df_marked)
    summary_df = summarize(df_an) if df_an is not None and not df_an.empty else pd.DataFrame()

    if summary_df is not None and not summary_df.empty:
        upsert_latest(summary_df)
        upsert_hist_daily(summary_df, snapshot_day=today_yyyymmdd())

    t_an = time_mod.time() - t1
    if TIMING_LOG:
        log(f"[T] analyze+upsert_sec={t_an:.2f} | summary_rows={0 if summary_df is None else len(summary_df)}")

    log(f"[RUN] {label} | DONE | month={run_month}")


# =========================
# main: ✅ 상주 서비스 모드 (매일 08:22/20:22 실행, 종료하지 않음)
# =========================
def main():
    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== SERVICE MODE: stay resident; run daily at 08:22 and 20:22 ===")
    log(f"work_mem={WORK_MEM} | keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}")
    log(f"[LOG] tee_path={LOG_PATH}")

    # DB 준비(블로킹) + 테이블 보장
    get_engine_blocking()
    get_conn_pg_blocking()
    ensure_tables_and_indexes()

    # 오늘 RUN_08_22 / RUN_20_22 실행 여부(날짜로 추적)
    ran_1_day: date | None = None
    ran_2_day: date | None = None

    try:
        while True:
            now = datetime.now()
            today = now.date()

            t1_dt = _slot_dt_for(today, RUN_TIME_1)
            t2_dt = _slot_dt_for(today, RUN_TIME_2)

            # 이미 시간이 지났는데 오늘 실행 안 했으면 즉시 실행
            if now >= t1_dt and ran_1_day != today:
                run_pipeline_once("RUN_08_22")
                ran_1_day = today

            if now >= t2_dt and ran_2_day != today:
                run_pipeline_once("RUN_20_22")
                ran_2_day = today

            # 다음 목표시간 계산
            next_dt = _next_slot_target(now, ran_1_day, ran_2_day)
            ran_1 = (ran_1_day == today)
            ran_2 = (ran_2_day == today)

            # 다음 실행까지 대기(하트비트 포함)
            _sleep_with_heartbeat_until(next_dt, ran_1, ran_2)

    finally:
        close_db()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[INTERRUPT] 사용자 중단")
        raise
    except Exception:
        log("\n[ERROR] 예외 발생")
        traceback.print_exc()
        raise
