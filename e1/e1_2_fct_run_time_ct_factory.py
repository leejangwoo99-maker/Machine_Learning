# -*- coding: utf-8 -*-
"""
FCT RunTime CT 분석 파이프라인 (iqz step 전용) - FACTORY DAEMON (file_path PK)

[Source]
- a2_fct_table.fct_table

[Filter]
- station: FCT1~4 (기존 MP=2 분할 설명은 유지하되, 구현은 MP=1로 동작)
- barcode_information LIKE 'B%'
- result <> 'FAIL'
- remark=PD     -> step_description='1.36 Test iqz(uA)'
- remark=Non-PD -> step_description='1.32 Test iqz(uA)'

[Daemon]
- ✅ 5초마다 무한 루프
- 신규 데이터만 캐시에 추가 (중복 방지 PK = file_path 단독)
- 월 롤오버(YYYYMM 변경) 시 캐시/상태 리셋

[Output 정책]
1) LATEST(summary): e1_FCT_ct.fct_run_time_ct
   - UNIQUE (station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE
   - created_at/updated_at 포함

2) HIST(summary): e1_FCT_ct.fct_run_time_ct_hist
   - UNIQUE (snapshot_day, station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE (하루 1개 롤링)
   - snapshot_ts 포함

3) OUTLIER(detail): e1_FCT_ct.fct_upper_outlier_ct_list
   - UNIQUE(station, remark, barcode_information, end_day, end_time)
   - INSERT = ON CONFLICT DO NOTHING (누적 적재 + 중복 방지)

[id NULL 해결]
- latest/hist/outlier 테이블 모두 id 자동채번 보정

[요구사항 반영(안정화)]
- ✅ 멀티프로세스 = 1개 (MP 사용 제거)
- ✅ DB 서버 접속 실패/실행 중 끊김 시 무한 재시도(연결 성공할 때까지 블로킹)
- ✅ 백엔드별 상시 연결을 1개로 고정(풀 최소화)
  - SQLAlchemy: pool_size=1, max_overflow=0
  - psycopg2: 작업 단위 연결(기존 유지) + 실패시 무한재시도
- ✅ work_mem 폭증 방지( SQLAlchemy/psycopg2 세션마다 SET work_mem )
- ✅ (추가) keepalive(connect_args) + connection error 감지 시 engine dispose/rebuild

[로그 모니터링 추가]
- ✅ 스키마: k_demon_heath_check (없으면 생성)
- ✅ 테이블: e1_2_log (없으면 생성)
- ✅ 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- ✅ 저장 시 DataFrame 컬럼 순서: end_day, end_time, info, contents

[기타]
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 콘솔 대기(종료 방지)
"""

import time
import os
import urllib.parse
from datetime import datetime, date
from typing import List

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError

import plotly.graph_objects as go
import plotly.io as pio

import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"

TARGET_SCHEMA = "e1_FCT_ct"

# LATEST(summary)
TBL_RUN_TIME_LATEST = "fct_run_time_ct"
# HIST(summary) 하루 1개 롤링
TBL_RUN_TIME_HIST = "fct_run_time_ct_hist"
# OUTLIER(detail) 누적 적재
TBL_OUTLIER_LIST = "fct_upper_outlier_ct_list"

# 로그 저장(헬스체크)
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e1_2_log"

# 데몬 루프
LOOP_INTERVAL_SEC = 5
FETCH_LIMIT = 200000

# (설명은 유지하되, 구현은 MP=1로 통합 운용)
PROC_1_STATIONS = ["FCT1", "FCT2"]
PROC_2_STATIONS = ["FCT3", "FCT4"]


# =========================
# 안정화 파라미터
# =========================
DB_RETRY_INTERVAL_SEC = 5
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
CONNECT_TIMEOUT_SEC = 5

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

_ENGINE = None


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")

def current_yyyymm(now: datetime | None = None) -> str:
    now = now or datetime.now()
    return now.strftime("%Y%m")

def _masked_db_info() -> str:
    return f'postgresql://{DB_CONFIG["user"]}:***@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'

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

def _cast_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    if "month" in out.columns:
        out["month"] = out["month"].astype(str)
    return out


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError)):
        return True

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
    ]
    return any(k in msg for k in keys)

def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


# =========================
# 1-2) DB 로그 (best-effort)
# =========================
def _now_day_time():
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")

def _normalize_info(info: str) -> str:
    if info is None:
        return "info"
    s = str(info).strip().lower()
    return s if s else "info"

def ensure_log_table_blocking():
    while True:
        conn = None
        try:
            conn = get_conn_psycopg2_blocking(DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";')
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA}".{LOG_TABLE} (
                        id BIGSERIAL PRIMARY KEY,
                        end_day   TEXT NOT NULL,
                        end_time  TEXT NOT NULL,
                        info      TEXT NOT NULL,
                        contents  TEXT,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                """)
            conn.commit()
            return
        except Exception as e:
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            log(f"[DB][RETRY] ensure_log_table_blocking failed: {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass

def write_db_log_best_effort(info: str, contents: str):
    """
    요청 사양:
    - end_day(yyyymmdd), end_time(hh:mi:ss), info, contents
    - DataFrame 컬럼 순서대로 저장
    - info는 소문자
    """
    end_day, end_time = _now_day_time()
    info_norm = _normalize_info(info)
    contents = "" if contents is None else str(contents)

    # DataFrame 컬럼 순서 강제
    df_log = pd.DataFrame(
        [(end_day, end_time, info_norm, contents)],
        columns=["end_day", "end_time", "info", "contents"],
    )

    rows = [tuple(r) for r in df_log.itertuples(index=False, name=None)]
    insert_sql = f"""
    INSERT INTO "{LOG_SCHEMA}".{LOG_TABLE} (end_day, end_time, info, contents)
    VALUES %s
    """
    template = "(%s,%s,%s,%s)"

    conn = None
    try:
        # 로그는 데몬 흐름 방해하지 않도록 단발성 best-effort
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            connect_timeout=2,
            keepalives=PG_KEEPALIVES,
            keepalives_idle=PG_KEEPALIVES_IDLE,
            keepalives_interval=PG_KEEPALIVES_INTERVAL,
            keepalives_count=PG_KEEPALIVES_COUNT,
            application_name="fct_runtime_ct_daemon_log",
        )
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute("SET work_mem TO %s;", (WORK_MEM,))
            execute_values(cur, insert_sql, rows, template=template, page_size=1000)
        conn.commit()
    except Exception:
        # 로그 저장 실패는 콘솔만 유지 (무한루프 방지)
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if conn:
                conn.close()
        except Exception:
            pass

def log_event(info: str, contents: str):
    line = f"[{_normalize_info(info).upper()}] {contents}"
    log(line)
    write_db_log_best_effort(info, contents)


# =========================
# 2) DB: 엔진/커넥션
# =========================
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

    log_event("info", f"DB = {_masked_db_info()}")

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
            "application_name": "fct_runtime_ct_daemon",
        },
    )

def get_engine_blocking(config=DB_CONFIG):
    global _ENGINE

    if _ENGINE is not None:
        while True:
            try:
                with _ENGINE.connect() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    conn.execute(text("SELECT 1"))
                return _ENGINE
            except Exception as e:
                log_event("down", f"engine ping failed -> rebuild: {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                time.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE = _build_engine(config)
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            log_event(
                "info",
                f"engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})",
            )
            return _ENGINE
        except Exception as e:
            log_event("down", f"engine create/connect failed: {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)

def get_conn_psycopg2_blocking(config=DB_CONFIG):
    while True:
        conn = None
        try:
            conn = psycopg2.connect(
                host=config["host"],
                port=config["port"],
                dbname=config["dbname"],
                user=config["user"],
                password=config["password"],
                connect_timeout=CONNECT_TIMEOUT_SEC,
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
                application_name="fct_runtime_ct_daemon",
            )
            conn.autocommit = False
            with conn.cursor() as cur:
                cur.execute("SET work_mem TO %s;", (WORK_MEM,))
            conn.commit()
            return conn
        except Exception as e:
            log_event("down", f"psycopg2 connect failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.close()
            except Exception:
                pass
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 3) 테이블/인덱스/ID 보정
# =========================
def ensure_tables_and_indexes():
    while True:
        try:
            conn = get_conn_psycopg2_blocking(DB_CONFIG)
            try:
                with conn.cursor() as cur:
                    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

                    # LATEST
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (
                            id BIGINT,
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
                    """)
                    cur.execute(f"""
                        ALTER TABLE "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST}
                        ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now();
                    """)
                    cur.execute(f"""
                        ALTER TABLE "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST}
                        ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
                    """)
                    cur.execute(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_RUN_TIME_LATEST}_key
                        ON "{TARGET_SCHEMA}".{TBL_RUN_TIME_LATEST} (station, remark, month);
                    """)

                    # HIST
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (
                            id BIGINT,
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
                    """)
                    cur.execute(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_RUN_TIME_HIST}_day_key
                        ON "{TARGET_SCHEMA}".{TBL_RUN_TIME_HIST} (snapshot_day, station, remark, month);
                    """)

                    # OUTLIER
                    cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (
                            id BIGINT,
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
                    """)
                    cur.execute(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_OUTLIER_LIST}_key
                        ON "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (station, remark, barcode_information, end_day, end_time);
                    """)

                conn.commit()
            finally:
                try:
                    conn.close()
                except Exception:
                    pass

            break

        except Exception as e:
            log_event("down", f"ensure_tables_and_indexes failed: {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)

    fix_id_sequence(TARGET_SCHEMA, TBL_RUN_TIME_LATEST, "fct_run_time_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_RUN_TIME_HIST, "fct_run_time_ct_hist_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_OUTLIER_LIST, "fct_upper_outlier_ct_list_id_seq")

    log_event("info", f'target tables ensured in schema "{TARGET_SCHEMA}"')


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
            conn = get_conn_psycopg2_blocking(DB_CONFIG)
            with conn.cursor() as cur:
                cur.execute(do_sql)
            conn.commit()
            return
        except Exception as e:
            log_event("down", f"fix_id_sequence({schema}.{table}) failed: {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


# =========================
# 4) 로딩 + 전처리 (PK=file_path)
# =========================
def fetch_new_rows_by_file_path(engine, stations: List[str], run_month: str, seen_file_paths: set[str]) -> pd.DataFrame:
    query = f"""
    SELECT
        file_path,
        station,
        remark,
        barcode_information,
        step_description,
        result,
        end_day,
        end_time,
        run_time
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
    ORDER BY end_day ASC, end_time ASC
    LIMIT :limit
    """

    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                df = pd.read_sql(
                    text(query),
                    conn,
                    params={"stations": stations, "run_month": run_month, "limit": FETCH_LIMIT},
                )
            break
        except Exception as e:
            if _is_connection_error(e):
                log_event("down", f"fetch_new_rows conn error -> rebuild: {type(e).__name__}: {repr(e)}")
                _dispose_engine()
            else:
                log_event("error", f"fetch_new_rows failed: {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking(DB_CONFIG)

    if df is None or df.empty:
        return df

    df["file_path"] = df["file_path"].astype(str).str.strip()
    df = df[df["file_path"].notna() & (df["file_path"] != "")].copy()
    if df.empty:
        return df

    df = df[~df["file_path"].isin(seen_file_paths)].copy()
    if df.empty:
        return df

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(8)
    df["end_time"] = df["end_time"].astype(str).str.strip()

    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["run_time"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log_event("info", f"run_time NaN 제거: {dropped} rows drop (new rows)")

    if df.empty:
        return df

    df["month"] = df["end_day"].str.slice(0, 6)
    df = df.sort_values(["end_day", "end_time"], ascending=[True, True]).reset_index(drop=True)
    return df


# =========================
# 5) 요약 생성
# =========================
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

    plotly_json = _make_plotly_box_json(vals, name=name)

    return {
        "sample_amount": n,
        "run_time_lower_outlier": _outlier_range_str(lower_outliers),
        "q1": _round2(q1),
        "median": _round2(median),
        "q3": _round2(q3),
        "run_time_upper_outlier": _outlier_range_str(upper_outliers),
        "del_out_run_time_av": _round2(del_out_mean),
        "plotly_json": plotly_json,
    }

def build_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame(columns=[
            "station", "remark", "month", "sample_amount",
            "run_time_lower_outlier", "q1", "median", "q3",
            "run_time_upper_outlier", "del_out_run_time_av", "plotly_json"
        ])

    groups = list(df.groupby(["station", "remark", "month"], sort=True))

    rows = []
    for (station, remark, month), g in groups:
        vals = g["run_time"].astype(float).to_numpy()
        name = f"{station}_{remark}_{month}"
        d = boxplot_summary_from_values(vals, name=name)
        d.update({"station": station, "remark": remark, "month": month})
        rows.append(d)

    out = pd.DataFrame(rows)
    out = out.sort_values(["station", "remark", "month"]).reset_index(drop=True)
    return out


def build_upper_outlier_df(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty or summary_df is None or summary_df.empty:
        return pd.DataFrame()

    upper_outlier_rows = []
    total = len(summary_df)

    for i, row in summary_df.iterrows():
        if (i + 1) % 20 == 0 or i == 0 or (i + 1) == total:
            log_event("info", f"outlier scan {i+1}/{total} ...")

        station = row["station"]
        remark = row["remark"]
        month = row["month"]

        g = df_raw[
            (df_raw["station"] == station) &
            (df_raw["remark"] == remark) &
            (df_raw["month"] == month)
        ].copy()

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
        upper_outlier_rows.append(out_df)

    if not upper_outlier_rows:
        return pd.DataFrame()

    upper_outlier_df = pd.concat(upper_outlier_rows, ignore_index=True)
    upper_outlier_df = upper_outlier_df[
        ["station", "remark", "barcode_information", "end_day", "end_time", "run_time", "month", "upper_threshold"]
    ].sort_values(["station", "remark", "month", "end_day", "end_time"]).reset_index(drop=True)

    return upper_outlier_df


# =========================
# 6) DB 저장
# =========================
def upsert_latest_run_time_ct(summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        return

    df = _cast_df_for_db(summary_df)
    df = df.where(pd.notnull(df), None)

    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
        "plotly_json",
    ]
    df = df[cols].copy()
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
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
    VALUES %s
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
        updated_at             = now();
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now(), now())"

    while True:
        conn = None
        try:
            conn = get_conn_psycopg2_blocking(DB_CONFIG)
            with conn.cursor() as cur:
                if rows:
                    execute_values(cur, insert_sql, rows, template=template, page_size=1000)
            conn.commit()
            return
        except Exception as e:
            log_event("down", f"upsert_latest_run_time_ct failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time.sleep(DB_RETRY_INTERVAL_SEC)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


def upsert_hist_run_time_ct_daily(summary_df: pd.DataFrame, snapshot_day: str):
    if summary_df is None or summary_df.empty:
        return

    df = _cast_df_for_db(summary_df)
    df = df.where(pd.notnull(df), None)

    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
        "plotly_json",
    ]
    df = df[cols].copy()
    df.insert(0, "snapshot_day", snapshot_day)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
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
    VALUES %s
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
        plotly_json            = EXCLUDED.plotly_json;
    """
    template = "(%s, now(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)"

    while True:
        conn = None
        try:
            conn = get_conn_psycopg2_blocking(DB_CONFIG)
            with conn.cursor() as cur:
                if rows:
                    execute_values(cur, insert_sql, rows, template=template, page_size=1000)
            conn.commit()
            return
        except Exception as e:
            log_event("down", f"upsert_hist_run_time_ct_daily failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time.sleep(DB_RETRY_INTERVAL_SEC)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


def insert_outlier_list(upper_outlier_df: pd.DataFrame):
    if upper_outlier_df is None or upper_outlier_df.empty:
        log_event("info", "upper outlier 저장 생략 (데이터 없음)")
        return

    df = upper_outlier_df.copy()
    df = df.where(pd.notnull(df), None)

    cols = [
        "station", "remark", "barcode_information", "end_day", "end_time",
        "run_time", "month", "upper_threshold",
    ]
    df = df[cols].copy()
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_OUTLIER_LIST} (
        station, remark, barcode_information, end_day, end_time,
        run_time, month, upper_threshold
    )
    VALUES %s
    ON CONFLICT (station, remark, barcode_information, end_day, end_time)
    DO NOTHING;
    """
    template = "(" + ",".join(["%s"] * len(cols)) + ")"

    while True:
        conn = None
        try:
            conn = get_conn_psycopg2_blocking(DB_CONFIG)
            with conn.cursor() as cur:
                if rows:
                    execute_values(cur, insert_sql, rows, template=template, page_size=2000)
            conn.commit()
            log_event("info", f"upper outlier 누적 저장 완료: insert candidates={len(rows)} (중복키는 pass)")
            return
        except Exception as e:
            log_event("down", f"insert_outlier_list failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time.sleep(DB_RETRY_INTERVAL_SEC)
        finally:
            try:
                if conn:
                    conn.close()
            except Exception:
                pass


# =========================
# 7) 재계산 + 저장
# =========================
def recompute_and_upsert(cache_df: pd.DataFrame):
    if cache_df is None or cache_df.empty:
        return

    snapshot_day = today_yyyymmdd()
    summary_df = build_summary_df(cache_df)
    outlier_df = build_upper_outlier_df(cache_df, summary_df)

    upsert_latest_run_time_ct(summary_df)
    upsert_hist_run_time_ct_daily(summary_df, snapshot_day=snapshot_day)
    insert_outlier_list(outlier_df)


# =========================
# 8) main
# =========================
def main():
    start_dt = datetime.now()
    log_event("info", f"{start_dt:%Y-%m-%d %H:%M:%S} | daemon mode (pk=file_path)")
    log_event("info", f"loop_interval={LOOP_INTERVAL_SEC}s | fetch_limit={FETCH_LIMIT}")
    log_event("info", f"src={SRC_SCHEMA}.{SRC_TABLE} -> target schema={TARGET_SCHEMA}")
    log_event("info", f"work_mem={WORK_MEM} | sqlalchemy pool_size=1 max_overflow=0 | db_retry={DB_RETRY_INTERVAL_SEC}s")

    # 로그 테이블 먼저 준비
    ensure_log_table_blocking()
    log_event("info", f'log table ready: "{LOG_SCHEMA}".{LOG_TABLE}')

    ensure_tables_and_indexes()

    stations_all = list(set(PROC_1_STATIONS + PROC_2_STATIONS))
    run_month = current_yyyymm()

    cache_df: pd.DataFrame | None = None
    seen_file_paths: set[str] = set()

    eng = get_engine_blocking(DB_CONFIG)

    try:
        while True:
            loop_t0 = time.time()

            now = datetime.now()
            cur_month = current_yyyymm(now)
            if cur_month != run_month:
                log_event("info", f"month rollover {run_month} -> {cur_month} (cache reset)")
                run_month = cur_month
                cache_df = None
                seen_file_paths.clear()

            try:
                df_new = fetch_new_rows_by_file_path(
                    eng,
                    stations=stations_all,
                    run_month=run_month,
                    seen_file_paths=seen_file_paths,
                )
            except Exception as e:
                log_event("down", f"fetch_new_rows failed: {type(e).__name__}: {repr(e)}")
                if _is_connection_error(e):
                    _dispose_engine()
                eng = get_engine_blocking(DB_CONFIG)
                df_new = pd.DataFrame()

            if df_new is not None and not df_new.empty:
                new_paths = df_new["file_path"].astype(str).tolist()
                seen_file_paths.update(new_paths)

                if cache_df is None or cache_df.empty:
                    cache_df = df_new.copy()
                else:
                    cache_df = pd.concat([cache_df, df_new], ignore_index=True)
                    cache_df = cache_df.drop_duplicates(subset=["file_path"], keep="last").reset_index(drop=True)

                log_event("info", f"new rows={len(df_new)} | cache={len(cache_df)} | seen={len(seen_file_paths)} | month={run_month}")
                recompute_and_upsert(cache_df)
            else:
                log_event("sleep", "no new rows")

            elapsed = time.time() - loop_t0
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                log_event("sleep", f"loop sleep {sleep_sec:.2f}s")
                time.sleep(sleep_sec)

    except KeyboardInterrupt:
        log_event("info", "keyboardinterrupt received. exiting loop...")
    except Exception as e:
        log_event("error", f"unhandled exception: {type(e).__name__}: {repr(e)}")
        raise
    finally:
        try:
            if _ENGINE is not None:
                _ENGINE.dispose()
        except Exception:
            pass

        end_dt = datetime.now()
        elapsed = (end_dt - start_dt).total_seconds()
        log_event("info", f"{end_dt:%Y-%m-%d %H:%M:%S} | end | elapsed={elapsed:.1f}s")

        try:
            input("Press Enter to close this window...")
        except Exception:
            pass


if __name__ == "__main__":
    main()
