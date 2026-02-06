# -*- coding: utf-8 -*-
"""
Vision RunTime CT (intensity8) 분석 파이프라인 - REALTIME(5s loop) + LATEST/HIST 저장 + DB 로그 저장

[Source]
- a3_vision_table.vision_table

[Filter(기본)]
- barcode_information LIKE 'B%'
- station IN ('Vision1','Vision2')
- remark IN ('PD','Non-PD')
- step_description = 'intensity8'
- COALESCE(result,'') <> 'FAIL'
- end_day: "현재 달(YYYYMM)" 데이터만 대상 (월 롤오버 시 캐시 리셋)

[Summary (station, remark, month)]
- sample_amount
- IQR 기반 outlier 범위 문자열
- q1/median/q3
- outlier 제거 평균(del_out_run_time_av)
- plotly_json (boxplot, validate=False)

[Save 정책]
1) LATEST: e2_vision_ct.vision_run_time_ct
   - UNIQUE (station, remark, month)
   - ON CONFLICT DO UPDATE (최신값 갱신)
   - created_at/updated_at 포함

2) HIST(하루 1개 롤링): e2_vision_ct.vision_run_time_ct_hist
   - UNIQUE (snapshot_day, station, remark, month)
   - ON CONFLICT DO UPDATE (그날 마지막 값만 유지)
   - snapshot_ts 포함

[id NULL 해결]
- latest/hist 모두 id 자동채번 보정(컬럼/시퀀스/default/NULL 채움/setval)

[Runtime]
- 무한 루프(5초)
- 신규 데이터가 들어올 때만 요약/UPSERT 수행
- 예외 발생 시 콘솔이 자동으로 닫히지 않도록 hold_console_open 적용

✅ 이번 요청 반영
- ✅ 멀티프로세스 = 1개 (MP 제거)
- ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
- ✅ 실행 중 서버 연결이 끊겨도(네트워크/서버 재시작 등)
     - fetch / month-load / upsert 모두 "연결 복구까지 무한 재시도"
     - SQLAlchemy engine dispose/rebuild
     - psycopg2 연결 상태 확인 후 재연결(1개 재사용)
- ✅ 백엔드별 상시 연결 1개로 고정(풀 최소화)
  * SQLAlchemy engine: pool_size=1, max_overflow=0
  * psycopg2: 1개 연결 재사용(죽으면 폐기 후 재연결)
- ✅ work_mem 폭증 방지: 세션마다 SET work_mem 적용

✅ 로그 DB 적재 반영
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: e2_2_log (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- log 호출 시 콘솔 + DB 동시 저장
"""

import os
import urllib.parse
from datetime import datetime, date
import time as time_mod

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError

import plotly.graph_objects as go
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
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a3_vision_table"
SRC_TABLE = "vision_table"

TARGET_SCHEMA = "e2_vision_ct"
TBL_LATEST = "vision_run_time_ct"
TBL_HIST = "vision_run_time_ct_hist"

STEP_DESC = "intensity8"

# ✅ MP 제거(단일 프로세스)
STATIONS_ALL = ["Vision1", "Vision2"]

# 루프/조회 제한
LOOP_INTERVAL_SEC = 5
FETCH_LIMIT = 200000

# ✅ 연결/리소스 제한
DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = 5
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ keepalive (환경변수로 조정 가능)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

# ✅ 로그 저장 대상
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e2_2_log"

# 내부 플래그(로그 테이블 준비 여부)
_LOG_TABLE_READY = False


# =========================
# 1) 유틸
# =========================
def _now_day_time():
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")


def _safe_print(msg: str):
    print(msg, flush=True)


def _normalize_info(info: str | None) -> str:
    if info is None:
        return "info"
    v = str(info).strip().lower()
    return v if v else "info"


def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


def current_yyyymm(dt: datetime | None = None) -> str:
    base = dt if dt is not None else datetime.now()
    return base.strftime("%Y%m")


def _make_plotly_json(values: np.ndarray, name: str) -> str:
    """
    EXE(onefile)에서 plotly validators 오류 방지 -> validate=False
    """
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), name=name, boxpoints=False))
    return fig.to_json(validate=False)


def hold_console_open(exit_code: int = 1):
    """
    어떤 환경에서도(더블클릭/스케줄러/서비스) 에러 시 콘솔이 바로 닫히지 않게 보장.
    - stdin이 있으면 Enter 대기
    - stdin이 없으면 무한 sleep로 콘솔 유지
    """
    try:
        log("hold", "프로그램이 종료되지 않도록 대기합니다.")
        log("hold", "Enter 입력 가능하면 Enter를 누르세요. 입력이 불가한 환경이면 계속 유지됩니다.")
        try:
            input()
            raise SystemExit(exit_code)
        except Exception:
            while True:
                time_mod.sleep(60)
    except SystemExit:
        raise
    except Exception:
        while True:
            time_mod.sleep(60)


def _is_conn_error(e: Exception) -> bool:
    """
    SQLAlchemy/psycopg2 모두에 대해 "연결 끊김/네트워크" 계열 오류를 최대한 넓게 감지.
    """
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


# =========================
# 1-1) DB: 상시 연결 1개 + 무한 재시도 + work_mem 제한
# =========================
_ENGINE = None
_PG_CONN = None


def _build_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
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
            "application_name": "vision_runtime_ct_intensity8",
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
            _safe_print(f"[DB][RETRY] engine connect failed: {type(e).__name__}: {repr(e)}")
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
                    application_name="vision_runtime_ct_intensity8",
                )
                _PG_CONN.autocommit = False
                with _PG_CONN.cursor() as cur:
                    cur.execute("SET work_mem TO %s;", (WORK_MEM,))
                _PG_CONN.commit()
            return _PG_CONN
        except Exception as e:
            _safe_print(f"[DB][RETRY] psycopg2 connect failed: {type(e).__name__}: {repr(e)}")
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
# 1-2) 로그 테이블 준비 + 로그 적재
# =========================
def ensure_log_table():
    """
    로그 스키마/테이블 생성:
    k_demon_heath_check.e2_2_log
    컬럼:
      end_day  TEXT (yyyymmdd)
      end_time TEXT (hh:mi:ss)
      info     TEXT (소문자)
      contents TEXT
    """
    global _LOG_TABLE_READY
    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";')
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA}".{LOG_TABLE} (
                        end_day  TEXT NOT NULL,
                        end_time TEXT NOT NULL,
                        info     TEXT NOT NULL,
                        contents TEXT
                    );
                """)
            conn.commit()
            _LOG_TABLE_READY = True
            return
        except Exception as e:
            _safe_print(f"[DB][RETRY] ensure_log_table failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            if _is_conn_error(e):
                global _PG_CONN
                try:
                    if _PG_CONN is not None:
                        _PG_CONN.close()
                except Exception:
                    pass
                _PG_CONN = None
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def _insert_log_row_blocking(info: str, contents: str):
    """
    1행 DataFrame(end_day, end_time, info, contents) 생성 후 insert.
    실패 시 메인 로직 영향 최소화를 위해 예외를 삼키고 콘솔에만 출력.
    """
    global _LOG_TABLE_READY
    try:
        if not _LOG_TABLE_READY:
            ensure_log_table()

        end_day, end_time = _now_day_time()
        info_norm = _normalize_info(info)

        # 요구사항: dataframe화 후 저장 + 컬럼 순서 고정
        df = pd.DataFrame([{
            "end_day": end_day,
            "end_time": end_time,
            "info": info_norm,
            "contents": str(contents) if contents is not None else None,
        }])[["end_day", "end_time", "info", "contents"]]

        rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

        sql = f"""
            INSERT INTO "{LOG_SCHEMA}".{LOG_TABLE}
            (end_day, end_time, info, contents)
            VALUES %s
        """
        template = "(%s,%s,%s,%s)"

        while True:
            conn = None
            try:
                conn = get_conn_pg_blocking()
                with conn.cursor() as cur:
                    cur.execute("SET work_mem TO %s;", (WORK_MEM,))
                    execute_values(cur, sql, rows, template=template, page_size=1000)
                conn.commit()
                return
            except Exception as e:
                try:
                    if conn:
                        conn.rollback()
                except Exception:
                    pass
                if _is_conn_error(e):
                    global _PG_CONN
                    try:
                        if _PG_CONN is not None:
                            _PG_CONN.close()
                    except Exception:
                        pass
                    _PG_CONN = None
                # 로그 적재 오류는 메인 처리 방해하지 않도록 짧게 재시도 후 포기
                _safe_print(f"[LOG][WARN] log insert retry: {type(e).__name__}: {repr(e)}")
                time_mod.sleep(1)
                # 과도한 블로킹 방지: 1회 재시도 후 탈출
                break

    except Exception as e:
        _safe_print(f"[LOG][WARN] log insert failed: {type(e).__name__}: {repr(e)}")


def log(msg: str, level: str = "info"):
    """
    사용 방식:
      log("내용") -> level='info'
      log("내용", "error")
    """
    lv = _normalize_info(level)
    line = f"[{lv.upper()}] {msg}"
    _safe_print(line)
    _insert_log_row_blocking(lv, msg)


# =========================
# 2) 테이블/인덱스/ID 보정 (DB 실패 시 무한 재시도)
# =========================
def ensure_tables_and_indexes():
    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

                # --- LATEST
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_LATEST} (
                        id BIGINT,
                        station TEXT NOT NULL,
                        remark  TEXT NOT NULL,
                        month   TEXT NOT NULL
                    );
                """)

                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS sample_amount INTEGER;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS run_time_lower_outlier TEXT;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS q1 DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS median DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS q3 DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS run_time_upper_outlier TEXT;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS del_out_run_time_av DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS plotly_json JSONB;')

                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;')
                cur.execute(f"""
                    ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST}
                    ALTER COLUMN created_at SET DEFAULT now(),
                    ALTER COLUMN updated_at SET DEFAULT now();
                """)
                cur.execute(f'UPDATE "{TARGET_SCHEMA}".{TBL_LATEST} SET created_at = COALESCE(created_at, now());')
                cur.execute(f'UPDATE "{TARGET_SCHEMA}".{TBL_LATEST} SET updated_at = COALESCE(updated_at, now());')

                cur.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_LATEST}_key
                    ON "{TARGET_SCHEMA}".{TBL_LATEST} (station, remark, month);
                """)

                # --- HIST
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_HIST} (
                        id BIGINT,
                        snapshot_day TEXT NOT NULL,
                        station TEXT NOT NULL,
                        remark  TEXT NOT NULL,
                        month   TEXT NOT NULL
                    );
                """)

                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS snapshot_ts TIMESTAMPTZ;')
                cur.execute(f"""
                    ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST}
                    ALTER COLUMN snapshot_ts SET DEFAULT now();
                """)
                cur.execute(f'UPDATE "{TARGET_SCHEMA}".{TBL_HIST} SET snapshot_ts = COALESCE(snapshot_ts, now());')

                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS sample_amount INTEGER;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS run_time_lower_outlier TEXT;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS q1 DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS median DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS q3 DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS run_time_upper_outlier TEXT;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS del_out_run_time_av DOUBLE PRECISION;')
                cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS plotly_json JSONB;')

                cur.execute(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_HIST}_day_key
                    ON "{TARGET_SCHEMA}".{TBL_HIST} (snapshot_day, station, remark, month);
                """)

            conn.commit()
            break

        except Exception as e:
            log(f"ensure_tables_and_indexes failed: {type(e).__name__}: {repr(e)}", "error")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            if _is_conn_error(e):
                global _PG_CONN
                try:
                    if _PG_CONN is not None:
                        _PG_CONN.close()
                except Exception:
                    pass
                _PG_CONN = None
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    fix_id_sequence(TARGET_SCHEMA, TBL_LATEST, "vision_run_time_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_HIST, "vision_run_time_ct_hist_id_seq")
    log(f'target tables ensured in schema "{TARGET_SCHEMA}"', "ok")


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
                cur.execute("SET work_mem TO %s;", (WORK_MEM,))
                cur.execute(do_sql)
            conn.commit()
            return
        except Exception as e:
            log(f"fix_id_sequence({schema}.{table}) failed: {type(e).__name__}: {repr(e)}", "error")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            if _is_conn_error(e):
                global _PG_CONN
                try:
                    if _PG_CONN is not None:
                        _PG_CONN.close()
                except Exception:
                    pass
                _PG_CONN = None
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 3) 월 전체 로드 + 요약
# =========================
def _read_sql_blocking(engine, sql: str, params: dict) -> pd.DataFrame:
    eng = engine
    while True:
        try:
            with eng.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                return pd.read_sql(text(sql), conn, params=params)
        except Exception as e:
            log(f"read_sql failed: {type(e).__name__}: {repr(e)}", "error")
            if _is_conn_error(e):
                _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            eng = get_engine_blocking()


def load_source_month(engine, stations: list[str], run_month: str) -> pd.DataFrame:
    sql = f"""
    SELECT
        barcode_information,
        station,
        remark,
        step_description,
        result,
        end_day,
        end_time,
        run_time
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE barcode_information LIKE 'B%%'
      AND station = ANY(:stations)
      AND remark IN ('PD','Non-PD')
      AND step_description = :step_desc
      AND COALESCE(result,'') <> 'FAIL'
      AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
    ORDER BY end_day ASC, end_time ASC
    """
    df = _read_sql_blocking(
        engine,
        sql,
        {"stations": stations, "run_month": run_month, "step_desc": STEP_DESC},
    )

    if df is None or df.empty:
        return df

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    df = df.dropna(subset=["run_time"]).copy()

    dt_str = df["end_day"] + " " + df["end_time"]
    df["end_dt"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")
    df = df.dropna(subset=["end_dt"]).copy()

    df = df.sort_values(["end_dt", "station", "remark"]).reset_index(drop=True)
    df["month"] = df["end_dt"].dt.strftime("%Y%m")
    return df


def _summary_from_group(station: str, remark: str, month: str, series: pd.Series) -> dict | None:
    vals = series.dropna().astype(float).to_numpy()
    if vals.size == 0:
        return None

    q1 = float(np.percentile(vals, 25))
    med = float(np.percentile(vals, 50))
    q3 = float(np.percentile(vals, 75))
    iqr = q3 - q1

    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    lower_out = vals[vals < lower_fence]
    upper_out = vals[vals > upper_fence]

    run_time_lower_outlier = f"{lower_out.min():.2f}~{lower_fence:.2f}" if lower_out.size > 0 else None
    run_time_upper_outlier = f"{upper_fence:.2f}~{upper_out.max():.2f}" if upper_out.size > 0 else None

    inliers = vals[(vals >= lower_fence) & (vals <= upper_fence)]
    del_out_mean = float(np.mean(inliers)) if inliers.size > 0 else np.nan

    plotly_json = _make_plotly_json(vals, name=f"{station}_{remark}_{month}")

    return {
        "station": station,
        "remark": remark,
        "month": str(month),
        "sample_amount": int(vals.size),
        "run_time_lower_outlier": run_time_lower_outlier,
        "q1": round(q1, 2),
        "median": round(med, 2),
        "q3": round(q3, 2),
        "run_time_upper_outlier": run_time_upper_outlier,
        "del_out_run_time_av": round(del_out_mean, 2) if not np.isnan(del_out_mean) else None,
        "plotly_json": plotly_json,
    }


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    rows = []
    groups = list(df.groupby(["station", "remark", "month"], sort=True))
    total = len(groups)
    log(f"그룹 수 = {total}", "info")

    for i, ((st, rk, mo), g) in enumerate(groups, start=1):
        if i == 1 or i == total or i % 20 == 0:
            log(f"group {i}/{total} ... ({st},{rk},{mo})", "progress")
        r = _summary_from_group(st, rk, mo, g["run_time"])
        if r is not None:
            rows.append(r)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["month", "station", "remark"]).reset_index(drop=True)


# =========================
# 4) 저장: LATEST/HIST UPSERT
# =========================
def _execute_values_retry(sql_text: str, rows: list, template: str, page_size: int = 2000):
    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                cur.execute("SET work_mem TO %s;", (WORK_MEM,))
                if rows:
                    execute_values(cur, sql_text, rows, template=template, page_size=page_size)
            conn.commit()
            return
        except Exception as e:
            log(f"execute_values failed: {type(e).__name__}: {repr(e)}", "error")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            if _is_conn_error(e):
                global _PG_CONN
                try:
                    if _PG_CONN is not None:
                        _PG_CONN.close()
                except Exception:
                    pass
                _PG_CONN = None
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def upsert_latest(summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
        "plotly_json",
    ]
    df = df[cols]
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_LATEST} (
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
    _execute_values_retry(insert_sql, rows, template=template, page_size=2000)


def upsert_hist_daily(summary_df: pd.DataFrame, snapshot_day: str):
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
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
    _execute_values_retry(insert_sql, rows, template=template, page_size=2000)


# =========================
# 5) main: REALTIME LOOP
# =========================
def main():
    start_dt = datetime.now()
    log(f"start {start_dt:%Y-%m-%d %H:%M:%S}", "start")
    log("realtime mode: current-month incremental + periodic upsert", "info")
    log(f"loop_interval={LOOP_INTERVAL_SEC}s | fetch_limit={FETCH_LIMIT} | work_mem={WORK_MEM}", "info")

    # DB 연결/엔진 준비 + 테이블 보정 + 로그 테이블 준비
    engine = get_engine_blocking()
    get_conn_pg_blocking()
    ensure_log_table()
    ensure_tables_and_indexes()

    run_month = current_yyyymm()
    snapshot_day = today_yyyymmdd()

    # last_ts: (station, remark) -> pd.Timestamp
    last_ts: dict[tuple[str, str], pd.Timestamp] = {}

    def _fetch_new_rows(engine_local, stations: list[str], run_month_local: str) -> pd.DataFrame:
        while True:
            try:
                min_last = min(last_ts.values()) if last_ts else None

                extra_where = ""
                params = {
                    "stations": stations,
                    "run_month": run_month_local,
                    "step_desc": STEP_DESC,
                    "limit": FETCH_LIMIT,
                }
                if min_last is not None:
                    extra_where = """
                      AND ( (regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') || ' ' || COALESCE(end_time::text,''))::timestamp > :min_last )
                    """
                    params["min_last"] = str(min_last)

                sql_query = f"""
                SELECT
                    barcode_information,
                    station,
                    remark,
                    end_day,
                    end_time,
                    run_time
                FROM {SRC_SCHEMA}.{SRC_TABLE}
                WHERE barcode_information LIKE 'B%%'
                  AND station = ANY(:stations)
                  AND remark IN ('PD','Non-PD')
                  AND step_description = :step_desc
                  AND COALESCE(result,'') <> 'FAIL'
                  AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
                  {extra_where}
                ORDER BY end_day ASC, end_time ASC
                LIMIT :limit
                """

                with engine_local.connect() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    df = pd.read_sql(text(sql_query), conn, params=params)

                if df is None or df.empty:
                    return df

                df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
                df["end_time"] = df["end_time"].astype(str).str.strip()
                df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

                dt_str = df["end_day"] + " " + df["end_time"]
                df["end_ts"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")
                df = df.dropna(subset=["end_ts"]).copy()
                df = df.dropna(subset=["run_time"]).copy()

                keep_parts = []
                for (st, rm), g in df.groupby(["station", "remark"], sort=False):
                    lt = last_ts.get((st, rm))
                    keep_parts.append(g if lt is None else g[g["end_ts"] > lt])

                if not keep_parts:
                    return df.iloc[0:0].copy()
                return pd.concat(keep_parts, ignore_index=True)

            except Exception as e:
                log(f"fetch_new_rows failed: {type(e).__name__}: {repr(e)}", "error")
                if _is_conn_error(e):
                    _dispose_engine()
                    log("db connection down -> engine rebuild", "down")
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine_local = get_engine_blocking()

    def _update_last_ts(df_new: pd.DataFrame):
        for (st, rm), g in df_new.groupby(["station", "remark"], sort=False):
            last_ts[(st, rm)] = g["end_ts"].max()

    try:
        while True:
            now = datetime.now()

            # 월 롤오버
            cur_month = current_yyyymm(now)
            if cur_month != run_month:
                log(f"month rollover {run_month} -> {cur_month} (cache reset)", "info")
                run_month = cur_month
                snapshot_day = today_yyyymmdd()
                last_ts.clear()

            # 신규분 체크
            df_new = _fetch_new_rows(engine, STATIONS_ALL, run_month)

            if df_new is not None and not df_new.empty:
                _update_last_ts(df_new)
                log(f"new rows={len(df_new)} month={run_month}", "info")

                df_month = load_source_month(engine, stations=STATIONS_ALL, run_month=run_month)
                summary_all = build_summary(df_month) if df_month is not None and not df_month.empty else pd.DataFrame()

                if summary_all is not None and not summary_all.empty:
                    upsert_latest(summary_all)
                    upsert_hist_daily(summary_all, snapshot_day=snapshot_day)
                    log("upsert latest + hist ok", "ok")
                else:
                    log("summary 없음 -> 저장 생략", "skip")
            else:
                log("sleep 5s (no new rows)", "sleep")

            time_mod.sleep(LOOP_INTERVAL_SEC)

    finally:
        close_db()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("사용자 중단", "interrupt")
        hold_console_open(0)
    except Exception:
        import traceback
        err_txt = traceback.format_exc()
        log("예외 발생", "error")
        _safe_print(err_txt)
        _insert_log_row_blocking("error", err_txt)
        hold_console_open(1)
