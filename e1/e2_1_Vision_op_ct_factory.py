# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인 - current-month incremental + periodic UPSERT
- (기존) MP=2 고정  -> ✅ MP=1 고정
- LATEST/HIST 저장
- id/sequence 자동 보정
- 테이블 스키마 자동 동기화(ALTER TABLE ... ADD COLUMN IF NOT EXISTS)

핵심 수정:
- cache_df에는 end_ts 컬럼을 유지(절대 rename하지 않음)
- 분석 직전 df_for_analysis = cache_df.rename({"end_ts":"end_dt"})로만 변환
- KeyError: 'end_ts' 해결

추가(기존 유지):
1) only-run 판정 로직을 "시간순(end_day,end_time 오름차순) 연속 10개 이상"으로 정확히 변경
2) IDLE(신규 없음) 상태에서도 하트비트 로그 출력
3) fetch/analysis 단계 소요시간 로그

✅ 요청 반영(핵심 사양):
- ✅ 멀티프로세스 = 1개
- ✅ (2번 방식) ALWAYS-ON: main()은 종료하지 않고 매일 08:22 / 20:22에만 1회 실행, 그 외 대기하며 다음날 반복
- ✅ DB 서버 접속 실패/실행 중 끊김 시 무한 재시도(연결 성공할 때까지 블로킹)
- ✅ 백엔드별 상시 연결을 1개로 고정(풀 최소화)
  * SQLAlchemy engine: pool_size=1, max_overflow=0
  * psycopg2: 1개 연결을 재사용(동일 프로세스 내) + 끊김 감지 시 재연결
- ✅ work_mem 폭증 방지
  * SQLAlchemy 세션: SET work_mem
  * psycopg2 세션: SET work_mem

(이번 반영 포인트)
- run_pipeline_once() 수행 중 DB가 끊기면:
  - SQLAlchemy: dispose/rebuild 후 무한 재시도
  - psycopg2: 연결 상태 확인 후 재연결 + 재시도
  - UPSERT 또한 연결끊김 시 재시도(트랜잭션 롤백/커밋 안전)

[추가 반영]
- 데몬 동작 로그를 DB에 저장
  1) 스키마: k_demon_heath_check (없으면 생성)
  2) 테이블: e2_1_log (없으면 생성)
  3) 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
  4) 저장 전 DataFrame 컬럼 순서 고정: end_day, end_time, info, contents

[운영 안정화 추가]
- ✅ SLEEP/IDLE heartbeat 로그는 DB 저장 안 함(콘솔만) -> 로그 테이블 폭증 방지
"""

import os
import sys
import warnings
import urllib.parse
from datetime import datetime, date, time as dtime, timedelta
import time as time_mod

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError

import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import execute_values


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
    "password": "",  # 비번은 보완 사항
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e2_vision_ct"

TBL_LATEST = "vision_op_ct"
TBL_HIST   = "vision_op_ct_hist"

# ✅ 로그 저장 스키마/테이블
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE  = "e2_1_log"

OPCT_MAX_SEC = 600
ONLY_RUN_MIN_LEN = 10

# ✅ MP=1: station 전체를 단일 프로세스에서 처리
STATIONS_ALL = ["Vision1", "Vision2"]

# ✅ 스케줄 실행 시간 (요구사항)
RUN_TIME_1 = dtime(8, 22, 0)
RUN_TIME_2 = dtime(20, 22, 0)

WAIT_INTERVAL_SEC = 5
FETCH_LIMIT = 200000

IDLE_HEARTBEAT = True
TIMING_LOG = True

# ✅ 안정화/리소스 제한
DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = 5
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ keepalive (환경변수로 조정 가능)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)


def _hard_pause_console():
    if os.environ.get("NO_PAUSE", "").strip() == "1":
        return

    try:
        if sys.stdin and sys.stdin.isatty():
            input("\n[PAUSE] 종료하려면 Enter를 누르세요...")
            return
    except Exception:
        pass

    try:
        if os.name == "nt":
            os.system("pause")
            return
    except Exception:
        pass

    try:
        time_mod.sleep(30)
    except Exception:
        pass


def pause_on_exit(exit_code: int = 0):
    try:
        if getattr(sys, "frozen", False):
            _hard_pause_console()
    finally:
        raise SystemExit(exit_code)


def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


def current_yyyymm(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now()
    return now.strftime("%Y%m")


def _is_conn_error(e: Exception) -> bool:
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
# ✅ ALWAYS-ON 스케줄 유틸
# =========================
def _schedule_candidates_for_day(day: date):
    return [
        datetime(day.year, day.month, day.day, RUN_TIME_1.hour, RUN_TIME_1.minute, RUN_TIME_1.second),
        datetime(day.year, day.month, day.day, RUN_TIME_2.hour, RUN_TIME_2.minute, RUN_TIME_2.second),
    ]


def _next_run_datetime(now_dt: datetime) -> datetime:
    """
    now_dt 기준 "가장 가까운 다음 실행시각" 1개 반환.
    - 오늘 08:22 아직이면 -> 오늘 08:22
    - else 오늘 20:22 아직이면 -> 오늘 20:22
    - else -> 내일 08:22
    """
    today = now_dt.date()
    t1, t2 = _schedule_candidates_for_day(today)
    if now_dt < t1:
        return t1
    if now_dt < t2:
        return t2
    tomorrow = today + timedelta(days=1)
    return _schedule_candidates_for_day(tomorrow)[0]


def _sleep_until(target_dt: datetime):
    """
    target_dt까지 대기. DB/CPU 부하 최소화.

    ✅ 요구사항 반영:
    - 대기 중에도 로그 DB에 1분 간격으로 '대기중' 로그 적재
    - 로그 테이블 폭증 방지: DB 저장은 60초마다 1회만
    """
    last_db_log_ts = 0.0  # epoch seconds

    while True:
        now = datetime.now()
        if now >= target_dt:
            return

        remaining = (target_dt - now).total_seconds()

        # 멀리 있으면 크게, 가까우면 촘촘히
        if remaining > 3600:
            sleep_sec = 300.0   # 5분
        elif remaining > 600:
            sleep_sec = 60.0    # 1분
        elif remaining > 120:
            sleep_sec = 30.0
        else:
            sleep_sec = 5.0

        # ✅ DB에는 1분에 1번만 적재
        now_ts = time_mod.time()
        should_db_log = (now_ts - last_db_log_ts) >= 60.0

        msg = (
            f"[WAITING] until={target_dt:%Y-%m-%d %H:%M:%S} | "
            f"remaining={int(remaining)}s | sleep={sleep_sec:.0f}s"
        )

        # 콘솔은 항상, DB는 1분 간격
        db_log("wait", msg, echo=True, save_db=should_db_log)

        if should_db_log:
            last_db_log_ts = now_ts

        time_mod.sleep(sleep_sec)


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
            "application_name": "vision_opct_schedule",
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
                    application_name="vision_opct_schedule",
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
# 1-2) DB 로그 저장 유틸
# =========================
def _ensure_log_table_blocking():
    """로그 테이블 보장(무한 재시도)."""
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
            return
        except Exception as e:
            print(f"[DB][RETRY] ensure log table failed: {type(e).__name__}: {repr(e)}", flush=True)
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def db_log(info: str, contents: str, echo: bool = True, save_db: bool = True):
    """
    로그를 콘솔 + (선택) DB에 저장.
    - info는 소문자로 강제
    - DataFrame 컬럼 순서: end_day, end_time, info, contents
    """
    info_l = (info or "").strip().lower()
    if not info_l:
        info_l = "info"

    now = datetime.now()
    end_day = now.strftime("%Y%m%d")
    end_time = now.strftime("%H:%M:%S")
    contents_s = str(contents) if contents is not None else ""

    if echo:
        print(f"[{info_l}] {contents_s}", flush=True)

    if not save_db:
        return

    df_log = pd.DataFrame(
        [[end_day, end_time, info_l, contents_s]],
        columns=["end_day", "end_time", "info", "contents"]
    )

    insert_sql = f"""
    INSERT INTO "{LOG_SCHEMA}".{LOG_TABLE} (end_day, end_time, info, contents)
    VALUES %s
    """
    rows = [tuple(r) for r in df_log.itertuples(index=False, name=None)]

    while True:
        conn = None
        try:
            conn = get_conn_pg_blocking()
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, rows, template="(%s,%s,%s,%s)", page_size=1000)
            conn.commit()
            return
        except Exception as e:
            print(f"[DB][RETRY] log insert failed: {type(e).__name__}: {repr(e)}", flush=True)
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

                # UNIQUE INDEX
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
            db_log("error", f"ensure_tables_and_indexes failed: {type(e).__name__}: {repr(e)}")
            try:
                if conn:
                    conn.rollback()
            except Exception:
                pass
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    fix_id_sequence(TARGET_SCHEMA, TBL_LATEST, "vision_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_HIST,   "vision_op_ct_hist_id_seq")

    db_log("info", f'target tables ensured in schema "{TARGET_SCHEMA}"')


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
            db_log("error", f"fix_id_sequence({schema}.{table}) failed: {type(e).__name__}: {repr(e)}")
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
    """
    only-run 판정 로직:
    - end_dt 시간순 정렬
    - station이 바뀔 때마다 run_id 증가
    - 해당 run 길이가 ONLY_RUN_MIN_LEN 이상이고 Vision1/2이면 only-run
    """
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
            db_log("error", f"execute_values failed: {type(e).__name__}: {repr(e)}")
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
# 6) 1회 실행 파이프라인 (스케줄 타임에만 호출)
# =========================
def run_pipeline_once(label: str):
    db_log("info", f"{label} start")

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
            db_log("down", f"fetch failed, retry: {type(e).__name__}: {repr(e)}")
            if _is_conn_error(e):
                _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            eng = get_engine_blocking()

    t_fetch = time_mod.time() - t0
    if TIMING_LOG:
        db_log("info", f"fetch_sec={t_fetch:.2f}, rows={0 if df is None else len(df)}")

    if df is None or df.empty:
        db_log("info", f"{label} no rows in month={run_month}, skip")
        return

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    df["end_time_str"] = df["end_time"].astype(str).str.strip()
    df["end_ts"] = pd.to_datetime(df["end_day"] + " " + df["end_time_str"], errors="coerce", format="mixed")
    df = df.dropna(subset=["end_ts"]).copy()
    if df.empty:
        db_log("error", f"{label} all rows dropped by end_ts parse, skip")
        return

    t1 = time_mod.time()

    # ✅ cache_df에는 end_ts 유지(절대 rename하지 않음)
    cache_df = df[["station", "remark", "end_day", "end_time_str", "end_ts"]].copy()
    cache_df = cache_df.sort_values(["station", "remark", "end_ts"], kind="mergesort").reset_index(drop=True)
    cache_df["op_ct"] = cache_df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    cache_df["month"] = cache_df["end_ts"].dt.strftime("%Y%m")

    # ✅ 분석 직전에만 end_dt로 변환
    df_for_analysis = cache_df.rename(columns={"end_ts": "end_dt"}).copy()

    df_marked = mark_only_runs(df_for_analysis)
    df_an = build_analysis_df(df_marked)
    summary_df = summarize(df_an) if df_an is not None and not df_an.empty else pd.DataFrame()

    if summary_df is not None and not summary_df.empty:
        upsert_latest(summary_df)
        upsert_hist_daily(summary_df, snapshot_day=today_yyyymmdd())

    t_an = time_mod.time() - t1
    if TIMING_LOG:
        db_log("info", f"analyze_upsert_sec={t_an:.2f}, summary_rows={0 if summary_df is None else len(summary_df)}")

    db_log("info", f"{label} done, month={run_month}")


# =========================
# main: ✅ (2번 방식) ALWAYS-ON: 매일 08:22/20:22 실행, 종료 없이 다음날 반복
# =========================
def main():
    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")

    # DB 연결/로그 테이블 준비
    get_engine_blocking()
    get_conn_pg_blocking()
    _ensure_log_table_blocking()

    db_log("info", "ALWAYS-ON schedule mode: run at 08:22 and 20:22 EVERY DAY (no exit)")
    db_log("info", f"wait_interval={WAIT_INTERVAL_SEC}s, fetch_limit={FETCH_LIMIT}, work_mem={WORK_MEM}")
    db_log(
        "info",
        f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}, "
        f"sqlalchemy pool_size=1 max_overflow=0"
    )

    ensure_tables_and_indexes()

    # 날짜별 실행 완료 체크: {"yyyymmdd": {"08:22:00","20:22:00"}}
    done_map: dict[str, set] = {}

    try:
        while True:
            now = datetime.now()
            next_dt = _next_run_datetime(now)

            target_day = next_dt.strftime("%Y%m%d")
            target_label = next_dt.strftime("%H:%M:%S")

            done_set = done_map.setdefault(target_day, set())

            # 이미 실행한 스케줄이면 스킵
            if target_label in done_set:
                time_mod.sleep(1.0)
                continue

            # 오래된 날짜 정리(메모리 누수 방지)
            if len(done_map) > 5:
                for k in sorted(list(done_map.keys()))[:-3]:
                    done_map.pop(k, None)

            # ✅ IDLE heartbeat는 콘솔만(DB 저장 안 함)
            if IDLE_HEARTBEAT:
                db_log(
                    "sleep",
                    f"now={now:%H:%M:%S}, next={next_dt:%H:%M:%S}, done({target_day})={sorted(list(done_set))}",
                    echo=True,
                    save_db=False
                )
            else:
                log(f"[WAIT] next_run={next_dt:%Y-%m-%d %H:%M:%S}")

            _sleep_until(next_dt)

            # ✅ 스케줄 1회는 "성공할 때까지" 재시도
            while True:
                try:
                    run_pipeline_once(f"run_{target_label.replace(':','_')}")
                    done_map.setdefault(target_day, set()).add(target_label)
                    db_log("info", f"[OK] done day={target_day} time={target_label} -> {sorted(list(done_map[target_day]))}")
                    break
                except Exception as e:
                    if _is_conn_error(e):
                        db_log("down", f"[RUN][RETRY] {target_label} conn error -> rebuild engine: {type(e).__name__}: {repr(e)}")
                        _dispose_engine()
                        _ = get_engine_blocking()
                    else:
                        db_log("error", f"[RUN][RETRY] {target_label} failed: {type(e).__name__}: {repr(e)}")
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            # 하루 2회 모두 완료되면 요약만 찍고 계속
            if done_map.get(target_day) == {"08:22:00", "20:22:00"}:
                db_log("end", f"[DAY-END] {target_day} both runs completed. keep alive for next day.")

    finally:
        try:
            db_log("info", "closing db connections")
        except Exception:
            pass
        close_db()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        try:
            db_log("info", "interrupt by user")
        except Exception:
            log("[INTERRUPT] 사용자 중단")
        pause_on_exit(0)
    except Exception:
        import traceback
        err = traceback.format_exc()
        try:
            db_log("error", err)
        except Exception:
            log("\n[ERROR] 예외 발생")
            traceback.print_exc()
        pause_on_exit(1)
    else:
        try:
            db_log("info", "normal exit")
        except Exception:
            pass
        if getattr(sys, "frozen", False):
            _hard_pause_console()