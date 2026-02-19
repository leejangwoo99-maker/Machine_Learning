# -*- coding: utf-8 -*-
"""
e1_1_FCT_op_ct_factory_schedule.py

[스케줄 실행 방식 - 적용(요청)]
- 매일 08:22:00에 1회 실행 -> 계산/UPSERT
- 매일 20:22:00에 1회 실행 -> 계산/UPSERT
- 그 외 시간에는 대기만 함
- 2회 모두 완료되면 종료

[요청 반영(안정화)]
- ✅ 멀티프로세스 = 1개 (MP=2 제거)
- ✅ DB 서버 접속 실패/실행 중 끊김 시 무한 재시도(연결 성공할 때까지 블로킹)
- ✅ 백엔드별 상시 연결 1개 고정(풀 최소화: pool_size=1, max_overflow=0)
- ✅ work_mem 폭증 방지: 세션마다 SET work_mem (환경변수 PG_WORK_MEM로 조정 가능)

[기존 기능 유지]
- boxplot html 저장
- plotly_json 저장
- UPH(left/right/whole) 계산
- LATEST: 키 기준 UPSERT(최신 갱신)
- HIST  : 하루 1개 롤링 UPSERT(ON CONFLICT DO UPDATE)
- id NULL 해결: id/시퀀스/default/백필/setval 동기화
- DataFrame 콘솔 출력 없음(로그만)

(추가: 실행 중 끊김 복구 강화)
- ✅ SQLAlchemy engine 사용 구간: pool_pre_ping + 연결 오류 감지 시 dispose 후 재생성
- ✅ psycopg2 직접 접속 구간: 연결 오류 시 무한 재접속/재시도 (이미 while True로 감싸져 있음)
- ✅ keepalive(connect_args/환경변수) 옵션 추가(가능 범위 내)

[추가: 데몬 헬스체크 로그 DB 저장]
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: e1_1_log (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 로그는 end_day, end_time, info, contents 순서로 DataFrame화 후 저장
"""

import time
import os
import urllib.parse
from datetime import datetime, date, time as dtime

from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import plotly.express as px

import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, OperationalError, DBAPIError
from pandas.api.types import CategoricalDtype


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

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e1_FCT_ct"

TBL_OPCT_LATEST  = "fct_op_ct"
TBL_WHOLE_LATEST = "fct_whole_op_ct"

TBL_OPCT_HIST  = "fct_op_ct_hist"
TBL_WHOLE_HIST = "fct_whole_op_ct_hist"

OUT_DIR = Path("./fct_opct_boxplot_html")
OUT_DIR.mkdir(parents=True, exist_ok=True)

OPCT_MAX_SEC = 600
FETCH_LIMIT = 200000

# 스케줄 실행 시간(요구사항)
RUN_TIME_1 = dtime(8, 22, 0)
RUN_TIME_2 = dtime(20, 22, 0)

# 기존 상수는 유지하되 MP는 제거 (단일 프로세스에서 전체 station 처리)
ALL_STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]

# =========================
# ✅ 안정화 파라미터
# =========================
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")  # 필요 시 환경변수로 조정
DB_RETRY_INTERVAL_SEC = 5

# ✅ (추가/권장) keepalive 옵션(환경변수로 조정 가능)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

# =========================
# ✅ 로그 DB(헬스체크) 설정
# =========================
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e1_1_log"

_ENGINE = None


# =========================
# 1) 유틸
# =========================
def _normalize_info(info: str) -> str:
    s = (info or "info").strip().lower()
    return s if s else "info"


def _masked_db_info() -> str:
    return f'postgresql://{DB_CONFIG["user"]}:***@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError)):
        return True

    msg = (str(e) or "").lower()
    keywords = [
        "server closed the connection",
        "connection not open",
        "terminating connection",
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
    return any(k in msg for k in keywords)


def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


def _build_engine(config=DB_CONFIG):
    pw = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{pw}"
        f"@{config['host']}:{config['port']}/{config['dbname']}?connect_timeout=5"
    )
    print(f"[INFO] DB = {_masked_db_info()}", flush=True)

    # ✅ 풀 최소화(상시 연결 1개로 고정) + ✅ keepalive
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args={
            "connect_timeout": 5,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "e1_1_fct_op_ct_schedule",
        },
    )


def get_engine_blocking(config=DB_CONFIG):
    """
    ✅ DB 접속 실패/실행 중 끊김 시 무한 재시도(연결 성공까지 블로킹)
    ✅ 엔진 풀 최소화(pool_size=1, max_overflow=0)
    ✅ work_mem 폭증 방지(세션마다 SET)
    """
    global _ENGINE

    if _ENGINE is not None:
        while True:
            try:
                with _ENGINE.connect() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    conn.execute(text("SELECT 1"))
                return _ENGINE
            except Exception as e:
                print(f"[DB][RETRY] ping failed -> rebuild: {type(e).__name__}: {repr(e)}", flush=True)
                _dispose_engine()
                time.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE = _build_engine(config)
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            print(
                f"[DB][OK] engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})",
                flush=True
            )
            return _ENGINE
        except Exception as e:
            print(f"[DB][RETRY] engine create/connect failed: {type(e).__name__}: {repr(e)}", flush=True)
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


def current_yyyymm(now: datetime | None = None) -> str:
    now = now or datetime.now()
    return now.strftime("%Y%m")


def _cast_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    out = df.copy()
    if "month" in out.columns:
        out["month"] = out["month"].astype(str)
    for c in out.columns:
        if isinstance(out[c].dtype, CategoricalDtype):
            out[c] = out[c].astype(str)
    return out


# =========================
# 1-1) 로그 테이블 보장 + 로그 저장
# =========================
def ensure_log_table():
    """
    로그 스키마/테이블 보장:
      k_demon_heath_check.e1_1_log
    """
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        end_day   TEXT NOT NULL,   -- yyyymmdd
        end_time  TEXT NOT NULL,   -- hh:mi:ss
        info      TEXT NOT NULL,   -- 소문자
        contents  TEXT,
        created_at TIMESTAMP DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE}_day_time
      ON {LOG_SCHEMA}.{LOG_TABLE}(end_day, end_time);

    CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE}_info
      ON {LOG_SCHEMA}.{LOG_TABLE}(info);
    """
    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute(ddl)
                conn.commit()
            print(f"[OK] log table ensured: {LOG_SCHEMA}.{LOG_TABLE}", flush=True)
            return
        except Exception as e:
            print(f"[DB][RETRY] ensure_log_table failed: {type(e).__name__}: {repr(e)}", flush=True)
            time.sleep(DB_RETRY_INTERVAL_SEC)


def _save_log_to_db(info: str, contents: str):
    """
    요구사항:
    1) 로그 생성
    2) end_day : yyyymmdd
    3) end_time: hh:mi:ss
    4) info: 소문자
    5) contents: 나머지 내용
    6) end_day, end_time, info, contents 순서 DataFrame화 후 저장
    """
    now = datetime.now()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": str(contents),
    }

    # 컬럼 순서 고정 DataFrame
    log_df = pd.DataFrame([[row["end_day"], row["end_time"], row["info"], row["contents"]]],
                          columns=["end_day", "end_time", "info", "contents"])

    insert_sql = sql.SQL("""
        INSERT INTO {}.{} (end_day, end_time, info, contents)
        VALUES (%(end_day)s, %(end_time)s, %(info)s, %(contents)s)
    """).format(sql.Identifier(LOG_SCHEMA), sql.Identifier(LOG_TABLE))

    # 로그 저장 자체도 연결 실패 시 재시도(무한)
    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.executemany(insert_sql.as_string(conn), log_df.to_dict(orient="records"))
                conn.commit()
            return
        except Exception as e:
            # 여기서는 재귀 로그 방지 위해 print만
            print(f"[DB][RETRY] save_log failed: {type(e).__name__}: {repr(e)}", flush=True)
            time.sleep(DB_RETRY_INTERVAL_SEC)


def log(msg: str, info: str = "info", save_db: bool = True):
    """
    공통 로그 출력 + DB 저장
    info는 반드시 소문자로 정규화
    """
    info_n = _normalize_info(info)
    print(msg, flush=True)
    if save_db:
        try:
            _save_log_to_db(info_n, msg)
        except Exception as e:
            # 여기서는 print만(무한 재귀 방지)
            print(f"[WARN] log db write skipped: {type(e).__name__}: {repr(e)}", flush=True)


def _sleep_until(target_dt: datetime):
    """
    target_dt까지 대기(초 단위 sleep). DB/파일 부하를 만들지 않도록 1~30초로 슬립.
    """
    while True:
        now = datetime.now()
        if now >= target_dt:
            return
        sec = (target_dt - now).total_seconds()
        sleep_sec = min(max(sec, 1.0), 30.0)
        log(f"[SLEEP] waiting {sleep_sec:.1f}s until {target_dt:%Y-%m-%d %H:%M:%S}", info="sleep")
        time.sleep(sleep_sec)


def _next_run_datetimes(now_dt: datetime) -> List[datetime]:
    """
    오늘 기준으로 남아있는 실행 타임(08:22, 20:22)을 반환.
    이미 지난 타임은 제외.
    """
    d = now_dt.date()
    cands = [
        datetime(d.year, d.month, d.day, RUN_TIME_1.hour, RUN_TIME_1.minute, RUN_TIME_1.second),
        datetime(d.year, d.month, d.day, RUN_TIME_2.hour, RUN_TIME_2.minute, RUN_TIME_2.second),
    ]
    return [t for t in cands if t >= now_dt]


# =========================
# 2) 테이블/인덱스/ID 보정 (기존 유지)
# =========================
def ensure_schema(conn, schema_name: str):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    conn.commit()


def ensure_tables_and_indexes():
    """
    기존 로직 유지.
    ✅ 실행 중 끊김/접속 실패 시 무한 재시도(블로킹)
    """
    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                ensure_schema(conn, TARGET_SCHEMA)

                with conn.cursor() as cur:
                    # LATEST: fct_op_ct
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            station TEXT NOT NULL,
                            remark  TEXT NOT NULL,
                            month   TEXT NOT NULL,
                            sample_amount INT,
                            op_ct_lower_outlier TEXT,
                            q1 NUMERIC(12,2),
                            median NUMERIC(12,2),
                            q3 NUMERIC(12,2),
                            op_ct_upper_outlier TEXT,
                            del_out_op_ct_av NUMERIC(12,2),
                            plotly_json TEXT
                        );
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

                    cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();""")
                                .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))
                    cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();""")
                                .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_op_ct_key
                        ON {}.{} (station, remark, month);
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

                    # LATEST: fct_whole_op_ct
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            station TEXT NOT NULL,
                            remark  TEXT NOT NULL,
                            month   TEXT NOT NULL,
                            ct_eq NUMERIC(12,2),
                            uph   NUMERIC(12,2),
                            final_ct NUMERIC(12,2)
                        );
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

                    cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();""")
                                .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))
                    cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();""")
                                .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_whole_op_ct_key
                        ON {}.{} (station, remark, month);
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

                    # HIST: fct_op_ct_hist
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            snapshot_day TEXT NOT NULL,
                            snapshot_ts  TIMESTAMP DEFAULT now(),
                            station TEXT NOT NULL,
                            remark  TEXT NOT NULL,
                            month   TEXT NOT NULL,
                            sample_amount INT,
                            op_ct_lower_outlier TEXT,
                            q1 NUMERIC(12,2),
                            median NUMERIC(12,2),
                            q3 NUMERIC(12,2),
                            op_ct_upper_outlier TEXT,
                            del_out_op_ct_av NUMERIC(12,2),
                            plotly_json TEXT
                        );
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_op_ct_hist_day_key
                        ON {}.{} (snapshot_day, station, remark, month);
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST)))

                    # HIST: fct_whole_op_ct_hist
                    cur.execute(sql.SQL("""
                        CREATE TABLE IF NOT EXISTS {}.{} (
                            id BIGINT,
                            snapshot_day TEXT NOT NULL,
                            snapshot_ts  TIMESTAMP DEFAULT now(),
                            station TEXT NOT NULL,
                            remark  TEXT NOT NULL,
                            month   TEXT NOT NULL,
                            ct_eq NUMERIC(12,2),
                            uph   NUMERIC(12,2),
                            final_ct NUMERIC(12,2)
                        );
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST)))

                    cur.execute(sql.SQL("""
                        CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_whole_op_ct_hist_day_key
                        ON {}.{} (snapshot_day, station, remark, month);
                    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST)))

                conn.commit()

            break

        except Exception as e:
            log(f"[DB][RETRY] ensure_tables_and_indexes failed: {type(e).__name__}: {repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)

    # id 자동채번/NULL 보정 (4개 테이블 모두)
    fix_id_sequence(TARGET_SCHEMA, TBL_OPCT_LATEST,  "fct_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_WHOLE_LATEST, "fct_whole_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_OPCT_HIST,    "fct_op_ct_hist_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_WHOLE_HIST,   "fct_whole_op_ct_hist_id_seq")

    log(f'[OK] target tables ensured in schema "{TARGET_SCHEMA}"', info="info")


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
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.execute(do_sql)
                conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] fix_id_sequence({schema}.{table}) failed: {type(e).__name__}: {repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 3) 소스 로딩(월 전체) + op_ct 계산
# =========================
def load_month_df(engine, stations: List[str], run_month: str) -> pd.DataFrame:
    sql_query = f"""
    SELECT
        station,
        remark,
        end_day,
        end_time
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE station = ANY(:stations)
      AND remark IN ('PD','Non-PD')
      AND result <> 'FAIL'
      AND goodorbad <> 'BadFile'
      AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
    ORDER BY station ASC, remark ASC, end_time ASC
    """

    while True:
        try:
            # ✅ 세션 설정 후 read_sql
            with engine.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                df = pd.read_sql(
                    text(sql_query),
                    conn,
                    params={"stations": stations, "run_month": run_month},
                )
            break
        except Exception as e:
            # ✅ 실행 중 끊김 복구
            if _is_connection_error(e):
                log(f"[DB][RETRY] load_month_df conn error -> rebuild: {type(e).__name__}: {repr(e)}", info="down")
                _dispose_engine()
            else:
                log(f"[DB][RETRY] load_month_df failed: {type(e).__name__}: {repr(e)}", info="error")
            time.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking(DB_CONFIG)

    if df.empty:
        return df

    df["end_day"] = pd.to_datetime(df["end_day"], errors="coerce").dt.date
    df["end_time_str"] = df["end_time"].astype(str)
    df["end_ts"] = pd.to_datetime(df["end_day"].astype(str) + " " + df["end_time_str"], errors="coerce")

    df = df.dropna(subset=["end_ts"]).copy()
    df = df.sort_values(["station", "remark", "end_ts"], ascending=True).reset_index(drop=True)

    df["op_ct"] = df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    df["month"] = df["end_ts"].dt.strftime("%Y%m")
    return df


# =========================
# 4) Boxplot 요약 + plotly_json
# =========================
def _range_str(values: pd.Series):
    values = values.dropna()
    if len(values) == 0:
        return None
    vmin = float(values.min())
    vmax = float(values.max())
    return f"{vmin:.1f}~{vmax:.1f}"


def summarize_group(g: pd.DataFrame) -> Dict:
    s = g["op_ct"].dropna()
    s = s[s <= OPCT_MAX_SEC]

    sample_amount = len(s)
    if sample_amount == 0:
        return {
            "sample_amount": 0,
            "op_ct_lower_outlier": None,
            "q1": None,
            "median": None,
            "q3": None,
            "op_ct_upper_outlier": None,
            "del_out_op_ct_av": None,
            "plotly_json": None,
        }

    q1 = float(s.quantile(0.25))
    med = float(s.quantile(0.50))
    q3 = float(s.quantile(0.75))
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    lower_outliers = s[s < lower_bound]
    upper_outliers = s[s > upper_bound]

    s_wo = s[(s >= lower_bound) & (s <= upper_bound)]
    avg_wo = float(s_wo.mean()) if len(s_wo) else None

    station = g["station"].iloc[0]
    remark = g["remark"].iloc[0]
    month = g["month"].iloc[0]

    fig = px.box(pd.DataFrame({"op_ct": s}), y="op_ct", points="outliers", title=None)

    html_name = f"boxplot_{station}_{remark}_{month}.html"
    html_path = OUT_DIR / html_name
    fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)

    return {
        "sample_amount": int(sample_amount),
        "op_ct_lower_outlier": _range_str(lower_outliers),
        "q1": round(q1, 2),
        "median": round(med, 2),
        "q3": round(q3, 2),
        "op_ct_upper_outlier": _range_str(upper_outliers),
        "del_out_op_ct_av": round(avg_wo, 2) if avg_wo is not None else None,
        "plotly_json": fig.to_json(),
    }


def build_summary_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=[
            "station", "remark", "month",
            "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
            "op_ct_upper_outlier", "del_out_op_ct_av", "plotly_json"
        ])

    groups = list(df_raw.groupby(["station", "remark", "month"], sort=True))

    rows = []
    for (station, remark, month), g in groups:
        stats = summarize_group(g)
        rows.append({
            "station": station,
            "remark": remark,
            "month": month,
            **stats
        })

    out = pd.DataFrame(rows)
    out = out.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    return out


# =========================
# 5) UPH/CTeq 계산 (left/right/whole)
# =========================
def parallel_uph(ct_series: pd.Series) -> float:
    ct = ct_series.dropna()
    ct = ct[ct > 0]
    if len(ct) == 0:
        return np.nan
    return 3600.0 * (1.0 / ct).sum()


def build_final_df_86(summary_df: pd.DataFrame) -> pd.DataFrame:
    if summary_df is None or summary_df.empty:
        return pd.DataFrame(columns=["station", "remark", "month", "ct_eq", "uph", "final_ct"])

    b = summary_df[summary_df["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()
    b["del_out_op_ct_av"] = pd.to_numeric(b["del_out_op_ct_av"], errors="coerce")

    side_map = {"FCT1": "left", "FCT2": "left", "FCT3": "right", "FCT4": "right"}
    b["side"] = b["station"].map(side_map)

    grp_side = (
        b.groupby(["month", "remark", "side"], as_index=False)
         .agg(ct_list=("del_out_op_ct_av", lambda x: list(x)))
    )

    grp_side["uph"] = grp_side["ct_list"].apply(lambda lst: parallel_uph(pd.Series(lst)))
    grp_side["ct_eq"] = np.where(grp_side["uph"] > 0, 3600.0 / grp_side["uph"], np.nan)

    grp_side["uph"] = grp_side["uph"].round(2)
    grp_side["ct_eq"] = grp_side["ct_eq"].round(2)

    left_right_df = grp_side.rename(columns={"side": "station"})[
        ["station", "remark", "month", "ct_eq", "uph"]
    ].copy()
    left_right_df["final_ct"] = np.nan

    whole_df = left_right_df.groupby(["month", "remark"], as_index=False)["uph"].sum()
    whole_df["station"] = "whole"
    whole_df["ct_eq"] = np.nan
    whole_df["final_ct"] = np.where(
        whole_df["uph"] > 0,
        (3600.0 / whole_df["uph"]).round(2),
        np.nan
    )

    final_df = pd.concat(
        [
            left_right_df[["station", "remark", "month", "ct_eq", "uph", "final_ct"]],
            whole_df[["station", "remark", "month", "ct_eq", "uph", "final_ct"]],
        ],
        ignore_index=True
    )

    station_order = pd.CategoricalDtype(["left", "right", "whole"], ordered=True)
    final_df["station"] = final_df["station"].astype(station_order)
    final_df = final_df.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    return final_df


# =========================
# 6) 저장 (LATEST UPSERT + HIST 하루롤링 UPSERT)
# =========================
def upsert_latest_opct(df: pd.DataFrame):
    if df is None or df.empty:
        return
    df = _cast_df_for_db(df)

    cols = [
        "station", "remark", "month",
        "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
        "op_ct_upper_outlier", "del_out_op_ct_av", "plotly_json"
    ]
    df_load = df[cols].copy()

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            station, remark, month,
            sample_amount, op_ct_lower_outlier, q1, median, q3,
            op_ct_upper_outlier, del_out_op_ct_av, plotly_json,
            created_at, updated_at
        )
        VALUES (
            %(station)s, %(remark)s, %(month)s,
            %(sample_amount)s, %(op_ct_lower_outlier)s, %(q1)s, %(median)s, %(q3)s,
            %(op_ct_upper_outlier)s, %(del_out_op_ct_av)s, %(plotly_json)s,
            now(), now()
        )
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
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST))

    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.executemany(stmt.as_string(conn), df_load.to_dict(orient="records"))
                conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] upsert_latest_opct failed: {type(e).__name__}: {repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


def upsert_latest_whole(df: pd.DataFrame):
    if df is None or df.empty:
        return
    df = _cast_df_for_db(df)

    cols = ["station", "remark", "month", "ct_eq", "uph", "final_ct"]
    df_load = df[cols].copy()

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            station, remark, month,
            ct_eq, uph, final_ct,
            created_at, updated_at
        )
        VALUES (
            %(station)s, %(remark)s, %(month)s,
            %(ct_eq)s, %(uph)s, %(final_ct)s,
            now(), now()
        )
        ON CONFLICT (station, remark, month)
        DO UPDATE SET
            ct_eq      = EXCLUDED.ct_eq,
            uph        = EXCLUDED.uph,
            final_ct   = EXCLUDED.final_ct,
            updated_at = now();
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST))

    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.executemany(stmt.as_string(conn), df_load.to_dict(orient="records"))
                conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] upsert_latest_whole failed: {type(e).__name__}: {repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


def upsert_hist_opct_daily(df: pd.DataFrame, snapshot_day: str):
    if df is None or df.empty:
        return
    df = _cast_df_for_db(df)

    cols = [
        "station", "remark", "month",
        "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
        "op_ct_upper_outlier", "del_out_op_ct_av", "plotly_json"
    ]
    df_load = df[cols].copy()
    df_load.insert(0, "snapshot_day", snapshot_day)

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            snapshot_day, snapshot_ts,
            station, remark, month,
            sample_amount, op_ct_lower_outlier, q1, median, q3,
            op_ct_upper_outlier, del_out_op_ct_av, plotly_json
        )
        VALUES (
            %(snapshot_day)s, now(),
            %(station)s, %(remark)s, %(month)s,
            %(sample_amount)s, %(op_ct_lower_outlier)s, %(q1)s, %(median)s, %(q3)s,
            %(op_ct_upper_outlier)s, %(del_out_op_ct_av)s, %(plotly_json)s
        )
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
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST))

    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.executemany(stmt.as_string(conn), df_load.to_dict(orient="records"))
                conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] upsert_hist_opct_daily failed: {type(e).__name__}: {repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


def upsert_hist_whole_daily(df: pd.DataFrame, snapshot_day: str):
    if df is None or df.empty:
        return
    df = _cast_df_for_db(df)

    cols = ["station", "remark", "month", "ct_eq", "uph", "final_ct"]
    df_load = df[cols].copy()
    df_load.insert(0, "snapshot_day", snapshot_day)

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            snapshot_day, snapshot_ts,
            station, remark, month,
            ct_eq, uph, final_ct
        )
        VALUES (
            %(snapshot_day)s, now(),
            %(station)s, %(remark)s, %(month)s,
            %(ct_eq)s, %(uph)s, %(final_ct)s
        )
        ON CONFLICT (snapshot_day, station, remark, month)
        DO UPDATE SET
            snapshot_ts = now(),
            ct_eq       = EXCLUDED.ct_eq,
            uph         = EXCLUDED.uph,
            final_ct    = EXCLUDED.final_ct;
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST))

    while True:
        try:
            with psycopg2.connect(**DB_CONFIG) as conn:
                with conn.cursor() as cur:
                    cur.executemany(stmt.as_string(conn), df_load.to_dict(orient="records"))
                conn.commit()
            return
        except Exception as e:
            log(f"[DB][RETRY] upsert_hist_whole_daily failed: {type(e).__name__}: {repr(e)}", info="down")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 7) 작업 1회 실행(오늘 기준 월 전체) - 단일 프로세스
# =========================
def run_pipeline_once(label: str, run_month: str):
    snapshot_day = today_yyyymmdd()
    log(f"[RUN] {label} | snapshot_day={snapshot_day} | month={run_month} | START", info="info")

    # ✅ 루프 실행 직전에 엔진 상태 점검/복구
    eng = get_engine_blocking(DB_CONFIG)

    df_raw = load_month_df(eng, ALL_STATIONS, run_month)
    if df_raw is None or df_raw.empty:
        log(f"[RUN] {label} | no source rows (month={run_month}) -> skip save", info="info")
        return

    summary_df = build_summary_df(df_raw)
    whole_df = build_final_df_86(summary_df)

    upsert_latest_opct(summary_df)
    upsert_latest_whole(whole_df)
    upsert_hist_opct_daily(summary_df, snapshot_day=snapshot_day)
    upsert_hist_whole_daily(whole_df, snapshot_day=snapshot_day)

    log(f"[RUN] {label} | DONE (latest+hist upsert) | summary={len(summary_df):,} whole={len(whole_df):,}", info="info")


# =========================
# 8) main: 08:22 / 20:22에만 실행 후 종료
# =========================
def main():
    # 0) 로그 테이블 먼저 보장
    ensure_log_table()

    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}", info="start")
    log("=== SCHEDULE MODE: run at 08:22 and 20:22, then exit ===", info="info")
    log(f"work_mem={WORK_MEM} | engine pool_size=1 max_overflow=0", info="info")

    # 1) 대상 테이블/인덱스/시퀀스 보장 (DB 실패 시 무한 재시도)
    ensure_tables_and_indexes()

    # 2) 오늘 남은 실행 타임 계산
    now = datetime.now()
    run_times = _next_run_datetimes(now)
    if not run_times:
        log("[INFO] all scheduled runs for today already passed. exit.", info="info")
        return

    done = set()
    for target_dt in run_times:
        label = target_dt.strftime("%H:%M:%S")
        log(f"[WAIT] next_run={target_dt:%Y-%m-%d %H:%M:%S}", info="wait")
        _sleep_until(target_dt)

        run_month = current_yyyymm()

        # ✅ 스케줄 1회는 "성공할 때까지" 재시도
        while True:
            try:
                run_pipeline_once(label=label, run_month=run_month)
                done.add(label)
                break
            except Exception as e:
                if _is_connection_error(e):
                    log(f"[RUN][RETRY] {label} conn error -> rebuild engine: {type(e).__name__}: {repr(e)}", info="down")
                    _dispose_engine()
                    _ = get_engine_blocking(DB_CONFIG)
                else:
                    log(f"[RUN][RETRY] {label} failed: {type(e).__name__}: {repr(e)}", info="error")
                time.sleep(DB_RETRY_INTERVAL_SEC)

    log(f"[END] done={sorted(done)} | exit.", info="end")


if __name__ == "__main__":
    main()
