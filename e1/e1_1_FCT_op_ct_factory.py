# -*- coding: utf-8 -*-
"""
e1_1_FCT_op_ct_factory_schedule.py

[스케줄 실행 방식]
- 매일 08:22:00에 1회 실행 (오늘 데이터 전체 대상) -> 계산/UPSERT
- 매일 20:22:00에 1회 실행 (오늘 데이터 전체 대상) -> 계산/UPSERT
- 그 외 시간에는 대기만 함
- 2회 모두 완료되면 종료

[정책]
- LATEST: 키 기준 UPSERT (최신값 갱신)
- HIST  : 하루 1개 롤링
  * UNIQUE(snapshot_day, station, remark, month) 유지
  * INSERT = ON CONFLICT DO UPDATE (DO NOTHING 아님)

[id NULL 해결]
- id 컬럼 없으면 생성
- 시퀀스 없으면 생성
- id default nextval 강제
- 기존 NULL id는 nextval로 채움
- setval 동기화

[멀티프로세스]
- MP=2 고정
  * p1: FCT1,FCT2
  * p2: FCT3,FCT4

[기존 기능 유지]
- boxplot html 저장
- plotly_json 저장
- UPH(left/right/whole) 계산
- DataFrame 콘솔 출력 없음(로그만)
"""

import sys
import time
import urllib.parse
from datetime import datetime, date, time as dtime

from multiprocessing import get_context
from pathlib import Path
from typing import List, Dict, Tuple

import numpy as np
import pandas as pd
import plotly.express as px

import psycopg2
from psycopg2 import sql
from sqlalchemy import create_engine, text
from pandas.api.types import CategoricalDtype


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

# 스키마 대소문자 유지 (요구사항)
TARGET_SCHEMA = "e1_FCT_ct"

# 최신 갱신 테이블
TBL_OPCT_LATEST  = "fct_op_ct"
TBL_WHOLE_LATEST = "fct_whole_op_ct"

# 누적(하루 1개 롤링) 테이블
TBL_OPCT_HIST  = "fct_op_ct_hist"
TBL_WHOLE_HIST = "fct_whole_op_ct_hist"

# boxplot html 저장 폴더(기존 기능 유지)
OUT_DIR = Path("./fct_opct_boxplot_html")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# op_ct 필터
OPCT_MAX_SEC = 600

# 실행 스케줄
RUN_TIME = dtime(0, 10, 0)  # (legacy) monthly schedule trigger (no longer used)
LOOP_INTERVAL_SEC = 5  # 실시간 루프 간격(초)
FETCH_LIMIT = 200000   # 1회 조회 최대 row (과다 메모리/트래픽 방지)


# 멀티프로세스 분담
PROC_1_STATIONS = ["FCT1", "FCT2"]
PROC_2_STATIONS = ["FCT3", "FCT4"]


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


def is_monthly_run_time(now: datetime) -> bool:
    """매월 1일 00:10에만 실행(스케줄러 트리거 전용)."""
    return (now.day == 1) and (now.hour == 0) and (now.minute == 10)


def prev_month_yyyymm(now: datetime) -> str:
    """실행 시점(now)이 매월 1일일 때, '직전월(YYYYMM)'을 반환.
    예) 2026-02-01 실행 -> '202601'
        2026-01-01 실행 -> '202512'
    """
    y = now.year
    m = now.month
    if m == 1:
        return f"{y-1}12"
    return f"{y}{(m-1):02d}"


def get_engine(config=DB_CONFIG):
    pw = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{pw}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )
    return create_engine(conn_str, pool_pre_ping=True)


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


def _sleep_until(target_dt: datetime):
    """
    target_dt까지 대기. (초 단위 sleep)
    """
    while True:
        now = datetime.now()
        if now >= target_dt:
            return
        sec = (target_dt - now).total_seconds()
        # 너무 짧게 쪼개지 않도록 1~30초 정도로 슬립
        time.sleep(min(max(sec, 1.0), 30.0))


# =========================
# 2) 테이블/인덱스/ID 보정
# =========================
def ensure_schema(conn, schema_name: str):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    conn.commit()


def ensure_tables_and_indexes():
    """
    - 스키마 생성
    - 테이블 생성(없으면)
    - created_at/updated_at 컬럼 없으면 추가 (LATEST용)
    - UNIQUE 인덱스 보장
    - id 자동채번/NULL 보정
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_schema(conn, TARGET_SCHEMA)

        with conn.cursor() as cur:
            # -------------------------
            # LATEST: fct_op_ct
            # -------------------------
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

            # -------------------------
            # LATEST: fct_whole_op_ct
            # -------------------------
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

            # -------------------------
            # HIST: fct_op_ct_hist (하루 1개 롤링)
            # -------------------------
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

            # -------------------------
            # HIST: fct_whole_op_ct_hist (하루 1개 롤링)
            # -------------------------
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

    # id 자동채번/NULL 보정 (4개 테이블 모두)
    fix_id_sequence(TARGET_SCHEMA, TBL_OPCT_LATEST,  "fct_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_WHOLE_LATEST, "fct_whole_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_OPCT_HIST,    "fct_op_ct_hist_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_WHOLE_HIST,   "fct_whole_op_ct_hist_id_seq")

    log(f'[OK] target tables ensured in schema "{TARGET_SCHEMA}"')


def fix_id_sequence(schema: str, table: str, seq_name: str):
    """
    - id 컬럼 없으면 추가
    - 시퀀스 없으면 생성
    - id default nextval 강제
    - id NULL인 기존 행은 nextval로 채움
    - 시퀀스 setval = max(id)+1 로 동기화
    """
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
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(do_sql)
        conn.commit()


# =========================
# 3) 소스 로딩 (오늘 데이터 전체) + op_ct 계산
# =========================
def load_month_df(engine, stations: List[str], run_month: str) -> pd.DataFrame:
    """
    소스 로딩 (해당 월 전체 데이터) + op_ct 계산

    - run_month: 'YYYYMM'
    - end_day 컬럼 타입(TEXT/DATE 혼재 가능성을 고려하여 regexp_replace 기반 월 필터 적용
    """
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

    df = pd.read_sql(
        text(sql_query),
        engine,
        params={"stations": stations, "run_month": run_month},
    )

    if df.empty:
        return df

    df["end_day"] = pd.to_datetime(df["end_day"], errors="coerce").dt.date
    df["end_time_str"] = df["end_time"].astype(str)

    df["end_ts"] = pd.to_datetime(
        df["end_day"].astype(str) + " " + df["end_time_str"],
        errors="coerce",
    )

    df = df.dropna(subset=["end_ts"]).copy()
    df = df.sort_values(["station", "remark", "end_ts"], ascending=True).reset_index(drop=True)

    df["op_ct"] = df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    df["month"] = df["end_ts"].dt.strftime("%Y%m")

    return df

# =========================
# 4) Boxplot 요약 + plotly_json (기존 기능 유지)
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

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()


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

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()


def upsert_hist_opct_daily(df: pd.DataFrame, snapshot_day: str):
    """
    HIST: 하루 1개 롤링
    UNIQUE(snapshot_day, station, remark, month)
    => ON CONFLICT DO UPDATE
    """
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

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()


def upsert_hist_whole_daily(df: pd.DataFrame, snapshot_day: str):
    """
    HIST: 하루 1개 롤링
    UNIQUE(snapshot_day, station, remark, month)
    => ON CONFLICT DO UPDATE
    """
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

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()


# =========================
# 7) 작업 1회 실행(오늘 전체)
# =========================
def run_once_for_stations(stations: List[str], run_month: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    eng = get_engine(DB_CONFIG)

    df_raw = load_month_df(eng, stations, run_month)
    if df_raw is None or df_raw.empty:
        return (
            pd.DataFrame(columns=[
                "station", "remark", "month",
                "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
                "op_ct_upper_outlier", "del_out_op_ct_av", "plotly_json"
            ]),
            pd.DataFrame(columns=["station", "remark", "month", "ct_eq", "uph", "final_ct"])
        )

    summary_df = build_summary_df(df_raw)
    whole_df = build_final_df_86(summary_df)
    return summary_df, whole_df


def worker(stations: List[str], run_month: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    try:
        return run_once_for_stations(stations, run_month)
    except Exception as e:
        return (e, None)


def run_pipeline_once(label: str, run_month: str):
    """
    MP=2로 p1/p2 병렬 실행 후 결과 합쳐서 저장(UPSERT)
    """
    snapshot_day = today_yyyymmdd()

    log(f"[RUN] {label} | snapshot_day={snapshot_day} | START")

    ctx = get_context("spawn")
    with ctx.Pool(processes=2) as pool:
        tasks = [(PROC_1_STATIONS, run_month), (PROC_2_STATIONS, run_month)]
        results = pool.starmap(worker, tasks)

    # 에러 검사
    for r in results:
        if isinstance(r[0], Exception):
            raise r[0]

    # 결과 합치기
    summary_all = pd.concat([r[0] for r in results], ignore_index=True)
    whole_all = pd.concat([r[1] for r in results], ignore_index=True)

    # 저장 (latest upsert + hist 하루롤링 upsert)
    upsert_latest_opct(summary_all)
    upsert_latest_whole(whole_all)
    upsert_hist_opct_daily(summary_all, snapshot_day=snapshot_day)
    upsert_hist_whole_daily(whole_all, snapshot_day=snapshot_day)

    log(f"[RUN] {label} | DONE (latest+hist upsert)")


# =========================
# 8) main: 08:22 / 20:22 에만 실행 후 종료
# =========================
def main():
    start_dt = datetime.now()
    start_perf = time.perf_counter()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== REALTIME MODE: current-month incremental + periodic UPSERT ===")
    log(f"loop_interval={LOOP_INTERVAL_SEC}s | fetch_limit={FETCH_LIMIT}")

    # 0) 대상 테이블/인덱스/시퀀스 보장
    ensure_tables_and_indexes()

    # 1) 실시간 캐시/상태
    run_month = current_yyyymm()
    last_ts = {}  # key=(station, remark) -> last_end_ts (pd.Timestamp)
    cache_df = None

    def _key_cols(df: pd.DataFrame):
        if 'opct_fct' in ("opct_fct","opct_vision"):
            return ["station", "remark", "end_ts"]
        return ["station", "remark", "barcode_information", "end_day", "end_time"]

    def _fetch_new_rows(eng, stations: list[str], run_month_local: str) -> pd.DataFrame:
        min_last = None
        if last_ts:
            min_last = min([v for v in last_ts.values() if v is not None], default=None)

        if min_last is None:
            extra_where = ""
            params = {"stations": stations, "run_month": run_month_local, "limit": FETCH_LIMIT}
        else:
            extra_where = " AND ( (COALESCE(end_day::text,'') || ' ' || COALESCE(end_time::text,''))::timestamp > :min_last ) "
            params = {"stations": stations, "run_month": run_month_local, "min_last": str(min_last), "limit": FETCH_LIMIT}

        if 'opct_fct' in ("opct_fct","opct_vision"):
            sql_query = f"""
            SELECT station, remark, end_day, end_time
            FROM {SRC_SCHEMA}.{SRC_TABLE}
            WHERE station = ANY(:stations)
              AND remark IN ('PD','Non-PD')
              AND result <> 'FAIL'
              AND goodorbad <> 'BadFile'
              AND substring(regexp_replace(COALESCE(end_day::text,''), '\D', '', 'g') from 1 for 6) = :run_month
              {extra_where}
            ORDER BY station ASC, remark ASC, end_time ASC
            LIMIT :limit
            """
            df = pd.read_sql(text(sql_query), eng, params=params)
        else:
            sql_query = f"""
            SELECT station, remark, barcode_information, end_day, end_time, run_time
            FROM {SRC_SCHEMA}.{SRC_TABLE}
            WHERE station = ANY(:stations)
              AND remark IN ('PD','Non-PD')
              AND substring(regexp_replace(COALESCE(end_day::text,''), '\D', '', 'g') from 1 for 6) = :run_month
              {extra_where}
            ORDER BY station ASC, remark ASC, end_time ASC
            LIMIT :limit
            """
            df = pd.read_sql(text(sql_query), eng, params=params)

        if df is None or df.empty:
            return df

        df["end_day"] = pd.to_datetime(df["end_day"], errors="coerce").dt.date
        df["end_time_str"] = df["end_time"].astype(str)
        df["end_ts"] = pd.to_datetime(df["end_day"].astype(str) + " " + df["end_time_str"], errors="coerce")
        df = df.dropna(subset=["end_ts"]).copy()

        keep = []
        for (st, rm), g in df.groupby(["station","remark"], sort=False):
            lt = last_ts.get((st, rm))
            if lt is None:
                keep.append(g)
            else:
                keep.append(g[g["end_ts"] > lt])
        return pd.concat(keep, ignore_index=True) if keep else df.iloc[0:0].copy()

    def _update_last_ts(df_new: pd.DataFrame):
        if df_new is None or df_new.empty:
            return
        for (st, rm), g in df_new.groupby(["station","remark"], sort=False):
            last_ts[(st, rm)] = g["end_ts"].max()

    eng = get_engine(DB_CONFIG)
    try:
        while True:
            now = datetime.now()
            cur_month = current_yyyymm(now)
            if cur_month != run_month:
                log(f"[MONTH ROLLOVER] {run_month} -> {cur_month} (cache reset)")
                run_month = cur_month
                last_ts.clear()
                cache_df = None

            stations_all = list(set(PROC_1_STATIONS + PROC_2_STATIONS))
            df_new = _fetch_new_rows(eng, stations_all, run_month)

            if df_new is not None and not df_new.empty:
                if 'opct_fct' in ("opct_fct","opct_vision"):
                    # 앵커(각 station/remark 마지막 1행) + 신규로 op_ct 재계산
                    if cache_df is not None and not cache_df.empty:
                        anchors = []
                        for (st, rm), g in cache_df.groupby(["station","remark"], sort=False):
                            anchors.append(g.sort_values("end_ts").tail(1)[["station","remark","end_day","end_time","end_time_str","end_ts"]])
                        df_anchor = pd.concat(anchors, ignore_index=True) if anchors else None
                        df_tmp = pd.concat([df_anchor, df_new], ignore_index=True) if df_anchor is not None else df_new.copy()
                    else:
                        df_tmp = df_new.copy()

                    df_tmp = df_tmp.sort_values(["station","remark","end_ts"]).reset_index(drop=True)
                    df_tmp["op_ct"] = df_tmp.groupby(["station","remark"])["end_ts"].diff().dt.total_seconds()
                    df_tmp["month"] = df_tmp["end_ts"].dt.strftime("%Y%m")

                    df_new_for_cache = df_tmp[df_tmp["end_ts"].isin(df_new["end_ts"])].copy()
                else:
                    df_new["month"] = df_new["end_ts"].dt.strftime("%Y%m")
                    df_new_for_cache = df_new

                if cache_df is None or cache_df.empty:
                    cache_df = df_new_for_cache.copy()
                else:
                    cache_df = pd.concat([cache_df, df_new_for_cache], ignore_index=True)
                    cache_df = cache_df.drop_duplicates(subset=_key_cols(cache_df), keep="last")

                _update_last_ts(df_new)

                log(f"[NEW] rows={len(df_new_for_cache)} | cache={len(cache_df)} | month={run_month}")

                # 저장/업데이트: 기존 로직 재사용
                if 'opct_fct' == "opct_fct":
                    summary_df = build_summary_df(cache_df)
                    whole_df = build_final_df_86(summary_df)
                    upsert_latest_opct(summary_df)
                    upsert_latest_whole(whole_df)
                    upsert_hist_opct_daily(summary_df, snapshot_day=today_yyyymmdd())
                    upsert_hist_whole_daily(whole_df, snapshot_day=today_yyyymmdd())
                elif 'opct_fct' == "runtime_fct":
                    summary_df = build_summary_df(cache_df)
                    outlier_df = build_upper_outlier_df(cache_df, summary_df)
                    upsert_latest_run_time_ct(summary_df)
                    upsert_hist_run_time_ct_daily(summary_df, snapshot_day=today_yyyymmdd())
                    insert_outlier_list(outlier_df)
                elif 'opct_fct' == "opct_vision":
                    df_marked = mark_only_runs(cache_df)
                    df_an = build_analysis_df(df_marked)
                    summary_df = summarize(df_an) if df_an is not None and not df_an.empty else pd.DataFrame()
                    if summary_df is not None and not summary_df.empty:
                        upsert_latest(summary_df)
                        upsert_hist_daily(summary_df, snapshot_day=today_yyyymmdd())
                elif 'opct_fct' == "runtime_vision":
                    summary_df = build_summary(cache_df)
                    if summary_df is not None and not summary_df.empty:
                        upsert_latest(summary_df)
                        upsert_hist_daily(summary_df, snapshot_day=today_yyyymmdd())

            time.sleep(LOOP_INTERVAL_SEC)

    finally:
        try:
            eng.dispose()
        except Exception:
            pass

if __name__ == "__main__":
    main()
