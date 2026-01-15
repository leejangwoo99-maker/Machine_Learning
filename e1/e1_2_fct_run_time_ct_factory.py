# -*- coding: utf-8 -*-
"""
FCT RunTime CT 분석 파이프라인 (iqz step 전용) - FACTORY DAEMON (file_path PK)

[Source]
- a2_fct_table.fct_table

[Filter]
- station: FCT1~4 (MP=2 분할)
- barcode_information LIKE 'B%'
- result <> 'FAIL'
- remark=PD     -> step_description='1.36 Test iqz(uA)'
- remark=Non-PD -> step_description='1.32 Test iqz(uA)'

[Daemon]
- 1초마다 무한 루프
- 신규 데이터만 캐시에 추가 (중복 방지 PK = file_path 단독)
- 월 롤오버(YYYYMM 변경) 시 캐시/상태 리셋

[Output 정책(기존 유지)]
1) LATEST(summary): e1_FCT_ct.fct_run_time_ct
   - UNIQUE (station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE (최신값으로 갱신)
   - created_at/updated_at 포함

2) HIST(summary): e1_FCT_ct.fct_run_time_ct_hist
   - UNIQUE (snapshot_day, station, remark, month) 유지
   - INSERT = ON CONFLICT DO UPDATE => “그날 마지막 값”만 남김(하루 1개 롤링)
   - snapshot_ts 포함

3) OUTLIER(detail): e1_FCT_ct.fct_upper_outlier_ct_list
   - 기존 UNIQUE(station, remark, barcode_information, end_day, end_time) 유지 (요구사항 그대로)
   - INSERT = ON CONFLICT DO NOTHING (누적 적재 + 중복 방지)

[id NULL 해결]
- latest/hist/outlier 테이블 모두 id 자동채번 보정
  * id 컬럼 없으면 생성
  * 시퀀스 없으면 생성
  * id default nextval 강제
  * 기존 NULL id는 nextval로 채움
  * setval 동기화

[요구사항]
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 콘솔 대기(종료 방지)
"""

import time
import urllib.parse
from datetime import datetime, date
from multiprocessing import get_context
from typing import List, Tuple

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text

import plotly.graph_objects as go
import plotly.io as pio

import psycopg2
from psycopg2.extras import execute_values


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

SRC_SCHEMA = "a2_fct_table"
SRC_TABLE  = "fct_table"

TARGET_SCHEMA = "e1_FCT_ct"

# LATEST(summary)
TBL_RUN_TIME_LATEST = "fct_run_time_ct"
# HIST(summary) 하루 1개 롤링
TBL_RUN_TIME_HIST   = "fct_run_time_ct_hist"
# OUTLIER(detail) 누적 적재
TBL_OUTLIER_LIST    = "fct_upper_outlier_ct_list"

# 데몬 루프
LOOP_INTERVAL_SEC = 1         # 1초마다 무한 루프
FETCH_LIMIT = 200000          # 1회 조회 최대 row (과다 메모리/트래픽 방지)

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

def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str, pool_pre_ping=True)

def get_conn_psycopg2(config=DB_CONFIG):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )

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


# =========================
# 2) 테이블/인덱스/ID 보정
# =========================
def ensure_tables_and_indexes():
    """
    - 스키마 생성
    - LATEST/HIST/OUTLIER 테이블 생성(없으면)
    - UNIQUE 인덱스 보장
    - id 자동채번/NULL 보정
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            # schema
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

            # -------------------------
            # LATEST(summary)
            # -------------------------
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
            # 기존 테이블이 이미 존재하면 CREATE TABLE IF NOT EXISTS로는 컬럼 추가가 되지 않으므로,
            # 과거 버전 테이블(컬럼 누락)을 위해 ALTER로 보정한다.
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

            # -------------------------
            # HIST(summary) 하루 1개 롤링
            # -------------------------
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

            # -------------------------
            # OUTLIER(detail) 누적 적재
            # (요구사항 유지: UNIQUE는 기존 그대로)
            # -------------------------
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

    # id 자동채번/NULL 보정 (3개 테이블 모두)
    fix_id_sequence(TARGET_SCHEMA, TBL_RUN_TIME_LATEST, "fct_run_time_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_RUN_TIME_HIST,   "fct_run_time_ct_hist_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_OUTLIER_LIST,    "fct_upper_outlier_ct_list_id_seq")

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
# 3) 로딩 + 전처리 (월 필터, station subset)
#    (중복 방지 PK = file_path 단독)
# =========================
def load_source_month(engine, stations: List[str], run_month: str) -> pd.DataFrame:
    """
    소스 로딩 (해당 월 전체 데이터, station subset)

    - file_path 포함 (PK)
    - end_day 컬럼 타입(TEXT/DATE 혼재 가능성 대비
    """
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
    """

    df = pd.read_sql(text(query), engine, params={"stations": stations, "run_month": run_month})

    if df is None or df.empty:
        return df

    # file_path 필수
    df["file_path"] = df["file_path"].astype(str).str.strip()
    df = df[df["file_path"].notna() & (df["file_path"] != "")].copy()
    if df.empty:
        return df

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(8)
    df["end_time"] = df["end_time"].astype(str).str.strip()

    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["run_time"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] run_time NaN 제거: {dropped} rows drop")

    df["month"] = df["end_day"].str.slice(0, 6)
    df = df.sort_values(["end_day", "end_time"], ascending=[True, True]).reset_index(drop=True)
    return df


# =========================
# 4) 요약 생성 (boxplot 통계 + plotly_json)
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


# =========================
# 5) Upper outlier 상세 DF 생성
# =========================
def build_upper_outlier_df(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    if df_raw is None or df_raw.empty or summary_df is None or summary_df.empty:
        return pd.DataFrame()

    upper_outlier_rows = []
    total = len(summary_df)

    for i, row in summary_df.iterrows():
        if (i + 1) % 20 == 0 or i == 0 or (i + 1) == total:
            log(f"[PROGRESS] outlier scan {i+1}/{total} ...")

        station = row["station"]
        remark  = row["remark"]
        month   = row["month"]

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
# 6) DB 저장 (LATEST UPSERT + HIST 하루롤링 UPSERT + OUTLIER append)
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

    conn = get_conn_psycopg2(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=1000)
        conn.commit()
    finally:
        conn.close()


def upsert_hist_run_time_ct_daily(summary_df: pd.DataFrame, snapshot_day: str):
    """
    HIST: UNIQUE(snapshot_day, station, remark, month) 유지
    => ON CONFLICT DO UPDATE (하루 1개 롤링)
    """
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

    conn = get_conn_psycopg2(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=1000)
        conn.commit()
    finally:
        conn.close()


def insert_outlier_list(upper_outlier_df: pd.DataFrame):
    """
    OUTLIER LIST: 누적 적재 + 중복키 방지
    UNIQUE(station, remark, barcode_information, end_day, end_time)
    ON CONFLICT DO NOTHING
    """
    if upper_outlier_df is None or upper_outlier_df.empty:
        log("[SKIP] upper outlier 저장 생략 (데이터 없음)")
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

    conn = get_conn_psycopg2(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=2000)
        conn.commit()
        log(f"[OK] upper outlier 누적 저장 완료: insert candidates={len(rows)} (중복키는 PASS)")
    finally:
        conn.close()


# =========================
# 7) MP Worker (station subset)
# =========================
def worker(stations: List[str], run_month: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    returns: (summary_df, outlier_df)
    """
    eng = get_engine(DB_CONFIG)
    try:
        df_raw = load_source_month(eng, stations=stations, run_month=run_month)
    finally:
        try:
            eng.dispose()
        except Exception:
            pass

    if df_raw is None or df_raw.empty:
        return (
            pd.DataFrame(columns=[
                "station", "remark", "month", "sample_amount",
                "run_time_lower_outlier", "q1", "median", "q3",
                "run_time_upper_outlier", "del_out_run_time_av", "plotly_json"
            ]),
            pd.DataFrame(columns=[
                "station", "remark", "barcode_information", "end_day", "end_time",
                "run_time", "month", "upper_threshold"
            ])
        )

    summary_df = build_summary_df(df_raw)
    outlier_df = build_upper_outlier_df(df_raw, summary_df)
    return summary_df, outlier_df


# =========================
# 8) 데몬: 신규 행만 캐시에 누적 (PK=file_path)
# =========================
def fetch_new_rows_by_file_path(engine, stations: List[str], run_month: str, seen_file_paths: set[str]) -> pd.DataFrame:
    """
    - run_month 범위 내에서 조회
    - file_path 기준으로 '미처리(seen_file_paths에 없는 것)'만 반환
    - 반환 df는 run_time NaN 제거/정규화 수행
    """
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
    df = pd.read_sql(
        text(query),
        engine,
        params={"stations": stations, "run_month": run_month, "limit": FETCH_LIMIT},
    )

    if df is None or df.empty:
        return df

    df["file_path"] = df["file_path"].astype(str).str.strip()
    df = df[df["file_path"].notna() & (df["file_path"] != "")].copy()
    if df.empty:
        return df

    # 신규만 필터 (PK=file_path 단독)
    df = df[~df["file_path"].isin(seen_file_paths)].copy()
    if df.empty:
        return df

    # 전처리
    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(8)
    df["end_time"] = df["end_time"].astype(str).str.strip()

    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    before = len(df)
    df = df.dropna(subset=["run_time"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] run_time NaN 제거: {dropped} rows drop (new rows)")

    if df.empty:
        return df

    df["month"] = df["end_day"].str.slice(0, 6)
    df = df.sort_values(["end_day", "end_time"], ascending=[True, True]).reset_index(drop=True)
    return df


def recompute_and_upsert(cache_df: pd.DataFrame):
    """
    캐시 전체로 summary/outlier 재계산 후 UPSERT/INSERT 수행
    """
    if cache_df is None or cache_df.empty:
        return

    snapshot_day = today_yyyymmdd()
    summary_df = build_summary_df(cache_df)
    outlier_df = build_upper_outlier_df(cache_df, summary_df)

    upsert_latest_run_time_ct(summary_df)
    upsert_hist_run_time_ct_daily(summary_df, snapshot_day=snapshot_day)
    insert_outlier_list(outlier_df)


# =========================
# 9) main: 1초 무한루프 데몬 + 콘솔 대기
# =========================
def main():
    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S} | DAEMON MODE (PK=file_path)")
    log(f"loop_interval={LOOP_INTERVAL_SEC}s | fetch_limit={FETCH_LIMIT}")
    log(f"src={SRC_SCHEMA}.{SRC_TABLE} -> target schema={TARGET_SCHEMA}")

    ensure_tables_and_indexes()

    stations_all = list(set(PROC_1_STATIONS + PROC_2_STATIONS))
    run_month = current_yyyymm()

    # 캐시 및 중복 방지 집합(PK=file_path)
    cache_df: pd.DataFrame | None = None
    seen_file_paths: set[str] = set()

    eng = get_engine(DB_CONFIG)

    try:
        while True:
            now = datetime.now()
            cur_month = current_yyyymm(now)

            # 월 롤오버: 캐시/상태 리셋
            if cur_month != run_month:
                log(f"[MONTH ROLLOVER] {run_month} -> {cur_month} (cache reset)")
                run_month = cur_month
                cache_df = None
                seen_file_paths.clear()

            df_new = fetch_new_rows_by_file_path(
                eng,
                stations=stations_all,
                run_month=run_month,
                seen_file_paths=seen_file_paths,
            )

            if df_new is not None and not df_new.empty:
                # seen set 업데이트
                new_paths = df_new["file_path"].astype(str).tolist()
                for p in new_paths:
                    seen_file_paths.add(p)

                # 캐시에 누적 (PK=file_path 단독 dedup)
                if cache_df is None or cache_df.empty:
                    cache_df = df_new.copy()
                else:
                    cache_df = pd.concat([cache_df, df_new], ignore_index=True)
                    cache_df = cache_df.drop_duplicates(subset=["file_path"], keep="last").reset_index(drop=True)

                log(f"[NEW] rows={len(df_new)} | cache={len(cache_df)} | seen={len(seen_file_paths)} | month={run_month}")

                # 재계산 + 저장
                recompute_and_upsert(cache_df)

            time.sleep(LOOP_INTERVAL_SEC)

    except KeyboardInterrupt:
        log("[STOP] KeyboardInterrupt received. exiting loop...")
    finally:
        try:
            eng.dispose()
        except Exception:
            pass

        end_dt = datetime.now()
        elapsed = (end_dt - start_dt).total_seconds()
        log(f"[END] {end_dt:%Y-%m-%d %H:%M:%S} | elapsed={elapsed:.1f}s")

        # 콘솔 자동 종료 방지(대기)
        try:
            input("Press Enter to close this window...")
        except Exception:
            pass


if __name__ == "__main__":
    main()