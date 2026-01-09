# -*- coding: utf-8 -*-
"""
FCT RunTime CT 분석 파이프라인 (iqz step 전용) - FACTORY SCHEDULE FINAL

[Source]
- a2_fct_table.fct_table

[Filter]
- station: FCT1~4 (MP=2 분할)
- barcode_information LIKE 'B%'
- result <> 'FAIL'
- remark=PD     -> step_description='1.36 Test iqz(uA)'
- remark=Non-PD -> step_description='1.32 Test iqz(uA)'

[Schedule]
- 매일 08:22:00에 1회 실행(오늘 데이터 전체 대상) -> 계산/UPSERT
- 매일 20:22:00에 1회 실행(오늘 데이터 전체 대상) -> 계산/UPSERT
- 남은 스케줄 없으면 종료
- 두 번 실행 완료되면 종료

[Output 정책(지금까지 반영한 것과 동일)]
1) LATEST(summary): e1_FCT_ct.fct_run_time_ct
   - UNIQUE (station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE (최신값으로 갱신)
   - created_at/updated_at 포함

2) HIST(summary): e1_FCT_ct.fct_run_time_ct_hist
   - UNIQUE (snapshot_day, station, remark, month) 유지
   - INSERT = ON CONFLICT DO UPDATE => “그날 마지막 값”만 남김(하루 1개 롤링)
   - snapshot_ts 포함

3) OUTLIER(detail): e1_FCT_ct.fct_upper_outlier_ct_list
   - UNIQUE (station, remark, barcode_information, end_day, end_time)
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
- 실행 시작/종료 시각 및 총 소요 시간 출력
"""

import sys
import time
import urllib.parse
from datetime import datetime, date, time as dtime
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

# 스케줄 (하루 2회)
RUN_TIME = dtime(0, 10, 0)  # 매월 1일 00:10:00 (Task Scheduler)
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

def _next_run_datetimes(now_dt: datetime) -> List[datetime]:
    d = now_dt.date()
    cands = [
        datetime(d.year, d.month, d.day, RUN_TIME_1.hour, RUN_TIME_1.minute, RUN_TIME_1.second),
        datetime(d.year, d.month, d.day, RUN_TIME_2.hour, RUN_TIME_2.minute, RUN_TIME_2.second),
    ]
    return [t for t in cands if t >= now_dt]

def _sleep_until(target_dt: datetime):
    while True:
        now = datetime.now()
        if now >= target_dt:
            return
        sec = (target_dt - now).total_seconds()
        time.sleep(min(max(sec, 1.0), 30.0))


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
# 3) 로딩 + 전처리 (오늘 데이터만, station subset)
# =========================
def load_source_month(engine, stations: List[str], run_month: str) -> pd.DataFrame:
    """
    소스 로딩 (해당 월 전체 데이터, station subset)

    - run_month: 'YYYYMM'
    - end_day 컬럼 타입(TEXT/DATE 혼재 가능성 대비
    """
    query = f"""
    SELECT
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
    df_raw = load_source_month(eng, stations=stations, run_month=run_month)
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


def run_pipeline_once(label: str, run_month: str):
    """
    MP=2 병렬 실행 후 결과 합쳐서 저장
    """
    snapshot_day = today_yyyymmdd()
    log(f"[RUN] {label} | snapshot_day={snapshot_day} | START")

    ctx = get_context("spawn")
    with ctx.Pool(processes=2) as pool:
        tasks = [(PROC_1_STATIONS, run_month), (PROC_2_STATIONS, run_month)]
        results = pool.starmap(worker, tasks)

    summary_all = pd.concat([r[0] for r in results], ignore_index=True)
    outlier_all = pd.concat([r[1] for r in results], ignore_index=True)

    # 저장: latest upsert + hist 하루롤링 upsert + outlier append
    upsert_latest_run_time_ct(summary_all)
    upsert_hist_run_time_ct_daily(summary_all, snapshot_day=snapshot_day)
    insert_outlier_list(outlier_all)

    log(f"[RUN] {label} | DONE (latest+hist upsert, outlier append)")


# =========================
# 8) main: 08:22 / 20:22 에만 실행 후 종료
# =========================
def main():
    start_dt = datetime.now()
    start_perf = time.perf_counter()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== FCT RunTime CT Pipeline (MONTHLY: 1st day 00:10) ===")

    try:
        now = datetime.now()
        if not is_monthly_run_time(now):
            log("[SKIP] 이 스크립트는 Task Scheduler가 매월 1일 00:10에 호출할 때만 실행합니다.")
            return

        run_month = prev_month_yyyymm(now)

        log("[0/3] target table ensure...")
        ensure_tables_and_indexes()

        log("[1/3] monthly compute+upsert...")
        run_pipeline_once(label=f"monthly_run_{run_month}", run_month=run_month)

        end_dt = datetime.now()
        elapsed = time.perf_counter() - start_perf
        log(f"[END] {end_dt:%Y-%m-%d %H:%M:%S} | elapsed={elapsed:.1f}s")

    except Exception as e:
        log(f"[FATAL] {e}")
        raise


if __name__ == "__main__":
    main()
