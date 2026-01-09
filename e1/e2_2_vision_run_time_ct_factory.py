# -*- coding: utf-8 -*-
"""
Vision RunTime CT (intensity8) 분석 파이프라인 - SCHEDULE(08:22/20:22) + MP=2 고정 + LATEST/HIST 저장

[Source]
- a3_vision_table.vision_table

[Filter(기본)]
- barcode_information LIKE 'B%'
- station IN ('Vision1','Vision2')
- remark IN ('PD','Non-PD')
- step_description = 'intensity8'
- result <> 'FAIL'
- end_day: "현재 날짜(오늘)" 데이터 전체 대상
- end_day: "현재 달(YYYYMM)" 제한 유지

[Summary (station, remark, month)]
- sample_amount
- IQR 기반 outlier 범위 문자열
- q1/median/q3
- outlier 제거 평균(del_out_run_time_av)
- plotly_json (boxplot, validate=False)

[Save 정책] (e1/e2 통일)
1) LATEST: e2_vision_ct.vision_run_time_ct
   - UNIQUE (station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE (최신값 갱신)
   - created_at/updated_at 포함

2) HIST(하루 1개 롤링): e2_vision_ct.vision_run_time_ct_hist
   - UNIQUE (snapshot_day, station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE (그날 마지막 값만 유지)
   - snapshot_ts 포함

[id NULL 해결]
- latest/hist 모두 id 자동채번 보정(컬럼/시퀀스/default/NULL 채움/setval)

[Schedule]
- 08:22:00, 20:22:00 에만 실행
- 트리거 시점에 "오늘 데이터 전체" 대상으로 계산/UPSERT 후 종료
- 오늘 남은 스케줄이 없으면 즉시 종료
"""

import os
import sys
import urllib.parse
from datetime import datetime, date, time as dtime
import time as time_mod
from multiprocessing import get_context

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import plotly.graph_objects as go
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

SRC_SCHEMA = "a3_vision_table"
SRC_TABLE  = "vision_table"

TARGET_SCHEMA = "e2_vision_ct"

# LATEST/HIST 테이블
TBL_LATEST = "vision_run_time_ct"
TBL_HIST   = "vision_run_time_ct_hist"

STEP_DESC = "intensity8"

# =========================
# MP=2 고정 (Vision1 / Vision2 분담)
# =========================
PROC_1_STATIONS = ["Vision1"]
PROC_2_STATIONS = ["Vision2"]

# =========================
# 스케줄 (하루 2회만)
# =========================
RUN_TIME = dtime(0, 10, 0)  # 매월 1일 00:10:00 (Task Scheduler)
# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")

def current_yyyymm() -> str:
    return datetime.now().strftime("%Y%m")


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

def get_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str, pool_pre_ping=True)

def get_conn_pg(cfg=DB_CONFIG):
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )

def _make_plotly_json(values: np.ndarray, name: str) -> str:
    """
    EXE(onefile)에서 plotly validators 오류 방지 -> validate=False
    """
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), name=name, boxpoints=False))
    return fig.to_json(validate=False)

def _next_run_datetimes(now_dt: datetime):
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
        time_mod.sleep(min(max(sec, 1.0), 30.0))


# =========================
# 2) 테이블/인덱스/ID 보정
# =========================
def ensure_tables_and_indexes():
    """
    - 스키마 생성
    - LATEST/HIST 테이블 생성(없으면)
    - UNIQUE 인덱스 보장
    - id 자동채번/NULL 보정
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

            # LATEST
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_LATEST} (
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
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_LATEST}_key
                ON "{TARGET_SCHEMA}".{TBL_LATEST} (station, remark, month);
            """)

            # HIST (하루 1개 롤링)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_HIST} (
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
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_HIST}_day_key
                ON "{TARGET_SCHEMA}".{TBL_HIST} (snapshot_day, station, remark, month);
            """)

        conn.commit()

    # id 자동채번/NULL 보정
    fix_id_sequence(TARGET_SCHEMA, TBL_LATEST, "vision_run_time_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_HIST,   "vision_run_time_ct_hist_id_seq")

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
# 3) 로딩 (현재달 + 오늘 데이터 전체, station subset)
# =========================
def load_source_month(engine, stations: list, run_month: str) -> pd.DataFrame:
    """
    로딩 + 전처리 (해당 월 전체, station subset)

    - run_month: 'YYYYMM'
    - 기존의 현재달/오늘 제한 중 '오늘' 제한을 제거하여 월 전체로 확장
    """
    q = text(f"""
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
    """)

    df = pd.read_sql(q, engine, params={"stations": stations, "run_month": run_month, "step_desc": STEP_DESC})

    if df.empty:
        return df

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["run_time"]).copy()
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] run_time NaN 제거: {dropped} rows")

    # end_dt 생성(정렬 안정용)
    dt_str = df["end_day"] + " " + df["end_time"].astype(str).str.strip()
    df["end_dt"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")
    before_dt = len(df)
    df = df.dropna(subset=["end_dt"]).copy()
    dropped_dt = before_dt - len(df)
    if dropped_dt:
        log(f"[INFO] end_dt 파싱 실패 drop: {dropped_dt} rows")

    df = df.sort_values(["end_dt", "station", "remark"]).reset_index(drop=True)
    df["month"] = df["end_dt"].dt.strftime("%Y%m")

    return df


# =========================
# 4) 요약 DF 생성
# =========================
def _summary_from_group(station: str, remark: str, month: str, series: pd.Series) -> dict:
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
    log(f"[INFO] 그룹 수 = {total}")

    for i, ((st, rk, mo), g) in enumerate(groups, start=1):
        if i == 1 or i == total or i % 20 == 0:
            log(f"[PROGRESS] group {i}/{total} ... ({st},{rk},{mo})")
        r = _summary_from_group(st, rk, mo, g["run_time"])
        if r is not None:
            rows.append(r)

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["month", "station", "remark"]).reset_index(drop=True)
    return out


# =========================
# 5) 저장: LATEST UPSERT + HIST 하루롤링 UPSERT
# =========================
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

    conn = get_conn_pg(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=2000)
        conn.commit()
    finally:
        conn.close()


def upsert_hist_daily(summary_df: pd.DataFrame, snapshot_day: str):
    """
    UNIQUE(snapshot_day, station, remark, month) 유지
    ON CONFLICT DO UPDATE 로 하루 1개 롤링
    """
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

    conn = get_conn_pg(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=2000)
        conn.commit()
    finally:
        conn.close()


# =========================
# 6) MP Worker / pipeline
# =========================
def worker(stations: list, run_month: str):
    eng = get_engine(DB_CONFIG)
    df = load_source_month(eng, stations=stations, run_month=run_month)
    if df is None or df.empty:
        return pd.DataFrame()

    summary_df = build_summary(df)
    return summary_df


def run_pipeline_once(label: str, run_month: str):
    snapshot_day = today_yyyymmdd()
    log(f"[RUN] {label} | snapshot_day={snapshot_day} | START")

    ctx = get_context("spawn")
    with ctx.Pool(processes=2) as pool:
        tasks = [(PROC_1_STATIONS, run_month), (PROC_2_STATIONS, run_month)]
        results = pool.starmap(worker, tasks)

    summary_all = pd.concat(results, ignore_index=True)

    if summary_all is None or summary_all.empty:
        log("[SKIP] summary 없음 -> 저장 생략")
        return

    upsert_latest(summary_all)
    upsert_hist_daily(summary_all, snapshot_day=snapshot_day)

    log(f"[RUN] {label} | DONE (latest upsert + hist daily rolling)")


# =========================
# main: 08:22 / 20:22 에만 실행 후 종료
# =========================
def main():
    start_dt = datetime.now()
    start_perf = time.perf_counter()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== Vision RunTime CT (intensity8) Pipeline (MONTHLY: 1st day 00:10) ===")

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
