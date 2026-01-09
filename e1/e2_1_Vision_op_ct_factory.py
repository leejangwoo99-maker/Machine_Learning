# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인 - SCHEDULE(08:22/20:22) + MP=2 고정 + LATEST/HIST 저장

[Source]
- a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history

[Filter]
- station IN ('Vision1','Vision2')
- result != 'FAIL' (NULL 포함 대비 COALESCE)
- goodorbad != 'BadFile' (NULL 포함 대비 COALESCE)
- end_day가 "현재 날짜 기준의 달(YYYYMM)"만

[Logic] (기존 유지)
- end_dt 생성 후 정렬
- station 변경 시 run_id 증가, run_len >= 10 => vision_only_run
- (station, remark)별 op_ct 계산
- op_ct NaN 제거 + op_ct<=600만 분석
- 정상군: vision_only_run 제외
- only군: vision_only_run만 (Vision1_only / Vision2_only로 라벨링)
- (station_out, remark, month) 단위 boxplot 통계 + plotly_json 생성

[Save 정책] (e1 정책과 동일하게 반영)
1) LATEST: e2_vision_ct.vision_op_ct
   - UNIQUE (station, remark, month)
   - INSERT = ON CONFLICT DO UPDATE (최신값 갱신)
   - created_at/updated_at 포함

2) HIST(하루 1개 롤링): e2_vision_ct.vision_op_ct_hist
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
import warnings
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
# Thread 제한(원 코드 유지)
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
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e2_vision_ct"   # 소문자 고정

# LATEST/HIST 테이블
TBL_LATEST = "vision_op_ct"
TBL_HIST   = "vision_op_ct_hist"

OPCT_MAX_SEC = 600
ONLY_RUN_MIN_LEN = 10

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

def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str, pool_pre_ping=True)

def get_conn_pg(config=DB_CONFIG):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )

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
    # validate=False 유지(원 코드 안정성)
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), boxpoints=False, name=title))
    fig.update_layout(title=title, showlegend=False)
    return fig.to_json(validate=False)

def fmt_range(a: float, b: float) -> str:
    return f"{a:.2f}~{b:.2f}"

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
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_LATEST}_key
                ON "{TARGET_SCHEMA}".{TBL_LATEST} (station, remark, month);
            """)

            # HIST (하루 1개 롤링)
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
            cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_HIST}_day_key
                ON "{TARGET_SCHEMA}".{TBL_HIST} (snapshot_day, station, remark, month);
            """)

        conn.commit()

    # id 자동채번/NULL 보정
    fix_id_sequence(TARGET_SCHEMA, TBL_LATEST, "vision_op_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_HIST,   "vision_op_ct_hist_id_seq")

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
# 3) 로딩 + 전처리 (현재달 + 오늘 데이터 전체, station subset)
# =========================
def load_source_month(engine, stations: list, run_month: str) -> pd.DataFrame:
    """
    로딩 + 전처리 (해당 월 전체, station subset)

    - run_month: 'YYYYMM'
    - 기존의 현재달 제한 로직은 유지하되, today(YYYYMMDD) 제한은 제거하여 월 전체로 확장
    """
    q = text(f"""
    SELECT
        station,
        remark,
        end_day,
        end_time,
        result,
        goodorbad
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE station = ANY(:stations)
      AND COALESCE(result,'') <> 'FAIL'
      AND COALESCE(goodorbad,'') <> 'BadFile'
      AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
    ORDER BY end_day ASC, end_time ASC
    """)

    df = pd.read_sql(q, engine, params={"stations": stations, "run_month": run_month})

    if df.empty:
        return df

    # end_dt 생성
    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    dt_str = df["end_day"] + " " + df["end_time"].astype(str).str.strip()

    df["end_dt"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")
    before = len(df)
    df = df.dropna(subset=["end_dt"]).copy()
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] end_dt 파싱 실패 drop: {dropped} rows")

    df = df.sort_values(["end_dt", "station", "remark"]).reset_index(drop=True)
    df["month"] = df["end_dt"].dt.strftime("%Y%m")
    return df


# =========================
# 4) run_id / only_run 판정
# =========================
def mark_only_runs(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    if out.empty:
        out["run_id"] = []
        out["run_len"] = []
        out["is_vision_only_run"] = []
        return out

    out["run_id"] = (out["station"] != out["station"].shift(1)).cumsum()
    run_sizes = out.groupby("run_id")["station"].size().rename("run_len")
    out = out.merge(run_sizes, on="run_id", how="left")
    out["is_vision_only_run"] = out["run_len"] >= ONLY_RUN_MIN_LEN
    return out


# =========================
# 5) op_ct 계산 + 분석 DF 생성
# =========================
def build_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    d = df.sort_values(["station", "remark", "end_dt"]).copy()
    d["op_ct"] = d.groupby(["station", "remark"])["end_dt"].diff().dt.total_seconds()

    df_an = d.dropna(subset=["op_ct"]).copy()
    df_an = df_an[df_an["op_ct"] <= OPCT_MAX_SEC].copy()
    return df_an


# =========================
# 6) 요약 생성
# =========================
def summarize(df_an: pd.DataFrame) -> pd.DataFrame:
    if df_an is None or df_an.empty:
        return pd.DataFrame()

    df_normal = df_an[df_an["is_vision_only_run"] == False].copy()
    df_only   = df_an[df_an["is_vision_only_run"] == True].copy()

    tasks = []

    # 정상군
    if not df_normal.empty:
        dn = df_normal.copy()
        dn["station_out"] = dn["station"]
        for (st, rk, mo), g in dn.groupby(["station_out", "remark", "month"], sort=True):
            op_list = g["op_ct"].dropna().astype(float).to_numpy()
            if op_list.size == 0:
                continue
            tasks.append((st, rk, mo, op_list))

    # only군
    if not df_only.empty:
        do = df_only.copy()
        do["station_out"] = (
            do["station"].map({"Vision1": "Vision1_only", "Vision2": "Vision2_only"})
            .fillna(do["station"])
        )
        for (st, rk, mo), g in do.groupby(["station_out", "remark", "month"], sort=True):
            op_list = g["op_ct"].dropna().astype(float).to_numpy()
            if op_list.size == 0:
                continue
            tasks.append((st, rk, mo, op_list))

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

        del_out_mean = (
            round(stats["del_out_mean"], 2)
            if not np.isnan(stats["del_out_mean"])
            else np.nan
        )

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

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    out = out.sort_values(["remark", "station", "month"]).reset_index(drop=True)
    return out


# =========================
# 7) 저장: LATEST UPSERT + HIST 하루롤링 UPSERT
# =========================
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
# 8) MP Worker / pipeline
# =========================
def worker(stations: list, run_month: str):
    eng = get_engine(DB_CONFIG)
    df = load_source_month(eng, stations=stations, run_month=run_month)
    if df is None or df.empty:
        return pd.DataFrame()

    df = mark_only_runs(df)
    df_an = build_analysis_df(df)
    if df_an is None or df_an.empty:
        return pd.DataFrame()

    summary_df = summarize(df_an)
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
    log("=== Vision OP-CT Pipeline (MONTHLY: 1st day 00:10) ===")

    try:
        now = datetime.now()
        if not is_monthly_run_time(now):
            log("[SKIP] 이 스크립트는 Task Scheduler가 매월 1일 00:10에 호출할 때만 실행합니다.")
            return

        run_month = prev_month_yyyymm(now)

        ensure_tables_and_indexes()

        # 월 1회 즉시 실행 후 종료 (대기/루프 없음)
        run_pipeline_once(label=f"monthly_run_{run_month}", run_month=run_month)

        end_dt = datetime.now()
        elapsed = time.perf_counter() - start_perf
        log(f"[END] {end_dt:%Y-%m-%d %H:%M:%S} | elapsed={elapsed:.1f}s")

    except Exception as e:
        log(f"[FATAL] {e}")
        raise


if __name__ == "__main__":
    main()
