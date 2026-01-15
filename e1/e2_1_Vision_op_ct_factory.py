# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인 - current-month incremental + periodic UPSERT
- MP=2 고정
- LATEST/HIST 저장
- id/sequence 자동 보정
- 기존 테이블 스키마 자동 동기화(ALTER TABLE ... ADD COLUMN IF NOT EXISTS)

이번 수정(핵심):
- cache_df에는 end_ts 컬럼을 유지(절대 rename하지 않음)
- 분석 직전 df_for_analysis = cache_df.rename({"end_ts":"end_dt"})로만 변환
- KeyError: 'end_ts' 해결

추가(이번 반영):
1) only-run 판정 로직을 "시간순(end_day,end_time 오름차순) 연속 10개 이상"으로 정확히 변경
2) IDLE(신규 없음) 상태에서도 하트비트 로그 출력 -> 멈춘 것처럼 보이는 문제 해소
3) fetch/analysis 단계 소요시간 로그(병목 구간 확인용)
"""

import os
import sys
import warnings
import urllib.parse
from datetime import datetime, date, time as dtime
import time as time_mod

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text

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
    "host": "192.168.108.162",
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

PROC_1_STATIONS = ["Vision1"]
PROC_2_STATIONS = ["Vision2"]

RUN_TIME_1 = dtime(8, 22, 0)
RUN_TIME_2 = dtime(20, 22, 0)

LOOP_INTERVAL_SEC = 5
FETCH_LIMIT = 200000

# ✅ 추가: 신규가 없을 때도 로그를 찍을지(하트비트)
IDLE_HEARTBEAT = True
# ✅ 추가: 병목 확인용 상세 타이밍 로그
TIMING_LOG = True


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
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), boxpoints=False, name=title))
    fig.update_layout(title=title, showlegend=False)
    return fig.to_json(validate=False)


def fmt_range(a: float, b: float) -> str:
    return f"{a:.2f}~{b:.2f}"


# =========================
# 2) 테이블/인덱스/ID 보정 + 스키마 동기화
# =========================
def ensure_tables_and_indexes():
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

            # CREATE if not exists
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

            # LATEST 컬럼 동기화
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

            # HIST snapshot_ts 동기화
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
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(do_sql)
        conn.commit()


# =========================
# 4) run_id / only_run 판정 (end_dt 기반)
# =========================
def mark_only_runs(df: pd.DataFrame) -> pd.DataFrame:
    """
    [요구사항대로 정확히 반영]
    - end_dt(= end_day+end_time) 오름차순으로 정렬
    - 그 시간순에서 station 값이 Vision1 또는 Vision2로 "연속" 10개 이상인 구간만 True
    - True인 행들만 summarize()에서 Vision1_only / Vision2_only로 이름이 변경되어 집계됨
    """
    out = df.copy()
    if out is None or out.empty:
        out = pd.DataFrame(columns=list(df.columns) if df is not None else [])
        out["run_id"] = []
        out["run_len"] = []
        out["is_vision_only_run"] = []
        return out

    # ✅ 시간순 정렬 (end_day,end_time 오름차순 == end_dt 오름차순)
    out = out.sort_values(["end_dt"], kind="mergesort").reset_index(drop=True)

    # ✅ station 연속(run) 구간: station이 바뀌면 run_id 증가
    out["run_id"] = (out["station"] != out["station"].shift(1)).cumsum()

    # ✅ run 길이
    run_sizes = out.groupby("run_id")["station"].size().rename("run_len")
    out = out.merge(run_sizes, on="run_id", how="left")

    # ✅ only-run = (Vision1/Vision2) AND (연속길이>=10)
    is_vision_station = out["station"].isin(["Vision1", "Vision2"])
    out["is_vision_only_run"] = is_vision_station & (out["run_len"] >= ONLY_RUN_MIN_LEN)

    return out


def build_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    # op_ct 계산은 station+remark 그룹별 시간차이(diff)이므로, 그룹별로 시간 정렬 후 diff
    d = df.sort_values(["station", "remark", "end_dt"], kind="mergesort").copy()
    d["op_ct"] = d.groupby(["station", "remark"])["end_dt"].diff().dt.total_seconds()

    df_an = d.dropna(subset=["op_ct"]).copy()
    df_an = df_an[df_an["op_ct"] <= OPCT_MAX_SEC].copy()
    return df_an


def summarize(df_an: pd.DataFrame) -> pd.DataFrame:
    """
    [요구사항 반영]
    - is_vision_only_run == True 인 경우에만
      Vision1 -> Vision1_only, Vision2 -> Vision2_only 로 "분리 집계"
    - False 인 경우는 Vision1/Vision2 그대로 집계
    - (station_out, remark, month) 단위로 boxplot 통계 + plotly_json 생성
    """
    if df_an is None or df_an.empty:
        return pd.DataFrame()

    df_normal = df_an[df_an["is_vision_only_run"] == False].copy()
    df_only   = df_an[df_an["is_vision_only_run"] == True].copy()

    tasks: list[tuple[str, str, str, np.ndarray]] = []

    # normal 집계(이름 유지)
    if not df_normal.empty:
        dn = df_normal.copy()
        dn["station_out"] = dn["station"]
        for (st, rk, mo), g in dn.groupby(["station_out", "remark", "month"], sort=True):
            vals = g["op_ct"].dropna().astype(float).to_numpy()
            if vals.size:
                tasks.append((st, rk, str(mo), vals))

    # only 집계(True인 것만 _only로 이름 변경)
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
# 7) 저장
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
# main (핵심 수정: cache_df는 end_ts 유지)
# =========================
def main():
    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== REALTIME MODE: current-month incremental + periodic UPSERT ===")
    log(f"loop_interval={LOOP_INTERVAL_SEC}s | fetch_limit={FETCH_LIMIT}")

    ensure_tables_and_indexes()

    run_month = current_yyyymm()
    last_ts: dict[tuple[str, str], pd.Timestamp] = {}
    cache_df: pd.DataFrame | None = None

    def _fetch_new_rows(eng, stations: list[str], run_month_local: str) -> pd.DataFrame:
        sql_query = f"""
        SELECT station, remark, end_day, end_time, result, goodorbad
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE station = ANY(:stations)
          AND remark IN ('PD','Non-PD')
          AND COALESCE(result,'') <> 'FAIL'
          AND COALESCE(goodorbad,'') <> 'BadFile'
          AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
        ORDER BY end_day ASC, end_time ASC
        LIMIT :limit
        """
        df = pd.read_sql(
            text(sql_query),
            eng,
            params={"stations": stations, "run_month": run_month_local, "limit": FETCH_LIMIT},
        )
        if df is None or df.empty:
            return df

        # end_ts 생성
        df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
        df["end_time_str"] = df["end_time"].astype(str).str.strip()
        df["end_ts"] = pd.to_datetime(df["end_day"] + " " + df["end_time_str"], errors="coerce", format="mixed")
        df = df.dropna(subset=["end_ts"]).copy()

        # last_ts 이후만 남김 (기존 기능 유지: strict '>' )
        keep = []
        for (st, rm), g in df.groupby(["station", "remark"], sort=False):
            lt = last_ts.get((st, rm))
            keep.append(g if lt is None else g[g["end_ts"] > lt])

        return pd.concat(keep, ignore_index=True) if keep else df.iloc[0:0].copy()

    def _update_last_ts(df_new: pd.DataFrame):
        if df_new is None or df_new.empty:
            return
        for (st, rm), g in df_new.groupby(["station", "remark"], sort=False):
            last_ts[(st, rm)] = g["end_ts"].max()

    eng = get_engine(DB_CONFIG)

    idle_tick = 0
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

            t0 = time_mod.time()
            df_new = _fetch_new_rows(eng, stations_all, run_month)
            t_fetch = time_mod.time() - t0

            if TIMING_LOG:
                log(f"[T] fetch_sec={t_fetch:.2f} | new_rows={0 if df_new is None else len(df_new)}")

            # ✅ 추가: 신규 없을 때도 하트비트 출력 (멈춘 것처럼 보이는 문제 방지)
            if df_new is None or df_new.empty:
                idle_tick += 1
                if IDLE_HEARTBEAT:
                    log(f"[IDLE] no new rows | month={run_month} | sleep={LOOP_INTERVAL_SEC}s | tick={idle_tick}")
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            idle_tick = 0

            # ========= 앵커 + 신규 op_ct 재계산 =========
            t1 = time_mod.time()

            if cache_df is not None and not cache_df.empty:
                anchors = []
                for (st, rm), g in cache_df.groupby(["station", "remark"], sort=False):
                    anchors.append(
                        g.sort_values("end_ts").tail(1)[["station","remark","end_day","end_time_str","end_ts"]]
                    )
                df_anchor = pd.concat(anchors, ignore_index=True) if anchors else None

                df_tmp = (
                    pd.concat([df_anchor, df_new[["station","remark","end_day","end_time_str","end_ts"]]], ignore_index=True)
                    if df_anchor is not None
                    else df_new[["station","remark","end_day","end_time_str","end_ts"]].copy()
                )
            else:
                df_tmp = df_new[["station","remark","end_day","end_time_str","end_ts"]].copy()

            df_tmp = df_tmp.sort_values(["station","remark","end_ts"]).reset_index(drop=True)
            df_tmp["op_ct"] = df_tmp.groupby(["station","remark"])["end_ts"].diff().dt.total_seconds()
            df_tmp["month"] = df_tmp["end_ts"].dt.strftime("%Y%m")

            # 신규(end_ts 기준)만 cache 반영 (기존 기능 유지)
            df_new_for_cache = df_tmp[df_tmp["end_ts"].isin(df_new["end_ts"])].copy()

            if cache_df is None or cache_df.empty:
                cache_df = df_new_for_cache.copy()
            else:
                cache_df = pd.concat([cache_df, df_new_for_cache], ignore_index=True)
                cache_df = cache_df.drop_duplicates(subset=["station","remark","end_ts"], keep="last")

            _update_last_ts(df_new)

            log(f"[NEW] rows={len(df_new_for_cache)} | cache={len(cache_df)} | month={run_month}")

            # ========= 분석은 '뷰'에서만 end_dt로 변환 =========
            df_for_analysis = cache_df.rename(columns={"end_ts": "end_dt"}).copy()
            if "month" not in df_for_analysis.columns:
                df_for_analysis["month"] = df_for_analysis["end_dt"].dt.strftime("%Y%m")

            df_marked = mark_only_runs(df_for_analysis)
            df_an = build_analysis_df(df_marked)
            summary_df = summarize(df_an) if df_an is not None and not df_an.empty else pd.DataFrame()

            if summary_df is not None and not summary_df.empty:
                upsert_latest(summary_df)
                upsert_hist_daily(summary_df, snapshot_day=today_yyyymmdd())

            t_all = time_mod.time() - t1
            if TIMING_LOG:
                log(f"[T] analyze+upsert_sec={t_all:.2f} | summary_rows={0 if summary_df is None else len(summary_df)}")

            time_mod.sleep(LOOP_INTERVAL_SEC)

    finally:
        try:
            eng.dispose()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[INTERRUPT] 사용자 중단")
        pause_on_exit(0)
    except Exception:
        log("\n[ERROR] 예외 발생")
        import traceback
        traceback.print_exc()
        pause_on_exit(1)
    else:
        if getattr(sys, "frozen", False):
            _hard_pause_console()
