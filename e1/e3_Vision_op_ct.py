# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인
- Source: a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history
- Filter:
  * station IN ('Vision1','Vision2')
  * result != 'FAIL' (NULL 포함 대비 COALESCE)
  * goodorbad != 'BadFile' (NULL 포함 대비 COALESCE)
- Logic:
  * end_dt 생성 후 정렬
  * station 변경 시 run_id 증가, run_len >= 10 => vision_only_run
  * (station, remark)별 op_ct 계산
  * op_ct NaN 제거 + op_ct<=600만 분석
  * 정상군: vision_only_run 제외
  * only군: vision_only_run만 (Vision1_only / Vision2_only로 라벨링)
  * (station_out, remark, month) 단위 boxplot 통계 + plotly_json 생성
- Save:
  * e2_vision_ct.vision_op_ct
  * UNIQUE (station, remark, month), ON CONFLICT DO NOTHING

요구사항:
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 실행 시작/종료 시각 및 총 소요 시간 출력 추가
"""

import os
import sys
import time
from datetime import datetime
import warnings

# --- Thread 제한(원 코드 유지)
os.environ["OMP_NUM_THREADS"] = "1"
os.environ["MKL_NUM_THREADS"] = "1"
os.environ["OPENBLAS_NUM_THREADS"] = "1"
os.environ["NUMEXPR_NUM_THREADS"] = "1"

warnings.filterwarnings(
    "ignore",
    message=r"KMeans is known to have a memory leak on Windows with MKL.*"
)

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
import urllib.parse

import plotly.graph_objects as go

import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e2_vision_ct"   # 소문자 고정
TARGET_TABLE  = "vision_op_ct"

OPCT_MAX_SEC = 600
ONLY_RUN_MIN_LEN = 10


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)

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
    return fig.to_json()

def fmt_range(a: float, b: float) -> str:
    return f"{a:.2f}~{b:.2f}"


# =========================
# 2) 로딩 + 전처리
# =========================
def load_source(engine) -> pd.DataFrame:
    q = text(f"""
    SELECT
        station,
        remark,
        end_day,
        end_time,
        result,
        goodorbad
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE station IN ('Vision1','Vision2')
      AND COALESCE(result,'') <> 'FAIL'
      AND COALESCE(goodorbad,'') <> 'BadFile'
    ORDER BY end_day ASC, end_time ASC
    """)

    log("[1/6] 원본 로딩 시작...")
    df = pd.read_sql(q, engine)
    log(f"[OK] 로딩 완료 (rows={len(df)})")

    log("[2/6] end_dt 생성 및 month 생성...")
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

    log("[OK] end_dt/month 완료")
    return df


# =========================
# 3) run_id / only_run 판정
# =========================
def mark_only_runs(df: pd.DataFrame) -> pd.DataFrame:
    log("[3/6] run_id/run_len 계산 및 only_run 표시...")

    out = df.copy()
    out["run_id"] = (out["station"] != out["station"].shift(1)).cumsum()
    run_sizes = out.groupby("run_id")["station"].size().rename("run_len")
    out = out.merge(run_sizes, on="run_id", how="left")
    out["is_vision_only_run"] = out["run_len"] >= ONLY_RUN_MIN_LEN

    n_only = int(out["is_vision_only_run"].sum())
    log(f"[OK] only_run 표시 완료 (only_run rows={n_only})")
    return out


# =========================
# 4) op_ct 계산 + 분석 대상(df_an) 생성
# =========================
def build_analysis_df(df: pd.DataFrame) -> pd.DataFrame:
    log("[4/6] op_ct 계산 및 op_ct<=600 필터...")

    d = df.sort_values(["station", "remark", "end_dt"]).copy()
    d["op_ct"] = d.groupby(["station", "remark"])["end_dt"].diff().dt.total_seconds()

    df_an = d.dropna(subset=["op_ct"]).copy()
    df_an = df_an[df_an["op_ct"] <= OPCT_MAX_SEC].copy()

    log(f"[OK] 분석 대상 생성 완료 (rows={len(df_an)})")
    return df_an


# =========================
# 5) 요약 생성 (정상/only 분리)
# =========================
def summarize(df_an: pd.DataFrame) -> pd.DataFrame:
    log("[5/6] 요약(summary_df) 생성...")

    rows = []

    def summarize_subset(df_subset: pd.DataFrame, station_label_map=None):
        if station_label_map is not None:
            dsub = df_subset.copy()
            dsub["station_out"] = dsub["station"].map(station_label_map).fillna(dsub["station"])
        else:
            dsub = df_subset.copy()
            dsub["station_out"] = dsub["station"]

        group_cols = ["station_out", "remark", "month"]
        groups = list(dsub.groupby(group_cols, sort=True))

        for idx, ((st, rk, mo), g) in enumerate(groups, start=1):
            if idx % 30 == 0 or idx == 1 or idx == len(groups):
                log(f"[PROGRESS] group {idx}/{len(groups)} ... ({st}, {rk}, {mo})")

            vals = g["op_ct"].astype(float).to_numpy()
            if len(vals) == 0:
                continue

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
                "month": mo,
                "sample_amount": stats["sample_amount"],
                "op_ct_lower_outlier": fmt_range(lower, q1),
                "q1": q1,
                "median": med,
                "q3": q3,
                "op_ct_upper_outlier": fmt_range(q3, upper),
                "del_out_op_ct_av": del_out_mean,
                "plotly_json": plotly_json,
            })

    df_normal = df_an[df_an["is_vision_only_run"] == False].copy()
    log(f"[INFO] 정상군 rows={len(df_normal)}")
    summarize_subset(df_normal)

    df_only = df_an[df_an["is_vision_only_run"] == True].copy()
    log(f"[INFO] only군 rows={len(df_only)}")
    summarize_subset(df_only, station_label_map={"Vision1": "Vision1_only", "Vision2": "Vision2_only"})

    summary_df = pd.DataFrame(rows)
    if summary_df.empty:
        log("[WARN] summary_df가 비었습니다(저장할 데이터 없음).")
        return summary_df

    summary_df = summary_df.sort_values(["remark", "station", "month"]).reset_index(drop=True)
    summary_df.insert(0, "id", np.arange(1, len(summary_df) + 1))

    summary_df = summary_df[
        ["id", "station", "remark", "month", "sample_amount",
         "op_ct_lower_outlier", "q1", "median", "q3", "op_ct_upper_outlier",
         "del_out_op_ct_av", "plotly_json"]
    ]

    log(f"[OK] summary_df 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 6) DB 저장
# =========================
def save_to_db(summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        log("[SKIP] 저장할 데이터가 없어 DB 저장 생략")
        return

    log("[6/6] DB 저장 시작 (ON CONFLICT DO NOTHING)...")

    with get_conn_pg(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                   .format(sql.Identifier(TARGET_SCHEMA))
            )

            cur.execute(
                sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
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
                    inserted_at          TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT uq_vision_op_ct UNIQUE (station, remark, month)
                )
                """).format(
                    sql.Identifier(TARGET_SCHEMA),
                    sql.Identifier(TARGET_TABLE)
                )
            )

            conn.commit()

            insert_sql = sql.SQL("""
                INSERT INTO {}.{} (
                    station, remark, month,
                    sample_amount,
                    op_ct_lower_outlier,
                    q1, median, q3,
                    op_ct_upper_outlier,
                    del_out_op_ct_av,
                    plotly_json
                )
                VALUES %s
                ON CONFLICT (station, remark, month) DO NOTHING
            """).format(
                sql.Identifier(TARGET_SCHEMA),
                sql.Identifier(TARGET_TABLE)
            )

            records = [
                (
                    r["station"],
                    r["remark"],
                    r["month"],
                    int(r["sample_amount"]) if pd.notna(r["sample_amount"]) else None,
                    r["op_ct_lower_outlier"],
                    float(r["q1"]) if pd.notna(r["q1"]) else None,
                    float(r["median"]) if pd.notna(r["median"]) else None,
                    float(r["q3"]) if pd.notna(r["q3"]) else None,
                    r["op_ct_upper_outlier"],
                    float(r["del_out_op_ct_av"]) if pd.notna(r["del_out_op_ct_av"]) else None,
                    r["plotly_json"],
                )
                for _, r in summary_df.iterrows()
            ]

            if records:
                execute_values(
                    cur,
                    insert_sql.as_string(conn),
                    records,
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)"
                )

            conn.commit()

    log(f"[OK] Insert attempted: {len(summary_df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE} (duplicates PASSED)")


# =========================
# main
# =========================
def main():
    start_dt = datetime.now()
    start_ts = time.perf_counter()

    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== Vision OP-CT Pipeline START ===")

    try:
        engine = get_engine(DB_CONFIG)

        df = load_source(engine)
        df = mark_only_runs(df)
        df_an = build_analysis_df(df)
        summary_df = summarize(df_an)
        save_to_db(summary_df)

        log("=== Vision OP-CT Pipeline DONE ===")

    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)

    finally:
        end_dt = datetime.now()
        elapsed = time.perf_counter() - start_ts
        log(f"[END]   {end_dt:%Y-%m-%d %H:%M:%S}")
        log(f"[TIME]  total_elapsed = {elapsed:.2f} sec ({elapsed/60:.2f} min)")


if __name__ == "__main__":
    main()
