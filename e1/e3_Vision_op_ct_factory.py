# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인 - Realtime Loop + MP=2 고정

- Source: a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history
- Filter:
  * station IN ('Vision1','Vision2')
  * result != 'FAIL' (NULL 포함 대비 COALESCE)
  * goodorbad != 'BadFile' (NULL 포함 대비 COALESCE)
  * end_day = 오늘(CURRENT_DATE)

- Realtime Filter:
  * end_dt >= max(now-120sec, cutoff_ts)

- Logic:
  * end_dt 생성 후 정렬
  * station 변경 시 run_id 증가, run_len >= 10 => vision_only_run
  * (station, remark)별 op_ct 계산
  * op_ct NaN 제거 + op_ct<=600만 분석
  * 정상군: vision_only_run 제외
  * only군: vision_only_run만 (Vision1_only / Vision2_only 라벨)
  * (station_out, remark, month) 단위 boxplot 통계 + plotly_json 생성

- Save:
  * e2_vision_ct.vision_op_ct
  * UNIQUE (station, remark, month), ON CONFLICT DO NOTHING

요구사항(요청):
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- [멀티프로세스] 2개 고정
- [무한 루프] 1초마다 재실행
- [윈도우] end_day = 오늘만
- [실시간] 현재 시간 기준 120초 이내 + cutoff_ts 이후만 처리
"""

import os
import sys
import time
import warnings
import urllib.parse
from datetime import datetime, timedelta, date
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text

import plotly.graph_objects as go

import psycopg2
from psycopg2 import sql
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
TARGET_TABLE  = "vision_op_ct"

OPCT_MAX_SEC = 600
ONLY_RUN_MIN_LEN = 10

# ===== 멀티프로세스 고정 =====
MAX_WORKERS = 2

# ===== 실시간 루프/필터 =====
LOOP_INTERVAL_SEC = 1
REALTIME_WINDOW_SEC = 120
CUTOFF_TS = 1765501841.4473598  # 요청값


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
    return fig.to_json()

def fmt_range(a: float, b: float) -> str:
    return f"{a:.2f}~{b:.2f}"


# =========================
# 2) 로딩 + 전처리 (오늘 + 최근 120초 + cutoff 이후)
# =========================
def load_source(engine) -> pd.DataFrame:
    """
    - SQL: end_day = CURRENT_DATE 로 로드 최소화
    - end_dt 생성 후:
        end_dt >= max(now-120s, cutoff_dt)
      만족하는 행만 유지
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
    WHERE station IN ('Vision1','Vision2')
      AND COALESCE(result,'') <> 'FAIL'
      AND COALESCE(goodorbad,'') <> 'BadFile'
      AND end_day = CURRENT_DATE
    ORDER BY end_day ASC, end_time ASC
    """)

    log("[1/6] 원본 로딩(오늘) 시작...")
    df = pd.read_sql(q, engine)
    log(f"[OK] 로딩 완료 (rows={len(df)})")

    if df is None or len(df) == 0:
        return df

    log("[2/6] end_dt 생성 + realtime filter + month 생성...")

    # end_day 정리(숫자만 남김)
    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    dt_str = df["end_day"] + " " + df["end_time"].astype(str).str.strip()

    # end_dt 생성
    df["end_dt"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")

    before = len(df)
    df = df.dropna(subset=["end_dt"]).copy()
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] end_dt 파싱 실패 drop: {dropped} rows")

    # ===== 실시간 필터 =====
    now_dt = datetime.now()
    window_start = now_dt - timedelta(seconds=REALTIME_WINDOW_SEC)
    cutoff_dt = datetime.fromtimestamp(float(CUTOFF_TS))
    threshold = max(window_start, cutoff_dt)

    before2 = len(df)
    df = df[df["end_dt"] >= threshold].copy()
    after2 = len(df)
    log(f"[INFO] realtime filter: end_dt >= {threshold.strftime('%Y-%m-%d %H:%M:%S')}  (kept {after2}/{before2})")

    if after2 == 0:
        return df

    df = df.sort_values(["end_dt", "station", "remark"]).reset_index(drop=True)
    df["month"] = df["end_dt"].dt.strftime("%Y%m")

    log("[OK] end_dt/month 완료")
    return df


# =========================
# 3) run_id / only_run 판정
# =========================
def mark_only_runs(df: pd.DataFrame) -> pd.DataFrame:
    log("[3/6] run_id/run_len 계산 및 only_run 표시...")

    if df is None or len(df) == 0:
        return df

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

    if df is None or len(df) == 0:
        return pd.DataFrame()

    d = df.sort_values(["station", "remark", "end_dt"]).copy()
    d["op_ct"] = d.groupby(["station", "remark"])["end_dt"].diff().dt.total_seconds()

    df_an = d.dropna(subset=["op_ct"]).copy()
    df_an = df_an[df_an["op_ct"] <= OPCT_MAX_SEC].copy()

    log(f"[OK] 분석 대상 생성 완료 (rows={len(df_an)})")
    return df_an


# =========================
# 5) 요약 생성 (정상/only 분리) - MP=2
# =========================
def _summary_worker(args):
    """
    args = (station_out, remark, month, op_ct_list)
    """
    st, rk, mo, op_list = args
    vals = np.asarray(op_list, dtype=float)
    if vals.size == 0:
        return None

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

    return {
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
    }

def summarize(df_an: pd.DataFrame) -> pd.DataFrame:
    log(f"[5/6] 요약(summary_df) 생성... (MP={MAX_WORKERS})")

    if df_an is None or len(df_an) == 0:
        log("[WARN] 요약 생성 대상 데이터가 없습니다.")
        return pd.DataFrame()

    df_normal = df_an[df_an["is_vision_only_run"] == False].copy()
    df_only   = df_an[df_an["is_vision_only_run"] == True].copy()

    log(f"[INFO] 정상군 rows={len(df_normal)}")
    log(f"[INFO] only군 rows={len(df_only)}")

    tasks = []

    # (A) 정상군: station_out = station
    if not df_normal.empty:
        dn = df_normal.copy()
        dn["station_out"] = dn["station"]
        for (st, rk, mo), g in dn.groupby(["station_out", "remark", "month"], sort=True):
            op_list = g["op_ct"].dropna().astype(float).tolist()
            tasks.append((st, rk, mo, op_list))

    # (B) only군: station_out = Vision1_only / Vision2_only
    if not df_only.empty:
        do = df_only.copy()
        do["station_out"] = do["station"].map({"Vision1": "Vision1_only", "Vision2": "Vision2_only"}).fillna(do["station"])
        for (st, rk, mo), g in do.groupby(["station_out", "remark", "month"], sort=True):
            op_list = g["op_ct"].dropna().astype(float).tolist()
            tasks.append((st, rk, mo, op_list))

    total = len(tasks)
    if total == 0:
        log("[WARN] summary 생성 대상 그룹이 없습니다.")
        return pd.DataFrame()

    log(f"[INFO] 그룹 수 = {total}")

    rows = []
    done = 0
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_summary_worker, t) for t in tasks]
        for fut in as_completed(futures):
            r = fut.result()
            if r is not None:
                rows.append(r)
            done += 1
            if done == 1 or done == total or done % 30 == 0:
                log(f"[PROGRESS] group {done}/{total} ...")

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
            # 1) 스키마 생성
            cur.execute(
                sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                   .format(sql.Identifier(TARGET_SCHEMA))
            )

            # 2) 테이블 생성
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
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    page_size=2000
                )

            conn.commit()

    log(f"[OK] Insert attempted: {len(summary_df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE} (duplicates PASSED)")


# =========================
# 7) 1회 실행(루프 내부)
# =========================
def run_once(engine):
    df = load_source(engine)
    if df is None or len(df) == 0:
        log("[INFO] 처리 대상 데이터 없음 (오늘/120초/cutoff 조건).")
        return

    df = mark_only_runs(df)
    df_an = build_analysis_df(df)
    summary_df = summarize(df_an)
    save_to_db(summary_df)

    log("=== 1-cycle DONE ===")


# =========================
# main (무한루프 1초)
# =========================
def main():
    try:
        log("=== Vision OP-CT Realtime Loop START ===")
        log(f"[INFO] MP workers = {MAX_WORKERS} (fixed)")
        log(f"[INFO] end_day = today({date.today()})")
        log(f"[INFO] realtime window = {REALTIME_WINDOW_SEC}s, cutoff_ts = {CUTOFF_TS}")

        engine = get_engine(DB_CONFIG)

        while True:
            tick = time.time()
            run_once(engine)

            elapsed = time.time() - tick
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    except KeyboardInterrupt:
        log("[STOP] KeyboardInterrupt")
        sys.exit(0)
    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    from multiprocessing import freeze_support
    freeze_support()
    main()
