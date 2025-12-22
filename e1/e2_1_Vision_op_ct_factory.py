# -*- coding: utf-8 -*-
"""
Vision OP-CT 분석 파이프라인 - Realtime Window + MP=2 고정

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

요구사항(추가 반영):
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- [멀티프로세스] 2개 고정
- [무한 루프] 1초마다 재실행
- [실행 윈도우]
  * 08:27:00 시작 ~ 08:29:59 종료
  * 20:27:00 시작 ~ 20:29:59 종료
- [유효 날짜 범위] end_day가 "현재 날짜 기준의 달(YYYYMM)"만
- [실시간] 현재 시간 기준 120초 이내 데이터만 반영 (end_dt 기준)
- [미완성 방지 역할 분리]
  * 본 스크립트는 파일을 직접 파싱하지 않고 DB만 조회합니다.
  * 따라서 "파일 크기 0.2초 간격 2회 동일 / mtime 안정 / lock 파일 체크"는 파서(적재)쪽에서 수행
  * 본 스크립트에서는 DB 안전 조회를 위해 STABLE_DATA_SEC(기본 2초) 버퍼를 둡니다.
"""

import os
import sys
import warnings
import urllib.parse
from datetime import datetime, time as dtime
import time as time_mod
from multiprocessing import freeze_support
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

# =========================
# Realtime Loop 사양(요청 반영)
# =========================
# ✅ MP 2개 고정
MAX_WORKERS = 2

# ✅ 1초 루프
LOOP_INTERVAL_SEC = 1.0

# ✅ 최근 120초
RECENT_SECONDS = 120

# ✅ DB 안정화 버퍼(방금 적재된 row 회피)
STABLE_DATA_SEC = 2

# ✅ 실행 윈도우
WINDOWS = [
    (dtime(8, 27, 0),  dtime(8, 29, 59)),
    (dtime(20, 27, 0), dtime(20, 29, 59)),
]

HEARTBEAT_EVERY_LOOPS = 30


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def current_yyyymm() -> str:
    return datetime.now().strftime("%Y%m")

def now_in_windows(now_dt: datetime) -> bool:
    t = now_dt.time()
    for s, e in WINDOWS:
        if s <= t <= e:
            return True
    return False

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
    """
    ✅ validate=False 로 JSON 생성 (EXE onefile에서 plotly validators 데이터 누락 시에도 안정적으로 동작)
    """
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), boxpoints=False, name=title))
    fig.update_layout(title=title, showlegend=False)
    return fig.to_json(validate=False)

def fmt_range(a: float, b: float) -> str:
    return f"{a:.2f}~{b:.2f}"


# =========================
# 2) 로딩 + 전처리 (현재달 + 최근120초 + 안정화버퍼)
# =========================
def load_source(engine) -> pd.DataFrame:
    """
    - end_day: 현재달(YYYYMM)만
    - end_dt: now-RECENT_SECONDS ~ now-STABLE_DATA_SEC 만
    """
    yyyymm = current_yyyymm()

    # end_day는 숫자만 남긴 뒤(YYYYMMDD), end_time은 그대로 붙여서 timestamp로 파싱
    # end_time 포맷이 'HH:MM:SS' 인 경우에도 to_timestamp는 맞춰주기 어려우니,
    # 여기서는 "python에서 end_dt 생성" 로직을 유지하고, SQL에서는 end_day(YYYYMM) + 대략적 시간 필터만 수행.
    # -> 정확한 recent 120초 필터는 python end_dt로 한 번 더 거름.
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
      AND substring(regexp_replace(COALESCE(end_day,''), '\\\\D', '', 'g') from 1 for 6) = :yyyymm
    ORDER BY end_day ASC, end_time ASC
    """)

    log("[1/6] 원본 로딩 시작(현재달)...")
    df = pd.read_sql(q, engine, params={"yyyymm": yyyymm})
    log(f"[OK] 로딩 완료 (rows={len(df)})")

    if len(df) == 0:
        return df

    log("[2/6] end_dt 생성 및 month 생성 + 최근120초 필터...")
    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    dt_str = df["end_day"] + " " + df["end_time"].astype(str).str.strip()

    # 원 코드 유지: format="mixed"
    df["end_dt"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")

    before = len(df)
    df = df.dropna(subset=["end_dt"]).copy()
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] end_dt 파싱 실패 drop: {dropped} rows")

    df = df.sort_values(["end_dt", "station", "remark"]).reset_index(drop=True)
    df["month"] = df["end_dt"].dt.strftime("%Y%m")

    # ✅ 최근 120초 + 안정화 버퍼 적용(현재시간 기준)
    now_dt = datetime.now()
    lower_dt = now_dt - pd.Timedelta(seconds=RECENT_SECONDS)
    upper_dt = now_dt - pd.Timedelta(seconds=STABLE_DATA_SEC)

    before2 = len(df)
    df = df[(df["end_dt"] >= lower_dt) & (df["end_dt"] <= upper_dt)].copy()
    log(f"[INFO] recent filter: {before2} -> {len(df)} rows (now-120s ~ now-{STABLE_DATA_SEC}s)")

    log("[OK] end_dt/month/recent 완료")
    return df


# =========================
# 3) run_id / only_run 판정
# =========================
def mark_only_runs(df: pd.DataFrame) -> pd.DataFrame:
    log("[3/6] run_id/run_len 계산 및 only_run 표시...")

    out = df.copy()
    if out.empty:
        out["run_id"] = []
        out["run_len"] = []
        out["is_vision_only_run"] = []
        log("[OK] 입력 없음 -> only_run 표시 스킵")
        return out

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

    if df is None or df.empty:
        log("[OK] 입력 없음 -> df_an 빈 DF 반환")
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

    if df_an is None or df_an.empty:
        log("[WARN] df_an이 비었습니다(저장할 데이터 없음).")
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
        do["station_out"] = (
            do["station"].map({"Vision1": "Vision1_only", "Vision2": "Vision2_only"})
            .fillna(do["station"])
        )
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
                    template="(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)",
                    page_size=2000
                )

            conn.commit()

    log(f"[OK] Insert attempted: {len(summary_df)} rows into {TARGET_SCHEMA}.{TARGET_TABLE} (duplicates PASSED)")


# =========================
# ONE SHOT
# =========================
def main_once():
    log("=== Vision OP-CT Pipeline RUN (ONE SHOT) ===")
    log(f"[INFO] workers={MAX_WORKERS} | month={current_yyyymm()} | recent={RECENT_SECONDS}s | stable_buf={STABLE_DATA_SEC}s")

    engine = get_engine(DB_CONFIG)

    df = load_source(engine)
    if df is None or df.empty:
        log("[SKIP] 최근 데이터 없음 -> 저장 생략")
        return

    df = mark_only_runs(df)
    df_an = build_analysis_df(df)
    if df_an is None or df_an.empty:
        log("[SKIP] op_ct 유효 데이터 없음 -> 저장 생략")
        return

    summary_df = summarize(df_an)
    save_to_db(summary_df)

    log("=== DONE (ONE SHOT) ===")


# =========================
# Realtime loop (윈도우 시간에만)
# =========================
def realtime_loop():
    log("=== Vision OP-CT Realtime Loop START ===")
    log(f"[INFO] windows={[(s.strftime('%H:%M:%S'), e.strftime('%H:%M:%S')) for s,e in WINDOWS]}")
    log(f"[INFO] LOOP_INTERVAL_SEC={LOOP_INTERVAL_SEC} | workers={MAX_WORKERS}")

    loop_count = 0
    while True:
        loop_count += 1
        loop_start = time_mod.perf_counter()
        now_dt = datetime.now()

        if not now_in_windows(now_dt):
            if (loop_count % HEARTBEAT_EVERY_LOOPS) == 0:
                log(f"[IDLE] {now_dt:%Y-%m-%d %H:%M:%S} (out of window)")
            elapsed = time_mod.perf_counter() - loop_start
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                time_mod.sleep(sleep_sec)
            continue

        try:
            log(f"[RUN] {now_dt:%Y-%m-%d %H:%M:%S} (in window)")
            main_once()
        except Exception as e:
            log(f"[ERROR] {type(e).__name__}: {e}")

        elapsed = time_mod.perf_counter() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            time_mod.sleep(sleep_sec)


# =========================
# entry
# =========================
if __name__ == "__main__":
    freeze_support()
    exit_code = 0

    try:
        realtime_loop()
    except KeyboardInterrupt:
        log("\n[ABORT] 사용자 중단(CTRL+C)")
        exit_code = 130
    except Exception as e:
        log(f"[ERROR] Unhandled: {repr(e)}")
        exit_code = 1
    finally:
        # ✅ EXE로 실행 시 콘솔이 자동으로 닫히지 않도록 유지
        if getattr(sys, "frozen", False):
            print("\n[INFO] 프로그램이 종료되었습니다.")
            input("Press Enter to exit...")

    sys.exit(exit_code)
