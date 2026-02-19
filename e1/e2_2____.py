# -*- coding: utf-8 -*-
"""
Vision RunTime CT (intensity8) 분석 파이프라인 - MP 최대 적용
- Source: a3_vision_table.vision_table
- Filter:
  * barcode_information LIKE 'B%'
  * station IN ('Vision1','Vision2')
  * remark IN ('PD','Non-PD')
  * step_description = 'intensity8'
  * result <> 'FAIL'
  * ORDER BY end_day, end_time

- Summary (station, remark, month):
  * sample_amount
  * IQR 기반 outlier 범위 문자열
  * q1/median/q3
  * outlier 제거 평균(del_out_run_time_av)
  * plotly_json (boxplot)

- Save:
  * e2_vision_ct.vision_run_time_ct
  * PRIMARY KEY (station, remark, month)
  * ON CONFLICT DO UPDATE (UPSERT)

요구사항:
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 멀티프로세스: 최대 (CPU 코어 기준 상한 적용)
- EXE(onefile)에서 plotly validators 오류 방지 (validate=False)
- EXE에서 콘솔이 강제로 닫히지 않도록 유지 (frozen일 때만)
"""

import sys
import urllib.parse
from multiprocessing import cpu_count, freeze_support
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
import plotly.graph_objects as go


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SRC_SCHEMA = "a3_vision_table"
SRC_TABLE  = "vision_table"

TARGET_SCHEMA = "e2_vision_ct"
TARGET_TABLE  = "vision_run_time_ct"

STEP_DESC = "intensity8"

# =========================
# 멀티프로세스 최대 설정
# =========================
MAX_WORKERS_CAP = 12
MAX_WORKERS = max(1, min(cpu_count(), MAX_WORKERS_CAP))


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def get_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str, pool_pre_ping=True)

def _outlier_range_str(values: pd.Series, lower_fence: float, upper_fence: float):
    v = values.dropna().astype(float)
    if v.empty:
        return None, None

    lower_out = v[v < lower_fence]
    upper_out = v[v > upper_fence]

    lower_str = f"{lower_out.min():.2f}~{lower_fence:.2f}" if len(lower_out) > 0 else None
    upper_str = f"{upper_fence:.2f}~{upper_out.max():.2f}" if len(upper_out) > 0 else None
    return lower_str, upper_str

def _make_plotly_json(values: np.ndarray, name: str) -> str:
    """
    ★ 중요: EXE(onefile)에서 plotly validators(_validators.json) 찾다가 터지는 문제 방지
    -> validate=False 강제
    """
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), name=name, boxpoints=False))
    return fig.to_json(validate=False)


# =========================
# 2) 로딩
# =========================
def load_source(engine) -> pd.DataFrame:
    query = text(f"""
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
    WHERE 1=1
      AND barcode_information LIKE 'B%%'
      AND station IN ('Vision1', 'Vision2')
      AND remark IN ('PD', 'Non-PD')
      AND step_description = :step_desc
      AND result <> 'FAIL'
    ORDER BY end_day ASC, end_time ASC
    """)

    log("[1/5] 원본 데이터 로딩 시작...")
    df = pd.read_sql(query, engine, params={"step_desc": STEP_DESC})
    log(f"[OK] 로딩 완료 (rows={len(df)})")
    return df


# =========================
# 3) month 생성 + 정렬
# =========================
def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    log("[2/5] month 생성 및 정렬...")

    out = df.copy()
    out["end_day"] = out["end_day"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(8)
    out["month"] = out["end_day"].str.slice(0, 6)
    out = out.sort_values(["end_day", "end_time"], ascending=True).reset_index(drop=True)

    out["run_time"] = pd.to_numeric(out["run_time"], errors="coerce")
    before = len(out)
    out = out.dropna(subset=["run_time"]).reset_index(drop=True)
    dropped = before - len(out)
    if dropped:
        log(f"[INFO] run_time NaN 제거: {dropped} rows drop")

    log("[OK] 전처리 완료")
    return out


# =========================
# 4) 요약 DF 생성 (MP)
# =========================
def _summary_worker(args):
    """
    args = (station, remark, month, run_time_list)
    """
    station, remark, month, rt_list = args
    rt = np.asarray(rt_list, dtype=float)
    if rt.size == 0:
        return None

    q1 = float(np.percentile(rt, 25))
    med = float(np.percentile(rt, 50))
    q3 = float(np.percentile(rt, 75))
    iqr = q3 - q1

    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    v = pd.Series(rt)
    lower_str, upper_str = _outlier_range_str(v, lower_fence, upper_fence)

    rt_in = rt[(rt >= lower_fence) & (rt <= upper_fence)]
    del_out_mean = float(rt_in.mean()) if rt_in.size > 0 else np.nan

    plotly_json = _make_plotly_json(rt, name=f"{station}_{remark}_{month}")

    return {
        "station": station,
        "remark": remark,
        "month": str(month),
        "sample_amount": int(rt.size),
        "run_time_lower_outlier": lower_str,
        "q1": round(q1, 2),
        "median": round(med, 2),
        "q3": round(q3, 2),
        "run_time_upper_outlier": upper_str,
        "del_out_run_time_av": round(del_out_mean, 2) if not np.isnan(del_out_mean) else None,
        "plotly_json": plotly_json,
    }

def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    log(f"[3/5] 요약(summary_df) 생성... (MP 최대={MAX_WORKERS})")

    tasks = []
    for (station, remark, month), g in df.groupby(["station", "remark", "month"], dropna=False, sort=True):
        rt_list = g["run_time"].dropna().astype(float).tolist()
        tasks.append((station, remark, str(month), rt_list))

    total = len(tasks)
    log(f"[INFO] 그룹 수 = {total}")
    if total == 0:
        log("[WARN] 그룹이 없어 summary_df 생성 불가")
        return pd.DataFrame()

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

    summary_df = summary_df.sort_values(["month", "station", "remark"], ascending=True).reset_index(drop=True)
    summary_df.insert(0, "id", np.arange(1, len(summary_df) + 1))

    log(f"[OK] summary_df 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 5) DB UPSERT 저장
# =========================
def ensure_table(engine):
    create_schema_sql = text(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')
    create_table_sql = text(f"""
    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}"."{TARGET_TABLE}" (
        id                     INTEGER,
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
        updated_at             TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (station, remark, month)
    );
    """)
    with engine.begin() as conn:
        conn.execute(create_schema_sql)
        conn.execute(create_table_sql)

    log(f"[OK] ensured {TARGET_SCHEMA}.{TARGET_TABLE}")

def upsert_summary(engine, summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        log("[SKIP] upsert 생략 (summary_df empty)")
        return

    upsert_sql = text(f"""
    INSERT INTO "{TARGET_SCHEMA}"."{TARGET_TABLE}" (
        id, station, remark, month,
        sample_amount, run_time_lower_outlier, q1, median, q3,
        run_time_upper_outlier, del_out_run_time_av, plotly_json, updated_at
    )
    VALUES (
        :id, :station, :remark, :month,
        :sample_amount, :run_time_lower_outlier, :q1, :median, :q3,
        :run_time_upper_outlier, :del_out_run_time_av, (:plotly_json)::jsonb, now()
    )
    ON CONFLICT (station, remark, month)
    DO UPDATE SET
        id = EXCLUDED.id,
        sample_amount = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1 = EXCLUDED.q1,
        median = EXCLUDED.median,
        q3 = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av = EXCLUDED.del_out_run_time_av,
        plotly_json = EXCLUDED.plotly_json,
        updated_at = now();
    """)

    records = summary_df.to_dict(orient="records")

    log(f"[4/5] UPSERT 시작... (records={len(records)})")
    with engine.begin() as conn:
        conn.execute(upsert_sql, records)

    log(f"[OK] upserted {len(records)} rows into {TARGET_SCHEMA}.{TARGET_TABLE}")


# =========================
# main
# =========================
def main():
    log("=== Vision RunTime CT Pipeline START ===")
    log(f"[INFO] MP max_workers = {MAX_WORKERS} (cpu={cpu_count()}, cap={MAX_WORKERS_CAP})")

    engine = get_engine(DB_CONFIG)

    df = load_source(engine)
    df = preprocess(df)
    summary_df = build_summary(df)

    ensure_table(engine)
    upsert_summary(engine, summary_df)

    log("=== Vision RunTime CT Pipeline DONE ===")


if __name__ == "__main__":
    freeze_support()

    exit_code = 0
    try:
        main()
    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        exit_code = 1
    finally:
        # EXE(Nuitka/pyinstaller) 실행 시 콘솔 유지
        if getattr(sys, "frozen", False):
            print("\n[INFO] 프로그램이 종료되었습니다.")
            input("Press Enter to exit...")

    sys.exit(exit_code)
