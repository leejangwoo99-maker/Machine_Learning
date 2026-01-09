# -*- coding: utf-8 -*-
"""
FCT RunTime CT 분석 파이프라인 (iqz step 전용)
- Source: a2_fct_table.fct_table
- Filter:
  * station: FCT1~4
  * barcode_information LIKE 'B%'
  * result <> 'FAIL'
  * remark=PD     -> step_description='1.36 Test iqz(uA)'
  * remark=Non-PD -> step_description='1.32 Test iqz(uA)'

- Output:
  1) e1_FCT_ct.fct_run_time_ct
     (station, remark, month) UNIQUE, ON CONFLICT DO NOTHING
  2) e1_FCT_ct.fct_upper_outlier_ct_list
     UNIQUE INDEX (station, remark, barcode_information, end_day, end_time), ON CONFLICT DO NOTHING

요구사항:
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 실행 시작/종료 시각 및 총 소요 시간 출력 추가
"""

import sys
import time
from datetime import datetime
import urllib.parse

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
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a2_fct_table"
SRC_TABLE  = "fct_table"

TARGET_SCHEMA_1 = "e1_FCT_ct"
TARGET_TABLE_1  = "fct_run_time_ct"

TARGET_SCHEMA_2 = "e1_FCT_ct"
TARGET_TABLE_2  = "fct_upper_outlier_ct_list"


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


# =========================
# 2) 로딩 + 전처리
# =========================
def load_source(engine) -> pd.DataFrame:
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
        station IN ('FCT1','FCT2','FCT3','FCT4')
        AND barcode_information LIKE 'B%%'
        AND result <> 'FAIL'
        AND (
            (remark = 'PD' AND step_description = '1.36 Test iqz(uA)')
            OR
            (remark = 'Non-PD' AND step_description = '1.32 Test iqz(uA)')
        )
    ORDER BY end_day ASC, end_time ASC
    """
    log("[1/5] 원본 데이터 로딩 시작...")
    df = pd.read_sql(text(query), engine)
    log(f"[OK] 로딩 완료 (rows={len(df)})")

    log("[2/5] 전처리 (end_day/end_time 정리, month 생성, run_time 숫자화)...")
    df["end_day"] = df["end_day"].astype(str).str.strip()
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

    df["month"] = df["end_day"].str.slice(0, 6)
    df = df.sort_values(["end_day", "end_time"], ascending=[True, True]).reset_index(drop=True)

    before = len(df)
    df = df.dropna(subset=["run_time"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] run_time NaN 제거: {dropped} rows drop")

    log("[OK] 전처리 완료")
    return df


# =========================
# 3) 요약 생성 (boxplot 통계 + plotly_json)
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
    log("[3/5] (station, remark, month) 요약 DF 생성...")
    groups = list(df.groupby(["station", "remark", "month"], sort=True))
    log(f"[INFO] 그룹 수 = {len(groups)}")

    rows = []
    for idx, ((station, remark, month), g) in enumerate(groups, start=1):
        if idx % 20 == 0 or idx == 1 or idx == len(groups):
            log(f"[PROGRESS] summary {idx}/{len(groups)} ... ({station}, {remark}, {month})")

        vals = g["run_time"].astype(float).to_numpy()
        name = f"{station}_{remark}_{month}"
        d = boxplot_summary_from_values(vals, name=name)
        d.update({"station": station, "remark": remark, "month": month})
        rows.append(d)

    summary_df = pd.DataFrame(rows)
    summary_df.insert(0, "id", np.arange(1, len(summary_df) + 1))

    cols_order = [
        "id", "station", "remark", "month", "sample_amount",
        "run_time_lower_outlier", "q1", "median", "q3",
        "run_time_upper_outlier", "del_out_run_time_av", "plotly_json",
    ]
    cols_order = [c for c in cols_order if c in summary_df.columns]
    summary_df = summary_df[cols_order]

    summary_df = summary_df.sort_values(["station", "remark", "month"]).reset_index(drop=True)
    log(f"[OK] summary_df 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 4) DB 저장 1: fct_run_time_ct
# =========================
def save_run_time_ct(summary_df: pd.DataFrame):
    log("[4/5] DB 저장(1) fct_run_time_ct 시작...")

    to_save = summary_df.drop(columns=["id"], errors="ignore").copy()
    to_save = to_save.where(pd.notnull(to_save), None)

    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
        "plotly_json",
    ]
    to_save = to_save[cols]
    rows = [tuple(r) for r in to_save.itertuples(index=False, name=None)]

    create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA_1}";'

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA_1}".{TARGET_TABLE_1} (
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
        CONSTRAINT uq_fct_run_time_ct UNIQUE (station, remark, month)
    );
    """

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA_1}".{TARGET_TABLE_1} (
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json
    )
    VALUES %s
    ON CONFLICT (station, remark, month)
    DO NOTHING;
    """

    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)"

    conn = get_conn_psycopg2(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(create_schema_sql)
            cur.execute(create_table_sql)

            if rows:
                execute_values(
                    cur,
                    insert_sql,
                    rows,
                    template=template,
                    page_size=1000
                )
        conn.commit()
        log(f"[OK] fct_run_time_ct 저장 완료 (insert candidates={len(rows)}; 중복키는 PASS)")
    finally:
        conn.close()


# =========================
# 5) Upper outlier 상세 DF 생성 + DB 저장 2
# =========================
def build_upper_outlier_df(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    log("[5/5] upper outlier 상세 DF 생성 시작...")

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

    if upper_outlier_rows:
        upper_outlier_df = pd.concat(upper_outlier_rows, ignore_index=True)
    else:
        upper_outlier_df = pd.DataFrame()

    if upper_outlier_df.empty:
        log("[OK] upper outlier 없음")
        return upper_outlier_df

    upper_outlier_df = upper_outlier_df.rename(columns={"barcode_information": "barcode"})

    upper_outlier_df = upper_outlier_df[
        ["barcode", "station", "end_day", "end_time", "remark", "run_time", "month", "upper_threshold"]
    ].sort_values(
        ["station", "remark", "month", "end_day", "end_time"]
    ).reset_index(drop=True)

    log(f"[OK] upper_outlier_df 생성 완료 (rows={len(upper_outlier_df)})")
    return upper_outlier_df

def save_upper_outlier_df(upper_outlier_df: pd.DataFrame):
    if upper_outlier_df is None or upper_outlier_df.empty:
        log("[SKIP] upper outlier 저장 생략 (데이터 없음)")
        return

    save_df = upper_outlier_df.copy()

    if "barcode_information" not in save_df.columns and "barcode" in save_df.columns:
        save_df = save_df.rename(columns={"barcode": "barcode_information"})

    required = ["station", "remark", "barcode_information", "end_day", "end_time"]
    missing = [c for c in required if c not in save_df.columns]
    if missing:
        raise ValueError(f"upper_outlier_df에 필요한 컬럼이 없습니다: {missing}")

    cols = [
        "station", "remark", "barcode_information", "end_day", "end_time",
        "run_time", "month", "upper_threshold",
    ]
    cols = [c for c in cols if c in save_df.columns]
    save_df = save_df[cols].copy()

    save_df = save_df.where(pd.notnull(save_df), None)
    rows = [tuple(r) for r in save_df.itertuples(index=False, name=None)]

    create_schema_sql = f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA_2}";'

    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA_2}".{TARGET_TABLE_2} (
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
    """

    create_unique_idx_sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS uq_fct_upper_outlier_ct_list
    ON "{TARGET_SCHEMA_2}".{TARGET_TABLE_2} (station, remark, barcode_information, end_day, end_time);
    """

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA_2}".{TARGET_TABLE_2} (
        {", ".join(cols)}
    )
    VALUES %s
    ON CONFLICT (station, remark, barcode_information, end_day, end_time)
    DO NOTHING;
    """

    template = "(" + ",".join(["%s"] * len(cols)) + ")"

    log("[INFO] DB 저장(2) fct_upper_outlier_ct_list 시작...")
    conn = get_conn_psycopg2(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(create_schema_sql)
            cur.execute(create_table_sql)
            cur.execute(create_unique_idx_sql)

            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=2000)

        conn.commit()
        log(f"[OK] upper outlier 저장 완료: {len(rows)} rows (중복키는 PASS)")
    finally:
        conn.close()


# =========================
# main
# =========================
def main():
    # ---- 실행 시간 측정 시작 ----
    start_dt = datetime.now()
    start_ts = time.perf_counter()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== FCT RunTime CT Pipeline START ===")

    try:
        engine = get_engine(DB_CONFIG)

        df_raw = load_source(engine)
        summary_df = build_summary_df(df_raw)

        save_run_time_ct(summary_df)

        upper_outlier_df = build_upper_outlier_df(df_raw, summary_df)
        save_upper_outlier_df(upper_outlier_df)

        log("=== FCT RunTime CT Pipeline DONE ===")

    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)

    finally:
        # ---- 실행 시간 측정 종료 ----
        elapsed = time.perf_counter() - start_ts
        end_dt = datetime.now()
        log(f"[END]   {end_dt:%Y-%m-%d %H:%M:%S}")
        log(f"[TIME]  total_elapsed = {elapsed:.2f} sec ({elapsed/60:.2f} min)")


if __name__ == "__main__":
    main()
