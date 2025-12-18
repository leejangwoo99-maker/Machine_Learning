# -*- coding: utf-8 -*-
"""
FCT RunTime CT 분석 파이프라인 (iqz step 전용) - Realtime Loop + MP=2 고정

- Source: a2_fct_table.fct_table
- Filter:
  * station: FCT1~4
  * barcode_information LIKE 'B%'
  * result <> 'FAIL'
  * remark=PD     -> step_description='1.36 Test iqz(uA)'
  * remark=Non-PD -> step_description='1.32 Test iqz(uA)'

- Output:
  1) e1_FCT_ct.fct_run_time_ct
     UNIQUE (station, remark, month), ON CONFLICT DO NOTHING
  2) e1_FCT_ct.fct_upper_outlier_ct_list
     UNIQUE INDEX (station, remark, barcode_information, end_day, end_time), ON CONFLICT DO NOTHING

추가 사양(요청):
- [멀티프로세스] 2개 고정
- [무한 루프] 1초마다 재실행
- [윈도우] end_day = 오늘 기준 같은 월(YYYYMM)만 처리
  예) 20251218 -> '202512%' 범위만
- [실시간] end_ts 기준 현재시간 120초 이내 + cutoff_ts 이후만 처리
- (추가) 실행 타이밍 2회:
    1) 08:27:00 ~ 08:29:59 구간에서만 1초 루프 실행
    2) 20:27:00 ~ 20:29:59 구간에서만 1초 루프 실행
    (그 외 시간에는 대기)
- DataFrame 콘솔 출력 없음 / 진행상황만 표시
"""

import sys
import time
import urllib.parse
from datetime import datetime, timedelta, date, time as dtime
from concurrent.futures import ProcessPoolExecutor, as_completed

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

TARGET_SCHEMA_1 = "e1_FCT_ct"
TARGET_TABLE_1  = "fct_run_time_ct"

TARGET_SCHEMA_2 = "e1_FCT_ct"
TARGET_TABLE_2  = "fct_upper_outlier_ct_list"

# ===== 멀티프로세스 고정 =====
MAX_WORKERS = 2

# ===== 실시간 루프/필터 =====
LOOP_INTERVAL_SEC = 1
REALTIME_WINDOW_SEC = 120
CUTOFF_TS = 1765501841.4473598  # 요청값

# ===== 실행 타이밍(추가) =====
RUN_WINDOWS = [
    (dtime(8, 27, 0),  dtime(8, 29, 59)),
    (dtime(20, 27, 0), dtime(20, 29, 59)),
]


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

# ---- 실행 타이밍 헬퍼(추가) ----
def _now_time() -> dtime:
    return datetime.now().time().replace(microsecond=0)

def _is_in_run_window(t: dtime) -> bool:
    for start_t, end_t in RUN_WINDOWS:
        if start_t <= t <= end_t:
            return True
    return False

def _seconds_until_next_window(t: dtime) -> int:
    if _is_in_run_window(t):
        return 0

    now_sec = t.hour * 3600 + t.minute * 60 + t.second
    starts = []
    for start_t, _ in RUN_WINDOWS:
        s = start_t.hour * 3600 + start_t.minute * 60 + start_t.second
        starts.append(s)

    future = [s for s in starts if s > now_sec]
    if future:
        return min(future) - now_sec

    return (24 * 3600 - now_sec) + min(starts)


# =========================
# 2) 로딩 + 전처리 (이번달 + 최근 120초 + cutoff 이후)
# =========================
def load_source(engine) -> pd.DataFrame:
    """
    - SQL: 오늘 기준 '이번달' 범위만 로드(로드 최소화)
        end_day >= date_trunc('month', CURRENT_DATE)
        end_day <  date_trunc('month', CURRENT_DATE) + interval '1 month'
    - end_ts 생성 후:
        end_ts >= max(now-120s, cutoff_dt)
      만족하는 행만 남김
    """
    log("[1/5] 원본 데이터(이번달) 로딩 시작...")

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
        end_day >= date_trunc('month', CURRENT_DATE)::date
        AND end_day <  (date_trunc('month', CURRENT_DATE) + interval '1 month')::date
        AND station IN ('FCT1','FCT2','FCT3','FCT4')
        AND barcode_information LIKE 'B%%'
        AND result <> 'FAIL'
        AND (
            (remark = 'PD' AND step_description = '1.36 Test iqz(uA)')
            OR
            (remark = 'Non-PD' AND step_description = '1.32 Test iqz(uA)')
        )
    ORDER BY end_day ASC, end_time ASC
    """

    df = pd.read_sql(text(query), engine)
    log(f"[OK] 로딩 완료 (rows={len(df)})")

    if df is None or len(df) == 0:
        return df

    log("[2/5] 전처리 (end_ts/월/run_time 숫자화 + realtime filter)...")

    # 문자열 정리 + run_time 숫자화
    df["end_day"] = df["end_day"].astype(str).str.strip()
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

    # end_ts 생성
    df["end_ts"] = pd.to_datetime(df["end_day"] + " " + df["end_time"], errors="coerce")

    # month
    df["month"] = df["end_day"].str.slice(0, 6)

    # run_time/end_ts NaN 제거
    before = len(df)
    df = df.dropna(subset=["run_time", "end_ts"]).reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        log(f"[INFO] run_time/end_ts NaN 제거: {dropped} rows drop")

    # ===== 실시간 필터 =====
    now_dt = datetime.now()
    window_start = now_dt - timedelta(seconds=REALTIME_WINDOW_SEC)
    cutoff_dt = datetime.fromtimestamp(float(CUTOFF_TS))
    threshold = max(window_start, cutoff_dt)

    before2 = len(df)
    df = df[df["end_ts"] >= threshold].copy()
    after2 = len(df)

    log(f"[INFO] realtime filter: end_ts >= {threshold.strftime('%Y-%m-%d %H:%M:%S')}  (kept {after2}/{before2})")

    # 정렬
    df = df.sort_values(["end_day", "end_time"], ascending=[True, True]).reset_index(drop=True)

    log("[OK] 전처리 완료")
    return df


# =========================
# 3) 요약 생성 (boxplot 통계 + plotly_json) - MP=2
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

def _summary_worker(args):
    station, remark, month, run_time_list = args
    vals = np.asarray(run_time_list, dtype=float)
    name = f"{station}_{remark}_{month}"
    d = boxplot_summary_from_values(vals, name=name)
    d.update({"station": station, "remark": remark, "month": str(month)})
    return d

def build_summary_df(df: pd.DataFrame) -> pd.DataFrame:
    log(f"[3/5] (station, remark, month) 요약 DF 생성... (MP={MAX_WORKERS})")

    if df is None or len(df) == 0:
        return pd.DataFrame(columns=[
            "id","station","remark","month","sample_amount",
            "run_time_lower_outlier","q1","median","q3",
            "run_time_upper_outlier","del_out_run_time_av","plotly_json"
        ])

    group_items = []
    for (station, remark, month), g in df.groupby(["station", "remark", "month"], sort=True):
        group_items.append((station, remark, str(month), g["run_time"].astype(float).tolist()))

    total = len(group_items)
    log(f"[INFO] 그룹 수 = {total}")

    rows = []
    done = 0
    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_summary_worker, item) for item in group_items]
        for fut in as_completed(futures):
            rows.append(fut.result())
            done += 1
            if done == 1 or done == total or done % 20 == 0:
                log(f"[PROGRESS] summary {done}/{total} ...")

    summary_df = pd.DataFrame(rows)
    summary_df = summary_df.sort_values(["station", "remark", "month"]).reset_index(drop=True)
    summary_df.insert(0, "id", np.arange(1, len(summary_df) + 1))

    cols_order = [
        "id", "station", "remark", "month", "sample_amount",
        "run_time_lower_outlier", "q1", "median", "q3",
        "run_time_upper_outlier", "del_out_run_time_av", "plotly_json",
    ]
    summary_df = summary_df[[c for c in cols_order if c in summary_df.columns]]

    log(f"[OK] summary_df 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 4) DB 저장 1: fct_run_time_ct (DO NOTHING 유지)
# =========================
def save_run_time_ct(summary_df: pd.DataFrame):
    log("[4/5] DB 저장(1) fct_run_time_ct 시작...")

    if summary_df is None or len(summary_df) == 0:
        log("[SKIP] summary_df 비어있음 -> 저장 생략")
        return

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
                execute_values(cur, insert_sql, rows, template=template, page_size=1000)

        conn.commit()
        log(f"[OK] fct_run_time_ct 저장 완료 (insert candidates={len(rows)}; 중복키는 PASS)")
    finally:
        conn.close()


# =========================
# 5) Upper outlier 상세 DF 생성 + DB 저장 2 (MP=2)
# =========================
def _upper_outlier_worker(args):
    station, remark, month, rows_list = args
    if not rows_list:
        return []

    run_times = np.asarray([r[3] for r in rows_list], dtype=float)
    if run_times.size == 0:
        return []

    q1 = np.percentile(run_times, 25)
    q3 = np.percentile(run_times, 75)
    iqr = q3 - q1
    upper_th = q3 + 1.5 * iqr
    upper_th_r = round(float(upper_th), 2)

    out = []
    for (barcode_information, end_day, end_time, run_time) in rows_list:
        if run_time is None:
            continue
        try:
            if float(run_time) > upper_th:
                out.append((
                    station, remark, barcode_information, str(end_day), str(end_time),
                    float(run_time), str(month), upper_th_r
                ))
        except Exception:
            continue
    return out

def build_upper_outlier_df(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    log(f"[5/5] upper outlier 상세 DF 생성 시작... (MP={MAX_WORKERS})")

    if df_raw is None or len(df_raw) == 0 or summary_df is None or len(summary_df) == 0:
        log("[OK] upper outlier 없음(입력 데이터 없음)")
        return pd.DataFrame()

    group_map = {}
    for (station, remark, month), g in df_raw.groupby(["station", "remark", "month"], sort=False):
        rows_list = list(zip(
            g["barcode_information"].astype(str),
            g["end_day"].astype(str),
            g["end_time"].astype(str),
            g["run_time"].astype(float),
        ))
        group_map[(station, remark, str(month))] = rows_list

    tasks = []
    for _, r in summary_df.iterrows():
        key = (r["station"], r["remark"], str(r["month"]))
        tasks.append((r["station"], r["remark"], str(r["month"]), group_map.get(key, [])))

    total = len(tasks)
    done = 0
    out_rows = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_upper_outlier_worker, t) for t in tasks]
        for fut in as_completed(futures):
            out_rows.extend(fut.result())
            done += 1
            if done == 1 or done == total or done % 20 == 0:
                log(f"[PROGRESS] outlier scan {done}/{total} ...")

    if not out_rows:
        log("[OK] upper outlier 없음")
        return pd.DataFrame()

    upper_outlier_df = pd.DataFrame(
        out_rows,
        columns=[
            "station", "remark", "barcode_information", "end_day", "end_time",
            "run_time", "month", "upper_threshold"
        ]
    )

    upper_outlier_df = upper_outlier_df.sort_values(
        ["station", "remark", "month", "end_day", "end_time"]
    ).reset_index(drop=True)

    log(f"[OK] upper_outlier_df 생성 완료 (rows={len(upper_outlier_df)})")
    return upper_outlier_df

def save_upper_outlier_df(upper_outlier_df: pd.DataFrame):
    if upper_outlier_df is None or upper_outlier_df.empty:
        log("[SKIP] upper outlier 저장 생략 (데이터 없음)")
        return

    save_df = upper_outlier_df.copy()

    cols = [
        "station", "remark", "barcode_information", "end_day", "end_time",
        "run_time", "month", "upper_threshold",
    ]
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
# 6) 1회 실행(루프 내부)
# =========================
def run_once(engine):
    df_raw = load_source(engine)
    if df_raw is None or len(df_raw) == 0:
        log("[INFO] 처리 대상 데이터 없음 (이번달/120초/cutoff 조건).")
        return

    summary_df = build_summary_df(df_raw)
    save_run_time_ct(summary_df)

    upper_outlier_df = build_upper_outlier_df(df_raw, summary_df)
    save_upper_outlier_df(upper_outlier_df)

    log("=== 1-cycle DONE ===")


# =========================
# main (타임윈도우 기반 1초 루프)
# =========================
def main():
    try:
        log("=== FCT RunTime CT Realtime Loop START (Scheduled) ===")
        log(f"[INFO] MP workers = {MAX_WORKERS} (fixed)")
        log(f"[INFO] end_day = this month (based on today={date.today()})")
        log(f"[INFO] realtime window = {REALTIME_WINDOW_SEC}s, cutoff_ts = {CUTOFF_TS}")
        log(f"[INFO] run_windows = {RUN_WINDOWS}")

        engine = get_engine(DB_CONFIG)

        while True:
            now_t = _now_time()

            # 윈도우 밖이면 대기
            if not _is_in_run_window(now_t):
                wait_sec = _seconds_until_next_window(now_t)
                log(f"[WAIT] now={now_t} -> next window in {wait_sec}s")

                # 시작 시각 정밀하게 맞추기 위해 1초 단위 체크
                while wait_sec > 0:
                    time.sleep(1)
                    wait_sec -= 1
                    now_t = _now_time()
                    if _is_in_run_window(now_t):
                        break
                continue

            # 윈도우 안: 1초 주기 실행
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
