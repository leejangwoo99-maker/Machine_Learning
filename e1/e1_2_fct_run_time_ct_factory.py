# -*- coding: utf-8 -*-
"""
FCT RunTime CT 분석 파이프라인 (iqz step 전용) - Realtime Window + MP=2 고정

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

요구사항(추가 반영):
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- [멀티프로세스] 2개 고정 (요청)
- [무한 루프 기능] 1초마다 재실행
- [윈도우] 08:27:00 ~ 08:29:59, 20:27:00 ~ 20:29:59 에만 실행
- [유효 날짜 범위] end_day가 "현재 날짜 기준의 달(YYYYMM)"만 해당
- [실시간] 현재 시간 기준 120초 이내 데이터만 반영 (end_day+end_time -> end_ts 기준)
- [미완성 방지 역할 분리] 이 스크립트는 파일을 직접 파싱하지 않고 DB만 읽음
  * 따라서 "파일 크기/mtime/lock" 같은 파일 안정화 로직은 a2(파서/적재) 쪽에서 처리
  * 본 스크립트(e*)는 DB에서 최근 120초 범위 + 안정화 버퍼(STABLE_DATA_SEC) 적용으로 안전하게 조회

주의(필수 확인):
- a2_fct_table.fct_table 의 end_day 포맷이 'YYYYMMDD' 인지, 'YYYY-MM-DD' 인지에 따라 SQL 파싱이 달라집니다.
  현재 코드는 end_day가 'YYYYMMDD'라고 가정하고 to_timestamp(end_day||end_time, 'YYYYMMDDHH24MISS') 를 사용합니다.
  만약 end_time이 'HHMMSS'가 아니라 'HH:MM:SS'이면, 아래의 ENDTIME_FORMAT 을 'HH24:MI:SS' 형태로 바꿔주세요.
"""

import sys
import urllib.parse
from datetime import datetime, time as dtime
import time as time_mod
from multiprocessing import freeze_support
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

# =========================
# Realtime Loop 사양(요청 반영)
# =========================
# ✅ 워커 2개 고정
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

# end_day/end_time 포맷 가정
# end_day: 'YYYYMMDD'
# end_time: 'HHMMSS' (6자리)
ENDTS_FORMAT = "YYYYMMDDHH24MISS"


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
# 2) 로딩 + 전처리 (현재달 + 최근120초 + 안정화버퍼)
# =========================
def load_source(engine) -> pd.DataFrame:
    """
    - end_day: 현재달(YYYYMM)만
    - end_ts: now-RECENT_SECONDS ~ now-STABLE_DATA_SEC 만
    """
    yyyymm = current_yyyymm()

    query = f"""
    WITH base AS (
        SELECT
            station,
            remark,
            barcode_information,
            step_description,
            result,
            end_day,
            end_time,
            run_time,
            to_timestamp(end_day || end_time, '{ENDTS_FORMAT}') AS end_ts
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
            AND substring(end_day from 1 for 6) = :yyyymm
    )
    SELECT
        station, remark, barcode_information, step_description, result, end_day, end_time, run_time
    FROM base
    WHERE
        end_ts IS NOT NULL
        AND end_ts >= (now() - INTERVAL '{RECENT_SECONDS} seconds')
        AND end_ts <= (now() - INTERVAL '{STABLE_DATA_SEC} seconds')
    ORDER BY end_ts ASC
    """

    log("[1/5] 원본 데이터 로딩 시작(현재달 + 최근120초 + 안정화버퍼)...")
    df = pd.read_sql(text(query), engine, params={"yyyymm": yyyymm})
    log(f"[OK] 로딩 완료 (rows={len(df)})")

    if len(df) == 0:
        return df

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
        log("[SKIP] 입력 데이터 없음 -> summary_df 빈 DF 반환")
        return pd.DataFrame(columns=[
            "id","station","remark","month","sample_amount","run_time_lower_outlier",
            "q1","median","q3","run_time_upper_outlier","del_out_run_time_av","plotly_json"
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
# 4) DB 저장 1: fct_run_time_ct (ON CONFLICT DO NOTHING)
# =========================
def save_run_time_ct(summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        log("[SKIP] fct_run_time_ct 저장 생략 (데이터 없음)")
        return

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
        log("[SKIP] 입력 데이터 없음 -> upper outlier 생성 생략")
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
    ).sort_values(
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
# ONE SHOT
# =========================
def main_once():
    log("=== FCT RunTime CT Pipeline RUN (ONE SHOT) ===")
    log(f"[INFO] MP workers={MAX_WORKERS} | month={current_yyyymm()} | recent={RECENT_SECONDS}s | stable_buf={STABLE_DATA_SEC}s")

    engine = get_engine(DB_CONFIG)

    df_raw = load_source(engine)
    if df_raw is None or len(df_raw) == 0:
        log("[SKIP] 최근 데이터 없음 -> 저장 생략")
        return

    summary_df = build_summary_df(df_raw)
    save_run_time_ct(summary_df)

    upper_outlier_df = build_upper_outlier_df(df_raw, summary_df)
    save_upper_outlier_df(upper_outlier_df)

    log("=== DONE (ONE SHOT) ===")


# =========================
# Realtime loop (윈도우 시간에만)
# =========================
def realtime_loop():
    log("=== FCT RunTime CT Realtime Loop START ===")
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
        log(f"\n[ERROR] Unhandled exception: {repr(e)}")
        exit_code = 1
    finally:
        # Nuitka / PyInstaller EXE로 실행된 경우에만 콘솔 유지
        if getattr(sys, "frozen", False):
            print("\n[INFO] 프로그램이 종료되었습니다.")
            input("Press Enter to exit...")

    sys.exit(exit_code)
