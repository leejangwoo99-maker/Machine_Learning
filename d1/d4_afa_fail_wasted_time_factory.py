# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 (실시간 루프)
- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
- 결과: d1_machine_log.afa_fail_wasted_time (replace)

요구사항:
1) 1초마다 재실행 무한루프
2) 멀티프로세스 2개 고정
3) end_day = 오늘 날짜 기준 같은 월(YYYYMM)만 처리
   예) 20251218 -> '202512%' 범위만
4) 지정 컬럼 중복 제거 (SELECT DISTINCT 효과)
5) 실시간: 현재시간 기준 120초 이내 데이터만 처리

추가(실행 타이밍):
- 08:27:00 ~ 08:29:59 구간에만 1초 루프 실행
- 20:27:00 ~ 20:29:59 구간에만 1초 루프 실행
(그 외 시간에는 대기)
"""

import time as pytime
from datetime import datetime, time as dtime
import pandas as pd
from sqlalchemy import create_engine
import urllib.parse
from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor, as_completed


# ============================================
# 0. DB / 상수 설정
# ============================================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_MACHINE = "d1_machine_log"

TABLES_FCT = [
    ("FCT1_machine_log", "FCT1"),
    ("FCT2_machine_log", "FCT2"),
    ("FCT3_machine_log", "FCT3"),
    ("FCT4_machine_log", "FCT4"),
]

NG_TEXT  = "제품 감지 NG"
OFF_TEXT = "제품 검사 투입요구 ON"
MANUAL_TEXT = "Manual mode 전환"
AUTO_TEXT   = "Auto mode 전환"

TABLE_SAVE_SCHEMA = "d1_machine_log"
TABLE_SAVE_NAME   = "afa_fail_wasted_time"

# (2) 멀티프로세스 2개 고정
MAX_WORKERS = 2

# (5) 실시간 윈도우 (초)
REALTIME_WINDOW_SEC = 120

# (선택) epoch cutoff 강제 적용하고 싶으면 아래에 float epoch 넣기 (없으면 None)
FORCE_CUTOFF_TS = None


# ============================================
# 실행 타이밍(추가)
# ============================================
RUN_WINDOWS = [
    (dtime(8, 27, 0),  dtime(8, 29, 59)),
    (dtime(20, 27, 0), dtime(20, 29, 59)),
]


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


# ============================================
# 1. DB 유틸
# ============================================
def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)


# ============================================
# 2. FCT 로그 로드 (프로세스 단위 실행)
# ============================================
def load_fct_log_mp(args):
    table_name, station_label, db_config, schema_name, realtime_window_sec, force_cutoff_ts = args
    engine = get_engine(db_config)

    if force_cutoff_ts is None:
        ts_filter = (
            f"(to_date(end_day::text,'YYYYMMDD') + end_time::time) "
            f">= (now() - interval '{int(realtime_window_sec)} seconds')"
        )
    else:
        ts_filter = (
            f"(to_date(end_day::text,'YYYYMMDD') + end_time::time) "
            f">= to_timestamp({float(force_cutoff_ts)})"
        )

    # (3) 오늘 날짜 기준 같은 월(YYYYMM)만 처리
    month_filter = "end_day::text LIKE (to_char(CURRENT_DATE,'YYYYMM') || '%')"

    sql = f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {schema_name}."{table_name}"
        WHERE
            {month_filter}
            AND contents IN (%(ng)s, %(off)s, %(manual)s, %(auto)s)
            AND {ts_filter}
        ORDER BY end_day ASC, end_time ASC
    """

    df = pd.read_sql(
        sql,
        engine,
        params={"ng": NG_TEXT, "off": OFF_TEXT, "manual": MANUAL_TEXT, "auto": AUTO_TEXT},
    )
    df["station"] = station_label
    return df


def load_all_fct_logs_multiprocess(max_workers=MAX_WORKERS) -> pd.DataFrame:
    tasks = [
        (t, s, DB_CONFIG, SCHEMA_MACHINE, REALTIME_WINDOW_SEC, FORCE_CUTOFF_TS)
        for t, s in TABLES_FCT
    ]

    dfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(load_fct_log_mp, task) for task in tasks]
        for f in as_completed(futs):
            dfs.append(f.result())

    if not dfs:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    df_all = pd.concat(dfs, ignore_index=True)
    df_all = df_all.dropna(subset=["end_day", "end_time", "contents", "station"])
    return df_all


# ============================================
# 3. 계산 로직
# ============================================
def compute_afa_fail_wasted(df_all: pd.DataFrame) -> pd.DataFrame:
    base_cols = [
        "id", "end_day", "station",
        "from_contents", "from_time",
        "to_contents", "to_time",
        "wasted_time",
    ]

    if df_all.empty:
        return pd.DataFrame(columns=base_cols)

    df_evt = df_all[df_all["contents"].isin([NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT])].copy()
    df_evt = df_evt.sort_values(["end_day", "station", "end_time"]).reset_index(drop=True)

    result_rows = []

    for (end_day, station), grp in df_evt.groupby(["end_day", "station"], sort=False):
        pending_from_ts = None
        in_manual = False

        for _, row in grp.iterrows():
            contents = row["contents"]
            t = row["end_time"]

            ts = pd.to_datetime(f"{str(end_day)} {str(t)}", errors="coerce")
            if pd.isna(ts):
                continue

            if contents == MANUAL_TEXT:
                in_manual = True
                continue
            if contents == AUTO_TEXT:
                in_manual = False
                continue

            if contents == NG_TEXT:
                if in_manual:
                    continue
                if pending_from_ts is None:
                    pending_from_ts = ts
                continue

            if contents == OFF_TEXT and pending_from_ts is not None:
                from_ts = pending_from_ts
                to_ts = ts

                from_str = from_ts.strftime("%H:%M:%S.%f")[:-4]
                to_str   = to_ts.strftime("%H:%M:%S.%f")[:-4]
                wasted = round(abs((to_ts - from_ts).total_seconds()), 2)

                result_rows.append(
                    {
                        "end_day": end_day,
                        "station": station,
                        "from_contents": NG_TEXT,
                        "from_time": from_str,
                        "to_contents": OFF_TEXT,
                        "to_time": to_str,
                        "wasted_time": wasted,
                    }
                )

                pending_from_ts = None

    df_wasted = pd.DataFrame(result_rows)

    # (4) 지정 컬럼 기준 중복 제거 (SELECT DISTINCT 효과)
    distinct_cols = [
        "end_day", "station",
        "from_contents", "from_time",
        "to_contents", "to_time",
    ]

    if not df_wasted.empty:
        df_wasted = df_wasted.drop_duplicates(subset=distinct_cols, keep="first")
        df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)
        df_wasted.insert(0, "id", range(1, len(df_wasted) + 1))
        df_wasted = df_wasted[base_cols]
    else:
        df_wasted = pd.DataFrame(columns=base_cols)

    return df_wasted


# ============================================
# 4. DB 저장
# ============================================
def save_to_db(df_wasted: pd.DataFrame):
    engine = get_engine(DB_CONFIG)
    df_wasted.to_sql(
        TABLE_SAVE_NAME,
        con=engine,
        schema=TABLE_SAVE_SCHEMA,
        if_exists="replace",
        index=False,
    )
    print(f"[DONE] {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} 저장: {len(df_wasted)} rows", flush=True)


# ============================================
# 5. main (타임윈도우 기반 1초 루프)
# ============================================
def main_loop():
    print("[INFO] AFA FAIL wasted time scheduled realtime loop start", flush=True)
    print(f"[INFO] workers={MAX_WORKERS}, realtime_window={REALTIME_WINDOW_SEC}s, force_cutoff_ts={FORCE_CUTOFF_TS}", flush=True)
    print("[INFO] run_windows =", RUN_WINDOWS, flush=True)

    while True:
        now_t = _now_time()

        # 윈도우 밖이면 다음 윈도우까지 대기
        if not _is_in_run_window(now_t):
            wait_sec = _seconds_until_next_window(now_t)
            print(f"[WAIT] now={now_t} -> next window in {wait_sec}s", flush=True)
            while wait_sec > 0:
                pytime.sleep(1)
                wait_sec -= 1
                now_t = _now_time()
                if _is_in_run_window(now_t):
                    break
            continue

        # 윈도우 안: 1초 루프 실행
        try:
            df_all = load_all_fct_logs_multiprocess(max_workers=MAX_WORKERS)
            df_wasted = compute_afa_fail_wasted(df_all)
            save_to_db(df_wasted)
        except Exception as e:
            print("[ERROR]", repr(e), flush=True)

        pytime.sleep(1)


if __name__ == "__main__":
    freeze_support()
    main_loop()
