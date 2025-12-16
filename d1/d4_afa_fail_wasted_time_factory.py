# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 (실시간 루프)
- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
- 결과: d1_machine_log.afa_fail_wasted_time (replace)
요구사항 추가:
1) 1초마다 재실행 무한루프
2) 멀티프로세스 2개 고정
3) end_day = 오늘 날짜만
4) 지정 컬럼 중복 제거 (SELECT DISTINCT 효과)
5) 실시간: 현재시간 기준 120초 이내 데이터만 처리
"""

import os
import time as pytime
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
# 예: 1765501841.4473598
FORCE_CUTOFF_TS = None


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
#    - 오늘 날짜 + 최근 120초 + 이벤트 텍스트만 DB에서 필터링
# ============================================
def load_fct_log_mp(args):
    table_name, station_label, db_config, schema_name, realtime_window_sec, force_cutoff_ts = args
    engine = get_engine(db_config)

    # end_day(YYYYMMDD) + time 을 timestamp로 조합
    # - end_day: int/str 모두 대응 위해 end_day::text 사용
    # - time: 컬럼명이 time 이므로 충돌 방지를 위해 "time" 로 명시
    #
    # 조건:
    #  (3) end_day = CURRENT_DATE
    #  (5) ts >= NOW() - interval '120 seconds'  (또는 FORCE_CUTOFF_TS 있으면 그 기준)
    #
    # FORCE_CUTOFF_TS가 있으면: to_timestamp(force_cutoff_ts) 기준
    if force_cutoff_ts is None:
        ts_filter = f"(to_date(end_day::text,'YYYYMMDD') + \"time\"::time) >= (now() - interval '{int(realtime_window_sec)} seconds')"
    else:
        ts_filter = f"(to_date(end_day::text,'YYYYMMDD') + \"time\"::time) >= to_timestamp({float(force_cutoff_ts)})"

    sql = f"""
        SELECT
            end_day,
            "time" as time,
            contents,
            dayornight
        FROM {schema_name}."{table_name}"
        WHERE
            to_date(end_day::text,'YYYYMMDD') = CURRENT_DATE
            AND contents IN (%(ng)s, %(off)s, %(manual)s, %(auto)s)
            AND {ts_filter}
        ORDER BY end_day ASC, "time" ASC
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
        return pd.DataFrame(columns=["end_day", "time", "contents", "dayornight", "station"])

    df_all = pd.concat(dfs, ignore_index=True)

    # 혹시 모를 안전장치: 파이썬에서도 time 파싱 실패행 제거
    df_all = df_all.dropna(subset=["end_day", "time", "contents", "station"])
    return df_all


# ============================================
# 3. 계산 로직
# ============================================
def compute_afa_fail_wasted(df_all: pd.DataFrame) -> pd.DataFrame:
    if df_all.empty:
        return pd.DataFrame(
            columns=[
                "id", "end_day", "station",
                "from_contents", "from_time", "from_dorn",
                "to_contents", "to_time", "to_dorn",
                "wasted_time",
            ]
        )

    # 이벤트만 필터링(이미 SQL에서 제한했지만 안전)
    df_evt = df_all[df_all["contents"].isin([NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT])].copy()

    # 정렬: 같은 end_day 내 station별 time 순
    df_evt = df_evt.sort_values(["end_day", "station", "time"]).reset_index(drop=True)

    result_rows = []

    for (end_day, station), grp in df_evt.groupby(["end_day", "station"], sort=False):
        pending_from_ts = None
        pending_from_dorn = None
        in_manual = False

        for _, row in grp.iterrows():
            contents = row["contents"]
            t = row["time"]
            dorn = row["dayornight"]

            ts = pd.to_datetime(f"{str(end_day)} {str(t)}", errors="coerce")
            if pd.isna(ts):
                continue

            # Manual / Auto 구간
            if contents == MANUAL_TEXT:
                in_manual = True
                continue
            if contents == AUTO_TEXT:
                in_manual = False
                continue

            # NG
            if contents == NG_TEXT:
                if in_manual:
                    continue
                if pending_from_ts is None:
                    pending_from_ts = ts
                    pending_from_dorn = dorn
                continue

            # OFF
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
                        "from_dorn": pending_from_dorn,
                        "to_contents": OFF_TEXT,
                        "to_time": to_str,
                        "to_dorn": dorn,
                        "wasted_time": wasted,
                    }
                )

                pending_from_ts = None
                pending_from_dorn = None

    df_wasted = pd.DataFrame(result_rows)

    # (4) 지정 컬럼 기준 중복 제거 (SELECT DISTINCT 효과)
    distinct_cols = [
        "end_day", "station",
        "from_contents", "from_time", "from_dorn",
        "to_contents", "to_time", "to_dorn",
    ]
    if not df_wasted.empty:
        df_wasted = df_wasted.drop_duplicates(subset=distinct_cols, keep="first")
        df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)
        df_wasted.insert(0, "id", range(1, len(df_wasted) + 1))
    else:
        df_wasted = pd.DataFrame(
            columns=[
                "id", "end_day", "station",
                "from_contents", "from_time", "from_dorn",
                "to_contents", "to_time", "to_dorn",
                "wasted_time",
            ]
        )

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
        if_exists="replace",   # 요구사항 유지
        index=False,
    )
    print(f"[DONE] {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} 저장: {len(df_wasted)} rows")


# ============================================
# 5. main (1초 무한루프)
# ============================================
def main_loop():
    print("[INFO] AFA FAIL wasted time realtime loop start")
    print(f"[INFO] workers={MAX_WORKERS}, realtime_window={REALTIME_WINDOW_SEC}s, force_cutoff_ts={FORCE_CUTOFF_TS}")

    while True:
        try:
            df_all = load_all_fct_logs_multiprocess(max_workers=MAX_WORKERS)
            df_wasted = compute_afa_fail_wasted(df_all)
            save_to_db(df_wasted)
        except Exception as e:
            # 루프가 죽지 않게 로그만 남기고 계속
            print("[ERROR]", repr(e))

        pytime.sleep(1)


if __name__ == "__main__":
    freeze_support()
    main_loop()
