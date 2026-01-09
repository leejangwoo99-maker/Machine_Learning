# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 스크립트
- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
- 결과: d1_machine_log.afa_fail_wasted_time (replace)

[변경사항]
- dayornight 컬럼 제거
- time 컬럼 -> end_time 컬럼으로 변경
- 결과 테이블에서도 from_dorn / to_dorn 제거
- 실행 시작/종료 시각 및 총 소요 시간 출력 추가
"""

import os
import time
from datetime import datetime

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
    table_name, station_label, db_config, schema_name = args
    engine = get_engine(db_config)

    sql = f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {schema_name}."{table_name}"
        ORDER BY end_day ASC, end_time ASC
    """
    df = pd.read_sql(sql, engine)
    df["station"] = station_label
    return df


def load_all_fct_logs_multiprocess(max_workers=None) -> pd.DataFrame:
    tasks = [(t, s, DB_CONFIG, SCHEMA_MACHINE) for t, s in TABLES_FCT]

    dfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(load_fct_log_mp, task) for task in tasks]
        for f in as_completed(futs):
            dfs.append(f.result())

    if not dfs:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    return pd.concat(dfs, ignore_index=True)


# ============================================
# 3. 계산 로직
# ============================================
def compute_afa_fail_wasted(df_all: pd.DataFrame) -> pd.DataFrame:
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
                if not in_manual and pending_from_ts is None:
                    pending_from_ts = ts
                continue

            if contents == OFF_TEXT and pending_from_ts is not None:
                from_ts = pending_from_ts
                to_ts = ts

                result_rows.append(
                    {
                        "end_day": end_day,
                        "station": station,
                        "from_contents": NG_TEXT,
                        "from_time": from_ts.strftime("%H:%M:%S.%f")[:-4],
                        "to_contents": OFF_TEXT,
                        "to_time": to_ts.strftime("%H:%M:%S.%f")[:-4],
                        "wasted_time": round(abs((to_ts - from_ts).total_seconds()), 2),
                    }
                )

                pending_from_ts = None

    df_wasted = pd.DataFrame(result_rows)

    if not df_wasted.empty:
        df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)
        df_wasted.insert(0, "id", range(1, len(df_wasted) + 1))
    else:
        df_wasted = pd.DataFrame(
            columns=[
                "id", "end_day", "station",
                "from_contents", "from_time",
                "to_contents", "to_time",
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
        if_exists="replace",
        index=False,
    )

    print(f"[DONE] {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} 테이블에 {len(df_wasted)}행 저장 완료")


# ============================================
# 5. main
# ============================================
def main():
    start_dt = datetime.now()
    start_ts = time.perf_counter()

    print(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")

    df_all = load_all_fct_logs_multiprocess(max_workers=min(4, os.cpu_count() or 1))
    df_wasted = compute_afa_fail_wasted(df_all)
    save_to_db(df_wasted)

    elapsed = time.perf_counter() - start_ts
    end_dt = datetime.now()

    print(f"[END]   {end_dt:%Y-%m-%d %H:%M:%S}")
    print(f"[TIME]  total_elapsed = {elapsed:.2f} sec ({elapsed/60:.2f} min)")


if __name__ == "__main__":
    freeze_support()
    main()
