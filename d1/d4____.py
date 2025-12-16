# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 스크립트
- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
- 결과: d1_machine_log.afa_fail_wasted_time (replace)
"""

import os
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
#    - 멀티프로세스에서는 엔진을 프로세스 내부에서 생성해야 함
# ============================================
def load_fct_log_mp(args):
    table_name, station_label, db_config, schema_name = args

    engine = get_engine(db_config)

    sql = f"""
        SELECT
            end_day,
            time,
            contents,
            dayornight
        FROM {schema_name}."{table_name}"
        ORDER BY end_day ASC, time ASC
    """
    df = pd.read_sql(sql, engine)
    df["station"] = station_label
    return df


def load_all_fct_logs_multiprocess(max_workers=None) -> pd.DataFrame:
    tasks = [
        (t, s, DB_CONFIG, SCHEMA_MACHINE)
        for t, s in TABLES_FCT
    ]

    dfs = []
    # max_workers 미지정이면 CPU 코어수 기반으로 동작
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(load_fct_log_mp, task) for task in tasks]
        for f in as_completed(futs):
            dfs.append(f.result())

    df_all = pd.concat(dfs, ignore_index=True)
    return df_all


# ============================================
# 3. 계산 로직
# ============================================
def compute_afa_fail_wasted(df_all: pd.DataFrame) -> pd.DataFrame:
    # 이벤트만 필터링
    df_evt = df_all[df_all["contents"].isin([NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT])].copy()

    # 정렬
    df_evt = df_evt.sort_values(["end_day", "station", "time"]).reset_index(drop=True)

    result_rows = []

    # end_day, station 단위 처리
    for (end_day, station), grp in df_evt.groupby(["end_day", "station"], sort=False):
        pending_from_ts = None
        pending_from_dorn = None
        in_manual = False

        for _, row in grp.iterrows():
            contents = row["contents"]
            t = row["time"]
            dorn = row["dayornight"]

            # end_day는 20251001 같은 정수/문자일 수 있으므로 문자열로 안전 변환
            ts = pd.to_datetime(f"{str(end_day)} {str(t)}", errors="coerce")
            if pd.isna(ts):
                # 시간 파싱이 실패하면 스킵
                continue

            # Manual / Auto 상태
            if contents == MANUAL_TEXT:
                in_manual = True
                continue
            if contents == AUTO_TEXT:
                in_manual = False
                continue

            # NG 처리
            if contents == NG_TEXT:
                if in_manual:
                    continue

                # 연속 NG면 첫 NG만 유지
                if pending_from_ts is None:
                    pending_from_ts = ts
                    pending_from_dorn = dorn
                continue

            # OFF 처리 (pending NG가 있을 때만 페어링)
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

    if not df_wasted.empty:
        df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)
        df_wasted.insert(0, "id", range(1, len(df_wasted) + 1))
    else:
        # 컬럼 고정 (빈 결과여도 테이블 스키마 유지 목적)
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
        if_exists="replace",   # 누적이면 "append"
        index=False,
    )

    print(f"[DONE] {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} 테이블에 {len(df_wasted)}행 저장 완료")


# ============================================
# 5. main
# ============================================
def main():
    # (1) 멀티프로세스로 FCT1~4 로드
    df_all = load_all_fct_logs_multiprocess(max_workers=min(4, os.cpu_count() or 1))

    # (2) 계산
    df_wasted = compute_afa_fail_wasted(df_all)

    # (3) DB 저장
    save_to_db(df_wasted)


if __name__ == "__main__":
    freeze_support()  # Windows 멀티프로세스 안전장치
    main()