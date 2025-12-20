# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 (Realtime Loop)
- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
- 결과: d1_machine_log.afa_fail_wasted_time (replace)

추가 조건(요청 반영)
1) 멀티프로세스 2개 고정
2) 1초마다 무한루프 재실행
3) 유효 날짜: end_day = 오늘(YYYYMMDD)만
4) 실시간: 현재 시간 기준 60초 이내 새롭게 추가된 데이터만 처리
5) "파일 저장 중(미완성) 파싱 방지"에 준하는 보호 로직:
   - DB 레코드에서 end_time/contents 등 핵심값이 비정상인 row 제외
   - ts_filter(최근 60초)로 확정된 이벤트만 대상으로 계산
"""

import os
import time as pytime
import urllib.parse
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor, as_completed

# chained assignment를 에러로 승격(디버그용)
pd.options.mode.chained_assignment = "raise"

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

# [멀티프로세스] 2개 고정
MAX_WORKERS = 2

# [실시간] 현재 시간 기준 60초
REALTIME_WINDOW_SEC = 60

# [무한 루프] 1초마다
LOOP_INTERVAL_SEC = 1


def _log(msg: str):
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] {msg}", flush=True)


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

    # 운영 안정성(추천): connect_timeout
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        connect_args={"connect_timeout": 5},
    )


# ============================================
# 2. FCT 로그 로드 (프로세스 단위 실행)
#    - 멀티프로세스에서는 엔진을 프로세스 내부에서 생성해야 함
# ============================================
def load_fct_log_mp(args):
    table_name, station_label, db_config, schema_name, window_sec = args
    engine = get_engine(db_config)

    # 오늘 날짜(YYYYMMDD)만
    # end_day가 TEXT/INT 혼재 가능 → ::text로 통일
    today_yyyymmdd = datetime.now().strftime("%Y%m%d")

    # "파일 저장 중(미완성) 방지"에 준하는 방어:
    # - end_time이 NULL/빈값이면 제외
    # - contents가 NULL/빈값이면 제외
    # - end_day가 오늘이면서, (end_day + end_time) timestamp가 now()-60sec 이상인 것만
    sql = text(f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {schema_name}."{table_name}"
        WHERE
            end_day::text = :today
            AND contents IS NOT NULL AND trim(contents) <> ''
            AND end_time IS NOT NULL AND trim(end_time::text) <> ''
            AND contents IN (:ng, :off, :manual, :auto)
            AND (to_date(end_day::text,'YYYYMMDD') + end_time::time)
                >= (now() - (:win_sec || ' seconds')::interval)
        ORDER BY end_day ASC, end_time ASC
    """)

    df = pd.read_sql(
        sql,
        engine,
        params={
            "today": today_yyyymmdd,
            "ng": NG_TEXT,
            "off": OFF_TEXT,
            "manual": MANUAL_TEXT,
            "auto": AUTO_TEXT,
            "win_sec": int(window_sec),
        },
    ).copy()

    # chained assignment 방지
    df.loc[:, "station"] = station_label
    return df


def load_all_fct_logs_multiprocess(max_workers=MAX_WORKERS) -> pd.DataFrame:
    tasks = [(t, s, DB_CONFIG, SCHEMA_MACHINE, REALTIME_WINDOW_SEC) for t, s in TABLES_FCT]

    dfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(load_fct_log_mp, task) for task in tasks]
        for f in as_completed(futs):
            dfs.append(f.result())

    if not dfs:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    df_all = pd.concat(dfs, ignore_index=True)

    # 방어: 핵심 컬럼 결측 제거
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

    # 이벤트만 필터링
    df_evt = df_all[df_all["contents"].isin([NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT])].copy()

    # 정렬
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

    # DISTINCT 효과(실시간 루프 중 중복 방지)
    distinct_cols = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time"]

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

    _log(f"[DONE] {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} saved rows={len(df_wasted):,}")


# ============================================
# 5. 무한 루프 (1초)
# ============================================
def run_once():
    df_all = load_all_fct_logs_multiprocess(max_workers=MAX_WORKERS)
    df_wasted = compute_afa_fail_wasted(df_all)
    save_to_db(df_wasted)


def main_loop():
    _log(f"[START] realtime loop | workers={MAX_WORKERS} | window={REALTIME_WINDOW_SEC}s | interval={LOOP_INTERVAL_SEC}s")
    while True:
        try:
            run_once()
        except Exception as e:
            _log(f"[ERROR] {repr(e)}")
        pytime.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    freeze_support()
    main_loop()
