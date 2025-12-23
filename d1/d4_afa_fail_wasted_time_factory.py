# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 스크립트 (Realtime)

- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시

[변경사항/요청사항 통합 반영]
1) "멀티프로세스" 2개 고정 -> ✅ 스레드 2개 고정(ThreadPoolExecutor)
2) 1초마다 무한루프 재실행
3) 유효 날짜: end_day = 오늘(YYYYMMDD)만
4) 실시간: 현재 시간 기준 60초 이내(ts_filter) 데이터만 "결과로 채택"
   - 쿼리 단계에서는 컨텍스트 확보를 위해 lookback(기본 120초) 로드
   - 최종 결과는 from/to 모두 cutoff(최근 60초) 이상인 페어만 저장
5) "파일 저장 중(미완성) 파싱 방지" 준하는 보호 로직:
   - DB 레코드에서 end_time/contents 등 핵심값 비정상 row 제외(쿼리 + 파이썬 이중 방어)
   - ts_filter로 확정된 이벤트만 계산/저장

[저장 방식]
- 테이블: d1_machine_log.afa_fail_wasted_time
- append 저장 + 중복 방지(UNIQUE KEY) + ON CONFLICT DO NOTHING

[EXE/Nuitka 반영]
- 콘솔 자동 종료 방지(Enter 대기)
- 단계별 진행상태 출력(실시간 flush)
- 예외 발생 시 traceback 출력 후 콘솔 유지 + exit code 1
"""

import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta
from multiprocessing import freeze_support
from concurrent.futures import ThreadPoolExecutor, as_completed  # ✅ 변경

import pandas as pd
from sqlalchemy import create_engine, text
import psycopg2
from psycopg2.extras import execute_values

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

NG_TEXT = "제품 감지 NG"
OFF_TEXT = "제품 검사 투입요구 ON"
MANUAL_TEXT = "Manual mode 전환"
AUTO_TEXT = "Auto mode 전환"

VALID_CONTENTS = (NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT)

TABLE_SAVE_SCHEMA = "d1_machine_log"
TABLE_SAVE_NAME = "afa_fail_wasted_time"

# 요청 반영
MP_WORKERS_FIXED = 2          # ✅ 이제 "스레드 개수" 의미로 사용
LOOP_INTERVAL_SEC = 1
TS_WINDOW_SEC = 60
QUERY_LOOKBACK_SEC = 120

PROGRESS_EVERY_GROUPS = 50


# ============================================
# 공용: 로그/콘솔 대기
# ============================================
def log(msg: str):
    print(msg, flush=True)


def pause_console():
    try:
        input("\n[END] 작업이 종료되었습니다. 콘솔을 닫으려면 Enter를 누르세요...")
    except Exception:
        pass


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
    return create_engine(conn_str, pool_pre_ping=True)


def get_psycopg2_conn(config=DB_CONFIG):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
    )


# ============================================
# 2. 저장 테이블 준비(UNIQUE + append)
# ============================================
def ensure_save_table():
    engine = get_engine(DB_CONFIG)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (
        id           BIGSERIAL PRIMARY KEY,
        end_day      TEXT NOT NULL,
        station      TEXT NOT NULL,
        from_contents TEXT NOT NULL,
        from_time    TEXT NOT NULL,
        to_contents  TEXT NOT NULL,
        to_time      TEXT NOT NULL,
        wasted_time  NUMERIC(10,2) NOT NULL,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    uniq_sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_SAVE_NAME}_dedup
    ON {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (end_day, station, from_time, to_time, from_contents, to_contents);
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        conn.execute(text(uniq_sql))


# ============================================
# 3. FCT 로그 로드 (스레드 작업 단위로 사용)
# ============================================
def load_fct_log_mp(args):
    table_name, station_label, db_config, schema_name, today_yyyymmdd, cutoff_dt = args
    t0 = time.perf_counter()

    engine = get_engine(db_config)

    sql = f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {schema_name}."{table_name}"
        WHERE
            end_day = :today
            AND contents = ANY(:valid_contents)
            AND end_time ~ '^[0-2][0-9]:[0-5][0-9]:[0-5][0-9](\\.[0-9]{{1,6}})?$'
            AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS.MS') >= :cutoff
        ORDER BY end_day ASC, end_time ASC
    """

    df = pd.read_sql(
        sql,
        engine,
        params={
            "today": today_yyyymmdd,
            "valid_contents": list(VALID_CONTENTS),
            "cutoff": cutoff_dt,
        },
    ).copy()

    df.loc[:, "station"] = station_label
    elapsed = time.perf_counter() - t0
    return station_label, len(df), elapsed, df


def load_all_fct_logs_multiprocess(today_yyyymmdd: str, cutoff_dt: datetime) -> pd.DataFrame:
    """
    ✅ 기존 함수명을 유지하지만, 내부는 ThreadPoolExecutor로 동작.
    (호출부 변경 최소화 목적)
    """
    tasks = [
        (t, s, DB_CONFIG, SCHEMA_MACHINE, today_yyyymmdd, cutoff_dt)
        for t, s in TABLES_FCT
    ]

    log(f"[LOAD] 오늘={today_yyyymmdd} | lookback_cutoff={cutoff_dt:%Y-%m-%d %H:%M:%S} | threads={MP_WORKERS_FIXED}")

    dfs = []
    with ThreadPoolExecutor(max_workers=MP_WORKERS_FIXED) as ex:  # ✅ 변경
        futs = [ex.submit(load_fct_log_mp, task) for task in tasks]

        done_cnt = 0
        for f in as_completed(futs):
            station_label, nrows, elapsed, df = f.result()
            dfs.append(df)
            done_cnt += 1
            log(f"  - [{done_cnt}/{len(tasks)}] {station_label} 로드: rows={nrows:,} | {elapsed:.2f}s")

    if not dfs:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    df_all = pd.concat(dfs, ignore_index=True)
    log(f"[LOAD] 합계 rows={len(df_all):,}")
    return df_all


# ============================================
# 4. 계산 로직(최근 60초 확정 이벤트만 결과로 채택)
# ============================================
def _is_valid_row_basic(end_day, end_time, contents) -> bool:
    if end_day is None or end_time is None or contents is None:
        return False
    s_end_day = str(end_day)
    s_end_time = str(end_time)
    s_contents = str(contents)

    if not re.fullmatch(r"\d{8}", s_end_day):
        return False
    if s_contents not in VALID_CONTENTS:
        return False
    if not re.fullmatch(r"[0-2]\d:[0-5]\d:[0-5]\d:[0-5]\d(\.\d{1,6})?", s_end_time):
        return False
    return True


def compute_afa_fail_wasted(df_all: pd.DataFrame, ts_filter_cutoff: datetime) -> pd.DataFrame:
    if df_all.empty:
        return pd.DataFrame(columns=[
            "end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"
        ])

    log(f"[CALC] ts_filter_cutoff={ts_filter_cutoff:%Y-%m-%d %H:%M:%S} (최근 {TS_WINDOW_SEC}s 확정 이벤트만 결과)")

    mask_valid = df_all.apply(
        lambda r: _is_valid_row_basic(r["end_day"], r["end_time"], r["contents"]),
        axis=1
    )
    df_evt = df_all.loc[mask_valid].copy()
    if df_evt.empty:
        log("[CALC] 유효 이벤트가 없습니다(비정상 row 제거 후).")
        return pd.DataFrame(columns=[
            "end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"
        ])

    df_evt = df_evt.sort_values(["end_day", "station", "end_time"]).reset_index(drop=True)

    result_rows = []

    groups = list(df_evt.groupby(["end_day", "station"], sort=False))
    total_groups = len(groups)
    log(f"[CALC] 그룹 수(end_day x station): {total_groups:,}")

    grp_cnt = 0
    for (end_day, station), grp in groups:
        grp_cnt += 1
        if grp_cnt == 1 or grp_cnt % PROGRESS_EVERY_GROUPS == 0 or grp_cnt == total_groups:
            log(f"  - 진행: {grp_cnt:,}/{total_groups:,} (현재 {end_day}, {station})")

        pending_from_ts = None
        in_manual = False

        for _, row in grp.iterrows():
            contents = row["contents"]
            end_time = row["end_time"]

            ts = pd.to_datetime(f"{str(end_day)} {str(end_time)}", errors="coerce")
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

                if from_ts < ts_filter_cutoff or to_ts < ts_filter_cutoff:
                    pending_from_ts = None
                    continue

                from_str = from_ts.strftime("%H:%M:%S.%f")[:-4]
                to_str = to_ts.strftime("%H:%M:%S.%f")[:-4]
                wasted = round(abs((to_ts - from_ts).total_seconds()), 2)

                result_rows.append({
                    "end_day": str(end_day),
                    "station": str(station),
                    "from_contents": NG_TEXT,
                    "from_time": from_str,
                    "to_contents": OFF_TEXT,
                    "to_time": to_str,
                    "wasted_time": wasted,
                })

                pending_from_ts = None

    df_wasted = pd.DataFrame(result_rows)
    if df_wasted.empty:
        log("[CALC] 생성된 결과가 없습니다(최근 60초 조건 또는 이벤트 페어 미충족).")
        return df_wasted

    df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)
    log(f"[CALC] 결과 rows={len(df_wasted):,}")
    return df_wasted


# ============================================
# 5. DB 저장(append + ON CONFLICT DO NOTHING)
# ============================================
def save_to_db_append_on_conflict(df_wasted: pd.DataFrame):
    if df_wasted is None or df_wasted.empty:
        log("[SAVE] 저장할 데이터 없음.")
        return

    cols = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]
    rows = [tuple(x) for x in df_wasted[cols].to_numpy()]

    insert_sql = f"""
        INSERT INTO {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME}
        (end_day, station, from_contents, from_time, to_contents, to_time, wasted_time)
        VALUES %s
        ON CONFLICT (end_day, station, from_time, to_time, from_contents, to_contents)
        DO NOTHING
    """

    t0 = time.perf_counter()
    conn = None
    try:
        conn = get_psycopg2_conn(DB_CONFIG)
        conn.autocommit = False
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, rows, page_size=1000)
        conn.commit()
        elapsed = time.perf_counter() - t0
        log(f"[SAVE] INSERT 완료(중복은 자동 무시) | rows={len(rows):,} | {elapsed:.2f}s")
    finally:
        if conn is not None:
            conn.close()


# ============================================
# 6. main loop
# ============================================
def run_realtime_loop():
    ensure_save_table()

    log("=" * 78)
    log(f"[START] AFA FAIL wasted time REALTIME | {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] DB = {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    log(f"[INFO] SOURCE SCHEMA = {SCHEMA_MACHINE}")
    log(f"[INFO] SAVE TABLE    = {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (append + dedup)")
    log(f"[INFO] THREAD_WORKERS = {MP_WORKERS_FIXED}")
    log(f"[INFO] LOOP_INTERVAL_SEC = {LOOP_INTERVAL_SEC}")
    log(f"[INFO] TS_WINDOW_SEC = {TS_WINDOW_SEC} | QUERY_LOOKBACK_SEC = {QUERY_LOOKBACK_SEC}")
    log("=" * 78)

    while True:
        loop_t0 = time.perf_counter()
        now = datetime.now()
        today_yyyymmdd = now.strftime("%Y%m%d")

        query_cutoff_dt = now - timedelta(seconds=QUERY_LOOKBACK_SEC)
        ts_filter_cutoff = now - timedelta(seconds=TS_WINDOW_SEC)

        try:
            df_all = load_all_fct_logs_multiprocess(today_yyyymmdd=today_yyyymmdd, cutoff_dt=query_cutoff_dt)
            df_wasted = compute_afa_fail_wasted(df_all, ts_filter_cutoff=ts_filter_cutoff)
            save_to_db_append_on_conflict(df_wasted)

        except Exception as e:
            log("\n[ERROR] 루프 처리 중 예외가 발생했습니다.")
            log(f"  - {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            raise

        elapsed = time.perf_counter() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        time.sleep(sleep_sec)


def main():
    run_realtime_loop()


if __name__ == "__main__":
    freeze_support()  # 있어도 무방

    try:
        main()
    except KeyboardInterrupt:
        log("\n[STOP] 사용자 중단(Ctrl+C).")
        pause_console()
    except Exception as e:
        log("\n[ERROR] 예외가 발생했습니다.")
        log(f"  - {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        pause_console()
        sys.exit(1)
