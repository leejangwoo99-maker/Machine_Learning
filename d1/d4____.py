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

[EXE/Nuitka 반영]
- 콘솔 자동 종료 방지(Enter 대기)
- 단계별 진행상태 출력(실시간 flush)
- 멀티프로세스 로딩 진행표시
- 계산 단계 그룹 진행표시(50그룹마다)
- 예외 발생 시 traceback 출력 후 콘솔 유지 + exit code 1
"""

import os
import sys
import time
import urllib.parse
from datetime import datetime
from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine

# chained assignment를 에러로 승격(디버그용)
pd.options.mode.chained_assignment = "raise"

# ============================================
# 0. DB / 상수 설정
# ============================================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
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

TABLE_SAVE_SCHEMA = "d1_machine_log"
TABLE_SAVE_NAME = "afa_fail_wasted_time"

# 계산 진행 로그 주기 (요청: 50그룹)
PROGRESS_EVERY_GROUPS = 50


# ============================================
# 공용: 로그/콘솔 대기
# ============================================
def log(msg: str):
    print(msg, flush=True)


def pause_console():
    """
    EXE로 실행했을 때 콘솔이 자동 종료되는 것을 방지.
    - 표준입력이 없는 환경(서비스 등)에서는 예외 날 수 있으므로 방어.
    """
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
    # pool_pre_ping=True는 네트워크 DB에서 연결 끊김 방지에 유리
    return create_engine(conn_str, pool_pre_ping=True)


# ============================================
# 2. FCT 로그 로드 (프로세스 단위 실행)
#    - 멀티프로세스에서는 엔진을 프로세스 내부에서 생성해야 함
# ============================================
def load_fct_log_mp(args):
    table_name, station_label, db_config, schema_name = args
    t0 = time.perf_counter()

    engine = get_engine(db_config)

    sql = f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {schema_name}."{table_name}"
        ORDER BY end_day ASC, end_time ASC
    """

    df = pd.read_sql(sql, engine).copy()
    df.loc[:, "station"] = station_label

    elapsed = time.perf_counter() - t0
    return station_label, len(df), elapsed, df


def load_all_fct_logs_multiprocess(max_workers=None) -> pd.DataFrame:
    tasks = [(t, s, DB_CONFIG, SCHEMA_MACHINE) for t, s in TABLES_FCT]

    log(f"[STEP 1/3] DB 로드 시작 | workers={max_workers}")

    dfs = []
    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futs = [ex.submit(load_fct_log_mp, task) for task in tasks]

        done_cnt = 0
        for f in as_completed(futs):
            station_label, nrows, elapsed, df = f.result()
            dfs.append(df)
            done_cnt += 1
            log(f"  - [{done_cnt}/{len(tasks)}] {station_label} 로드 완료: rows={nrows:,} | {elapsed:.2f}s")

    if not dfs:
        log("[WARN] 로드된 데이터가 없습니다.")
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    df_all = pd.concat(dfs, ignore_index=True)
    log(f"[STEP 1/3] DB 로드 완료 | total rows={len(df_all):,}")
    return df_all


# ============================================
# 3. 계산 로직
# ============================================
def compute_afa_fail_wasted(df_all: pd.DataFrame) -> pd.DataFrame:
    log("[STEP 2/3] AFA wasted time 계산 시작")

    # 이벤트만 필터링
    df_evt = df_all[df_all["contents"].isin([NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT])].copy()
    log(f"  - 이벤트 필터링: {len(df_evt):,} / 전체 {len(df_all):,}")

    # 정렬
    df_evt = df_evt.sort_values(["end_day", "station", "end_time"]).reset_index(drop=True)

    result_rows = []

    # groupby 대상 수(진행률)
    groups = list(df_evt.groupby(["end_day", "station"], sort=False))
    total_groups = len(groups)
    log(f"  - 그룹 수(end_day x station): {total_groups:,}")

    grp_cnt = 0
    for (end_day, station), grp in groups:
        grp_cnt += 1

        # ✅ 50그룹마다 진행표시 (요청 반영)
        if grp_cnt == 1 or grp_cnt % PROGRESS_EVERY_GROUPS == 0 or grp_cnt == total_groups:
            log(f"  - 진행: {grp_cnt:,}/{total_groups:,} 그룹 처리 중... (현재 {end_day}, {station})")

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

                # 기존 형식 유지
                from_str = from_ts.strftime("%H:%M:%S.%f")[:-4]
                to_str = to_ts.strftime("%H:%M:%S.%f")[:-4]

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

    if not df_wasted.empty:
        df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)
        df_wasted.insert(0, "id", range(1, len(df_wasted) + 1))
    else:
        df_wasted = pd.DataFrame(
            columns=[
                "id",
                "end_day",
                "station",
                "from_contents",
                "from_time",
                "to_contents",
                "to_time",
                "wasted_time",
            ]
        )

    log(f"[STEP 2/3] 계산 완료 | rows={len(df_wasted):,}")
    return df_wasted


# ============================================
# 4. DB 저장
# ============================================
def save_to_db(df_wasted: pd.DataFrame):
    log("[STEP 3/3] DB 저장 시작")
    engine = get_engine(DB_CONFIG)

    df_wasted.to_sql(
        TABLE_SAVE_NAME,
        con=engine,
        schema=TABLE_SAVE_SCHEMA,
        if_exists="replace",
        index=False,
    )

    log(f"[STEP 3/3] 저장 완료 -> {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} | rows={len(df_wasted):,}")


# ============================================
# 5. main
# ============================================
def main():
    job_start = datetime.now()
    t0 = time.perf_counter()

    log("=" * 78)
    log(f"[START] AFA FAIL wasted time | {job_start:%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] DB = {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    log(f"[INFO] SOURCE SCHEMA = {SCHEMA_MACHINE}")
    log(f"[INFO] SAVE   TABLE  = {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME}")
    log(f"[INFO] PROGRESS_EVERY_GROUPS = {PROGRESS_EVERY_GROUPS}")
    log("=" * 78)

    # 네트워크 DB/디스크 병목 고려: 워커는 4 상한 권장
    max_workers = min(4, os.cpu_count() or 1)

    df_all = load_all_fct_logs_multiprocess(max_workers=max_workers)
    df_wasted = compute_afa_fail_wasted(df_all)
    save_to_db(df_wasted)

    elapsed = time.perf_counter() - t0
    log("=" * 78)
    log(f"[DONE] 전체 소요시간: {elapsed:.2f} sec")
    log("=" * 78)


if __name__ == "__main__":
    # Nuitka/EXE 멀티프로세스 필수
    freeze_support()

    try:
        main()
        pause_console()
    except Exception as e:
        log("\n[ERROR] 예외가 발생했습니다.")
        log(f"  - {type(e).__name__}: {e}")

        import traceback
        traceback.print_exc()

        pause_console()
        sys.exit(1)
