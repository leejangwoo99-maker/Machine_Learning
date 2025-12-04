from pathlib import Path
import re
from datetime import datetime, date
import time
import multiprocessing as mp
import os
import calendar

import psycopg2
from psycopg2.extras import execute_values

# ==========================
# 기본 경로 설정
# ==========================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")  # NAS 경로
VISION_FOLDER_NAME = "Vision03"                       # Vision 로그 중간 폴더
TARGET_FOLDERS = ["GoodFile", "BadFile"]

# 날짜 폴더 최소 기준 (이전 데이터 제외) - 고정 시작일
FIXED_START_DATE = date(2025, 10, 1)   # yyyymmdd에 해당

# 배치 처리 시 한 번에 DB에 넣을 최대 row 수 (메모리 최적화용)
BATCH_SIZE_ROWS = 50000

# 실시간 전용: 최근 N초 이내에 수정된 파일만 대상
REALTIME_LOOKBACK_SECONDS = 120  # 예: 최근 2분

# ==========================
# PostgreSQL 접속 정보
# ==========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_MAIN = "a3_vision_json_table"
SCHEMA_HIST = "a3_vision_json_table_processing_history"
TABLE_MAIN = "vision_json_table"
TABLE_HIST = "vision_json_table_processing_history"


# ==========================
# 날짜 윈도우 유틸
# ==========================
def six_months_ago(d: date) -> date:
    """
    오늘 기준 6개월 전 날짜 계산 (현재는 사용하지 않지만 참고용으로 남겨둠).
    """
    year = d.year
    month = d.month - 6
    if month <= 0:
        year -= 1
        month += 12

    last_day = calendar.monthrange(year, month)[1]
    day = min(d.day, last_day)
    return date(year, month, day)


def get_window_dates():
    """
    오늘 기준으로 '이번 달 1일 ~ 오늘' 범위를 반환.
    예)
      - today = 2025-12-04 → 2025-12-01 ~ 2025-12-04
      - today = 2025-12-31 → 2025-12-01 ~ 2025-12-31
      - today = 2026-01-01 → 2026-01-01 ~ 2026-01-01

    FIXED_START_DATE 이전은 무조건 제외.
    """
    today = date.today()

    # 이번 달 1일
    month_start = today.replace(day=1)

    # 고정 시작일 이후만
    window_start_date = max(month_start, FIXED_START_DATE)
    window_end_date = today

    return window_start_date, window_end_date


# ==========================
# DB 유틸
# ==========================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_tables(conn):
    with conn.cursor() as cur:
        # 메인 스키마/테이블
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_MAIN};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_MAIN}.{TABLE_MAIN} (
                id           BIGSERIAL PRIMARY KEY,
                file_path    TEXT NOT NULL,
                station      TEXT,
                barcode_information TEXT,
                step_description     TEXT,
                value        TEXT,
                min          TEXT,
                max          TEXT,
                result       TEXT,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

        # 히스토리 스키마/테이블
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_HIST};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_HIST}.{TABLE_HIST} (
                file_path    TEXT PRIMARY KEY,
                processed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )

    conn.commit()


def cleanup_old_data(conn, window_start_date: date):
    """
    (옵션) 현재 날짜 기준 6개월 이상된 데이터 삭제 (DELETE).
    지금은 Python 코드에서 호출하지 않고,
    필요 시 직접 SQL로 관리하는 것을 권장.

    - 메인 테이블 : processed_at < window_start_date 00:00:00
    - 히스토리    : processed_at < window_start_date 00:00:00
    """
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    cur = conn.cursor()

    # main
    cur.execute(
        f"""
        DELETE FROM {SCHEMA_MAIN}.{TABLE_MAIN}
        WHERE processed_at < %s
        """,
        (cutoff_dt,),
    )
    deleted_main = cur.rowcount

    # history
    cur.execute(
        f"""
        DELETE FROM {SCHEMA_HIST}.{TABLE_HIST}
        WHERE processed_at < %s
        """,
        (cutoff_dt,),
    )
    deleted_hist = cur.rowcount

    conn.commit()
    cur.close()

    print(
        f"[정리] 6개월 이전 데이터 삭제 완료 "
        f"(main={deleted_main}, hist={deleted_hist})",
        flush=True,
    )


def load_processed_file_paths(conn):
    """
    이미 처리된 file_path 목록 로딩.
    (현재는 전체 히스토리에서 가져오며,
     오래된 데이터는 DB에서 주기적으로 직접 정리하는 것을 추천.)
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT file_path
            FROM {SCHEMA_HIST}.{TABLE_HIST}
            """
        )
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_history(conn, file_paths):
    if not file_paths:
        return
    data = [(fp,) for fp in file_paths]
    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_HIST}.{TABLE_HIST}
                (file_path)
            VALUES %s
            ON CONFLICT (file_path) DO NOTHING
            """,
            data,
        )
    conn.commit()


def insert_main_rows(conn, rows):
    if not rows:
        return 0

    records = [
        (
            r["file_path"],
            r["Station"],
            r["Barcode information"],
            r["step_description"],
            r["value"],
            r["min"],
            r["max"],
            r["result"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_MAIN}.{TABLE_MAIN}
                (file_path, station, barcode_information, step_description,
                 value, min, max, result)
            VALUES %s
            """,
            records,
        )
    conn.commit()
    return len(records)


# ==========================
# 파싱 유틸
# ==========================
def parse_barcode_line(line: str) -> str:
    m = re.search(r"Barcode information\s*:\s*(.*)", line)
    return m.group(1).strip() if m else ""


def parse_program_line(line: str) -> str:
    m = re.search(r"Test Program\s*:\s*(.*)", line)
    if not m:
        return ""
    prog = m.group(1).strip()
    if prog == "LED1":
        return "Vision1"
    elif prog == "LED2":
        return "Vision2"
    else:
        return prog


def parse_data_lines(lines):
    """
    각 step 라인을 파싱해서 list[dict] 반환.
    pandas 없이 바로 dict 리스트로 만들도록 구성 (메모리 절약).
    """
    rows = []

    for raw_line in lines:
        line = raw_line.strip("\r\n")
        if not line.strip() or "," not in line:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            continue

        desc = re.sub(r"\s{2,}", " ", parts[0]).strip()
        value = parts[1] if len(parts) > 1 else ""
        min_val = parts[2] if len(parts) > 2 else ""
        max_val = parts[3] if len(parts) > 3 else ""
        result = parts[4] if len(parts) > 4 else ""

        rows.append(
            {
                "step_description": desc,
                "value": value,
                "min": min_val,
                "max": max_val,
                "result": result,
            }
        )

    return rows


# ==========================
# 워커: 한 파일 처리 (mp.Pool에서 사용)
# ==========================
def process_one_file(file_path_str: str):
    p = Path(file_path_str)
    try:
        with p.open("r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
    except Exception as e:
        print(f"[ERROR] 파일 읽기 오류: {p} ({e})", flush=True)
        return []

    if len(lines) < 19:
        return []

    barcode = parse_barcode_line(lines[4]) if len(lines) > 4 else ""
    station = parse_program_line(lines[5]) if len(lines) > 5 else ""
    step_rows = parse_data_lines(lines[18:])

    if not step_rows:
        return []

    rows = []
    for sr in step_rows:
        rows.append(
            {
                "file_path": str(p),
                "Station": station,
                "Barcode information": barcode,
                "step_description": sr["step_description"],
                "value": sr["value"],
                "min": sr["min"],
                "max": sr["max"],
                "result": sr["result"],
            }
        )

    return rows


# ==========================
# 한 번 실행(run_once)
# ==========================
def run_once():
    started_at = datetime.now()
    print(f"\n================ run_once 시작: {started_at} ================", flush=True)

    # 날짜 윈도우 계산 (이번 달 1일 ~ 오늘, FIXED_START_DATE 적용)
    window_start_date, window_end_date = get_window_dates()
    window_start_str = window_start_date.strftime("%Y%m%d")
    window_end_str = window_end_date.strftime("%Y%m%d")

    print(f"[윈도우] 파싱 기간: {window_start_date} ~ {window_end_date}", flush=True)

    # 실시간 기준 시각 (최근 N초 이내 수정된 파일만 대상)
    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_LOOKBACK_SECONDS
    print(
        f"[실시간] 최근 {REALTIME_LOOKBACK_SECONDS}초 이내 수정된 파일만 처리 (cutoff_ts={cutoff_ts})",
        flush=True,
    )

    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME
    print(f"[DEBUG] vision_root: {vision_root}", flush=True)

    if not vision_root.exists():
        print(f"[WARN] Vision03 폴더가 없음: {vision_root}", flush=True)
        return

    conn = get_connection()
    try:
        ensure_schema_and_tables(conn)

        # ✅ 데이터 삭제는 DB 쪽에서 직접 SQL로 관리하는 것을 추천
        # cleanup_old_data(conn, window_start_date)  # 필요하면 주석 해제해서 사용

        # 정리 후, 현재 테이블 기준으로 file_path 로드
        processed_set = load_processed_file_paths(conn)
        print(f"[INFO] 히스토리 file_path 수: {len(processed_set)}개", flush=True)

        # -------- 파일 스캔 (폴더 윈도우 + mtime 필터) --------
        file_list = []
        total_scanned = 0

        date_dirs = []
        for d in sorted(vision_root.iterdir()):
            if not d.is_dir():
                continue
            name = d.name
            # yyyymmdd 형식 + 윈도우 범위 안인지 체크
            if not re.fullmatch(r"\d{8}", name):
                continue
            if not (window_start_str <= name <= window_end_str):
                continue
            date_dirs.append(d)

        print(f"[DEBUG] 날짜 폴더 수(윈도우 적용 후): {len(date_dirs)}개", flush=True)

        for date_dir in date_dirs:
            folder_date = date_dir.name

            for gb in TARGET_FOLDERS:
                sub_dir = date_dir / gb
                if not sub_dir.exists():
                    continue

                for f in sub_dir.iterdir():
                    if not f.is_file():
                        continue

                    # 🔥 실시간 mtime 필터: 최근 REALTIME_LOOKBACK_SECONDS 이내 수정된 파일만 대상
                    try:
                        if f.stat().st_mtime < cutoff_ts:
                            continue
                    except FileNotFoundError:
                        # 사이에 삭제된 경우 등은 무시
                        continue

                    total_scanned += 1
                    fp_str = str(f)

                    # 이미 처리한 파일이면 스킵
                    if fp_str in processed_set:
                        continue

                    file_list.append(fp_str)

        print(f"[INFO] 전체 스캔 파일 수(윈도우+mtime 통과): {total_scanned}개", flush=True)
        print(f"[INFO] 이번 실행에서 새로 처리할 파일 수: {len(file_list)}개", flush=True)

        if not file_list:
            print("[INFO] 처리할 신규 파일 없음.", flush=True)
            return

        # -------- 멀티프로세싱 + 배치 처리 --------
        # CPU 코어 수와 상관없이 항상 4개 프로세스만 사용 (원하면 4로 조절 가능)
        cpu_cnt = 4
        print(f"[INFO] 멀티프로세스 사용 프로세스 수: {cpu_cnt}", flush=True)

        batch_rows = []
        batch_file_paths = set()
        total_inserted_rows = 0
        total_new_files = 0

        with mp.Pool(processes=cpu_cnt) as pool:
            for idx, rows in enumerate(
                pool.imap_unordered(process_one_file, file_list), start=1
            ):
                if rows:
                    batch_rows.extend(rows)
                    # 한 파일의 모든 row는 같은 file_path를 가지므로 첫 번째 것만 사용
                    batch_file_paths.add(rows[0]["file_path"])
                    total_new_files += 1

                # 배치 크기 도달 시 DB에 INSERT 후 메모리 해제
                if len(batch_rows) >= BATCH_SIZE_ROWS:
                    inserted = insert_main_rows(conn, batch_rows)
                    insert_history(conn, batch_file_paths)
                    total_inserted_rows += inserted

                    print(
                        f"[배치] {idx}/{len(file_list)} 파일 처리까지 "
                        f"(이번 배치 rows={inserted}, 누적 rows={total_inserted_rows})",
                        flush=True,
                    )

                    batch_rows.clear()
                    batch_file_paths.clear()

                # 진행 상황 로그
                if idx % 1000 == 0 or idx == len(file_list):
                    print(
                        f"[진행] {idx}/{len(file_list)} 파일 파싱 완료 "
                        f"(현재 배치 rows={len(batch_rows)})",
                        flush=True,
                    )

        # 남은 배치 처리
        if batch_rows:
            inserted = insert_main_rows(conn, batch_rows)
            insert_history(conn, batch_file_paths)
            total_inserted_rows += inserted
            print(
                f"[배치] 마지막 배치 처리 완료 (rows={inserted}, 누적 rows={total_inserted_rows})",
                flush=True,
            )

        finished_at = datetime.now()
        print(
            f"[완료] 신규 파일 {total_new_files}개, "
            f"신규 row {total_inserted_rows}개 PostgreSQL 파싱 완료. "
            f"(소요시간: {finished_at - started_at})",
            flush=True,
        )

    finally:
        conn.close()
        print("================ run_once 종료 ================\n", flush=True)


# ==========================
# 메인 루프: 1초마다 반복
# ==========================
if __name__ == "__main__":
    try:
        print("[START] a3_vision_json_table - 무한 루프 시작", flush=True)
        while True:
            try:
                run_once()
            except Exception as e:
                print("[ERROR] run_once 중 예외 발생:", e, flush=True)
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다. 프로그램을 종료합니다.")
