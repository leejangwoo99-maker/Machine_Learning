# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
import re
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_values

# ==========================
# 기본 경로 설정
# ==========================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
VISION_FOLDER_NAME = "Vision03"

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

# ==========================
# 스키마/테이블
# ==========================
SCHEMA_NAME = "a3_vision_table"
TABLE_NAME  = "vision_table"

# ==========================
# 추가 요구사항 설정
# ==========================
NUM_WORKERS = 2                 # ✅ 멀티프로세스 2개 고정
REALTIME_WINDOW_SEC = 120       # ✅ 최근 120초 이내 파일만 처리
USE_FIXED_CUTOFF_TS = False     # ✅ True면 아래 cutoff_ts 값을 "고정"으로 사용
FIXED_CUTOFF_TS = 1765501841.4473598

# ✅ DB DISTINCT file_path 재로딩 주기(루프 횟수)
DB_RELOAD_EVERY_LOOPS = 120     # 약 2분(1초 루프 기준)


# ==========================
# DB 유틸
# ==========================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_table(conn):
    """
    - 메인 테이블은 파일 1개당 여러 step row가 들어가는 구조
      => UNIQUE(file_path) / ON CONFLICT(file_path) 불가능
    - 따라서 중복 판정은:
      ✅ SELECT DISTINCT file_path WHERE end_day=오늘
    - 성능을 위해 file_path, end_day 인덱스 생성
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                id                  BIGSERIAL PRIMARY KEY,
                barcode_information TEXT,
                station             TEXT,
                run_time            DOUBLE PRECISION,
                end_day             TEXT,
                end_time            TEXT,
                remark              TEXT,
                step_description    TEXT,
                value               TEXT,
                min                 TEXT,
                max                 TEXT,
                result              TEXT,
                file_path           TEXT NOT NULL,
                processed_at        TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )

        # (안전) 이미 존재해도 OK
        cur.execute(
            f"""
            ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME}
            ADD COLUMN IF NOT EXISTS run_time DOUBLE PRECISION;
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_file_path
            ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);
            """
        )

        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_end_day
            ON {SCHEMA_NAME}.{TABLE_NAME} (end_day);
            """
        )

    conn.commit()


def get_processed_file_paths_today(conn, end_day: str) -> set:
    """
    ✅ fct_table과 동일:
    UNIQUE(file_path)는 불가능하므로,
    SELECT DISTINCT file_path WHERE end_day=오늘 로 “이미 적재된 파일”을 판별
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT file_path
            FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE end_day = %s
              AND file_path IS NOT NULL
              AND file_path <> '';
            """,
            (end_day,)
        )
        rows = cur.fetchall()
    return {r[0] for r in rows if r and r[0]}


def insert_main_rows(conn, rows):
    """
    메인 테이블은 파일당 여러 step row 저장 구조이므로
    file_path 기준 ON CONFLICT / UNIQUE 같은 건 사용하면 안 됨.
    """
    if not rows:
        return

    records = [
        (
            r.get("barcode_information", ""),
            r.get("station", ""),
            r.get("run_time", None),
            r.get("end_day", ""),
            r.get("end_time", ""),
            r.get("remark", ""),
            r.get("step_description", ""),
            r.get("value", ""),
            r.get("min", ""),
            r.get("max", ""),
            r.get("result", ""),
            r.get("file_path", ""),
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
                (barcode_information, station, run_time,
                 end_day, end_time, remark,
                 step_description, value, min, max, result, file_path)
            VALUES %s
            """,
            records,
            page_size=5000,
        )


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
    return "Vision1" if prog == "LED1" else "Vision2" if prog == "LED2" else prog


def classify_remark(barcode: str) -> str:
    if not barcode or len(barcode) < 18:
        return "Non-PD"
    return "PD" if barcode[17] in ("J", "S") else "Non-PD"


def clean_result_value(result: str) -> str:
    if result is None:
        return ""
    return str(result).replace("[", "").replace("]", "").strip()


def parse_end_day_from_path(file_path: Path) -> str:
    for parent in file_path.parents:
        if re.fullmatch(r"\d{8}", parent.name):
            return parent.name
    m = re.search(r"(\d{8})", file_path.name)
    return m.group(1) if m else ""


def parse_end_time_from_full_path(file_path: Path) -> str:
    full_path = str(file_path)
    m = re.search(r"_(\d{14})_", full_path)
    if not m:
        m = re.search(r"_(\d{14})_", file_path.name)
    if not m:
        return ""
    hhmiss = m.group(1)[8:14]
    return f"{hhmiss[0:2]}:{hhmiss[2:4]}:{hhmiss[4:6]}"


def parse_run_time_line(line: str):
    if not line:
        return None
    m = re.search(r"Run\s*Time\s*:\s*([0-9.]+)\s*s", line)
    if not m:
        return None
    try:
        return round(float(m.group(1)), 1)
    except ValueError:
        return None


def parse_data_lines(lines):
    index_list = []
    rows = []
    for raw_line in lines:
        line = raw_line.strip("\n\r")
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

        index_list.append(desc)
        rows.append({"value": value, "min": min_val, "max": max_val, "result": result})

    if not rows:
        return pd.DataFrame(columns=["value", "min", "max", "result"])

    return pd.DataFrame(rows, index=index_list)


# ==========================
# 멀티프로세싱 워커
# ==========================
def _worker_process_file(file_str: str):
    file_path = Path(file_str)
    try:
        with file_path.open("r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        print("[ERROR] 파일 읽기 오류:", file_str)
        return []

    if len(lines) < 19:
        return []

    barcode = parse_barcode_line(lines[4]) if len(lines) > 4 else ""
    station = parse_program_line(lines[5]) if len(lines) > 5 else ""
    remark = classify_remark(barcode)

    run_time = parse_run_time_line(lines[13]) if len(lines) > 13 else None

    end_day = parse_end_day_from_path(file_path)
    end_time = parse_end_time_from_full_path(file_path)

    df_steps = parse_data_lines(lines[18:])
    if df_steps.empty:
        return []

    out_rows = []
    for step_desc, row in df_steps.iterrows():
        out_rows.append(
            {
                "barcode_information": barcode,
                "station": station,
                "run_time": run_time,
                "end_day": end_day,
                "end_time": end_time,
                "remark": remark,
                "step_description": step_desc,
                "value": row.get("value", ""),
                "min": row.get("min", ""),
                "max": row.get("max", ""),
                "result": clean_result_value(row.get("result", "")),
                "file_path": str(file_path),
            }
        )
    return out_rows


# ==========================
# 메인 파싱 로직 (한 번 실행)
# ==========================
def run_once(conn, processed_set: set, today_str: str):
    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME
    if not vision_root.exists():
        print(f"[ERROR] {VISION_FOLDER_NAME} 폴더가 없음: {vision_root}")
        return

    # cutoff_ts 결정
    if USE_FIXED_CUTOFF_TS:
        cutoff_ts = float(FIXED_CUTOFF_TS)
    else:
        cutoff_ts = time.time() - REALTIME_WINDOW_SEC

    # 1) 폴더 스캔 (오늘 폴더만 + 최근 120초 파일만)
    target_files = []

    today_dir = vision_root / today_str
    if not today_dir.exists():
        print(f"[INFO] 오늘 폴더 없음: {today_dir} (today={today_str})")
        return

    for sub_name in ["GoodFile", "BadFile"]:
        sub_dir = today_dir / sub_name
        if not sub_dir.exists():
            continue

        for fp in sub_dir.glob("*"):
            if not fp.is_file():
                continue

            try:
                mtime = fp.stat().st_mtime
            except Exception:
                continue

            if mtime < cutoff_ts:
                continue

            target_files.append(str(fp))

    total = len(target_files)
    print(f"[INFO] 오늘({today_str}) & 최근{REALTIME_WINDOW_SEC}초 대상 파일 수: {total}개 (cutoff_ts={cutoff_ts})")
    if total == 0:
        print("[INFO] 대상 파일 없음.")
        return

    # ✅ fct_table과 동일: 이미 적재된 file_path(오늘 end_day)면 스킵
    files_to_process = [f for f in target_files if f not in processed_set]

    # (추가) 이번 사이클 내 중복 file_path 제거
    seen = set()
    uniq = []
    for f in files_to_process:
        if f in seen:
            continue
        seen.add(f)
        uniq.append(f)
    files_to_process = uniq

    print(f"[INFO] DB DISTINCT file_path(end_day=오늘) 중복 스킵: {total - len(files_to_process)}개")
    print(f"[INFO] 실제 처리 대상: {len(files_to_process)}개")

    if not files_to_process:
        print("[INFO] 처리할 신규 파일 없음.")
        return

    # 3) 멀티프로세스 파싱 (2개 고정)
    max_workers = NUM_WORKERS
    chunksize = max(20, len(files_to_process) // (max_workers * 8) or 1)

    all_new_rows = []

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for idx, rows in enumerate(
            executor.map(_worker_process_file, files_to_process, chunksize=chunksize),
            start=1,
        ):
            if rows:
                rows = [r for r in rows if r.get("end_day") == today_str]
                all_new_rows.extend(rows)

            if idx % 500 == 0 or idx == len(files_to_process):
                print(
                    f"[진행] {idx}/{len(files_to_process)} "
                    f"(누적 신규 row: {len(all_new_rows)}) "
                    f"[workers={max_workers}, chunksize={chunksize}]"
                )

    if not all_new_rows:
        print("[INFO] 신규 파싱된 데이터가 없습니다.")
        return

    # 4) INSERT + COMMIT
    try:
        insert_main_rows(conn, all_new_rows)
        conn.commit()
    except Exception:
        conn.rollback()
        raise

    # ✅ 이번에 적재한 file_path는 processed_set에 즉시 반영
    new_file_paths = {r["file_path"] for r in all_new_rows}
    processed_set.update(new_file_paths)

    print(f"[완료] 신규 파일 {len(new_file_paths)}개, 신규 row {len(all_new_rows)}개 PostgreSQL 파싱 완료.")


# ==========================
# 무한 루프 (1초)
# ==========================
def main():
    mp.freeze_support()

    with get_connection() as conn:
        ensure_schema_and_table(conn)

        current_day = datetime.now().strftime("%Y%m%d")
        processed_set = get_processed_file_paths_today(conn, current_day)
        print(f"[DB] Loaded DISTINCT file_path WHERE end_day={current_day}: {len(processed_set)}")

        loop_count = 0

        while True:
            loop_count += 1
            try:
                today_str = datetime.now().strftime("%Y%m%d")

                # 자정 날짜 변경 감지: 오늘 processed_set 재로딩
                if today_str != current_day:
                    current_day = today_str
                    processed_set = get_processed_file_paths_today(conn, current_day)
                    print(f"[DAY-CHANGE] end_day={current_day} | reload db_paths={len(processed_set)}")

                # 주기적 DB 재동기화(다른 프로세스 적재 반영)
                if (loop_count % DB_RELOAD_EVERY_LOOPS) == 0:
                    processed_set = get_processed_file_paths_today(conn, current_day)
                    print(f"[DB-RELOAD] end_day={current_day} | db_paths={len(processed_set)}")

                run_once(conn, processed_set, current_day)

            except Exception as e:
                print("[ERROR] run_once 중 예외 발생:", e)

            time.sleep(1)


if __name__ == "__main__":
    main()
