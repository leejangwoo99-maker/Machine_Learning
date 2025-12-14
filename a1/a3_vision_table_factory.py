from pathlib import Path
import pandas as pd
import re
import os
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_values

# ==========================
# [FACTORY] 기본 경로 설정
# ==========================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
VISION_FOLDER_NAME = "Vision03"

# ==========================
# [FACTORY] PostgreSQL 접속 정보
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
# DB 유틸
# ==========================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_table(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                id BIGSERIAL PRIMARY KEY,
                barcode_information TEXT,
                station             TEXT,
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
        """)

        cur.execute(f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_file_path
            ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);
        """)

    conn.commit()


def get_processed_file_paths(conn) -> set:
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT file_path FROM {SCHEMA_NAME}.{TABLE_NAME};")
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_main_rows(conn, rows):
    if not rows:
        return

    records = [
        (
            r["barcode_information"],
            r["station"],
            r["end_day"],
            r["end_time"],
            r["remark"],
            r["step_description"],
            r["value"],
            r["min"],
            r["max"],
            r["result"],
            r["file_path"],
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
                (barcode_information, station, end_day, end_time, remark,
                 step_description, value, min, max, result, file_path)
            VALUES %s
            """,
            records,
            page_size=3000,
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
    if barcode and len(barcode) >= 18 and barcode[17] in ("J", "S"):
        return "PD"
    return "Non-PD"


def clean_result_value(result: str) -> str:
    return str(result).replace("[", "").replace("]", "").strip() if result else ""


def parse_end_time_from_full_path(file_path: Path) -> str:
    m = re.search(r"_(\d{14})_", file_path.name)
    if not m:
        return ""
    ts = m.group(1)
    return f"{ts[8:10]}:{ts[10:12]}:{ts[12:14]}"


def parse_data_lines(lines):
    rows = []
    index_list = []

    for raw in lines:
        if "," not in raw:
            continue
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 2:
            continue

        index_list.append(re.sub(r"\s{2,}", " ", parts[0]))
        rows.append({
            "value": parts[1] if len(parts) > 1 else "",
            "min": parts[2] if len(parts) > 2 else "",
            "max": parts[3] if len(parts) > 3 else "",
            "result": parts[4] if len(parts) > 4 else "",
        })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, index=index_list)


# ==========================
# 워커
# ==========================
def _worker_process_file(file_str: str):
    file_path = Path(file_str)

    try:
        with file_path.open("r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        return []

    if len(lines) < 19:
        return []

    barcode = parse_barcode_line(lines[4])
    station = parse_program_line(lines[5])
    remark  = classify_remark(barcode)

    end_day  = datetime.now().strftime("%Y%m%d")
    end_time = parse_end_time_from_full_path(file_path)

    df = parse_data_lines(lines[18:])
    if df.empty:
        return []

    out = []
    for step, row in df.iterrows():
        out.append({
            "barcode_information": barcode,
            "station": station,
            "end_day": end_day,
            "end_time": end_time,
            "remark": remark,
            "step_description": step,
            "value": row["value"],
            "min": row["min"],
            "max": row["max"],
            "result": clean_result_value(row["result"]),
            "file_path": str(file_path),
        })

    return out


# ==========================
# factory 실시간 파싱
# ==========================
def run_once():
    today_str = datetime.now().strftime("%Y%m%d")
    cutoff_ts = time.time() - 120  # 120초

    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME / today_str
    if not vision_root.exists():
        return

    target_files = []
    for sub in ["GoodFile", "BadFile"]:
        sub_dir = vision_root / sub
        if not sub_dir.exists():
            continue

        for fp in sub_dir.glob("*"):
            if not fp.is_file():
                continue
            if fp.stat().st_mtime < cutoff_ts:
                continue
            target_files.append(str(fp))

    if not target_files:
        return

    with get_connection() as conn:
        ensure_schema_and_table(conn)
        processed = get_processed_file_paths(conn)

        files_to_process = [f for f in target_files if f not in processed]
        if not files_to_process:
            return

        all_rows = []

        with ProcessPoolExecutor(max_workers=2) as executor:
            for rows in executor.map(_worker_process_file, files_to_process, chunksize=10):
                if rows:
                    all_rows.extend(rows)

        if not all_rows:
            return

        try:
            insert_main_rows(conn, all_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        print(f"[FACTORY] 신규 파일 {len(files_to_process)} / 신규 row {len(all_rows)} 적재 완료")


# ==========================
# 메인 루프
# ==========================
def main():
    print("=== Vision FACTORY 실시간 파서 시작 ===")
    while True:
        try:
            run_once()
        except Exception as e:
            print("[ERROR]", e)
        time.sleep(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()
