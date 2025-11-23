from pathlib import Path
import pandas as pd
import re
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import os
import time

import psycopg2
from psycopg2.extras import execute_values

# ==========================
# 기본 경로 설정
# ==========================
# BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")  # 예시: NAS 경로
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")    # 로컬일 때 예시
VISION_FOLDER_NAME = "Vision03"                          # Vision 로그 중간 폴더

# ==========================
# PostgreSQL 접속 정보
# ==========================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# ==========================
# DB 유틸
# ==========================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_tables(conn):
    """필요한 스키마/테이블이 없으면 생성"""
    with conn.cursor() as cur:
        # 메인 데이터 스키마/테이블
        cur.execute("CREATE SCHEMA IF NOT EXISTS a3_vision_json_table;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS a3_vision_json_table.vision_json_table (
                id           BIGSERIAL PRIMARY KEY,
                file_path    TEXT NOT NULL,
                station      TEXT,
                barcode_information TEXT,
                step_description     TEXT,
                value        TEXT,
                min          TEXT,
                max          TEXT,
                result       TEXT,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )

        # 히스토리 스키마/테이블
        cur.execute("CREATE SCHEMA IF NOT EXISTS a3_vision_json_table_processing_history;")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS a3_vision_json_table_processing_history.vision_json_table_processing_history (
                file_path    TEXT PRIMARY KEY,
                processed_at TIMESTAMP NOT NULL DEFAULT NOW()
            );
            """
        )
    conn.commit()


def load_processed_file_paths(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT file_path
            FROM a3_vision_json_table_processing_history.vision_json_table_processing_history
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
            """
            INSERT INTO a3_vision_json_table_processing_history.vision_json_table_processing_history
                (file_path)
            VALUES %s
            ON CONFLICT (file_path) DO NOTHING
            """,
            data,
        )
    conn.commit()


def insert_main_rows(conn, rows):
    if not rows:
        return

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
            """
            INSERT INTO a3_vision_json_table.vision_json_table
                (file_path, station, barcode_information, step_description,
                 value, min, max, result)
            VALUES %s
            """,
            records,
        )
    conn.commit()


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
        rows.append(
            {
                "value": value,
                "min": min_val,
                "max": max_val,
                "result": result,
            }
        )

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
    vision = parse_program_line(lines[5]) if len(lines) > 5 else ""
    df_steps = parse_data_lines(lines[18:])

    if df_steps.empty:
        return []

    json_rows = []
    for step_desc, row in df_steps.iterrows():
        json_rows.append(
            {
                "file_path": str(file_path),
                "Station": vision,
                "Barcode information": barcode,
                "step_description": step_desc,
                "value": row.get("value", ""),
                "min": row.get("min", ""),
                "max": row.get("max", ""),
                "result": row.get("result", ""),
            }
        )

    return json_rows


# ==========================
# 메인 파싱 로직 (한 번 실행)
# ==========================
def run_once():
    # """
    # 1) RAW_LOG\Vision03\YYYYMMDD\GoodFile/BadFile 스캔
    # 2) history 테이블에 없는 파일만 파싱
    # 3) JSON(dict) 생성
    # 4) PostgreSQL 넣기
    # 5) history Insert
    # """

    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME
    if not vision_root.exists():
        print(f"[ERROR] Vision03 폴더가 없음: {vision_root}")
        return

    with get_connection() as conn:
        ensure_schema_and_tables(conn)
        processed_set = load_processed_file_paths(conn)

        target_files = []
        for date_dir in sorted(vision_root.iterdir()):
            if not date_dir.is_dir():
                continue
            for sub_name in ["GoodFile", "BadFile"]:
                sub_dir = date_dir / sub_name
                if not sub_dir.exists():
                    continue
                for file_path in sorted(sub_dir.glob("*")):
                    if file_path.is_file():
                        target_files.append(str(file_path))

        total = len(target_files)
        print(f"[INFO] 총 대상 파일 수: {total}개")

        files_to_process = [f for f in target_files if f not in processed_set]
        skip_by_history = total - len(files_to_process)

        print(f"[INFO] 이력 스킵: {skip_by_history}개")
        print(f"[INFO] 실제 처리 대상: {len(files_to_process)}개")

        if not files_to_process:
            print("[INFO] 처리할 신규 파일 없음.")
            return

        all_new_rows = []
        new_file_paths = []

        num_workers = os.cpu_count() or 4

        with ProcessPoolExecutor(max_workers=num_workers) as executor:
            for idx, json_rows in enumerate(executor.map(_worker_process_file, files_to_process), start=1):
                if json_rows:
                    all_new_rows.extend(json_rows)
                    new_file_paths.append(json_rows[0]["file_path"])

                if idx % 1000 == 0 or idx == len(files_to_process):
                    print(f"[진행] {idx}/{len(files_to_process)} (신규 파싱 파일 수: {len(new_file_paths)})")

        if not all_new_rows:
            print("[INFO] 신규 파싱된 데이터가 없습니다.")
            return

        insert_main_rows(conn, all_new_rows)
        insert_history(conn, new_file_paths)

        print(f"[완료] 신규 파일 {len(new_file_paths)}개, 신규 row {len(all_new_rows)}개 PostgreSQL 파싱 완료.")


# ==========================
# 무한 루프 (1초)
# ==========================
def main():
    while True:
        try:
            run_once()
        except Exception as e:
            print("[ERROR] run_once 중 예외 발생:", e)
        time.sleep(1)


if __name__ == "__main__":
    main()
