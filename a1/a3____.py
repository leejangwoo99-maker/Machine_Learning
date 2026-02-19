from pathlib import Path
import pandas as pd
import re
import os
import time
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp
from datetime import datetime, timedelta

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
    "password": "",#비번은 보완 사항
}

# ==========================
# 스키마/테이블
# ==========================
SCHEMA_NAME = "a3_vision_table"
TABLE_NAME  = "vision_table"

# ==========================
# 날짜 범위 고정 (요청)
# ==========================
START_DAY = 20251001
END_DAY   = 20251220

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
        """)

        cur.execute(f"""
        ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME}
        ADD COLUMN IF NOT EXISTS run_time DOUBLE PRECISION;
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
        print("[ERROR] 파일 읽기 오류:", file_str, flush=True)
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
# 날짜 폴더 반복자 (20251001~20251219)
# ==========================
def iter_date_dirs(vision_root: Path, start_yyyymmdd: int, end_yyyymmdd: int):
    start_dt = datetime.strptime(str(start_yyyymmdd), "%Y%m%d")
    end_dt   = datetime.strptime(str(end_yyyymmdd), "%Y%m%d")

    cur = start_dt
    while cur <= end_dt:
        day_str = cur.strftime("%Y%m%d")
        date_dir = vision_root / day_str
        if date_dir.is_dir():
            yield date_dir
        cur += timedelta(days=1)

# ==========================
# 메인 파싱 로직 (한 번 실행)  ★ run_once는 이것 하나만 존재해야 함
# ==========================
def run_once():
    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME
    if not vision_root.exists():
        print(f"[ERROR] {VISION_FOLDER_NAME} 폴더가 없음: {vision_root}", flush=True)
        return

    print(f"[INFO] 파일 스캔 시작 (범위 {START_DAY} ~ {END_DAY})", flush=True)
    print("[INFO] 현재 파싱할 데이터 수 파악 중...", flush=True)

    # 1) 날짜 범위 폴더만 스캔
    target_files = []
    scanned_days = 0

    for date_dir in iter_date_dirs(vision_root, START_DAY, END_DAY):
        scanned_days += 1
        for sub_name in ["GoodFile", "BadFile"]:
            sub_dir = date_dir / sub_name
            if not sub_dir.exists():
                continue
            for fp in sub_dir.glob("*"):
                if fp.is_file():
                    target_files.append(str(fp))

        if scanned_days % 10 == 0:
            print(
                f"[INFO] 스캔 진행중... 날짜폴더 {scanned_days}개, 현재 파일 {len(target_files)}개",
                flush=True
            )

    total = len(target_files)
    print(f"[INFO] 스캔 완료", flush=True)
    print(f"[INFO] 스캔 날짜폴더(존재하는 것만): {scanned_days}개", flush=True)
    print(f"[INFO] 총 대상 파일 수: {total}개", flush=True)

    if total == 0:
        print("[INFO] 대상 파일 없음. 이번 사이클 종료", flush=True)
        return

    # 2) DB 연결/초기화
    print("[INFO] DB 중복(file_path) 조회 시작", flush=True)
    with get_connection() as conn:
        ensure_schema_and_table(conn)

        processed_set = get_processed_file_paths(conn)

        files_to_process = [f for f in target_files if f not in processed_set]

        # 이번 사이클 내 중복 제거
        seen = set()
        uniq = []
        for f in files_to_process:
            if f in seen:
                continue
            seen.add(f)
            uniq.append(f)
        files_to_process = uniq

        print(f"[INFO] DB(file_path) 중복 스킵: {total - len(files_to_process)}개", flush=True)
        print(f"[INFO] 이번 사이클 실제 처리 대상 파일 수: {len(files_to_process)}개", flush=True)

        if not files_to_process:
            print("[INFO] 신규 파일 없음. 파싱 단계 스킵", flush=True)
            return

        # 3) 멀티프로세스 파싱
        print("[INFO] 파싱 시작", flush=True)

        cpu = os.cpu_count() or 4
        max_workers = min(cpu, 12)
        chunksize = max(20, len(files_to_process) // (max_workers * 8) or 1)

        all_new_rows = []
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            for idx, rows in enumerate(
                executor.map(_worker_process_file, files_to_process, chunksize=chunksize),
                start=1,
            ):
                if rows:
                    all_new_rows.extend(rows)

                if idx % 1000 == 0 or idx == len(files_to_process):
                    print(
                        f"[진행] {idx}/{len(files_to_process)} "
                        f"(누적 신규 row: {len(all_new_rows)}) "
                        f"[workers={max_workers}, chunksize={chunksize}]",
                        flush=True
                    )

        if not all_new_rows:
            print("[INFO] 신규 파싱된 데이터가 없습니다.", flush=True)
            return

        # 4) INSERT + COMMIT  (※ 여기 1번만)
        print(f"[INFO] DB INSERT 시작 (row={len(all_new_rows)})", flush=True)
        try:
            insert_main_rows(conn, all_new_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        new_files_cnt = len({r["file_path"] for r in all_new_rows})
        print(
            f"[완료] 신규 파일 {new_files_cnt}개, "
            f"신규 row {len(all_new_rows)}개 PostgreSQL 파싱 완료.",
            flush=True
        )

# ==========================
# 무한 루프 (1초)
# ==========================
if __name__ == "__main__":
    mp.freeze_support()
    try:
        run_once()
    except Exception as e:
        print("[ERROR] run_once 중 예외 발생:", e, flush=True)
