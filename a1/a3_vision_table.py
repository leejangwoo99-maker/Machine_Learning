from pathlib import Path
import pandas as pd
import re
import os
import time
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_values

# ==========================
# 기본 경로 설정
# ==========================
# BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")
VISION_FOLDER_NAME = "Vision03"

# ==========================
# PostgreSQL 접속 정보
# ==========================
DB_CONFIG = {
    "host": "localhost",
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
# DB 유틸
# ==========================
def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    # autocommit은 끄고(기본 False) 마지막에 commit/rollback으로 묶는 편이 안전
    return conn


def ensure_schema_and_table(conn):
    """
    - 메인 테이블만 사용
    - 중복 판정은 vision_table의 DISTINCT file_path로 수행 (FCT 방식)
    - 성능을 위해 file_path 인덱스는 생성
    - 추가: run_time 컬럼 (DOUBLE PRECISION)
    """
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        # 1) 테이블 생성 (최초 1회)
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

        # 2) 기존 테이블에 run_time이 없을 수도 있으니 안전하게 추가(있으면 무시)
        cur.execute(
            f"""
            ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME}
            ADD COLUMN IF NOT EXISTS run_time DOUBLE PRECISION;
            """
        )

        # 3) FCT와 동일: 중복 체크용 조회 성능을 위해 인덱스만 둠 (UNIQUE 금지)
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_file_path
            ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);
            """
        )

    conn.commit()


def get_processed_file_paths(conn) -> set:
    """
    FCT 방식 그대로:
    메인 테이블에서 DISTINCT file_path 전부 로드해서 set으로 반환
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT file_path FROM {SCHEMA_NAME}.{TABLE_NAME};")
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_main_rows(conn, rows):
    """
    메인 테이블은 파일당 여러 step row 저장해야 하므로
    file_path 기준 ON CONFLICT 같은 건 쓰면 안 됨.
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
            page_size=5000,  # 대량 insert 안정/속도 개선
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
    # 예: ...AE_20251001013928_FCT_P.txt  -> 01:39:28
    full_path = str(file_path)
    m = re.search(r"_(\d{14})_", full_path)
    if not m:
        m = re.search(r"_(\d{14})_", file_path.name)
    if not m:
        return ""
    hhmiss = m.group(1)[8:14]
    return f"{hhmiss[0:2]}:{hhmiss[2:4]}:{hhmiss[4:6]}"


def parse_run_time_line(line: str):
    """
    파일 14번째 줄(인덱스 13)에 존재한다고 가정:
      Run Time              : 1.8450683s
    -> float(1.8) 로 변환(소수점 1자리 반올림)
    """
    if not line:
        return None

    # 'Run Time : 1.8450683s' 형태에서 숫자만 추출
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

    # ✅ 추가: 14번째 줄(인덱스 13)에서 run_time 추출
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
def run_once():
    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME
    if not vision_root.exists():
        print(f"[ERROR] {VISION_FOLDER_NAME} 폴더가 없음: {vision_root}")
        return

    # 1) 폴더 스캔
    target_files = []
    for date_dir in sorted(vision_root.iterdir()):
        if not date_dir.is_dir():
            continue
        for sub_name in ["GoodFile", "BadFile"]:
            sub_dir = date_dir / sub_name
            if not sub_dir.exists():
                continue
            for fp in sorted(sub_dir.glob("*")):
                if fp.is_file():
                    target_files.append(str(fp))

    total = len(target_files)
    print(f"[INFO] 총 대상 파일 수: {total}개")
    if total == 0:
        print("[INFO] 대상 파일 없음.")
        return

    # 2) DB 연결/초기화
    with get_connection() as conn:
        ensure_schema_and_table(conn)

        # 3) FCT 방식: 메인 테이블에서 DISTINCT file_path 전부 로드
        processed_set = get_processed_file_paths(conn)

        # 4) 이미 처리된 file_path 스킵
        files_to_process = [f for f in target_files if f not in processed_set]

        # (추가 안전장치) 이번 사이클 내에서도 중복 file_path 제거
        seen = set()
        uniq = []
        for f in files_to_process:
            if f in seen:
                continue
            seen.add(f)
            uniq.append(f)
        files_to_process = uniq

        print(f"[INFO] DB(file_path) 중복 스킵: {total - len(files_to_process)}개")
        print(f"[INFO] 실제 처리 대상: {len(files_to_process)}개")

        if not files_to_process:
            print("[INFO] 처리할 신규 파일 없음.")
            return

        # 5) 멀티프로세스 파싱
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
                        f"[workers={max_workers}, chunksize={chunksize}]"
                    )

        if not all_new_rows:
            print("[INFO] 신규 파싱된 데이터가 없습니다.")
            return

        # 6) INSERT + COMMIT
        try:
            insert_main_rows(conn, all_new_rows)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

        new_files_cnt = len({r["file_path"] for r in all_new_rows})
        print(f"[완료] 신규 파일 {new_files_cnt}개, 신규 row {len(all_new_rows)}개 PostgreSQL 파싱 완료.")


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
    mp.freeze_support()
    main()
