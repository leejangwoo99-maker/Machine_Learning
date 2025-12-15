from pathlib import Path
import re
import time
from datetime import datetime
from multiprocessing import Pool, cpu_count, freeze_support

import psycopg2
from psycopg2 import sql
import pandas as pd

# ============================================
# 0) 기본 경로 / DB 설정
# ============================================

BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")

TC_FOLDERS = ["TC6", "TC7", "TC8", "TC9"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "a2_fct_table"
TABLE_NAME = "fct_table"

USE_MULTIPROCESSING = True  # 문제 생기면 False로 단일 프로세스 테스트


# ============================================
# 1) PostgreSQL 관련 함수
# ============================================

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def init_db(conn):
    """
    요청 컬럼/순서 반영:
    id, barcode_information, station, run_time, end_day, end_time, remark,
    step_description, value, min, max, result, file_path, processed_at
    """
    create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier(SCHEMA_NAME)
    )

    create_table_sql = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            id BIGSERIAL PRIMARY KEY,

            barcode_information TEXT,
            station            TEXT,
            run_time           TEXT,
            end_day            TEXT,
            end_time           TEXT,
            remark             TEXT,

            step_description   TEXT,
            value              TEXT,
            min                TEXT,
            max                TEXT,

            result             TEXT,

            file_path          TEXT NOT NULL,
            processed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

    create_idx_sql_1 = sql.SQL("""
        CREATE INDEX IF NOT EXISTS {} ON {}.{} (file_path);
    """).format(
        sql.Identifier(f"idx_{TABLE_NAME}_file_path"),
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
    )

    with conn.cursor() as cur:
        cur.execute(create_schema_sql)
        cur.execute(create_table_sql)
        cur.execute(create_idx_sql_1)


def get_processed_file_paths(conn) -> set:
    """
    file_path가 이미 존재하면 그 파일은 중복으로 간주하여 처리하지 않음.
    => DISTINCT file_path로 로드
    """
    query = sql.SQL("SELECT DISTINCT file_path FROM {}.{}").format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_records(conn, records: list[dict]):
    """
    파싱된 레코드를 DB에 INSERT.
    (요청 컬럼 순서에 맞춰 INSERT 컬럼 구성)
    """
    if not records:
        return

    df = pd.DataFrame(records)

    expected_cols = [
        "barcode_information",
        "station",
        "run_time",
        "end_day",
        "end_time",
        "remark",
        "step_description",
        "value",
        "min",
        "max",
        "result",
        "file_path",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    rows = list(df[expected_cols].itertuples(index=False, name=None))

    insert_sql = sql.SQL("""
        INSERT INTO {}.{} (
            barcode_information,
            station,
            run_time,
            end_day,
            end_time,
            remark,
            step_description,
            value,
            min,
            max,
            result,
            file_path
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)


# ============================================
# 2) FCT 로그 파싱용 정규식
# ============================================

STATION_PATTERN = re.compile(r"Station\s*:?\s*(\S+)", re.IGNORECASE)
BARCODE_PATTERN = re.compile(r"Barcode\s+information\s*:?\s*(.+)", re.IGNORECASE)

# Run Time              :27.0
RUNTIME_PATTERN = re.compile(r"Run\s*Time\s*:?\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)


def parse_end_day_end_time_from_path(file_path: Path):
    """
    end_day: ...\\TCx\\yyyymmdd\\GoodFile(or BadFile)\\... 에서 yyyymmdd
    end_time: 파일명 ..._yyyymmddhhmiss_... 에서 hh:mi:ss
    """
    end_day = ""
    try:
        end_day = file_path.parent.parent.name  # GoodFile/BadFile의 상위(yyyymmdd)
    except Exception:
        end_day = ""

    end_time = ""
    try:
        stem = file_path.stem
        parts = stem.split("_")
        if len(parts) >= 2:
            ts = parts[1]
            if ts.isdigit() and len(ts) == 14:
                hh, mi, ss = ts[8:10], ts[10:12], ts[12:14]
                end_time = f"{hh}:{mi}:{ss}"
    except Exception:
        end_time = ""

    return end_day, end_time


def make_remark_from_barcode(barcode: str) -> str:
    """
    barcode 18번째 글자(1-indexed)가 J/S면 PD, 아니면 Non-PD
    """
    if barcode and len(barcode) >= 18:
        c18 = barcode[17]
        return "PD" if c18 in ("J", "S") else "Non-PD"
    return ""


def parse_run_time(lines: list[str]) -> str:
    """
    파일 안 14번째 줄(1-indexed) 우선에서 Run Time 값을 추출
    예: Run Time              :27.0 -> "27.0"
    없으면 전체 라인에서 검색
    """
    if len(lines) >= 14:
        m = RUNTIME_PATTERN.search(lines[13])
        if m:
            return m.group(1).strip()

    for line in lines:
        m = RUNTIME_PATTERN.search(line)
        if m:
            return m.group(1).strip()

    return ""


def parse_station_barcode(lines: list[str]):
    station = None
    barcode = None

    # Station (3번째 줄 우선)
    if len(lines) >= 3:
        m = STATION_PATTERN.search(lines[2])
        if m:
            station = m.group(1).strip()
    if station is None:
        for line in lines:
            m = STATION_PATTERN.search(line)
            if m:
                station = m.group(1).strip()
                break

    # Barcode information (5번째 줄 우선)
    if len(lines) >= 5:
        m = BARCODE_PATTERN.search(lines[4])
        if m:
            barcode = m.group(1).strip()
    if barcode is None:
        for line in lines:
            m = BARCODE_PATTERN.search(line)
            if m:
                barcode = m.group(1).strip()
                break

    return station or "", barcode or ""


def parse_goodorbad_and_result(file_path: Path):
    """
    goodorbad: GoodFile/BadFile 폴더명
    result: GoodFile -> PASS, BadFile -> FAIL
    """
    goodorbad = ""
    try:
        goodorbad = file_path.parent.name  # GoodFile/BadFile
    except Exception:
        goodorbad = ""

    if goodorbad.lower() == "goodfile":
        result = "PASS"
    elif goodorbad.lower() == "badfile":
        result = "FAIL"
    else:
        result = ""

    return goodorbad, result


def parse_fct_file(file_path: Path) -> list[dict]:
    """
    FCT 로그 1개 파일 -> 여러 행(19~54줄 step별) 레코드 반환
    공통 컬럼(barcode/station/run_time/end_day/end_time/remark/file_path)은
    step 행마다 동일하게 복제하여 저장.
    """
    try:
        with file_path.open("r", encoding="cp949", errors="ignore") as f:
            lines = [line.rstrip("\n") for line in f]
    except UnicodeDecodeError:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = [line.rstrip("\n") for line in f]

    if not lines:
        return []

    end_day, end_time = parse_end_day_end_time_from_path(file_path)
    station, barcode = parse_station_barcode(lines)
    run_time = parse_run_time(lines)
    remark = make_remark_from_barcode(barcode)

    # 19~54번째 줄 step 데이터 파싱
    step_rows = parse_step_rows(lines)

    # step_rows가 없으면(형식 불일치/빈 파일) 저장 안 함
    if not step_rows:
        return []

    records: list[dict] = []
    for step in step_rows:
        rec = {
            "barcode_information": barcode,
            "station": station,
            "run_time": run_time,
            "end_day": end_day,
            "end_time": end_time,
            "remark": remark,

            "step_description": step.get("step_description", ""),
            "value": step.get("value", ""),
            "min": step.get("min", ""),
            "max": step.get("max", ""),
            "result": step.get("result", ""),

            "file_path": str(file_path),
        }
        records.append(rec)

    return records

# ============================================
# 3) 파일 수집
# ============================================

def collect_fct_files(base_dir: Path) -> list[Path]:
    """TC6~9 / yyyymmdd / GoodFile/BadFile 아래의 모든 *.txt 수집."""
    file_list: list[Path] = []
    for tc in TC_FOLDERS:
        tc_path = base_dir / tc
        if not tc_path.exists():
            continue

        for date_dir in tc_path.iterdir():
            if not date_dir.is_dir():
                continue

            for gb in TARGET_FOLDERS:
                target_dir = date_dir / gb
                if not target_dir.exists():
                    continue

                for f in target_dir.glob("*.txt"):
                    file_list.append(f)

    return file_list


# ============================================
# 4) 한 번의 사이클에서 할 일
# ============================================

def process_once():
    """
    한 번 사이클:
    DB 초기화 → processed file_path 확인 → 새 파일 파싱 → DB 적재
    file_path 중복이면 해당 파일은 통째로 스킵
    """
    conn = get_connection()
    try:
        init_db(conn)

        processed_files = get_processed_file_paths(conn)
        all_files = collect_fct_files(BASE_LOG_DIR)

        new_files = [p for p in all_files if str(p) not in processed_files]

        # 이번 사이클 내 중복 제거
        seen = set()
        uniq_new_files = []
        for p in new_files:
            s = str(p)
            if s in seen:
                continue
            seen.add(s)
            uniq_new_files.append(p)
        new_files = uniq_new_files

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print(f"  총 파일 수: {len(all_files)}개")
        print(f"  이미 처리된 파일 수(DB:distinct file_path): {len(processed_files)}개")
        print(f"  이번에 새로 처리할 파일 수: {len(new_files)}개")

        if not new_files:
            return

        all_records: list[dict] = []

        if USE_MULTIPROCESSING:
            n_proc = max(cpu_count() - 1, 1)
            print(f"  멀티프로세싱 사용: 프로세스 {n_proc}개")

            with Pool(processes=n_proc) as pool:
                for idx, recs in enumerate(
                    pool.imap_unordered(parse_fct_file, new_files, chunksize=50), start=1
                ):
                    all_records.extend(recs)
                    if idx % 2000 == 0 or idx == len(new_files):
                        print(f"    → 현재 {idx}/{len(new_files)} 파일 파싱 완료")
        else:
            print("  단일 프로세스로 처리 (USE_MULTIPROCESSING = False)")
            for idx, f in enumerate(new_files, start=1):
                recs = parse_fct_file(f)
                all_records.extend(recs)
                if idx % 2000 == 0 or idx == len(new_files):
                    print(f"    → 현재 {idx}/{len(new_files)} 파일 파싱 완료")

        print(f"  총 레코드 수(파일 단위 행 수): {len(all_records)}개 → DB 적재 중...")
        insert_records(conn, all_records)
        print("  DB 적재 완료.")

    finally:
        conn.close()


# ============================================
# 5) 메인 루프 (1초마다 재실행)
# ============================================

def main_loop():
    print("=== a2_fct_table / fct_table 시작 (1초마다 폴링) ===")
    print(f"기본 로그 경로: {BASE_LOG_DIR}")
    print(f"DB: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['dbname']} (user={DB_CONFIG['user']})")

    while True:
        try:
            process_once()
        except Exception as e:
            print(f"[에러 발생] {e}")
        time.sleep(1)

def clean_bracket_result(s: str) -> str:
    # "[PASS]" -> "PASS", " [ FAIL ] " -> "FAIL"
    if s is None:
        return ""
    s = s.strip()
    if s.startswith("[") and s.endswith("]"):
        s = s[1:-1]
    return s.strip()


def parse_step_rows(lines: list[str]) -> list[dict]:
    """
    19~54번째 줄(1-indexed) = lines[18:54] 구간을
    CSV(콤마) 형태로 파싱하여 step rows 반환.
    """
    step_records = []

    start_idx = 18  # 19번째 줄
    end_idx = 54    # 54번째 줄까지 (파이썬 slice는 end exclusive)
    step_lines = lines[start_idx:end_idx] if len(lines) > start_idx else []

    for raw in step_lines:
        if not raw:
            continue

        # 콤마로 분리 (최소 5개 컬럼 필요)
        parts = [p.strip() for p in raw.split(",")]
        if len(parts) < 5:
            continue

        step_desc = parts[0]
        value     = parts[1]
        min_v     = parts[2]
        max_v     = parts[3]
        res_raw   = parts[4]

        # 결과가 비어있거나, step_desc가 비어있으면 스킵
        if not step_desc:
            continue

        step_records.append({
            "step_description": step_desc,
            "value": value,
            "min": min_v,
            "max": max_v,
            "result": clean_bracket_result(res_raw),
        })

    return step_records


if __name__ == "__main__":
    freeze_support()
    main_loop()
