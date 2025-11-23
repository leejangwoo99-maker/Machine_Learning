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

# 예시: NAS 경로 (필요할 때 주석 해제해서 사용)
# BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")  # 예시: NAS 경로

# 기본: 로컬 RAW_LOG 경로
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")  # 로컬일 때 예시

TC_FOLDERS = ["TC6", "TC7", "TC8", "TC9"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

# PostgreSQL 접속 정보
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "a4_fct_json_table_processing"
TABLE_NAME = "fct_json_table_processing"

USE_MULTIPROCESSING = True  # 문제 생기면 False 로 바꿔서 단일프로세스로 테스트


# ============================================
# 1) PostgreSQL 관련 함수
# ============================================

def get_connection():
    """PostgreSQL 커넥션 생성."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = True
    return conn


def init_db(conn):
    """스키마와 테이블 생성 (존재하지 않으면)."""
    create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(
        sql.Identifier(SCHEMA_NAME)
    )

    create_table_sql = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {}.{} (
            id SERIAL PRIMARY KEY,
            file_path TEXT NOT NULL,
            station TEXT,
            barcode_information TEXT,
            step_description TEXT,
            value TEXT,
            min TEXT,
            max TEXT,
            result TEXT,
            created_at TIMESTAMP NOT NULL DEFAULT NOW()
        );
    """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

    with conn.cursor() as cur:
        cur.execute(create_schema_sql)
        cur.execute(create_table_sql)


def get_processed_file_paths(conn) -> set:
    """이미 DB에 적재된 file_path 목록(set) 조회."""
    query = sql.SQL("SELECT DISTINCT file_path FROM {}.{}").format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
    )
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_records(conn, records: list[dict]):
    """파싱된 레코드를 DB에 INSERT."""
    if not records:
        return

    # pandas -> 튜플 리스트로 변환해서 executemany 사용
    df = pd.DataFrame(records)

    # 컬럼 존재 보장
    expected_cols = [
        "file_path",
        "Station",
        "Barcode information",
        "step_description",
        "value",
        "min",
        "max",
        "result",
    ]
    for col in expected_cols:
        if col not in df.columns:
            df[col] = ""

    rows = list(
        df[expected_cols].itertuples(index=False, name=None)
    )

    insert_sql = sql.SQL("""
        INSERT INTO {}.{} (
            file_path,
            station,
            barcode_information,
            step_description,
            value,
            min,
            max,
            result
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

    with conn.cursor() as cur:
        cur.executemany(insert_sql, rows)


# ============================================
# 2) FCT 로그 파싱용 정규식
# ============================================

STATION_PATTERN = re.compile(r"Station\s*:?\s*(\S+)", re.IGNORECASE)
BARCODE_PATTERN = re.compile(r"Barcode\s+information\s*:?\s*(.+)", re.IGNORECASE)
STEP_PATTERN = re.compile(
    r"^(?P<desc>.+?)\s*,\s*(?P<value>[^,]*),\s*(?P<min>[^,]*),\s*(?P<max>[^,]*),\s*(?P<result>\[[^\]]*\])"
)


def normalize_step_desc(desc: str) -> str:
    """step description: 2개 이상 공백 -> 1개, 양끝 공백 제거."""
    return " ".join(desc.split())


def parse_fct_file(file_path: Path) -> list[dict]:
    """
    FCT 로그 한 개 파일을 읽어서
    JSON 레코드(딕셔너리) 리스트 반환.
    key는 기존 DataFrame 컬럼과 동일하게 사용:
      - Station
      - Barcode information
      - step_description
      - value
      - min
      - max
      - result
    + DB 중복 체크용 file_path 포함.
    """
    try:
        with file_path.open("r", encoding="cp949", errors="ignore") as f:
            lines = [line.rstrip("\n") for line in f]
    except UnicodeDecodeError:
        with file_path.open("r", encoding="utf-8", errors="ignore") as f:
            lines = [line.rstrip("\n") for line in f]

    if not lines:
        return []

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

    records = []
    for line in lines:
        m = STEP_PATTERN.match(line)
        if not m:
            continue

        desc_raw = m.group("desc")
        value_raw = m.group("value")
        min_raw = m.group("min")
        max_raw = m.group("max")
        result_raw = m.group("result")

        step_desc = normalize_step_desc(desc_raw)

        rec = {
            "file_path": str(file_path),
            "Station": station if station is not None else "",
            "Barcode information": barcode if barcode is not None else "",
            "step_description": step_desc,
            "value": str(value_raw).strip(),
            "min": str(min_raw).strip(),
            "max": str(max_raw).strip(),
            "result": str(result_raw).strip(),
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
    """한 번 사이클: DB 초기화 → 중복 file_path 확인 → 새 파일 파싱 → DB 적재."""
    conn = get_connection()
    try:
        init_db(conn)

        # 이미 처리된 file_path 목록
        processed_files = get_processed_file_paths(conn)

        # 전체 파일 스캔
        all_files = collect_fct_files(BASE_LOG_DIR)
        all_files_str = [str(p) for p in all_files]

        new_files = [Path(p) for p in all_files_str if p not in processed_files]

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print(f"  총 파일 수: {len(all_files)}개")
        print(f"  이미 처리된 파일 수(DB): {len(processed_files)}개")
        print(f"  이번에 새로 처리할 파일 수: {len(new_files)}개")

        all_records: list[dict] = []

        if not new_files:
            return  # 새 파일 없으면 끝

        if USE_MULTIPROCESSING:
            n_proc = max(cpu_count() - 1, 1)
            print(f"  멀티프로세싱 사용: 프로세스 {n_proc}개")

            with Pool(processes=n_proc) as pool:
                for idx, recs in enumerate(
                    pool.imap_unordered(parse_fct_file, new_files, chunksize=10), start=1
                ):
                    all_records.extend(recs)
                    if idx % 1000 == 0 or idx == len(new_files):
                        print(f"    → 현재 {idx}/{len(new_files)} 파일 파싱 완료")
        else:
            print("  단일 프로세스로 처리 (USE_MULTIPROCESSING = False)")
            for idx, f in enumerate(new_files, start=1):
                recs = parse_fct_file(f)
                all_records.extend(recs)
                if idx % 1000 == 0 or idx == len(new_files):
                    print(f"    → 현재 {idx}/{len(new_files)} 파일 파싱 완료")

        # DB INSERT
        print(f"  총 레코드 수(행 수): {len(all_records)}개 → DB 적재 중...")
        insert_records(conn, all_records)
        print("  DB 적재 완료.")

    finally:
        conn.close()


# ============================================
# 5) 메인 루프 (1초마다 재실행)
# ============================================

def main_loop():
    print("=== a4_fct_json_table_processing 시작 (1초마다 폴링) ===")
    print(f"기본 로그 경로: {BASE_LOG_DIR}")
    print(f"DB: {DB_CONFIG['host']}:{DB_CONFIG['port']} / {DB_CONFIG['dbname']} (user={DB_CONFIG['user']})")

    while True:
        try:
            process_once()
        except Exception as e:
            print(f"[에러 발생] {e}")
        # 1초 대기 후 다시 실행
        time.sleep(1)


if __name__ == "__main__":
    freeze_support()  # 윈도우 / exe 변환 시 안전
    main_loop()
