from pathlib import Path
import re
import time
from datetime import datetime, date
from multiprocessing import Pool, cpu_count, freeze_support
import calendar

import psycopg2
from psycopg2 import sql

# ============================================
# 0) 기본 경로 / DB 설정
# ============================================

# NAS 경로
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")

TC_FOLDERS = ["TC6", "TC7", "TC8", "TC9"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "a4_fct_json_table_processing"
TABLE_NAME = "fct_json_table_processing"

USE_MULTIPROCESSING = True  # 문제 생기면 False 로 바꿔서 단일프로세스로 테스트

# 고정 최소 시작일 (2025-10-01 이전 폴더는 전부 제외)
FIXED_START_DATE = date(2025, 10, 1)

# 한 번에 DB에 넣을 최대 row 수 (메모리 최적화용)
BATCH_SIZE_ROWS = 50000


# ============================================
# 날짜 윈도우 계산
# ============================================

def six_months_ago(d: date) -> date:
    """
    오늘 기준 6개월 전 날짜 계산 (relativedelta 없이 직접 구현).
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
    - today: 오늘
    - window_start_date: max(FIXED_START_DATE, today-6개월)
    - window_end_date: today

    예)
      처음엔 2025-10-01 ~ 오늘
      시간이 지나서 today-6개월이 2026-02-02라면 → 2026-02-02 ~ today
    """
    today = date.today()
    six_before = six_months_ago(today)
    window_start_date = max(FIXED_START_DATE, six_before)
    window_end_date = today
    return window_start_date, window_end_date


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


def cleanup_old_data(conn, window_start_date: date):
    """
    현재 날짜 기준 6개월 이상된 DB 데이터 삭제 (DELETE).
    created_at < window_start_date 00:00:00 기준으로 삭제.
    """
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    delete_sql = sql.SQL("""
        DELETE FROM {}.{}
        WHERE created_at < %s
    """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

    with conn.cursor() as cur:
        cur.execute(delete_sql, (cutoff_dt,))
        deleted = cur.rowcount

    print(f"[정리] 6개월 이전 DB 데이터 삭제 완료 (rows={deleted})")


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


def insert_records(conn, records: list[dict]) -> int:
    """파싱된 레코드를 DB에 INSERT."""
    if not records:
        return 0

    rows = []
    for r in records:
        rows.append((
            r.get("file_path", ""),
            r.get("Station", ""),
            r.get("Barcode information", ""),
            r.get("step_description", ""),
            r.get("value", ""),
            r.get("min", ""),
            r.get("max", ""),
            r.get("result", ""),
        ))

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

    return len(rows)


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
    key:
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
# 3) 파일 수집 (날짜 윈도우 적용)
# ============================================

def collect_fct_files(base_dir: Path, window_start_str: str, window_end_str: str) -> list[Path]:
    """
    TC6~9 / yyyymmdd / GoodFile/BadFile 아래의 모든 *.txt 수집.
    날짜 폴더는 window_start_str ~ window_end_str 범위만 처리.
    """
    file_list: list[Path] = []

    for tc in TC_FOLDERS:
        tc_path = base_dir / tc
        if not tc_path.exists():
            continue

        for date_dir in tc_path.iterdir():
            if not date_dir.is_dir():
                continue

            folder_name = date_dir.name.strip()
            # 폴더명 yyyymmdd 검사
            if not (folder_name.isdigit() and len(folder_name) == 8):
                continue

            # 날짜 윈도우 범위 체크
            if not (window_start_str <= folder_name <= window_end_str):
                continue

            # GoodFile / BadFile
            for gb in TARGET_FOLDERS:
                target_dir = date_dir / gb
                if not target_dir.exists():
                    continue

                # .txt 수집
                for f in target_dir.glob("*.txt"):
                    file_list.append(f)

    return file_list


# ============================================
# 4) 한 번의 사이클에서 할 일
# ============================================

def process_once():
    """한 번 사이클: 날짜 윈도우 적용 → DB 정리 → 중복 file_path 확인 → 새 파일 파싱 → 배치 DB 적재."""
    window_start_date, window_end_date = get_window_dates()
    window_start_str = window_start_date.strftime("%Y%m%d")
    window_end_str = window_end_date.strftime("%Y%m%d")

    print("\n==============================================")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] process_once 시작")
    print(f"[윈도우] 폴더/데이터 유효 기간: {window_start_date} ~ {window_end_date}")

    conn = get_connection()
    try:
        init_db(conn)

        # 6개월 이전 DB 데이터 정리
        cleanup_old_data(conn, window_start_date)

        # 정리 후, 이미 처리된 file_path 목록
        processed_files = get_processed_file_paths(conn)

        # 전체 파일 스캔 (날짜 윈도우 적용)
        all_files = collect_fct_files(BASE_LOG_DIR, window_start_str, window_end_str)
        all_files_str = [str(p) for p in all_files]

        new_files = [Path(p) for p in all_files_str if p not in processed_files]

        print(f"  총 파일 수(윈도우 내): {len(all_files)}개")
        print(f"  이미 처리된 파일 수(DB): {len(processed_files)}개")
        print(f"  이번에 새로 처리할 파일 수: {len(new_files)}개")

        if not new_files:
            print("  새로 처리할 파일 없음. 사이클 종료.")
            return  # 새 파일 없으면 끝

        total_inserted_rows = 0
        batch_records: list[dict] = []

        if USE_MULTIPROCESSING:
            # 🔥 멀티프로세싱 워커 수를 항상 2개로 고정
            n_proc = 2
            print(f"  멀티프로세싱 사용: 프로세스 {n_proc}개")

            with Pool(processes=n_proc) as pool:
                for idx, recs in enumerate(
                    pool.imap_unordered(parse_fct_file, new_files, chunksize=10), start=1
                ):
                    if recs:
                        batch_records.extend(recs)

                    # 배치 크기 도달 시 DB INSERT
                    if len(batch_records) >= BATCH_SIZE_ROWS:
                        inserted = insert_records(conn, batch_records)
                        total_inserted_rows += inserted
                        print(
                            f"    → 배치 INSERT (rows={inserted}, 누적 rows={total_inserted_rows}) "
                            f" at file {idx}/{len(new_files)}"
                        )
                        batch_records.clear()

                    if idx % 1000 == 0 or idx == len(new_files):
                        print(f"    → 현재 {idx}/{len(new_files)} 파일 파싱 완료")

        else:
            print("  단일 프로세스로 처리 (USE_MULTIPROCESSING = False)")
            for idx, f in enumerate(new_files, start=1):
                recs = parse_fct_file(f)
                if recs:
                    batch_records.extend(recs)

                if len(batch_records) >= BATCH_SIZE_ROWS:
                    inserted = insert_records(conn, batch_records)
                    total_inserted_rows += inserted
                    print(
                        f"    → 배치 INSERT (rows={inserted}, 누적 rows={total_inserted_rows}) "
                        f" at file {idx}/{len(new_files)}"
                    )
                    batch_records.clear()

                if idx % 1000 == 0 or idx == len(new_files):
                    print(f"    → 현재 {idx}/{len(new_files)} 파일 파싱 완료")

        # 남은 배치 처리
        if batch_records:
            inserted = insert_records(conn, batch_records)
            total_inserted_rows += inserted
            print(
                f"  마지막 배치 INSERT (rows={inserted}, 누적 rows={total_inserted_rows})"
            )

        print(f"  총 INSERT된 레코드 수: {total_inserted_rows}개")
        print("  DB 적재 완료.")

    finally:
        conn.close()
        print("process_once 종료")
        print("==============================================\n")


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
