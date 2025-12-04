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

# 실시간용: 최근 N초 이내 수정된 파일만 처리
REALTIME_LOOKBACK_SECONDS = 120  # 예: 최근 2분


# ============================================
# 날짜 윈도우 계산
# ============================================

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
    오늘 날짜 기준으로 '이번 달 1일 ~ 오늘' 범위를 반환.
    예)
      - 오늘 = 2025-12-04 → 2025-12-01 ~ 2025-12-04
      - 오늘 = 2025-12-31 → 2025-12-01 ~ 2025-12-31
      - 오늘 = 2026-01-01 → 2026-01-01 ~ 2026-01-01

    FIXED_START_DATE 이전은 무조건 제외.
    """
    today = date.today()

    # 이번 달 1일
    month_start = today.replace(day=1)

    # 고정 시작일 이후만
    window_start_date = max(month_start, FIXED_START_DATE)
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
    window_start_date 이전 DB 데이터 삭제 (DELETE).
    created_at < window_start_date 00:00:00 기준으로 삭제.

    ※ 현재 process_once에서는 호출하지 않음.
       필요 시 psql 또는 별도 관리 스크립트에서 실행하는 것을 권장.
    """
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    delete_sql = sql.SQL("""
        DELETE FROM {}.{}
        WHERE created_at < %s
    """).format(sql.Identifier(SCHEMA_NAME), sql.Identifier(TABLE_NAME))

    with conn.cursor() as cur:
        cur.execute(delete_sql, (cutoff_dt,))
        deleted = cur.rowcount

    print(f"[정리] window_start 이전 DB 데이터 삭제 완료 (rows={deleted})")


def get_processed_file_paths(conn, window_start_date: date) -> set:
    """
    이미 DB에 적재된 file_path 목록(set) 조회.
    - created_at >= window_start_date 기준으로만 조회해서
      오래된 데이터는 자동으로 제외 (윈도우 내 중복만 방지).
    """
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    query = sql.SQL("""
        SELECT DISTINCT file_path
        FROM {}.{}
        WHERE created_at >= %s
    """).format(
        sql.Identifier(SCHEMA_NAME),
        sql.Identifier(TABLE_NAME),
    )

    with conn.cursor() as cur:
        cur.execute(query, (cutoff_dt,))
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
# 3) 파일 수집 (날짜 윈도우 + mtime 필터)
# ============================================

def collect_fct_files(
    base_dir: Path,
    window_start_str: str,
    window_end_str: str,
    cutoff_ts: float,
) -> list[Path]:
    """
    TC6~9 / yyyymmdd / GoodFile/BadFile 아래의 모든 *.txt 수집.
    - 날짜 폴더는 window_start_str ~ window_end_str 범위만 처리.
    - 파일 mtime이 cutoff_ts (최근 REALTIME_LOOKBACK_SECONDS초) 이후인 경우만 대상.
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
                    try:
                        if f.stat().st_mtime < cutoff_ts:
                            # 실시간 윈도우 밖이면 건너뜀
                            continue
                    except FileNotFoundError:
                        continue

                    file_list.append(f)

    return file_list


# ============================================
# 4) 한 번의 사이클에서 할 일
# ============================================

def process_once():
    """
    한 번 사이클:
      - 날짜 윈도우(이번 달 1일 ~ 오늘) 적용
      - 실시간 mtime 윈도우 적용 (최근 N초)
      - 이미 처리된 file_path(윈도우 내 created_at 기준) 조회
      - 새 파일만 파싱 → 배치 단위로 DB 적재
    """
    cycle_start = time.time()
    window_start_date, window_end_date = get_window_dates()
    window_start_str = window_start_date.strftime("%Y%m%d")
    window_end_str = window_end_date.strftime("%Y%m%d")

    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_LOOKBACK_SECONDS

    print("\n==============================================")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] process_once 시작")
    print(f"[윈도우] 폴더/데이터 유효 기간: {window_start_date} ~ {window_end_date}")
    print(f"[실시간] 최근 {REALTIME_LOOKBACK_SECONDS}초 이내 수정된 파일만 처리 (cutoff_ts={cutoff_ts})")

    conn = get_connection()
    try:
        init_db(conn)

        # ✅ 삭제는 DB 측에서 별도 SQL로 관리하는 것을 권장
        # cleanup_old_data(conn, window_start_date)

        # 윈도우 이후(created_at >= window_start_date) 기준으로,
        # 이미 처리된 file_path 목록
        processed_files = get_processed_file_paths(conn, window_start_date)

        # 전체 파일 스캔 (날짜 윈도우 + mtime 윈도우 적용)
        all_files = collect_fct_files(BASE_LOG_DIR, window_start_str, window_end_str, cutoff_ts)
        all_files_str = [str(p) for p in all_files]

        new_files = [Path(p) for p in all_files_str if p not in processed_files]

        print(f"  총 파일 수(폴더+mtime 윈도우 내): {len(all_files)}개")
        print(f"  이미 처리된 파일 수(DB, created_at>=윈도우): {len(processed_files)}개")
        print(f"  이번에 새로 처리할 파일 수: {len(new_files)}개")

        if not new_files:
            print("  새로 처리할 파일 없음. 사이클 종료.")
            return  # 새 파일 없으면 끝

        total_inserted_rows = 0
        batch_records: list[dict] = []

        if USE_MULTIPROCESSING:
            # 🔥 멀티프로세싱 워커 수를 항상 4개로 고정
            n_proc = 4
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

        cycle_end = time.time()
        print(f"  총 INSERT된 레코드 수: {total_inserted_rows}개")
        print(f"  DB 적재 완료. (사이클 소요 시간: {cycle_end - cycle_start:.1f}초)")

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
