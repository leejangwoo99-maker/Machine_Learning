import re
import math
from pathlib import Path
from datetime import datetime, date
import time
from multiprocessing import Pool, cpu_count, freeze_support
import calendar

import psycopg2
from psycopg2 import sql


# =========================
# 기본 경로 설정
# =========================
BASE_LOG_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

# =========================
# PostgreSQL 설정
# =========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_PROCESSING = "c1_fct_testlog_detail_jason_processing"
TABLE_PROCESSING = "fct_testlog_detail_jason_processing"

SCHEMA_RESULT = "c1_fct_testlog_detail_result"
TABLE_RESULT = "fct_testlog_detail_result"

# 고정 최소 시작일 (이 날짜 이전 데이터는 무시)
FIXED_START_DATE = date(2025, 10, 1)

# 한 번에 INSERT할 최대 row 수 (향후 배치 확장용)
BATCH_SIZE_ROWS = 50000

# 실시간용: 최근 N초 이내 수정된 파일만 처리
REALTIME_LOOKBACK_SECONDS = 120  # 예: 최근 2분


# =========================
# 날짜 윈도우 유틸
# =========================
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
    FIXED_START_DATE 이전은 무조건 제외.
    """
    today = date.today()

    # 이번 달 1일
    month_start = today.replace(day=1)

    # 고정 시작일 이후만
    window_start_date = max(month_start, FIXED_START_DATE)
    window_end_date = today
    return window_start_date, window_end_date


# =========================
# 유틸 함수들
# =========================
def read_lines_with_encodings(file_path: Path):
    """
    여러 인코딩(cp949, utf-8, utf-8-sig)을 시도해서
    글자가 깨지지 않도록 안전하게 읽기.
    """
    for enc in ("cp949", "utf-8", "utf-8-sig"):
        try:
            with file_path.open("r", encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue

    # 그래도 안 되면 마지막에만 ignore 사용
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        return f.readlines()


def extract_yyyymmdd_from_name(name: str) -> str:
    """
    파일명(stem)에서 YYYYMMDD 추출.
    우선 정규식으로 20xxxxxx (8자리 숫자) 패턴을 찾고,
    없으면 사용자가 말한 규칙:
      - 첫 번째 '-' 와 그 뒤 첫 번째 '_' 사이
    를 시도한다.
    """
    # 1) 정규식으로 8자리 날짜(20xxxxxx) 찾기
    candidates = re.findall(r"(20\d{6})", name)
    if candidates:
        # 마지막 쪽이 진짜 날짜일 가능성이 큼
        return candidates[-1]

    # 2) fallback: 첫 번째 '-' 와 그 뒤 첫 번째 '_' 사이
    dash_pos = name.find("-")
    if dash_pos != -1:
        underscore_pos = name.find("_", dash_pos + 1)
        if underscore_pos != -1 and underscore_pos > dash_pos + 1:
            candidate = name[dash_pos + 1:underscore_pos]
            if candidate.isdigit() and len(candidate) == 8:
                return candidate

    # 3) 그래도 못 찾으면 빈 문자열
    return ""


def parse_filename(filepath: Path):
    """
    파일명에서 Barcode information, YYYYMMDD 추출

    규칙(복합):
    1) 확장자(.txt) 제거
    2) 첫 번째 '_' 앞까지 → Barcode information
    3) YYYYMMDD:
       - 우선 정규식(20xxxxxx 8자리)으로 찾기
       - 없으면 '첫 번째 - 와 두 번째 _ 사이' 규칙 시도
    """
    name = filepath.stem  # 확장자 제거

    # 2) Barcode information
    if "_" in name:
        barcode = name.split("_", 1)[0]
    else:
        barcode = name  # '_'가 없으면 전체를 바코드로

    # 3) YYYYMMDD
    yyyymmdd = extract_yyyymmdd_from_name(name)

    return barcode, yyyymmdd


def parse_time_line(line: str):
    """
    로그 한 줄에서 [hh:mm:ss.ss] 와 Test_item, Test_Time 추출
    - [hh:mm:ss.ss] 가 없으면 (None, None, None) 반환
    - Test_item 내부의 2개 이상 공백은 1개로 축소
    - Test_Time 은 time_str 그대로 (hh:mm:ss.ss)
    """
    m = re.search(r"\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s+(.*)", line)
    if not m:
        return None, None, None

    time_str = m.group(1)  # "hh:mm:ss.ss"
    test_item_raw = m.group(2)

    # 내용안에 공백 1개까지 허용, 2개 이상의 공백은 1개로 축소
    test_item = re.sub(r"\s{2,}", " ", test_item_raw).strip()

    test_time = time_str  # 그대로 저장

    return time_str, test_item, test_time


def time_to_seconds(time_str: str) -> float:
    """
    "hh:mm:ss.ss" → 초(float)로 변환
    """
    t = datetime.strptime(time_str, "%H:%M:%S.%f")
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1_000_000


def get_end_time_str(last_time_str: str) -> str:
    """
    마지막 [hh:mm:ss.ss] 에서 hh:mm:ss 만 추출해 문자열로 반환
    """
    t = datetime.strptime(last_time_str, "%H:%M:%S.%f")
    return t.strftime("%H:%M:%S")


def is_valid_deep_fct_path(p: Path, window_start_str: str, window_end_str: str) -> bool:
    """
    BASE_LOG_DIR 기준 상대경로가
    YYYY/MM/DD/어떤폴더/파일 구조인지 확인하고,
    그 YYYYMMDD가 window_start_str ~ window_end_str 범위에 들어가는지 확인.
    """
    try:
        rel = p.relative_to(BASE_LOG_DIR)
    except ValueError:
        return False

    parts = rel.parts  # ('2025', '10', '01', '...', 'file.txt') 등

    # 최소 구조: YYYY / MM / DD / (폴더) / 파일 → 4개 이상
    if len(parts) < 4:
        return False

    year, month, day = parts[0], parts[1], parts[2]

    if not (year.isdigit() and len(year) == 4):
        return False
    if not (month.isdigit() and len(month) == 2):
        return False
    if not (day.isdigit() and len(day) == 2):
        return False

    yyyymmdd = f"{year}{month}{day}"

    # 날짜 윈도우 범위 체크
    if not (window_start_str <= yyyymmdd <= window_end_str):
        return False

    return True


def extract_result_from_lines(lines):
    """
    파일 전체 라인에서 '테스트 결과 : NG/OK' 를 찾아 PASS/FAIL 리턴.
    - 마지막에 나오는 결과 기준.
    - NG → 'FAIL', OK → 'PASS'
    - 못 찾으면 None
    """
    for line in reversed(lines):
        m = re.search(
            r"\[\d{2}:\d{2}:\d{2}\.\d{2}\]\s*테스트 결과\s*:\s*(NG|OK)",
            line,
        )
        if m:
            status = m.group(1).strip().upper()
            if status == "NG":
                return "FAIL"
            elif status == "OK":
                return "PASS"
    return None


def process_one_file(file_path_str: str):
    """
    멀티프로세스에서 사용할 워커 함수.
    하나의 txt 파일을 파싱해서 (file_path, rows, error) 반환.

    rows 의 각 원소는 아래 컬럼을 가짐:
    - file_path
    - yyyymmdd
    - end_time
    - barcode_information
    - test_item
    - test_time
    - test_item_ct
    - result   ← PASS/FAIL (없으면 None)
    """
    file_path = Path(file_path_str)
    try:
        barcode, yyyymmdd = parse_filename(file_path)

        # 파일 내용 읽기 (인코딩 자동 처리)
        lines = read_lines_with_encodings(file_path)

        # 파일 전체에서 테스트 결과(PASS/FAIL) 추출
        result_status = extract_result_from_lines(lines)

        events = []  # (time_str, test_item, test_time)

        for line in lines:
            time_str, test_item, test_time = parse_time_line(line)
            if time_str is None:
                # [hh:mm:ss.ss] 가 없는 행은 완전히 무시
                continue
            events.append((time_str, test_item, test_time))

        # 유효한 타임스탬프가 하나도 없으면 이 파일은 스킵
        if not events:
            return file_path_str, [], None

        # End time: 마지막 이벤트의 시간에서 hh:mm:ss 추출
        last_time_str = events[-1][0]
        end_time = get_end_time_str(last_time_str)

        # Test_item_CT 계산
        rows = []
        prev_sec = None

        for time_str, test_item, test_time in events:
            cur_sec = time_to_seconds(time_str)

            if prev_sec is None:
                ct_value = None  # 첫 번째 Test_item은 NULL
            else:
                diff = cur_sec - prev_sec
                # 만약 시간 차가 음수면(자정 넘어간 경우 등) 24시간 더해줌
                if diff < 0:
                    diff += 24 * 3600
                ct_value = round(diff, 2)

            prev_sec = cur_sec

            rows.append(
                {
                    "file_path": file_path_str,
                    "yyyymmdd": yyyymmdd,
                    "end_time": end_time,
                    "barcode_information": barcode,
                    "test_item": test_item,
                    "test_time": test_time,
                    "test_item_ct": ct_value,
                    "result": result_status,
                }
            )

        return file_path_str, rows, None

    except Exception as e:
        return file_path_str, [], str(e)


# =========================
# PostgreSQL 관련 함수
# =========================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_tables(conn):
    with conn.cursor() as cur:
        # 스키마 생성
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_PROCESSING)))
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_RESULT)))

        # 처리 이력 테이블
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    id BIGSERIAL PRIMARY KEY,
                    file_path TEXT UNIQUE,
                    processed_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(SCHEMA_PROCESSING), sql.Identifier(TABLE_PROCESSING))
        )

        # 결과 저장 테이블 (result 컬럼 포함)
        cur.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {}.{} (
                    id BIGSERIAL PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    yyyymmdd VARCHAR(8),
                    end_time VARCHAR(8),
                    barcode_information TEXT,
                    test_item TEXT,
                    test_time VARCHAR(12),
                    test_item_ct DOUBLE PRECISION,
                    result VARCHAR(10),
                    processed_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW()
                )
                """
            ).format(sql.Identifier(SCHEMA_RESULT), sql.Identifier(TABLE_RESULT))
        )

        # 이미 존재하는 경우를 대비해 result 컬럼 보장
        cur.execute(
            sql.SQL(
                "ALTER TABLE {}.{} "
                "ADD COLUMN IF NOT EXISTS result VARCHAR(10)"
            ).format(sql.Identifier(SCHEMA_RESULT), sql.Identifier(TABLE_RESULT))
        )

        # 🔹 중복 방지용 유니크 인덱스
        #    (yyyymmdd, end_time, barcode_information, test_item) 조합이 유일하도록
        cur.execute(
            sql.SQL(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_fct_testlog_detail_result_uniq4
                ON {}.{} (yyyymmdd, end_time, barcode_information, test_item)
                """
            ).format(sql.Identifier(SCHEMA_RESULT), sql.Identifier(TABLE_RESULT))
        )

    conn.commit()


def cleanup_old_data(conn, window_start_date: date):
    """
    기준 날짜 이전 데이터 삭제.

    - 결과 테이블 : yyyymmdd < window_start_str 인 데이터 삭제
    - 처리 이력   : processed_time < window_start_date 00:00:00 인 데이터 삭제

    ※ 현재 run_once()에서는 자동 호출하지 않음.
       필요 시 수동으로 돌리는 것을 권장.
    """
    window_start_str = window_start_date.strftime("%Y%m%d")
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    with conn.cursor() as cur:
        # 결과 테이블 삭제 (yyyymmdd 기준)
        cur.execute(
            sql.SQL(
                """
                DELETE FROM {}.{}
                WHERE yyyymmdd IS NOT NULL
                  AND yyyymmdd <> ''
                  AND yyyymmdd < %s
                """
            ).format(sql.Identifier(SCHEMA_RESULT), sql.Identifier(TABLE_RESULT)),
            (window_start_str,),
        )
        deleted_result = cur.rowcount

        # 처리 이력 테이블 삭제 (processed_time 기준)
        cur.execute(
            sql.SQL(
                """
                DELETE FROM {}.{}
                WHERE processed_time < %s
                """
            ).format(sql.Identifier(SCHEMA_PROCESSING), sql.Identifier(TABLE_PROCESSING)),
            (cutoff_dt,),
        )
        deleted_hist = cur.rowcount

    conn.commit()
    print(
        f"[정리] 기준 이전 데이터 삭제 완료 "
        f"(result rows={deleted_result}, history rows={deleted_hist})"
    )


def get_processed_file_paths(conn, window_start_date: date):
    """
    이미 처리된 file_path 목록을 DB에서 가져와 set으로 반환.
    - processed_time >= window_start_date 기준으로만 조회
      (이번 달 윈도우 내 데이터만 중복 체크)
    """
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    with conn.cursor() as cur:
        cur.execute(
            sql.SQL(
                "SELECT file_path FROM {}.{} WHERE processed_time >= %s"
            ).format(
                sql.Identifier(SCHEMA_PROCESSING),
                sql.Identifier(TABLE_PROCESSING),
            ),
            (cutoff_dt,),
        )
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_results_and_history(conn, file_path, rows):
    """
    한 파일에 대한 파싱 결과(rows)를 결과 테이블에 INSERT 하고,
    처리 이력 테이블에도 file_path를 기록.

    🔹 (yyyymmdd, end_time, barcode_information, test_item) 조합이
       이미 존재하면 해당 행은 INSERT 하지 않음.
    """
    if not rows:
        return

    with conn.cursor() as cur:
        # 결과 테이블에 다중 INSERT (result 포함)
        insert_query = sql.SQL(
            """
            INSERT INTO {}.{} (
                file_path,
                yyyymmdd,
                end_time,
                barcode_information,
                test_item,
                test_time,
                test_item_ct,
                result
            )
            VALUES (%(file_path)s, %(yyyymmdd)s, %(end_time)s,
                    %(barcode_information)s, %(test_item)s,
                    %(test_time)s, %(test_item_ct)s, %(result)s)
            ON CONFLICT (yyyymmdd, end_time, barcode_information, test_item) DO NOTHING
            """
        ).format(sql.Identifier(SCHEMA_RESULT), sql.Identifier(TABLE_RESULT))

        cur.executemany(insert_query, rows)

        # 처리 이력 테이블에 file_path 기록 (중복이면 무시)
        cur.execute(
            sql.SQL(
                """
                INSERT INTO {}.{} (file_path, processed_time)
                VALUES (%s, NOW())
                ON CONFLICT (file_path) DO NOTHING
                """
            ).format(sql.Identifier(SCHEMA_PROCESSING), sql.Identifier(TABLE_PROCESSING)),
            (file_path,),
        )

    conn.commit()


# =========================
# 메인 1회 수행 로직
# =========================
def run_once():
    # 날짜 윈도우 계산 (이번 달 1일 ~ 오늘)
    window_start_date, window_end_date = get_window_dates()
    window_start_str = window_start_date.strftime("%Y%m%d")
    window_end_str = window_end_date.strftime("%Y%m%d")

    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_LOOKBACK_SECONDS

    print("\n================ run_once 시작 ================")
    print(f"[윈도우] 유효 날짜 범위: {window_start_date} ~ {window_end_date}")
    print(f"[실시간] 최근 {REALTIME_LOOKBACK_SECONDS}초 이내 수정된 파일만 처리 (cutoff_ts={cutoff_ts})")
    print(f"[DEBUG] BASE_LOG_DIR: {BASE_LOG_DIR}")

    # 0) DB 연결 및 스키마/테이블 준비
    conn = get_connection()
    ensure_schema_and_tables(conn)

    # 1) 전체 TXT 파일 스캔 (윈도우 + mtime 필터)
    all_found_txt_files = list(BASE_LOG_DIR.rglob("*.txt"))

    all_txt_files = []
    for p in all_found_txt_files:
        if not is_valid_deep_fct_path(p, window_start_str, window_end_str):
            continue
        try:
            if p.stat().st_mtime < cutoff_ts:
                # 실시간 윈도우 밖이면 제외
                continue
        except FileNotFoundError:
            continue
        all_txt_files.append(p)

    print(f"1) TXT 파일 스캔 완료 → 총 파일 수집(윈도우+mtime 내): {len(all_txt_files)}개")

    # 2) DB 이력 로드 (윈도우 내 already processed)
    processed_file_paths = get_processed_file_paths(conn, window_start_date)
    print(f"2) DB에서 불러온 이전 처리 파일 수(윈도우 내): {len(processed_file_paths)}개")

    target_files = [p for p in all_txt_files if str(p) not in processed_file_paths]
    total = len(target_files)
    print(f"3) 이번에 새로 처리할 대상 파일 수: {total}개")

    if total == 0:
        conn.close()
        print("   → 새로 처리할 파일이 없습니다.")
        print("=============== run_once 종료 ===============\n")
        return

    print("4) TXT 파일 멀티프로세스 처리 시작...")

    # 멀티프로세스 풀 구성 - 항상 2개 프로세스만 사용
    num_workers = 2
    print(f"   → 멀티프로세싱 워커 수: {num_workers}개")

    start_ts = time.time()

    with Pool(processes=num_workers) as pool:
        for idx, (file_path_str, rows, err) in enumerate(
            pool.imap_unordered(process_one_file, [str(p) for p in target_files]), start=1
        ):
            if err:
                print(f"   [ERROR] {file_path_str} 처리 중 오류: {err}")
                continue

            if not rows:
                # 유효 로그 없으면 skip
                continue

            # DB에 INSERT + 이력 기록 (파일 단위, 메모리 최소화)
            insert_results_and_history(conn, file_path_str, rows)

            # 진행 상황 출력
            if (idx % 1000 == 0) or (idx == total):
                print(f"   → 현재 {idx}/{total} 파일 처리 및 DB 저장 완료")

    elapsed = time.time() - start_ts
    print(f"   → 이번 run_once 처리 시간: {elapsed:.1f}초")
    print("=============== run_once 종료 ===============\n")


# =========================
# 엔트리 포인트
# =========================
def main():
    freeze_support()
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[MAIN ERROR] run_once 수행 중 오류: {e}")
        # 1초 대기 후 다시 실행 (무한 루프)
        time.sleep(1)


if __name__ == "__main__":
    main()
