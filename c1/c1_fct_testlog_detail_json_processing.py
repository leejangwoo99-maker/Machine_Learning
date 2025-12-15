import re
from pathlib import Path
from datetime import datetime
import time
from multiprocessing import Pool, freeze_support, cpu_count

import psycopg2
from psycopg2.extras import execute_values

# =========================
# 기본 경로 / DB 설정
# =========================
# ✅ 반드시 이 루트 아래의 YYYY/MM/DD 폴더 구조만 처리
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\machinlog\FCT")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# a. 비전 JSON 테이블 (station, result 매핑용)
SCHEMA_VISION = "a2_fct_table"
TABLE_VISION = "fct_table"

# ✅ 결과 테이블 (여기서 file_path로 중복 방지)
SCHEMA_RESULT = "c1_fct_testlog_detail_result"
TABLE_RESULT = "fct_testlog_detail_result"

# ✅ 멀티프로세스 최대량(논리 코어 수)
NUM_WORKERS = max(1, cpu_count())


# =====================================================================
# 0. 경로 구조 필터 (YYYY/MM/DD 강제)
# =====================================================================
def is_valid_yyyymmdd_path(root: Path, file_path: Path) -> bool:
    """
    root 기준 상대경로가 반드시:
      YYYY(4자리)/MM(2자리)/DD(2자리)/.../*.txt
    형태여야 True
    """
    try:
        rel = file_path.resolve().relative_to(root.resolve())
    except Exception:
        return False

    if len(rel.parts) < 3:
        return False

    yyyy, mm, dd = rel.parts[0], rel.parts[1], rel.parts[2]

    if not (yyyy.isdigit() and len(yyyy) == 4):
        return False
    if not (mm.isdigit() and len(mm) == 2):
        return False
    if not (dd.isdigit() and len(dd) == 2):
        return False

    # (선택) 월/일 범위 체크: 운영에서 오타 폴더 방지용
    m = int(mm)
    d = int(dd)
    if not (1 <= m <= 12):
        return False
    if not (1 <= d <= 31):
        return False

    return True


# =====================================================================
# 1. 로그 파싱 유틸
# =====================================================================
def read_lines_with_encodings(file_path: Path):
    for enc in ("cp949", "utf-8", "utf-8-sig"):
        try:
            with file_path.open("r", encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue
    with file_path.open("r", encoding="utf-8", errors="ignore") as f:
        return f.readlines()


def extract_yyyymmdd_from_name(name: str) -> str:
    candidates = re.findall(r"(20\d{6})", name)
    if candidates:
        return candidates[-1]

    dash_pos = name.find("-")
    if dash_pos != -1:
        underscore_pos = name.find("_", dash_pos + 1)
        if underscore_pos != -1 and underscore_pos > dash_pos + 1:
            candidate = name[dash_pos + 1:underscore_pos]
            if candidate.isdigit() and len(candidate) == 8:
                return candidate
    return ""


def parse_filename(filepath: Path):
    name = filepath.stem
    if "_" in name:
        barcode = name.split("_", 1)[0]
    else:
        barcode = name
    yyyymmdd = extract_yyyymmdd_from_name(name)
    return barcode, yyyymmdd


def parse_time_line(line: str):
    m = re.search(r"\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s+(.*)", line)
    if not m:
        return None, None, None
    time_str = m.group(1)
    raw = m.group(2)
    test_item = re.sub(r"\s{2,}", " ", raw).strip()
    return time_str, test_item, time_str


def time_to_seconds(time_str: str) -> float:
    t = datetime.strptime(time_str, "%H:%M:%S.%f")
    return t.hour * 3600 + t.minute * 60 + t.second + t.microsecond / 1_000_000


def get_end_time_str(last_time_str: str) -> str:
    t = datetime.strptime(last_time_str, "%H:%M:%S.%f")
    return t.strftime("%H:%M:%S")


def parse_one_file_to_rows(file_path: Path):
    """
    파일 1개 파싱 → rows(list[dict])
    """
    barcode, yyyymmdd = parse_filename(file_path)
    lines = read_lines_with_encodings(file_path)

    events = []
    for line in lines:
        time_str, test_item, test_time = parse_time_line(line)
        if time_str is None:
            continue
        events.append((time_str, test_item, test_time))

    if not events:
        return []

    last_time_str = events[-1][0]
    end_time = get_end_time_str(last_time_str)

    rows = []
    prev_sec = None
    for time_str, test_item, test_time in events:
        cur_sec = time_to_seconds(time_str)
        if prev_sec is None:
            ct_value = None
        else:
            diff = cur_sec - prev_sec
            if diff < 0:
                diff += 24 * 3600
            ct_value = round(diff, 2)
        prev_sec = cur_sec

        rows.append(
            {
                "file_path": str(file_path),
                "end_day": yyyymmdd,
                "end_time": end_time,
                "barcode_information": barcode,
                "test_item": test_item,
                "test_time": test_time,
                "test_item_ct": ct_value,
            }
        )

    return rows


# =====================================================================
# 2. PostgreSQL 유틸
# =====================================================================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_and_tables(conn):
    with conn.cursor() as cur:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RESULT};")

        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_RESULT} (
                id BIGSERIAL PRIMARY KEY
            );
            """
        )

        # 컬럼 호환(yyyymmdd -> end_day)
        cur.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA_RESULT}'
                      AND table_name   = '{TABLE_RESULT}'
                      AND column_name  = 'yyyymmdd'
                )
                AND NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = '{SCHEMA_RESULT}'
                      AND table_name   = '{TABLE_RESULT}'
                      AND column_name  = 'end_day'
                ) THEN
                    EXECUTE 'ALTER TABLE {SCHEMA_RESULT}.{TABLE_RESULT} RENAME COLUMN yyyymmdd TO end_day';
                END IF;
            END
            $$;
            """
        )

        cur.execute(
            f"""
            ALTER TABLE {SCHEMA_RESULT}.{TABLE_RESULT}
                ADD COLUMN IF NOT EXISTS file_path TEXT,
                ADD COLUMN IF NOT EXISTS end_day VARCHAR(8),
                ADD COLUMN IF NOT EXISTS end_time VARCHAR(8),
                ADD COLUMN IF NOT EXISTS barcode_information TEXT,
                ADD COLUMN IF NOT EXISTS station TEXT,
                ADD COLUMN IF NOT EXISTS remark TEXT,
                ADD COLUMN IF NOT EXISTS result TEXT,
                ADD COLUMN IF NOT EXISTS test_item TEXT,
                ADD COLUMN IF NOT EXISTS test_time VARCHAR(12),
                ADD COLUMN IF NOT EXISTS test_item_ct DOUBLE PRECISION,
                ADD COLUMN IF NOT EXISTS "group" BIGINT,
                ADD COLUMN IF NOT EXISTS problem1 TEXT,
                ADD COLUMN IF NOT EXISTS problem2 TEXT,
                ADD COLUMN IF NOT EXISTS problem3 TEXT,
                ADD COLUMN IF NOT EXISTS fail_test_item TEXT,
                ADD COLUMN IF NOT EXISTS processed_time TIMESTAMP WITHOUT TIME ZONE DEFAULT NOW();
            """
        )

        # ✅ DISTINCT file_path 조회 성능 향상용 인덱스 (권장)
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_RESULT}_file_path
            ON {SCHEMA_RESULT}.{TABLE_RESULT} (file_path);
            """
        )

    conn.commit()


def get_processed_file_paths_from_result(conn) -> set:
    """
    ✅ 중복방지: 결과 테이블에서 SELECT DISTINCT file_path
    """
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT file_path FROM {SCHEMA_RESULT}.{TABLE_RESULT} WHERE file_path IS NOT NULL;")
        rows = cur.fetchall()
    return {r[0] for r in rows}


def insert_results_local(rows: list[dict]):
    """
    워커 프로세스에서 호출됨:
    - 각 워커가 자기 커넥션으로 결과테이블에만 INSERT
    """
    if not rows:
        return 0

    conn = None
    try:
        conn = get_connection()
        with conn.cursor() as cur:
            cols = (
                "file_path",
                "end_day",
                "end_time",
                "barcode_information",
                "test_item",
                "test_time",
                "test_item_ct",
            )
            values = [
                (
                    r["file_path"],
                    r["end_day"],
                    r["end_time"],
                    r["barcode_information"],
                    r["test_item"],
                    r["test_time"],
                    r["test_item_ct"],
                )
                for r in rows
            ]

            insert_sql = f"""
                INSERT INTO {SCHEMA_RESULT}.{TABLE_RESULT} ({",".join(cols)})
                VALUES %s;
            """
            execute_values(cur, insert_sql, values, page_size=2000)

        conn.commit()
        return len(rows)

    except Exception as e:
        # 워커 내부 오류는 메인에서 메시지 볼 수 있게 리턴
        fp = rows[0]["file_path"] if rows else "UNKNOWN"
        return f"[DB ERROR] {fp}: {e}"

    finally:
        if conn:
            conn.close()


# =====================================================================
# 3. 후처리 (station/result/remark/group/problem 매핑)
# =====================================================================
def postprocess_result_table(conn):
    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT} b
            SET station = a.station,
                result  = a.result
            FROM {SCHEMA_VISION}.{TABLE_VISION} a
            WHERE a.barcode_information = b.barcode_information
              AND b.station IS NULL;
            """
        )
        print("[POST] station / result 매핑 완료")

        cur.execute(
            f"""
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT}
            SET remark =
                CASE
                    WHEN substring(barcode_information FROM 18 FOR 1) IN ('J','S')
                        THEN 'PD'
                    ELSE 'Non-PD'
                END;
            """
        )
        print("[POST] remark 채우기 완료")

        cur.execute(
            f"""
            WITH group_map AS (
                SELECT
                    id,
                    DENSE_RANK() OVER (
                        ORDER BY end_day, end_time, barcode_information
                    ) AS grp
                FROM {SCHEMA_RESULT}.{TABLE_RESULT}
            )
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT} t
            SET "group" = g.grp
            FROM group_map g
            WHERE t.id = g.id;
            """
        )
        print("[POST] group 번호 부여 완료")

        cur.execute(
            f"""
            UPDATE {SCHEMA_RESULT}.{TABLE_RESULT}
            SET problem1 = NULL,
                problem2 = NULL,
                problem3 = NULL,
                fail_test_item = NULL;
            """
        )
        print("[POST] problem1/2/3, fail_test_item 초기화 완료")

        # ---- 여기부터는 기존 6) PD 매핑, 7) Non-PD 매핑 블록을 그대로 붙여 넣으세요 ----

    conn.commit()


# =====================================================================
# 4. 워커: 파싱 + DB 저장 (멀티프로세스 대상)
# =====================================================================
def process_and_insert_one_file(file_path_str: str):
    try:
        p = Path(file_path_str)
        rows = parse_one_file_to_rows(p)
        if not rows:
            return (file_path_str, 0, None)

        inserted = insert_results_local(rows)
        if isinstance(inserted, str) and inserted.startswith("[DB ERROR]"):
            return (file_path_str, 0, inserted)

        return (file_path_str, inserted, None)

    except Exception as e:
        return (file_path_str, 0, str(e))


# =====================================================================
# 5. 메인 실행
# =====================================================================
def run_once():
    print("\n================ run_once 시작 ================")
    print(f"[DEBUG] BASE_LOG_DIR: {BASE_LOG_DIR}")

    conn = get_connection()
    ensure_schema_and_tables(conn)

    # ✅ 전체 스캔은 하되, 반드시 YYYY/MM/DD 경로 구조만 인정
    all_found_txt_files = list(BASE_LOG_DIR.rglob("*.txt"))
    valid_txt_files = [p for p in all_found_txt_files if is_valid_yyyymmdd_path(BASE_LOG_DIR, p)]
    print(f"1) TXT 파일 전체 스캔 → {len(all_found_txt_files)}개")
    print(f"2) YYYY/MM/DD 경로 조건 통과 파일 → {len(valid_txt_files)}개")

    # ✅ 중복방지: 결과 테이블에서 DISTINCT file_path
    processed = get_processed_file_paths_from_result(conn)
    print(f"3) 결과테이블 기준(SELECT DISTINCT file_path) 이미 처리된 파일 수: {len(processed)}개")

    target_files = [p for p in valid_txt_files if str(p) not in processed]
    total = len(target_files)
    print(f"4) 새로 처리할 파일 수: {total}개")

    if total == 0:
        conn.close()
        print("   → 새로 처리할 파일이 없습니다.")
        print("=============== run_once 종료 ===============\n")
        return

    print("5) 멀티프로세스(파싱+DB저장) 시작...")
    print(f"   → 워커 수: {NUM_WORKERS}개 (cpu_count 최대)")

    start_ts = time.time()

    with Pool(processes=NUM_WORKERS) as pool:
        for idx, (file_path_str, inserted_cnt, err) in enumerate(
            pool.imap_unordered(process_and_insert_one_file, [str(p) for p in target_files]),
            start=1,
        ):
            if err:
                print(f"   [ERROR] {file_path_str} 처리 중 오류: {err}")

            if (idx % 50 == 0) or (idx == total):
                print(f"   → {idx}/{total} 파일 처리 완료")

    elapsed = time.time() - start_ts
    print(f"   → 파싱+DB저장 소요 시간: {elapsed:.1f}초")

    # 후처리는 메인 프로세스에서 1회 수행
    postprocess_result_table(conn)

    conn.close()
    print("=============== run_once 종료 ===============\n")


def main():
    freeze_support()
    while True:
        try:
            run_once()
        except Exception as e:
            print(f"[MAIN ERROR] run_once 수행 중 오류: {e}")
        time.sleep(1)


if __name__ == "__main__":
    main()
