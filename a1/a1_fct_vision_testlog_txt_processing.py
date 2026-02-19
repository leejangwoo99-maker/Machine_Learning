from pathlib import Path
from datetime import datetime
import time
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_batch

# ============================================
# 0. 기본 설정 (여기만 수정)
# ============================================
# NAS 사용할 땐 아래 주석 해제:
# BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")

MIDDLE_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision03"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

FCT_MAP = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

# DB_CONFIG = {
#     "host": "192.168.108.162",
#     "port": 5432,
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "!",
# }

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

# 요구사항 스키마/테이블 (History)
SCHEMA_HISTORY = "a1_fct_vision_testlog_txt_processing_history"
TABLE_HISTORY  = "fct_vision_testlog_txt_processing_history"

# 삭제 대상 스키마
SCHEMA_RESULT = "a1_fct_vision_testlog_txt_processing_result"
SCHEMA_DETAIL = "a1_fct_vision_testlog_txt_processing_result_detail"


# ============================================
# 1. DB 유틸
# ============================================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_db(conn):
    """
    (2)(3) 스키마 삭제
    (1) history 스키마/테이블 생성 (요구사항 컬럼 반영)
    """
    cur = conn.cursor()

    # (2) result 스키마 삭제
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_RESULT} CASCADE;")

    # (3) detail 스키마 삭제
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_DETAIL} CASCADE;")

    # (1) history 스키마/테이블 생성 (컬럼 순서 반영)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_HISTORY};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_HISTORY}.{TABLE_HISTORY} (
            id                  BIGSERIAL PRIMARY KEY,

            barcode_information TEXT,              -- BA1WJ... (파일명 파싱)
            station             TEXT,              -- equipment -> station
            end_day             TEXT,              -- date_folder -> end_day (yyyymmdd)
            end_time            TEXT,              -- hh:mi:ss
            remark              TEXT,              -- PD / Non-PD
            result              TEXT,              -- PASS / FAIL
            goodorbad           TEXT,              -- good_bad -> goodorbad
            filename            TEXT NOT NULL,
            file_path           TEXT NOT NULL,     -- full_path -> file_path
            processed_at        TIMESTAMPTZ NOT NULL
        );
        """
    )

    # 중복 체크용 인덱스
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_file_path ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(file_path);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_filename ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(filename);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_barcode ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(barcode_information);"
    )

    conn.commit()
    cur.close()


def load_processed_keys(conn):
    """
    history에 올라간 file_path / filename set 로드
    -> 둘 중 하나라도 겹치면 중복 스킵
    """
    cur = conn.cursor()
    cur.execute(f"SELECT file_path, filename FROM {SCHEMA_HISTORY}.{TABLE_HISTORY};")
    rows = cur.fetchall()
    cur.close()

    processed_paths = set()
    processed_names = set()
    for fp, fn in rows:
        if fp:
            processed_paths.add(fp)
        if fn:
            processed_names.add(fn)
    return processed_paths, processed_names


def insert_history_rows(conn, rows):
    if not rows:
        return 0

    cur = conn.cursor()
    execute_batch(
        cur,
        f"""
        INSERT INTO {SCHEMA_HISTORY}.{TABLE_HISTORY}
            (file_path, station, end_day, goodorbad, filename, processed_at,
             end_time, barcode_information, remark, result)
        VALUES (%s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
        """,
        [
            (
                r["file_path"],
                r["station"],
                r["end_day"],
                r["goodorbad"],
                r["filename"],
                r["processed_at"],
                r["end_time"],
                r["barcode_information"],
                r["remark"],
                r["result"],
            )
            for r in rows
        ],
        page_size=1000,
    )
    conn.commit()
    cur.close()
    return len(rows)


# ============================================
# 2. Vision 설비 분류
# ============================================
def classify_vision_equipment(file_path: Path):
    """
    파일 6번째 줄의 Test Program으로 Vision1/Vision2 결정
    """
    equipment = "Vision3"
    test_program = None
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
        if len(lines) >= 6:
            line6 = lines[5]
            if "Test Program" in line6:
                if "LED1" in line6:
                    equipment, test_program = "Vision1", "LED1"
                elif "LED2" in line6:
                    equipment, test_program = "Vision2", "LED2"
                else:
                    equipment = "Vision3"
            else:
                equipment = "Vision3"
    except Exception:
        equipment = "Vision3"
        test_program = None
    return equipment, test_program


# ============================================
# 3. full_path(=file_path)에서 end_time / barcode / remark / result 추출
# ============================================
def parse_fields_from_full_path(full_path_str: str):
    """
    요구사항:
    a) '_'과 '_' 사이는 yyyymmddhhmiss -> hh:mi:ss 로 end_time
    b) BA1WJ로 시작해서 '_' 전까지 barcode_information
    c) barcode_information 18번째가 J/S -> PD else Non-PD
    d) full_path 제일 뒤에서 5번째 글자:
       - 'P' -> PASS
       - 'F' -> FAIL
       (그 외/판정불가 -> None)
    """
    p = Path(full_path_str)
    filename = p.name
    stem = p.stem  # 확장자 제거

    barcode_information = None
    end_time = None
    remark = None
    result = None

    # (b) barcode_information: BA1WJ... 첫 '_' 전까지
    parts = stem.split("_")
    if parts and parts[0].startswith("BA1WJ"):
        barcode_information = parts[0]

    # (a) end_time: 첫 '_' ~ 두번째 '_' 사이(yyyymmddhhmiss)
    # 예) ..._20251001014641_FCT_P.txt  -> parts[1] == 20251001014641
    if len(parts) >= 2:
        ts = parts[1]
        if ts.isdigit() and len(ts) == 14:
            hh = ts[8:10]
            mi = ts[10:12]
            ss = ts[12:14]
            end_time = f"{hh}:{mi}:{ss}"

    # (c) remark: barcode 18번째(1-indexed) -> index 17
    if barcode_information and len(barcode_information) >= 18:
        c18 = barcode_information[17]
        remark = "PD" if c18 in ("J", "S") else "Non-PD"

    # (d) result: full_path 제일 뒤에서 5번째 글자
    # 예) ..._FCT_P.txt  -> 뒤에서 5번째가 'P'
    if len(full_path_str) >= 5:
        ch = full_path_str[-5]
        if ch == "P":
            result = "PASS"
        elif ch == "F":
            result = "FAIL"
        else:
            result = None

    return barcode_information, end_time, remark, result, filename


# ============================================
# 4. 한 파일 처리 (멀티프로세스)
# ============================================
def process_one_file(args):
    """
    args: (file_path_str, mid, folder_date, gb)
    return: history_row dict
    """
    file_path_str, mid, folder_date, gb = args
    p = Path(file_path_str)

    # station 분류
    if mid in FCT_MAP:
        station = FCT_MAP[mid]
    else:
        station, _ = classify_vision_equipment(p)

    barcode_information, end_time, remark, result, filename = parse_fields_from_full_path(
        file_path_str
    )

    history_row = {
        "file_path": file_path_str,     # full_path -> file_path
        "station": station,             # equipment -> station
        "end_day": folder_date,         # date_folder -> end_day
        "goodorbad": gb,                # good_bad -> goodorbad
        "filename": filename,
        "processed_at": datetime.now(),

        "end_time": end_time,
        "barcode_information": barcode_information,
        "remark": remark,
        "result": result,
    }
    return history_row


# ============================================
# 5. 한 번 실행(run_once): 파일 스캔 → 멀티프로세스 → DB 저장
# ============================================
def run_once():
    started_at = datetime.now()
    print("\n==================== run_once 시작 ====================")
    print(f"시각: {started_at}")

    conn = get_connection()
    try:
        init_db(conn)

        processed_paths, processed_names = load_processed_keys(conn)
        print(f"[이력] 이미 처리된 file_path 수 : {len(processed_paths)}")
        print(f"[이력] 이미 처리된 filename 수 : {len(processed_names)}")

        file_infos = []
        total_scanned = 0
        seen_paths_this_run = set()
        seen_names_this_run = set()

        for mid in MIDDLE_FOLDERS:
            mid_path = BASE_LOG_DIR / mid
            if not mid_path.exists():
                print(f"[SKIP] {mid_path} 없음")
                continue

            for date_folder in sorted(mid_path.iterdir()):
                if not date_folder.is_dir():
                    continue

                folder_date = date_folder.name  # yyyymmdd

                for gb in TARGET_FOLDERS:
                    gb_path = date_folder / gb
                    if not gb_path.exists():
                        continue

                    for f in gb_path.iterdir():
                        if not f.is_file():
                            continue

                        total_scanned += 1
                        file_path_str = str(f)
                        filename = f.name

                        # DB 중복 스킵
                        if (file_path_str in processed_paths) or (filename in processed_names):
                            continue

                        # 이번 run 내 중복 스킵
                        if (file_path_str in seen_paths_this_run) or (filename in seen_names_this_run):
                            continue

                        seen_paths_this_run.add(file_path_str)
                        seen_names_this_run.add(filename)
                        file_infos.append((file_path_str, mid, folder_date, gb))

        print(f"[스캔] 전체 스캔 파일 수: {total_scanned}")
        print(f"[스캔] 이번 실행에서 새로 처리할 파일 수: {len(file_infos)}")

        if not file_infos:
            print("[정보] 새로 처리할 파일이 없습니다.")
            return

        cpu_cnt = max(1, mp.cpu_count() - 1)
        print(f"[멀티프로세스] 사용 프로세스 수: {cpu_cnt}")

        with mp.Pool(processes=cpu_cnt) as pool:
            history_rows = pool.map(process_one_file, file_infos)

        n_hist = insert_history_rows(conn, history_rows)
        print(f"[DB] history 저장 건수 : {n_hist}")

    finally:
        conn.close()
        print("==================== run_once 종료 ====================")


# ============================================
# 6. 메인 루프: 1초마다 무한 반복
# ============================================
if __name__ == "__main__":
    mp.freeze_support()
    try:
        while True:
            run_once()
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다. 프로그램을 종료합니다.")
