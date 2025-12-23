# -*- coding: utf-8 -*-
from pathlib import Path
from datetime import datetime
import time
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_batch

# ============================================
# 0. 공장 전용 설정 (고정)
# ============================================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")

MIDDLE_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision03"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

FCT_MAP = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_HISTORY = "a1_fct_vision_testlog_txt_processing_history"
TABLE_HISTORY  = "fct_vision_testlog_txt_processing_history"

SCHEMA_RESULT = "a1_fct_vision_testlog_txt_processing_result"
SCHEMA_DETAIL = "a1_fct_vision_testlog_txt_processing_result_detail"

REALTIME_WINDOW_SEC = 120
MP_PROCESSES = 2


# ============================================
# 1. DB 유틸
# ============================================
def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_db(conn):
    """
    (2)(3) 스키마 삭제
    (1) history 스키마/테이블 생성
    (추가) file_path 단독 중복 방지용 UNIQUE 인덱스 + end_day 조회 인덱스
    """
    cur = conn.cursor()

    # (2) result 스키마 삭제
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_RESULT} CASCADE;")

    # (3) detail 스키마 삭제
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_DETAIL} CASCADE;")

    # (1) history 스키마/테이블 생성
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_HISTORY};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_HISTORY}.{TABLE_HISTORY} (
            id                  BIGSERIAL PRIMARY KEY,

            barcode_information TEXT,
            station             TEXT,
            end_day             TEXT,
            end_time            TEXT,
            remark              TEXT,
            result              TEXT,
            goodorbad           TEXT,
            filename            TEXT NOT NULL,
            file_path           TEXT NOT NULL,
            processed_at        TIMESTAMPTZ NOT NULL
        );
        """
    )

    # (핵심) file_path 단독 중복 방지: UNIQUE 인덱스(권장)
    cur.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_{TABLE_HISTORY}_file_path
        ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(file_path);
        """
    )

    # (성능) end_day 필터 조회용 인덱스
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_end_day
        ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(end_day);
        """
    )

    # (옵션) 추가 조회 인덱스(기존 유지 가능)
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_barcode ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(barcode_information);"
    )

    conn.commit()
    cur.close()


def load_processed_paths_today(conn, end_day_today: str):
    """
    (변경) 오늘(end_day=오늘) 처리 이력의 file_path만 set 로드
    -> 메모리 사용량 대폭 감소
    """
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT file_path
        FROM {SCHEMA_HISTORY}.{TABLE_HISTORY}
        WHERE end_day = %s;
        """,
        (end_day_today,)
    )
    rows = cur.fetchall()
    cur.close()
    return {fp for (fp,) in rows if fp}


def insert_history_rows(conn, rows):
    """
    (변경) UNIQUE(file_path) 기준으로 중복 방지
    - ON CONFLICT DO NOTHING 적용
    """
    if not rows:
        return 0

    cur = conn.cursor()
    execute_batch(
        cur,
        f"""
        INSERT INTO {SCHEMA_HISTORY}.{TABLE_HISTORY}
            (barcode_information, station, end_day, end_time, remark, result,
             goodorbad, filename, file_path, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s)
        ON CONFLICT (file_path) DO NOTHING
        """,
        [
            (
                r["barcode_information"],
                r["station"],
                r["end_day"],
                r["end_time"],
                r["remark"],
                r["result"],
                r["goodorbad"],
                r["filename"],
                r["file_path"],
                r["processed_at"],
            )
            for r in rows
        ],
        page_size=1000,
    )
    # rowcount는 execute_batch에서 신뢰도가 떨어질 수 있어, 커밋 후 len(rows) 그대로 반환하지 않습니다.
    # 여기서는 삽입 시도 건수 반환(로그용)으로 둡니다.
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
    stem = p.stem

    barcode_information = None
    end_time = None
    remark = None
    result = None

    parts = stem.split("_")

    # (b)
    if parts and parts[0].startswith("BA1WJ"):
        barcode_information = parts[0]

    # (a)
    if len(parts) >= 2:
        ts = parts[1]
        if ts.isdigit() and len(ts) == 14:
            hh = ts[8:10]
            mi = ts[10:12]
            ss = ts[12:14]
            end_time = f"{hh}:{mi}:{ss}"

    # (c)
    if barcode_information and len(barcode_information) >= 18:
        c18 = barcode_information[17]
        remark = "PD" if c18 in ("J", "S") else "Non-PD"

    # (d)
    if len(full_path_str) >= 5:
        ch = full_path_str[-5]
        if ch == "P":
            result = "PASS"
        elif ch == "F":
            result = "FAIL"

    return barcode_information, end_time, remark, result, filename


# ============================================
# 4. 한 파일 처리 (멀티프로세스)
# ============================================
def process_one_file(args):
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

    return {
        "barcode_information": barcode_information,
        "station": station,
        "end_day": folder_date,
        "end_time": end_time,
        "remark": remark,
        "result": result,
        "goodorbad": gb,
        "filename": filename,
        "file_path": file_path_str,
        "processed_at": datetime.now(),
    }


# ============================================
# 5. 한 번 실행(run_once): 오늘 폴더 + 120초 이내 파일만 처리
# ============================================
def run_once():
    started_at = datetime.now()
    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_WINDOW_SEC

    today_yyyymmdd = datetime.now().strftime("%Y%m%d")  # 오늘 날짜(end_day)

    print("\n==================== run_once 시작 ====================")
    print(f"시각: {started_at} / end_day(today)={today_yyyymmdd} / window={REALTIME_WINDOW_SEC}s")

    conn = get_connection()
    try:
        init_db(conn)

        # (변경) 오늘 end_day 기준 file_path만 로드
        processed_paths = load_processed_paths_today(conn, today_yyyymmdd)
        print(f"[이력] 오늘(end_day={today_yyyymmdd}) 처리된 file_path 수 : {len(processed_paths)}")

        file_infos = []
        total_scanned = 0

        # (변경) 이번 run 내 중복도 file_path만
        seen_paths_this_run = set()

        for mid in MIDDLE_FOLDERS:
            mid_path = BASE_LOG_DIR / mid
            if not mid_path.exists():
                print(f"[SKIP] {mid_path} 없음")
                continue

            # 오늘 날짜 폴더만
            date_folder = mid_path / today_yyyymmdd
            if not date_folder.exists() or (not date_folder.is_dir()):
                continue

            folder_date = date_folder.name  # == today_yyyymmdd

            for gb in TARGET_FOLDERS:
                gb_path = date_folder / gb
                if not gb_path.exists():
                    continue

                for f in gb_path.iterdir():
                    if not f.is_file():
                        continue

                    total_scanned += 1

                    # 120초 이내 수정된 파일만 처리
                    try:
                        mtime = f.stat().st_mtime
                    except Exception:
                        continue

                    if mtime < cutoff_ts:
                        continue

                    file_path_str = str(f)

                    # DB 중복 스킵 (file_path 단독)
                    if file_path_str in processed_paths:
                        continue

                    # 이번 run 내 중복 스킵 (file_path 단독)
                    if file_path_str in seen_paths_this_run:
                        continue

                    seen_paths_this_run.add(file_path_str)
                    file_infos.append((file_path_str, mid, folder_date, gb))

        print(f"[스캔] 전체 스캔 파일 수(오늘 폴더 내): {total_scanned}")
        print(f"[스캔] 이번 실행에서 새로 처리할 파일 수(120초 이내): {len(file_infos)}")

        if not file_infos:
            print("[정보] 새로 처리할 파일이 없습니다.")
            return

        print(f"[멀티프로세스] 사용 프로세스 수: {MP_PROCESSES}")

        with mp.Pool(processes=MP_PROCESSES) as pool:
            history_rows = pool.map(process_one_file, file_infos)

        n_try = insert_history_rows(conn, history_rows)
        print(f"[DB] history INSERT 시도 건수 : {n_try} (중복은 DB에서 자동 무시)")

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
