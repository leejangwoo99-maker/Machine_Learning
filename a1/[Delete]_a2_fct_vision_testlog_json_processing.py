import re
import json
import time
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed

import pandas as pd
import psycopg2
import psycopg2.extras

# ==============================
# 설정 영역
# ==============================

# 기본 로그 경로 설정
# BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")  # 예시: NAS 경로
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")  # 로컬일 때 예시

MIDDLE_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision03"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]  # yyyymmdd\GoodFile / yyyymmdd\BadFile

TC_TO_FCT = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

# PostgreSQL 접속 정보
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

# NAS로 바꿀 때는 이거 쓰면 됨
# DB_CONFIG = {
#     "host": "192.168.108.162",
#     "port": 5432,
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "leejangwoo1!",
# }

SCHEMA_NAME = "a2_fct_vision_testlog_json_processing"
TABLE_NAME = "fct_vision_testlog_json_processing"

# ==============================
# 공통 유틸 함수
# ==============================

def read_text_file(path: Path):
    """텍스트 파일을 읽어 줄 단위 리스트로 반환 (인코딩 자동 처리 시도)."""
    for enc in ("cp949", "utf-8-sig", "utf-8"):
        try:
            with path.open("r", encoding=enc, errors="replace") as f:
                return [line.rstrip("\n\r") for line in f]
        except UnicodeDecodeError:
            continue
    with path.open("rb") as f:
        return f.read().decode("latin1", errors="replace").splitlines()

def parse_colon_line(line: str):
    """
    'Key       :Value' 형태를 'Key'(좌측), 'Value'(우측)로 분리해서 반환.
    좌우 공백은 strip.
    """
    if ":" not in line:
        return line.strip(), ""
    left, right = line.split(":", 1)
    return left.strip(), right.strip()

def parse_end_time_fct(line: str):
    """
    FCT용 End Time 파싱
    예) 'End Time                :2025/10/01  01:46:41'
    -> End day: '20251001', End time: '01:46:41'
    """
    _, value = parse_colon_line(line)
    m = re.search(r"(?P<date>\d{4}/\d{2}/\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2})", value)
    if not m:
        return "", ""
    day_raw = m.group("date")       # '2025/10/01'
    time_raw = m.group("time")      # '01:46:41'
    day = day_raw.replace("/", "")  # '20251001'
    return day, time_raw

def parse_end_time_vision(line: str):
    """
    Vision용 End Time 파싱
    예) 'End Time                : 2025-10-01 04:30:55'
    -> End day: '20251001', End time: '04:30:55'
    """
    _, value = parse_colon_line(line)
    m = re.search(r"(?P<date>\d{4}-\d{2}-\d{2})\s+(?P<time>\d{2}:\d{2}:\d{2})", value)
    if not m:
        return "", ""
    day_raw = m.group("date")       # '2025-10-01'
    time_raw = m.group("time")      # '04:30:55'
    day = day_raw.replace("-", "")  # '20251001'
    return day, time_raw

step_pattern = re.compile(
    r"""^
    (?P<step_no>\d+\.\d+)\s+
    (?P<desc>.+?)\s*,\s*
    (?P<value>[^,]+)\s*,\s*
    (?P<min>[^,]+)\s*,\s*
    (?P<max>[^,]+)\s*,\s*
    \[(?P<result>[^\]]+)\]
    """,
    re.VERBOSE,
)

def parse_steps(lines, start_idx: int = 18):
    """
    19번째 줄(인덱스 18)부터 끝까지 step 파싱.
    예: '1.01 Test Input Voltage(V) , 14.67, 14.60, 14.80, [PASS]'
    """
    steps = []
    for line in lines[start_idx:]:
        line = line.strip()
        if not line:
            continue
        m = step_pattern.match(line)
        if not m:
            continue
        step_no = m.group("step_no").strip()
        desc = m.group("desc").strip()
        value = m.group("value").strip()
        min_v = m.group("min").strip()
        max_v = m.group("max").strip()
        result = m.group("result").strip()

        step_dict = {
            step_no: desc,          # "1.01": "Test Input Voltage(V)"
            "value": value,
            "min": min_v,
            "max": max_v,
            "step result": result,
        }
        steps.append(step_dict)
    return steps

def classify_equipment(middle_folder: str, lines):
    """
    TC6~9 -> FCT1~4 매핑
    Vision03 -> 6번째 줄 Test Program 기준 Vision1/2
    """
    if middle_folder in TC_TO_FCT:
        return TC_TO_FCT[middle_folder]

    # ★ 여기서 폴더명 Vision03 기준으로 분기 ★
    if middle_folder == "Vision03":
        # 6번째 줄 (index 5)에 Test Program이 있다고 가정
        if len(lines) >= 6:
            key, value = parse_colon_line(lines[5])
            if "LED1" in value:
                return "Vision1"
            if "LED2" in value:
                return "Vision2"
        return "Vision_Unknown"

    return "Unknown"

def parse_one_log_file(path: Path, middle_folder: str):
    """
    한 개 로그 파일을 파싱해서 JSON용 딕셔너리 + 요약 레코드 반환.
    JSON 파일로 저장하지 않고, PostgreSQL INSERT용 데이터만 만든다.
    """
    lines = read_text_file(path)

    if len(lines) < 19:
        return None  # 너무 짧으면 스킵

    equip_group = classify_equipment(middle_folder, lines)

    # Station / End Time 분기
    if equip_group.startswith("FCT"):
        station_key, station_val = parse_colon_line(lines[2])
        end_day, end_time = parse_end_time_fct(lines[8])
    elif equip_group.startswith("Vision"):
        # Vision1~2: 3번째 줄 Station 무시, Station은 Vision1/Vision2로 저장
        station_val = equip_group
        end_day, end_time = parse_end_time_vision(lines[8])
    else:
        station_key, station_val = parse_colon_line(lines[2])
        end_day, end_time = parse_end_time_fct(lines[8])

    # Barcode (5번째 줄)
    barcode_key, barcode_val = parse_colon_line(lines[4])

    # Result (13번째 줄)
    result_key, result_val = parse_colon_line(lines[12])

    # Run Time (14번째 줄)
    runtime_key, runtime_val = parse_colon_line(lines[13])

    # step 파싱 (19번째 줄부터)
    steps = parse_steps(lines, start_idx=18)

    # JSON 구조 (기존 생성했던 컬럼들을 key로 사용)
    json_data = {
        "End day": end_day,
        "End time": end_time,
        "Station": station_val,
        "Barcode information": barcode_val,
        "Result": result_val,
        "Run Time": runtime_val,
        "equipment_group": equip_group,
        "equipment_raw": middle_folder,
        "file_path": str(path),
        "steps": steps,
    }

    # 요약 레코드 (DB의 개별 컬럼용)
    record = {
        "equipment_group": equip_group,
        "equipment_raw": middle_folder,
        "file_path": str(path),
        "End day": end_day,
        "End time": end_time,
        "Station": station_val,
        "Barcode information": barcode_val,
        "Result": result_val,
        "Run Time": runtime_val,
    }

    return json_data, record

# ==============================
# DB 관련 함수
# ==============================

def get_connection():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn

def ensure_schema_and_table(conn):
    with conn.cursor() as cur:
        # 스키마 생성
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

        # 테이블 생성
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
            id BIGSERIAL PRIMARY KEY,
            file_path TEXT UNIQUE,
            equipment_group TEXT,
            equipment_raw TEXT,
            end_day TEXT,
            end_time TEXT,
            station TEXT,
            barcode_information TEXT,
            result TEXT,
            run_time TEXT,
            payload JSONB,
            created_at TIMESTAMPTZ DEFAULT NOW()
        );
        """)
    conn.commit()

def load_existing_file_paths(conn):
    """
    기존에 DB에 이미 INSERT된 file_path 목록을 set으로 불러온다.
    """
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT file_path FROM {SCHEMA_NAME}.{TABLE_NAME};"
        )
        rows = cur.fetchall()
    return set(r[0] for r in rows)

def insert_records(conn, json_objects, records):
    """
    파싱 결과를 PostgreSQL에 INSERT.
    JSON 전체는 payload(jsonb)에, 주요 필드는 컬럼으로 분리 저장.
    """
    if not json_objects or not records:
        return

    rows = []
    for json_obj, rec in zip(json_objects, records):
        rows.append((
            rec["file_path"],
            rec["equipment_group"],
            rec["equipment_raw"],
            rec["End day"],
            rec["End time"],
            rec["Station"],
            rec["Barcode information"],
            rec["Result"],
            rec["Run Time"],
            psycopg2.extras.Json(json_obj),
        ))

    sql = f"""
    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
    (
        file_path,
        equipment_group,
        equipment_raw,
        end_day,
        end_time,
        station,
        barcode_information,
        result,
        run_time,
        payload
    )
    VALUES %s
    ON CONFLICT (file_path) DO NOTHING;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur, sql, rows, template=None, page_size=500
        )
    conn.commit()

# ==============================
# 멀티프로세싱용 래퍼
# ==============================

def parse_one_wrapper(args):
    """
    멀티프로세싱용 래퍼.
    args: (path_str, middle_folder)
    """
    path_str, middle_folder = args
    path = Path(path_str)
    try:
        return parse_one_log_file(path, middle_folder)
    except Exception:
        # 필요하면 에러 로깅
        # print(f"[ERROR] {path_str}: {e}")
        return None

# ==============================
# 한 번의 스캔/파싱/DB 업로드 사이클
# ==============================

def run_one_cycle():
    # DB 연결
    conn = get_connection()
    try:
        ensure_schema_and_table(conn)
        existing_paths = load_existing_file_paths(conn)
        print(f"[INFO] DB에 이미 등록된 file_path 수: {len(existing_paths)}")

        # 1) 대상 TXT 파일 전체 수집 (기존 DB에 있는 file_path는 스킵)
        targets = []
        skipped = 0

        for mid in MIDDLE_FOLDERS:
            mid_dir = BASE_LOG_DIR / mid
            if not mid_dir.exists():
                # print(f"[SKIP] {mid_dir} 존재하지 않음")
                continue

            for date_dir in mid_dir.iterdir():
                if not date_dir.is_dir():
                    continue

                for sub in TARGET_FOLDERS:
                    target_dir = date_dir / sub
                    if not target_dir.exists():
                        continue

                    for txt_path in target_dir.glob("*.txt"):
                        path_str = str(txt_path)
                        if path_str in existing_paths:
                            skipped += 1
                            continue
                        targets.append((path_str, mid))

        print(f"[CYCLE] 새로 처리할 대상 TXT 수: {len(targets)}, DB기반 스킵 수: {skipped}")

        if not targets:
            conn.commit()
            conn.close()
            return

        all_json_objects = []
        records = []

        # 2) 멀티프로세스로 파싱
        with ProcessPoolExecutor() as executor:
            futures = [executor.submit(parse_one_wrapper, t) for t in targets]

            for i, f in enumerate(as_completed(futures), start=1):
                result = f.result()
                if result is None:
                    continue
                json_obj, rec = result
                all_json_objects.append(json_obj)
                records.append(rec)

                if i % 1000 == 0:
                    print(f"  → 현재 {i}/{len(targets)} 파일 파싱 완료")

        print(f"[CYCLE] 실제 파싱 성공 파일 수: {len(records)}")

        # 3) DB INSERT
        insert_records(conn, all_json_objects, records)
        print(f"[CYCLE] DB INSERT 완료")

    finally:
        try:
            conn.close()
        except Exception:
            pass

# ==============================
# 메인: 1초마다 무한 반복
# ==============================

def main():
    print("[START] a2_fct_vision_testlog_json_processing - 무한 루프 시작")
    while True:
        try:
            run_one_cycle()
        except Exception as e:
            print(f"[ERROR] run_one_cycle 예외 발생: {e}")
        time.sleep(1)  # 1초마다 재실행

if __name__ == "__main__":
    main()
