import os
import json
from pathlib import Path
from datetime import datetime

import psycopg2
from psycopg2.extras import execute_batch, Json

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==========================
# 0. 파싱 모드 / 스키마 설정
# ==========================

# 나중에 "VISION", "OTHER" 등으로 확장할 예정
LOG_TYPE = "FCT"  # 현재는 FCT만 사용

if LOG_TYPE == "FCT":
    SCHEMA_NAME = "fct"
    TABLE_NAME = "fct_results"
elif LOG_TYPE == "VISION":
    # TODO: Vision 로그를 fct와 별도 스키마/테이블로 저장할 때 사용
    SCHEMA_NAME = "vision"
    TABLE_NAME = "vision_results"
else:
    raise ValueError(f"지원하지 않는 LOG_TYPE: {LOG_TYPE}")

# ==========================
# 1. 기본 설정
# ==========================

# (1) 기본 경로
BASE_DIR = Path(r"C:\Users\user\Desktop\1")

# (2) TC 폴더 → FCT 매핑 (최종)
TC_TO_FCT = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

# (3) DB 연결 정보
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

# ==========================
# 2. DB 연결 & 테이블 생성
# ==========================

def find_all_json_files():
    """
    TC6~TC9 / YYYYMMDD / GoodFile, BadFile 안의 모든 JSON 파일 경로 수집
    """
    file_infos = []  # (tc_folder_name, file_type, json_path)

    for tc_folder_name in TC_TO_FCT.keys():
        station_folder_path = BASE_DIR / tc_folder_name
        if not station_folder_path.is_dir():
            continue

        for date_dir in station_folder_path.iterdir():
            if not date_dir.is_dir():
                continue
            if not (date_dir.name.isdigit() and len(date_dir.name) == 8):
                continue  # YYYYMMDD 아님

            for file_type in ["GoodFile", "BadFile"]:
                type_dir = date_dir / file_type
                if not type_dir.is_dir():
                    continue

                for json_path in type_dir.glob("*.json"):
                    file_infos.append((tc_folder_name, file_type, json_path))

    return file_infos

# 중복 파일명 확인
def load_existing_filenames():
    """
    이미 DB에 올라가 있는 file_name 목록을 set 으로 반환
    """
    conn = get_connection()
    cur = conn.cursor()
    # ★ 스키마/테이블 이름을 변수로 사용
    cur.execute(f"SELECT file_name FROM {SCHEMA_NAME}.{TABLE_NAME};")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    return { (r[0].strip() if r[0] is not None else None) for r in rows if r[0] is not None }

def process_one_file(tc_folder_name, file_type, json_path):
    # """
    # JSON 파일 하나를 읽어서 record dict로 변환
    # 병렬 처리용 워커 함수
    # """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)  # ★ 반드시 필요
    except Exception as e:
        print(f"\n[WARN] JSON 로드 실패: {json_path} - {e}")
        return None

    # JSON에서 기본 정보 추출
    source_path = str(data.get("source_path", "")) or str(json_path)
    file_name   = data.get("file_name", json_path.name)

    station_folder = tc_folder_name
    fct_station    = TC_TO_FCT.get(tc_folder_name)

    result = data.get("result", "")
    result = result.strip().upper() if isinstance(result, str) else None

    barcode = data.get("Barcode  information", "")

    # 사람 기준 18번째 문자만 사용
    pn_18th_char = classify_barcode(barcode)

    end_time_str = data.get("end_time", "")
    end_time_dt, test_date = parse_end_time(end_time_str)

    record = {
        "source_path": source_path,
        "file_name": file_name,
        "station_folder": station_folder,
        "fct_station": fct_station,
        "file_type": file_type,
        "result": result,
        "barcode": barcode,
        "pn_18th_char": pn_18th_char,
        "end_time": end_time_dt,
        "test_date": test_date,
        "json_data": data,
    }
    return record

def get_connection():
    print("=== get_connection() DB_CONFIG repr ===")
    for k, v in DB_CONFIG.items():
        print(f"{k} ->", repr(v))

    conn = psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )
    conn.autocommit = True
    return conn

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    id              SERIAL PRIMARY KEY,
    source_path     TEXT,
    file_name       TEXT,
    station_folder  TEXT,
    fct_station     TEXT,
    file_type       TEXT,
    result          TEXT,
    barcode         TEXT,
    pn_18th_char    TEXT,
    end_time        TIMESTAMP,
    test_date       DATE,
    json_data       JSONB
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_fct_date
    ON {SCHEMA_NAME}.{TABLE_NAME} (fct_station, test_date, end_time DESC);
"""

def init_db():
    print("1) DB 초기화(테이블 생성)…")
    conn = get_connection()
    cur = conn.cursor()

    # 1) 현재 LOG_TYPE에 맞는 스키마 생성 (예: fct)
    cur.execute(
        f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME} AUTHORIZATION {DB_CONFIG['user']};"
    )

    # 2) 테이블 / 인덱스 생성 (스키마 명은 CREATE_TABLE_SQL 안에 포함됨)
    cur.execute(CREATE_TABLE_SQL)

    cur.close()
    conn.close()
    print("  → 테이블/인덱스 생성 완료")

# ==========================
# 3. 유틸 함수
# ==========================

def parse_end_time(end_time_str: str):
    if not end_time_str:
        return None, None
    norm = " ".join(end_time_str.split())
    try:
        dt = datetime.strptime(norm, "%Y/%m/%d %H:%M:%S")
        return dt, dt.date()
    except ValueError:
        print(f"[WARN] end_time 파싱 실패: {end_time_str!r}")
        return None, None

def classify_barcode(barcode: str):
    if not barcode:
        return None
    barcode = barcode.strip()

    # 사람 기준 18번째 문자 → 인덱스 17
    if len(barcode) < 18:
        return None

    ch = barcode[17]
    return ch

# ==========================
# 4. JSON 파일 스캔 & 파싱
# ==========================

def collect_records():
    print("2) JSON 파일 스캔 및 파싱…")
    print(f"  → BASE_DIR: {BASE_DIR}")
    print(f"  → BASE_DIR exists? {BASE_DIR.exists()}")

    # 1) 전체 JSON 파일 목록 수집
    file_infos = find_all_json_files()
    total_files = len(file_infos)
    print(f"  → 전체 JSON 파일 수: {total_files}")

    if total_files == 0:
        print("  → JSON 파일이 없습니다.")
        return []

    # 2) DB에 이미 존재하는 file_name 목록 로드
    existing_names = load_existing_filenames()
    print(f"  → DB에 이미 존재하는 file_name 수: {len(existing_names)}")

    # 3) 중복 file_name 스킵하고 새 파일만 남기기
    filtered_file_infos = []
    skipped = 0

    for (tc_folder_name, file_type, json_path) in file_infos:
        try:
            # ★ JSON을 열어서 내부의 file_name 기준으로 비교
            with open(json_path, "r", encoding="utf-8") as f:
                js = json.load(f)

            # process_one_file 과 동일한 규칙 사용:
            # data.get("file_name", json_path.name)
            file_name = (js.get("file_name", json_path.name) or "").strip()
        except Exception as e:
            print(f"\n[WARN] JSON 로드/파일명 추출 실패: {json_path} - {e}")
            continue

        if not file_name:
            # file_name 못 읽으면 그냥 새 파일로 취급(필요하면 continue로 바꿀 수 있음)
            filtered_file_infos.append((tc_folder_name, file_type, json_path))
            continue

        # ★ DB에 이미 존재하는 file_name 이면 스킵
        if file_name in existing_names:
            skipped += 1
            continue

        filtered_file_infos.append((tc_folder_name, file_type, json_path))

    file_infos = filtered_file_infos
    total_files = len(file_infos)

    print(f"  → 중복 file_name으로 스킵된 파일 수: {skipped}")
    print(f"  → 새로 파싱할 JSON 파일 수: {total_files}")

    if total_files == 0:
        print("  → 새로 파싱할 JSON 파일이 없습니다.")
        return []

    # 아래부터는 기존 코드 그대로 (TC별/Good/Bad 집계 + 멀티스레드 파싱)
    total_per_tc = {tc: 0 for tc in TC_TO_FCT.keys()}
    total_per_type = {"GoodFile": 0, "BadFile": 0}

    for tc_folder_name, file_type, json_path in file_infos:
        total_per_tc[tc_folder_name] = total_per_tc.get(tc_folder_name, 0) + 1
        total_per_type[file_type] = total_per_type.get(file_type, 0) + 1

    processed = 0
    processed_per_tc = {tc: 0 for tc in TC_TO_FCT.keys()}
    processed_per_type = {"GoodFile": 0, "BadFile": 0}

    start_time = time.time()
    records = []

    max_workers = os.cpu_count() or 4

    def format_eta(processed, total, start_time):
        elapsed = time.time() - start_time
        if processed == 0 or elapsed < 1e-6:
            return "--:--"
        rate = processed / elapsed
        remaining = total - processed
        eta_sec = remaining / rate
        m, s = divmod(int(eta_sec), 60)
        if m > 99:
            return ">99m"
        return f"{m:02d}:{s:02d}"

    def format_bar(percent, length=30):
        filled = int(percent / 100 * length)
        if filled > length:
            filled = length
        return "█" * filled + "-" * (length - filled)

    print(f"  → 멀티스레드 파싱 시작 (workers={max_workers})")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_info = {
            executor.submit(process_one_file, tc_folder_name, file_type, json_path):
            (tc_folder_name, file_type, json_path)
            for (tc_folder_name, file_type, json_path) in file_infos
        }

        for future in as_completed(future_to_info):
            tc_folder_name, file_type, json_path = future_to_info[future]
            record = future.result()
            if record is None:
                continue

            records.append(record)

            processed += 1
            processed_per_tc[tc_folder_name] = processed_per_tc.get(tc_folder_name, 0) + 1
            processed_per_type[file_type] = processed_per_type.get(file_type, 0) + 1

            total_percent = processed / total_files * 100

            tc_percents = {
                tc: (processed_per_tc[tc] / total_per_tc[tc] * 100) if total_per_tc[tc] > 0 else 0.0
                for tc in TC_TO_FCT.keys()
            }

            good_percent = processed_per_type["GoodFile"] / total_per_type["GoodFile"] * 100 if total_per_type["GoodFile"] > 0 else 0.0
            bad_percent  = processed_per_type["BadFile"]  / total_per_type["BadFile"]  * 100 if total_per_type["BadFile"]  > 0 else 0.0

            bar = format_bar(total_percent, length=30)
            eta = format_eta(processed, total_files, start_time)

            line = (
                f"\r  → Total {total_percent:6.2f}% [{bar}] ETA {eta} "
                f"| TC6 {tc_percents['TC6']:5.1f}% TC7 {tc_percents['TC7']:5.1f}% "
                f"TC8 {tc_percents['TC8']:5.1f}% TC9 {tc_percents['TC9']:5.1f}% "
                f"| Good {good_percent:5.1f}% Bad {bad_percent:5.1f}%"
            )
            print(line, end="", flush=True)

    print("\n  → 파싱 완료")
    print(f"  → 총 파싱된 레코드 수: {len(records)}")

    return records

# ==========================
# 5. DB INSERT
# ==========================

INSERT_SQL = f"""
INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
    source_path,
    file_name,
    station_folder,
    fct_station,
    file_type,
    result,
    barcode,
    pn_18th_char,
    end_time,
    test_date,
    json_data
) VALUES (
    %(source_path)s,
    %(file_name)s,
    %(station_folder)s,
    %(fct_station)s,
    %(file_type)s,
    %(result)s,
    %(barcode)s,
    %(pn_18th_char)s,
    %(end_time)s,
    %(test_date)s,
    %(json_data)s
)
ON CONFLICT (file_name) DO NOTHING;
"""

def insert_records(records):
    print("3) RDBMS 삽입…")
    if not records:
        print("  → 삽입할 레코드가 없습니다.")
        return

    conn = get_connection()
    cur = conn.cursor()

    for r in records:
        r["json_data"] = Json(r["json_data"])

    try:
        execute_batch(cur, INSERT_SQL, records, page_size=500)
        conn.commit()
        print(f"  → {len(records)} 건 삽입 완료.")
    except Exception as e:
        conn.rollback()
        print("[ERROR] 레코드 삽입 중 오류:", e)
    finally:
        cur.close()
        conn.close()

# ==========================
# 6. 메인 실행
# ==========================

def main():
    init_db()
    records = collect_records()
    insert_records(records)
    print("완료.")

if __name__ == "__main__":
    main()
