import re
import time
import calendar
from pathlib import Path
from datetime import datetime, date
from concurrent.futures import ProcessPoolExecutor, as_completed

import psycopg2
import psycopg2.extras

# ==============================
# 설정 영역
# ==============================

# 기본 로그 경로 설정 (HistoryLog NAS)
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")  # Z: 드라이브 원본 경로
# BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")  # 로컬 테스트용

# 중간 폴더
MIDDLE_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision03"]

# 날짜 폴더 아래에서 우선 탐색할 서브폴더 (없으면 날짜 폴더 바로 밑 파일도 스캔)
TARGET_FOLDERS = ["GoodFile", "BadFile"]

# TC6~9 → FCT1~4 매핑
TC_TO_FCT = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

# PostgreSQL 접속 정보 (NAS DB)
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SCHEMA_NAME = "a2_fct_vision_testlog_json_processing"
TABLE_NAME = "fct_vision_testlog_json_processing"

# 고정 최소 시작일 (이 날짜 이전 폴더는 무조건 제외)
FIXED_START_DATE = date(2025, 10, 1)

# 한 번에 파싱 + INSERT 할 최대 파일 수 (메모리 최적화용)
BATCH_SIZE = 10000

# 최근 N초 이내에 수정된 파일만 "실시간 대상"으로 처리
REALTIME_LOOKBACK_SECONDS = 120  # 예: 최근 120초(2분) 안에 변경된 파일만 파싱

# ==============================
# 날짜 윈도우 계산
# ==============================

def six_months_ago(d: date) -> date:
    """
    오늘 기준 6개월 전 날짜 계산 (relativedelta 없이 직접 구현).
    (현재 로직에서는 사용하지 않지만, 필요시 참고용으로 남겨둠.)
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
    오늘 기준으로 '이번 달 1일 ~ 오늘' 범위를 반환.
    예)
      - today = 2025-12-04 → 2025-12-01 ~ 2025-12-04
      - today = 2025-12-31 → 2025-12-01 ~ 2025-12-31
      - today = 2026-01-01 → 2026-01-01 ~ 2026-01-01
    """
    today = date.today()

    # 이번 달 1일
    month_start = today.replace(day=1)

    # 최소 시작일(FIXED_START_DATE) 적용
    window_start_date = max(month_start, FIXED_START_DATE)

    # 윈도우 끝 = 오늘
    window_end_date = today

    return window_start_date, window_end_date


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
    """'Key       :Value' 형태를 'Key', 'Value'로 분리."""
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
    day_raw = m.group("date")
    time_raw = m.group("time")
    day = day_raw.replace("/", "")
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
    day_raw = m.group("date")
    time_raw = m.group("time")
    day = day_raw.replace("-", "")
    return day, time_raw


# step 라인 파싱용
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
    """19번째 줄(인덱스 18)부터 끝까지 step 파싱."""
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
            step_no: desc,
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

    if middle_folder == "Vision03":
        if len(lines) >= 6:
            _, value = parse_colon_line(lines[5])
            if "LED1" in value:
                return "Vision1"
            if "LED2" in value:
                return "Vision2"
        return "Vision_Unknown"

    return "Unknown"


def parse_one_log_file(path: Path, middle_folder: str):
    """한 개 로그 파일 파싱 → (json_data, record) 리턴."""
    lines = read_text_file(path)

    if len(lines) < 19:
        return None

    equip_group = classify_equipment(middle_folder, lines)

    if equip_group.startswith("FCT"):
        _, station_val = parse_colon_line(lines[2])
        end_day, end_time = parse_end_time_fct(lines[8])
    elif equip_group.startswith("Vision"):
        station_val = equip_group  # Vision1/2 표기
        end_day, end_time = parse_end_time_vision(lines[8])
    else:
        _, station_val = parse_colon_line(lines[2])
        end_day, end_time = parse_end_time_fct(lines[8])

    _, barcode_val = parse_colon_line(lines[4])
    _, result_val = parse_colon_line(lines[12])
    _, runtime_val = parse_colon_line(lines[13])

    steps = parse_steps(lines, start_idx=18)

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
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")
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


def insert_records(conn, json_objects, records):
    if not json_objects or not records:
        return 0

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
        psycopg2.extras.execute_values(cur, sql, rows, template=None, page_size=500)
    conn.commit()
    return len(rows)

# ==============================
# 멀티프로세싱용 래퍼
# ==============================

def parse_one_wrapper(args):
    path_str, middle_folder = args
    path = Path(path_str)
    try:
        return parse_one_log_file(path, middle_folder)
    except Exception:
        return None

# ==============================
# 배치 처리 (메모리 최적화)
# ==============================

def process_batch(executor, batch_targets, conn, batch_index):
    if not batch_targets:
        return 0

    print(f"[배치] #{batch_index} - {len(batch_targets)}개 파일 파싱 시작")
    futures = [executor.submit(parse_one_wrapper, t) for t in batch_targets]

    json_list = []
    record_list = []

    for i, f in enumerate(as_completed(futures), start=1):
        result = f.result()
        if result is None:
            continue
        json_obj, rec = result
        json_list.append(json_obj)
        record_list.append(rec)

        if i % 1000 == 0:
            print(f"  → 배치 #{batch_index} 현재 {i}/{len(batch_targets)} 파싱 완료")

    inserted = insert_records(conn, json_list, record_list)
    print(f"[배치] #{batch_index} - DB INSERT 완료 (inserted={inserted})")

    return inserted

# ==============================
# 한 번의 스캔/파싱/DB 업로드 사이클
# ==============================

def run_one_cycle():
    cycle_start = time.time()

    # 날짜 윈도우 계산 (이번 달 1일 ~ 오늘)
    window_start_date, window_end_date = get_window_dates()
    window_start_str = window_start_date.strftime("%Y%m%d")
    window_end_str = window_end_date.strftime("%Y%m%d")

    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_LOOKBACK_SECONDS  # 🔥 최근 N초 이내 파일만 대상

    print("\n==================== CYCLE START ====================")
    print("[DEBUG] BASE_LOG_DIR         :", BASE_LOG_DIR)
    print("[DEBUG] BASE_LOG_DIR exists? :", BASE_LOG_DIR.exists())
    print(f"[윈도우] 파싱 기간: {window_start_date} ~ {window_end_date}")
    print(f"[실시간] 최근 {REALTIME_LOOKBACK_SECONDS}초 이내 수정된 파일만 처리 (cutoff_ts={cutoff_ts})")

    conn = get_connection()
    try:
        ensure_schema_and_table(conn)

        total_scanned = 0       # 윈도우+mtime 조건을 통과한 파일 수
        total_new_target = 0    # 이번 사이클에서 파싱 대상이 된 파일 수
        total_inserted = 0      # INSERT 시도 건수(중복은 DB가 무시)
        batch_index = 1

        # 🔥 멀티프로세싱 워커 수를 4개로 고정
        max_workers = 4
        print(f"[멀티프로세싱] 사용 프로세스 수: {max_workers}")

        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            batch_targets = []

            for mid in MIDDLE_FOLDERS:
                mid_dir = BASE_LOG_DIR / mid
                print("[DEBUG] mid_dir:", mid_dir, "exists?:", mid_dir.exists())

                if not mid_dir.exists():
                    continue

                for date_dir in sorted(mid_dir.iterdir()):
                    if not date_dir.is_dir():
                        continue

                    date_folder_name = date_dir.name  # 예: "20251122"
                    if not (date_folder_name.isdigit() and len(date_folder_name) == 8):
                        # yyyymmdd 형태가 아니면 스킵
                        continue

                    # 윈도우 기준으로 yyyymmdd 문자열 비교 (같은 형식이면 문자열 비교 == 날짜 비교)
                    if not (window_start_str <= date_folder_name <= window_end_str):
                        continue

                    # 이 시점에서 date_dir는 윈도우 안에 있는 폴더
                    any_txt_here = False

                    # 1) GoodFile / BadFile 밑 파일들
                    for sub in TARGET_FOLDERS:
                        target_dir = date_dir / sub
                        if not target_dir.exists():
                            continue

                        for f in target_dir.iterdir():
                            if not f.is_file():
                                continue

                            # 🔥 실시간 mtime 필터: 최근 REALTIME_LOOKBACK_SECONDS 이내 수정된 파일만
                            try:
                                if f.stat().st_mtime < cutoff_ts:
                                    continue
                            except FileNotFoundError:
                                # 사이에 파일이 삭제된 경우 등은 그냥 무시
                                continue

                            total_scanned += 1

                            path_str = str(f)
                            if len(batch_targets) < 5:
                                print("[DEBUG] FOUND FILE (sub):", path_str)

                            batch_targets.append((path_str, mid))
                            total_new_target += 1
                            any_txt_here = True

                            if len(batch_targets) >= BATCH_SIZE:
                                total_inserted += process_batch(
                                    executor,
                                    batch_targets,
                                    conn,
                                    batch_index,
                                )
                                batch_targets = []
                                batch_index += 1

                    # 2) GoodFile/BadFile 없고 날짜 폴더 바로 밑에 파일 있는 경우
                    if not any_txt_here:
                        for f in date_dir.iterdir():
                            if not f.is_file():
                                continue

                            # 🔥 실시간 mtime 필터
                            try:
                                if f.stat().st_mtime < cutoff_ts:
                                    continue
                            except FileNotFoundError:
                                continue

                            total_scanned += 1

                            path_str = str(f)
                            if len(batch_targets) < 5:
                                print("[DEBUG] FOUND FILE (date_dir):", path_str)

                            batch_targets.append((path_str, mid))
                            total_new_target += 1

                            if len(batch_targets) >= BATCH_SIZE:
                                total_inserted += process_batch(
                                    executor,
                                    batch_targets,
                                    conn,
                                    batch_index,
                                )
                                batch_targets = []
                                batch_index += 1

            # 남은 배치 처리
            if batch_targets:
                total_inserted += process_batch(
                    executor,
                    batch_targets,
                    conn,
                    batch_index,
                )

        duration = time.time() - cycle_start
        print(f"[CYCLE] 전체 스캔 파일 수       : {total_scanned}")
        print(f"[CYCLE] 새로 대상이 된 파일 수  : {total_new_target}")
        print(f"[CYCLE] 이번 사이클 INSERT 시도 수: {total_inserted}")
        print(f"[CYCLE] 소요 시간: {duration:.1f}초")
        print("==================== CYCLE END ====================")

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
        time.sleep(1)

if __name__ == "__main__":
    main()
