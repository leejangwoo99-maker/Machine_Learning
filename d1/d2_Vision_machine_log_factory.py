# -*- coding: utf-8 -*-
# ============================================
# Vision Machine Log Parser (Realtime + MP2 + Dedup + TodayOnly)
# - 주/야간 로직 제거
# - dayornight 컬럼 제거
# - time 컬럼 -> end_time 컬럼으로 변경
# - end_day는 파일명(YYYYMMDD) 기준으로만 저장
# - 오늘 파일(YYYYMMDD==오늘)만 처리
# - (end_day, station, end_time, contents) 중복 방지 (UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - 현재시간 기준 120초 이내 새롭게 추가/수정된 파일만 처리(mtime 기준)
# ============================================

import re
import time as time_mod
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse


# ============================================
# 1. 기본 설정
# ============================================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log")
VISION_DIR_NAME = "Vision"  # 루트 하위 Vision 폴더를 탐색

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "d1_machine_log"
TABLE_MAP = {
    "Vision1": "Vision1_machine_log",
    "Vision2": "Vision2_machine_log",
}

# =========================
# 요구사항: 멀티프로세스 2개 고정
# =========================
USE_MULTIPROCESSING = True
N_PROCESSES = 2
POOL_CHUNKSIZE = 10

# =========================
# 요구사항: 실시간 120초 이내 신규/수정 파일만
# =========================
REALTIME_WINDOW_SEC = 120
SLEEP_SEC = 1

# 처리 캐시: path -> last_mtime
PROCESSED_MTIME = {}


# ============================================
# 2. DB 엔진 생성
# ============================================
def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    print("[INFO] Connection String:", conn_str)
    return create_engine(conn_str)


# ============================================
# 3. 스키마 및 테이블 생성 + UNIQUE INDEX(중복방지)
# ============================================
def ensure_schema_tables(engine):
    ddl_template = """
    CREATE TABLE IF NOT EXISTS {schema}."{table}" (
        id          BIGSERIAL PRIMARY KEY,
        end_day     VARCHAR(8),     -- yyyymmdd (파일명 기준)
        station     VARCHAR(10),    -- Vision1 / Vision2
        end_time    VARCHAR(12),    -- hh:mi:ss.ss
        contents    VARCHAR(200)
    );
    """

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

        for st, tbl in TABLE_MAP.items():
            conn.execute(text(ddl_template.format(schema=SCHEMA_NAME, table=tbl)))

            # 중복 방지: (end_day, station, end_time, contents) UNIQUE INDEX
            idx_name = f"ux_{SCHEMA_NAME}_{tbl.lower()}_dedup"
            conn.execute(text(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                ON {SCHEMA_NAME}."{tbl}" (end_day, station, end_time, contents)
            """))

    print("[INFO] Schema, tables, unique indexes ready")


# ============================================
# 4. 파일 파싱 유틸
# ============================================
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$")
FILENAME_PATTERN = re.compile(r"(\d{8})_Vision([12])_Machine_Log", re.IGNORECASE)


def clean_contents(raw: str, max_len: int = 200):
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())  # 연속 공백 정리
    return s[:max_len].strip()


def _open_text_file(path: Path):
    # Vision 로그 인코딩 혼재 대응
    try:
        return path.open("r", encoding="utf-8", errors="ignore")
    except Exception:
        return path.open("r", encoding="cp949", errors="ignore")


def parse_machine_log_file(path_str: str):
    """
    멀티프로세스 워커용 (pickle 가능)
    한 파일에서 rows(list[dict]) 반환
    - end_day: 파일명 YYYYMMDD 그대로
    - 오늘 파일(YYYYMMDD==오늘)만 반환
    """
    file_path = Path(path_str)

    m = FILENAME_PATTERN.search(file_path.name)
    if not m:
        return []

    file_ymd = m.group(1)
    vision_no = m.group(2)  # '1' or '2'
    station = f"Vision{vision_no}"

    today_ymd = datetime.now().strftime("%Y%m%d")
    if file_ymd != today_ymd:
        return []  # 오늘 파일만

    result = []
    try:
        with _open_text_file(file_path) as f:
            for line in f:
                line = line.rstrip("\n")
                m2 = LINE_PATTERN.match(line)
                if not m2:
                    continue

                end_time_str = m2.group(1)
                contents_raw = m2.group(2)

                # 시간 포맷 검증
                try:
                    _ = datetime.strptime(end_time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                contents = clean_contents(contents_raw, max_len=200)

                result.append(
                    {
                        "end_day": file_ymd,
                        "station": station,
                        "end_time": end_time_str,
                        "contents": contents,
                    }
                )
    except Exception as e:
        print(f"[WARN] Failed to read: {file_path} / {e}")
        return []

    return result


# ============================================
# 5. "실시간 120초 이내" 신규/수정 파일만 수집
#    디렉토리 구조: Vision\\YYYY\\MM\\파일명
# ============================================
def list_target_files_realtime(base_dir: Path):
    files = []
    if not base_dir.exists():
        print("[WARN] BASE_DIR not found:", base_dir)
        return files

    cutoff_ts = time_mod.time() - REALTIME_WINDOW_SEC
    today_ymd = datetime.now().strftime("%Y%m%d")

    for year_dir in sorted(base_dir.iterdir()):
        if not (year_dir.is_dir() and year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not (month_dir.is_dir() and month_dir.name.isdigit() and len(month_dir.name) == 2):
                continue

            for file_path in month_dir.iterdir():
                if not file_path.is_file():
                    continue

                m = FILENAME_PATTERN.search(file_path.name)
                if not m:
                    continue

                # 오늘 파일만
                file_ymd = m.group(1)
                if file_ymd != today_ymd:
                    continue

                try:
                    mtime = file_path.stat().st_mtime
                except OSError:
                    continue

                # 120초 이내 변경 파일만
                if mtime < cutoff_ts:
                    continue

                # 새롭게 추가/수정된 파일만 (캐시)
                fp = str(file_path)
                prev_mtime = PROCESSED_MTIME.get(fp, 0)
                if mtime <= prev_mtime:
                    continue

                files.append(fp)

    if files:
        print(f"[INFO] Realtime target files: {len(files)} (window={REALTIME_WINDOW_SEC}s)")
    return files


# ============================================
# 6. DB Insert (중복은 DB에서 회피)
# ============================================
def insert_to_db(engine, df: pd.DataFrame):
    if df.empty:
        return

    # 정렬: end_day 오름차순, end_time 오름차순
    df = df.sort_values(["end_day", "end_time"]).reset_index(drop=True)

    with engine.begin() as conn:
        for st, tbl in TABLE_MAP.items():
            sub = df[df["station"] == st]
            if sub.empty:
                continue

            insert_sql = text(f"""
                INSERT INTO {SCHEMA_NAME}."{tbl}" (end_day, station, end_time, contents)
                VALUES (:end_day, :station, :end_time, :contents)
                ON CONFLICT (end_day, station, end_time, contents) DO NOTHING
            """)

            conn.execute(insert_sql, sub.to_dict(orient="records"))
            print(f"[DB] Insert attempted {len(sub)} rows into {SCHEMA_NAME}.{tbl} (duplicates ignored)")


# ============================================
# 7. main (1초 무한루프)
# ============================================
def main():
    engine = get_engine()
    ensure_schema_tables(engine)

    while True:
        try:
            files = list_target_files_realtime(BASE_DIR)
            if not files:
                time_mod.sleep(SLEEP_SEC)
                continue

            # 캐시 먼저 갱신(루프 중복 방지)
            for fp in files:
                try:
                    PROCESSED_MTIME[fp] = Path(fp).stat().st_mtime
                except OSError:
                    PROCESSED_MTIME[fp] = time_mod.time()

            all_records = []

            if USE_MULTIPROCESSING and len(files) >= 2:
                procs = min(N_PROCESSES, len(files))
                print(f"[INFO] Using multiprocessing: {procs} processes")
                with Pool(processes=procs) as pool:
                    for rows in pool.imap_unordered(parse_machine_log_file, files, chunksize=POOL_CHUNKSIZE):
                        if rows:
                            all_records.extend(rows)
            else:
                print("[INFO] Multiprocessing disabled (or not enough files).")
                for fp in files:
                    rows = parse_machine_log_file(fp)
                    if rows:
                        all_records.extend(rows)

            if not all_records:
                time_mod.sleep(SLEEP_SEC)
                continue

            df = pd.DataFrame(all_records)
            insert_to_db(engine, df)

        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user.")
            break
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")

        time_mod.sleep(SLEEP_SEC)


if __name__ == "__main__":
    freeze_support()
    main()
