# -*- coding: utf-8 -*-
# ============================================
# Main Machine Log Parser (Realtime / MP=2 / Dedup / TodayOnly)
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


# =========================
# 1. 경로 / DB 설정
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\Main")

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "d1_machine_log"
TABLE_NAME  = "Main_machine_log"

LINE_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$')
FILE_PATTERN = re.compile(r"(\d{8})_Main_Machine_Log\.txt$", re.IGNORECASE)

# =========================
# 요구사항 고정값
# =========================
N_PROCESSES = 2
POOL_CHUNKSIZE = 10
REALTIME_WINDOW_SEC = 120
SLEEP_SEC = 1

# 처리 캐시 (path -> mtime)
PROCESSED_MTIME = {}


# =========================
# 2. DB 엔진
# =========================
def get_engine(config=DB_CONFIG):
    password = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{password}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )
    print("[INFO] Connection String:", conn_str)
    return create_engine(conn_str)


# =========================
# 3. 스키마 / 테이블 / UNIQUE INDEX
# =========================
def ensure_schema_table(engine):
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."{TABLE_NAME}" (
                id BIGSERIAL PRIMARY KEY,
                end_day     VARCHAR(8),     -- yyyymmdd (파일명 기준)
                station     VARCHAR(10),    -- Main
                end_time    VARCHAR(12),    -- hh:mi:ss.ss
                contents    VARCHAR(75)
            )
        """))

        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_NAME.lower()}_dedup
            ON "{SCHEMA_NAME}"."{TABLE_NAME}"
            (end_day, station, end_time, contents)
        """))

    print("[INFO] Schema / table / unique index ready")


# =========================
# 4. 파일 파싱 (워커)
# =========================
def parse_main_log_file(path_str: str):
    """
    - end_day: 파일명 YYYYMMDD 그대로
    - end_time: 라인에서 추출한 time_str
    - 오늘 파일(YYYYMMDD==오늘)만 처리
    """
    file_path = Path(path_str)
    m = FILE_PATTERN.search(file_path.name)
    if not m:
        return []

    file_ymd = m.group(1)
    today_ymd = datetime.now().strftime("%Y%m%d")

    # 오늘 파일만
    if file_ymd != today_ymd:
        return []

    rows = []
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            for line in f:
                line = line.rstrip()
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                end_time_str, contents_raw = mm.groups()

                # 시간 포맷 검증
                try:
                    _ = datetime.strptime(end_time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                contents = contents_raw.replace("\x00", "").strip()[:75]

                rows.append({
                    "end_day": file_ymd,
                    "station": "Main",
                    "end_time": end_time_str,   # time -> end_time
                    "contents": contents,
                })
    except Exception:
        return []

    return rows


# =========================
# 5. 실시간 대상 파일 수집
# =========================
def list_target_files_realtime(base_dir: Path):
    targets = []
    if not base_dir.exists():
        print("[WARN] BASE_DIR not found:", base_dir)
        return targets

    cutoff_ts = time_mod.time() - REALTIME_WINDOW_SEC
    today_ymd = datetime.now().strftime("%Y%m%d")

    for year_dir in base_dir.iterdir():
        if not (year_dir.is_dir() and year_dir.name.isdigit()):
            continue

        for month_dir in year_dir.iterdir():
            if not (month_dir.is_dir() and month_dir.name.isdigit()):
                continue

            for fp in month_dir.iterdir():
                if not (fp.is_file() and FILE_PATTERN.search(fp.name)):
                    continue

                # 오늘 파일만
                m = FILE_PATTERN.search(fp.name)
                if not m or m.group(1) != today_ymd:
                    continue

                try:
                    mtime = fp.stat().st_mtime
                except OSError:
                    continue

                if mtime < cutoff_ts:
                    continue

                fp_str = str(fp)
                if mtime <= PROCESSED_MTIME.get(fp_str, 0):
                    continue

                targets.append(fp_str)

    if targets:
        print(f"[INFO] Realtime target files: {len(targets)}")

    return targets


# =========================
# 6. DB INSERT
# =========================
def insert_to_db(engine, df: pd.DataFrame):
    if df.empty:
        return

    df = df.sort_values(["end_day", "end_time"])

    insert_sql = text(f"""
        INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}"
        (end_day, station, end_time, contents)
        VALUES (:end_day, :station, :end_time, :contents)
        ON CONFLICT (end_day, station, end_time, contents)
        DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, df.to_dict(orient="records"))

    print(f"[DB] Insert attempted {len(df)} rows (duplicates ignored)")


# =========================
# 7. main (1초 무한루프)
# =========================
def main():
    engine = get_engine()
    ensure_schema_table(engine)

    while True:
        try:
            files = list_target_files_realtime(BASE_DIR)
            if not files:
                time_mod.sleep(SLEEP_SEC)
                continue

            # 캐시 선반영
            for fp in files:
                try:
                    PROCESSED_MTIME[fp] = Path(fp).stat().st_mtime
                except OSError:
                    PROCESSED_MTIME[fp] = time_mod.time()

            all_rows = []
            with Pool(processes=min(N_PROCESSES, len(files))) as pool:
                for rows in pool.imap_unordered(parse_main_log_file, files, chunksize=POOL_CHUNKSIZE):
                    if rows:
                        all_rows.extend(rows)

            if all_rows:
                df = pd.DataFrame(all_rows)
                insert_to_db(engine, df)

        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user")
            break
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")

        time_mod.sleep(SLEEP_SEC)


if __name__ == "__main__":
    freeze_support()
    main()
