# -*- coding: utf-8 -*-
# ============================================
# Main Machine Log Parser (Realtime / MP=2)
# ============================================

import re
import time as time_mod
from pathlib import Path
from datetime import datetime, date, time as dtime, timedelta
from multiprocessing import Pool, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse


# =========================
# 1. 경로 / DB 설정
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "d1_machine_log"
TABLE_NAME  = "Main_machine_log"

DAY_START = dtime(8, 30, 0)
DAY_END   = dtime(20, 29, 59)

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
                end_day     VARCHAR(8),
                station     VARCHAR(10),
                dayornight  VARCHAR(10),
                time        VARCHAR(12),
                contents    VARCHAR(75)
            )
        """))

        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_NAME.lower()}_dedup
            ON "{SCHEMA_NAME}"."{TABLE_NAME}"
            (end_day, station, dayornight, time, contents)
        """))

    print("[INFO] Schema / table / unique index ready")


# =========================
# 4. 주 / 야간 판정
# =========================
def classify_shift(file_date: date, t: dtime):
    if DAY_START <= t <= DAY_END:
        return file_date.strftime("%Y%m%d"), "day"
    elif dtime(20, 30, 0) <= t <= dtime(23, 59, 59):
        return file_date.strftime("%Y%m%d"), "night"
    elif dtime(0, 0, 0) <= t <= dtime(8, 29, 59):
        return (file_date - timedelta(days=1)).strftime("%Y%m%d"), "night"
    else:
        return file_date.strftime("%Y%m%d"), "night"


# =========================
# 5. 파일 파싱 (워커)
# =========================
def parse_main_log_file(path_str: str):
    file_path = Path(path_str)
    m = FILE_PATTERN.search(file_path.name)
    if not m:
        return []

    file_ymd = m.group(1)
    file_date = datetime.strptime(file_ymd, "%Y%m%d").date()
    today_ymd = datetime.now().strftime("%Y%m%d")

    rows = []
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            for line in f:
                line = line.rstrip()
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                time_str, contents_raw = mm.groups()
                try:
                    t = datetime.strptime(time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                end_day, dayornight = classify_shift(file_date, t)

                # (3) 오늘 데이터만
                if end_day != today_ymd:
                    continue

                contents = contents_raw.replace("\x00", "").strip()[:75]

                rows.append({
                    "end_day": end_day,
                    "station": "Main",
                    "dayornight": dayornight,
                    "time": time_str,
                    "contents": contents,
                })
    except Exception:
        return []

    return rows


# =========================
# 6. 실시간 대상 파일 수집
# =========================
def list_target_files_realtime(base_dir: Path):
    targets = []
    cutoff_ts = time_mod.time() - REALTIME_WINDOW_SEC

    for year_dir in base_dir.iterdir():
        if not (year_dir.is_dir() and year_dir.name.isdigit()):
            continue

        for month_dir in year_dir.iterdir():
            if not (month_dir.is_dir() and month_dir.name.isdigit()):
                continue

            for fp in month_dir.iterdir():
                if not (fp.is_file() and FILE_PATTERN.search(fp.name)):
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
# 7. DB INSERT
# =========================
def insert_to_db(engine, df: pd.DataFrame):
    if df.empty:
        return

    df["dayornight"] = pd.Categorical(df["dayornight"], ["day", "night"], ordered=True)
    df = df.sort_values(["end_day", "dayornight", "time"])

    insert_sql = text(f"""
        INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}"
        (end_day, station, dayornight, time, contents)
        VALUES (:end_day, :station, :dayornight, :time, :contents)
        ON CONFLICT (end_day, station, dayornight, time, contents)
        DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(insert_sql, df.to_dict(orient="records"))

    print(f"[DB] Insert attempted {len(df)} rows (duplicates ignored)")


# =========================
# 8. main (1초 무한루프)
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
