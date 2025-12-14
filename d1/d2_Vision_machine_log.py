# -*- coding: utf-8 -*-
# ============================================
# Vision Machine Log Parser (Multiprocessing)
# ============================================

import re
from pathlib import Path
from datetime import datetime, date, timedelta, time as dt_time
from multiprocessing import Pool, cpu_count, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse


# ============================================
# 1. 기본 설정
# ============================================
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\machinlog\Vision")

DB_CONFIG = {
    "host": "localhost",
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
    return create_engine(conn_str)


# ============================================
# 3. 스키마 및 테이블 생성
# ============================================
def ensure_schema_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))

        ddl_template = """
        CREATE TABLE IF NOT EXISTS {schema}."{table}" (
            end_day     VARCHAR(8),
            station     VARCHAR(10),
            dayornight  VARCHAR(10),
            time        VARCHAR(12),
            contents    VARCHAR(200)
        )
        """
        for st, tbl in TABLE_MAP.items():
            conn.execute(text(ddl_template.format(schema=SCHEMA_NAME, table=tbl)))

    print("[INFO] Schema & tables ready")


# ============================================
# 4. Day/Night 판정
# ============================================
def classify_day_night(file_date: date, t: dt_time):
    t_day_start = dt_time(8, 30, 0)
    t_day_end = dt_time(20, 29, 59, 999999)
    t_night_start = dt_time(20, 30, 0)
    t_night_end = dt_time(23, 59, 59, 999999)
    t_morning_end = dt_time(8, 29, 59, 999999)

    if t_day_start <= t <= t_day_end:
        return file_date.strftime("%Y%m%d"), "day"
    elif t_night_start <= t <= t_night_end:
        return file_date.strftime("%Y%m%d"), "night"
    elif dt_time(0, 0, 0) <= t <= t_morning_end:
        prev_date = file_date - timedelta(days=1)
        return prev_date.strftime("%Y%m%d"), "night"

    return file_date.strftime("%Y%m%d"), "day"


# ============================================
# 5. 파일 파싱 유틸
# ============================================
line_pattern = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$")


def clean_contents(raw: str, max_len: int = 75):
    cleaned = "".join(ch for ch in raw if ch.isprintable())
    return cleaned[:max_len].strip()


def _open_text_file(path: Path):
    """
    Vision 로그 인코딩이 혼재할 수 있어서 utf-8 우선, 실패 시 cp949 fallback.
    """
    try:
        return path.open("r", encoding="utf-8", errors="ignore")
    except Exception:
        return path.open("r", encoding="cp949", errors="ignore")


def parse_machine_log_file(file_path: Path, station: str):
    """
    파일명 예: 20251001_Vision1_Machine_Log
    """
    m = re.search(r"(\d{8})_Vision[12]_Machine_Log", file_path.name)
    if not m:
        return []

    file_date = datetime.strptime(m.group(1), "%Y%m%d").date()
    result = []

    with _open_text_file(file_path) as f:
        for line in f:
            line = line.rstrip("\n")
            m2 = line_pattern.match(line)
            if not m2:
                continue

            time_str = m2.group(1)
            contents_raw = m2.group(2)

            try:
                t = datetime.strptime(time_str, "%H:%M:%S.%f").time()
            except ValueError:
                continue

            end_day, dayornight = classify_day_night(file_date, t)
            contents = clean_contents(contents_raw)

            result.append(
                {
                    "end_day": end_day,
                    "station": station,
                    "dayornight": dayornight,
                    "time": time_str,
                    "contents": contents,
                }
            )

    return result


# ============================================
# 6. 전체 파일 탐색
# ============================================
def iter_log_files(base_dir: Path):
    """
    디렉토리 구조:
    Vision\\YYYY\\MM\\파일명
    """
    for year_dir in sorted(base_dir.iterdir()):
        if not (year_dir.is_dir() and year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not (month_dir.is_dir() and month_dir.name.isdigit() and len(month_dir.name) == 2):
                continue

            for file_path in sorted(month_dir.iterdir()):
                if (
                    file_path.is_file()
                    and re.match(r"\d{8}_Vision[12]_Machine_Log", file_path.name)
                ):
                    yield file_path


# ============================================
# 7. 멀티프로세스 워커
# ============================================
def worker_parse_one(file_path_str: str):
    """
    Pool 워커 함수는 pickle 가능해야 하므로,
    Path 대신 str로 받고 내부에서 Path 복원.
    """
    file_path = Path(file_path_str)

    if "_Vision1_" in file_path.name:
        station = "Vision1"
    elif "_Vision2_" in file_path.name:
        station = "Vision2"
    else:
        return []

    rows = parse_machine_log_file(file_path, station)
    return rows


# ============================================
# 8. DB Insert
# ============================================
def insert_to_db(engine, df: pd.DataFrame):
    if df.empty:
        print("[WARN] DataFrame empty. Nothing to insert.")
        return

    # 정렬(원하시는 기준 유지)
    df["dayornight"] = pd.Categorical(df["dayornight"], ["day", "night"], ordered=True)
    df = df.sort_values(["end_day", "dayornight", "time"]).reset_index(drop=True)

    with engine.begin() as conn:
        for st, tbl in TABLE_MAP.items():
            sub = df[df["station"] == st]
            if sub.empty:
                continue

            # to_sql 성능 옵션: method="multi" + chunksize
            sub.to_sql(
                tbl,
                schema=SCHEMA_NAME,
                con=conn,
                index=False,
                if_exists="append",
                method="multi",
                chunksize=5000,
            )
            print(f"[DB] Inserted {len(sub)} rows into {SCHEMA_NAME}.{tbl}")


# ============================================
# 9. main
# ============================================
def main():
    engine = get_engine()
    ensure_schema_tables(engine)

    files = [str(p) for p in iter_log_files(BASE_LOG_DIR)]
    print(f"[INFO] Total files found = {len(files)}")

    if not files:
        print("[DONE] No files.")
        return

    # 멀티프로세스 개수(PC 상황에 따라 조정)
    nproc = max(1, cpu_count() - 1)
    print(f"[INFO] Using multiprocessing: {nproc} processes")

    all_records = []
    parsed_files = 0

    with Pool(processes=nproc) as pool:
        for rows in pool.imap_unordered(worker_parse_one, files, chunksize=10):
            parsed_files += 1
            if rows:
                all_records.extend(rows)

            # 진행 로그(과도한 출력 방지)
            if parsed_files % 20 == 0 or parsed_files == len(files):
                print(
                    f"[PROGRESS] files={parsed_files}/{len(files)}, total_rows={len(all_records)}"
                )

    print(f"[INFO] Total parsed rows = {len(all_records)}")

    df = pd.DataFrame(all_records)
    if df.empty:
        print("[DONE] Parsed 0 rows.")
        return

    # DB 적재
    insert_to_db(engine, df)

    print("[DONE] Vision Machine Log Parsing Completed")


if __name__ == "__main__":
    freeze_support()
    main()
