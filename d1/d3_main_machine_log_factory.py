# -*- coding: utf-8 -*-
# ============================================
# Main Machine Log Parser (Realtime / MP=2 / Dedup / TodayOnly)
# - 주/야간 로직 제거
# - dayornight 컬럼 제거
# - time 컬럼 -> end_time 컬럼으로 변경
# - end_day는 파일명(YYYYMMDD) 기준으로만 저장
# - 오늘 파일(YYYYMMDD==오늘)만 처리
# - (end_day, station, end_time, contents) 중복 방지 (UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - ❌ 현재시간 기준 120초 이내 새롭게 추가/수정된 파일만 처리(mtime 기준)  -> 제거
# - ✅ 하루 1개 파일이 계속 append 되는 구조에 맞게
#      "offset(tell/seek) 기반 tail-follow"로 신규 라인만 적재
# ============================================

import re
import os
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
SLEEP_SEC = 1

# ✅ 처리 캐시 (path -> 마지막 읽은 파일 byte offset)
PROCESSED_OFFSET = {}


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
# 4. 파일 파싱 (워커) - TAIL FOLLOW
# =========================
def parse_main_log_file(args):
    """
    - end_day: 파일명 YYYYMMDD 그대로
    - end_time: 라인에서 추출한 time_str
    - 오늘 파일(YYYYMMDD==오늘)만 처리
    - ✅ offset 기반: 마지막 읽은 위치 이후 신규 라인만 파싱
    """
    path_str, start_offset = args
    file_path = Path(path_str)

    m = FILE_PATTERN.search(file_path.name)
    if not m:
        return path_str, start_offset, []

    file_ymd = m.group(1)
    today_ymd = datetime.now().strftime("%Y%m%d")

    # 오늘 파일만
    if file_ymd != today_ymd:
        return path_str, start_offset, []

    # 파일이 truncate/재생성 된 경우 대비: offset > size 이면 0으로 리셋
    try:
        size = file_path.stat().st_size
    except Exception:
        return path_str, start_offset, []

    if start_offset > size:
        start_offset = 0

    # 읽을 게 없으면 그대로 리턴
    if start_offset == size:
        return path_str, start_offset, []

    rows = []
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            f.seek(start_offset, os.SEEK_SET)

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
                    "end_time": end_time_str,
                    "contents": contents,
                })

            new_offset = f.tell()

    except Exception:
        return path_str, start_offset, []

    return path_str, new_offset, rows


# =========================
# 5. 실시간 대상 파일 수집 (✅ mtime 필터 제거)
# =========================
def list_target_files_realtime(base_dir: Path):
    """
    - 기존: 120초 mtime 필터로 '최근 변경 파일'만
    - 변경: 오늘 파일만 수집 (하루 1개 파일 append 구조)
    """
    targets = []
    if not base_dir.exists():
        print("[WARN] BASE_DIR not found:", base_dir)
        return targets

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

                m = FILE_PATTERN.search(fp.name)
                if not m or m.group(1) != today_ymd:
                    continue

                targets.append(str(fp))

    if targets:
        print(f"[INFO] Today target files: {len(targets)}")

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

            # ✅ 캐시 선반영(원본 정책 유지) -> offset 정책으로 유지
            # - 파일이 새로 발견되면 offset=0에서 시작
            # - 이미 있던 파일이면 이전 offset부터 이어서 읽음
            tasks = []
            for fp in files:
                tasks.append((fp, int(PROCESSED_OFFSET.get(fp, 0))))

            all_rows = []
            new_offsets = {}

            # ✅ 기존 MP 구조 유지: Pool + imap_unordered + chunksize
            with Pool(processes=min(N_PROCESSES, len(tasks))) as pool:
                for path_str, offset_after, rows in pool.imap_unordered(
                    parse_main_log_file, tasks, chunksize=POOL_CHUNKSIZE
                ):
                    new_offsets[path_str] = int(offset_after)
                    if rows:
                        all_rows.extend(rows)

            # ✅ offset 캐시 갱신
            PROCESSED_OFFSET.update(new_offsets)

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
