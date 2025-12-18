# -*- coding: utf-8 -*-
# fct_machine_log_parser_mp_realtime.py
# ============================================
# FCT 머신 로그 파싱 + PostgreSQL 적재 (실시간/중복방지/오늘자만)
# - 1초마다 무한루프
# - 멀티프로세스 2개 고정
# - end_day = 오늘(현재 날짜)만 적재  (파일명 YYYYMMDD 기준)
# - (end_day, station, end_time, contents) 중복 방지 (UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - 현재시간 기준 120초 이내 새롭게 추가/수정된 파일만 처리(mtime 기준)
# - end_time은 hh:mm:ss.ss 문자열로 고정 저장(VARCHAR(12))
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
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

# 테이블명은 정확히 FCT1_machine_log ~ FCT4_machine_log 사용
# (대문자/특수문자 안전 위해 " "로 감쌈)
TABLE_BY_STATION = {
    "FCT1": '"FCT1_machine_log"',
    "FCT2": '"FCT2_machine_log"',
    "FCT3": '"FCT3_machine_log"',
    "FCT4": '"FCT4_machine_log"',
}

# 파일명 패턴 (YYYYMMDD_FCT1~4_Machine_Log, YYYYMMDD_PDI1~4_Machine_Log 모두 허용)
FILENAME_PATTERN = re.compile(r"(\d{8})_(FCT|PDI)([1-4])_Machine_Log", re.IGNORECASE)

# 라인 패턴:
# - [hh:mm:ss] 또는 [hh:mm:ss.xx] 또는 [hh:mm:ss.xxxxxx] 모두 허용
# - group(1)=hh:mm:ss, group(2)=frac(옵션), group(3)=contents
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?\]\s*(.*)$")


# ============================================
# 2) 요구사항: 멀티프로세스 2개 고정
# ============================================
USE_MULTIPROCESSING = True
N_PROCESSES = 2
POOL_CHUNKSIZE = 10

# ============================================
# 3) 요구사항: 실시간 120초 이내 "새롭게 추가/수정된 파일"만 처리
# ============================================
REALTIME_WINDOW_SEC = 120
SLEEP_SEC = 1

# 처리 캐시: path -> last_mtime
PROCESSED_MTIME = {}


# ============================================
# 4. DB 엔진 생성 & 스키마/테이블 생성 + UNIQUE INDEX
# ============================================
def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    print("[INFO] Connection String:", conn_str)
    return create_engine(conn_str, pool_pre_ping=True)


CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id          BIGSERIAL PRIMARY KEY,
    end_day     VARCHAR(8),    -- yyyymmdd (파일명 기준)
    station     VARCHAR(10),   -- FCT1~4
    end_time    VARCHAR(12),   -- hh:mm:ss.ss (문자열 고정)
    contents    VARCHAR(75)    -- 최대 75글자
);
"""


def _index_safe_name(table_quoted: str) -> str:
    # '"FCT1_machine_log"' -> fct1_machine_log
    return table_quoted.replace('"', "").lower()


def ensure_schema_and_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

        for station, table_quoted in TABLE_BY_STATION.items():
            ddl = CREATE_TABLE_TEMPLATE.format(schema=SCHEMA, table=table_quoted)
            conn.execute(text(ddl))
            print(f"[INFO] Table ensured: {SCHEMA}.{table_quoted}")

            # 중복 방지용 UNIQUE INDEX (end_time 문자열 기준)
            idx_name = f"ux_{SCHEMA}_{_index_safe_name(table_quoted)}_dedup"
            conn.execute(text(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                ON {SCHEMA}.{table_quoted} (end_day, station, end_time, contents)
            """))
            print(f"[INFO] Unique index ensured: {idx_name}")


# ============================================
# 5. contents 정리 (공백 정리 + 75자리 제한)
# ============================================
def clean_contents(raw: str, max_len: int = 75) -> str:
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


# ============================================
# 6. 단일 로그 파일 파싱
#    + end_day=오늘인 파일/행만 적재(파일명 YYYYMMDD 기준)
#    + end_time은 hh:mm:ss.ss로 정규화 후 문자열 저장
# ============================================
def parse_machine_log_file(path_str: str):
    """
    한 개의 로그 파일을 파싱해서
    [{'end_day','station','end_time','contents'}, ...] 반환
    - end_day는 파일명(YYYYMMDD) 그대로 저장
    - 오늘 날짜(YYYYMMDD)와 같은 파일만 적재
    - end_time은 항상 hh:mm:ss.ss 로 정규화 (문자열)
    """
    path = Path(path_str)

    m = FILENAME_PATTERN.search(path.name)
    if not m:
        return []

    file_ymd = m.group(1)        # YYYYMMDD (파일명 날짜)
    no = m.group(3)              # '1'~'4'
    station = f"FCT{no}"         # PDI도 FCT로 취급

    today_ymd = datetime.now().strftime("%Y%m%d")
    if file_ymd != today_ymd:
        return []  # 오늘 파일만

    rows = []
    try:
        with path.open("r", encoding="cp949", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\n")
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                hms = mm.group(1)           # "13:16:34"
                frac = mm.group(2) or ""    # "12" or "" or "123456"
                contents_raw = mm.group(3)

                # 항상 소수점 2자리로 정규화
                if frac:
                    frac2 = frac.ljust(2, "0")[:2]
                else:
                    frac2 = "00"

                end_time_str = f"{hms}.{frac2}"  # "13:16:34.12" or "13:16:34.00"

                # 유효성 검증
                try:
                    datetime.strptime(end_time_str, "%H:%M:%S.%f")
                except ValueError:
                    continue

                contents = clean_contents(contents_raw)

                rows.append(
                    {
                        "end_day": file_ymd,
                        "station": station,
                        "end_time": end_time_str,   # ✅ 문자열 저장
                        "contents": contents,
                    }
                )
    except Exception as e:
        print(f"[WARN] Failed to read: {path} / {e}")
        return []

    return rows


# ============================================
# 7. yyyy/mm 폴더만 순회하며 "실시간 신규 파일"만 수집
#    - 현재시간 기준 120초 이내 (mtime)
#    - "새롭게 추가/수정된 파일만" 처리 (PROCESSED_MTIME 캐시 비교)
#    - 오늘 파일명(YYYYMMDD)만 대상으로 제한
# ============================================
def list_target_files_realtime():
    files = []

    if not BASE_DIR.exists():
        print("[WARN] BASE_DIR not found:", BASE_DIR)
        return files

    cutoff_ts = time_mod.time() - REALTIME_WINDOW_SEC
    today_ymd = datetime.now().strftime("%Y%m%d")

    for year_dir in sorted(BASE_DIR.iterdir()):
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

                # 오늘 파일만 (파일명 YYYYMMDD 기준)
                file_ymd = m.group(1)
                if file_ymd != today_ymd:
                    continue

                try:
                    mtime = file_path.stat().st_mtime
                except OSError:
                    continue

                # 120초 이내 변경된 파일만
                if mtime < cutoff_ts:
                    continue

                # 새롭게 추가/수정된 파일만 (캐시 비교)
                fp_str = str(file_path)
                prev_mtime = PROCESSED_MTIME.get(fp_str, 0)
                if mtime <= prev_mtime:
                    continue

                files.append(fp_str)

    if files:
        print(f"[INFO] Realtime target files: {len(files)} (window={REALTIME_WINDOW_SEC}s)")
    return files


def collect_all_rows_multiprocess(file_list):
    """
    멀티프로세스로 파일별 파싱 후 station별로 묶어서 반환
    """
    data_by_station = {st: [] for st in TABLE_BY_STATION.keys()}
    if not file_list:
        return data_by_station

    if USE_MULTIPROCESSING and len(file_list) >= 2:
        procs = min(N_PROCESSES, len(file_list))
        print(f"[INFO] Multiprocessing enabled: processes={procs}, chunksize={POOL_CHUNKSIZE}")

        with Pool(processes=procs) as pool:
            for rows in pool.imap_unordered(parse_machine_log_file, file_list, chunksize=POOL_CHUNKSIZE):
                for r in rows:
                    st = r["station"]
                    if st in data_by_station:
                        data_by_station[st].append(r)
    else:
        for fp in file_list:
            rows = parse_machine_log_file(fp)
            for r in rows:
                st = r["station"]
                if st in data_by_station:
                    data_by_station[st].append(r)

    return data_by_station


# ============================================
# 8. DB INSERT
#    - 중복 회피: UNIQUE INDEX + ON CONFLICT DO NOTHING
# ============================================
def insert_to_db(engine, data_by_station):
    with engine.begin() as conn:
        for station, rows in data_by_station.items():
            if not rows:
                continue

            df = pd.DataFrame(rows)

            # 정렬: end_day 오름차순, end_time 오름차순
            df.sort_values(by=["end_day", "end_time"], ascending=[True, True], inplace=True)

            table_quoted = TABLE_BY_STATION[station]
            full_table = f"{SCHEMA}.{table_quoted}"

            insert_sql = text(f"""
                INSERT INTO {full_table} (end_day, station, end_time, contents)
                VALUES (:end_day, :station, :end_time, :contents)
                ON CONFLICT (end_day, station, end_time, contents) DO NOTHING
            """)

            conn.execute(insert_sql, df.to_dict(orient="records"))
            print(f"[INFO] Insert attempted {len(df)} rows → {full_table} (duplicates ignored)")


# ============================================
# 9. main (1초 무한루프)
# ============================================
def main():
    engine = get_engine()
    ensure_schema_and_tables(engine)

    while True:
        try:
            file_list = list_target_files_realtime()
            if not file_list:
                time_mod.sleep(SLEEP_SEC)
                continue

            # 캐시 업데이트 (원본 정책 유지)
            for fp in file_list:
                try:
                    PROCESSED_MTIME[fp] = Path(fp).stat().st_mtime
                except OSError:
                    PROCESSED_MTIME[fp] = time_mod.time()

            data_by_station = collect_all_rows_multiprocess(file_list)
            insert_to_db(engine, data_by_station)

        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user.")
            break
        except Exception as e:
            print(f"[ERROR] Loop error: {e}")

        time_mod.sleep(SLEEP_SEC)


if __name__ == "__main__":
    freeze_support()
    main()
