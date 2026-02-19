# -*- coding: utf-8 -*-
# fct_machine_log_parser_mp.py
# ============================================
# FCT 머신 로그 파싱 + PostgreSQL 적재 (멀티프로세스 파싱)
# - 주/야간 로직 제거
# - dayornight 컬럼 제거
# - end_day는 파일명(YYYYMMDD) 기준으로만 저장
# - time 컬럼 제거 -> end_time 컬럼으로 통일
# - end_time은 hh:mm:ss.ss 문자열로 고정 저장 (VARCHAR(12))
# ============================================

import re
from pathlib import Path
from datetime import datetime
from multiprocessing import Pool, cpu_count, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse


# ============================================
# 1. 기본 설정
# ============================================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")  # 루트 (YYYY/MM 구조)

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SCHEMA = "d1_machine_log"

# 테이블명은 정확히 FCT1_machine_log ~ FCT4_machine_log 사용 (대문자 포함 가능 → " "로 감싸서 생성)
TABLE_BY_STATION = {
    "FCT1": 'FCT1_machine_log',
    "FCT2": 'FCT2_machine_log',
    "FCT3": 'FCT3_machine_log',
    "FCT4": 'FCT4_machine_log',
}

# 파일명 패턴 (YYYYMMDD_FCT1~4_Machine_Log, YYYYMMDD_PDI1~4_Machine_Log 모두 허용)
FILENAME_PATTERN = re.compile(r"(\d{8})_(FCT|PDI)([1-4])_Machine_Log", re.IGNORECASE)

# 라인 패턴: [hh:mi:ss] 또는 [hh:mi:ss.xxx...] 모두 허용
# group(1)=hh:mm:ss, group(2)=frac(옵션), group(3)=contents
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?\]\s*(.*)$")

# 멀티프로세스 설정
USE_MULTIPROCESSING = True
N_PROCESSES = max(1, cpu_count() - 1)  # CPU 여유 1개 남김
POOL_CHUNKSIZE = 10                    # 파일 수가 많으면 10~50 권장


# ============================================
# 2. DB 엔진 생성 & 스키마/테이블 생성
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
CREATE TABLE IF NOT EXISTS {schema}."{table}" (
    id          BIGSERIAL PRIMARY KEY,
    end_day     VARCHAR(8),     -- yyyymmdd (파일명 기준)
    station     VARCHAR(10),    -- FCT1~4
    end_time    VARCHAR(12),    -- hh:mm:ss.ss (문자열 고정)
    contents    VARCHAR(75)     -- 최대 75글자
);
"""


def ensure_schema_and_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

        for st, tbl in TABLE_BY_STATION.items():
            ddl = CREATE_TABLE_TEMPLATE.format(schema=SCHEMA, table=tbl)
            conn.execute(text(ddl))
            print(f'[INFO] Table ensured: {SCHEMA}."{tbl}"')


# ============================================
# 3. contents 정리 (공백 정리 + 75자리 제한)
# ============================================
def clean_contents(raw: str, max_len: int = 75) -> str:
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


# ============================================
# 4. 단일 로그 파일 파싱 (멀티프로세스용)
# ============================================
def parse_machine_log_file(path_str: str):
    """
    한 개의 로그 파일을 파싱해서 rows(list[dict]) 반환
    - end_day: 파일명 YYYYMMDD 그대로 저장
    - end_time: 항상 hh:mm:ss.ss 문자열로 정규화하여 저장
      (소수점 없으면 .00 / 1자리면 0패딩 / 3~6자리는 앞 2자리만 사용)
    """
    path = Path(path_str)

    m = FILENAME_PATTERN.search(path.name)
    if not m:
        return []

    file_ymd = m.group(1)        # YYYYMMDD
    no = m.group(3)              # '1'~'4'
    station = f"FCT{no}"         # PDI도 FCT로 취급

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
                        "end_time": end_time_str,
                        "contents": contents,
                    }
                )

    except Exception as e:
        print(f"[WARN] Failed to read: {path} / {e}")
        return []

    return rows


# ============================================
# 5. yyyy/mm 폴더만 순회하며 파일 목록 수집
# ============================================
def list_target_files():
    files = []

    if not BASE_DIR.exists():
        print("[WARN] BASE_DIR not found:", BASE_DIR)
        return files

    for year_dir in sorted(BASE_DIR.iterdir()):
        if not (year_dir.is_dir() and year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not (month_dir.is_dir() and month_dir.name.isdigit() and len(month_dir.name) == 2):
                continue

            print(f"[INFO] Scan folder: {month_dir}")

            for file_path in sorted(month_dir.iterdir()):
                if not file_path.is_file():
                    continue
                if not FILENAME_PATTERN.search(file_path.name):
                    continue
                files.append(str(file_path))

    print(f"[INFO] Target files: {len(files)}")
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
        print("[INFO] Multiprocessing disabled (or not enough files).")
        for fp in file_list:
            rows = parse_machine_log_file(fp)
            for r in rows:
                st = r["station"]
                if st in data_by_station:
                    data_by_station[st].append(r)

    return data_by_station


# ============================================
# 6. DB INSERT (end_day 오름차순, end_time 오름차순)
# ============================================
def insert_to_db(engine, data_by_station):
    with engine.begin() as conn:
        for station, rows in data_by_station.items():
            if not rows:
                print(f"[INFO] {station}: no rows, skip.")
                continue

            df = pd.DataFrame(rows)

            df.sort_values(
                by=["end_day", "end_time"],
                ascending=[True, True],
                inplace=True,
            )

            tbl = TABLE_BY_STATION[station]
            full_table = f'{SCHEMA}."{tbl}"'

            insert_sql = text(
                f"""
                INSERT INTO {full_table} (end_day, station, end_time, contents)
                VALUES (:end_day, :station, :end_time, :contents)
                """
            )

            conn.execute(insert_sql, df.to_dict(orient="records"))
            print(f"[INFO] Inserted {len(df)} rows → {full_table}")


# ============================================
# 7. main
# ============================================
def main():
    engine = get_engine()
    ensure_schema_and_tables(engine)

    file_list = list_target_files()
    data_by_station = collect_all_rows_multiprocess(file_list)

    insert_to_db(engine, data_by_station)
    print("[DONE] 머신 로그 파싱 및 DB 적재 완료")


if __name__ == "__main__":
    freeze_support()
    main()
