# fct_machine_log_parser_mp.py
# ============================================
# FCT 머신 로그 파싱 + PostgreSQL 적재 (멀티프로세스 파싱)
# ============================================

import re
from pathlib import Path
from datetime import datetime, time, timedelta
from multiprocessing import Pool, cpu_count, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# ============================================
# 1. 기본 설정
# ============================================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

# 테이블명은 정확히 FCT1_machine_log ~ FCT4_machine_log 사용
TABLE_BY_STATION = {
    "FCT1": '"FCT1_machine_log"',
    "FCT2": '"FCT2_machine_log"',
    "FCT3": '"FCT3_machine_log"',
    "FCT4": '"FCT4_machine_log"',
}

# 주/야간 기준
DAY_START = time(8, 30, 0)
DAY_END   = time(20, 29, 59)
NIGHT_START = time(20, 30, 0)
NIGHT_END_EARLY = time(8, 29, 59)

# 파일명 패턴 (YYYYMMDD_FCT1~4_Machine_Log, YYYYMMDD_PDI1~4_Machine_Log 모두 허용)
FILENAME_PATTERN = re.compile(r"(\d{8})_(FCT|PDI)([1-4])_Machine_Log", re.IGNORECASE)

# 라인 패턴: [hh:mi:ss.ss] 내용
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$")

# 멀티프로세스 설정
USE_MULTIPROCESSING = True
N_PROCESSES = max(1, cpu_count() - 1)  # CPU 여유 1개 남김
POOL_CHUNKSIZE = 10                   # 파일 수가 많으면 10~50 권장


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
    return create_engine(conn_str)


CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id          BIGSERIAL PRIMARY KEY,
    end_day     VARCHAR(8),   -- yyyymmdd
    station     VARCHAR(10),  -- FCT1~4
    dayornight  VARCHAR(10),  -- day / night
    time        TIME,         -- hh:mi:ss.ss
    contents    VARCHAR(75)   -- 최대 75글자
);
"""


def ensure_schema_and_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

    with engine.begin() as conn:
        for station, table in TABLE_BY_STATION.items():
            ddl = CREATE_TABLE_TEMPLATE.format(schema=SCHEMA, table=table)
            conn.execute(text(ddl))
            print(f"[INFO] Table ensured: {SCHEMA}.{table}")


# ============================================
# 3. 주간/야간 & end_day 계산
# ============================================
def get_shift_and_endday(file_ymd: str, t: time):
    """
    file_ymd : 파일명 기준 날짜 (YYYYMMDD)
    t        : 라인에서 추출한 시간
    반환값   : (end_day(문자열 YYYYMMDD), dayornight 'day'/'night')
    """
    file_date = datetime.strptime(file_ymd, "%Y%m%d").date()

    if DAY_START <= t <= DAY_END:
        return file_ymd, "day"

    if t >= NIGHT_START:
        return file_ymd, "night"

    if t <= NIGHT_END_EARLY:
        prev = file_date - timedelta(days=1)
        return prev.strftime("%Y%m%d"), "night"

    return file_ymd, "day"


# ============================================
# 4. contents 정리 (특수문자 제거 + 75자리 제한)
# ============================================
def clean_contents(raw: str, max_len: int = 75) -> str:
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = s.strip()
    return s[:max_len]


# ============================================
# 5. 단일 로그 파일 파싱 (멀티프로세스용: top-level 함수)
# ============================================
def parse_machine_log_file(path_str: str):
    """
    한 개의 로그 파일을 파싱해서
    [{'end_day', 'station', 'dayornight', 'time', 'contents'}, ...] 리스트 반환
    """
    path = Path(path_str)

    m = FILENAME_PATTERN.search(path.name)
    if not m:
        return []

    file_ymd = m.group(1)        # YYYYMMDD
    no = m.group(3)              # '1'~'4'
    station = f"FCT{no}"         # PDI1~4도 FCT1~4로 취급

    rows = []

    try:
        with path.open("r", encoding="cp949", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\n")
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                time_str = mm.group(1)
                contents_raw = mm.group(2)

                try:
                    t = datetime.strptime(time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                end_day, dayornight = get_shift_and_endday(file_ymd, t)
                contents = clean_contents(contents_raw)

                rows.append(
                    {
                        "end_day": end_day,
                        "station": station,
                        "dayornight": dayornight,
                        "time": t,
                        "contents": contents,
                    }
                )
    except Exception as e:
        print(f"[WARN] Failed to read: {path} / {e}")
        return []

    return rows


# ============================================
# 6. yyyy/mm 폴더만 순회하며 파일 목록 수집
# ============================================
def list_target_files():
    files = []

    if not BASE_LOG_DIR.exists():
        print("[WARN] BASE_LOG_DIR not found:", BASE_LOG_DIR)
        return files

    for year_dir in sorted(BASE_LOG_DIR.iterdir()):
        if not (year_dir.is_dir() and year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue  # yyyy 폴더만

        for month_dir in sorted(year_dir.iterdir()):
            if not (month_dir.is_dir() and month_dir.name.isdigit() and len(month_dir.name) == 2):
                continue  # mm 폴더만

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
# 7. DB INSERT (end_day 오름차순, day > night, time 오름차순)
# ============================================
def insert_to_db(engine, data_by_station):
    with engine.begin() as conn:
        for station, rows in data_by_station.items():
            if not rows:
                print(f"[INFO] {station}: no rows, skip.")
                continue

            df = pd.DataFrame(rows)

            # day → night 순으로 정렬 (요구사항 유지)
            df["dayornight"] = pd.Categorical(df["dayornight"], ["day", "night"], ordered=True)

            df.sort_values(
                by=["end_day", "dayornight", "time"],
                ascending=[True, True, True],
                inplace=True,
            )

            table_quoted = TABLE_BY_STATION[station]
            full_table = f"{SCHEMA}.{table_quoted}"

            insert_sql = text(
                f"""
                INSERT INTO {full_table} (end_day, station, dayornight, time, contents)
                VALUES (:end_day, :station, :dayornight, :time, :contents)
                """
            )

            conn.execute(insert_sql, df.to_dict(orient="records"))
            print(f"[INFO] Inserted {len(df)} rows → {full_table}")


# ============================================
# 8. main
# ============================================
def main():
    engine = get_engine()
    ensure_schema_and_tables(engine)

    file_list = list_target_files()
    data_by_station = collect_all_rows_multiprocess(file_list)

    insert_to_db(engine, data_by_station)
    print("[DONE] 머신 로그 파싱 및 DB 적재 완료")


if __name__ == "__main__":
    # Windows 멀티프로세스 필수
    freeze_support()
    main()