# main_machine_log_parser_multiprocess.py
import re
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# =========================
# 1. 경로 / DB 설정
# =========================
BASE_DIR = Path(r"C:\Users\user\Desktop\machinlog\Main")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "d1_machine_log"
TABLE_NAME  = "Main_machine_log"

# [hh:mi:ss.ss] (내용)
LINE_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$')

# 파일명 패턴: 20251101_Main_Machine_Log.txt
FILE_PATTERN = re.compile(r"^\d{8}_Main_Machine_Log\.txt$")


def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    print("[INFO] Connection String:", conn_str)
    return create_engine(conn_str)


def parse_main_log_file(file_path_str: str):
    """
    워커 프로세스에서 실행될 함수.
    - end_day: 파일명(YYYYMMDD) 그대로 저장
    - end_time: 라인에서 추출한 time_str 저장
    - dayornight 없음
    """
    file_path = Path(file_path_str)

    m = re.match(r'^(\d{8})_Main_Machine_Log', file_path.stem)
    if not m:
        return []

    file_ymd = m.group(1)  # YYYYMMDD (파일명 기준)

    records = []
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\r\n")
                match = LINE_PATTERN.match(line)
                if not match:
                    continue

                end_time_str = match.group(1)
                contents_raw = match.group(2)

                # 시간 포맷 검증(유효하지 않으면 skip)
                try:
                    _ = datetime.strptime(end_time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                contents = contents_raw.replace("\x00", "").strip()[:75]

                records.append(
                    {
                        "end_day": file_ymd,
                        "station": "Main",
                        "end_time": end_time_str,  # time -> end_time
                        "contents": contents,
                    }
                )
    except Exception:
        return []

    return records


def collect_target_files(base_dir: Path):
    """
    base_dir\\YYYY\\MM\\ 아래에서
    20251101_Main_Machine_Log.txt 패턴만 수집
    """
    targets = []
    if not base_dir.exists():
        return targets

    for year_dir in sorted(base_dir.iterdir()):
        if not (year_dir.is_dir() and year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not (month_dir.is_dir() and month_dir.name.isdigit() and len(month_dir.name) == 2):
                continue

            for file_path in sorted(month_dir.iterdir()):
                if not file_path.is_file():
                    continue
                if not FILE_PATTERN.match(file_path.name):
                    continue
                targets.append(file_path)

    return targets


def ensure_schema_and_table(engine):
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."{TABLE_NAME}" (
        end_day     VARCHAR(8),    -- yyyymmdd (파일명 기준)
        station     VARCHAR(10),   -- Main
        end_time    VARCHAR(12),   -- hh:mi:ss.ss
        contents    VARCHAR(75)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))
        conn.execute(text(create_table_sql))


def main():
    engine = get_engine()
    ensure_schema_and_table(engine)

    # 1) 대상 파일 수집
    target_files = collect_target_files(BASE_DIR)
    print(f"[INFO] 대상 파일 수: {len(target_files)}")
    if not target_files:
        print("[WARN] 대상 파일이 없습니다.")
        return

    # 2) 멀티프로세스 파싱
    max_workers = max(1, cpu_count() - 1)  # 과부하 방지
    print(f"[INFO] 멀티프로세스 workers: {max_workers}")

    all_records = []
    submitted = 0
    done = 0

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        futures = []
        for fp in target_files:
            futures.append(ex.submit(parse_main_log_file, str(fp)))
            submitted += 1

        for fut in as_completed(futures):
            res = fut.result()
            if res:
                all_records.extend(res)
            done += 1
            if done % 10 == 0 or done == submitted:
                print(f"[PROGRESS] {done}/{submitted} files parsed...")

    print(f"[INFO] 총 레코드 수: {len(all_records)}")
    if not all_records:
        print("[WARN] 파싱된 데이터가 없습니다.")
        return

    # 3) 정렬 & DB 적재(메인 프로세스에서만)
    df = pd.DataFrame(all_records)
    df = df.sort_values(by=["end_day", "end_time"]).reset_index(drop=True)
    df = df[["end_day", "station", "end_time", "contents"]]

    print("[INFO] Sample head:")
    print(df.head(5).to_string(index=False))

    df.to_sql(
        TABLE_NAME,
        engine,
        schema=SCHEMA_NAME,
        if_exists="replace",  # 누적이면 "append"
        index=False,
        method="multi",
        chunksize=2000,
    )
    print(f"[DONE] {SCHEMA_NAME}.{TABLE_NAME} 테이블에 {len(df)}건 적재 완료")


if __name__ == "__main__":
    freeze_support()  # Windows 멀티프로세스 안전장치
    main()
