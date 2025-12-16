# main_machine_log_parser_multiprocess.py
import re
from pathlib import Path
from datetime import datetime, date, time as dtime, timedelta
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count, freeze_support

import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse

# =========================
# 1. 경로 / DB 설정
# =========================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "d1_machine_log"
TABLE_NAME  = "Main_machine_log"

DAY_START = dtime(8, 30, 0)     # 08:30:00
DAY_END   = dtime(20, 29, 59)   # 20:29:59

# [hh:mi:ss.ss] (내용)
LINE_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$')

# 파일명 패턴
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


def classify_shift(file_date: date, t: dtime):
    """
    주/야간 및 end_day 결정

    - day : 파일 날짜, 08:30:00 ~ 20:29:59
    - night(저녁) : 파일 날짜, 20:30:00 ~ 23:59:59
    - night(새벽) : 파일 날짜의 다음날 로그 00:00:00 ~ 08:29:59 → end_day = 파일 날짜 - 1일
    """
    if DAY_START <= t <= DAY_END:
        end_day = file_date
        dayornight = "day"
    elif dtime(20, 30, 0) <= t <= dtime(23, 59, 59):
        end_day = file_date
        dayornight = "night"
    elif dtime(0, 0, 0) <= t <= dtime(8, 29, 59):
        end_day = file_date - timedelta(days=1)
        dayornight = "night"
    else:
        end_day = file_date
        dayornight = "night"

    end_day_str = end_day.strftime("%Y%m%d")
    return end_day_str, dayornight


def parse_main_log_file(file_path_str: str):
    """
    워커 프로세스에서 실행될 함수.
    - 입력은 Path 객체 대신 str로 받는 것을 권장(피클 안정성)
    - 반환: list[dict]
    """
    file_path = Path(file_path_str)

    m = re.match(r'^(\d{8})_Main_Machine_Log', file_path.stem)
    if not m:
        return []

    yyyymmdd = m.group(1)
    file_date = datetime.strptime(yyyymmdd, "%Y%m%d").date()

    records = []
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            for line in f:
                line = line.rstrip("\r\n")
                match = LINE_PATTERN.match(line)
                if not match:
                    continue

                time_str = match.group(1)
                contents_raw = match.group(2)

                try:
                    t_obj = datetime.strptime(time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                end_day_str, dayornight = classify_shift(file_date, t_obj)

                contents = contents_raw.replace("\x00", "").strip()[:75]

                records.append(
                    {
                        "end_day": end_day_str,
                        "station": "Main",
                        "dayornight": dayornight,
                        "time": time_str,
                        "contents": contents,
                    }
                )
    except Exception as e:
        # 파일 단위 예외는 잡고 빈 리스트 반환(전체 작업 중단 방지)
        return []

    return records


def collect_target_files(base_dir: Path):
    # """
    # C:\...\Main\yyyy\mm\ 아래에서
    # 20251101_Main_Machine_Log.txt 만 수집
    # """
    targets = []
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


def main():
    engine = get_engine()

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))

    # 1) 대상 파일 수집
    target_files = collect_target_files(BASE_DIR)
    print(f"[INFO] 대상 파일 수: {len(target_files)}")
    if not target_files:
        print("[WARN] 대상 파일이 없습니다.")
        return

    # 2) 멀티프로세스 파싱
    max_workers = max(1, cpu_count() - 1)  # 과부하 방지(원하면 cpu_count()로)
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

    df["day_order"] = df["dayornight"].map({"day": 0, "night": 1}).fillna(1).astype(int)
    df = df.sort_values(by=["end_day", "day_order", "time"]).reset_index(drop=True)
    df = df[["end_day", "station", "dayornight", "time", "contents"]]

    print("[INFO] Sample head:")
    print(df.head(5).to_string(index=False))

    df.to_sql(
        TABLE_NAME,
        engine,
        schema=SCHEMA_NAME,
        if_exists="replace",  # 누적이면 "append"
        index=False,
        method="multi",
        chunksize=2000,       # 상황에 따라 1000~10000 조절
    )
    print(f"[DONE] {SCHEMA_NAME}.{TABLE_NAME} 테이블에 {len(df)}건 적재 완료")


if __name__ == "__main__":
    freeze_support()  # Windows 멀티프로세스 안전장치
    main()
