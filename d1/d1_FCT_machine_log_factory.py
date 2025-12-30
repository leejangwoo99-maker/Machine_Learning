# -*- coding: utf-8 -*-
# fct_machine_log_parser_mp_realtime.py
# ============================================
# FCT 머신 로그 파싱 + PostgreSQL 적재 (실시간/중복방지/오늘자만) - "파일 1개/일 append" 최적화
#
# 유지:
# - 1초마다 무한루프
# - 멀티프로세스 2개 고정
# - end_day = 오늘(현재 날짜)만 적재 (파일명 YYYYMMDD 기준)
# - (end_day, station, end_time, contents) 중복 방지 (UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - end_time은 hh:mm:ss.ss 문자열로 고정 저장(VARCHAR(12))
#
# 변경(요청 반영):
# - ✅ 120초(mtime) 조건 제거
# - ✅ 하루 1개 파일이 계속 append되는 구조이므로, "offset 기반 tail-follow"로 신규 라인만 처리
#   (파일 전체 재파싱/중복 insert 시도 폭발 방지)
# ============================================

import os
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
# 3) 요구사항: 1초마다 무한루프
# ============================================
SLEEP_SEC = 1

# ============================================
# 4) offset 기반 tail-follow 캐시
#    - path -> 마지막 읽은 파일 byte 위치
#    - (중요) 멀티프로세스에서는 전역 dict가 공유되지 않으므로,
#      워커에는 (path, offset)을 넘기고, (new_offset)을 다시 받아 메인에서 갱신한다.
# ============================================
PROCESSED_OFFSET = {}   # path(str) -> int(byte_offset)


# ============================================
# 5. DB 엔진 생성 & 스키마/테이블 생성 + UNIQUE INDEX
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
# 6. contents 정리 (공백 정리 + 75자리 제한)
# ============================================
def clean_contents(raw: str, max_len: int = 75) -> str:
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


# ============================================
# 7. 단일 로그 파일 파싱 (tail-follow)
#    - (path_str, offset) 입력
#    - 신규로 append된 라인만 읽어서 rows 생성
#    - (rows, new_offset, station, path_str) 반환
# ============================================
def _parse_machine_log_file_tail(args):
    """
    args = (path_str, offset)

    return dict:
      {
        "path": path_str,
        "station": "FCT1"~"FCT4",
        "new_offset": int,
        "rows": [ {end_day, station, end_time, contents}, ... ],
        "status": "OK" | "SKIP_NOT_TODAY" | "SKIP_BADNAME" | "SKIP_NOCHANGE" | "ERROR"
      }
    """
    path_str, offset = args
    path = Path(path_str)

    m = FILENAME_PATTERN.search(path.name)
    if not m:
        return {"path": path_str, "station": None, "new_offset": offset, "rows": [], "status": "SKIP_BADNAME"}

    file_ymd = m.group(1)           # YYYYMMDD (파일명 날짜)
    no = m.group(3)                 # '1'~'4'
    station = f"FCT{no}"            # PDI도 FCT로 취급

    today_ymd = datetime.now().strftime("%Y%m%d")
    if file_ymd != today_ymd:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": "SKIP_NOT_TODAY"}

    # 파일이 truncate(재생성/로테이션) 된 경우: offset이 파일 크기보다 크면 0으로 리셋
    try:
        size = path.stat().st_size
    except Exception:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": "ERROR"}

    if offset > size:
        offset = 0

    # offset == size면 읽을 게 없음
    if offset == size:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": "SKIP_NOCHANGE"}

    rows = []
    new_offset = offset

    try:
        with path.open("r", encoding="cp949", errors="ignore") as f:
            f.seek(offset, os.SEEK_SET)

            for line in f:
                line = line.rstrip("\n")
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                hms = mm.group(1)            # "13:16:34"
                frac = mm.group(2) or ""     # "12" or "" or "123456"
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

            new_offset = f.tell()

    except Exception as e:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": f"ERROR:{e}"}

    return {"path": path_str, "station": station, "new_offset": new_offset, "rows": rows, "status": "OK"}


# ============================================
# 8. 오늘 파일 목록 수집 (120초/mtime 조건 없음)
#    - 구조가 "하루 1개 파일"이므로, 오늘 날짜 파일을 찾아서 그 파일들을 tail-follow
# ============================================
def list_target_files_realtime():
    """
    오늘 로그 파일만 반환 (FCT1~4 하루 1개 고정)
    - (주의) BASE_DIR 하위 구조가 yyyy/mm 폴더에 파일이 존재하는 전제(기존 로직 유지)
    """
    files = []
    today_ymd = datetime.now().strftime("%Y%m%d")

    if not BASE_DIR.exists():
        print("[WARN] BASE_DIR not found:", BASE_DIR)
        return files

    for year_dir in BASE_DIR.iterdir():
        if not year_dir.is_dir() or not year_dir.name.isdigit() or len(year_dir.name) != 4:
            continue

        for month_dir in year_dir.iterdir():
            if not month_dir.is_dir() or not month_dir.name.isdigit() or len(month_dir.name) != 2:
                continue

            # ✅ 기존 구조 유지: month_dir 바로 아래 파일들만
            for file_path in month_dir.iterdir():
                if not file_path.is_file():
                    continue

                m = FILENAME_PATTERN.search(file_path.name)
                if not m:
                    continue

                file_ymd = m.group(1)
                if file_ymd != today_ymd:
                    continue

                files.append(str(file_path))

    return files


# ============================================
# 9. 멀티프로세스로 tail-follow 파싱
#    - (path, offset)을 워커에 넘기고
#    - (rows, new_offset)을 받아서 메인에서 PROCESSED_OFFSET 갱신
# ============================================
def collect_all_rows_multiprocess(file_list):
    """
    멀티프로세스로 파일별 파싱 후 station별로 묶어서 반환
    return:
      data_by_station: { "FCT1": [row...], ... }
      offset_updates : { path_str: new_offset, ... }
    """
    data_by_station = {st: [] for st in TABLE_BY_STATION.keys()}
    offset_updates = {}

    if not file_list:
        return data_by_station, offset_updates

    # 워커 입력: (path, offset)
    tasks = []
    for fp in file_list:
        tasks.append((fp, int(PROCESSED_OFFSET.get(fp, 0))))

    if USE_MULTIPROCESSING and len(tasks) >= 2:
        procs = min(N_PROCESSES, len(tasks))
        # 파일이 많지 않아도 요구사항대로 프로세스 2개 고정(가능 범위 내)
        if procs < N_PROCESSES:
            procs = procs  # 파일 수가 1이면 1개만 가능
        print(f"[INFO] Multiprocessing enabled: processes={procs}, chunksize={POOL_CHUNKSIZE}")

        with Pool(processes=procs) as pool:
            for out in pool.imap_unordered(_parse_machine_log_file_tail, tasks, chunksize=POOL_CHUNKSIZE):
                fp = out["path"]
                st = out["station"]
                offset_updates[fp] = out["new_offset"]

                if out["status"] == "OK" and out["rows"]:
                    if st in data_by_station:
                        data_by_station[st].extend(out["rows"])
    else:
        for t in tasks:
            out = _parse_machine_log_file_tail(t)
            fp = out["path"]
            st = out["station"]
            offset_updates[fp] = out["new_offset"]

            if out["status"] == "OK" and out["rows"]:
                if st in data_by_station:
                    data_by_station[st].extend(out["rows"])

    return data_by_station, offset_updates


# ============================================
# 10. DB INSERT
#     - 중복 회피: UNIQUE INDEX + ON CONFLICT DO NOTHING
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
# 11. main (1초 무한루프)
# ============================================
def main():
    engine = get_engine()
    ensure_schema_and_tables(engine)

    print("[INFO] Realtime mode: tail-follow (offset based), loop every 1s")
    print("[INFO] No mtime window filter (120s removed). Only today's files are tailed.")
    print(f"[INFO] BASE_DIR = {BASE_DIR}")

    while True:
        try:
            file_list = list_target_files_realtime()
            if not file_list:
                time_mod.sleep(SLEEP_SEC)
                continue

            data_by_station, offset_updates = collect_all_rows_multiprocess(file_list)

            # ✅ offset 갱신은 반드시 "메인 프로세스"에서 수행
            for fp, new_off in offset_updates.items():
                PROCESSED_OFFSET[fp] = int(new_off)

            # 신규 라인만 들어왔으므로, 신규 row가 있는 station만 insert 시도
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
