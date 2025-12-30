# -*- coding: utf-8 -*-
# ============================================
# d2_Vision_machine_log_factory.py
# Vision Machine Log Parser (Realtime + Thread2 + TodayOnly) - OPTION A (DB가 중복 영구 차단)
#
# [기존 기능 유지]
# - 오늘 파일만 파싱
# - 스레드 2개 고정
# - INSERT는 ON CONFLICT DO NOTHING => 중복은 무조건 "조용히 무시"
# - 어떤 상황에서도 프로세스가 멈추지 않도록(예외는 로그만 찍고 continue)
# - INIT(스키마/테이블) 실패 시 재시도
# - 날짜 변경 시 캐시 초기화
# - loop pacing (SLEEP_SEC - elapsed)
# - LOG_EVERY_LOOP 주기적 요약 로그
#
# [요청 반영 변경]
# - ❌ 120초(mtime) 조건 제거
# - ✅ 하루 1개 파일이 계속 append 되는 구조이므로,
#      "offset 기반 tail-follow"로 신규 라인만 처리 (파일 전체 재파싱 제거)
# ============================================

import re
import sys
import os
import time as time_mod
from pathlib import Path
from datetime import datetime
from multiprocessing import freeze_support
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import urllib.parse


# ============================================
# 1) 기본 설정
# ============================================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log")

DB_CONFIG = {
    "host": "192.168.108.162",
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

# 스레드 2개 고정
THREAD_WORKERS = 2

# 1초 루프
SLEEP_SEC = 1

# ✅ offset 캐시: path -> 마지막 읽은 파일 byte 위치
PROCESSED_OFFSET = {}

# 콘솔 로그 주기(너무 많은 출력 방지)
LOG_EVERY_LOOP = 10  # 10회(=약 10초)마다 한번 요약 로그


# ============================================
# 2) 공용 로그/콘솔 유지
# ============================================
def log(msg: str):
    print(msg, flush=True)


def pause_console():
    try:
        input("\n[END] 작업이 종료되었습니다. 콘솔을 닫으려면 Enter를 누르세요...")
    except Exception:
        pass


# ============================================
# 3) DB 엔진
# ============================================
def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]

    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        "?connect_timeout=5"
    )
    log(f"[INFO] DB={host}:{port}/{dbname}")

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=3,
        pool_recycle=300,
    )


def ensure_schema_tables(engine):
    """
    - 스키마/테이블만 보장
    - UNIQUE INDEX는 사용자가 이미 만들었으므로 여기서는 만들지 않음
    """
    ddl_template = """
    CREATE TABLE IF NOT EXISTS {schema}."{table}" (
        end_day     VARCHAR(8),     -- yyyymmdd (파일명 기준)
        station     VARCHAR(10),    -- Vision1 / Vision2
        end_time    VARCHAR(12),    -- hh:mi:ss.xx (문자열)
        contents    VARCHAR(200)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
        for _, tbl in TABLE_MAP.items():
            conn.execute(text(ddl_template.format(schema=SCHEMA_NAME, table=tbl)))

    log("[INFO] Schema/tables ready.")


# ============================================
# 4) 파일 파싱 유틸
# ============================================
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$")
FILENAME_PATTERN = re.compile(r"(\d{8})_Vision([12])_Machine_Log", re.IGNORECASE)

YEAR_RE = re.compile(r"^\d{4}$")
MONTH_RE = re.compile(r"^\d{2}$")


def clean_contents(raw: str, max_len: int = 200):
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


def _open_text_file(path: Path):
    try:
        return path.open("r", encoding="utf-8", errors="ignore")
    except Exception:
        return path.open("r", encoding="cp949", errors="ignore")


# ============================================
# 5) 오늘 파일 목록 수집 (✅ 120초/mtime 조건 제거)
#    - 기존의 year/month 폴더 구조 순회/검증/정렬 방식 유지
# ============================================
def list_target_files_today(base_dir: Path, today_ymd: str):
    files = []
    if not base_dir.exists():
        log(f"[WARN] BASE_DIR not found: {base_dir}")
        return files

    for year_dir in sorted(base_dir.iterdir()):
        if not (year_dir.is_dir() and YEAR_RE.match(year_dir.name)):
            continue

        for month_dir in sorted(year_dir.iterdir()):
            if not (month_dir.is_dir() and MONTH_RE.match(month_dir.name)):
                continue

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
# 6) Tail-follow 파서 (offset 기반)
#    - (path_str, today_ymd, offset) -> (rows, new_offset, station, path_str, status)
#    - 기존 parse_machine_log_file()의 기능을 유지하되,
#      파일 전체가 아니라 offset 이후 "신규 라인만" 읽는다.
# ============================================
def parse_machine_log_file_tail(path_str: str, today_ymd: str, offset: int):
    """
    return:
      rows(list[dict]), new_offset(int), station(str|None), path_str(str), status(str)
    """
    file_path = Path(path_str)

    m = FILENAME_PATTERN.search(file_path.name)
    if not m:
        return [], offset, None, path_str, "SKIP_BADNAME"

    file_ymd = m.group(1)
    vision_no = m.group(2)
    station = f"Vision{vision_no}"

    if file_ymd != today_ymd:
        return [], offset, station, path_str, "SKIP_NOT_TODAY"

    # 파일이 truncate(재생성/로테이션) 된 경우 보호: offset > size면 0 리셋
    try:
        size = file_path.stat().st_size
    except Exception as e:
        return [], offset, station, path_str, f"ERROR_STAT:{type(e).__name__}"

    if offset > size:
        offset = 0

    # 읽을 게 없으면 스킵
    if offset == size:
        return [], offset, station, path_str, "SKIP_NOCHANGE"

    result = []
    new_offset = offset

    try:
        with _open_text_file(file_path) as f:
            # 텍스트 모드에서도 seek/tell 동작(바이트 기반)하도록 offset을 그대로 사용
            f.seek(offset, os.SEEK_SET)

            for line in f:
                line = line.rstrip("\n")
                m2 = LINE_PATTERN.match(line)
                if not m2:
                    continue

                end_time_str = m2.group(1)
                contents_raw = m2.group(2)

                # 시간 포맷 검증(완화)
                try:
                    _ = datetime.strptime(end_time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                contents = clean_contents(contents_raw, max_len=200)

                result.append(
                    {
                        "end_day": file_ymd,
                        "station": station,
                        "end_time": end_time_str,
                        "contents": contents,
                    }
                )

            new_offset = f.tell()

    except Exception as e:
        log(f"[WARN] Failed to read(tail): {file_path} / {type(e).__name__}: {e}")
        return [], offset, station, path_str, f"ERROR_READ:{type(e).__name__}"

    return result, int(new_offset), station, path_str, "OK"


# ============================================
# 7) DB Insert (중복은 무조건 무시) - 기존 유지
# ============================================
def insert_to_db(engine, df: pd.DataFrame):
    if df is None or df.empty:
        return 0

    df = df.sort_values(["end_day", "end_time"]).reset_index(drop=True)

    inserted_attempt = 0
    with engine.begin() as conn:
        for st, tbl in TABLE_MAP.items():
            sub = df[df["station"] == st]
            if sub.empty:
                continue

            insert_sql = text(f"""
                INSERT INTO {SCHEMA_NAME}."{tbl}" (end_day, station, end_time, contents)
                VALUES (:end_day, :station, :end_time, :contents)
                ON CONFLICT (end_day, station, end_time, contents)
                DO NOTHING
            """)
            records = sub.to_dict(orient="records")
            conn.execute(insert_sql, records)

            inserted_attempt += len(records)

    return inserted_attempt


# ============================================
# 8) main loop - 기존 구조 유지(요약로그/INIT재시도/예외정책/페이싱)
# ============================================
def main():
    log("### BUILD: d2 Vision parser OPTION-A (TAIL-FOLLOW) v2025-12-30-01 ###")

    engine = get_engine()

    # INIT(스키마/테이블) - 실패해도 재시도 (기존 유지)
    while True:
        try:
            ensure_schema_tables(engine)
            break
        except Exception as e:
            log(f"[FATAL-INIT] ensure_schema_tables failed: {e}")
            time_mod.sleep(3)

    log("=" * 78)
    log("[START] d2 Vision machine log realtime parser")
    log(f"[INFO] BASE_DIR={BASE_DIR}")
    log(f"[INFO] THREADS={THREAD_WORKERS} | SLEEP={SLEEP_SEC}s")
    log("[INFO] DEDUP POLICY = DB UNIQUE INDEX + ON CONFLICT DO NOTHING")
    log("[INFO] FILE POLICY = TodayOnly + Tail-Follow(offset) | (mtime window removed)")
    log("=" * 78)

    loop_i = 0
    last_day = datetime.now().strftime("%Y%m%d")

    while True:
        loop_i += 1
        loop_t0 = time_mod.perf_counter()

        try:
            today_ymd = datetime.now().strftime("%Y%m%d")

            # 날짜 변경 시: 캐시 초기화(오늘만 보니까 과감히 리셋) - 기존 유지
            if today_ymd != last_day:
                log(f"[INFO] Day changed {last_day} -> {today_ymd} | reset processed cache(offset)")
                last_day = today_ymd
                PROCESSED_OFFSET.clear()

            files = list_target_files_today(BASE_DIR, today_ymd=today_ymd)
            if not files:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(f"[LOOP] no files (today={today_ymd})")
                time_mod.sleep(SLEEP_SEC)
                continue

            # 파싱(tail-follow) - 스레드 2개 고정 유지 + as_completed 유지
            all_records = []
            offset_updates = {}

            if len(files) >= 2:
                workers = min(THREAD_WORKERS, len(files))
                with ThreadPoolExecutor(max_workers=workers) as ex:
                    futs = []
                    for fp in files:
                        off = int(PROCESSED_OFFSET.get(fp, 0))
                        futs.append(ex.submit(parse_machine_log_file_tail, fp, today_ymd, off))

                    for f in as_completed(futs):
                        rows, new_off, station, fp, status = f.result()
                        offset_updates[fp] = int(new_off)
                        if rows:
                            all_records.extend(rows)
            else:
                fp = files[0]
                off = int(PROCESSED_OFFSET.get(fp, 0))
                rows, new_off, station, fp, status = parse_machine_log_file_tail(fp, today_ymd, off)
                offset_updates[fp] = int(new_off)
                if rows:
                    all_records.extend(rows)

            # ✅ offset 갱신(항상 메인에서)
            for fp, new_off in offset_updates.items():
                PROCESSED_OFFSET[fp] = int(new_off)

            if not all_records:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(f"[LOOP] parsed=0 | files={len(files)} | (tail no new lines)")
                # pacing below
            else:
                df = pd.DataFrame(all_records)

                # DB insert (중복 무시) - 예외 정책 유지
                try:
                    attempted = insert_to_db(engine, df)
                    if loop_i % LOG_EVERY_LOOP == 0 or attempted > 0:
                        log(f"[DB] attempted={attempted:,} | files={len(files)} | parsed={len(df):,}")
                except SQLAlchemyError as e:
                    log(f"[WARN] DB insert error (ignored, continue): {type(e).__name__}: {e}")

        except KeyboardInterrupt:
            log("\n[STOP] Interrupted by user.")
            break
        except Exception as e:
            log(f"[ERROR] Loop error (continue): {type(e).__name__}: {e}")

        # loop pacing - 기존 유지
        elapsed = time_mod.perf_counter() - loop_t0
        time_mod.sleep(max(0.0, SLEEP_SEC - elapsed))


if __name__ == "__main__":
    freeze_support()
    try:
        main()
    except KeyboardInterrupt:
        log("\n[STOP] 사용자 중단(Ctrl+C).")
        pause_console()
    except Exception as e:
        log("\n[UNHANDLED] 치명 오류가 발생했습니다.")
        log(f"  - {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        pause_console()
        sys.exit(1)
