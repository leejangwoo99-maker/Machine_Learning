# -*- coding: utf-8 -*-
"""
a2_fct_table_parser_mp.py (Nuitka/Windows multiprocessing 안정화 통합본)

요구사항 요약
- RAW_LOG 경로:
  \\192.168.108.101\HistoryLog\TC6\yyyymmdd\{GoodFile,BadFile}\*.txt
  \\192.168.108.101\HistoryLog\TC7\yyyymmdd\{GoodFile,BadFile}\*.txt
  \\192.168.108.101\HistoryLog\TC8\yyyymmdd\{GoodFile,BadFile}\*.txt
  \\192.168.108.101\HistoryLog\TC9\yyyymmdd\{GoodFile,BadFile}\*.txt

- 날짜 제한: 20251001 ~ 20251220 (폴더명 yyyymmdd 기준)
- TC6->FCT1, TC7->FCT2, TC8->FCT3, TC9->FCT4  => station 컬럼 값
- 파일명 파싱:
  * barcode_information = 파일명 첫 '_' 전까지
  * end_day  = 첫번째 '_' 다음 14자리(yyyymmddhhmiss) 중 yyyymmdd
  * end_time = 첫번째 '_' 다음 14자리 중 hhmiss
  * remark = barcode_information 18번째 문자(1-indexed)가 'J' 또는 'S'이면 'PD', 아니면 'Non-PD'

- 파일 내용 파싱:
  * run_time: 14번째 줄에서 Run Time :xx.x 형태를 찾아 TEXT로 저장(없으면 전체 탐색)
  * 19번째 줄부터:
    - step_description: 첫 필드(콤마 전)에서 41자리까지
    - value/min/max: 두번째/세번째/네번째 필드
    - result: 다섯번째 필드에서 [] 제거, PASS/FAIL

- DB 저장:
  * schema: a2_fct_table
  * table : fct_table
  * columns:
    [barcode_information, remark, station, end_day, end_time, run_time,
     step_description, value, min, max, result, file_path]

- 로그:
  * 파싱 절차/정보, 시작시간/종료시간 출력
  * 진행 로그/flush 구조 유지

- 멀티프로세스:
  * Windows/Nuitka 안정화: spawn 컨텍스트 고정 + 결과 반환을 tuple로 경량화
  * chunksize 상향(파이프 통신 빈도 감소)
  * 워커 상한(UNC + 대용량 I/O 환경 안정화)
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import multiprocessing as mp  # ✅ spawn 컨텍스트 고정용
import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 사용자 설정
# =========================

BASE_DIR = Path(r"\\192.168.108.101\HistoryLog")

TC_TO_STATION = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

TARGET_SUBFOLDERS = ["GoodFile", "BadFile"]

# 폴더(yyyymmdd) 기준 제한
START_DAY = "20251001"
END_DAY   = "20251220"

# DB 설정
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "a2_fct_table"
TABLE_NAME  = "fct_table"

# 멀티프로세스 설정
USE_MULTIPROCESSING = True

# ✅ 워커 상한 (UNC + 대용량 I/O 환경 안정화)
MAX_WORKERS = min(8, max(1, (os.cpu_count() or 2) - 1))

# ✅ chunksize 상향 (파이프 통신 빈도 감소)
POOL_CHUNKSIZE = 2000

# INSERT 청크 설정
MODE = "stable"  # "basic" 또는 "stable"
FLUSH_ROWS_BASIC = 50_000
FLUSH_ROWS_STABLE = 200_000

# execute_values page_size (한 번의 statement 내 values 분할)
EXECUTE_VALUES_PAGE_SIZE = 10_000

# 수집 진행 로그 heartbeat
COLLECT_HEARTBEAT_EVERY = 5000


# =========================
# 1) 로깅/유틸
# =========================

def log(msg: str) -> None:
    print(msg, flush=True)


def in_day_range(day: str, start_day: str, end_day: str) -> bool:
    return start_day <= day <= end_day


def safe_read_text(file_path: Path) -> Optional[List[str]]:
    try:
        for enc in ("utf-8", "cp949", "euc-kr", "latin-1"):
            try:
                return file_path.read_text(encoding=enc, errors="strict").splitlines()
            except UnicodeDecodeError:
                continue
        return file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return None


def iter_days(start_yyyymmdd: str, end_yyyymmdd: str):
    """start~end 날짜를 YYYYMMDD 문자열로 순차 생성"""
    s = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    e = datetime.strptime(end_yyyymmdd, "%Y%m%d").date()
    d = s
    while d <= e:
        yield d.strftime("%Y%m%d")
        d += timedelta(days=1)


# =========================
# 2) 파일명 파싱
# =========================

def parse_filename(file_path: Path) -> Optional[Tuple[str, str, str, str]]:
    name = file_path.name
    if "_" not in name:
        return None

    parts = name.split("_")
    if len(parts) < 2:
        return None

    barcode_information = parts[0].strip()
    dt14 = parts[1].strip()
    if not re.fullmatch(r"\d{14}", dt14):
        return None

    end_day = dt14[:8]
    end_time = dt14[8:]

    remark = "Non-PD"
    if len(barcode_information) >= 18:
        ch = barcode_information[17]  # 18번째(1-indexed)
        if ch in ("J", "S"):
            remark = "PD"

    return barcode_information, end_day, end_time, remark


# =========================
# 3) 파일 내용 파싱
# =========================

_RUN_TIME_RE = re.compile(r"Run\s*Time\s*:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)

def parse_run_time(lines: List[str]) -> str:
    # 14번째 줄(1-indexed) => index 13
    if len(lines) >= 14:
        m = _RUN_TIME_RE.search(lines[13])
        if m:
            return m.group(1).strip()

    # fallback: 전체 탐색
    for ln in lines:
        m = _RUN_TIME_RE.search(ln)
        if m:
            return m.group(1).strip()

    return ""


def parse_test_lines(lines: List[str]) -> List[Tuple[str, str, str, str, str]]:
    out: List[Tuple[str, str, str, str, str]] = []
    if len(lines) < 19:
        return out

    # 19번째 줄(1-indexed)부터 => index 18부터
    for raw in lines[18:]:
        if not raw.strip():
            continue
        cols = [c.strip() for c in raw.split(",")]
        if len(cols) < 5:
            continue

        step = cols[0].strip()[:41]
        value = cols[1].strip()
        min_v = cols[2].strip()
        max_v = cols[3].strip()

        result = cols[4].strip().replace("[", "").replace("]", "").strip()
        if not step:
            continue
        if result not in ("PASS", "FAIL"):
            result = re.sub(r"\s+", "", result)

        out.append((step, value, min_v, max_v, result))

    return out


# =========================
# 4) 파일 1개 파싱 (✅ 반환을 tuple로 경량화)
# =========================
# DB insert tuple 구조:
# (barcode_information, remark, station, end_day, end_time, run_time,
#  step_description, value, min, max, result, file_path)

RowTuple = Tuple[str, str, str, str, str, str, str, str, str, str, str, str]

def parse_one_file(args: Tuple[str, str]) -> List[RowTuple]:
    file_path_str, station = args
    fp = Path(file_path_str)

    info = parse_filename(fp)
    if info is None:
        return []
    barcode_information, end_day, end_time, remark = info

    if not in_day_range(end_day, START_DAY, END_DAY):
        return []

    lines = safe_read_text(fp)
    if lines is None:
        return []

    run_time = parse_run_time(lines)
    tests = parse_test_lines(lines)
    if not tests:
        return []

    out: List[RowTuple] = []
    for (step_description, value, min_v, max_v, result) in tests:
        out.append((
            barcode_information,
            remark,
            station,
            end_day,
            end_time,
            run_time,
            step_description,
            value,
            min_v,   # -> DB column "min"
            max_v,   # -> DB column "max"
            result,
            str(fp),
        ))
    return out


# =========================
# 5) 파일 수집 (전수 iterdir 제거 + 진행 로그 유지)
# =========================

def collect_target_files() -> List[Tuple[str, str]]:
    """
    return [(file_path_str, station), ...]
    핵심:
      - tc_dir.iterdir()로 전체 날짜 폴더 열거하지 않음
      - START_DAY~END_DAY 날짜를 직접 생성해서 해당 폴더만 접근
      - 진행 로그를 날짜/폴더 단위로 출력
    """
    jobs: List[Tuple[str, str]] = []

    total_days = 0
    total_dirs = 0
    total_files = 0

    for tc, station in TC_TO_STATION.items():
        tc_dir = BASE_DIR / tc
        if not tc_dir.exists():
            log(f"[WARN] Not found: {tc_dir}")
            continue

        log(f"[COLLECT] TC={tc} start ...")

        for day in iter_days(START_DAY, END_DAY):
            total_days += 1
            day_dir = tc_dir / day
            if not day_dir.exists():
                continue

            for sub in TARGET_SUBFOLDERS:
                sub_dir = day_dir / sub
                if not sub_dir.exists():
                    continue

                total_dirs += 1
                log(f"[COLLECT] scanning: {tc}\\{day}\\{sub}")

                local_count = 0
                for fp in sub_dir.glob("*.txt"):
                    # UNC에서는 is_file()이 stat를 유발할 수 있으나 안정성 위해 유지
                    if fp.is_file():
                        jobs.append((str(fp), station))
                        local_count += 1
                        if local_count % COLLECT_HEARTBEAT_EVERY == 0:
                            log(f"[COLLECT]   ... {tc}\\{day}\\{sub} files={local_count:,}")

                total_files += local_count

    log(f"[COLLECT] done. days_checked={total_days:,} dirs_scanned={total_dirs:,} files_found={total_files:,}")
    return jobs


# =========================
# 6) DB (DDL + INSERT)
# =========================

DDL_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
    barcode_information TEXT,
    remark              TEXT,
    station             TEXT,
    end_day             TEXT,
    end_time            TEXT,
    run_time            TEXT,
    step_description    TEXT,
    value               TEXT,
    min                 TEXT,
    max                 TEXT,
    result              TEXT,
    file_path           TEXT
);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_end_day
    ON {SCHEMA_NAME}.{TABLE_NAME} (end_day);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_station_end_day
    ON {SCHEMA_NAME}.{TABLE_NAME} (station, end_day);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_barcode
    ON {SCHEMA_NAME}.{TABLE_NAME} (barcode_information);
"""

INSERT_COLS = (
    "barcode_information, remark, station, end_day, end_time, run_time, "
    "step_description, value, min, max, result, file_path"
)

INSERT_SQL = f"""
INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} ({INSERT_COLS})
VALUES %s
"""

def get_conn():
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        dbname=DB_CONFIG["dbname"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )

def ensure_schema_table() -> None:
    with get_conn() as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(DDL_SQL)

def flush_rows(cur, buffer_rows: List[RowTuple]) -> int:
    if not buffer_rows:
        return 0
    execute_values(
        cur,
        INSERT_SQL,
        buffer_rows,  # ✅ 이미 tuple list
        page_size=EXECUTE_VALUES_PAGE_SIZE,
    )
    return len(buffer_rows)


# =========================
# 7) 메인 실행 (진행 로그/flush 구조 유지)
# =========================

def main() -> int:
    job_name = "a2_fct_table_parser_mp"
    start_dt = datetime.now()
    start_ts = time.perf_counter()

    flush_rows_threshold = FLUSH_ROWS_STABLE if MODE.lower() == "stable" else FLUSH_ROWS_BASIC

    log("============================================================")
    log(f"[START] {job_name} | {start_dt:%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] BASE_DIR = {BASE_DIR}")
    log(f"[INFO] DATE RANGE (folder/file) = {START_DAY} ~ {END_DAY}")
    log(f"[INFO] MODE = {MODE} | flush_threshold={flush_rows_threshold:,}")
    log(f"[INFO] MULTIPROCESSING = {USE_MULTIPROCESSING} | workers={MAX_WORKERS if USE_MULTIPROCESSING else 1}")
    log(f"[INFO] POOL_CHUNKSIZE = {POOL_CHUNKSIZE if USE_MULTIPROCESSING else 'N/A'}")
    log(f"[INFO] TARGET = {SCHEMA_NAME}.{TABLE_NAME}")
    log("============================================================")

    log("[STEP] 1) Collect target txt files ...")
    jobs = collect_target_files()
    log(f"[INFO] candidates(files) = {len(jobs):,}")

    if not jobs:
        log("[DONE] No files to process.")
        return 0

    log("[STEP] 2) Ensure schema/table/index ...")
    ensure_schema_table()
    log("[OK] Schema/table ready.")

    log("[STEP] 3) Parse + Chunk Insert ...")
    total_files = 0
    total_rows = 0
    buffer: List[RowTuple] = []

    with get_conn() as conn:
        with conn.cursor() as cur:
            if USE_MULTIPROCESSING:
                # ✅ Windows/Nuitka 안정화: spawn 컨텍스트 고정
                ctx = mp.get_context("spawn")
                with ctx.Pool(processes=MAX_WORKERS) as pool:
                    for rows in pool.imap_unordered(parse_one_file, jobs, chunksize=POOL_CHUNKSIZE):
                        total_files += 1
                        if rows:
                            buffer.extend(rows)

                        if len(buffer) >= flush_rows_threshold:
                            inserted = flush_rows(cur, buffer)
                            conn.commit()
                            total_rows += inserted
                            log(f"[FLUSH] inserted={inserted:,} | total_rows={total_rows:,} | processed_files={total_files:,}/{len(jobs):,}")
                            buffer.clear()
            else:
                for j in jobs:
                    rows = parse_one_file(j)
                    total_files += 1
                    if rows:
                        buffer.extend(rows)

                    if len(buffer) >= flush_rows_threshold:
                        inserted = flush_rows(cur, buffer)
                        conn.commit()
                        total_rows += inserted
                        log(f"[FLUSH] inserted={inserted:,} | total_rows={total_rows:,} | processed_files={total_files:,}/{len(jobs):,}")
                        buffer.clear()

            if buffer:
                inserted = flush_rows(cur, buffer)
                conn.commit()
                total_rows += inserted
                log(f"[FLUSH-LAST] inserted={inserted:,} | total_rows={total_rows:,} | processed_files={total_files:,}/{len(jobs):,}")
                buffer.clear()

    end_dt = datetime.now()
    elapsed = time.perf_counter() - start_ts

    log("============================================================")
    log(f"[END] {job_name} | {end_dt:%Y-%m-%d %H:%M:%S}")
    log(f"[RESULT] files_processed={total_files:,} / candidates={len(jobs):,}")
    log(f"[RESULT] rows_inserted={total_rows:,}")
    log(f"[ELAPSED] {elapsed:,.2f} sec")
    log("============================================================")
    return 0


# =========================
# 8) 엔트리포인트 (EXE 콘솔 자동 종료 방지: 강제 홀드)
# =========================

def hold_console(exit_code: int) -> None:
    """
    exe 실행 시 콘솔이 자동으로 닫히는 것을 방지하기 위한 강제 홀드.
    - 파이프 실행/리다이렉션 등으로 stdin이 없으면 EOFError가 날 수 있어 예외 처리.
    - Windows에서 exe로 더블클릭 실행했을 때 특히 유효.
    """
    try:
        print("\n" + "=" * 60)
        print(f"[HOLD] 종료 코드(exit_code) = {exit_code}")
        print("[HOLD] 콘솔을 닫으려면 Enter를 누르세요...")
        print("=" * 60)
        input()
    except EOFError:
        time.sleep(5)
    except Exception:
        try:
            time.sleep(5)
        except Exception:
            pass


if __name__ == "__main__":
    mp.freeze_support()  # ✅ Nuitka/Windows spawn 부트스트랩

    exit_code = 0
    try:
        exit_code = main()
    except KeyboardInterrupt:
        print("\n[ABORT] 사용자 중단(CTRL+C)")
        exit_code = 130
    except Exception as e:
        print("\n[ERROR] Unhandled exception:", repr(e))
        exit_code = 1
    finally:
        hold_console(exit_code)

    raise SystemExit(exit_code)
