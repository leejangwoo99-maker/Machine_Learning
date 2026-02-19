# -*- coding: utf-8 -*-
# """
# a2_fct_table_parser_mp.py
#
# 요구사항 요약
# - RAW_LOG 경로:
#   C:\Users\user\Desktop\RAW_LOG\TC6\yyyymmdd\{GoodFile,BadFile}\*.txt
#   C:\Users\user\Desktop\RAW_LOG\TC7\yyyymmdd\{GoodFile,BadFile}\*.txt
#   C:\Users\user\Desktop\RAW_LOG\TC8\yyyymmdd\{GoodFile,BadFile}\*.txt
#   C:\Users\user\Desktop\RAW_LOG\TC9\yyyymmdd\{GoodFile,BadFile}\*.txt
#
# - 날짜 제한: 20251001 ~ 20251220 (폴더명 yyyymmdd 기준)
# - TC6->FCT1, TC7->FCT2, TC8->FCT3, TC9->FCT4  => station 컬럼 값
# - 파일명 파싱:
#   barcode_information = 파일명 첫 '_' 전까지
#   end_day  = 첫번째 '_' 다음 14자리(yyyymmddhhmiss) 중 yyyymmdd
#   end_time = 첫번째 '_' 다음 14자리 중 hhmiss
#   remark = barcode_information 18번째 문자(1-indexed)가 'J' 또는 'S'이면 'PD', 아니면 'Non-PD'
#
# - 파일 내용 파싱:
#   run_time: "14번째 줄"에서 Run Time :xx.x 형태를 찾아 TEXT로 저장
#   19번째 줄부터:
#     step_description: 첫 필드(콤마 전)에서 41자리까지
#     value/min/max: 두번째/세번째/네번째 필드
#     result: 다섯번째 필드에서 [] 제거, PASS/FAIL
#
# - DB 저장:
#   DB_CONFIG (localhost)
#   schema: a2_fct_table
#   table : fct_table
#   columns:
#     [barcode_information, remark, station, end_day, end_time, run_time,
#      step_description, value, min, max, result, file_path]
#
# - 로그:
#   파싱 절차/정보, 시작시간/종료시간 출력
# - 멀티프로세스: 최대
# - 대용량 안정 버전: "청크 단위 즉시 INSERT (예: 200,000 rows마다 flush)" 지원
# """

from __future__ import annotations

import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from multiprocessing import Pool, cpu_count, freeze_support
from typing import Iterable, List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 사용자 설정
# =========================

BASE_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")

TC_TO_STATION = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

TARGET_SUBFOLDERS = ["GoodFile", "BadFile"]  # 둘 다 타겟

# 폴더(yyyymmdd) 기준 제한
START_DAY = "20251001"
END_DAY   = "20251220"

# DB 설정
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SCHEMA_NAME = "a2_fct_table"
TABLE_NAME  = "fct_table"

# 멀티프로세스 설정
USE_MULTIPROCESSING = True
MAX_WORKERS = max(1, cpu_count() - 1)  # "최대" 의미로 CPU-1 사용 (필요 시 100%로 바꾸려면 cpu_count())

# INSERT 청크 설정
# - 기본 모드: 자주 flush (메모리 절감, 다만 DB round-trip 증가)
# - 대용량 안정 모드: 200,000 rows마다 flush (요청 사양)
MODE = "stable"  # "basic" 또는 "stable"
FLUSH_ROWS_BASIC = 50_000
FLUSH_ROWS_STABLE = 200_000

# execute_values page_size (한 번의 statement 내 values 분할)
EXECUTE_VALUES_PAGE_SIZE = 10_000


# =========================
# 1) 유틸
# =========================

@dataclass(frozen=True)
class ParsedRow:
    barcode_information: str
    remark: str
    station: str
    end_day: str
    end_time: str
    run_time: str
    step_description: str
    value: str
    min_v: str
    max_v: str
    result: str
    file_path: str

    def as_tuple(self) -> Tuple[str, str, str, str, str, str, str, str, str, str, str, str]:
        return (
            self.barcode_information,
            self.remark,
            self.station,
            self.end_day,
            self.end_time,
            self.run_time,
            self.step_description,
            self.value,
            self.min_v,
            self.max_v,
            self.result,
            self.file_path,
        )


def log(msg: str) -> None:
    print(msg, flush=True)


def is_yyyymmdd(s: str) -> bool:
    return bool(re.fullmatch(r"\d{8}", s))


def in_day_range(day: str, start_day: str, end_day: str) -> bool:
    # day, start_day, end_day are "YYYYMMDD"
    return start_day <= day <= end_day


def safe_read_text(file_path: Path) -> Optional[List[str]]:
    try:
        # ANSI/UTF-8 혼재 가능성 고려
        for enc in ("utf-8", "cp949", "euc-kr", "latin-1"):
            try:
                return file_path.read_text(encoding=enc, errors="strict").splitlines()
            except UnicodeDecodeError:
                continue
        # 마지막 fallback
        return file_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    except Exception:
        return None


# =========================
# 2) 파일명 파싱
# =========================

def parse_filename(file_path: Path, station: str) -> Optional[Tuple[str, str, str, str]]:
    """
    return (barcode_information, end_day, end_time, remark)
    """
    name = file_path.name
    if "_" not in name:
        return None

    parts = name.split("_")
    if len(parts) < 2:
        return None

    barcode_information = parts[0].strip()
    dt14 = parts[1].strip()  # yyyymmddhhmiss 기대
    if not re.fullmatch(r"\d{14}", dt14):
        return None

    end_day = dt14[:8]
    end_time = dt14[8:]  # hhmiss

    # remark: barcode 18번째(1-indexed) => python index 17
    remark = "Non-PD"
    if len(barcode_information) >= 18:
        ch = barcode_information[17]
        if ch in ("J", "S"):
            remark = "PD"

    return barcode_information, end_day, end_time, remark


# =========================
# 3) 파일 내용 파싱
# =========================

_RUN_TIME_RE = re.compile(r"Run\s*Time\s*:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)

def parse_run_time(lines: List[str]) -> str:
    """
    요구: 14줄에 숫자 데이터 -> TEXT로 run_time 저장
    다만 실제 포맷 변동 대비해서:
      - 14번째 줄(index 13)에서 우선 검색
      - 실패 시 전체 라인에서 'Run Time : 숫자' 패턴 검색
    """
    # 14번째 줄 우선
    if len(lines) >= 14:
        m = _RUN_TIME_RE.search(lines[13])
        if m:
            return m.group(1).strip()

    # fallback: 전체 검색
    for ln in lines:
        m = _RUN_TIME_RE.search(ln)
        if m:
            return m.group(1).strip()

    return ""  # 못 찾으면 빈 문자열


def parse_test_lines(lines: List[str]) -> List[Tuple[str, str, str, str, str]]:
    """
    19번째 줄부터(index 18) 대입
    각 라인 예:
      1.01 Test Input Voltage(V)              ,           14.67,           14.60,           14.80,          [PASS]
    return list of (step_description, value, min_v, max_v, result)
    """
    out: List[Tuple[str, str, str, str, str]] = []

    if len(lines) < 19:
        return out

    for raw in lines[18:]:
        if not raw.strip():
            continue
        # 콤마 분리
        cols = [c.strip() for c in raw.split(",")]
        if len(cols) < 5:
            continue

        step = cols[0].strip()
        step = step[:41]  # 41자리 까지

        value = cols[1].strip()
        min_v = cols[2].strip()
        max_v = cols[3].strip()

        result = cols[4].strip()
        # [] 제거
        result = result.replace("[", "").replace("]", "").strip()

        # 지나치게 이상한 값 방지(원하면 제거 가능)
        if not step:
            continue
        if result not in ("PASS", "FAIL"):
            # 그래도 저장은 하되, 공백/문자 섞여 있으면 정리
            result = re.sub(r"\s+", "", result)

        out.append((step, value, min_v, max_v, result))

    return out


def parse_one_file(args: Tuple[str, str]) -> List[ParsedRow]:
    """
    Worker에서 실행: 파일 1개 -> rows N개
    args = (file_path_str, station)
    """
    file_path_str, station = args
    fp = Path(file_path_str)

    info = parse_filename(fp, station)
    if info is None:
        return []
    barcode_information, end_day, end_time, remark = info

    # 날짜 제한: 파일명에서 뽑은 end_day 기준으로도 한 번 더 필터링
    if not in_day_range(end_day, START_DAY, END_DAY):
        return []

    lines = safe_read_text(fp)
    if lines is None:
        return []

    run_time = parse_run_time(lines)
    tests = parse_test_lines(lines)
    if not tests:
        return []

    rows: List[ParsedRow] = []
    for (step_description, value, min_v, max_v, result) in tests:
        rows.append(
            ParsedRow(
                barcode_information=barcode_information,
                remark=remark,
                station=station,
                end_day=end_day,
                end_time=end_time,
                run_time=run_time,
                step_description=step_description,
                value=value,
                min_v=min_v,
                max_v=max_v,
                result=result,
                file_path=str(fp),
            )
        )
    return rows


# =========================
# 4) 파일 수집
# =========================

def collect_target_files() -> List[Tuple[str, str]]:
    """
    return [(file_path_str, station), ...]
    """
    jobs: List[Tuple[str, str]] = []

    for tc, station in TC_TO_STATION.items():
        tc_dir = BASE_DIR / tc
        if not tc_dir.exists():
            log(f"[WARN] Not found: {tc_dir}")
            continue

        # yyyymmdd 폴더 스캔
        for day_dir in tc_dir.iterdir():
            if not day_dir.is_dir():
                continue
            day = day_dir.name
            if not is_yyyymmdd(day):
                continue
            if not in_day_range(day, START_DAY, END_DAY):
                continue

            for sub in TARGET_SUBFOLDERS:
                sub_dir = day_dir / sub
                if not sub_dir.exists():
                    continue
                for fp in sub_dir.glob("*.txt"):
                    if fp.is_file():
                        jobs.append((str(fp), station))

    return jobs


# =========================
# 5) DB (DDL + INSERT)
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

-- 성능/운영 편의 인덱스(대용량에서 효과 큼)
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


def flush_rows(cur, buffer_rows: List[ParsedRow]) -> int:
    if not buffer_rows:
        return 0
    tuples = [r.as_tuple() for r in buffer_rows]
    execute_values(
        cur,
        INSERT_SQL,
        tuples,
        page_size=EXECUTE_VALUES_PAGE_SIZE,
    )
    return len(buffer_rows)


# =========================
# 6) 메인 실행
# =========================

def main() -> int:
    job_name = "a2_fct_table_parser_mp"
    start_dt = datetime.now()
    start_ts = time.perf_counter()

    log("============================================================")
    log(f"[START] {job_name} | {start_dt:%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] BASE_DIR = {BASE_DIR}")
    log(f"[INFO] DATE RANGE (folder/file) = {START_DAY} ~ {END_DAY}")
    log(f"[INFO] MODE = {MODE}")
    log(f"[INFO] MULTIPROCESSING = {USE_MULTIPROCESSING} | workers={MAX_WORKERS if USE_MULTIPROCESSING else 1}")
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

    flush_rows_threshold = FLUSH_ROWS_STABLE if MODE.lower() == "stable" else FLUSH_ROWS_BASIC

    log("[STEP] 3) Parse + Chunk Insert ...")
    total_files = 0
    total_rows = 0
    buffer: List[ParsedRow] = []

    with get_conn() as conn:
        # 대용량 안정성을 위해 autocommit False(기본)에서, flush 단위로 commit
        with conn.cursor() as cur:
            if USE_MULTIPROCESSING:
                with Pool(processes=MAX_WORKERS) as pool:
                    # imap_unordered로 처리 완료되는 대로 받음
                    for rows in pool.imap_unordered(parse_one_file, jobs, chunksize=200):
                        total_files += 1
                        if rows:
                            buffer.extend(rows)

                        # flush 조건
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

            # 마지막 잔여 flush
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


if __name__ == "__main__":
    freeze_support()
    raise SystemExit(main())
