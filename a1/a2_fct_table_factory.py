# -*- coding: utf-8 -*-
"""
a2_fct_table_parser_mp_realtime.py

추가 반영
- 파일 저장 중(미완성) 파싱 방지:
  * 파일 size/mtime이 짧은 간격으로 2회 연속 동일하면 안정 파일로 판단
  * 불일치/예외 시 이번 루프 스킵

기존 반영
- 최근 60초(mtime) 파일만 파싱
- 중복 방지: UNIQUE(file_path) + ON CONFLICT DO NOTHING
- 폴더 구조: \\HistoryLog\\TCx\\YYYYMMDD\\{BadFile,GoodFile}\\*.txt
- 멀티프로세스 2개 고정
- 1초마다 무한 루프
"""

from __future__ import annotations

import re
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from multiprocessing import Pool, freeze_support
from typing import List, Optional, Tuple

import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 설정
# =========================

BASE_DIR = Path(r"\\192.168.108.101\HistoryLog")

TC_TO_STATION = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

TARGET_SUBFOLDERS = ["GoodFile", "BadFile"]

# 실시간 조건
REALTIME_SEC = 60     # 최근 60초
LOOP_SEC = 1          # 1초마다 재실행

# 멀티프로세스 고정
USE_MULTIPROCESSING = True
WORKERS = 2

# [추가] 미완성 파일 방지(안정성 체크)
STABILITY_CHECK_ENABLED = True
STABILITY_INTERVAL_SEC = 0.2   # 200ms 후 다시 stat 비교
STABILITY_TRIES = 2            # 연속 2회 동일해야 통과 (요청 취지에 맞춤)

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}
SCHEMA_NAME = "a2_fct_table"
TABLE_NAME  = "fct_table"

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


def today_yyyymmdd(now: Optional[datetime] = None) -> str:
    now = now or datetime.now()
    return now.strftime("%Y%m%d")


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


def file_is_stable(fp: Path) -> bool:
    """
    미완성 파일 방지:
    - 지정한 간격(STABILITY_INTERVAL_SEC)으로 stat을 반복해서
      (size, mtime)가 연속으로 동일하면 stable 판정.
    - 네트워크/락/권한 이슈 등 예외 발생 시 False.
    """
    try:
        s0 = fp.stat()
        prev = (s0.st_size, s0.st_mtime)

        for _ in range(STABILITY_TRIES - 1):
            time.sleep(STABILITY_INTERVAL_SEC)
            s1 = fp.stat()
            cur = (s1.st_size, s1.st_mtime)
            if cur != prev:
                return False
            prev = cur

        # size가 0인 파일은 보통 미완성/깨짐일 확률이 높아서 방어적으로 제외
        if prev[0] <= 0:
            return False

        return True
    except Exception:
        return False


# =========================
# 2) 파싱 로직
# =========================

_RUN_TIME_RE = re.compile(r"Run\s*Time\s*:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)

def parse_filename(file_path: Path) -> Optional[Tuple[str, str, str, str]]:
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
    dt14 = parts[1].strip()
    if not re.fullmatch(r"\d{14}", dt14):
        return None

    end_day = dt14[:8]
    end_time = dt14[8:]

    remark = "Non-PD"
    if len(barcode_information) >= 18:
        ch = barcode_information[17]
        if ch in ("J", "S"):
            remark = "PD"

    return barcode_information, end_day, end_time, remark


def parse_run_time(lines: List[str]) -> str:
    if len(lines) >= 14:
        m = _RUN_TIME_RE.search(lines[13])
        if m:
            return m.group(1).strip()
    for ln in lines:
        m = _RUN_TIME_RE.search(ln)
        if m:
            return m.group(1).strip()
    return ""


def parse_test_lines(lines: List[str]) -> List[Tuple[str, str, str, str, str]]:
    out: List[Tuple[str, str, str, str, str]] = []
    if len(lines) < 19:
        return out

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


def parse_one_file(args: Tuple[str, str, str]) -> List[ParsedRow]:
    """
    args = (file_path_str, station, today)
    """
    file_path_str, station, today = args
    fp = Path(file_path_str)

    # [추가] 안정성 체크: 쓰는 중이면 파싱 스킵
    if STABILITY_CHECK_ENABLED:
        if not file_is_stable(fp):
            return []

    info = parse_filename(fp)
    if info is None:
        return []
    barcode_information, end_day, end_time, remark = info

    # 오늘 파일만
    if end_day != today:
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
# 3) 파일 수집 (오늘 폴더 + 최근 60초 mtime)
# =========================

def collect_recent_files(today: str, cutoff_ts: float) -> List[Tuple[str, str, str]]:
    jobs: List[Tuple[str, str, str]] = []

    for tc, station in TC_TO_STATION.items():
        day_dir = BASE_DIR / tc / today
        if not day_dir.exists():
            continue

        for sub in TARGET_SUBFOLDERS:
            sub_dir = day_dir / sub
            if not sub_dir.exists():
                continue

            for fp in sub_dir.glob("*.txt"):
                try:
                    if not fp.is_file():
                        continue
                    # 최근 60초 mtime
                    if fp.stat().st_mtime >= cutoff_ts:
                        jobs.append((str(fp), station, today))
                except Exception:
                    continue

    return jobs


# =========================
# 4) DB DDL + INSERT
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

CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_NAME}_file_path
    ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_end_day
    ON {SCHEMA_NAME}.{TABLE_NAME} (end_day);

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_station_end_day
    ON {SCHEMA_NAME}.{TABLE_NAME} (station, end_day);
"""

INSERT_COLS = (
    "barcode_information, remark, station, end_day, end_time, run_time, "
    "step_description, value, min, max, result, file_path"
)

INSERT_SQL = f"""
INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} ({INSERT_COLS})
VALUES %s
ON CONFLICT (file_path) DO NOTHING
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
    execute_values(cur, INSERT_SQL, tuples, page_size=EXECUTE_VALUES_PAGE_SIZE)
    return len(buffer_rows)


# =========================
# 5) 무한 루프 실행
# =========================

def run_forever() -> int:
    job_name = "a2_fct_table_parser_mp_realtime"
    log("============================================================")
    log(f"[BOOT] {job_name} | {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] BASE_DIR = {BASE_DIR}")
    log(f"[INFO] REALTIME_SEC = {REALTIME_SEC} | LOOP_SEC = {LOOP_SEC}")
    log(f"[INFO] WORKERS = {WORKERS}")
    log(f"[INFO] STABILITY_CHECK = {STABILITY_CHECK_ENABLED} | interval={STABILITY_INTERVAL_SEC}s | tries={STABILITY_TRIES}")
    log(f"[INFO] TARGET = {SCHEMA_NAME}.{TABLE_NAME}")
    log("============================================================")

    log("[STEP] Ensure schema/table/index ...")
    ensure_schema_table()
    log("[OK] Schema/table ready.")

    pool: Optional[Pool] = None
    if USE_MULTIPROCESSING:
        pool = Pool(processes=WORKERS)

    try:
        while True:
            loop_start_dt = datetime.now()
            loop_start_ts = time.perf_counter()

            now = datetime.now()
            today = today_yyyymmdd(now)
            cutoff_ts = (now - timedelta(seconds=REALTIME_SEC)).timestamp()

            jobs = collect_recent_files(today=today, cutoff_ts=cutoff_ts)

            if not jobs:
                time.sleep(LOOP_SEC)
                continue

            total_files = 0
            total_rows_try_insert = 0
            buffer: List[ParsedRow] = []

            with get_conn() as conn:
                with conn.cursor() as cur:
                    if pool is not None:
                        for rows in pool.imap_unordered(parse_one_file, jobs, chunksize=50):
                            total_files += 1
                            if rows:
                                buffer.extend(rows)
                    else:
                        for j in jobs:
                            rows = parse_one_file(j)
                            total_files += 1
                            if rows:
                                buffer.extend(rows)

                    if buffer:
                        inserted = flush_rows(cur, buffer)
                        conn.commit()
                        total_rows_try_insert += inserted
                        buffer.clear()

            elapsed = time.perf_counter() - loop_start_ts
            log(
                f"[LOOP] {loop_start_dt:%H:%M:%S} | today={today} | "
                f"recent_files={len(jobs):,} | parsed_files={total_files:,} | "
                f"rows_try_insert={total_rows_try_insert:,} | elapsed={elapsed:,.2f}s"
            )

            time.sleep(LOOP_SEC)

    except KeyboardInterrupt:
        log("[STOP] KeyboardInterrupt. Exit.")
        return 0
    finally:
        if pool is not None:
            pool.close()
            pool.join()


if __name__ == "__main__":
    freeze_support()
    raise SystemExit(run_forever())
