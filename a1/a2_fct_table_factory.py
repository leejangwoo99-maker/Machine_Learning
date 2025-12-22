# -*- coding: utf-8 -*-
"""
a2_fct_table_parser_mp_realtime.py (Nuitka/Windows onefile + realtime loop)

추가 사양 반영(통합본)
- [멀티프로세스] 워커 2개 고정
- [무한 루프] 1초마다 재실행
- [윈도우] 유효 날짜 범위: end_day(파일명)이 "현재 날짜(오늘)"만
- [실시간] 현재 시간 기준 120초 이내 새롭게 추가/수정된 파일만 파싱 (mtime 기준)
- [미완성 파싱 방지] 저장 중일 가능성이 있는 파일 스킵 + 안정화 체크(크기/mtime 고정) 후 파싱
  * 폴더 내 lock/tmp/part 등의 "저장중 암시 파일" 존재 시 스킵
  * 파일명 자체가 임시 파일 패턴이면 스킵(~$, .tmp, .part ...)
  * mtime 안정(MIN_FILE_AGE_SEC)
  * 파일 크기 0.2초 간격 2회 동일 + mtime 동일 (연속 동일)
  * open(rb)로 간단 read 가능 여부 확인
  * 텍스트 읽기 가능 여부 확인
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import multiprocessing as mp
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

# =========================
# Realtime / Loop 사양
# =========================
USE_MULTIPROCESSING = True

# ✅ 워커 2개 고정
MAX_WORKERS = 2

# 루프 주기(초)
LOOP_INTERVAL_SEC = 1.0

# ✅ "현재 시간 기준 120초 이내"만 파싱 (mtime 기준)
RECENT_SECONDS = 120

# ✅ 저장 중(미완성) 파싱 방지 파라미터
MIN_FILE_AGE_SEC = 2.0          # mtime이 너무 최근이면(2초 이내) 스킵

# 요청 사양: "파일 크기 0.2초 간격 2회 동일"
STABILITY_CHECK_SEC = 0.2       # ✅ 0.2초
STABILITY_RETRY = 2             # ✅ 2회 연속 동일 필요

# ✅ lock/임시파일 패턴 (저장 중 파일 스킵)
LOCK_GLOB_PATTERNS = [
    "*.lock", "~$*", "*.tmp", "*.part", "*.partial", "*.crdownload"
]
SUSPECT_NAME_SUFFIXES = (".tmp", ".part", ".partial", ".crdownload")
SUSPECT_NAME_PREFIXES = ("~$",)

# pool 통신 감소(파일 수가 적을 수도 있으니 과도하게 키울 필요 없음)
POOL_CHUNKSIZE = 500

# INSERT 청크
FLUSH_ROWS_REALTIME = 20_000
EXECUTE_VALUES_PAGE_SIZE = 5_000

# 수집/처리 로그
HEARTBEAT_EVERY_LOOPS = 30  # 30루프(약 30초)마다 상태 로그


# =========================
# 1) 로깅/유틸
# =========================

def log(msg: str) -> None:
    print(msg, flush=True)

def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")

def safe_stat(fp: Path):
    try:
        return fp.stat()
    except Exception:
        return None

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

def is_recent_file(fp: Path, now_ts: float, recent_sec: int) -> bool:
    st = safe_stat(fp)
    if st is None:
        return False
    return (now_ts - st.st_mtime) <= recent_sec

def has_lock_files_in_dir(dir_path: Path) -> bool:
    """
    폴더 내 lock/tmp/part 등 저장 중을 암시하는 파일이 있으면 True
    (네트워크 경로에서 glob 에러가 날 수 있어 보수적으로 처리)
    """
    try:
        for pat in LOCK_GLOB_PATTERNS:
            if any(dir_path.glob(pat)):
                return True
        return False
    except Exception:
        # 에러 시 보수적으로 "락 있음" 처리 -> 미완성 파싱 방지
        return True

def is_suspect_filename(fp: Path) -> bool:
    name = fp.name
    if name.startswith(SUSPECT_NAME_PREFIXES):
        return True
    lname = name.lower()
    if lname.endswith(SUSPECT_NAME_SUFFIXES):
        return True
    return False

def is_file_complete(fp: Path, now_ts: float) -> bool:
    """
    저장 중(미완성) 파일 파싱 방지 (강화 버전):
    0) 파일명 자체가 임시/저장중 패턴이면 스킵
    1) 동일 폴더에 lock/tmp/part 파일이 있으면 스킵
    2) mtime이 너무 최근이면 스킵 (MIN_FILE_AGE_SEC)
    3) 파일 크기 0.2초 간격 2회 연속 동일 + mtime 동일
    4) open(rb)로 간단 read 가능 여부 확인
    5) 텍스트 읽기 가능 여부 확인
    """
    # 0) 임시/의심 파일명 스킵
    if is_suspect_filename(fp):
        return False

    parent = fp.parent

    # 1) 폴더 내 lock/tmp/part 등 존재 시 스킵
    if has_lock_files_in_dir(parent):
        return False

    st1 = safe_stat(fp)
    if st1 is None:
        return False

    # 파일 크기가 0이면 아직 작성 중일 가능성 큼
    if st1.st_size <= 0:
        return False

    # 2) 너무 최근에 수정된 파일은 저장 중일 확률이 높음
    if (now_ts - st1.st_mtime) < MIN_FILE_AGE_SEC:
        return False

    size_prev = st1.st_size
    mtime_prev = st1.st_mtime

    # 3) 안정화 체크(크기/mtime 고정) - 0.2초 간격 2회 "연속 동일"
    stable_hits = 0
    for _ in range(STABILITY_RETRY):
        time.sleep(STABILITY_CHECK_SEC)

        st2 = safe_stat(fp)
        if st2 is None:
            return False

        # 중간에 lock 파일이 생길 수 있으므로 보수적으로 한 번 더 체크
        if has_lock_files_in_dir(parent):
            return False

        if st2.st_size == size_prev and st2.st_mtime == mtime_prev and st2.st_size > 0:
            stable_hits += 1
        else:
            stable_hits = 0
            size_prev = st2.st_size
            mtime_prev = st2.st_mtime

    if stable_hits < STABILITY_RETRY:
        return False

    # 4) 열기 가능 여부(저장 중/잠금 상태면 예외 날 수 있음)
    try:
        with open(fp, "rb") as f:
            _ = f.read(64)
    except Exception:
        return False

    # 5) 텍스트 읽기 가능 여부(인코딩 다중 시도)
    lines = safe_read_text(fp)
    if lines is None:
        return False

    return True


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

RowTuple = Tuple[str, str, str, str, str, str, str, str, str, str, str, str]
# (barcode_information, remark, station, end_day, end_time, run_time,
#  step_description, value, min, max, result, file_path)

def parse_one_file(args: Tuple[str, str, str]) -> List[RowTuple]:
    """
    args = (file_path_str, station, today_str)
    - today_str와 end_day가 다르면 즉시 제외
    """
    file_path_str, station, today_str = args
    fp = Path(file_path_str)

    info = parse_filename(fp)
    if info is None:
        return []
    barcode_information, end_day, end_time, remark = info

    # ✅ 오늘 날짜만
    if end_day != today_str:
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
            min_v,
            max_v,
            result,
            str(fp),
        ))
    return out


# =========================
# 5) 실시간 파일 수집 (오늘 폴더 + 최근 120초 + 미완성 방지)
# =========================

ProcessedInfo = Tuple[float, int, float]  # (mtime, size, last_seen_ts)

def collect_recent_files(processed: Dict[str, ProcessedInfo]) -> List[Tuple[str, str, str]]:
    """
    return [(file_path_str, station, today_str), ...]
    조건:
      - \\...\\TCx\\YYYYMMDD\\{GoodFile,BadFile}\\*.txt
      - YYYYMMDD = 오늘
      - fp.mtime >= now - 120초
      - fp가 이전에 처리되었더라도 (mtime/size 변경)되면 다시 처리
      - 미완성(저장 중) 파일은 스킵
    """
    jobs: List[Tuple[str, str, str]] = []
    now_ts = time.time()
    today_str = today_yyyymmdd()
    cutoff_ts = now_ts - RECENT_SECONDS

    # 캐시 정리(메모리 폭증 방지): 마지막 관측이 10분 넘은 항목 제거
    expire_before = now_ts - 600
    stale_keys = [k for k, v in processed.items() if v[2] < expire_before]
    for k in stale_keys:
        processed.pop(k, None)

    for tc, station in TC_TO_STATION.items():
        tc_dir = BASE_DIR / tc / today_str
        if not tc_dir.exists():
            continue

        for sub in TARGET_SUBFOLDERS:
            sub_dir = tc_dir / sub
            if not sub_dir.exists():
                continue

            # 오늘 폴더 내 txt만
            for fp in sub_dir.glob("*.txt"):
                # 임시/의심 파일명은 즉시 스킵
                if is_suspect_filename(fp):
                    continue

                st = safe_stat(fp)
                if st is None:
                    continue

                # 최근 120초 이내 변경 파일만
                if st.st_mtime < cutoff_ts:
                    continue

                fkey = str(fp)
                prev = processed.get(fkey)

                # 변경이 없는 파일은 스킵 (중복 파싱 방지)
                if prev is not None:
                    prev_mtime, prev_size, _ = prev
                    if prev_mtime == st.st_mtime and prev_size == st.st_size:
                        processed[fkey] = (prev_mtime, prev_size, now_ts)  # last_seen 갱신
                        continue

                # 저장 중(미완성) 방지(강화)
                if not is_file_complete(fp, now_ts):
                    processed[fkey] = (st.st_mtime, st.st_size, now_ts)
                    continue

                # 통과 -> 작업 등록 + 캐시 업데이트
                jobs.append((fkey, station, today_str))
                processed[fkey] = (st.st_mtime, st.st_size, now_ts)

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

CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_file_path
    ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);
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
        buffer_rows,
        page_size=EXECUTE_VALUES_PAGE_SIZE,
    )
    return len(buffer_rows)


# =========================
# 7) 무한루프 메인
# =========================

def realtime_loop() -> int:
    job_name = "a2_fct_table_parser_mp_realtime"
    start_dt = datetime.now()

    log("============================================================")
    log(f"[START] {job_name} | {start_dt:%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] BASE_DIR = {BASE_DIR}")
    log(f"[INFO] TODAY(end_day) ONLY = {today_yyyymmdd()}")
    log(f"[INFO] RECENT_SECONDS(mtime) = {RECENT_SECONDS}")
    log(f"[INFO] LOOP_INTERVAL_SEC = {LOOP_INTERVAL_SEC}")
    log(f"[INFO] MULTIPROCESSING = {USE_MULTIPROCESSING} | workers={MAX_WORKERS if USE_MULTIPROCESSING else 1}")
    log(f"[INFO] TARGET = {SCHEMA_NAME}.{TABLE_NAME}")
    log(f"[INFO] MIN_FILE_AGE_SEC={MIN_FILE_AGE_SEC} | STABILITY_CHECK_SEC={STABILITY_CHECK_SEC} | STABILITY_RETRY={STABILITY_RETRY}")
    log(f"[INFO] LOCK_GLOB_PATTERNS={LOCK_GLOB_PATTERNS}")
    log("============================================================")

    log("[STEP] Ensure schema/table/index ...")
    ensure_schema_table()
    log("[OK] Schema/table ready.")

    processed: Dict[str, ProcessedInfo] = {}
    loop_count = 0

    # DB connection은 루프 동안 유지(성능/안정성)
    with get_conn() as conn:
        with conn.cursor() as cur:
            if USE_MULTIPROCESSING:
                ctx = mp.get_context("spawn")
                pool = ctx.Pool(processes=MAX_WORKERS)
            else:
                pool = None

            try:
                while True:
                    loop_count += 1
                    loop_start = time.perf_counter()
                    now_dt = datetime.now()
                    today_str = now_dt.strftime("%Y%m%d")

                    # 1) 최근 파일 수집
                    jobs = collect_recent_files(processed)

                    if (loop_count % HEARTBEAT_EVERY_LOOPS) == 0:
                        log(f"[HEARTBEAT] {now_dt:%Y-%m-%d %H:%M:%S} | jobs={len(jobs):,} | cache={len(processed):,} | today={today_str}")

                    if not jobs:
                        # 1초 주기 유지
                        elapsed = time.perf_counter() - loop_start
                        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
                        if sleep_sec > 0:
                            time.sleep(sleep_sec)
                        continue

                    # 2) 파싱 + insert
                    buffer: List[RowTuple] = []
                    files_processed = 0
                    rows_inserted = 0

                    if USE_MULTIPROCESSING and pool is not None:
                        for rows in pool.imap_unordered(parse_one_file, jobs, chunksize=POOL_CHUNKSIZE):
                            files_processed += 1
                            if rows:
                                buffer.extend(rows)

                            if len(buffer) >= FLUSH_ROWS_REALTIME:
                                inserted = flush_rows(cur, buffer)
                                conn.commit()
                                rows_inserted += inserted
                                log(f"[FLUSH] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}")
                                buffer.clear()
                    else:
                        for j in jobs:
                            rows = parse_one_file(j)
                            files_processed += 1
                            if rows:
                                buffer.extend(rows)

                            if len(buffer) >= FLUSH_ROWS_REALTIME:
                                inserted = flush_rows(cur, buffer)
                                conn.commit()
                                rows_inserted += inserted
                                log(f"[FLUSH] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}")
                                buffer.clear()

                    if buffer:
                        inserted = flush_rows(cur, buffer)
                        conn.commit()
                        rows_inserted += inserted
                        log(f"[FLUSH-LAST] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}")
                        buffer.clear()

                    log(f"[LOOP] {now_dt:%H:%M:%S} | jobs={len(jobs):,} | files={files_processed:,} | inserted_rows={rows_inserted:,}")

                    # 3) 1초 주기 유지
                    elapsed = time.perf_counter() - loop_start
                    sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
                    if sleep_sec > 0:
                        time.sleep(sleep_sec)

            finally:
                if pool is not None:
                    try:
                        pool.close()
                        pool.join()
                    except Exception:
                        pass

    return 0


# =========================
# 8) 엔트리포인트 (EXE 콘솔 자동 종료 방지)
# =========================

def hold_console(exit_code: int) -> None:
    """
    exe 더블클릭 실행 시 콘솔 자동 종료 방지.
    무한루프이므로 일반적으로 호출되지 않지만, 예외/중단 시 유효.
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
        exit_code = realtime_loop()
    except KeyboardInterrupt:
        print("\n[ABORT] 사용자 중단(CTRL+C)")
        exit_code = 130
    except Exception as e:
        print("\n[ERROR] Unhandled exception:", repr(e))
        exit_code = 1
    finally:
        hold_console(exit_code)

    raise SystemExit(exit_code)
