# -*- coding: utf-8 -*-
"""
a2_fct_table_parser_mp_realtime.py (Nuitka/Windows onefile + realtime loop)

[통합 수정 사항]
- 중복 방지 기준: file_path 단독
- DB 중복 방지: 오늘(end_day=오늘) 기준으로 fct_table에 이미 저장된 file_path만 SELECT DISTINCT로 로드
- run 내 중복 방지: file_path 단독
- 날짜 변경(자정) 감지 시: 캐시 초기화 + 오늘 DB file_path 재로딩
- ⚠️ fct_table은 파일 1개당 여러 row 구조이므로 file_path UNIQUE 인덱스는 생성하지 않음

[추가/수정 사양 반영]
- 멀티프로세스 = 1개
- 무한 루프 인터벌 5초
- DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
- 백엔드별 상시 연결을 1개로 고정(풀 최소화)  -> 프로세스 전체에서 psycopg2 커넥션 1개만 유지/재사용
- work_mem 폭증 방지 -> 연결 직후 세션에 SET work_mem 적용(환경변수로 조정 가능)

[추가 반영(요청사항)]
- ✅ 실행 중(쿼리/커밋/INSERT 중 포함) 서버 연결이 끊겨도 무한 재접속 후 자동 재시도
- ✅ keepalive 옵션(권장) 추가: 죽은 TCP 세션 감지 가속
- ✅ 연결 오류 판별 강화(OperationalError/InterfaceError + 메시지 키워드)
"""

from __future__ import annotations

import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import multiprocessing as mp
import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 사용자 설정
# =========================

# ✅ (중요) psycopg2/libpq 에러 메시지 인코딩 이슈 방지(EXE에서 UnicodeDecodeError 예방)
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

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
    "host": "100.105.75.47",
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

# ✅ 요구사항: 멀티프로세스 = 1개
USE_MULTIPROCESSING = False   # 기능은 유지(코드 구조), 하지만 요구사항에 따라 비활성
MAX_WORKERS = 1               # 명시적으로 1

# ✅ 요구사항: 무한 루프 인터벌 5초
LOOP_INTERVAL_SEC = 5.0

RECENT_SECONDS = 120

# ✅ 저장 중(미완성) 파싱 방지 파라미터
MIN_FILE_AGE_SEC = 2.0
STABILITY_CHECK_SEC = 0.2
STABILITY_RETRY = 2

LOCK_GLOB_PATTERNS = [
    "*.lock", "~$*", "*.tmp", "*.part", "*.partial", "*.crdownload"
]
SUSPECT_NAME_SUFFIXES = (".tmp", ".part", ".partial", ".crdownload")
SUSPECT_NAME_PREFIXES = ("~$",)

POOL_CHUNKSIZE = 500

FLUSH_ROWS_REALTIME = 20_000
EXECUTE_VALUES_PAGE_SIZE = 5_000

HEARTBEAT_EVERY_LOOPS = 30  # 30루프마다 상태 로그

# ✅ DB에서 "오늘 end_day" 기준 file_path 재로딩 주기(루프 횟수)
DB_RELOAD_EVERY_LOOPS = 120  # 약 10분(5초 루프 기준)

# ✅ 요구사항: DB 접속 실패 시 무한 재시도(블로킹)
DB_RETRY_INTERVAL_SEC = 5.0

# ✅ 요구사항: work_mem 폭증 방지(세션별 cap) - 환경변수로 조정 가능
#    예) set PG_WORK_MEM=8MB
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ (추가/권장) 끊긴 TCP 세션을 빠르게 감지하기 위한 keepalive
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


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
    저장 중(미완성) 파일 파싱 방지 (강화 버전)
    """
    if is_suspect_filename(fp):
        return False

    parent = fp.parent

    if has_lock_files_in_dir(parent):
        return False

    st1 = safe_stat(fp)
    if st1 is None:
        return False

    if st1.st_size <= 0:
        return False

    if (now_ts - st1.st_mtime) < MIN_FILE_AGE_SEC:
        return False

    size_prev = st1.st_size
    mtime_prev = st1.st_mtime

    stable_hits = 0
    for _ in range(STABILITY_RETRY):
        time.sleep(STABILITY_CHECK_SEC)

        st2 = safe_stat(fp)
        if st2 is None:
            return False

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

    try:
        with open(fp, "rb") as f:
            _ = f.read(64)
    except Exception:
        return False

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
        ch = barcode_information[17]
        if ch in ("J", "S"):
            remark = "PD"

    return barcode_information, end_day, end_time, remark


# =========================
# 3) 파일 내용 파싱
# =========================

_RUN_TIME_RE = re.compile(r"Run\s*Time\s*:\s*([0-9]+(?:\.[0-9]+)?)", re.IGNORECASE)

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


# =========================
# 4) 파일 1개 파싱 (반환을 tuple로 경량화)
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

def collect_recent_files(
    processed: Dict[str, ProcessedInfo],
    already_in_db_paths_today: Set[str],
    today_str: str,
) -> List[Tuple[str, str, str]]:
    """
    return [(file_path_str, station, today_str), ...]
    조건:
      - \\...\\TCx\\YYYYMMDD\\{GoodFile,BadFile}\\*.txt
      - YYYYMMDD = 오늘
      - fp.mtime >= now - 120초
      - file_path가 DB에 이미 존재(오늘 end_day)하면 스킵  (✅ file_path 단독 기준)
      - processed dict는 run-time 중복 방지(변경없으면 스킵)
      - 미완성(저장 중) 파일은 스킵
    """
    jobs: List[Tuple[str, str, str]] = []
    now_ts = time.time()
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

            for fp in sub_dir.glob("*.txt"):
                if is_suspect_filename(fp):
                    continue

                st = safe_stat(fp)
                if st is None:
                    continue

                if st.st_mtime < cutoff_ts:
                    continue

                fkey = str(fp)

                # ✅ DB 중복 스킵 (file_path 단독, 오늘 end_day 기준)
                if fkey in already_in_db_paths_today:
                    continue

                prev = processed.get(fkey)

                # 변경이 없는 파일은 스킵 (중복 파싱 방지)
                if prev is not None:
                    prev_mtime, prev_size, _ = prev
                    if prev_mtime == st.st_mtime and prev_size == st.st_size:
                        processed[fkey] = (prev_mtime, prev_size, now_ts)
                        continue

                if not is_file_complete(fp, now_ts):
                    processed[fkey] = (st.st_mtime, st.st_size, now_ts)
                    continue

                jobs.append((fkey, station, today_str))
                processed[fkey] = (st.st_mtime, st.st_size, now_ts)

    return jobs


# =========================
# 6) DB (DDL + INSERT + 오늘 file_path 로드)
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

def _apply_session_safety(conn) -> None:
    """
    ✅ work_mem 폭증 방지: 세션에 cap 적용
    + ping(SELECT 1)으로 연결 유효성 확인
    """
    with conn.cursor() as cur:
        cur.execute("SET work_mem TO %s;", (WORK_MEM,))
        cur.execute("SELECT 1;")
    try:
        conn.rollback()
    except Exception:
        pass

_CONN = None  # ✅ 백엔드별 상시 연결 1개 고정(프로세스 전체 1개)

def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass

def _reset_conn(reason: str = ""):
    """커넥션 강제 폐기 + 다음 ensure_conn에서 무한 재연결 유도"""
    global _CONN
    try:
        if reason:
            log(f"[DB][RESET] {reason}")
    except Exception:
        pass
    try:
        _safe_close(_CONN)
    except Exception:
        pass
    _CONN = None

def _is_connection_error(e: Exception) -> bool:
    """
    서버 끊김/네트워크/소켓/세션 종료 등 '재연결'이 필요한 오류인지 판단
    """
    if isinstance(e, (psycopg2.OperationalError, psycopg2.InterfaceError)):
        return True

    msg = (str(e) or "").lower()
    keywords = [
        "server closed the connection",
        "connection not open",
        "terminating connection",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
    ]
    return any(k in msg for k in keywords)

def get_conn_blocking():
    """
    ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
    """
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                dbname=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                connect_timeout=5,
                application_name="a2_fct_table_parser_mp_realtime",

                # keepalive 옵션 (libpq 지원)
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
            )
            conn.autocommit = False
            _apply_session_safety(conn)
            log(
                f"[DB][OK] connected (work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})"
            )
            return conn
        except Exception as e:
            log(f"[DB][RETRY] connect failed: {type(e).__name__}: {repr(e)}")
            try:
                time.sleep(DB_RETRY_INTERVAL_SEC)
            except Exception:
                pass

def ensure_conn():
    """
    ✅ 상시 연결 1개 유지 + 죽으면 무한 재연결(블로킹)
    """
    global _CONN
    if _CONN is None:
        _CONN = get_conn_blocking()
        return _CONN

    if getattr(_CONN, "closed", 1) != 0:
        _reset_conn("conn.closed != 0")
        _CONN = get_conn_blocking()
        return _CONN

    try:
        _apply_session_safety(_CONN)
        return _CONN
    except Exception as e:
        if _is_connection_error(e):
            log(f"[DB][WARN] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}")
            _reset_conn("ping failed")
            _CONN = get_conn_blocking()
            return _CONN
        raise

def ensure_schema_table() -> None:
    """
    schema/table/index 생성.
    ✅ 실행 중 끊김 포함: 실패 시 무한 재시도.
    """
    while True:
        conn = ensure_conn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(DDL_SQL)
            conn.autocommit = False
            return
        except psycopg2.Error as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] ensure_schema_table failed(conn): {type(e).__name__}: {repr(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("ensure_schema_table connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            log(f"[DB][FATAL] ensure_schema_table db error: {type(e).__name__}: {repr(e)}")
            raise
        except Exception:
            raise

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

def load_db_file_paths_today(conn, end_day_today: str) -> Set[str]:
    """
    ✅ 오늘(end_day=오늘) 기준으로 fct_table에 이미 저장된 file_path 목록을 로드 (DISTINCT)
    - file_path 단독 중복 방지
    - 메모리 절감을 위해 '오늘'만
    ✅ 실행 중 끊김 포함: 호출부에서 무한 재시도
    """
    with conn.cursor() as cur:
        cur.execute(
            f"""
            SELECT DISTINCT file_path
            FROM {SCHEMA_NAME}.{TABLE_NAME}
            WHERE end_day = %s
              AND file_path IS NOT NULL
              AND file_path <> '';
            """,
            (end_day_today,)
        )
        rows = cur.fetchall()
    return {r[0] for r in rows if r and r[0]}


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
    log(f"[INFO] MULTIPROCESSING = {USE_MULTIPROCESSING} | workers=1")
    log(f"[INFO] TARGET = {SCHEMA_NAME}.{TABLE_NAME}")
    log(f"[INFO] MIN_FILE_AGE_SEC={MIN_FILE_AGE_SEC} | STABILITY_CHECK_SEC={STABILITY_CHECK_SEC} | STABILITY_RETRY={STABILITY_RETRY}")
    log(f"[INFO] LOCK_GLOB_PATTERNS={LOCK_GLOB_PATTERNS}")
    log(f"[INFO] DB_RELOAD_EVERY_LOOPS={DB_RELOAD_EVERY_LOOPS}")
    log(f"[INFO] DB_RETRY_INTERVAL_SEC={DB_RETRY_INTERVAL_SEC}")
    log(f"[INFO] WORK_MEM(cap)={WORK_MEM}")
    log(f"[INFO] keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}")
    log("============================================================")

    log("[STEP] Ensure schema/table/index ...")
    ensure_schema_table()
    log("[OK] Schema/table ready.")

    processed: Dict[str, ProcessedInfo] = {}
    loop_count = 0

    # ✅ 상시 연결 1개 유지
    conn = ensure_conn()

    # pool 변수는 기존 기능 구조를 유지하기 위해 남겨두되, 요구사항에 따라 사용하지 않음
    pool = None

    # ✅ 오늘 기준 DB file_path 캐시
    current_day = today_yyyymmdd()
    while True:
        try:
            conn = ensure_conn()
            already_in_db_paths_today: Set[str] = load_db_file_paths_today(conn, current_day)
            log(f"[DB] Loaded DISTINCT file_path for end_day={current_day}: {len(already_in_db_paths_today):,}")
            break
        except psycopg2.Error as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] load_db_file_paths_today failed(conn): {type(e).__name__}: {repr(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("load_db_file_paths_today connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            log(f"[DB][FATAL] load_db_file_paths_today db error: {type(e).__name__}: {repr(e)}")
            raise
        except Exception:
            raise

    try:
        while True:
            loop_count += 1
            loop_start = time.perf_counter()
            now_dt = datetime.now()
            today_str = now_dt.strftime("%Y%m%d")

            # ✅ 날짜 변경 감지(자정): 캐시 초기화 + DB file_path 재로딩
            if today_str != current_day:
                current_day = today_str
                processed.clear()

                while True:
                    try:
                        conn = ensure_conn()
                        already_in_db_paths_today = load_db_file_paths_today(conn, current_day)
                        log(f"[DAY-CHANGE] today={current_day} | reset caches | db_paths={len(already_in_db_paths_today):,}")
                        break
                    except psycopg2.Error as e:
                        if _is_connection_error(e):
                            log(f"[DB][RETRY] day-change reload failed(conn): {type(e).__name__}: {repr(e)}")
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _reset_conn("day-change reload connection error")
                            time.sleep(DB_RETRY_INTERVAL_SEC)
                            continue
                        log(f"[DB][FATAL] day-change reload db error: {type(e).__name__}: {repr(e)}")
                        raise
                    except Exception:
                        raise

            # ✅ 주기적 DB file_path 재로딩(프로그램 재시작/타 프로세스 적재 반영)
            if (loop_count % DB_RELOAD_EVERY_LOOPS) == 0:
                while True:
                    try:
                        conn = ensure_conn()
                        already_in_db_paths_today = load_db_file_paths_today(conn, current_day)
                        log(f"[DB-RELOAD] end_day={current_day} | db_paths={len(already_in_db_paths_today):,}")
                        break
                    except psycopg2.Error as e:
                        if _is_connection_error(e):
                            log(f"[DB][RETRY] periodic reload failed(conn): {type(e).__name__}: {repr(e)}")
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _reset_conn("periodic reload connection error")
                            time.sleep(DB_RETRY_INTERVAL_SEC)
                            continue
                        log(f"[DB][FATAL] periodic reload db error: {type(e).__name__}: {repr(e)}")
                        raise
                    except Exception:
                        raise

            # 1) 최근 파일 수집 (✅ DB에 이미 있는 file_path는 스킵)
            jobs = collect_recent_files(processed, already_in_db_paths_today, current_day)

            if (loop_count % HEARTBEAT_EVERY_LOOPS) == 0:
                log(f"[HEARTBEAT] {now_dt:%Y-%m-%d %H:%M:%S} | jobs={len(jobs):,} | cache={len(processed):,} | db_paths={len(already_in_db_paths_today):,} | today={current_day}")

            if not jobs:
                elapsed = time.perf_counter() - loop_start
                sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
                if sleep_sec > 0:
                    time.sleep(sleep_sec)
                continue

            # 2) 파싱 + insert
            buffer: List[RowTuple] = []
            files_processed = 0
            rows_inserted = 0

            # ✅ 요구사항: 멀티프로세스 1개 -> 단일 프로세스로 순차 파싱
            for j in jobs:
                rows = parse_one_file(j)
                files_processed += 1
                if rows:
                    buffer.extend(rows)

                if len(buffer) >= FLUSH_ROWS_REALTIME:
                    # ✅ DB 작업 도중 끊겨도 무한 재연결 후 flush 재시도
                    while True:
                        conn = ensure_conn()
                        try:
                            with conn.cursor() as cur:
                                inserted = flush_rows(cur, buffer)
                            conn.commit()
                            rows_inserted += inserted

                            # ✅ 방금 처리된 파일들 file_path를 DB캐시에 즉시 반영(중복 방지 강화)
                            for r in buffer:
                                already_in_db_paths_today.add(r[-1])

                            log(f"[FLUSH] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}")
                            buffer.clear()
                            break

                        except psycopg2.Error as e:
                            if _is_connection_error(e):
                                log(f"[DB][RETRY] flush failed(conn): {type(e).__name__}: {repr(e)}")
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
                                _reset_conn("flush connection error")
                                time.sleep(DB_RETRY_INTERVAL_SEC)
                                continue
                            log(f"[DB][FATAL] flush db error: {type(e).__name__}: {repr(e)}")
                            raise
                        except Exception:
                            raise

            if buffer:
                while True:
                    conn = ensure_conn()
                    try:
                        with conn.cursor() as cur:
                            inserted = flush_rows(cur, buffer)
                        conn.commit()
                        rows_inserted += inserted

                        for r in buffer:
                            already_in_db_paths_today.add(r[-1])

                        log(f"[FLUSH-LAST] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}")
                        buffer.clear()
                        break

                    except psycopg2.Error as e:
                        if _is_connection_error(e):
                            log(f"[DB][RETRY] flush-last failed(conn): {type(e).__name__}: {repr(e)}")
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _reset_conn("flush-last connection error")
                            time.sleep(DB_RETRY_INTERVAL_SEC)
                            continue
                        log(f"[DB][FATAL] flush-last db error: {type(e).__name__}: {repr(e)}")
                        raise
                    except Exception:
                        raise

            log(f"[LOOP] {now_dt:%H:%M:%S} | jobs={len(jobs):,} | files={files_processed:,} | inserted_rows={rows_inserted:,}")

            # 3) 5초 주기 유지
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
    mp.freeze_support()

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
