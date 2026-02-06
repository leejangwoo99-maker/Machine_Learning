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

[로그 DB 저장 사양 추가]
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: k_demon_heath_check.a2_log (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 로그를 dataframe 형식(end_day, end_time, info, contents 순서)으로 적재하는 개념으로 DB 저장
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
TABLE_NAME = "fct_table"

# 로그 저장 스키마/테이블
LOG_SCHEMA_NAME = "k_demon_heath_check"
LOG_TABLE_NAME = "a2_log"

# =========================
# Realtime / Loop 사양
# =========================

# ✅ 요구사항: 멀티프로세스 = 1개
USE_MULTIPROCESSING = False
MAX_WORKERS = 1

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

HEARTBEAT_EVERY_LOOPS = 30
DB_RELOAD_EVERY_LOOPS = 120
DB_RETRY_INTERVAL_SEC = 5.0

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ keepalive
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


# =========================
# 1) 로깅/유틸
# =========================

def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")

def now_hhmmss() -> str:
    return datetime.now().strftime("%H:%M:%S")

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
    try:
        for pat in LOCK_GLOB_PATTERNS:
            if any(dir_path.glob(pat)):
                return True
        return False
    except Exception:
        return True

def is_suspect_filename(fp: Path) -> bool:
    name = fp.name
    if name.startswith(SUSPECT_NAME_PREFIXES):
        return True
    lname = name.lower()
    if lname.endswith(SUSPECT_NAME_SUFFIXES):
        return True
    return False


# =========================
# 1-1) DB 로그 버퍼/적재
# =========================

# 컬럼 순서 고정: end_day, end_time, info, contents
LogRow = Tuple[str, str, str, str]
LOG_BUFFER: List[LogRow] = []
LOG_FLUSH_MIN_ROWS = 1
LOG_CONTENTS_MAX_LEN = 4000

DDL_LOG_SQL = f"""
CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA_NAME};

CREATE TABLE IF NOT EXISTS {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (
    end_day  TEXT NOT NULL,
    end_time TEXT NOT NULL,
    info     TEXT NOT NULL,
    contents TEXT
);

CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE_NAME}_end_day
    ON {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (end_day);

CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE_NAME}_info
    ON {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (info);

CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE_NAME}_end_day_time
    ON {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (end_day, end_time);
"""

INSERT_LOG_SQL = f"""
INSERT INTO {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (end_day, end_time, info, contents)
VALUES %s
"""

def _norm_info(info: str) -> str:
    s = (info or "").strip().lower()
    if not s:
        s = "info"
    return re.sub(r"[^a-z0-9_ -]", "", s)[:64] or "info"

def log(msg: str, info: str = "info", save_db: bool = True) -> None:
    print(msg, flush=True)
    if not save_db:
        return
    row: LogRow = (
        today_yyyymmdd(),
        now_hhmmss(),
        _norm_info(info),
        (msg or "")[:LOG_CONTENTS_MAX_LEN],
    )
    LOG_BUFFER.append(row)

def _flush_log_buffer_once(conn) -> int:
    if not LOG_BUFFER:
        return 0
    rows = list(LOG_BUFFER)
    with conn.cursor() as cur:
        execute_values(cur, INSERT_LOG_SQL, rows, page_size=1000)
    conn.commit()
    LOG_BUFFER.clear()
    return len(rows)

def ensure_log_table() -> None:
    while True:
        conn = ensure_conn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(DDL_LOG_SQL)
            conn.autocommit = False
            log(f"[OK] log table ready: {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME}", info="info", save_db=True)
            return
        except psycopg2.Error as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] ensure_log_table failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("ensure_log_table connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][FATAL] ensure_log_table db error: {type(e).__name__}: {repr(e)}", flush=True)
            raise

def flush_log_buffer_blocking() -> None:
    # 로그 적재 실패 시에도 메인 기능 살리기 위해 무한 재시도
    while LOG_BUFFER:
        conn = ensure_conn()
        try:
            inserted = _flush_log_buffer_once(conn)
            if inserted:
                print(f"[LOG][DB] inserted={inserted}", flush=True)
            return
        except psycopg2.Error as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] flush_log_buffer failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("flush_log_buffer connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][WARN] flush_log_buffer db error(drop this batch): {type(e).__name__}: {repr(e)}", flush=True)
            # 치명적 오류(스키마 권한 등)에서 무한 적체 방지: 현재 배치 폐기
            LOG_BUFFER.clear()
            return


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
# 4) 파일 1개 파싱
# =========================

RowTuple = Tuple[str, str, str, str, str, str, str, str, str, str, str, str]

def parse_one_file(args: Tuple[str, str, str]) -> List[RowTuple]:
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
# 5) 파일 수집
# =========================

ProcessedInfo = Tuple[float, int, float]

def is_file_complete(fp: Path, now_ts: float) -> bool:
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

def collect_recent_files(
    processed: Dict[str, ProcessedInfo],
    already_in_db_paths_today: Set[str],
    today_str: str,
) -> List[Tuple[str, str, str]]:
    jobs: List[Tuple[str, str, str]] = []
    now_ts = time.time()
    cutoff_ts = now_ts - RECENT_SECONDS

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

                if fkey in already_in_db_paths_today:
                    continue

                prev = processed.get(fkey)

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
    with conn.cursor() as cur:
        cur.execute("SET work_mem TO %s;", (WORK_MEM,))
        cur.execute("SELECT 1;")
    try:
        conn.rollback()
    except Exception:
        pass

_CONN = None

def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass

def _reset_conn(reason: str = ""):
    global _CONN
    try:
        if reason:
            print(f"[DB][RESET] {reason}", flush=True)
    except Exception:
        pass
    try:
        _safe_close(_CONN)
    except Exception:
        pass
    _CONN = None

def _is_connection_error(e: Exception) -> bool:
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
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
            )
            conn.autocommit = False
            _apply_session_safety(conn)
            print(
                f"[DB][OK] connected (work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})",
                flush=True
            )
            return conn
        except Exception as e:
            print(f"[DB][RETRY] connect failed: {type(e).__name__}: {repr(e)}", flush=True)
            try:
                time.sleep(DB_RETRY_INTERVAL_SEC)
            except Exception:
                pass

def ensure_conn():
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
            print(f"[DB][WARN] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}", flush=True)
            _reset_conn("ping failed")
            _CONN = get_conn_blocking()
            return _CONN
        raise

def ensure_schema_table() -> None:
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
                print(f"[DB][RETRY] ensure_schema_table failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("ensure_schema_table connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][FATAL] ensure_schema_table db error: {type(e).__name__}: {repr(e)}", flush=True)
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

    print("============================================================", flush=True)
    log(f"[START] {job_name} | {start_dt:%Y-%m-%d %H:%M:%S}", info="info")
    log(f"[INFO] BASE_DIR = {BASE_DIR}", info="info")
    log(f"[INFO] TODAY(end_day) ONLY = {today_yyyymmdd()}", info="info")
    log(f"[INFO] RECENT_SECONDS(mtime) = {RECENT_SECONDS}", info="info")
    log(f"[INFO] LOOP_INTERVAL_SEC = {LOOP_INTERVAL_SEC}", info="info")
    log(f"[INFO] MULTIPROCESSING = {USE_MULTIPROCESSING} | workers=1", info="info")
    log(f"[INFO] TARGET = {SCHEMA_NAME}.{TABLE_NAME}", info="info")
    log(f"[INFO] LOG_TARGET = {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME}", info="info")
    log(f"[INFO] MIN_FILE_AGE_SEC={MIN_FILE_AGE_SEC} | STABILITY_CHECK_SEC={STABILITY_CHECK_SEC} | STABILITY_RETRY={STABILITY_RETRY}", info="info")
    log(f"[INFO] LOCK_GLOB_PATTERNS={LOCK_GLOB_PATTERNS}", info="info")
    log(f"[INFO] DB_RELOAD_EVERY_LOOPS={DB_RELOAD_EVERY_LOOPS}", info="info")
    log(f"[INFO] DB_RETRY_INTERVAL_SEC={DB_RETRY_INTERVAL_SEC}", info="info")
    log(f"[INFO] WORK_MEM(cap)={WORK_MEM}", info="info")
    log(f"[INFO] keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}", info="info")
    print("============================================================", flush=True)

    # fct 테이블 / log 테이블 생성
    log("[STEP] Ensure schema/table/index ...", info="info")
    ensure_schema_table()
    log("[OK] main table ready.", info="info")

    ensure_log_table()
    flush_log_buffer_blocking()

    processed: Dict[str, ProcessedInfo] = {}
    loop_count = 0

    conn = ensure_conn()
    pool = None

    current_day = today_yyyymmdd()
    while True:
        try:
            conn = ensure_conn()
            already_in_db_paths_today: Set[str] = load_db_file_paths_today(conn, current_day)
            log(f"[DB] Loaded DISTINCT file_path for end_day={current_day}: {len(already_in_db_paths_today):,}", info="info")
            flush_log_buffer_blocking()
            break
        except psycopg2.Error as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] load_db_file_paths_today failed(conn): {type(e).__name__}: {repr(e)}", info="down")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("load_db_file_paths_today connection error")
                flush_log_buffer_blocking()
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            log(f"[DB][FATAL] load_db_file_paths_today db error: {type(e).__name__}: {repr(e)}", info="error")
            flush_log_buffer_blocking()
            raise

    try:
        while True:
            loop_count += 1
            loop_start = time.perf_counter()
            now_dt = datetime.now()
            today_str = now_dt.strftime("%Y%m%d")

            if today_str != current_day:
                current_day = today_str
                processed.clear()

                while True:
                    try:
                        conn = ensure_conn()
                        already_in_db_paths_today = load_db_file_paths_today(conn, current_day)
                        log(f"[DAY-CHANGE] today={current_day} | reset caches | db_paths={len(already_in_db_paths_today):,}", info="info")
                        flush_log_buffer_blocking()
                        break
                    except psycopg2.Error as e:
                        if _is_connection_error(e):
                            log(f"[DB][RETRY] day-change reload failed(conn): {type(e).__name__}: {repr(e)}", info="down")
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _reset_conn("day-change reload connection error")
                            flush_log_buffer_blocking()
                            time.sleep(DB_RETRY_INTERVAL_SEC)
                            continue
                        log(f"[DB][FATAL] day-change reload db error: {type(e).__name__}: {repr(e)}", info="error")
                        flush_log_buffer_blocking()
                        raise

            if (loop_count % DB_RELOAD_EVERY_LOOPS) == 0:
                while True:
                    try:
                        conn = ensure_conn()
                        already_in_db_paths_today = load_db_file_paths_today(conn, current_day)
                        log(f"[DB-RELOAD] end_day={current_day} | db_paths={len(already_in_db_paths_today):,}", info="info")
                        flush_log_buffer_blocking()
                        break
                    except psycopg2.Error as e:
                        if _is_connection_error(e):
                            log(f"[DB][RETRY] periodic reload failed(conn): {type(e).__name__}: {repr(e)}", info="down")
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _reset_conn("periodic reload connection error")
                            flush_log_buffer_blocking()
                            time.sleep(DB_RETRY_INTERVAL_SEC)
                            continue
                        log(f"[DB][FATAL] periodic reload db error: {type(e).__name__}: {repr(e)}", info="error")
                        flush_log_buffer_blocking()
                        raise

            jobs = collect_recent_files(processed, already_in_db_paths_today, current_day)

            if (loop_count % HEARTBEAT_EVERY_LOOPS) == 0:
                log(
                    f"[HEARTBEAT] {now_dt:%Y-%m-%d %H:%M:%S} | jobs={len(jobs):,} | cache={len(processed):,} | db_paths={len(already_in_db_paths_today):,} | today={current_day}",
                    info="info"
                )

            if not jobs:
                elapsed = time.perf_counter() - loop_start
                sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
                if sleep_sec > 0:
                    log(f"[SLEEP] no jobs | sleep={sleep_sec:.2f}s", info="sleep")
                    if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                        flush_log_buffer_blocking()
                    time.sleep(sleep_sec)
                continue

            buffer: List[RowTuple] = []
            files_processed = 0
            rows_inserted = 0

            for j in jobs:
                rows = parse_one_file(j)
                files_processed += 1
                if rows:
                    buffer.extend(rows)

                if len(buffer) >= FLUSH_ROWS_REALTIME:
                    while True:
                        conn = ensure_conn()
                        try:
                            with conn.cursor() as cur:
                                inserted = flush_rows(cur, buffer)
                            conn.commit()
                            rows_inserted += inserted

                            for r in buffer:
                                already_in_db_paths_today.add(r[-1])

                            log(f"[FLUSH] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}", info="info")
                            buffer.clear()
                            if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                                flush_log_buffer_blocking()
                            break

                        except psycopg2.Error as e:
                            if _is_connection_error(e):
                                log(f"[DB][RETRY] flush failed(conn): {type(e).__name__}: {repr(e)}", info="down")
                                try:
                                    conn.rollback()
                                except Exception:
                                    pass
                                _reset_conn("flush connection error")
                                if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                                    flush_log_buffer_blocking()
                                time.sleep(DB_RETRY_INTERVAL_SEC)
                                continue
                            log(f"[DB][FATAL] flush db error: {type(e).__name__}: {repr(e)}", info="error")
                            if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                                flush_log_buffer_blocking()
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

                        log(f"[FLUSH-LAST] inserted={inserted:,} | loop_rows={rows_inserted:,} | loop_files={files_processed:,}/{len(jobs):,}", info="info")
                        buffer.clear()
                        if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                            flush_log_buffer_blocking()
                        break

                    except psycopg2.Error as e:
                        if _is_connection_error(e):
                            log(f"[DB][RETRY] flush-last failed(conn): {type(e).__name__}: {repr(e)}", info="down")
                            try:
                                conn.rollback()
                            except Exception:
                                pass
                            _reset_conn("flush-last connection error")
                            if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                                flush_log_buffer_blocking()
                            time.sleep(DB_RETRY_INTERVAL_SEC)
                            continue
                        log(f"[DB][FATAL] flush-last db error: {type(e).__name__}: {repr(e)}", info="error")
                        if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                            flush_log_buffer_blocking()
                        raise

            log(f"[LOOP] {now_dt:%H:%M:%S} | jobs={len(jobs):,} | files={files_processed:,} | inserted_rows={rows_inserted:,}", info="info")
            if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                flush_log_buffer_blocking()

            elapsed = time.perf_counter() - loop_start
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                log(f"[SLEEP] loop done | sleep={sleep_sec:.2f}s", info="sleep")
                if len(LOG_BUFFER) >= LOG_FLUSH_MIN_ROWS:
                    flush_log_buffer_blocking()
                time.sleep(sleep_sec)

    finally:
        # 종료 직전 로그 버퍼 최대한 반영
        try:
            if LOG_BUFFER:
                flush_log_buffer_blocking()
        except Exception:
            pass

        if pool is not None:
            try:
                pool.close()
                pool.join()
            except Exception:
                pass

    return 0


# =========================
# 8) 엔트리포인트
# =========================

def hold_console(exit_code: int) -> None:
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
        # 종료 이벤트를 DB 로그에 남기고 시도
        try:
            LOG_BUFFER.append((today_yyyymmdd(), now_hhmmss(), "error", "[ABORT] keyboard interrupt"))
            flush_log_buffer_blocking()
        except Exception:
            pass
        exit_code = 130
    except Exception as e:
        print("\n[ERROR] Unhandled exception:", repr(e))
        try:
            LOG_BUFFER.append((today_yyyymmdd(), now_hhmmss(), "error", f"[ERROR] unhandled exception: {repr(e)}"))
            flush_log_buffer_blocking()
        except Exception:
            pass
        exit_code = 1
    finally:
        hold_console(exit_code)

    raise SystemExit(exit_code)
