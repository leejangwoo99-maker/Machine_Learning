# -*- coding: utf-8 -*-
# fct_machine_log_parser_realtime_optimized.py
# ============================================
# FCT 머신 로그 파싱 + PostgreSQL 적재 (실시간/중복방지/오늘자만) - "파일 1개/일 append" 최적화
#
# ✅ 최적화 반영(요청 5개)
# 1) 오늘(YYYY/MM) 폴더만 스캔 (UNC 디렉토리 전체 순회 제거)
# 2) Pool 매 루프 생성 제거 → ThreadPoolExecutor 재사용(기본 4 workers)
# 3) 바이너리 tail(rb) + "진짜 byte offset" 저장/복원 (텍스트 tell/seek cookie 문제 제거)
# 4) pandas 제거 + psycopg2.execute_values 기반 bulk insert (빠른 대량 insert)
# 5) 커서 저장 주기 기본 30초(환경변수로 조정) + 종료 시 force save 유지
#
# ✅ 유지/요구사항 유지
# - 무한루프(기본 5초)
# - end_day = 오늘(파일명 YYYYMMDD 기준)만 적재
# - (end_day, station, end_time, contents) 중복 방지 (UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - end_time은 hh:mm:ss.ss 문자열로 고정 저장(VARCHAR(12))
# - DB 접속 실패/중간 끊김 시 무한 재시도(블로킹) + engine dispose 후 재연결
# - Engine 1개 재사용(풀 최소: pool_size=1, max_overflow=0, pool_pre_ping)
# - work_mem 세션에 SET (환경변수 PG_WORK_MEM)
# - keepalive 옵션(connect_args)
#
# ✅ 추가 반영(이번 요청)
# - 콘솔 + 파일 로그 저장(날짜별 롤링)
# - 로그 경로: C:\AptivAgent\logs\d1_FC_machine_log_factory\
# - get_engine_blocking() None.connect 무한루프 구조 제거(vision에서 발생했던 동일 문제 예방)
# ============================================

from __future__ import annotations

import os
import re
import time as time_mod
import atexit
import traceback
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
import urllib.parse

# psycopg2 extras for fast bulk insert
try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


# ============================================
# ✅ (중요) psycopg2/libpq 에러 메시지 인코딩 이슈(EXE) 방지
# ============================================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")


# ============================================
# ✅ 로그(콘솔 + 파일) / 날짜별 롤링
# ============================================
LOG_DIR = Path(os.getenv("D1_FCT_LOG_DIR", r"C:\AptivAgent\logs\d1_FC_machine_log_factory"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
_FILE_LOG_DISABLED = False

def _log_file_path():
    day = datetime.now().strftime("%Y%m%d")
    return LOG_DIR / f"fct_machine_log_parser_realtime_optimized_{day}.log"

def log(msg: str):
    global _FILE_LOG_DISABLED
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    if _FILE_LOG_DISABLED:
        return
    try:
        fp = _log_file_path()
        with fp.open("a", encoding="utf-8", errors="ignore") as f:
            f.write(line + "\n")
    except Exception as e:
        _FILE_LOG_DISABLED = True
        print(f"[{ts}] [WARN] file logging disabled due to error: {type(e).__name__}: {e}", flush=True)

def log_exc(prefix: str, e: Exception):
    log(f"{prefix}: {type(e).__name__}: {repr(e)}")
    tb = traceback.format_exc()
    for line in tb.rstrip().splitlines():
        log(f"{prefix} TRACE: {line}")


# ============================================
# 1. 기본 설정
# ============================================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

TABLE_BY_STATION = {
    "FCT1": '"FCT1_machine_log"',
    "FCT2": '"FCT2_machine_log"',
    "FCT3": '"FCT3_machine_log"',
    "FCT4": '"FCT4_machine_log"',
}

# 파일명 예: 20260201_FCT1_Machine_Log ...
FILENAME_PATTERN = re.compile(r"(\d{8})_(FCT|PDI)([1-4])_Machine_Log", re.IGNORECASE)

# 라인 예: [12:34:56.7] message  or [12:34:56] message
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?\]\s*(.*)$")

# ============================================
# ✅ 무한루프 인터벌 5초
# ============================================
SLEEP_SEC = int(os.getenv("SLEEP_SEC", "5"))

# ============================================
# ✅ Thread workers (기본 4) - 파일(4개) 병렬 tail
# ============================================
PARSE_WORKERS = int(os.getenv("PARSE_WORKERS", "4"))

# ============================================
# offset 기반 tail-follow 캐시 (진짜 byte offset)
# ============================================
PROCESSED_OFFSET: dict[str, int] = {}  # path(str) -> int(byte_offset)

CURSOR_SAVE_INTERVAL_SEC = int(os.getenv("CURSOR_SAVE_INTERVAL_SEC", "30"))  # 기본 30초
_last_cursor_save_ts = 0.0
_cursor_dirty_paths: set[str] = set()

CURSOR_TABLE = "machine_log_cursor"


# ============================================
# ✅ DB 접속 실패 시 무한 재시도 / 상시 Engine 1개 / work_mem 폭증 방지
# ============================================
DB_RETRY_INTERVAL_SEC = int(os.getenv("DB_RETRY_INTERVAL_SEC", "5"))
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
_ENGINE = None

# ✅ keepalive 옵션 (환경변수로 조정 가능)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


def _index_safe_name(table_quoted: str) -> str:
    return table_quoted.replace('"', "").lower()


def _masked_db_info(cfg=DB_CONFIG) -> str:
    return f"postgresql://{cfg['user']}:***@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError)):
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


def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


def _build_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"

    eng = create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=1800,
        future=True,
        connect_args={
            "connect_timeout": 5,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "fct_machine_log_parser_realtime_optimized",
        },
    )
    log(f"[INFO] DB = {_masked_db_info(cfg)}")
    return eng


def get_engine_blocking():
    """
    Engine 1개 재사용 + 접속 실패/끊김 시 무한 재시도(블로킹) + 세션 work_mem 적용

    ✅ (중요) None.connect 무한루프 방지:
    - ping 실패 시 _dispose_engine()로 _ENGINE=None이 되면,
      그 즉시 ping 루프를 빠져나와 "새 엔진 생성 루프"로 내려가게 함.
    """
    global _ENGINE

    # 1) 기존 엔진 ping
    while _ENGINE is not None:
        try:
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            return _ENGINE
        except Exception as e:
            log(f"[DB][RETRY] engine ping failed -> rebuild")
            log_exc("[DB][RETRY] ping error", e)
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    # 2) 새 엔진 생성
    while True:
        try:
            _ENGINE = _build_engine(DB_CONFIG)
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            log(
                f"[DB][OK] engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})"
            )
            return _ENGINE
        except Exception as e:
            log(f"[DB][RETRY] engine create/connect failed")
            log_exc("[DB][RETRY] connect error", e)
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# ============================================
# 5. 스키마/테이블 생성 + UNIQUE INDEX + 커서 테이블
# ============================================
CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id          BIGSERIAL PRIMARY KEY,
    end_day     VARCHAR(8),    -- yyyymmdd (파일명 기준)
    station     VARCHAR(10),   -- FCT1~4
    end_time    VARCHAR(12),   -- hh:mm:ss.ss (문자열 고정)
    contents    VARCHAR(75)    -- 최대 75글자
);
"""

CREATE_CURSOR_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{CURSOR_TABLE} (
    path         TEXT PRIMARY KEY,
    file_day     VARCHAR(8) NOT NULL,
    station      VARCHAR(10),
    byte_offset  BIGINT NOT NULL,
    file_size    BIGINT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

CREATE_CURSOR_DAY_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{SCHEMA}_{CURSOR_TABLE}_day
ON {SCHEMA}.{CURSOR_TABLE} (file_day);
"""


def ensure_schema_and_tables(engine):
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

                for station, table_quoted in TABLE_BY_STATION.items():
                    ddl = CREATE_TABLE_TEMPLATE.format(schema=SCHEMA, table=table_quoted)
                    conn.execute(text(ddl))
                    log(f"[INFO] Table ensured: {SCHEMA}.{table_quoted}")

                    idx_name = f"ux_{SCHEMA}_{_index_safe_name(table_quoted)}_dedup"
                    conn.execute(
                        text(
                            f"""
                            CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                            ON {SCHEMA}.{table_quoted} (end_day, station, end_time, contents)
                            """
                        )
                    )
                    log(f"[INFO] Unique index ensured: {idx_name}")

                conn.execute(text(CREATE_CURSOR_TABLE_SQL))
                conn.execute(text(CREATE_CURSOR_DAY_INDEX_SQL))
                log(f"[INFO] Cursor table ensured: {SCHEMA}.{CURSOR_TABLE}")

            return
        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] ensure_schema_and_tables conn error -> rebuild")
                log_exc("[DB][RETRY] ensure_schema_and_tables", e)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue
            log(f"[DB][RETRY] ensure_schema_and_tables failed")
            log_exc("[DB][RETRY] ensure_schema_and_tables", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# ============================================
# 파일명에서 (file_day, station) 추출
# ============================================
def extract_day_station_from_path(path_str: str):
    p = Path(path_str)
    m = FILENAME_PATTERN.search(p.name)
    if not m:
        return None, None
    file_day = m.group(1)
    no = m.group(3)
    station = f"FCT{no}"
    return file_day, station


# ============================================
# DB에서 오늘자 커서 로드
# ============================================
def load_today_offsets_from_db(engine, today_ymd: str) -> dict[str, int]:
    while True:
        try:
            loaded: dict[str, int] = {}
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                rows = conn.execute(
                    text(
                        f"""
                        SELECT path, byte_offset
                        FROM {SCHEMA}.{CURSOR_TABLE}
                        WHERE file_day = :today
                        """
                    ),
                    {"today": today_ymd},
                ).fetchall()

                for r in rows:
                    loaded[str(r[0])] = int(r[1])

            if loaded:
                log(f"[INFO] Loaded cursor offsets from DB: {len(loaded)} file(s) for {today_ymd}")
            else:
                log(f"[INFO] No cursor offsets in DB for {today_ymd} (fresh start)")
            return loaded
        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] load_today_offsets conn error -> rebuild")
                log_exc("[DB][RETRY] load_today_offsets", e)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue
            log(f"[DB][RETRY] load_today_offsets failed")
            log_exc("[DB][RETRY] load_today_offsets", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# ============================================
# DB에 커서 UPSERT 저장
# ============================================
def upsert_cursor_offsets_to_db(engine, path_to_offset: dict[str, int]):
    if not path_to_offset:
        return

    records = []
    for path_str, off in path_to_offset.items():
        file_day, station = extract_day_station_from_path(path_str)
        if not file_day:
            continue
        try:
            file_size = Path(path_str).stat().st_size
        except Exception:
            file_size = None

        records.append(
            {
                "path": path_str,
                "file_day": file_day,
                "station": station,
                "byte_offset": int(off),
                "file_size": file_size,
            }
        )

    if not records:
        return

    sql = text(
        f"""
        INSERT INTO {SCHEMA}.{CURSOR_TABLE}
            (path, file_day, station, byte_offset, file_size)
        VALUES
            (:path, :file_day, :station, :byte_offset, :file_size)
        ON CONFLICT (path) DO UPDATE SET
            file_day    = EXCLUDED.file_day,
            station     = EXCLUDED.station,
            byte_offset = EXCLUDED.byte_offset,
            file_size   = EXCLUDED.file_size,
            updated_at  = now()
        """
    )

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(sql, records)
            log(f"[INFO] Cursor saved to DB: {len(records)} file(s)")
            return
        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] cursor upsert conn error -> rebuild")
                log_exc("[DB][RETRY] cursor upsert", e)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue
            log(f"[DB][RETRY] cursor upsert failed")
            log_exc("[DB][RETRY] cursor upsert", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# ============================================
# contents 정리
# ============================================
def clean_contents(raw: str, max_len: int = 75) -> str:
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


# ============================================
# 단일 로그 파일 파싱 (binary tail-follow)
# ============================================
def parse_machine_log_file_tail_binary(path_str: str, offset: int, today_ymd: str):
    path = Path(path_str)

    m = FILENAME_PATTERN.search(path.name)
    if not m:
        return {"path": path_str, "station": None, "new_offset": offset, "rows": [], "status": "SKIP_BADNAME"}

    file_ymd = m.group(1)
    station = f"FCT{m.group(3)}"

    if file_ymd != today_ymd:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": "SKIP_NOT_TODAY"}

    try:
        size = path.stat().st_size
    except Exception as e:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": f"ERROR_STAT:{type(e).__name__}"}

    if offset > size:
        offset = 0
    if offset == size:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": "SKIP_NOCHANGE"}

    rows = []
    new_offset = offset

    try:
        with path.open("rb") as f:
            f.seek(offset, os.SEEK_SET)

            while True:
                pos = f.tell()
                bline = f.readline()
                if not bline:
                    break

                # 미완성 라인(개행 없음) → offset 되돌리고 다음 루프에서 처리
                if not bline.endswith(b"\n"):
                    f.seek(pos, os.SEEK_SET)
                    break

                line = bline.decode("cp949", errors="ignore").rstrip("\r\n")
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                hms = mm.group(1)
                frac = mm.group(2) or ""
                contents_raw = mm.group(3)

                frac2 = (frac.ljust(2, "0")[:2] if frac else "00")
                end_time_str = f"{hms}.{frac2}"

                if len(end_time_str) != 11 or end_time_str[2] != ":" or end_time_str[5] != ":" or end_time_str[8] != ".":
                    continue

                contents = clean_contents(contents_raw)

                rows.append(
                    {
                        "end_day": file_ymd,
                        "station": station,
                        "end_time": end_time_str,
                        "contents": contents,
                    }
                )

            new_offset = f.tell()

    except Exception as e:
        return {"path": path_str, "station": station, "new_offset": offset, "rows": [], "status": f"ERROR_READ:{type(e).__name__}:{e}"}

    return {"path": path_str, "station": station, "new_offset": new_offset, "rows": rows, "status": "OK"}


# ============================================
# 오늘 파일 목록 수집 (오늘 YYYY/MM 폴더만)
# ============================================
def list_target_files_today_only(today_ymd: str) -> list[str]:
    files: list[str] = []

    if not BASE_DIR.exists():
        log(f"[WARN] BASE_DIR not found: {BASE_DIR}")
        return files

    year = today_ymd[:4]
    month = today_ymd[4:6]

    month_dir = BASE_DIR / year / month
    if not month_dir.exists():
        return files

    try:
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
    except Exception as e:
        log(f"[WARN] list_target_files_today_only failed")
        log_exc("[WARN] list_target_files_today_only", e)

    return files


# ============================================
# ThreadPool로 tail-follow 파싱 (재사용)
# ============================================
def collect_all_rows_threadpool(executor: ThreadPoolExecutor, file_list: list[str], today_ymd: str):
    data_by_station: dict[str, list[dict]] = {st: [] for st in TABLE_BY_STATION.keys()}
    offset_updates: dict[str, int] = {}

    if not file_list:
        return data_by_station, offset_updates

    futures = []
    for fp in file_list:
        off = int(PROCESSED_OFFSET.get(fp, 0))
        futures.append(executor.submit(parse_machine_log_file_tail_binary, fp, off, today_ymd))

    for fut in as_completed(futures):
        out = fut.result()
        fp = out["path"]
        st = out["station"]
        offset_updates[fp] = int(out["new_offset"])

        if out["status"] == "OK" and out["rows"]:
            if st in data_by_station:
                data_by_station[st].extend(out["rows"])

    return data_by_station, offset_updates


# ============================================
# DB INSERT (중복 회피)
# ============================================
def _bulk_insert_execute_values(conn, full_table: str, rows: list[dict]):
    if not rows:
        return 0

    dbapi_conn = getattr(conn.connection, "connection", None)
    if dbapi_conn is None or psycopg2 is None:
        return _bulk_insert_executemany(conn, full_table, rows)

    cur = dbapi_conn.cursor()
    sql = f"""
        INSERT INTO {full_table} (end_day, station, end_time, contents)
        VALUES %s
        ON CONFLICT (end_day, station, end_time, contents) DO NOTHING
    """
    values = [(r["end_day"], r["station"], r["end_time"], r["contents"]) for r in rows]
    page_size = int(os.getenv("INSERT_PAGE_SIZE", "2000"))

    psycopg2.extras.execute_values(cur, sql, values, page_size=page_size)  # type: ignore
    return len(values)


def _bulk_insert_executemany(conn, full_table: str, rows: list[dict]):
    if not rows:
        return 0
    insert_sql = text(
        f"""
        INSERT INTO {full_table} (end_day, station, end_time, contents)
        VALUES (:end_day, :station, :end_time, :contents)
        ON CONFLICT (end_day, station, end_time, contents) DO NOTHING
        """
    )
    conn.execute(insert_sql, rows)
    return len(rows)


def insert_to_db(engine, data_by_station: dict[str, list[dict]]):
    while True:
        try:
            total_attempt = 0
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})

                for station, rows in data_by_station.items():
                    if not rows:
                        continue

                    full_table = f"{SCHEMA}.{TABLE_BY_STATION[station]}"

                    if os.getenv("LOCAL_DEDUP", "1") == "1":
                        seen = set()
                        deduped = []
                        for r in rows:
                            k = (r["end_day"], r["station"], r["end_time"], r["contents"])
                            if k in seen:
                                continue
                            seen.add(k)
                            deduped.append(r)
                        rows = deduped

                    attempted = _bulk_insert_execute_values(conn, full_table, rows)
                    total_attempt += attempted
                    log(f"[INFO] Insert attempted {attempted} rows → {full_table} (duplicates ignored)")

            if total_attempt:
                log(f"[INFO] Insert batch done. attempted_total={total_attempt}")
            return

        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] insert_to_db conn error -> rebuild engine")
                log_exc("[DB][RETRY] insert_to_db", e)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue

            log(f"[DB][RETRY] insert_to_db failed")
            log_exc("[DB][RETRY] insert_to_db", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# ============================================
# 커서 저장 스케줄러
# ============================================
def maybe_save_cursor(engine, force: bool = False):
    global _last_cursor_save_ts, _cursor_dirty_paths

    now_ts = time_mod.time()
    if not force:
        if not _cursor_dirty_paths:
            return
        if (now_ts - _last_cursor_save_ts) < CURSOR_SAVE_INTERVAL_SEC:
            return

    if not _cursor_dirty_paths:
        return

    payload = {p: int(PROCESSED_OFFSET.get(p, 0)) for p in list(_cursor_dirty_paths)}
    upsert_cursor_offsets_to_db(engine, payload)

    _cursor_dirty_paths.clear()
    _last_cursor_save_ts = now_ts


# ============================================
# main (5초 무한루프)
# ============================================
def main():
    log("[BOOT] fct_machine_log_parser_realtime_optimized starting")
    log(f"[INFO] LOG_DIR={LOG_DIR}")

    engine = get_engine_blocking()
    ensure_schema_and_tables(engine)

    today_ymd = datetime.now().strftime("%Y%m%d")
    loaded = load_today_offsets_from_db(engine, today_ymd)
    if loaded:
        PROCESSED_OFFSET.update(loaded)

    def _on_exit():
        try:
            maybe_save_cursor(engine, force=True)
        except Exception as e:
            log("[WARN] Exit cursor save failed")
            log_exc("[WARN] Exit cursor save failed", e)

    atexit.register(_on_exit)

    log(f"[INFO] Realtime mode: binary tail-follow (byte offset), loop every {SLEEP_SEC}s")
    log("[INFO] Only today's files are tailed. Cursor is persisted to DB periodically.")
    log(f"[INFO] BASE_DIR = {BASE_DIR}")
    log(f"[INFO] Cursor save interval = {CURSOR_SAVE_INTERVAL_SEC}s")
    log(f"[INFO] Parse workers = {PARSE_WORKERS} (ThreadPoolExecutor)")
    log(f"[INFO] Engine pool minimized: pool_size=1, max_overflow=0 | session work_mem={WORK_MEM}")
    log(f"[INFO] keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}")

    with ThreadPoolExecutor(max_workers=max(1, PARSE_WORKERS)) as executor:
        while True:
            loop_t0 = time_mod.perf_counter()
            try:
                now_today = datetime.now().strftime("%Y%m%d")
                if now_today != today_ymd:
                    today_ymd = now_today
                    PROCESSED_OFFSET.clear()
                    _cursor_dirty_paths.clear()

                    engine = get_engine_blocking()
                    loaded2 = load_today_offsets_from_db(engine, today_ymd)
                    PROCESSED_OFFSET.update(loaded2)
                    log(f"[DAY-CHANGE] Reloaded cursor for {today_ymd}: {len(loaded2)} file(s)")

                t_scan0 = time_mod.perf_counter()
                file_list = list_target_files_today_only(today_ymd)
                t_scan1 = time_mod.perf_counter()

                if not file_list:
                    maybe_save_cursor(engine, force=False)
                    time_mod.sleep(SLEEP_SEC)
                    continue

                t_parse0 = time_mod.perf_counter()
                data_by_station, offset_updates = collect_all_rows_threadpool(executor, file_list, today_ymd)
                t_parse1 = time_mod.perf_counter()

                changed = 0
                for fp, new_off in offset_updates.items():
                    new_off_int = int(new_off)
                    old_off = int(PROCESSED_OFFSET.get(fp, 0))
                    PROCESSED_OFFSET[fp] = new_off_int
                    if new_off_int != old_off:
                        _cursor_dirty_paths.add(fp)
                        changed += 1

                t_ins0 = time_mod.perf_counter()
                insert_to_db(engine, data_by_station)
                t_ins1 = time_mod.perf_counter()

                t_cur0 = time_mod.perf_counter()
                maybe_save_cursor(engine, force=False)
                t_cur1 = time_mod.perf_counter()

                loop_t1 = time_mod.perf_counter()

                if os.getenv("PERF_LOG", "1") == "1":
                    total_rows = sum(len(v) for v in data_by_station.values())
                    log(
                        "[PERF] scan=%.3fs parse=%.3fs insert=%.3fs cursor=%.3fs loop=%.3fs files=%d changed=%d rows=%d"
                        % (
                            (t_scan1 - t_scan0),
                            (t_parse1 - t_parse0),
                            (t_ins1 - t_ins0),
                            (t_cur1 - t_cur0),
                            (loop_t1 - loop_t0),
                            len(file_list),
                            changed,
                            total_rows,
                        )
                    )

            except KeyboardInterrupt:
                log("[STOP] Interrupted by user.")
                try:
                    maybe_save_cursor(engine, force=True)
                except Exception as e:
                    log("[WARN] Final cursor save failed")
                    log_exc("[WARN] Final cursor save failed", e)
                break

            except Exception as e:
                if _is_connection_error(e):
                    log("[DB][RETRY] loop-level conn error -> rebuild engine")
                    log_exc("[DB][RETRY] loop-level", e)
                    _dispose_engine()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine = get_engine_blocking()
                else:
                    log("[ERROR] Loop error (continue)")
                    log_exc("[ERROR] Loop error", e)

            elapsed = time_mod.perf_counter() - loop_t0
            time_mod.sleep(max(0.0, SLEEP_SEC - elapsed))


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[STOP] user interrupted (Ctrl+C)")
    except Exception as e:
        log("[UNHANDLED] fatal error")
        log_exc("[UNHANDLED]", e)
        raise
