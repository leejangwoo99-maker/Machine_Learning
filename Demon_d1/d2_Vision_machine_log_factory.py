# -*- coding: utf-8 -*-
# d2_Vision_machine_log_factory.py
# ============================================
# Vision 머신 로그 파싱 + PostgreSQL 적재 (실시간/중복방지/오늘자만)
#
# 운영체계:
# 1) 오늘(YYYY/MM) 폴더만 스캔
# 2) ThreadPoolExecutor 재사용(파일 파싱 병렬)
# 3) 바이너리 tail(rb) + byte offset 저장/복원
# 4) pandas 제거 + psycopg2.execute_values bulk insert
# 5) 커서 저장 주기 기본 30초
# 6) 헬스 로그 DB 저장을 별도 스레드로 분리
#    - 본 데이터 적재와 health log 적재 엔진 분리
#    - queue.Queue 기반 producer-consumer
# 7) DB 접속 실패/중간 끊김 시 무한 재시도
# 8) pool 최소화 / engine 재사용 / work_mem 세션 적용
#
# 대상:
# - BASE_DIR\YYYY\MM\YYYYMMDD_Vision1_Machine_Log.txt
# - BASE_DIR\YYYY\MM\YYYYMMDD_Vision2_Machine_Log.txt
#
# DB:
# - schema: d1_machine_log
# - table : Vision1_machine_log, Vision2_machine_log
# - dedup : (end_day, station, end_time, contents) UNIQUE INDEX
#
# health log:
# - schema: k_demon_heath_check
# - table : d2_log
# - cols  : end_day, end_time, info, contents
# ============================================

from __future__ import annotations

import os
import re
import time as time_mod
import atexit
import traceback
import threading
import queue
import urllib.parse

from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


# ============================================
# psycopg2/libpq 인코딩 안정화
# ============================================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")


# ============================================
# 로그(콘솔 + 파일) / 날짜별 롤링
# ============================================
LOG_DIR = Path(os.getenv("D2_LOG_DIR", r"C:\AptivAgent\logs\d2_Vision_machine_log_factory"))
LOG_DIR.mkdir(parents=True, exist_ok=True)
_FILE_LOG_DISABLED = False


def _log_file_path():
    day = datetime.now().strftime("%Y%m%d")
    return LOG_DIR / f"d2_Vision_machine_log_factory_{day}.log"


# ============================================
# DB / 경로 설정
# ============================================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\Vision")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",  # 비밀번호 입력
}

SCHEMA = "d1_machine_log"
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "d2_log"

TABLE_BY_STATION = {
    "Vision1": '"Vision1_machine_log"',
    "Vision2": '"Vision2_machine_log"',
}

CURSOR_TABLE = "machine_log_cursor"

FILENAME_PATTERN = re.compile(r"(\d{8})_Vision([12])_Machine_Log", re.IGNORECASE)
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?\]\s*(.*)$")

SLEEP_SEC = int(os.getenv("SLEEP_SEC", "5"))
PARSE_WORKERS = int(os.getenv("PARSE_WORKERS", "2"))
CURSOR_SAVE_INTERVAL_SEC = int(os.getenv("CURSOR_SAVE_INTERVAL_SEC", "30"))

DB_RETRY_INTERVAL_SEC = int(os.getenv("DB_RETRY_INTERVAL_SEC", "5"))
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

DB_LOG_QUEUE_MAX = int(os.getenv("DB_LOG_QUEUE_MAX", "10000"))
DB_LOG_BATCH_SIZE = int(os.getenv("DB_LOG_BATCH_SIZE", "300"))
DB_LOG_FLUSH_INTERVAL_SEC = float(os.getenv("DB_LOG_FLUSH_INTERVAL_SEC", "2.0"))
DB_LOG_CONTENTS_MAXLEN = int(os.getenv("DB_LOG_CONTENTS_MAXLEN", "2000"))


# ============================================
# 전역 상태
# ============================================
PROCESSED_OFFSET: dict[str, int] = {}
_cursor_dirty_paths: set[str] = set()
_last_cursor_save_ts = 0.0

_ENGINE_MAIN = None
_ENGINE_HEALTH = None

db_log_queue: queue.Queue = queue.Queue(maxsize=max(1000, DB_LOG_QUEUE_MAX))
_db_log_enabled = True


# ============================================
# 유틸
# ============================================
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


def _normalize_info(info: str) -> str:
    if not info:
        return "info"
    s = re.sub(r"[^a-z0-9_]+", "_", info.strip().lower())
    s = s.strip("_")
    return s or "info"


def _infer_info_from_msg(msg: str) -> str:
    m = (msg or "").lower()
    if "[error]" in m or "trace" in m or "fatal" in m or "[unhandled]" in m:
        return "error"
    if "[retry]" in m or "down" in m or "failed" in m or "conn error" in m:
        return "down"
    if "[boot]" in m or "[ok]" in m:
        return "boot"
    if "[stop]" in m:
        return "stop"
    if "sleep" in m:
        return "sleep"
    if "[perf]" in m:
        return "perf"
    if "[warn]" in m:
        return "warn"
    return "info"


def _decode_log_line(bline: bytes) -> str:
    """
    원본 Vision 로그가 UTF-8 형식이므로 UTF-8 계열을 우선 사용.
    BOM 가능성 때문에 utf-8-sig 우선.
    혹시 예외 파일이 섞여 들어오면 마지막 fallback으로 cp949 사용.
    """
    for enc in ("utf-8-sig", "utf-8", "cp949"):
        try:
            return bline.decode(enc)
        except UnicodeDecodeError:
            continue
    return bline.decode("utf-8", errors="ignore")


# ============================================
# 로그
# ============================================
def _enqueue_db_log(info: str, contents: str):
    global _db_log_enabled
    if not _db_log_enabled:
        return

    now = datetime.now()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": (contents or "")[:DB_LOG_CONTENTS_MAXLEN],
    }

    try:
        db_log_queue.put_nowait(row)
    except queue.Full:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] [WARN] db_log_queue full. health log dropped.", flush=True)


def log(msg: str, info: str | None = None):
    global _FILE_LOG_DISABLED

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"

    print(line, flush=True)

    if not _FILE_LOG_DISABLED:
        try:
            fp = _log_file_path()
            with fp.open("a", encoding="utf-8", errors="ignore") as f:
                f.write(line + "\n")
        except Exception as e:
            _FILE_LOG_DISABLED = True
            print(f"[{ts}] [WARN] file logging disabled due to error: {type(e).__name__}: {e}", flush=True)

    try:
        tag = _normalize_info(info) if info else _infer_info_from_msg(msg)
        _enqueue_db_log(tag, msg)
    except Exception:
        pass


def log_exc(prefix: str, e: Exception):
    log(f"{prefix}: {type(e).__name__}: {repr(e)}", info="error")
    tb = traceback.format_exc()
    for line in tb.rstrip().splitlines():
        log(f"{prefix} TRACE: {line}", info="error")


# ============================================
# Engine 생성 / 관리
# ============================================
def _build_engine(cfg=DB_CONFIG, application_name: str = "vision_machine_log_parser"):
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
            "connect_timeout": 10,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": application_name,
        },
    )
    return eng


def _dispose_engine_main():
    global _ENGINE_MAIN
    try:
        if _ENGINE_MAIN is not None:
            _ENGINE_MAIN.dispose()
    except Exception:
        pass
    _ENGINE_MAIN = None


def _dispose_engine_health():
    global _ENGINE_HEALTH
    try:
        if _ENGINE_HEALTH is not None:
            _ENGINE_HEALTH.dispose()
    except Exception:
        pass
    _ENGINE_HEALTH = None


def get_engine_main_blocking():
    global _ENGINE_MAIN

    while _ENGINE_MAIN is not None:
        try:
            with _ENGINE_MAIN.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            return _ENGINE_MAIN
        except Exception as e:
            log("[DB][RETRY] main engine ping failed -> rebuild", info="down")
            log_exc("[DB][RETRY] main ping error", e)
            _dispose_engine_main()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE_MAIN = _build_engine(DB_CONFIG, "vision_machine_log_parser_main")
            with _ENGINE_MAIN.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            log(
                f"[DB][OK] main engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})",
                info="boot",
            )
            log(f"[INFO] MAIN DB = {_masked_db_info(DB_CONFIG)}", info="info")
            return _ENGINE_MAIN
        except Exception as e:
            log("[DB][RETRY] main engine create/connect failed", info="down")
            log_exc("[DB][RETRY] main connect error", e)
            _dispose_engine_main()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def get_engine_health_blocking():
    global _ENGINE_HEALTH

    while _ENGINE_HEALTH is not None:
        try:
            with _ENGINE_HEALTH.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            return _ENGINE_HEALTH
        except Exception as e:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [WARN] health engine ping failed: {e}", flush=True)
            _dispose_engine_health()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE_HEALTH = _build_engine(DB_CONFIG, "vision_machine_log_parser_health")
            with _ENGINE_HEALTH.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            log("[DB][OK] health engine ready (dedicated for health log)", info="boot")
            return _ENGINE_HEALTH
        except Exception as e:
            print(
                f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [WARN] health engine create/connect failed: "
                f"{type(e).__name__}: {e}",
                flush=True
            )
            _dispose_engine_health()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# ============================================
# DDL
# ============================================
CREATE_TABLE_TEMPLATE = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id          BIGSERIAL PRIMARY KEY,
    end_day     VARCHAR(8),
    station     VARCHAR(10),
    end_time    VARCHAR(12),
    contents    VARCHAR(200)
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

CREATE_HEALTH_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {HEALTH_SCHEMA}.{HEALTH_TABLE} (
    id        BIGSERIAL PRIMARY KEY,
    end_day   VARCHAR(8)  NOT NULL,
    end_time  VARCHAR(8)  NOT NULL,
    info      VARCHAR(32) NOT NULL,
    contents  TEXT
);
"""

CREATE_HEALTH_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{HEALTH_SCHEMA}_{HEALTH_TABLE}_day_time
ON {HEALTH_SCHEMA}.{HEALTH_TABLE} (end_day, end_time);
"""


def ensure_schema_and_tables(engine_main, engine_health):
    while True:
        try:
            with engine_main.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

                for station, table_quoted in TABLE_BY_STATION.items():
                    ddl = CREATE_TABLE_TEMPLATE.format(schema=SCHEMA, table=table_quoted)
                    conn.execute(text(ddl))
                    log(f"[INFO] Table ensured: {SCHEMA}.{table_quoted}", info="info")

                    idx_name = f"ux_{SCHEMA}_{_index_safe_name(table_quoted)}_dedup"
                    conn.execute(
                        text(
                            f"""
                            CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                            ON {SCHEMA}.{table_quoted} (end_day, station, end_time, contents)
                            """
                        )
                    )
                    log(f"[INFO] Unique index ensured: {idx_name}", info="info")

                conn.execute(text(CREATE_CURSOR_TABLE_SQL))
                conn.execute(text(CREATE_CURSOR_DAY_INDEX_SQL))
                log(f"[INFO] Cursor table ensured: {SCHEMA}.{CURSOR_TABLE}", info="info")

            with engine_health.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA}"))
                conn.execute(text(CREATE_HEALTH_TABLE_SQL))
                conn.execute(text(CREATE_HEALTH_INDEX_SQL))
                log(f"[INFO] Health log table ensured: {HEALTH_SCHEMA}.{HEALTH_TABLE}", info="info")

            return

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] ensure_schema_and_tables conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] ensure_schema_and_tables", e)
                _dispose_engine_main()
                _dispose_engine_health()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine_main = get_engine_main_blocking()
                engine_health = get_engine_health_blocking()
                continue

            log("[DB][RETRY] ensure_schema_and_tables failed", info="down")
            log_exc("[DB][RETRY] ensure_schema_and_tables", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine_main = get_engine_main_blocking()
            engine_health = get_engine_health_blocking()


# ============================================
# 파일명에서 file_day station 추출
# ============================================
def extract_day_station_from_path(path_str: str):
    p = Path(path_str)
    m = FILENAME_PATTERN.search(p.name)
    if not m:
        return None, None
    file_day = m.group(1)
    station = f"Vision{m.group(2)}"
    return file_day, station


# ============================================
# 커서 로드 / 저장
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
                log(f"[INFO] Loaded cursor offsets from DB: {len(loaded)} file(s) for {today_ymd}", info="info")
            else:
                log(f"[INFO] No cursor offsets in DB for {today_ymd} (fresh start)", info="info")
            return loaded

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] load_today_offsets conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] load_today_offsets", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue

            log("[DB][RETRY] load_today_offsets failed", info="down")
            log_exc("[DB][RETRY] load_today_offsets", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_main_blocking()


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
            log(f"[INFO] Cursor saved to DB: {len(records)} file(s)", info="info")
            return

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] cursor upsert conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] cursor upsert", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue

            log("[DB][RETRY] cursor upsert failed", info="down")
            log_exc("[DB][RETRY] cursor upsert", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_main_blocking()


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
# contents 정리
# ============================================
def clean_contents(raw: str, max_len: int = 200) -> str:
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


# ============================================
# 단일 로그 파일 파싱 binary tail follow
# ============================================
def parse_machine_log_file_tail_binary(path_str: str, offset: int, today_ymd: str):
    path = Path(path_str)

    m = FILENAME_PATTERN.search(path.name)
    if not m:
        return {"path": path_str, "station": None, "new_offset": offset, "rows": [], "status": "SKIP_BADNAME"}

    file_ymd = m.group(1)
    station = f"Vision{m.group(2)}"

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

                # 마지막 줄이 아직 쓰는 중이면 보류
                if not bline.endswith(b"\n"):
                    f.seek(pos, os.SEEK_SET)
                    break

                line = _decode_log_line(bline).rstrip("\r\n")
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

                contents = clean_contents(contents_raw, max_len=200)

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
# 오늘 파일 목록 수집
# ============================================
def list_target_files_today_only(today_ymd: str) -> list[str]:
    files: list[str] = []

    if not BASE_DIR.exists():
        log(f"[WARN] BASE_DIR not found: {BASE_DIR}", info="down")
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
        log("[WARN] list_target_files_today_only failed", info="error")
        log_exc("[WARN] list_target_files_today_only", e)

    return files


# ============================================
# ThreadPool로 tail follow 파싱
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
# DB INSERT
# ============================================
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
    page_size = int(os.getenv("INSERT_PAGE_SIZE", "500"))

    psycopg2.extras.execute_values(cur, sql, values, page_size=page_size)  # type: ignore
    return len(values)


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
                    log(f"[INFO] Insert attempted {attempted} rows → {full_table} (duplicates ignored)", info="info")

            if total_attempt:
                log(f"[INFO] Insert batch done. attempted_total={total_attempt}", info="info")
            return

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] insert_to_db conn error -> rebuild main engine", info="down")
                log_exc("[DB][RETRY] insert_to_db", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue

            log("[DB][RETRY] insert_to_db failed", info="down")
            log_exc("[DB][RETRY] insert_to_db", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_main_blocking()


# ============================================
# Health log worker
# ============================================
class HealthLogWorker:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="HealthLogWorker", daemon=True)
        self._local_buffer: list[dict] = []

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def join(self, timeout: float | None = None):
        self.thread.join(timeout=timeout)

    def _flush_batch(self, batch: list[dict]):
        if not batch:
            return

        while True:
            try:
                engine = get_engine_health_blocking()
                with engine.begin() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    conn.execute(
                        text(
                            f"""
                            INSERT INTO {HEALTH_SCHEMA}.{HEALTH_TABLE}
                            (end_day, end_time, info, contents)
                            VALUES
                            (:end_day, :end_time, :info, :contents)
                            """
                        ),
                        batch,
                    )
                return

            except Exception as e:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [WARN] health log flush failed: {type(e).__name__}: {e}", flush=True)

                if _is_connection_error(e):
                    _dispose_engine_health()

                time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    def _drain_queue(self, max_items: int):
        drained = 0
        while drained < max_items:
            try:
                item = db_log_queue.get_nowait()
                self._local_buffer.append(item)
                drained += 1
            except queue.Empty:
                break

    def _run(self):
        last_flush_ts = time_mod.time()

        while not self.stop_event.is_set():
            try:
                timeout = max(0.2, DB_LOG_FLUSH_INTERVAL_SEC / 2.0)
                try:
                    item = db_log_queue.get(timeout=timeout)
                    self._local_buffer.append(item)
                except queue.Empty:
                    pass

                self._drain_queue(max_items=max(0, DB_LOG_BATCH_SIZE - len(self._local_buffer)))

                now_ts = time_mod.time()
                need_flush = False

                if len(self._local_buffer) >= DB_LOG_BATCH_SIZE:
                    need_flush = True
                elif self._local_buffer and (now_ts - last_flush_ts) >= DB_LOG_FLUSH_INTERVAL_SEC:
                    need_flush = True

                if need_flush:
                    batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                    del self._local_buffer[:len(batch)]
                    self._flush_batch(batch)
                    last_flush_ts = now_ts

            except Exception as e:
                ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [WARN] HealthLogWorker loop error: {type(e).__name__}: {e}", flush=True)
                time_mod.sleep(1.0)

        # 종료 시 남은 queue + local buffer flush
        try:
            while True:
                try:
                    item = db_log_queue.get_nowait()
                    self._local_buffer.append(item)
                except queue.Empty:
                    break

            while self._local_buffer:
                batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                del self._local_buffer[:len(batch)]
                self._flush_batch(batch)

        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] HealthLogWorker final flush error: {type(e).__name__}: {e}", flush=True)


# ============================================
# main
# ============================================
def main():
    global _cursor_dirty_paths

    log("[BOOT] d2_Vision_machine_log_factory starting", info="boot")
    log(f"[INFO] LOG_DIR={LOG_DIR}", info="info")
    log(f"[INFO] BASE_DIR = {BASE_DIR}", info="info")
    log(f"[INFO] Realtime mode: binary tail-follow (byte offset), loop every {SLEEP_SEC}s", info="info")
    log("[INFO] Only today's files are tailed. Cursor is persisted to DB periodically.", info="info")
    log(f"[INFO] Cursor save interval = {CURSOR_SAVE_INTERVAL_SEC}s", info="info")
    log(f"[INFO] Parse workers = {PARSE_WORKERS} (ThreadPoolExecutor)", info="info")
    log(f"[INFO] MAIN engine pool minimized: pool_size=1, max_overflow=0 | session work_mem={WORK_MEM}", info="info")
    log(f"[INFO] HEALTH engine dedicated: pool_size=1, max_overflow=0 | flush_interval={DB_LOG_FLUSH_INTERVAL_SEC}s", info="info")
    log(f"[INFO] health log queue max={DB_LOG_QUEUE_MAX}, batch={DB_LOG_BATCH_SIZE}", info="info")
    log(f"[INFO] keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}", info="info")

    engine_main = get_engine_main_blocking()
    engine_health = get_engine_health_blocking()
    ensure_schema_and_tables(engine_main, engine_health)

    health_worker = HealthLogWorker()
    health_worker.start()
    log("[INFO] HealthLogWorker started", info="info")

    today_ymd = datetime.now().strftime("%Y%m%d")
    loaded = load_today_offsets_from_db(engine_main, today_ymd)
    if loaded:
        PROCESSED_OFFSET.update(loaded)

    def _on_exit():
        try:
            eng = get_engine_main_blocking()
            maybe_save_cursor(eng, force=True)
        except Exception as e:
            log("[WARN] Exit cursor save failed", info="error")
            log_exc("[WARN] Exit cursor save failed", e)

        try:
            health_worker.stop()
            health_worker.join(timeout=15.0)
        except Exception as e:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] health worker shutdown failed: {e}", flush=True)

        _dispose_engine_main()
        _dispose_engine_health()

    atexit.register(_on_exit)

    with ThreadPoolExecutor(max_workers=max(1, PARSE_WORKERS)) as executor:
        while True:
            loop_t0 = time_mod.perf_counter()

            try:
                now_today = datetime.now().strftime("%Y%m%d")
                if now_today != today_ymd:
                    today_ymd = now_today
                    PROCESSED_OFFSET.clear()
                    _cursor_dirty_paths.clear()

                    engine_main = get_engine_main_blocking()
                    loaded2 = load_today_offsets_from_db(engine_main, today_ymd)
                    PROCESSED_OFFSET.update(loaded2)
                    log(f"[DAY-CHANGE] Reloaded cursor for {today_ymd}: {len(loaded2)} file(s)", info="info")

                t_scan0 = time_mod.perf_counter()
                file_list = list_target_files_today_only(today_ymd)
                t_scan1 = time_mod.perf_counter()

                if not file_list:
                    maybe_save_cursor(engine_main, force=False)
                    if os.getenv("SLEEP_LOG", "0") == "1":
                        log("[INFO] no target file, sleep", info="sleep")
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
                insert_to_db(engine_main, data_by_station)
                t_ins1 = time_mod.perf_counter()

                t_cur0 = time_mod.perf_counter()
                maybe_save_cursor(engine_main, force=False)
                t_cur1 = time_mod.perf_counter()

                loop_t1 = time_mod.perf_counter()

                if os.getenv("PERF_LOG", "1") == "1":
                    total_rows = sum(len(v) for v in data_by_station.values())
                    log(
                        "[PERF] scan=%.3fs parse=%.3fs insert=%.3fs cursor=%.3fs loop=%.3fs files=%d changed=%d rows=%d qsize=%d"
                        % (
                            (t_scan1 - t_scan0),
                            (t_parse1 - t_parse0),
                            (t_ins1 - t_ins0),
                            (t_cur1 - t_cur0),
                            (loop_t1 - loop_t0),
                            len(file_list),
                            changed,
                            total_rows,
                            db_log_queue.qsize(),
                        ),
                        info="perf",
                    )

            except KeyboardInterrupt:
                log("[STOP] Interrupted by user.", info="stop")

                try:
                    maybe_save_cursor(engine_main, force=True)
                except Exception as e:
                    log("[WARN] Final cursor save failed", info="error")
                    log_exc("[WARN] Final cursor save failed", e)

                try:
                    health_worker.stop()
                    health_worker.join(timeout=15.0)
                    log("[INFO] HealthLogWorker stopped", info="info")
                except Exception as e:
                    log("[WARN] HealthLogWorker stop failed", info="error")
                    log_exc("[WARN] HealthLogWorker stop failed", e)

                break

            except Exception as e:
                if _is_connection_error(e):
                    log("[DB][RETRY] loop-level conn error -> rebuild main engine", info="down")
                    log_exc("[DB][RETRY] loop-level", e)
                    _dispose_engine_main()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    engine_main = get_engine_main_blocking()
                else:
                    log("[ERROR] Loop error (continue)", info="error")
                    log_exc("[ERROR] Loop error", e)

            elapsed = time_mod.perf_counter() - loop_t0
            sleep_sec = max(0.0, SLEEP_SEC - elapsed)
            if sleep_sec > 0 and os.getenv("SLEEP_LOG", "0") == "1":
                log(f"[INFO] sleep {sleep_sec:.2f}s", info="sleep")
            time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[STOP] user interrupted (Ctrl+C)", info="stop")
    except Exception as e:
        log("[UNHANDLED] fatal error", info="error")
        log_exc("[UNHANDLED]", e)
        raise