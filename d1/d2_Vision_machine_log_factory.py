# -*- coding: utf-8 -*-
# ============================================
# d2_Vision_machine_log_factory.py
# Vision Machine Log Parser (Realtime + Thread2 + TodayOnly) - OPTION A (DB가 중복 영구 차단)
#
# [핵심 운영 목표]
# - 오늘 파일만 파싱 (Vision\YYYY\MM만 정확히 스캔)
# - 스레드 2개 고정
# - offset 기반 tail-follow (신규 라인만 처리)
# - 중복은 DB에서 영구 차단(UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - 어떤 상황에서도 프로세스가 멈추지 않도록(예외는 로그 후 continue)
# - INIT(스키마/테이블/인덱스) 실패 시 재시도
# - 날짜 변경 시 캐시 초기화 + 해당일 커서(offset) DB 로드
# - 커서(offset) DB 주기 저장/재시작 로드
#
# [최종 안정화 포인트]
# - ✅ 무한 루프 인터벌 5초
# - ✅ DB 서버 접속 실패/실행 중 끊김: engine dispose 후 "연결 성공까지" 무한 재접속(블로킹)
# - ✅ 상시 연결 1개로 고정(풀 최소화): pool_size=1, max_overflow=0
# - ✅ work_mem 폭증 방지: connect 시 options로 -c work_mem=... (SET 바인드 파라미터 제거)
# - ✅ INSERT 재시도 정책:
#     * 연결 오류만 무한 재시도
#     * 데이터/제약/인코딩 등 비연결 오류는 해당 배치 스킵(루프는 계속)
# - ✅ 파일 로그 남김(콘솔 + 파일 동시 로깅, 일자별 롤링)
#
# [추가 반영: 데몬 헬스 로그 DB 저장]
# - ✅ 스키마: k_demon_heath_check (없으면 생성)
# - ✅ 테이블: d2_log (없으면 생성)
# - ✅ 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
# - ✅ end_day, end_time, info, contents 순서로 DataFrame화 후 저장
# ============================================

import re
import sys
import os
import time as time_mod
import atexit
import traceback
from pathlib import Path
from datetime import datetime
from multiprocessing import freeze_support
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
import urllib.parse


# ============================================
# ✅ libpq/psycopg2 인코딩 안정화(EXE 환경)
# ============================================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")


# ============================================
# 1) 기본 설정
# ============================================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\Vision")

DB_CONFIG = {
    "host": "100.105.75.47",
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

THREAD_WORKERS = 2
SLEEP_SEC = 5
LOG_EVERY_LOOP = 10

# offset 캐시: path -> 마지막 읽은 파일 byte 위치
PROCESSED_OFFSET = {}

FILENAME_PATTERN = re.compile(r"(\d{8})_Vision([12])_Machine_Log", re.IGNORECASE)
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$")


# ============================================
# 2) 커서 DB 저장 설정
# ============================================
CURSOR_TABLE = "machine_log_cursor"
CURSOR_SAVE_INTERVAL_SEC = 5
_last_cursor_save_ts = 0.0
_cursor_dirty_paths = set()


# ============================================
# 2-1) 데몬 헬스 로그 DB 저장 설정 (신규)
# ============================================
HEALTH_SCHEMA = "k_demon_heath_check"  # 사용자 사양 그대로 사용
HEALTH_TABLE = "d2_log"

RUNTIME_LOG_SAVE_INTERVAL_SEC = 5
_last_runtime_log_save_ts = 0.0
_runtime_log_buffer = []  # [{"end_day","end_time","info","contents"}, ...]


# ============================================
# 3) DB 재시도 / 풀 최소화 / work_mem 제한
# ============================================
DB_RETRY_INTERVAL_SEC = 5

# 예: 4MB, 16MB, 128kB, 1GB
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# keepalive (기본 ON, 필요 시 OFF)
ENABLE_PG_KEEPALIVE = os.getenv("ENABLE_PG_KEEPALIVE", "1").strip() not in ("0", "false", "False", "OFF", "off")
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

_ENGINE = None


# ============================================
# 4) ✅ 로그(콘솔 + 파일 동시) / 일자별 롤링
# ============================================
LOG_DIR = Path(os.getenv("D2_LOG_DIR", r"C:\AptivAgent\logs\d2_Vision_machine_log_factory"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

_FILE_LOG_DISABLED = False


def _console_print(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _log_file_path():
    day = datetime.now().strftime("%Y%m%d")
    return LOG_DIR / f"d2_Vision_machine_log_factory_{day}.log"


def _normalize_info_tag(info: str) -> str:
    s = (info or "info").strip().lower()
    if not s:
        s = "info"
    return s


def _append_runtime_log(info: str, contents: str):
    """
    사양:
    - end_day: yyyymmdd
    - end_time: hh:mi:ss
    - info: 소문자
    - contents: 나머지 내용
    """
    now = datetime.now()
    rec = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": _normalize_info_tag(info),
        "contents": str(contents or "")[:2000],  # 안전 길이 제한
    }
    _runtime_log_buffer.append(rec)


def log(msg: str, info: str = "info", to_db: bool = True):
    """
    - 콘솔 + 파일 동시 기록
    - 파일은 append, UTF-8
    - 파일 기록 실패가 반복되면(_FILE_LOG_DISABLED) 콘솔만 유지
    - 필요시 런타임 로그 버퍼에 적재 (나중에 DB flush)
    """
    global _FILE_LOG_DISABLED

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"

    # 1) 콘솔
    print(line, flush=True)

    # 2) 파일
    if not _FILE_LOG_DISABLED:
        try:
            fp = _log_file_path()
            with fp.open("a", encoding="utf-8", errors="ignore") as f:
                f.write(line + "\n")
        except Exception as e:
            _FILE_LOG_DISABLED = True
            print(f"[{ts}] [WARN] file logging disabled due to error: {type(e).__name__}: {e}", flush=True)

    # 3) DB 버퍼
    if to_db:
        try:
            _append_runtime_log(info=info, contents=msg)
        except Exception:
            # 절대 메인 로직 영향 주지 않음
            pass


def log_exc(prefix: str, e: Exception, info: str = "error"):
    """
    예외 상세(traceback)까지 파일로 남김 + DB 런타임 로그 버퍼 적재
    """
    log(f"{prefix}: {type(e).__name__}: {repr(e)}", info=info, to_db=True)
    tb = traceback.format_exc()
    for line in tb.rstrip().splitlines():
        log(f"{prefix} TRACE: {line}", info=info, to_db=True)


def pause_console():
    try:
        input("\n[END] 작업이 종료되었습니다. 콘솔을 닫으려면 Enter를 누르세요...")
    except Exception:
        pass


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


def _build_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]

    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        "?connect_timeout=5"
    )
    log(f"[INFO] DB={_masked_db_info(config)}", info="info")

    connect_args = {
        "connect_timeout": 5,
        "application_name": "d2_Vision_machine_log_factory",
        "options": f"-c work_mem={WORK_MEM}",
    }

    if ENABLE_PG_KEEPALIVE:
        connect_args.update({
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
        })

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args=connect_args,
    )


def get_engine_blocking():
    """
    ✅ DB 서버 접속 실패/실행 중 끊김 시 무한 재시도(연결 성공까지 블로킹)
    ✅ Engine 1개만 생성/재사용(풀 최소화)
    """
    global _ENGINE

    # 1) 기존 엔진 ping
    while _ENGINE is not None:
        try:
            with _ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE
        except Exception as e:
            log("[DB][RETRY] engine ping failed -> rebuild", info="down")
            log_exc("[DB][RETRY] ping error", e, info="error")
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    # 2) 새 엔진 생성/연결 재시도
    while True:
        try:
            _ENGINE = _build_engine(DB_CONFIG)
            with _ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))

            ka = (
                f"{PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}"
                if ENABLE_PG_KEEPALIVE else "off"
            )
            log(f"[DB][OK] engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, keepalive={ka})", info="info")
            return _ENGINE

        except Exception as e:
            log("[DB][RETRY] engine create/connect failed", info="down")
            log_exc("[DB][RETRY] connect error", e, info="error")
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# ============================================
# 5) 스키마/테이블/인덱스 (중복 영구 차단)
# ============================================
CREATE_CURSOR_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{CURSOR_TABLE} (
    path         TEXT PRIMARY KEY,
    file_day     VARCHAR(8) NOT NULL,
    station      VARCHAR(10),
    byte_offset  BIGINT NOT NULL,
    file_size    BIGINT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

CREATE_CURSOR_DAY_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{SCHEMA_NAME}_{CURSOR_TABLE}_day
ON {SCHEMA_NAME}.{CURSOR_TABLE} (file_day);
"""

# 신규: 헬스 로그 테이블
CREATE_HEALTH_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {HEALTH_SCHEMA}.{HEALTH_TABLE} (
    end_day   VARCHAR(8)  NOT NULL,
    end_time  VARCHAR(8)  NOT NULL,
    info      VARCHAR(50) NOT NULL,
    contents  TEXT
);
"""

CREATE_HEALTH_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{HEALTH_SCHEMA}_{HEALTH_TABLE}_day_time
ON {HEALTH_SCHEMA}.{HEALTH_TABLE} (end_day, end_time);
"""


def ensure_schema_tables(engine):
    """
    INIT 실패 시 재시도(블로킹)
    - 테이블 생성
    - UNIQUE INDEX 생성(중복 영구 차단)
    - 커서 테이블 생성
    - 헬스 로그 스키마/테이블 생성
    """
    ddl_template = """
    CREATE TABLE IF NOT EXISTS {schema}."{table}" (
        end_day     VARCHAR(8),
        station     VARCHAR(10),
        end_time    VARCHAR(12),
        contents    VARCHAR(200)
    );
    """
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME}"))
                for _, tbl in TABLE_MAP.items():
                    conn.execute(text(ddl_template.format(schema=SCHEMA_NAME, table=tbl)))

                    idx_name = f'ux_{SCHEMA_NAME}_{tbl}_dedup'
                    conn.execute(text(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                        ON {SCHEMA_NAME}."{tbl}" (end_day, station, end_time, contents)
                    """))

                conn.execute(text(CREATE_CURSOR_TABLE_SQL))
                conn.execute(text(CREATE_CURSOR_DAY_INDEX_SQL))

                # 헬스 로그 스키마/테이블
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA}"))
                conn.execute(text(CREATE_HEALTH_TABLE_SQL))
                conn.execute(text(CREATE_HEALTH_INDEX_SQL))

            log("[INFO] Schema/tables/indexes ready (incl. cursor + health log table).", info="info")
            return

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] ensure_schema_tables conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] ensure_schema_tables", e, info="error")
                _dispose_engine()
            else:
                log("[FATAL-INIT][RETRY] ensure_schema_tables failed", info="error")
                log_exc("[FATAL-INIT][RETRY] ensure_schema_tables", e, info="error")

            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# ============================================
# 6) 커서 유틸
# ============================================
def extract_day_station_from_path(path_str: str):
    p = Path(path_str)
    m = FILENAME_PATTERN.search(p.name)
    if not m:
        return None, None
    file_day = m.group(1)
    vision_no = m.group(2)
    station = f"Vision{vision_no}"
    return file_day, station


def load_today_offsets_from_db(engine, today_ymd: str):
    while True:
        try:
            loaded = {}
            with engine.begin() as conn:
                rows = conn.execute(
                    text(f"""
                        SELECT path, byte_offset
                        FROM {SCHEMA_NAME}.{CURSOR_TABLE}
                        WHERE file_day = :today
                    """),
                    {"today": today_ymd}
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
                log_exc("[DB][RETRY] load_today_offsets", e, info="error")
                _dispose_engine()
            else:
                log("[DB][RETRY] load_today_offsets failed", info="error")
                log_exc("[DB][RETRY] load_today_offsets", e, info="error")

            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


def upsert_cursor_offsets_to_db(engine, path_to_offset: dict):
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

        records.append({
            "path": path_str,
            "file_day": file_day,
            "station": station,
            "byte_offset": int(off),
            "file_size": file_size,
        })

    if not records:
        return

    sql = text(f"""
        INSERT INTO {SCHEMA_NAME}.{CURSOR_TABLE}
            (path, file_day, station, byte_offset, file_size)
        VALUES
            (:path, :file_day, :station, :byte_offset, :file_size)
        ON CONFLICT (path) DO UPDATE SET
            file_day    = EXCLUDED.file_day,
            station     = EXCLUDED.station,
            byte_offset = EXCLUDED.byte_offset,
            file_size   = EXCLUDED.file_size,
            updated_at  = now()
    """)

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(sql, records)
            log(f"[INFO] Cursor saved to DB: {len(records)} file(s)", info="info")
            return
        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] cursor upsert conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] cursor upsert", e, info="error")
                _dispose_engine()
            else:
                log("[DB][RETRY] cursor upsert failed", info="error")
                log_exc("[DB][RETRY] cursor upsert", e, info="error")

            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


def maybe_save_cursor(engine, force=False):
    global _last_cursor_save_ts, _cursor_dirty_paths

    now_ts = time_mod.time()
    if not force:
        if not _cursor_dirty_paths:
            return
        if (now_ts - _last_cursor_save_ts) < CURSOR_SAVE_INTERVAL_SEC:
            return

    if not _cursor_dirty_paths:
        return

    payload = {p: PROCESSED_OFFSET.get(p, 0) for p in list(_cursor_dirty_paths)}
    upsert_cursor_offsets_to_db(engine, payload)

    _cursor_dirty_paths.clear()
    _last_cursor_save_ts = now_ts


# ============================================
# 6-1) 런타임 로그(DB) 저장 유틸 (신규)
# ============================================
def flush_runtime_logs_to_db(engine, force=False):
    """
    - 버퍼를 DataFrame으로 만든 뒤
      [end_day, end_time, info, contents] 순서로 DB 저장
    - 연결 오류는 무한 재시도
    - 비연결 오류는 해당 flush만 스킵(메인 루프 계속)
    """
    global _last_runtime_log_save_ts, _runtime_log_buffer

    if not _runtime_log_buffer:
        return

    now_ts = time_mod.time()
    if not force and (now_ts - _last_runtime_log_save_ts) < RUNTIME_LOG_SAVE_INTERVAL_SEC:
        return

    # 사양: 컬럼 순서 고정 DataFrame
    df = pd.DataFrame(_runtime_log_buffer, columns=["end_day", "end_time", "info", "contents"])
    if df.empty:
        _last_runtime_log_save_ts = now_ts
        return

    sql = text(f"""
        INSERT INTO {HEALTH_SCHEMA}.{HEALTH_TABLE}
            (end_day, end_time, info, contents)
        VALUES
            (:end_day, :end_time, :info, :contents)
    """)

    records = df.to_dict(orient="records")

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(sql, records)

            # 성공 시 버퍼 비우기
            saved_n = len(records)
            _runtime_log_buffer = []
            _last_runtime_log_save_ts = now_ts

            # 재귀 방지: DB 로그 저장 알림은 DB버퍼 미적재
            log(f"[INFO] runtime logs saved to DB: {saved_n}", info="info", to_db=False)
            return

        except Exception as e:
            if _is_connection_error(e):
                _console_print("[DB][RETRY] runtime log flush conn error -> rebuild")
                _console_print(f"[DB][RETRY] runtime log flush: {type(e).__name__}: {e}")
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue

            # 비연결 오류: 이번 flush만 포기(버퍼는 유지해서 다음 주기 재시도 가능)
            _console_print(f"[DB][SKIP] runtime log flush non-conn error: {type(e).__name__}: {e}")
            _last_runtime_log_save_ts = now_ts
            return


# ============================================
# 7) 파일 파싱 유틸
# ============================================
def clean_contents(raw: str, max_len: int = 200):
    s = raw.replace("\n", " ").replace("\r", " ").replace("\t", " ")
    s = " ".join(s.split())
    return s[:max_len].strip()


def _open_text_file(path: Path):
    try:
        return path.open("r", encoding="utf-8", errors="ignore")
    except Exception:
        return path.open("r", encoding="cp949", errors="ignore")


def list_target_files_today(base_dir: Path, today_ymd: str):
    files = []
    if not base_dir.exists():
        log(f"[WARN] BASE_DIR not found: {base_dir}", info="warn")
        return files

    now = datetime.now()
    year_dir = base_dir / f"{now.year:04d}"
    month_dir = year_dir / f"{now.month:02d}"

    if not month_dir.exists():
        log(f"[WARN] month_dir not found: {month_dir}", info="warn")
        return files

    candidates = [
        month_dir / f"{today_ymd}_Vision1_Machine_Log.txt",
        month_dir / f"{today_ymd}_Vision2_Machine_Log.txt",
    ]

    for fp in candidates:
        if fp.is_file():
            files.append(str(fp))

    if not files:
        for fp in month_dir.glob(f"{today_ymd}_Vision*_Machine_Log.*"):
            if fp.is_file() and FILENAME_PATTERN.search(fp.name):
                files.append(str(fp))

    return sorted(set(files))


def parse_machine_log_file_tail(path_str: str, today_ymd: str, offset: int):
    file_path = Path(path_str)

    m = FILENAME_PATTERN.search(file_path.name)
    if not m:
        return [], offset, None, path_str, "skip_badname"

    file_ymd = m.group(1)
    vision_no = m.group(2)
    station = f"Vision{vision_no}"

    if file_ymd != today_ymd:
        return [], offset, station, path_str, "skip_not_today"

    try:
        size = file_path.stat().st_size
    except Exception as e:
        return [], offset, station, path_str, f"error_stat:{type(e).__name__.lower()}"

    if offset > size:
        offset = 0

    if offset == size:
        return [], offset, station, path_str, "sleep"

    result = []
    new_offset = offset

    try:
        with _open_text_file(file_path) as f:
            f.seek(offset, os.SEEK_SET)

            for line in f:
                line = line.rstrip("\n")
                m2 = LINE_PATTERN.match(line)
                if not m2:
                    continue

                end_time_str = m2.group(1)
                contents_raw = m2.group(2)

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
        log(f"[WARN] Failed to read(tail): {file_path} / {type(e).__name__}: {e}", info="error")
        return [], offset, station, path_str, f"error_read:{type(e).__name__.lower()}"

    return result, int(new_offset), station, path_str, "ok"


# ============================================
# 8) DB INSERT (중복 무시) + 재시도 정책(연결 오류만 무한)
# ============================================
def insert_to_db(engine, df: pd.DataFrame):
    if df is None or df.empty:
        return 0

    df = df.sort_values(["end_day", "end_time"]).reset_index(drop=True)

    insert_sql_map = {
        st: text(f"""
            INSERT INTO {SCHEMA_NAME}."{tbl}" (end_day, station, end_time, contents)
            VALUES (:end_day, :station, :end_time, :contents)
            ON CONFLICT (end_day, station, end_time, contents)
            DO NOTHING
        """)
        for st, tbl in TABLE_MAP.items()
    }

    while True:
        try:
            inserted_attempt = 0
            with engine.begin() as conn:
                for st, _tbl in TABLE_MAP.items():
                    sub = df[df["station"] == st]
                    if sub.empty:
                        continue
                    records = sub.to_dict(orient="records")
                    conn.execute(insert_sql_map[st], records)
                    inserted_attempt += len(records)

            return inserted_attempt

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] insert conn error -> rebuild engine", info="down")
                log_exc("[DB][RETRY] insert", e, info="error")
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue

            log("[DB][SKIP] insert non-conn error -> drop this batch", info="error")
            log_exc("[DB][SKIP] insert", e, info="error")
            return 0


# ============================================
# 9) main loop
# ============================================
def main():
    global _last_cursor_save_ts

    log("### BUILD: d2 Vision parser OPTION-A (TAIL-FOLLOW) + cursor-persist + db-blocking + pool-min + healthlog-db ###", info="info")
    log(f"[INFO] LOG_DIR={LOG_DIR}", info="info")
    log(f"[INFO] HEALTH_LOG_TABLE={HEALTH_SCHEMA}.{HEALTH_TABLE}", info="info")

    engine = get_engine_blocking()
    ensure_schema_tables(engine)

    today_ymd_init = datetime.now().strftime("%Y%m%d")
    loaded = load_today_offsets_from_db(engine, today_ymd_init)
    if loaded:
        PROCESSED_OFFSET.update(loaded)

    def _on_exit():
        try:
            maybe_save_cursor(engine, force=True)
        except Exception as e:
            log("[WARN] Exit cursor save failed", info="error")
            log_exc("[WARN] Exit cursor save failed", e, info="error")

        try:
            flush_runtime_logs_to_db(engine, force=True)
        except Exception as e:
            _console_print(f"[WARN] Exit runtime-log flush failed: {type(e).__name__}: {e}")

    atexit.register(_on_exit)

    log("=" * 78, info="info")
    log("[START] d2 Vision machine log realtime parser", info="info")
    log(f"[INFO] BASE_DIR={BASE_DIR}", info="info")
    log(f"[INFO] THREADS={THREAD_WORKERS} | SLEEP={SLEEP_SEC}s", info="info")
    log("[INFO] DEDUP POLICY = UNIQUE INDEX + ON CONFLICT DO NOTHING", info="info")
    log("[INFO] FILE POLICY = TodayOnly + Tail-Follow(offset)", info="info")
    log("[INFO] FILE SCAN = Vision\\YYYY\\MM fixed (fast on UNC)", info="info")
    log(f"[INFO] CURSOR PERSIST = DB({SCHEMA_NAME}.{CURSOR_TABLE}) | interval={CURSOR_SAVE_INTERVAL_SEC}s", info="info")
    log(f"[INFO] Engine pool minimized: pool_size=1, max_overflow=0 | work_mem={WORK_MEM}", info="info")
    log(f"[INFO] keepalive={'on' if ENABLE_PG_KEEPALIVE else 'off'}", info="info")
    log("=" * 78, info="info")

    loop_i = 0
    last_day = datetime.now().strftime("%Y%m%d")

    while True:
        loop_i += 1
        loop_t0 = time_mod.perf_counter()

        try:
            today_ymd = datetime.now().strftime("%Y%m%d")

            if today_ymd != last_day:
                log(f"[INFO] Day changed {last_day} -> {today_ymd} | reset processed cache(offset)", info="info")
                last_day = today_ymd
                PROCESSED_OFFSET.clear()
                _cursor_dirty_paths.clear()
                _last_cursor_save_ts = 0.0

                engine = get_engine_blocking()
                loaded2 = load_today_offsets_from_db(engine, today_ymd)
                if loaded2:
                    PROCESSED_OFFSET.update(loaded2)

            files = list_target_files_today(BASE_DIR, today_ymd=today_ymd)
            if not files:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(
                        f"[LOOP] no files (today={today_ymd}) | expected_dir={BASE_DIR}\\{datetime.now().year:04d}\\{datetime.now().month:02d}",
                        info="sleep"
                    )

                maybe_save_cursor(engine, force=False)
                flush_runtime_logs_to_db(engine, force=False)

                time_mod.sleep(SLEEP_SEC)
                continue

            all_records = []
            offset_updates = {}

            workers = min(THREAD_WORKERS, len(files))
            workers = max(1, workers)

            with ThreadPoolExecutor(max_workers=workers) as ex:
                futs = []
                for fp in files:
                    off = int(PROCESSED_OFFSET.get(fp, 0))
                    futs.append(ex.submit(parse_machine_log_file_tail, fp, today_ymd, off))

                for f in as_completed(futs):
                    rows, new_off, _station, fp, _status = f.result()
                    offset_updates[fp] = int(new_off)
                    if rows:
                        all_records.extend(rows)

            # offset 갱신 + dirty mark
            for fp, new_off in offset_updates.items():
                new_off_int = int(new_off)
                old_off = int(PROCESSED_OFFSET.get(fp, 0))
                PROCESSED_OFFSET[fp] = new_off_int
                if new_off_int != old_off:
                    _cursor_dirty_paths.add(fp)

            if not all_records:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(f"[LOOP] parsed=0 | files={len(files)} | (tail no new lines)", info="sleep")
            else:
                df = pd.DataFrame(all_records)
                attempted = insert_to_db(engine, df)
                if loop_i % LOG_EVERY_LOOP == 0 or attempted > 0:
                    log(f"[DB] attempted={attempted:,} | files={len(files)} | parsed={len(df):,}", info="info")

            maybe_save_cursor(engine, force=False)
            flush_runtime_logs_to_db(engine, force=False)

        except KeyboardInterrupt:
            log("\n[STOP] Interrupted by user.", info="info")
            try:
                maybe_save_cursor(engine, force=True)
            except Exception as e:
                log("[WARN] Final cursor save failed", info="error")
                log_exc("[WARN] Final cursor save failed", e, info="error")

            try:
                flush_runtime_logs_to_db(engine, force=True)
            except Exception as e:
                _console_print(f"[WARN] Final runtime-log flush failed: {type(e).__name__}: {e}")
            break

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] loop-level conn error -> rebuild engine", info="down")
                log_exc("[DB][RETRY] loop-level", e, info="error")
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
            else:
                log("[ERROR] Loop error (continue)", info="error")
                log_exc("[ERROR] Loop error", e, info="error")

            # 예외 뒤에도 런타임 로그는 가능한 저장 시도
            try:
                flush_runtime_logs_to_db(engine, force=False)
            except Exception as fe:
                _console_print(f"[WARN] runtime-log flush after exception failed: {type(fe).__name__}: {fe}")

        elapsed = time_mod.perf_counter() - loop_t0
        time_mod.sleep(max(0.0, SLEEP_SEC - elapsed))


if __name__ == "__main__":
    freeze_support()
    try:
        main()
    except KeyboardInterrupt:
        log("\n[STOP] 사용자 중단(Ctrl+C).", info="info")
        pause_console()
    except Exception as e:
        log("\n[UNHANDLED] 치명 오류가 발생했습니다.", info="error")
        log_exc("[UNHANDLED]", e, info="error")

        # 최종 flush 시도
        try:
            engine = get_engine_blocking()
            flush_runtime_logs_to_db(engine, force=True)
        except Exception:
            pass

        pause_console()
        sys.exit(1)
