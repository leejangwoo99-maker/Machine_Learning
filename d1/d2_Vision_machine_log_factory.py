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
from sqlalchemy.exc import OperationalError, DBAPIError, SQLAlchemyError
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
# 기본 로그 폴더 (필요 시 환경변수로 변경)
# - D2_LOG_DIR=C:\AptivAgent\logs\d2_Vision_machine_log_factory
LOG_DIR = Path(os.getenv("D2_LOG_DIR", r"C:\AptivAgent\logs\d2_Vision_machine_log_factory"))
LOG_DIR.mkdir(parents=True, exist_ok=True)

# 파일 I/O 실패 시 무한 에러 방지를 위한 플래그
_FILE_LOG_DISABLED = False


def _log_file_path():
    day = datetime.now().strftime("%Y%m%d")
    return LOG_DIR / f"d2_Vision_machine_log_factory_{day}.log"


def log(msg: str):
    """
    - 콘솔 + 파일 동시 기록
    - 파일은 append, UTF-8
    - 파일 기록 실패가 반복되면(_FILE_LOG_DISABLED) 콘솔만 유지
    """
    global _FILE_LOG_DISABLED
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"

    # 1) 콘솔
    print(line, flush=True)

    # 2) 파일
    if _FILE_LOG_DISABLED:
        return
    try:
        fp = _log_file_path()
        with fp.open("a", encoding="utf-8", errors="ignore") as f:
            f.write(line + "\n")
    except Exception as e:
        # 파일 로그가 실패해도 프로세스가 죽으면 안 됨
        _FILE_LOG_DISABLED = True
        print(f"[{ts}] [WARN] file logging disabled due to error: {type(e).__name__}: {e}", flush=True)


def log_exc(prefix: str, e: Exception):
    """
    예외 상세(traceback)까지 파일로 남김
    """
    log(f"{prefix}: {type(e).__name__}: {repr(e)}")
    tb = traceback.format_exc()
    for line in tb.rstrip().splitlines():
        log(f"{prefix} TRACE: {line}")


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
    log(f"[INFO] DB={_masked_db_info(config)}")

    # ✅ work_mem은 options로 세션 초기값 설정 (SET 바인딩 제거)
    connect_args = {
        "connect_timeout": 5,
        "application_name": "d2_Vision_machine_log_factory",
        "options": f"-c work_mem={WORK_MEM}",
    }

    # ✅ keepalive는 환경에 따라 토글
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

    [중요 수정]
    - 기존: _ENGINE ping 실패 후 dispose되어 None이 되었는데도 같은 while True에서 _ENGINE.connect() 재시도 → NoneType.connect 무한 반복 가능
    - 수정: while _ENGINE is not None: 형태로 ping 수행 → dispose되면 즉시 빠져나와 아래 "새 엔진 생성 루프"로 진입
    """
    global _ENGINE

    # 1) 기존 엔진이 있으면 ping 체크
    while _ENGINE is not None:
        try:
            with _ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE
        except Exception as e:
            log(f"[DB][RETRY] engine ping failed -> rebuild")
            log_exc("[DB][RETRY] ping error", e)
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            # 여기서 while 조건 재평가 -> _ENGINE is None이면 아래 create loop로 내려감

    # 2) 새 엔진 생성/연결 무한 재시도
    while True:
        try:
            _ENGINE = _build_engine(DB_CONFIG)
            with _ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))

            ka = (
                f"{PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}"
                if ENABLE_PG_KEEPALIVE else "OFF"
            )
            log(f"[DB][OK] engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, keepalive={ka})")
            return _ENGINE

        except Exception as e:
            log(f"[DB][RETRY] engine create/connect failed")
            log_exc("[DB][RETRY] connect error", e)
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


def ensure_schema_tables(engine):
    """
    INIT 실패 시 재시도(블로킹)
    - 테이블 생성
    - UNIQUE INDEX 생성(중복 영구 차단)
    - 커서 테이블 생성
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

                    # ✅ 중복 영구 차단: (end_day, station, end_time, contents)
                    idx_name = f'ux_{SCHEMA_NAME}_{tbl}_dedup'
                    conn.execute(text(f"""
                        CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
                        ON {SCHEMA_NAME}."{tbl}" (end_day, station, end_time, contents)
                    """))

                conn.execute(text(CREATE_CURSOR_TABLE_SQL))
                conn.execute(text(CREATE_CURSOR_DAY_INDEX_SQL))

            log("[INFO] Schema/tables/indexes ready (incl. cursor table).")
            return

        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] ensure_schema_tables conn error -> rebuild")
                log_exc("[DB][RETRY] ensure_schema_tables", e)
                _dispose_engine()
            else:
                log(f"[FATAL-INIT][RETRY] ensure_schema_tables failed")
                log_exc("[FATAL-INIT][RETRY] ensure_schema_tables", e)

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
    """
    ✅ DB 실패/끊김 포함: 무한 재시도(블로킹)
    """
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
                log(f"[INFO] Loaded cursor offsets from DB: {len(loaded)} file(s) for {today_ymd}")
            else:
                log(f"[INFO] No cursor offsets in DB for {today_ymd} (fresh start)")
            return loaded

        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] load_today_offsets conn error -> rebuild")
                log_exc("[DB][RETRY] load_today_offsets", e)
                _dispose_engine()
            else:
                log(f"[DB][RETRY] load_today_offsets failed")
                log_exc("[DB][RETRY] load_today_offsets", e)

            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


def upsert_cursor_offsets_to_db(engine, path_to_offset: dict):
    """
    ✅ DB 실패/끊김 포함: 무한 재시도(블로킹)
    """
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
            log(f"[INFO] Cursor saved to DB: {len(records)} file(s)")
            return
        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] cursor upsert conn error -> rebuild")
                log_exc("[DB][RETRY] cursor upsert", e)
                _dispose_engine()
            else:
                log(f"[DB][RETRY] cursor upsert failed")
                log_exc("[DB][RETRY] cursor upsert", e)

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
    """
    ✅ Vision\YYYY\MM만 정확히 스캔 (UNC/NAS 성능 안정화)
    """
    files = []
    if not base_dir.exists():
        log(f"[WARN] BASE_DIR not found: {base_dir}")
        return files

    now = datetime.now()
    year_dir = base_dir / f"{now.year:04d}"
    month_dir = year_dir / f"{now.month:02d}"

    if not month_dir.exists():
        log(f"[WARN] month_dir not found: {month_dir}")
        return files

    candidates = [
        month_dir / f"{today_ymd}_Vision1_Machine_Log.txt",
        month_dir / f"{today_ymd}_Vision2_Machine_Log.txt",
    ]

    for fp in candidates:
        if fp.is_file():
            files.append(str(fp))

    # 보험: 파일 0개일 때만 가볍게 glob
    if not files:
        for fp in month_dir.glob(f"{today_ymd}_Vision*_Machine_Log.*"):
            if fp.is_file() and FILENAME_PATTERN.search(fp.name):
                files.append(str(fp))

    return sorted(set(files))


def parse_machine_log_file_tail(path_str: str, today_ymd: str, offset: int):
    file_path = Path(path_str)

    m = FILENAME_PATTERN.search(file_path.name)
    if not m:
        return [], offset, None, path_str, "SKIP_BADNAME"

    file_ymd = m.group(1)
    vision_no = m.group(2)
    station = f"Vision{vision_no}"

    if file_ymd != today_ymd:
        return [], offset, station, path_str, "SKIP_NOT_TODAY"

    try:
        size = file_path.stat().st_size
    except Exception as e:
        return [], offset, station, path_str, f"ERROR_STAT:{type(e).__name__}"

    # truncate/로테이션 보호
    if offset > size:
        offset = 0

    if offset == size:
        return [], offset, station, path_str, "SKIP_NOCHANGE"

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
            # ✅ 연결 문제만 무한 재시도
            if _is_connection_error(e):
                log(f"[DB][RETRY] insert conn error -> rebuild engine")
                log_exc("[DB][RETRY] insert", e)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
                continue

            # ✅ 비연결(데이터/제약/인코딩 등)은 배치 스킵하고 루프 진행
            log(f"[DB][SKIP] insert non-conn error -> drop this batch")
            log_exc("[DB][SKIP] insert", e)
            return 0


# ============================================
# 9) main loop
# ============================================
def main():
    global _last_cursor_save_ts

    log("### BUILD: d2 Vision parser OPTION-A (TAIL-FOLLOW) + cursor-persist + db-blocking + pool-min (FINAL+FILELOG) ###")
    log(f"[INFO] LOG_DIR={LOG_DIR}")

    engine = get_engine_blocking()
    ensure_schema_tables(engine)

    # ✅ 시작 시: DB에서 오늘 커서(offset) 로드
    today_ymd_init = datetime.now().strftime("%Y%m%d")
    loaded = load_today_offsets_from_db(engine, today_ymd_init)
    if loaded:
        PROCESSED_OFFSET.update(loaded)

    # ✅ 정상 종료 시 마지막 커서 강제 저장
    def _on_exit():
        try:
            maybe_save_cursor(engine, force=True)
        except Exception as e:
            log(f"[WARN] Exit cursor save failed")
            log_exc("[WARN] Exit cursor save failed", e)

    atexit.register(_on_exit)

    log("=" * 78)
    log("[START] d2 Vision machine log realtime parser")
    log(f"[INFO] BASE_DIR={BASE_DIR}")
    log(f"[INFO] THREADS={THREAD_WORKERS} | SLEEP={SLEEP_SEC}s")
    log("[INFO] DEDUP POLICY = UNIQUE INDEX + ON CONFLICT DO NOTHING")
    log("[INFO] FILE POLICY = TodayOnly + Tail-Follow(offset)")
    log("[INFO] FILE SCAN = Vision\\YYYY\\MM fixed (fast on UNC)")
    log(f"[INFO] CURSOR PERSIST = DB({SCHEMA_NAME}.{CURSOR_TABLE}) | interval={CURSOR_SAVE_INTERVAL_SEC}s")
    log(f"[INFO] Engine pool minimized: pool_size=1, max_overflow=0 | work_mem={WORK_MEM}")
    log(f"[INFO] keepalive={'ON' if ENABLE_PG_KEEPALIVE else 'OFF'}")
    log("=" * 78)

    loop_i = 0
    last_day = datetime.now().strftime("%Y%m%d")

    while True:
        loop_i += 1
        loop_t0 = time_mod.perf_counter()

        try:
            today_ymd = datetime.now().strftime("%Y%m%d")

            # 날짜 변경 시 캐시 초기화 + 새 날짜 커서 로드
            if today_ymd != last_day:
                log(f"[INFO] Day changed {last_day} -> {today_ymd} | reset processed cache(offset)")
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
                    log(f"[LOOP] no files (today={today_ymd}) | expected_dir={BASE_DIR}\\{datetime.now().year:04d}\\{datetime.now().month:02d}")

                maybe_save_cursor(engine, force=False)
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

            # ✅ offset 갱신 + dirty mark
            for fp, new_off in offset_updates.items():
                new_off_int = int(new_off)
                old_off = int(PROCESSED_OFFSET.get(fp, 0))
                PROCESSED_OFFSET[fp] = new_off_int
                if new_off_int != old_off:
                    _cursor_dirty_paths.add(fp)

            if not all_records:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(f"[LOOP] parsed=0 | files={len(files)} | (tail no new lines)")
            else:
                df = pd.DataFrame(all_records)
                attempted = insert_to_db(engine, df)
                if loop_i % LOG_EVERY_LOOP == 0 or attempted > 0:
                    log(f"[DB] attempted={attempted:,} | files={len(files)} | parsed={len(df):,}")

            maybe_save_cursor(engine, force=False)

        except KeyboardInterrupt:
            log("\n[STOP] Interrupted by user.")
            try:
                maybe_save_cursor(engine, force=True)
            except Exception as e:
                log(f"[WARN] Final cursor save failed")
                log_exc("[WARN] Final cursor save failed", e)
            break

        except Exception as e:
            # 루프 레벨에서도 연결 문제면 엔진 재생성 후 계속
            if _is_connection_error(e):
                log(f"[DB][RETRY] loop-level conn error -> rebuild engine")
                log_exc("[DB][RETRY] loop-level", e)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
            else:
                log(f"[ERROR] Loop error (continue)")
                log_exc("[ERROR] Loop error", e)

        # loop pacing
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
        log_exc("[UNHANDLED]", e)
        pause_console()
        sys.exit(1)
