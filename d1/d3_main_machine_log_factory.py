# -*- coding: utf-8 -*-
# ============================================
# Main Machine Log Parser (Realtime / MP=1 / Dedup / TodayOnly)
# - 주/야간 로직 제거
# - dayornight 컬럼 제거
# - time 컬럼 -> end_time 컬럼으로 변경
# - end_day는 파일명(YYYYMMDD) 기준으로만 저장
# - 오늘 파일(YYYYMMDD==오늘)만 처리
# - (end_day, station, end_time, contents) 중복 방지 (UNIQUE INDEX + ON CONFLICT DO NOTHING)
# - ❌ 120초(mtime) 조건 제거
# - ✅ 하루 1개 파일 append 구조에 맞게 offset(tell/seek) 기반 tail-follow
#
# [재부팅/강제종료 대비 커서 영구 저장]
# - ✅ 마지막 파싱 byte offset을 DB에 주기적으로 저장(UPSERT)
# - ✅ 재시작 시 DB에서 offset 로드 후 그 지점부터 tail-follow
#
# [이번 요청 반영]
# - ✅ 멀티프로세스 = 1개 (Pool 제거, 단일 프로세스 파싱)
# - ✅ 무한 루프 인터벌 5초
# - ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
# - ✅ 백엔드별 상시 연결 1개로 고정(풀 최소화): Engine pool_size=1, max_overflow=0
# - ✅ work_mem 폭증 방지: 세션에 SET work_mem 적용 (환경변수 PG_WORK_MEM로 조정)
# - ✅ (추가) 실행 중 서버/네트워크 끊김 발생 시: engine dispose 후 무한 재접속/재시도
# - ✅ (추가/권장) TCP keepalive 옵션(가능 범위) + pool_pre_ping 유지
#
# [로그 DB 저장 추가]
# - ✅ 스키마: k_demon_heath_check (없으면 생성)
# - ✅ 테이블: d3_log (없으면 생성)
# - ✅ 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
# - ✅ end_day, end_time, info, contents 순서로 dataframe화 후 저장
# ============================================

import re
import os
import time as time_mod
import atexit
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
import urllib.parse


# =========================
# ✅ libpq/psycopg2 인코딩 안정화(EXE 환경)
# =========================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")


# =========================
# 1. 경로 / DB 설정
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\Main")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "d1_machine_log"
TABLE_NAME = "Main_machine_log"

# 로그 저장 대상
LOG_SCHEMA_NAME = "k_demon_heath_check"
LOG_TABLE_NAME = "d3_log"

LINE_PATTERN = re.compile(r'^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$')
FILE_PATTERN = re.compile(r"(\d{8})_Main_Machine_Log\.txt$", re.IGNORECASE)


# =========================
# ✅ 요청 반영 고정값
# =========================
SLEEP_SEC = 5
DB_RETRY_INTERVAL_SEC = 5
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# 처리 캐시 (path -> 마지막 읽은 파일 byte offset)
PROCESSED_OFFSET = {}

# =========================
# 커서 DB 저장 설정
# =========================
CURSOR_TABLE = "machine_log_cursor"
CURSOR_SAVE_INTERVAL_SEC = 5
_last_cursor_save_ts = 0.0
_cursor_dirty_paths = set()

# Engine singleton
_ENGINE = None

# keepalive 옵션
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


# =========================
# 2. DB 엔진 (풀 최소화 + 무한 재시도)
# =========================
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
    password = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{password}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
        "?connect_timeout=5"
    )
    print(f"[INFO] DB = {_masked_db_info(config)}", flush=True)

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args={
            "connect_timeout": 5,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "main_machine_log_parser_realtime",
        },
    )


def get_engine_blocking():
    """
    DB 접속 실패/실행 중 끊김 시 무한 재시도
    Engine 1개 재사용
    """
    global _ENGINE

    if _ENGINE is not None:
        while True:
            try:
                with _ENGINE.connect() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    conn.execute(text("SELECT 1"))
                return _ENGINE
            except Exception as e:
                print(f"[DB][RETRY] engine ping failed -> rebuild: {type(e).__name__}: {repr(e)}", flush=True)
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE = _build_engine(DB_CONFIG)
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            print(
                f"[DB][OK] engine ready (pool_size=1, max_overflow=0, work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})",
                flush=True
            )
            return _ENGINE
        except Exception as e:
            print(f"[DB][RETRY] engine create/connect failed: {type(e).__name__}: {repr(e)}", flush=True)
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 3. 스키마 / 테이블 / INDEX + 커서 / 로그테이블
# =========================
CREATE_CURSOR_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}".{CURSOR_TABLE} (
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
ON "{SCHEMA_NAME}".{CURSOR_TABLE} (file_day);
"""

CREATE_LOG_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA_NAME}"."{LOG_TABLE_NAME}" (
    id BIGSERIAL PRIMARY KEY,
    end_day  VARCHAR(8)  NOT NULL,
    end_time VARCHAR(8)  NOT NULL,
    info     VARCHAR(20) NOT NULL,
    contents TEXT
);
"""

CREATE_LOG_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE_NAME}_day_time
ON "{LOG_SCHEMA_NAME}"."{LOG_TABLE_NAME}" (end_day, end_time);
"""


def _now_log_day_time():
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")


def _make_log_df(info: str, contents: str) -> pd.DataFrame:
    end_day, end_time = _now_log_day_time()
    safe_info = (info or "info").strip().lower()[:20]
    safe_contents = (contents or "").replace("\x00", " ").strip()
    return pd.DataFrame([{
        "end_day": end_day,
        "end_time": end_time,
        "info": safe_info,
        "contents": safe_contents
    }], columns=["end_day", "end_time", "info", "contents"])


def log_to_db(engine, info: str, contents: str):
    """
    로그를 DB에 저장.
    실패 시 프로그램 진행을 막지 않기 위해 콘솔 출력만 수행.
    """
    try:
        df = _make_log_df(info, contents)
        records = df.to_dict(orient="records")
        sql = text(f"""
            INSERT INTO "{LOG_SCHEMA_NAME}"."{LOG_TABLE_NAME}"
            (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """)
        with engine.begin() as conn:
            conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
            conn.execute(sql, records)
    except Exception as e:
        print(f"[LOG][WARN] DB log write failed: {type(e).__name__}: {repr(e)}", flush=True)


def emit_log(engine, info: str, contents: str, print_prefix: str = "[INFO]"):
    msg = f"{contents}"
    print(f"{print_prefix} {msg}", flush=True)
    log_to_db(engine, info, contents)


def ensure_schema_table(engine):
    """
    INIT/DDL 무한 재시도
    """
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})

                # main
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA_NAME}"'))
                conn.execute(text(f"""
                    CREATE TABLE IF NOT EXISTS "{SCHEMA_NAME}"."{TABLE_NAME}" (
                        id BIGSERIAL PRIMARY KEY,
                        end_day     VARCHAR(8),
                        station     VARCHAR(10),
                        end_time    VARCHAR(12),
                        contents    VARCHAR(75)
                    )
                """))
                conn.execute(text(f"""
                    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_NAME.lower()}_dedup
                    ON "{SCHEMA_NAME}"."{TABLE_NAME}"
                    (end_day, station, end_time, contents)
                """))

                # cursor
                conn.execute(text(CREATE_CURSOR_TABLE_SQL))
                conn.execute(text(CREATE_CURSOR_DAY_INDEX_SQL))

                # health log
                conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA_NAME}"'))
                conn.execute(text(CREATE_LOG_TABLE_SQL))
                conn.execute(text(CREATE_LOG_INDEX_SQL))

            print("[INFO] Schema/table/index ready (main + cursor + health log)", flush=True)
            try:
                log_to_db(engine, "info", "schema/table/index ready (main + cursor + health log)")
            except Exception:
                pass
            return

        except Exception as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] ensure_schema_table conn error -> rebuild: {type(e).__name__}: {repr(e)}", flush=True)
                _dispose_engine()
            else:
                print(f"[DB][RETRY] ensure_schema_table failed: {type(e).__name__}: {repr(e)}", flush=True)

            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# =========================
# 4. 커서 유틸
# =========================
def extract_day_station_from_path(path_str: str):
    p = Path(path_str)
    m = FILE_PATTERN.search(p.name)
    if not m:
        return None, None
    file_day = m.group(1)
    station = "Main"
    return file_day, station


def load_today_offsets_from_db(engine, today_ymd: str):
    while True:
        try:
            loaded = {}
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})

                rows = conn.execute(
                    text(f"""
                        SELECT path, byte_offset
                        FROM "{SCHEMA_NAME}".{CURSOR_TABLE}
                        WHERE file_day = :today
                    """),
                    {"today": today_ymd}
                ).fetchall()

                for r in rows:
                    loaded[str(r[0])] = int(r[1])

            if loaded:
                emit_log(engine, "info", f"loaded cursor offsets from db: {len(loaded)} file(s) for {today_ymd}")
            else:
                emit_log(engine, "info", f"no cursor offsets in db for {today_ymd} (fresh start)")
            return loaded

        except Exception as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] load_today_offsets conn error -> rebuild: {type(e).__name__}: {repr(e)}", flush=True)
                _dispose_engine()
            else:
                print(f"[DB][RETRY] load_today_offsets failed: {type(e).__name__}: {repr(e)}", flush=True)

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
        INSERT INTO "{SCHEMA_NAME}".{CURSOR_TABLE}
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
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(sql, records)

            emit_log(engine, "info", f"cursor saved to db: {len(records)} file(s)")
            return

        except Exception as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] cursor upsert conn error -> rebuild: {type(e).__name__}: {repr(e)}", flush=True)
                _dispose_engine()
            else:
                print(f"[DB][RETRY] cursor upsert failed: {type(e).__name__}: {repr(e)}", flush=True)

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


# =========================
# 5. 파일 파싱 - TAIL FOLLOW
# =========================
def parse_main_log_file(path_str: str, start_offset: int):
    """
    return: (path_str, new_offset, rows)
    """
    file_path = Path(path_str)

    m = FILE_PATTERN.search(file_path.name)
    if not m:
        return path_str, start_offset, []

    file_ymd = m.group(1)
    today_ymd = datetime.now().strftime("%Y%m%d")

    if file_ymd != today_ymd:
        return path_str, start_offset, []

    try:
        size = file_path.stat().st_size
    except Exception:
        return path_str, start_offset, []

    if start_offset > size:
        start_offset = 0

    if start_offset == size:
        return path_str, start_offset, []

    rows = []
    new_offset = start_offset

    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            f.seek(start_offset, os.SEEK_SET)

            for line in f:
                line = line.rstrip()
                mm = LINE_PATTERN.match(line)
                if not mm:
                    continue

                end_time_str, contents_raw = mm.groups()

                try:
                    _ = datetime.strptime(end_time_str, "%H:%M:%S.%f").time()
                except ValueError:
                    continue

                contents = contents_raw.replace("\x00", "").strip()[:75]

                rows.append({
                    "end_day": file_ymd,
                    "station": "Main",
                    "end_time": end_time_str,
                    "contents": contents,
                })

            new_offset = f.tell()

    except Exception:
        return path_str, start_offset, []

    return path_str, int(new_offset), rows


# =========================
# 6. 실시간 대상 파일 수집
# =========================
def list_target_files_realtime(base_dir: Path):
    targets = []
    if not base_dir.exists():
        return targets

    today_ymd = datetime.now().strftime("%Y%m%d")

    for year_dir in base_dir.iterdir():
        if not (year_dir.is_dir() and year_dir.name.isdigit()):
            continue

        for month_dir in year_dir.iterdir():
            if not (month_dir.is_dir() and month_dir.name.isdigit()):
                continue

            for fp in month_dir.iterdir():
                if not (fp.is_file() and FILE_PATTERN.search(fp.name)):
                    continue

                m = FILE_PATTERN.search(fp.name)
                if not m or m.group(1) != today_ymd:
                    continue

                targets.append(str(fp))

    return targets


# =========================
# 7. DB INSERT (main)
# =========================
def insert_to_db(engine, df: pd.DataFrame):
    if df is None or df.empty:
        return 0

    df = df.sort_values(["end_day", "end_time"])

    insert_sql = text(f"""
        INSERT INTO "{SCHEMA_NAME}"."{TABLE_NAME}"
        (end_day, station, end_time, contents)
        VALUES (:end_day, :station, :end_time, :contents)
        ON CONFLICT (end_day, station, end_time, contents)
        DO NOTHING
    """)

    records = df.to_dict(orient="records")

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(insert_sql, records)

            emit_log(engine, "info", f"insert attempted {len(df)} rows (duplicates ignored)")
            return len(df)

        except Exception as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] insert conn error -> rebuild engine: {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    log_to_db(engine, "down", f"insert conn error -> rebuild engine: {type(e).__name__}: {repr(e)}")
                except Exception:
                    pass
                _dispose_engine()
            else:
                print(f"[DB][RETRY] insert failed (blocked until success): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    log_to_db(engine, "error", f"insert failed: {type(e).__name__}: {repr(e)}")
                except Exception:
                    pass

            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_blocking()


# =========================
# 8. main
# =========================
def main():
    engine = get_engine_blocking()
    ensure_schema_table(engine)
    emit_log(engine, "info", "boot main_machine_log_parser_realtime started")

    today_ymd = datetime.now().strftime("%Y%m%d")

    loaded = load_today_offsets_from_db(engine, today_ymd)
    if loaded:
        PROCESSED_OFFSET.update(loaded)

    def _on_exit():
        try:
            maybe_save_cursor(engine, force=True)
            emit_log(engine, "info", "graceful exit cursor save done")
        except Exception as e:
            print(f"[WARN] Exit cursor save failed: {e}", flush=True)
            try:
                log_to_db(engine, "error", f"exit cursor save failed: {type(e).__name__}: {repr(e)}")
            except Exception:
                pass

    atexit.register(_on_exit)

    while True:
        loop_t0 = time_mod.perf_counter()

        try:
            now_day = datetime.now().strftime("%Y%m%d")
            if now_day != today_ymd:
                emit_log(engine, "info", f"day changed {today_ymd} -> {now_day} | reset offsets")
                today_ymd = now_day
                PROCESSED_OFFSET.clear()
                _cursor_dirty_paths.clear()
                global _last_cursor_save_ts
                _last_cursor_save_ts = 0.0

                engine = get_engine_blocking()

                loaded2 = load_today_offsets_from_db(engine, today_ymd)
                if loaded2:
                    PROCESSED_OFFSET.update(loaded2)

            files = list_target_files_realtime(BASE_DIR)
            if not files:
                maybe_save_cursor(engine, force=False)
                # 상태 확인용 sleep 로그 (너무 과다 적재 방지를 위해 1회/루프만)
                log_to_db(engine, "sleep", f"no target files, sleep {SLEEP_SEC}s")
                time_mod.sleep(SLEEP_SEC)
                continue

            emit_log(engine, "info", f"today target files: {len(files)}")

            all_rows = []
            new_offsets = {}

            for fp in files:
                off = int(PROCESSED_OFFSET.get(fp, 0))
                path_str, offset_after, rows = parse_main_log_file(fp, off)
                new_offsets[path_str] = int(offset_after)
                if rows:
                    all_rows.extend(rows)

            for fp, new_off in new_offsets.items():
                new_off_int = int(new_off)
                old_off = int(PROCESSED_OFFSET.get(fp, 0))
                PROCESSED_OFFSET[fp] = new_off_int
                if new_off_int != old_off:
                    _cursor_dirty_paths.add(fp)

            if all_rows:
                df = pd.DataFrame(all_rows)
                insert_to_db(engine, df)
            else:
                log_to_db(engine, "info", "no new log lines parsed")

            maybe_save_cursor(engine, force=False)

        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user", flush=True)
            try:
                log_to_db(engine, "info", "keyboardinterrupt stop requested")
                maybe_save_cursor(engine, force=True)
            except Exception as e:
                print(f"[WARN] Final cursor save failed: {e}", flush=True)
            break

        except Exception as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] loop-level conn error -> rebuild engine: {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    log_to_db(engine, "down", f"loop-level conn error: {type(e).__name__}: {repr(e)}")
                except Exception:
                    pass
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
            else:
                print(f"[ERROR] Loop error: {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    log_to_db(engine, "error", f"loop error: {type(e).__name__}: {repr(e)}")
                except Exception:
                    pass

        elapsed = time_mod.perf_counter() - loop_t0
        sleep_left = max(0.0, SLEEP_SEC - elapsed)
        if sleep_left > 0:
            # 루프 sleep 상태도 health log에 남김
            log_to_db(engine, "sleep", f"loop pacing sleep {sleep_left:.2f}s")
        time_mod.sleep(sleep_left)


if __name__ == "__main__":
    main()
