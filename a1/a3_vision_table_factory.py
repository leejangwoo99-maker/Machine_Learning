# -*- coding: utf-8 -*-
from pathlib import Path
import os
import pandas as pd
import re
import time
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_values

# ==========================
# ✅ (중요) psycopg2/libpq 에러 메시지 인코딩 때문에 EXE에서 UnicodeDecodeError 방지
# ==========================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

# ==========================
# 기본 경로 설정
# ==========================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
VISION_FOLDER_NAME = "Vision03"

# ==========================
# PostgreSQL 접속 정보
# ==========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

# ==========================
# 메인 스키마/테이블
# ==========================
SCHEMA_NAME = "a3_vision_table"
TABLE_NAME = "vision_table"

# ==========================
# 로그 스키마/테이블 (추가 요구사항)
# ==========================
LOG_SCHEMA_NAME = "k_demon_heath_check"
LOG_TABLE_NAME = "a3_log"

# ==========================
# 추가 요구사항 설정
# ==========================
# ✅ 요구사항: 멀티프로세스 = 1개
NUM_WORKERS = 1

REALTIME_WINDOW_SEC = 120       # ✅ 최근 120초 이내 파일만 처리
USE_FIXED_CUTOFF_TS = False     # ✅ True면 아래 cutoff_ts 값을 "고정"으로 사용
FIXED_CUTOFF_TS = 1765501841.4473598

# ✅ DB DISTINCT file_path 재로딩 주기(루프 횟수)
DB_RELOAD_EVERY_LOOPS = 120     # 약 10분(5초 루프 기준)

# ✅ 요구사항: 무한 루프 인터벌 5초
LOOP_INTERVAL_SEC = 5

# ✅ 요구사항: DB 접속 실패 시 무한 재시도(연결 성공까지 블로킹)
DB_RETRY_INTERVAL_SEC = 5

# ✅ 요구사항: work_mem 폭증 방지(세션별 cap) - 환경변수로 조정 가능
#    예) set PG_WORK_MEM=8MB
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ (추가/권장) 끊긴 TCP 세션을 빠르게 감지하기 위한 keepalive
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

# ✅ 요구사항: 백엔드별 상시 연결 1개 고정(풀 최소화)
_CONN = None


# ==========================
# 콘솔 + DB 로그 유틸
# ==========================
def _now_day_time():
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")


def _print(msg: str):
    try:
        print(msg, flush=True)
    except Exception:
        pass


def _insert_log_rows(conn, rows):
    """
    rows: list[dict] with keys end_day, end_time, info, contents
    """
    if not rows:
        return

    # ✅ 요구사항: end_day, end_time, info, contents 순서대로 dataframe화
    df = pd.DataFrame(rows, columns=["end_day", "end_time", "info", "contents"])

    records = [tuple(x) for x in df[["end_day", "end_time", "info", "contents"]].to_numpy()]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME}
                (end_day, end_time, info, contents)
            VALUES %s
            """,
            records,
            page_size=1000,
        )


def log_event(info: str, contents: str, echo: bool = True):
    """
    - info는 반드시 소문자 저장
    - 로깅 실패해도 메인 로직은 계속
    """
    info_val = (info or "info").strip().lower()
    end_day, end_time = _now_day_time()
    row = {
        "end_day": end_day,
        "end_time": end_time,
        "info": info_val,
        "contents": str(contents),
    }

    if echo:
        _print(f"[{info_val}] {contents}")

    # DB 로그는 "있으면 저장", 실패해도 메인 동작 지속
    try:
        conn = ensure_conn()
        _insert_log_rows(conn, [row])
        conn.commit()
    except Exception as e:
        # 로그 저장 실패는 콘솔만 남기고 무시
        try:
            _print(f"[warn] log insert failed: {type(e).__name__}: {repr(e)}")
        except Exception:
            pass
        try:
            if 'conn' in locals() and conn:
                conn.rollback()
        except Exception:
            pass
        if _is_connection_error(e):
            _reset_conn("log insert connection error")


# ==========================
# DB 유틸
# ==========================
def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def _reset_conn(reason: str = ""):
    """커넥션 강제 폐기 + 다음 ensure_conn에서 무한 재연결 유도"""
    global _CONN
    if reason:
        _print(f"[db][reset] {reason}")
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


def _apply_session_safety(conn):
    """
    ✅ work_mem 폭증 방지: 세션에 cap 적용 + 연결 유효성 ping
    """
    with conn.cursor() as cur:
        cur.execute("SET work_mem TO %s;", (WORK_MEM,))
        cur.execute("SELECT 1;")
    try:
        conn.rollback()
    except Exception:
        pass


def get_connection_blocking():
    """
    ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
    + (권장) keepalive로 죽은 TCP 세션 감지 가속
    """
    while True:
        try:
            conn = psycopg2.connect(
                **DB_CONFIG,
                connect_timeout=5,
                application_name="a3_vision_table_realtime",
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
            )
            conn.autocommit = False
            _apply_session_safety(conn)
            _print(
                f"[db][ok] connected (work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})"
            )
            return conn
        except Exception as e:
            _print(f"[db][retry] connect failed: {type(e).__name__}: {repr(e)}")
            try:
                time.sleep(DB_RETRY_INTERVAL_SEC)
            except Exception:
                pass


def ensure_conn():
    """
    ✅ 백엔드별 상시 연결 1개 고정(풀 최소화)
    """
    global _CONN

    if _CONN is None:
        _CONN = get_connection_blocking()
        return _CONN

    if getattr(_CONN, "closed", 1) != 0:
        _reset_conn("conn.closed != 0")
        _CONN = get_connection_blocking()
        return _CONN

    try:
        _apply_session_safety(_CONN)
        return _CONN
    except Exception as e:
        if _is_connection_error(e):
            _print(f"[db][warn] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}")
            _reset_conn("ping failed")
            _CONN = get_connection_blocking()
            return _CONN
        raise


def ensure_schema_and_table(conn):
    """
    메인 테이블 보장
    """
    while True:
        conn = ensure_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};")

                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                        id                  BIGSERIAL PRIMARY KEY,
                        barcode_information TEXT,
                        station             TEXT,
                        run_time            DOUBLE PRECISION,
                        end_day             TEXT,
                        end_time            TEXT,
                        remark              TEXT,
                        step_description    TEXT,
                        value               TEXT,
                        min                 TEXT,
                        max                 TEXT,
                        result              TEXT,
                        file_path           TEXT NOT NULL,
                        processed_at        TIMESTAMP NOT NULL DEFAULT NOW()
                    );
                    """
                )

                cur.execute(
                    f"""
                    ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME}
                    ADD COLUMN IF NOT EXISTS run_time DOUBLE PRECISION;
                    """
                )

                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_file_path
                    ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);
                    """
                )

                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_end_day
                    ON {SCHEMA_NAME}.{TABLE_NAME} (end_day);
                    """
                )

            conn.commit()
            return

        except psycopg2.Error as e:
            if _is_connection_error(e):
                _print(f"[db][retry] ensure_schema_and_table failed(conn): {type(e).__name__}: {repr(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("ensure_schema_and_table connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            _print(f"[db][fatal] ensure_schema_and_table db error: {type(e).__name__}: {repr(e)}")
            raise


def ensure_log_schema_and_table(conn):
    """
    ✅ 로그 스키마/테이블 보장
    - 스키마: k_demon_heath_check
    - 테이블: a3_log
    """
    while True:
        conn = ensure_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA_NAME};")
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (
                        id BIGSERIAL PRIMARY KEY,
                        end_day  TEXT NOT NULL,     -- yyyymmdd
                        end_time TEXT NOT NULL,     -- hh:mi:ss
                        info     TEXT NOT NULL,     -- 소문자
                        contents TEXT
                    );
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE_NAME}_day_time
                    ON {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (end_day, end_time);
                    """
                )
                cur.execute(
                    f"""
                    CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE_NAME}_info
                    ON {LOG_SCHEMA_NAME}.{LOG_TABLE_NAME} (info);
                    """
                )
            conn.commit()
            return
        except psycopg2.Error as e:
            if _is_connection_error(e):
                _print(f"[db][retry] ensure_log_schema_and_table failed(conn): {type(e).__name__}: {repr(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("ensure_log_schema_and_table connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            _print(f"[db][fatal] ensure_log_schema_and_table db error: {type(e).__name__}: {repr(e)}")
            raise


def get_processed_file_paths_today(conn, end_day: str) -> set:
    while True:
        conn = ensure_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT DISTINCT file_path
                    FROM {SCHEMA_NAME}.{TABLE_NAME}
                    WHERE end_day = %s
                      AND file_path IS NOT NULL
                      AND file_path <> '';
                    """,
                    (end_day,)
                )
                rows = cur.fetchall()
            return {r[0] for r in rows if r and r[0]}

        except psycopg2.Error as e:
            if _is_connection_error(e):
                _print(f"[db][retry] get_processed_file_paths_today failed(conn): {type(e).__name__}: {repr(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("get_processed_file_paths_today connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            _print(f"[db][fatal] get_processed_file_paths_today db error: {type(e).__name__}: {repr(e)}")
            raise


def insert_main_rows(conn, rows):
    if not rows:
        return

    records = [
        (
            r.get("barcode_information", ""),
            r.get("station", ""),
            r.get("run_time", None),
            r.get("end_day", ""),
            r.get("end_time", ""),
            r.get("remark", ""),
            r.get("step_description", ""),
            r.get("value", ""),
            r.get("min", ""),
            r.get("max", ""),
            r.get("result", ""),
            r.get("file_path", ""),
        )
        for r in rows
    ]

    with conn.cursor() as cur:
        execute_values(
            cur,
            f"""
            INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
                (barcode_information, station, run_time,
                 end_day, end_time, remark,
                 step_description, value, min, max, result, file_path)
            VALUES %s
            """,
            records,
            page_size=5000,
        )


# ==========================
# 파싱 유틸
# ==========================
def parse_barcode_line(line: str) -> str:
    m = re.search(r"Barcode information\s*:\s*(.*)", line)
    return m.group(1).strip() if m else ""


def parse_program_line(line: str) -> str:
    m = re.search(r"Test Program\s*:\s*(.*)", line)
    if not m:
        return ""
    prog = m.group(1).strip()
    return "Vision1" if prog == "LED1" else "Vision2" if prog == "LED2" else prog


def classify_remark(barcode: str) -> str:
    if not barcode or len(barcode) < 18:
        return "Non-PD"
    return "PD" if barcode[17] in ("J", "S") else "Non-PD"


def clean_result_value(result: str) -> str:
    if result is None:
        return ""
    return str(result).replace("[", "").replace("]", "").strip()


def parse_end_day_from_path(file_path: Path) -> str:
    for parent in file_path.parents:
        if re.fullmatch(r"\d{8}", parent.name):
            return parent.name
    m = re.search(r"(\d{8})", file_path.name)
    return m.group(1) if m else ""


def parse_end_time_from_full_path(file_path: Path) -> str:
    full_path = str(file_path)
    m = re.search(r"_(\d{14})_", full_path)
    if not m:
        m = re.search(r"_(\d{14})_", file_path.name)
    if not m:
        return ""
    hhmiss = m.group(1)[8:14]
    return f"{hhmiss[0:2]}:{hhmiss[2:4]}:{hhmiss[4:6]}"


def parse_run_time_line(line: str):
    if not line:
        return None
    m = re.search(r"Run\s*Time\s*:\s*([0-9.]+)\s*s", line)
    if not m:
        return None
    try:
        return round(float(m.group(1)), 1)
    except ValueError:
        return None


def parse_data_lines(lines):
    index_list = []
    rows = []
    for raw_line in lines:
        line = raw_line.strip("\n\r")
        if not line.strip() or "," not in line:
            continue

        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 2:
            continue

        desc = re.sub(r"\s{2,}", " ", parts[0]).strip()
        value = parts[1] if len(parts) > 1 else ""
        min_val = parts[2] if len(parts) > 2 else ""
        max_val = parts[3] if len(parts) > 3 else ""
        result = parts[4] if len(parts) > 4 else ""

        index_list.append(desc)
        rows.append({"value": value, "min": min_val, "max": max_val, "result": result})

    if not rows:
        return pd.DataFrame(columns=["value", "min", "max, result"])

    return pd.DataFrame(rows, index=index_list)


# ==========================
# 워커(멀티프로세스/단일 공용)
# ==========================
def _worker_process_file(file_str: str):
    file_path = Path(file_str)
    try:
        with file_path.open("r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
    except Exception:
        return []

    if len(lines) < 19:
        return []

    barcode = parse_barcode_line(lines[4]) if len(lines) > 4 else ""
    station = parse_program_line(lines[5]) if len(lines) > 5 else ""
    remark = classify_remark(barcode)
    run_time = parse_run_time_line(lines[13]) if len(lines) > 13 else None
    end_day = parse_end_day_from_path(file_path)
    end_time = parse_end_time_from_full_path(file_path)

    df_steps = parse_data_lines(lines[18:])
    if df_steps.empty:
        return []

    out_rows = []
    for step_desc, row in df_steps.iterrows():
        out_rows.append(
            {
                "barcode_information": barcode,
                "station": station,
                "run_time": run_time,
                "end_day": end_day,
                "end_time": end_time,
                "remark": remark,
                "step_description": step_desc,
                "value": row.get("value", ""),
                "min": row.get("min", ""),
                "max": row.get("max", ""),
                "result": clean_result_value(row.get("result", "")),
                "file_path": str(file_path),
            }
        )
    return out_rows


# ==========================
# 메인 파싱 로직 (한 번 실행)
# ==========================
def run_once(conn, processed_set: set, today_str: str):
    vision_root = BASE_LOG_DIR / VISION_FOLDER_NAME
    if not vision_root.exists():
        log_event("error", f"{VISION_FOLDER_NAME} 폴더가 없음: {vision_root}")
        return

    cutoff_ts = float(FIXED_CUTOFF_TS) if USE_FIXED_CUTOFF_TS else (time.time() - REALTIME_WINDOW_SEC)

    target_files = []
    today_dir = vision_root / today_str
    if not today_dir.exists():
        log_event("info", f"오늘 폴더 없음: {today_dir} (today={today_str})")
        return

    for sub_name in ["GoodFile", "BadFile"]:
        sub_dir = today_dir / sub_name
        if not sub_dir.exists():
            continue

        for fp in sub_dir.glob("*"):
            if not fp.is_file():
                continue
            try:
                mtime = fp.stat().st_mtime
            except Exception:
                continue
            if mtime < cutoff_ts:
                continue
            target_files.append(str(fp))

    total = len(target_files)
    log_event("info", f"오늘({today_str}) & 최근{REALTIME_WINDOW_SEC}초 대상 파일 수: {total}개 (cutoff_ts={cutoff_ts})")

    if total == 0:
        log_event("sleep", "대상 파일 없음")
        return

    files_to_process = [f for f in target_files if f not in processed_set]

    # 이번 사이클 내 중복 제거
    seen = set()
    uniq = []
    for f in files_to_process:
        if f in seen:
            continue
        seen.add(f)
        uniq.append(f)
    files_to_process = uniq

    log_event("info", f"db distinct file_path(end_day=오늘) 중복 스킵: {total - len(files_to_process)}개")
    log_event("info", f"실제 처리 대상: {len(files_to_process)}개")

    if not files_to_process:
        log_event("sleep", "처리할 신규 파일 없음")
        return

    max_workers = NUM_WORKERS
    chunksize = max(20, len(files_to_process) // (max_workers * 8) or 1)

    all_new_rows = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        for idx, rows in enumerate(
            executor.map(_worker_process_file, files_to_process, chunksize=chunksize),
            start=1,
        ):
            if rows:
                rows = [r for r in rows if r.get("end_day") == today_str]
                all_new_rows.extend(rows)

            if idx % 500 == 0 or idx == len(files_to_process):
                log_event(
                    "info",
                    f"진행 {idx}/{len(files_to_process)} (누적 신규 row: {len(all_new_rows)}) "
                    f"[workers={max_workers}, chunksize={chunksize}]",
                    echo=False
                )

    if not all_new_rows:
        log_event("info", "신규 파싱된 데이터가 없음")
        return

    # INSERT + COMMIT (끊김 시 무한 재시도)
    while True:
        conn = ensure_conn()
        try:
            insert_main_rows(conn, all_new_rows)
            conn.commit()
            break
        except psycopg2.Error as e:
            if _is_connection_error(e):
                log_event("down", f"insert/commit connection error, retry: {type(e).__name__}: {repr(e)}")
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("insert/commit connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            log_event("error", f"insert/commit db error: {type(e).__name__}: {repr(e)}")
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        except Exception as e:
            log_event("error", f"insert/commit general error: {type(e).__name__}: {repr(e)}")
            try:
                conn.rollback()
            except Exception:
                pass
            raise

    new_file_paths = {r["file_path"] for r in all_new_rows}
    processed_set.update(new_file_paths)

    log_event("info", f"완료: 신규 파일 {len(new_file_paths)}개, 신규 row {len(all_new_rows)}개 적재 완료")


# ==========================
# 무한 루프 (5초)
# ==========================
def main():
    mp.freeze_support()

    _print("[boot] a3 vision realtime parser start")

    conn = ensure_conn()
    ensure_schema_and_table(conn)
    ensure_log_schema_and_table(conn)

    log_event("boot", "daemon started")
    log_event("info", f"work_mem={WORK_MEM}, loop_interval={LOOP_INTERVAL_SEC}s, realtime_window={REALTIME_WINDOW_SEC}s")

    current_day = datetime.now().strftime("%Y%m%d")
    processed_set = get_processed_file_paths_today(conn, current_day)
    log_event("info", f"loaded distinct file_path where end_day={current_day}: {len(processed_set)}")

    loop_count = 0

    while True:
        loop_count += 1
        try:
            today_str = datetime.now().strftime("%Y%m%d")

            if today_str != current_day:
                current_day = today_str
                processed_set = get_processed_file_paths_today(conn, current_day)
                log_event("info", f"day-change end_day={current_day} | reload db_paths={len(processed_set)}")

            if (loop_count % DB_RELOAD_EVERY_LOOPS) == 0:
                processed_set = get_processed_file_paths_today(conn, current_day)
                log_event("info", f"db-reload end_day={current_day} | db_paths={len(processed_set)}")

            run_once(conn, processed_set, current_day)

        except psycopg2.Error as e:
            if _is_connection_error(e):
                log_event("down", f"loop-level connection error: {repr(e)}")
                _reset_conn("loop-level connection error")
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            log_event("error", f"loop-level db error: {repr(e)}")
            raise

        except Exception as e:
            log_event("error", f"run_once exception: {type(e).__name__}: {repr(e)}")

        log_event("sleep", f"sleep {LOOP_INTERVAL_SEC}s", echo=False)
        time.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    main()
