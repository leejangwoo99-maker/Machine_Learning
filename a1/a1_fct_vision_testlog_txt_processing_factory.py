# -*- coding: utf-8 -*-
import os

# ✅ (중요) psycopg2/libpq가 뿜는 에러 메시지 인코딩 때문에 EXE에서 UnicodeDecodeError가 나는 경우 방지
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

from pathlib import Path
from datetime import datetime
import time
import multiprocessing as mp
import traceback
from typing import Optional, List, Dict

import pandas as pd
import psycopg2
from psycopg2.extras import execute_batch


# ============================================
# 0. 공장 전용 설정 (고정)
# ============================================
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")

MIDDLE_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision03"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

FCT_MAP = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_HISTORY = "a1_fct_vision_testlog_txt_processing_history"
TABLE_HISTORY  = "fct_vision_testlog_txt_processing_history"

SCHEMA_RESULT = "a1_fct_vision_testlog_txt_processing_result"
SCHEMA_DETAIL = "a1_fct_vision_testlog_txt_processing_result_detail"

# ✅ 추가: 데몬 상태 로그 저장 스키마/테이블
SCHEMA_HEALTH = "k_demon_heath_check"
TABLE_HEALTH_LOG = "a1_log"

REALTIME_WINDOW_SEC = 120

# ✅ 요구사항: 멀티프로세스 = 1개
MP_PROCESSES = 1

# ✅ 요구사항: 무한 루프 인터벌 5초
LOOP_INTERVAL_SEC = 5

# ✅ 요구사항: DB 접속 재시도 인터벌(연결 성공할 때까지 블로킹)
DB_RETRY_INTERVAL_SEC = 5

# ✅ 요구사항: work_mem 폭증 방지 (세션별 cap)
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ EXE 실행 시 문제 추적용 로그 파일(같은 폴더에 생성)
LOG_FILE = Path(__file__).with_name("a1_fct_vision_testlog_txt_processing_factory.log")

# ✅ (추가/권장) 끊긴 TCP 세션을 빠르게 감지하기 위한 keepalive
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))


# ============================================
# 0-1. 콘솔 HOLD + 로깅 유틸
# ============================================
_CONN = None  # 상시 1개 연결 유지(프로세스당)
_HEALTH_TABLE_READY = False


def _now_day_time():
    n = datetime.now()
    return n.strftime("%Y%m%d"), n.strftime("%H:%M:%S")


def _normalize_info(info: str) -> str:
    if not info:
        return "info"
    return str(info).strip().lower()


def _safe_append_file_log(msg: str) -> None:
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with open(LOG_FILE, "a", encoding="utf-8", errors="ignore") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


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


def _apply_session_safety(conn):
    with conn.cursor() as cur:
        cur.execute("SET work_mem TO %s;", (WORK_MEM,))
        cur.execute("SELECT 1;")
    try:
        conn.rollback()
    except Exception:
        pass


def _reset_connection(reason: str = ""):
    global _CONN, _HEALTH_TABLE_READY
    try:
        if reason:
            log_print(f"[DB][RESET] {reason}", info="down")
    except Exception:
        pass
    try:
        _safe_close(_CONN)
    except Exception:
        pass
    _CONN = None
    _HEALTH_TABLE_READY = False


def get_connection_blocking():
    while True:
        try:
            conn = psycopg2.connect(
                **DB_CONFIG,
                connect_timeout=5,
                application_name="a1_fct_vision_testlog_txt_processing_factory",
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
            )
            conn.autocommit = False
            _apply_session_safety(conn)
            log_print(
                f"[DB][OK] connected (work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})",
                info="info",
                save_db=False,  # 연결 직후라 별도 저장은 아래 ensure_health_log_table 후 가능
            )
            return conn

        except Exception as e:
            msg = f"[DB][RETRY] connect failed: {type(e).__name__}: {repr(e)}"
            log_print(msg, info="down", save_db=False)
            try:
                time.sleep(DB_RETRY_INTERVAL_SEC)
            except Exception:
                pass


def ensure_connection():
    global _CONN
    if _CONN is None:
        _CONN = get_connection_blocking()
        return _CONN

    if getattr(_CONN, "closed", 1) != 0:
        _reset_connection("conn.closed != 0")
        _CONN = get_connection_blocking()
        return _CONN

    try:
        _apply_session_safety(_CONN)
        return _CONN
    except Exception as e:
        if _is_connection_error(e):
            log_print(f"[DB][WARN] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}", info="down", save_db=False)
            _reset_connection("ping failed")
            _CONN = get_connection_blocking()
            return _CONN
        raise


def ensure_health_log_table(conn):
    """
    ✅ 사양:
      - 스키마: k_demon_heath_check (없으면 생성)
      - 테이블: a1_log (없으면 생성)
      - 컬럼: end_day, end_time, info, contents
    """
    global _HEALTH_TABLE_READY
    if _HEALTH_TABLE_READY:
        return

    cur = conn.cursor()
    try:
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_HEALTH};")
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SCHEMA_HEALTH}.{TABLE_HEALTH_LOG} (
                id BIGSERIAL PRIMARY KEY,
                end_day   TEXT NOT NULL,   -- yyyymmdd
                end_time  TEXT NOT NULL,   -- hh:mi:ss
                info      TEXT NOT NULL,   -- 소문자
                contents  TEXT NOT NULL
            );
            """
        )
        cur.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_{TABLE_HEALTH_LOG}_day_time
            ON {SCHEMA_HEALTH}.{TABLE_HEALTH_LOG}(end_day, end_time);
            """
        )
        conn.commit()
        _HEALTH_TABLE_READY = True
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def _insert_health_logs_df(conn, df: pd.DataFrame):
    """
    ✅ 사양 6:
      end_day, end_time, info, contents 순서 DataFrame화 후 저장
    """
    if df is None or df.empty:
        return

    cols = ["end_day", "end_time", "info", "contents"]
    df = df[cols].copy()

    rows = [tuple(x) for x in df.itertuples(index=False, name=None)]
    cur = conn.cursor()
    try:
        execute_batch(
            cur,
            f"""
            INSERT INTO {SCHEMA_HEALTH}.{TABLE_HEALTH_LOG}
            (end_day, end_time, info, contents)
            VALUES (%s, %s, %s, %s)
            """,
            rows,
            page_size=500
        )
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        cur.close()


def save_health_log(info: str, contents: str, conn: Optional[psycopg2.extensions.connection] = None):
    """
    데몬 동작 로그를 DB에 저장.
    - info는 소문자 강제
    - end_day/end_time 자동 생성
    - DataFrame으로 컬럼 순서 맞춰 저장
    """
    info = _normalize_info(info)
    end_day, end_time = _now_day_time()

    df = pd.DataFrame(
        [{
            "end_day": end_day,
            "end_time": end_time,
            "info": info,
            "contents": str(contents) if contents is not None else "",
        }],
        columns=["end_day", "end_time", "info", "contents"]  # 컬럼 순서 고정
    )

    # conn이 주어지지 않으면 현재 글로벌 연결 사용 시도
    local_conn = conn
    if local_conn is None:
        global _CONN
        local_conn = _CONN

    if local_conn is None or getattr(local_conn, "closed", 1) != 0:
        return  # DB 미연결 상태에서 무리하게 블로킹하지 않음(기존 흐름 유지)

    try:
        ensure_health_log_table(local_conn)
        _insert_health_logs_df(local_conn, df)
    except Exception:
        # health log 저장 실패는 본 로직을 막지 않음
        pass


def log_print(msg: str, info: str = "info", save_db: bool = True) -> None:
    """
    콘솔 출력 + 파일 로그 + (가능하면) DB 상태 로그 저장
    """
    try:
        print(msg, flush=True)
    except Exception:
        pass

    _safe_append_file_log(msg)

    if save_db:
        save_health_log(info=info, contents=msg)


def hold_console(exit_code: int) -> None:
    try:
        log_print("\n" + "=" * 70, info="info")
        log_print(f"[HOLD] exit_code={exit_code}", info="info")
        log_print(f"[HOLD] 로그 파일: {LOG_FILE}", info="info")
        log_print("[HOLD] 콘솔을 닫으려면 Enter를 누르세요...", info="sleep")
        log_print("=" * 70, info="info")
        input()
    except EOFError:
        time.sleep(10)
    except Exception:
        try:
            time.sleep(10)
        except Exception:
            pass


# ============================================
# 1. DB 초기화(기존 + health table)
# ============================================
def init_db(conn):
    cur = conn.cursor()

    # 기존 요구사항 유지
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_RESULT} CASCADE;")
    cur.execute(f"DROP SCHEMA IF EXISTS {SCHEMA_DETAIL} CASCADE;")

    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_HISTORY};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_HISTORY}.{TABLE_HISTORY} (
            id                  BIGSERIAL PRIMARY KEY,
            barcode_information TEXT,
            station             TEXT,
            end_day             TEXT,
            end_time            TEXT,
            remark              TEXT,
            result              TEXT,
            goodorbad           TEXT,
            filename            TEXT NOT NULL,
            file_path           TEXT NOT NULL,
            processed_at        TIMESTAMPTZ NOT NULL
        );
        """
    )

    cur.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_{TABLE_HISTORY}_file_path
        ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(file_path);
        """
    )

    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_end_day
        ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(end_day);
        """
    )

    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{TABLE_HISTORY}_barcode ON {SCHEMA_HISTORY}.{TABLE_HISTORY}(barcode_information);"
    )

    # ✅ 추가: health log 저장 테이블 생성
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_HEALTH};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_HEALTH}.{TABLE_HEALTH_LOG} (
            id BIGSERIAL PRIMARY KEY,
            end_day   TEXT NOT NULL,
            end_time  TEXT NOT NULL,
            info      TEXT NOT NULL,
            contents  TEXT NOT NULL
        );
        """
    )
    cur.execute(
        f"""
        CREATE INDEX IF NOT EXISTS idx_{TABLE_HEALTH_LOG}_day_time
        ON {SCHEMA_HEALTH}.{TABLE_HEALTH_LOG}(end_day, end_time);
        """
    )

    conn.commit()
    cur.close()

    global _HEALTH_TABLE_READY
    _HEALTH_TABLE_READY = True


def load_processed_paths_today(conn, end_day_today: str):
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT file_path
        FROM {SCHEMA_HISTORY}.{TABLE_HISTORY}
        WHERE end_day = %s;
        """,
        (end_day_today,),
    )
    rows = cur.fetchall()
    cur.close()
    return {fp for (fp,) in rows if fp}


def insert_history_rows(conn, rows: List[Dict]):
    if not rows:
        return 0

    cur = conn.cursor()
    try:
        execute_batch(
            cur,
            f"""
            INSERT INTO {SCHEMA_HISTORY}.{TABLE_HISTORY}
                (barcode_information, station, end_day, end_time, remark, result,
                 goodorbad, filename, file_path, processed_at)
            VALUES (%s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s)
            ON CONFLICT (file_path) DO NOTHING
            """,
            [
                (
                    r["barcode_information"],
                    r["station"],
                    r["end_day"],
                    r["end_time"],
                    r["remark"],
                    r["result"],
                    r["goodorbad"],
                    r["filename"],
                    r["file_path"],
                    r["processed_at"],
                )
                for r in rows
            ],
            page_size=1000,
        )
        conn.commit()
        return len(rows)

    except Exception as e:
        try:
            conn.rollback()
        except Exception:
            pass
        if _is_connection_error(e):
            raise
        raise

    finally:
        try:
            cur.close()
        except Exception:
            pass


# ============================================
# 2. Vision 설비 분류
# ============================================
def classify_vision_equipment(file_path: Path):
    equipment = "Vision3"
    test_program = None
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
        if len(lines) >= 6:
            line6 = lines[5]
            if "Test Program" in line6:
                if "LED1" in line6:
                    equipment, test_program = "Vision1", "LED1"
                elif "LED2" in line6:
                    equipment, test_program = "Vision2", "LED2"
                else:
                    equipment = "Vision3"
            else:
                equipment = "Vision3"
    except Exception:
        equipment = "Vision3"
        test_program = None
    return equipment, test_program


# ============================================
# 3. full_path 파싱
# ============================================
def parse_fields_from_full_path(full_path_str: str):
    p = Path(full_path_str)
    filename = p.name
    stem = p.stem

    barcode_information = None
    end_time = None
    remark = None
    result = None

    parts = stem.split("_")

    if parts and parts[0].startswith("BA1WJ"):
        barcode_information = parts[0]

    if len(parts) >= 2:
        ts = parts[1]
        if ts.isdigit() and len(ts) == 14:
            hh = ts[8:10]
            mi = ts[10:12]
            ss = ts[12:14]
            end_time = f"{hh}:{mi}:{ss}"

    if barcode_information and len(barcode_information) >= 18:
        c18 = barcode_information[17]
        remark = "PD" if c18 in ("J", "S") else "Non-PD"

    if len(full_path_str) >= 5:
        ch = full_path_str[-5]
        if ch == "P":
            result = "PASS"
        elif ch == "F":
            result = "FAIL"

    return barcode_information, end_time, remark, result, filename


# ============================================
# 4. 한 파일 처리 (멀티프로세스)
# ============================================
def process_one_file(args):
    file_path_str, mid, folder_date, gb = args
    p = Path(file_path_str)

    if mid in FCT_MAP:
        station = FCT_MAP[mid]
    else:
        station, _ = classify_vision_equipment(p)

    barcode_information, end_time, remark, result, filename = parse_fields_from_full_path(file_path_str)

    return {
        "barcode_information": barcode_information,
        "station": station,
        "end_day": folder_date,
        "end_time": end_time,
        "remark": remark,
        "result": result,
        "goodorbad": gb,
        "filename": filename,
        "file_path": file_path_str,
        "processed_at": datetime.now(),
    }


# ============================================
# 5. 한 번 실행(run_once)
# ============================================
def run_once():
    started_at = datetime.now()
    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_WINDOW_SEC
    today_yyyymmdd = datetime.now().strftime("%Y%m%d")

    log_print("\n==================== run_once 시작 ====================", info="info")
    log_print(f"시각: {started_at} / end_day(today)={today_yyyymmdd} / window={REALTIME_WINDOW_SEC}s", info="info")

    while True:
        conn = ensure_connection()
        try:
            init_db(conn)

            processed_paths = load_processed_paths_today(conn, today_yyyymmdd)
            log_print(f"[이력] 오늘(end_day={today_yyyymmdd}) 처리된 file_path 수 : {len(processed_paths)}", info="info")

            file_infos = []
            total_scanned = 0
            seen_paths_this_run = set()

            for mid in MIDDLE_FOLDERS:
                mid_path = BASE_LOG_DIR / mid
                if not mid_path.exists():
                    log_print(f"[SKIP] {mid_path} 없음", info="warn")
                    continue

                date_folder = mid_path / today_yyyymmdd
                if not date_folder.exists() or (not date_folder.is_dir()):
                    continue

                folder_date = date_folder.name

                for gb in TARGET_FOLDERS:
                    gb_path = date_folder / gb
                    if not gb_path.exists():
                        continue

                    for f in gb_path.iterdir():
                        if not f.is_file():
                            continue

                        total_scanned += 1

                        try:
                            mtime = f.stat().st_mtime
                        except Exception:
                            continue

                        if mtime < cutoff_ts:
                            continue

                        file_path_str = str(f)

                        if file_path_str in processed_paths:
                            continue

                        if file_path_str in seen_paths_this_run:
                            continue

                        seen_paths_this_run.add(file_path_str)
                        file_infos.append((file_path_str, mid, folder_date, gb))

            log_print(f"[스캔] 전체 스캔 파일 수(오늘 폴더 내): {total_scanned}", info="info")
            log_print(f"[스캔] 이번 실행에서 새로 처리할 파일 수(120초 이내): {len(file_infos)}", info="info")

            if not file_infos:
                log_print("[정보] 새로 처리할 파일이 없습니다.", info="sleep")
                return

            log_print(f"[멀티프로세스] 사용 프로세스 수: {MP_PROCESSES}", info="info")

            with mp.Pool(processes=MP_PROCESSES) as pool:
                history_rows = pool.map(process_one_file, file_infos)

            n_try = insert_history_rows(conn, history_rows)
            log_print(f"[DB] history INSERT 시도 건수 : {n_try} (중복은 DB에서 자동 무시)", info="info")

            return

        except psycopg2.Error as e:
            if _is_connection_error(e):
                log_print(f"[DB][RETRY] connection lost during operation: {type(e).__name__}: {repr(e)}", info="down", save_db=False)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_connection("operation failed (connection error)")
                try:
                    time.sleep(DB_RETRY_INTERVAL_SEC)
                except Exception:
                    pass
                continue

            log_print(f"[DB][FATAL] non-connection db error: {type(e).__name__}: {repr(e)}", info="error")
            raise

        except Exception:
            raise

        finally:
            log_print("==================== run_once 종료 ====================", info="info")


# ============================================
# 6. 메인 루프: 5초마다 무한 반복
# ============================================
if __name__ == "__main__":
    mp.freeze_support()
    exit_code = 0
    try:
        while True:
            run_once()
            log_print(f"[SLEEP] {LOOP_INTERVAL_SEC}초 대기", info="sleep")
            time.sleep(LOOP_INTERVAL_SEC)

    except KeyboardInterrupt:
        log_print("\n[ABORT] 사용자에 의해 중단되었습니다. 프로그램을 종료합니다.", info="error")
        exit_code = 130

    except Exception as e:
        log_print("\n[ERROR] Unhandled exception: " + repr(e), info="error")
        log_print(traceback.format_exc(), info="error")
        exit_code = 1

    finally:
        hold_console(exit_code)
        raise SystemExit(exit_code)
