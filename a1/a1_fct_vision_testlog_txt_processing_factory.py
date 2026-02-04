# -*- coding: utf-8 -*-
import os

# ✅ (중요) psycopg2/libpq가 뿜는 에러 메시지 인코딩 때문에 EXE에서 UnicodeDecodeError가 나는 경우 방지
# - DB 연결이 실패할 때(접속불가/인증실패/방화벽 등) 실제 원인 메시지를 UTF-8로 안전하게 출력하도록 강제
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

from pathlib import Path
from datetime import datetime
import time
import multiprocessing as mp
import traceback

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

REALTIME_WINDOW_SEC = 120

# ✅ 요구사항: 멀티프로세스 = 1개
MP_PROCESSES = 1

# ✅ 요구사항: 무한 루프 인터벌 5초
LOOP_INTERVAL_SEC = 5

# ✅ 요구사항: DB 접속 재시도 인터벌(연결 성공할 때까지 블로킹)
DB_RETRY_INTERVAL_SEC = 5

# ✅ 요구사항: work_mem 폭증 방지 (세션별 cap)
# - 환경변수로 조정 가능: PG_WORK_MEM (예: 4MB, 8MB, 16MB)
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
def log_print(msg: str) -> None:
    """
    콘솔 출력 + 파일 로그 동시 기록
    """
    try:
        print(msg, flush=True)
    except Exception:
        pass
    try:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        prev = LOG_FILE.read_text(encoding="utf-8", errors="ignore") if LOG_FILE.exists() else ""
        LOG_FILE.write_text(prev + f"[{ts}] {msg}\n", encoding="utf-8")
    except Exception:
        pass


def hold_console(exit_code: int) -> None:
    """
    ✅ EXE 더블클릭 실행 시 에러가 나도 콘솔이 닫히지 않도록 대기
    """
    try:
        log_print("\n" + "=" * 70)
        log_print(f"[HOLD] exit_code={exit_code}")
        log_print(f"[HOLD] 로그 파일: {LOG_FILE}")
        log_print("[HOLD] 콘솔을 닫으려면 Enter를 누르세요...")
        log_print("=" * 70)
        input()
    except EOFError:
        time.sleep(10)
    except Exception:
        try:
            time.sleep(10)
        except Exception:
            pass


# ============================================
# 1. DB 유틸 (요구사항 반영)
# - 서버가 중간에 끊겨도 무한 재접속 시도
# - 백엔드별 상시 연결 1개 고정(풀 최소화)
# - work_mem 폭증 방지 => 연결 직후 세션에 SET work_mem 적용
# ============================================
_CONN = None  # 상시 1개 연결 유지(프로세스당)


def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def _reset_connection(reason: str = ""):
    """
    커넥션 강제 폐기 + 다음 ensure_connection()에서 무한 재연결 유도
    """
    global _CONN
    try:
        if reason:
            log_print(f"[DB][RESET] {reason}")
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
    - OperationalError/InterfaceError는 거의 대부분 재연결 대상
    - 그 외에도 메시지에 connection 관련 키워드가 있으면 재연결로 처리
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
    세션(연결) 단위 안전 설정:
    - work_mem cap (폭증 방지)
    - SELECT 1로 연결 유효성 확인
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
    ✅ 연결 성공 시 work_mem cap 적용
    ✅ (권장) keepalive로 죽은 연결 감지 가속
    """
    while True:
        try:
            conn = psycopg2.connect(
                **DB_CONFIG,
                connect_timeout=5,
                application_name="a1_fct_vision_testlog_txt_processing_factory",

                # keepalive 옵션 (libpq 지원)
                keepalives=PG_KEEPALIVES,
                keepalives_idle=PG_KEEPALIVES_IDLE,
                keepalives_interval=PG_KEEPALIVES_INTERVAL,
                keepalives_count=PG_KEEPALIVES_COUNT,
            )
            conn.autocommit = False

            _apply_session_safety(conn)

            log_print(
                f"[DB][OK] connected (work_mem={WORK_MEM}, "
                f"keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT})"
            )
            return conn

        except Exception as e:
            log_print(f"[DB][RETRY] connect failed: {type(e).__name__}: {repr(e)}")
            try:
                time.sleep(DB_RETRY_INTERVAL_SEC)
            except Exception:
                pass


def ensure_connection():
    """
    ✅ 백엔드별 상시 연결 1개 고정(풀 최소화):
      - 프로세스 전체에서 _CONN 1개만 유지/재사용
      - 죽었으면 무한 재연결(블로킹)
    """
    global _CONN

    if _CONN is None:
        _CONN = get_connection_blocking()
        return _CONN

    if getattr(_CONN, "closed", 1) != 0:
        _reset_connection("conn.closed != 0")
        _CONN = get_connection_blocking()
        return _CONN

    # 살아있는지 가벼운 ping
    try:
        _apply_session_safety(_CONN)
        return _CONN
    except Exception as e:
        if _is_connection_error(e):
            log_print(f"[DB][WARN] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}")
            _reset_connection("ping failed")
            _CONN = get_connection_blocking()
            return _CONN
        # 연결문제가 아니라면 그대로 올려서 버그/SQL 문제를 드러냄
        raise


def init_db(conn):
    """
    (2)(3) 스키마 삭제
    (1) history 스키마/테이블 생성
    (추가) file_path 단독 중복 방지용 UNIQUE 인덱스 + end_day 조회 인덱스
    """
    cur = conn.cursor()

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

    conn.commit()
    cur.close()


def load_processed_paths_today(conn, end_day_today: str):
    """
    (변경) 오늘(end_day=오늘) 처리 이력의 file_path만 set 로드
    -> 메모리 사용량 대폭 감소
    """
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


def insert_history_rows(conn, rows):
    """
    (변경) UNIQUE(file_path) 기준으로 중복 방지
    - ON CONFLICT DO NOTHING 적용
    - ✅ 서버가 여기서 끊겨도 예외를 올려서 상위(run_once)가 무한 재접속하도록 함
    """
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
        # ✅ 연결 관련이면 상위로 올려서 재연결 루프 진입
        if _is_connection_error(e):
            raise
        # 연결 문제 아니면(예: 스키마/컬럼 오류) 역시 올려서 즉시 원인 노출
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
    """
    파일 6번째 줄의 Test Program으로 Vision1/Vision2 결정
    """
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
# 3. full_path(=file_path)에서 end_time / barcode / remark / result 추출
# ============================================
def parse_fields_from_full_path(full_path_str: str):
    """
    a) '_'과 '_' 사이는 yyyymmddhhmiss -> hh:mi:ss 로 end_time
    b) BA1WJ로 시작해서 '_' 전까지 barcode_information
    c) barcode_information 18번째가 J/S -> PD else Non-PD
    d) full_path 제일 뒤에서 5번째 글자:
       - 'P' -> PASS
       - 'F' -> FAIL
       (그 외/판정불가 -> None)
    """
    p = Path(full_path_str)
    filename = p.name
    stem = p.stem

    barcode_information = None
    end_time = None
    remark = None
    result = None

    parts = stem.split("_")

    # (b)
    if parts and parts[0].startswith("BA1WJ"):
        barcode_information = parts[0]

    # (a)
    if len(parts) >= 2:
        ts = parts[1]
        if ts.isdigit() and len(ts) == 14:
            hh = ts[8:10]
            mi = ts[10:12]
            ss = ts[12:14]
            end_time = f"{hh}:{mi}:{ss}"

    # (c)
    if barcode_information and len(barcode_information) >= 18:
        c18 = barcode_information[17]
        remark = "PD" if c18 in ("J", "S") else "Non-PD"

    # (d)
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

    # station 분류
    if mid in FCT_MAP:
        station = FCT_MAP[mid]
    else:
        station, _ = classify_vision_equipment(p)

    barcode_information, end_time, remark, result, filename = parse_fields_from_full_path(
        file_path_str
    )

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
# 5. 한 번 실행(run_once): 오늘 폴더 + 120초 이내 파일만 처리
# - ✅ 중간에 DB 서버가 끊겨도 무한 재연결 후 다시 수행
# ============================================
def run_once():
    started_at = datetime.now()
    now_ts = time.time()
    cutoff_ts = now_ts - REALTIME_WINDOW_SEC

    today_yyyymmdd = datetime.now().strftime("%Y%m%d")  # 오늘 날짜(end_day)

    log_print("\n==================== run_once 시작 ====================")
    log_print(f"시각: {started_at} / end_day(today)={today_yyyymmdd} / window={REALTIME_WINDOW_SEC}s")

    # ✅ DB 작업은 "연결 성공할 때까지" 무한 블로킹 재시도
    while True:
        conn = ensure_connection()
        try:
            init_db(conn)

            processed_paths = load_processed_paths_today(conn, today_yyyymmdd)
            log_print(f"[이력] 오늘(end_day={today_yyyymmdd}) 처리된 file_path 수 : {len(processed_paths)}")

            file_infos = []
            total_scanned = 0

            seen_paths_this_run = set()

            for mid in MIDDLE_FOLDERS:
                mid_path = BASE_LOG_DIR / mid
                if not mid_path.exists():
                    log_print(f"[SKIP] {mid_path} 없음")
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

                        # 120초 이내 수정된 파일만 처리
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

            log_print(f"[스캔] 전체 스캔 파일 수(오늘 폴더 내): {total_scanned}")
            log_print(f"[스캔] 이번 실행에서 새로 처리할 파일 수(120초 이내): {len(file_infos)}")

            if not file_infos:
                log_print("[정보] 새로 처리할 파일이 없습니다.")
                return

            log_print(f"[멀티프로세스] 사용 프로세스 수: {MP_PROCESSES}")

            with mp.Pool(processes=MP_PROCESSES) as pool:
                history_rows = pool.map(process_one_file, file_infos)

            # ✅ 여기서 끊겨도 insert_history_rows가 예외를 올림 -> 아래 재연결 루프로 진입
            n_try = insert_history_rows(conn, history_rows)
            log_print(f"[DB] history INSERT 시도 건수 : {n_try} (중복은 DB에서 자동 무시)")

            return  # 정상 종료

        except psycopg2.Error as e:
            # ✅ psycopg2 계열 오류 중 "연결 끊김"이면 무한 재연결 후 재시도
            if _is_connection_error(e):
                log_print(f"[DB][RETRY] connection lost during operation: {type(e).__name__}: {repr(e)}")
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

            # ✅ 연결 문제가 아닌 DB 오류(스키마/권한/SQL 등)는 무한루프 방지 위해 즉시 종료(상위로 전달)
            log_print(f"[DB][FATAL] non-connection db error: {type(e).__name__}: {repr(e)}")
            raise

        except Exception:
            # 기타 예외는 그대로 상위로 전달
            raise

        finally:
            # ✅ "상시 연결 1개 고정" 요구사항: 여기서 conn.close() 하지 않음
            log_print("==================== run_once 종료 ====================")


# ============================================
# 6. 메인 루프: 5초마다 무한 반복
# ============================================
if __name__ == "__main__":
    mp.freeze_support()
    exit_code = 0
    try:
        while True:
            run_once()
            time.sleep(LOOP_INTERVAL_SEC)

    except KeyboardInterrupt:
        log_print("\n[ABORT] 사용자에 의해 중단되었습니다. 프로그램을 종료합니다.")
        exit_code = 130

    except Exception as e:
        log_print("\n[ERROR] Unhandled exception: " + repr(e))
        log_print(traceback.format_exc())
        exit_code = 1

    finally:
        hold_console(exit_code)
        raise SystemExit(exit_code)
