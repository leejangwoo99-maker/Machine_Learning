# -*- coding: utf-8 -*-
"""
c1_fct_detail_loader_factory.py
============================================
FCT Detail TXT Parser -> PostgreSQL 적재 (공장용 최종본)

[기존 핵심 요구사항 유지]
1) ✅ mtime(수정시간) 완전 미사용 (UNC/NAS 환경에서 신뢰 불가)
2) ✅ 파일명 끝 토큰 "_HHMMSS(.fff)" = "시작시간" 으로 해석하여 후보를 선별
3) ✅ 파일 완료 판정 = "파일 크기 안정화"
4) ✅ 자정 전 시작→자정 후 종료로 인해 동일 run이 2개 폴더에 남는 케이스 대응(run_id 기반 + best path 선택)
5) ✅ DB 중복 방지: run_id 컬럼 + UNIQUE(run_id,test_time,contents) + ON CONFLICT DO NOTHING
6) ✅ 루프 기반 실시간 처리

[요구 사양 반영(추가/수정)]
- 멀티프로세스 = 1개
- 무한 루프 인터벌 5초
- DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
- 백엔드별 상시 연결을 1개로 고정(풀 최소화)  -> 프로세스 전체에서 psycopg2 커넥션 1개만 유지/재사용
- work_mem 폭증 방지 -> 연결 직후 세션에 SET work_mem 적용(환경변수로 조정 가능)
- ✅ (추가) 실행 중 서버 끊김/네트워크 단절 발생 시: 감지 즉시 커넥션 폐기 후 무한 재접속 + 재시도
- ✅ (추가/권장) keepalive 옵션으로 죽은 TCP 세션 감지 가속(환경변수로 조정 가능)
"""

import os
import re
import time as time_mod
from pathlib import Path
from datetime import datetime, timedelta, date, time as dt_time
from multiprocessing import Pool, freeze_support
import urllib.parse
from typing import Dict, Tuple, List, Optional

import psycopg2
from psycopg2.extras import execute_values


# =========================
# ✅ (중요) psycopg2/libpq 에러 메시지 인코딩 이슈(EXE) 방지
# =========================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")


# =========================
# 0) 설정
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")  # 루트 (YYYY/MM/DD 구조)

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "c1_fct_detail"
TABLE_NAME = "fct_detail"

# ✅ 후보 탐색 폭: "시작시간(파일명)" 기준 최근 N초 내 파일만 후보로 봄
CANDIDATE_WINDOW_SEC = 3600  # 60분 권장(운영 안정화 후 줄여도 됨)

# ✅ 파일 완료 판정: size가 연속 N회 동일하면 완료(안정화)
STABLE_REQUIRED = 3

# ✅ 요구사항: 무한루프 주기(초) = 5초
LOOP_SLEEP_SEC = 5

# ✅ 요구사항: 멀티프로세스 = 1개
MP_PROCESSES = 1
POOL_CHUNKSIZE = 10

# 라인 패턴: [hh:mm:ss.ss] 내용
LINE_RE = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{1,3})\]\s(.+)$")

# ✅ 파일명: (barcode)_yyyymmdd_(HHMMSS 또는 HHMMSS.xxx).txt
FNAME_RE = re.compile(r"^(.*)_(\d{8})_(\d{6}(?:\.\d{1,3})?)\.txt$", re.IGNORECASE)

# ✅ 요구사항: DB 접속 실패 시 무한 재시도(블로킹)
DB_RETRY_INTERVAL_SEC = 5

# ✅ 요구사항: work_mem 폭증 방지(세션별 cap) - 환경변수로 조정 가능
#    예) set PG_WORK_MEM=8MB
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# ✅ (추가/권장) 끊긴 TCP 세션을 빠르게 감지(환경변수로 조정 가능)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

# ✅ 요구사항: 백엔드별 상시 연결 1개 고정
_CONN = None


# =========================
# 1) DB / 유틸
# =========================
def _conn_str(cfg: dict) -> str:
    pw = urllib.parse.quote_plus(cfg["password"])
    return f"postgresql+psycopg2://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"


def _safe_close(conn):
    try:
        conn.close()
    except Exception:
        pass


def _reset_conn(reason: str = ""):
    """커넥션 강제 폐기 + 다음 ensure에서 무한 재연결 유도"""
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


def _psycopg2_conn_blocking(cfg: dict):
    """
    ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
    + keepalive로 죽은 TCP 세션 감지 가속
    """
    while True:
        try:
            conn = psycopg2.connect(
                host=cfg["host"],
                port=cfg["port"],
                dbname=cfg["dbname"],
                user=cfg["user"],
                password=cfg["password"],
                connect_timeout=5,
                application_name="c1_fct_detail_loader_factory",

                # keepalive 옵션 (libpq 지원)
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
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            except Exception:
                pass


def _ensure_conn():
    """
    ✅ 백엔드별 상시 연결 1개 고정(풀 최소화)
    - 프로세스 전체에서 커넥션 1개만 유지/재사용
    - 죽었으면 무한 재연결(블로킹)
    - 실행 중 끊김도 ping에서 감지/복구
    """
    global _CONN

    if _CONN is None:
        _CONN = _psycopg2_conn_blocking(DB_CONFIG)
        return _CONN

    if getattr(_CONN, "closed", 1) != 0:
        _reset_conn("conn.closed != 0")
        _CONN = _psycopg2_conn_blocking(DB_CONFIG)
        return _CONN

    try:
        _apply_session_safety(_CONN)
        return _CONN
    except Exception as e:
        if _is_connection_error(e):
            print(f"[DB][WARN] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}", flush=True)
            _reset_conn("ping failed")
            _CONN = _psycopg2_conn_blocking(DB_CONFIG)
            return _CONN
        raise


def _ensure_schema_and_table_and_runid():
    """
    - 스키마/테이블 보장
    - run_id 컬럼 추가
    - UNIQUE INDEX (run_id, test_time, contents) 보장
    ✅ 실행 중 끊김 포함: DB 실패 시 무한 재시도(블로킹)
    """
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        barcode_information TEXT,
        remark              TEXT,
        end_day             DATE,
        end_time            TIME,
        contents            VARCHAR(80),
        test_ct             DOUBLE PRECISION,
        test_time           VARCHAR(12),
        file_path           TEXT
    );

    ALTER TABLE {SCHEMA_NAME}.{TABLE_NAME}
    ADD COLUMN IF NOT EXISTS run_id TEXT;

    CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_file_path
        ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);

    CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_end_day
        ON {SCHEMA_NAME}.{TABLE_NAME} (end_day);

    -- ✅ 라인 단위 중복 방지(운영 안전장치)
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_NAME}_runid_dedup
        ON {SCHEMA_NAME}.{TABLE_NAME} (run_id, test_time, contents);
    """

    while True:
        conn = _ensure_conn()
        try:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.autocommit = False
            return
        except psycopg2.Error as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] DDL failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("DDL connection error")
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][FATAL] DDL db error: {type(e).__name__}: {repr(e)}", flush=True)
            raise
        except Exception:
            raise


def _load_processed_run_sizes() -> Dict[str, int]:
    """
    ✅ 재시작 후에도 효율을 위해 run_id별 처리 완료(또는 처리한 파일 크기)를 DB에서 복원
    - 같은 run_id가 이미 DB에 있으면, 기본적으로 "한 번은 처리됐다"는 뜻.
    - size는 런타임에서만 관리(재시작 시 0으로 초기화). (기존 로직 유지)
    ✅ 실행 중 끊김 포함: DB 실패 시 무한 재시도(블로킹)
    """
    sql = f"""
    SELECT DISTINCT run_id
    FROM {SCHEMA_NAME}.{TABLE_NAME}
    WHERE run_id IS NOT NULL
      AND run_id <> '';
    """
    out: Dict[str, int] = {}

    while True:
        conn = _ensure_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                rows = cur.fetchall()
            for r in rows:
                if r and r[0]:
                    out[str(r[0])] = 0
            return out
        except psycopg2.Error as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] load_processed_run_ids failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("load_processed_run_ids connection error")
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][FATAL] load_processed_run_ids db error: {type(e).__name__}: {repr(e)}", flush=True)
            raise
        except Exception:
            raise


def _infer_remark_strict(file_path: Path) -> Optional[str]:
    parts_upper = [p.upper() for p in file_path.parts]
    if any("PD NONE" in p for p in parts_upper):
        return "Non-PD"
    if any("PD" in p for p in parts_upper):
        return "PD"
    return None


def _safe_read_lines(path: Path) -> List[str]:
    encodings = ["cp949", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read().splitlines()
        except Exception:
            pass
    with open(path, "r", encoding="cp949", errors="replace") as f:
        return f.read().splitlines()


def _parse_time_to_seconds(t_str: str) -> float:
    hh = int(t_str[0:2])
    mm = int(t_str[3:5])
    ss = float(t_str[6:])
    return hh * 3600.0 + mm * 60.0 + ss


def _round_to_hms(t_str: str) -> dt_time:
    sec = _parse_time_to_seconds(t_str)
    sec_rounded = int(sec + 0.5)
    sec_rounded %= 24 * 3600
    hh = sec_rounded // 3600
    mm = (sec_rounded % 3600) // 60
    ss = sec_rounded % 60
    return dt_time(hour=hh, minute=mm, second=ss)


def _file_info_from_filename(fp: Path) -> Optional[Tuple[str, date, str, datetime]]:
    """
    return (run_id, base_day(date from yyyymmdd), start_hhmmss, start_dt)
    """
    m = FNAME_RE.match(fp.name)
    if not m:
        return None
    barcode = m.group(1).strip()
    yyyymmdd = m.group(2).strip()
    hhmmss_raw = m.group(3).strip()
    hhmmss = hhmmss_raw.split(".")[0]

    try:
        base_day = datetime.strptime(yyyymmdd, "%Y%m%d").date()
        start_dt = datetime.strptime(yyyymmdd + hhmmss, "%Y%m%d%H%M%S")
    except Exception:
        return None

    run_id = f"{barcode}_{yyyymmdd}_{hhmmss}"
    return run_id, base_day, hhmmss, start_dt


# =========================
# 2) 완료 판정(파일 size 안정화) 상태 캐시
# =========================
# path -> (last_size, stable_count)
FILE_STATE: Dict[str, Tuple[int, int]] = {}


def _update_stable_state(path_str: str) -> Tuple[Optional[int], int]:
    """
    return (size or None, stable_count)
    """
    p = Path(path_str)
    try:
        size = int(p.stat().st_size)
    except Exception:
        return None, 0

    last_size, stable = FILE_STATE.get(path_str, (None, 0))
    if last_size is None:
        FILE_STATE[path_str] = (size, 0)
        return size, 0

    if size == last_size:
        stable += 1
    else:
        stable = 0

    FILE_STATE[path_str] = (size, stable)
    return size, stable


# =========================
# 3) 스캔: 시작시간 기반 후보 + 자정 경계 폴더 자동 커버
# =========================
def _collect_candidates(now_dt: datetime, window_sec: int) -> Dict[str, List[str]]:
    """
    return: run_id -> [path1, path2, ...]  (동일 run_id가 여러 폴더에 존재 가능)
    """
    start_dt = now_dt - timedelta(seconds=window_sec)

    # 후보 window가 걸치는 날짜 폴더만 스캔(자정 케이스 자동 포함)
    day_set = {start_dt.date(), now_dt.date()}

    out: Dict[str, List[str]] = {}

    for day in day_set:
        y = f"{day.year:04d}"
        m = f"{day.month:02d}"
        d = f"{day.day:02d}"
        day_dir = BASE_DIR / y / m / d
        if not day_dir.exists():
            continue

        for fp in day_dir.rglob("*.txt"):
            if not fp.is_file():
                continue
            info = _file_info_from_filename(fp)
            if info is None:
                continue
            run_id, _, _, start_time_dt = info

            # ✅ 시작시간이 window 내인 파일만 후보
            if start_dt <= start_time_dt <= now_dt:
                out.setdefault(run_id, []).append(str(fp))

    return out


def _choose_best_path_for_run(run_id: str, paths: List[str]) -> Optional[str]:
    """
    동일 run_id에 대해 여러 path가 있으면 1개 선택:
    1) stable(>=STABLE_REQUIRED) 된 파일 우선
    2) 그 중 size 큰 것 우선
    3) stable이 하나도 없으면 None (아직 처리하지 않음)
    """
    best_path = None
    best_tuple = None  # (is_stable, size)

    for p in paths:
        size, stable = _update_stable_state(p)
        if size is None:
            continue
        is_stable = 1 if (stable >= STABLE_REQUIRED) else 0
        cand = (is_stable, size)

        if best_tuple is None or cand > best_tuple:
            best_tuple = cand
            best_path = p

    if best_tuple is None:
        return None
    if best_tuple[0] <= 0:
        return None  # 안정화된 파일이 없음
    return best_path


# =========================
# 4) 파일 파싱 (worker)
# =========================
def _parse_one_file_worker(args):
    """
    args = (path_str, run_id, base_day)
    return: (path_str, run_id, rows, status)
    status: OK | SKIP_REMARK | SKIP_BADNAME | SKIP_EMPTY | ERROR
    """
    path_str, run_id, base_day = args
    p = Path(path_str)

    # remark strict
    remark = _infer_remark_strict(p)
    if remark is None:
        return path_str, run_id, [], "SKIP_REMARK"

    # 파일명 재검증(안전)
    m = FNAME_RE.match(p.name)
    if not m:
        return path_str, run_id, [], "SKIP_BADNAME"

    barcode = m.group(1).strip()

    try:
        lines = _safe_read_lines(p)
    except Exception:
        return path_str, run_id, [], "ERROR"

    parsed_times: List[str] = []
    parsed_contents: List[str] = []

    for line in lines:
        mm2 = LINE_RE.match(line)
        if not mm2:
            continue
        t_str = mm2.group(1).strip()
        content = mm2.group(2).strip()
        if not content:
            continue
        parsed_times.append(t_str[:12])
        parsed_contents.append(content[:80])

    if not parsed_times:
        return path_str, run_id, [], "SKIP_EMPTY"

    # end_time(파일 내용 마지막 timestamp 반올림)
    end_time_obj = _round_to_hms(parsed_times[-1])

    # 자정 넘김 판정은 "파일 내용 기준"이 가장 안전
    first_sec = _parse_time_to_seconds(parsed_times[0])
    last_sec = _parse_time_to_seconds(parsed_times[-1])
    end_day = base_day
    if last_sec < first_sec:
        end_day = base_day + timedelta(days=1)

    # rows 생성
    rows = []
    prev_sec = None

    for t_str, content in zip(parsed_times, parsed_contents):
        cur_sec = _parse_time_to_seconds(t_str)
        test_ct = None
        if prev_sec is not None:
            diff = cur_sec - prev_sec
            if diff < 0:
                diff += 86400.0
            test_ct = diff
        prev_sec = cur_sec

        rows.append(
            (
                barcode,
                remark,
                end_day,
                end_time_obj,
                content,
                test_ct,
                t_str,
                str(p),
                run_id,
            )
        )

    return path_str, run_id, rows, "OK"


# =========================
# 5) DB Insert (ON CONFLICT DO NOTHING)
# =========================
def _insert_rows(conn, rows: List[tuple]) -> int:
    """
    ✅ 상시 연결 1개(conn)로 insert 수행
    ✅ 실행 중 서버 끊김 포함: DB 실패 시 무한 재시도(블로킹)
    """
    if not rows:
        return 0

    sql = f"""
    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
    (barcode_information, remark, end_day, end_time, contents, test_ct, test_time, file_path, run_id)
    VALUES %s
    ON CONFLICT (run_id, test_time, contents) DO NOTHING
    """

    while True:
        conn = _ensure_conn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=5000)
            conn.commit()
            return len(rows)

        except psycopg2.Error as e:
            if _is_connection_error(e):
                print(f"[DB][RETRY] insert failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    conn.rollback()
                except Exception:
                    pass
                _reset_conn("insert connection error")
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][FATAL] insert db error: {type(e).__name__}: {repr(e)}", flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            raise

        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise


# =========================
# 6) main loop
# =========================
def main():
    print(f"[INFO] Connection String: {_conn_str(DB_CONFIG)}")
    print(f"[INFO] BASE_DIR={BASE_DIR}")

    # ✅ 상시 연결 1개 확보(여기서부터 연결 실패 시 무한 블로킹 재시도)
    _ensure_conn()

    _ensure_schema_and_table_and_runid()
    print(f"[INFO] Table ensured: {SCHEMA_NAME}.{TABLE_NAME} (+ run_id + unique index)")

    # run_id별 처리 size 캐시(재시작 복원: run_id 존재만 복원, size는 0)
    processed_run_size = _load_processed_run_sizes()
    print(f"[INFO] processed_run_ids_loaded_from_db={len(processed_run_size):,}")

    print(f"[INFO] Candidate window(sec)={CANDIDATE_WINDOW_SEC:,} (filename start time based)")
    print(f"[INFO] Stable required(count)={STABLE_REQUIRED} (file size unchanged counts)")
    print(f"[INFO] Loop every {LOOP_SLEEP_SEC}s | MP={MP_PROCESSES}, chunksize={POOL_CHUNKSIZE}")
    print(f"[INFO] Session work_mem cap={WORK_MEM}")
    print(f"[INFO] TCP keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}")
    print("[INFO] Dedup policy: run_id based + DB UNIQUE(run_id,test_time,contents)")

    total_attempted_rows = 0
    loop_count = 0

    while True:
        try:
            loop_count += 1
            now_dt = datetime.now()

            # 1) 후보 수집: run_id -> paths
            cand_map = _collect_candidates(now_dt, CANDIDATE_WINDOW_SEC)
            if not cand_map:
                time_mod.sleep(LOOP_SLEEP_SEC)
                continue

            # 2) run_id별 best_path 선정(안정화된 파일만)
            ready_tasks = []
            ready_runs = 0
            skipped_not_stable = 0
            skipped_already_done = 0

            for run_id, paths in cand_map.items():
                best_path = _choose_best_path_for_run(run_id, paths)
                if best_path is None:
                    skipped_not_stable += 1
                    continue

                # size 확인 (state에 이미 갱신되어 있음)
                size, stable = _update_stable_state(best_path)
                if size is None or stable < STABLE_REQUIRED:
                    skipped_not_stable += 1
                    continue

                # ✅ 이미 처리한 run_id라도, 더 큰 size가 나타나면 재처리 허용
                prev_size = processed_run_size.get(run_id, -1)
                if prev_size >= 0 and size <= prev_size:
                    skipped_already_done += 1
                    continue

                # worker에 base_day 전달(파일명에서 date 추출)
                info = _file_info_from_filename(Path(best_path))
                if info is None:
                    continue
                _, base_day, _, _ = info

                ready_tasks.append((best_path, run_id, base_day))
                ready_runs += 1

            if not ready_tasks:
                time_mod.sleep(LOOP_SLEEP_SEC)
                continue

            # 3) 파싱 (✅ 요구사항: 멀티프로세스 = 1개)
            parsed_rows_all = []
            ok_files = 0
            skip_remark = 0
            skip_badname = 0
            skip_empty = 0
            error_cnt = 0

            # 기존 구조 유지(멀티프로세스)하되, processes=1로 고정
            with Pool(processes=MP_PROCESSES) as pool:
                for path_str, run_id, rows, status in pool.imap_unordered(
                    _parse_one_file_worker, ready_tasks, chunksize=POOL_CHUNKSIZE
                ):
                    if status == "OK":
                        ok_files += 1
                        if rows:
                            parsed_rows_all.extend(rows)

                        # 처리 완료 size 기록(추후 더 큰 파일이 나타나면 재처리 가능)
                        size, _ = _update_stable_state(path_str)
                        if size is not None:
                            processed_run_size[run_id] = int(size)
                        else:
                            processed_run_size[run_id] = processed_run_size.get(run_id, 0)

                    elif status == "SKIP_REMARK":
                        skip_remark += 1
                    elif status == "SKIP_BADNAME":
                        skip_badname += 1
                    elif status == "SKIP_EMPTY":
                        skip_empty += 1
                    else:
                        error_cnt += 1

            # 4) DB 적재(✅ 상시 연결 1개, 실행 중 끊김 시 무한 재시도)
            conn = _ensure_conn()
            attempted = _insert_rows(conn, parsed_rows_all)
            total_attempted_rows += attempted

            print(
                f"[INFO] loop_done | cand_runs={len(cand_map):,} ready_runs={ready_runs:,} "
                f"ok_files={ok_files:,} attempted_rows={attempted:,} total_attempted_rows={total_attempted_rows:,} "
                f"| skipped: not_stable={skipped_not_stable:,}, already_done={skipped_already_done:,} "
                f"| parse_skips: remark={skip_remark:,}, badname={skip_badname:,}, empty={skip_empty:,}, error={error_cnt:,}"
            )

        except KeyboardInterrupt:
            print("[INFO] KeyboardInterrupt. Stop.")
            break

        except psycopg2.Error as e:
            # ✅ 루프 바깥에서 터져도(드물지만) 연결 문제면 무한 재연결 유도
            if _is_connection_error(e):
                print(f"[DB][RETRY] loop-level connection error: {repr(e)}", flush=True)
                _reset_conn("loop-level connection error")
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            print(f"[DB][FATAL] loop-level db error: {repr(e)}", flush=True)
            raise

        except Exception as e:
            print(f"[WARN] loop_error: {repr(e)}", flush=True)

        time_mod.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    freeze_support()
    main()
