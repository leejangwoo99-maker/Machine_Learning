# -*- coding: utf-8 -*-
"""
c1_fct_detail_loader_factory.py
============================================
FCT Detail TXT Parser -> PostgreSQL 적재 (운영용 최종본)

[유지]
1) mtime 미사용
2) 파일명 끝 토큰 "_HHMMSS(.fff)" = 시작시간으로 해석
3) 파일 완료 판정 = 파일 크기 안정화
4) 자정 전 시작 -> 자정 후 종료 케이스 대응(run_id 기반 + best path 선택)
5) DB 중복 방지: UNIQUE(run_id,test_time,contents) + ON CONFLICT DO NOTHING
6) 루프 기반 실시간 처리
7) 단일 프로세스 연결 1개 유지
8) DB 연결 끊김 시 무한 재연결
9) advisory lock 기반 싱글 인스턴스 보장

[변경]
- DDL 제거
- BASE_DIR 없으면 ERROR 로그 후 즉시 종료
- 로그 테이블/메인 테이블은 이미 존재한다고 가정
"""

import os
import re
import sys
import time as time_mod
from pathlib import Path
from datetime import datetime, timedelta, date, time as dt_time
from multiprocessing import Pool, freeze_support
import urllib.parse
from typing import Dict, Tuple, List, Optional

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# =========================
# psycopg2/libpq 에러 메시지 인코딩 이슈(EXE) 방지
# =========================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")


# =========================
# 0) 설정
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SCHEMA_NAME = "c1_fct_detail"
TABLE_NAME = "fct_detail"

CANDIDATE_WINDOW_SEC = 3600
STABLE_REQUIRED = 3
LOOP_SLEEP_SEC = 5
MP_PROCESSES = 1
POOL_CHUNKSIZE = 10

LINE_RE = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{1,3})\]\s(.+)$")
FNAME_RE = re.compile(r"^(.*)_(\d{8})_(\d{6}(?:\.\d{1,3})?)\.txt$", re.IGNORECASE)

DB_RETRY_INTERVAL_SEC = 5
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

_CONN = None
_LOCK_HELD = False

ADVISORY_LOCK_KEY1 = 91031
ADVISORY_LOCK_KEY2 = 1001

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "c1_log"


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
        "could not receive data from server",
        "could not send data to server",
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


def _acquire_singleton_lock_or_exit(conn, reason: str = "startup"):
    global _LOCK_HELD

    sql = "SELECT pg_try_advisory_lock(%s, %s);"

    try:
        with conn.cursor() as cur:
            cur.execute(sql, (ADVISORY_LOCK_KEY1, ADVISORY_LOCK_KEY2))
            row = cur.fetchone()

        locked = bool(row[0]) if row else False

        if locked:
            _LOCK_HELD = True
            print(
                f"[LOCK][OK] advisory lock acquired "
                f"({ADVISORY_LOCK_KEY1}, {ADVISORY_LOCK_KEY2}) reason={reason}",
                flush=True,
            )
            return

        print(
            f"[LOCK][EXIT] another instance is already running; "
            f"lock=({ADVISORY_LOCK_KEY1}, {ADVISORY_LOCK_KEY2}) reason={reason}",
            flush=True,
        )
        try:
            _safe_close(conn)
        finally:
            sys.exit(0)

    except SystemExit:
        raise
    except Exception as e:
        print(f"[LOCK][ERR] acquire failed: {type(e).__name__}: {repr(e)}", flush=True)
        try:
            _safe_close(conn)
        finally:
            sys.exit(1)


def _release_singleton_lock(conn):
    global _LOCK_HELD

    if not _LOCK_HELD:
        return

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s, %s);", (ADVISORY_LOCK_KEY1, ADVISORY_LOCK_KEY2))
            row = cur.fetchone()
        unlocked = bool(row[0]) if row else False
        print(
            f"[LOCK][REL] advisory lock released={unlocked} "
            f"({ADVISORY_LOCK_KEY1}, {ADVISORY_LOCK_KEY2})",
            flush=True,
        )
    except Exception as e:
        print(f"[LOCK][WARN] release failed: {type(e).__name__}: {repr(e)}", flush=True)
    finally:
        _LOCK_HELD = False


def _psycopg2_conn_blocking(cfg: dict):
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
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def _ensure_conn():
    global _CONN
    global _LOCK_HELD

    if _CONN is None:
        _CONN = _psycopg2_conn_blocking(DB_CONFIG)
        _LOCK_HELD = False
        _acquire_singleton_lock_or_exit(_CONN, reason="initial_connect")
        return _CONN

    if getattr(_CONN, "closed", 1) != 0:
        _CONN = _psycopg2_conn_blocking(DB_CONFIG)
        _LOCK_HELD = False
        _acquire_singleton_lock_or_exit(_CONN, reason="reconnect_closed")
        return _CONN

    try:
        _apply_session_safety(_CONN)
        return _CONN

    except Exception as e:
        if _is_connection_error(e):
            print(f"[DB][WARN] connection unhealthy, will reconnect: {type(e).__name__}: {repr(e)}", flush=True)
            try:
                _safe_close(_CONN)
            except Exception:
                pass

            _CONN = _psycopg2_conn_blocking(DB_CONFIG)
            _LOCK_HELD = False
            _acquire_singleton_lock_or_exit(_CONN, reason="reconnect_after_error")
            return _CONN
        raise


def _now_day_time_str() -> Tuple[str, str]:
    now = datetime.now()
    return now.strftime("%Y%m%d"), now.strftime("%H:%M:%S")


def _to_lower_info(info: str) -> str:
    if info is None:
        return "info"
    return str(info).strip().lower() or "info"


def _db_log(info: str, contents: str):
    day_s, time_s = _now_day_time_str()
    info_s = _to_lower_info(info)
    contents_s = str(contents) if contents is not None else ""

    df = pd.DataFrame(
        [{"end_day": day_s, "end_time": time_s, "info": info_s, "contents": contents_s}],
        columns=["end_day", "end_time", "info", "contents"],
    )

    rows = list(df.itertuples(index=False, name=None))
    if not rows:
        return

    sql = f"""
    INSERT INTO {LOG_SCHEMA}.{LOG_TABLE}
    (end_day, end_time, info, contents)
    VALUES %s
    """

    while True:
        conn = _ensure_conn()
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=1000)
            conn.commit()
            return
        except psycopg2.Error as e:
            try:
                conn.rollback()
            except Exception:
                pass

            if _is_connection_error(e):
                print(f"[DB][RETRY] log insert failed(conn): {type(e).__name__}: {repr(e)}", flush=True)
                try:
                    _safe_close(conn)
                except Exception:
                    pass
                globals()["_CONN"] = None
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue

            print(f"[DB][WARN] log insert db error: {type(e).__name__}: {repr(e)}", flush=True)
            return

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            print(f"[DB][WARN] log insert error: {type(e).__name__}: {repr(e)}", flush=True)
            return


def _log(info: str, contents: str, print_also: bool = True):
    tag = _to_lower_info(info)
    msg = str(contents)

    if print_also:
        print(f"[{tag.upper()}] {msg}", flush=True)

    try:
        _db_log(tag, msg)
    except SystemExit:
        raise
    except Exception as e:
        print(f"[WARN] db_log_failed: {type(e).__name__}: {repr(e)}", flush=True)


def _fail_fast_if_base_dir_missing():
    if BASE_DIR.exists():
        print(f"[INFO] BASE_DIR exists: {BASE_DIR}", flush=True)
        return

    msg = f"BASE_DIR not found. stop. path={BASE_DIR}"
    try:
        _log("error", msg)
    except Exception:
        print(f"[ERROR] {msg}", flush=True)
    raise FileNotFoundError(msg)


def _load_processed_run_sizes() -> Dict[str, int]:
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
            try:
                conn.rollback()
            except Exception:
                pass

            if _is_connection_error(e):
                _log("down", f"load_processed_run_ids conn error -> reconnect: {type(e).__name__}: {repr(e)}")
                try:
                    _safe_close(conn)
                except Exception:
                    pass
                globals()["_CONN"] = None
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue

            _log("error", f"load_processed_run_ids db fatal: {type(e).__name__}: {repr(e)}")
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
FILE_STATE: Dict[str, Tuple[int, int]] = {}


def _update_stable_state(path_str: str) -> Tuple[Optional[int], int]:
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
    start_dt = now_dt - timedelta(seconds=window_sec)

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

            if start_dt <= start_time_dt <= now_dt:
                out.setdefault(run_id, []).append(str(fp))

    return out


def _choose_best_path_for_run(run_id: str, paths: List[str]) -> Optional[str]:
    best_path = None
    best_tuple = None

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
        return None

    return best_path


# =========================
# 4) 파일 파싱(worker)
# =========================
def _parse_one_file_worker(args):
    path_str, run_id, base_day = args
    p = Path(path_str)

    remark = _infer_remark_strict(p)
    if remark is None:
        return path_str, run_id, [], "SKIP_REMARK"

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

    end_time_obj = _round_to_hms(parsed_times[-1])

    first_sec = _parse_time_to_seconds(parsed_times[0])
    last_sec = _parse_time_to_seconds(parsed_times[-1])
    end_day = base_day
    if last_sec < first_sec:
        end_day = base_day + timedelta(days=1)

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
# 5) DB Insert
# =========================
def _insert_rows(rows: List[tuple]) -> int:
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
            try:
                conn.rollback()
            except Exception:
                pass

            if _is_connection_error(e):
                _log("down", f"insert conn error -> reconnect: {type(e).__name__}: {repr(e)}")
                try:
                    _safe_close(conn)
                except Exception:
                    pass
                globals()["_CONN"] = None
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                continue

            _log("error", f"insert db fatal: {type(e).__name__}: {repr(e)}")
            raise

        except Exception as e:
            try:
                conn.rollback()
            except Exception:
                pass
            _log("error", f"insert unexpected error: {type(e).__name__}: {repr(e)}")
            raise


# =========================
# 6) main loop
# =========================
def main():
    print(f"[INFO] Connection String: {_conn_str(DB_CONFIG)}", flush=True)
    print(f"[INFO] BASE_DIR={BASE_DIR}", flush=True)

    _ensure_conn()
    _log("info", f"singleton lock acquired: advisory ({ADVISORY_LOCK_KEY1}, {ADVISORY_LOCK_KEY2})")

    _fail_fast_if_base_dir_missing()

    processed_run_size = _load_processed_run_sizes()
    _log("info", f"processed_run_ids_loaded_from_db={len(processed_run_size):,}")

    _log("info", f"candidate window(sec)={CANDIDATE_WINDOW_SEC:,} (filename start time based)")
    _log("info", f"stable required(count)={STABLE_REQUIRED} (file size unchanged counts)")
    _log("info", f"loop every {LOOP_SLEEP_SEC}s | mp={MP_PROCESSES}, chunksize={POOL_CHUNKSIZE}")
    _log("info", f"session work_mem cap={WORK_MEM}")
    _log("info", f"tcp keepalive={PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}")
    _log("info", "dedup policy: run_id based + db unique(run_id,test_time,contents)")

    total_attempted_rows = 0
    loop_count = 0

    try:
        while True:
            try:
                loop_count += 1
                now_dt = datetime.now()

                cand_map = _collect_candidates(now_dt, CANDIDATE_WINDOW_SEC)
                if not cand_map:
                    _log("sleep", f"loop={loop_count} no candidates; sleep {LOOP_SLEEP_SEC}s")
                    time_mod.sleep(LOOP_SLEEP_SEC)
                    continue

                ready_tasks = []
                ready_runs = 0
                skipped_not_stable = 0
                skipped_already_done = 0

                for run_id, paths in cand_map.items():
                    best_path = _choose_best_path_for_run(run_id, paths)
                    if best_path is None:
                        skipped_not_stable += 1
                        continue

                    size, stable = _update_stable_state(best_path)
                    if size is None or stable < STABLE_REQUIRED:
                        skipped_not_stable += 1
                        continue

                    prev_size = processed_run_size.get(run_id, -1)
                    if prev_size >= 0 and size <= prev_size:
                        skipped_already_done += 1
                        continue

                    info = _file_info_from_filename(Path(best_path))
                    if info is None:
                        continue

                    _, base_day, _, _ = info
                    ready_tasks.append((best_path, run_id, base_day))
                    ready_runs += 1

                if not ready_tasks:
                    _log("sleep", f"loop={loop_count} no ready tasks; sleep {LOOP_SLEEP_SEC}s")
                    time_mod.sleep(LOOP_SLEEP_SEC)
                    continue

                parsed_rows_all = []
                ok_files = 0
                skip_remark = 0
                skip_badname = 0
                skip_empty = 0
                error_cnt = 0

                with Pool(processes=MP_PROCESSES) as pool:
                    for path_str, run_id, rows, status in pool.imap_unordered(
                        _parse_one_file_worker, ready_tasks, chunksize=POOL_CHUNKSIZE
                    ):
                        if status == "OK":
                            ok_files += 1
                            if rows:
                                parsed_rows_all.extend(rows)

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

                attempted = _insert_rows(parsed_rows_all)
                total_attempted_rows += attempted

                summary = (
                    f"loop_done loop={loop_count} | cand_runs={len(cand_map):,} ready_runs={ready_runs:,} "
                    f"ok_files={ok_files:,} attempted_rows={attempted:,} total_attempted_rows={total_attempted_rows:,} "
                    f"| skipped: not_stable={skipped_not_stable:,}, already_done={skipped_already_done:,} "
                    f"| parse_skips: remark={skip_remark:,}, badname={skip_badname:,}, empty={skip_empty:,}, error={error_cnt:,}"
                )
                _log("info", summary)

            except KeyboardInterrupt:
                _log("info", "keyboardinterrupt. stop.")
                break

            except SystemExit:
                raise

            except psycopg2.Error as e:
                if _is_connection_error(e):
                    _log("down", f"loop-level connection error -> reconnect: {repr(e)}")
                    try:
                        _safe_close(globals().get("_CONN"))
                    except Exception:
                        pass
                    globals()["_CONN"] = None
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    continue

                _log("error", f"loop-level db fatal: {repr(e)}")
                raise

            except Exception as e:
                _log("error", f"loop_error: {type(e).__name__}: {repr(e)}")
                raise

            time_mod.sleep(LOOP_SLEEP_SEC)

    finally:
        try:
            if globals().get("_CONN") is not None and getattr(globals().get("_CONN"), "closed", 1) == 0:
                _release_singleton_lock(globals().get("_CONN"))
        except Exception as e:
            print(f"[LOCK][WARN] final release error: {type(e).__name__}: {repr(e)}", flush=True)

        try:
            if globals().get("_CONN") is not None:
                _safe_close(globals().get("_CONN"))
        except Exception:
            pass


if __name__ == "__main__":
    freeze_support()
    main()