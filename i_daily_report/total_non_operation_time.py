# -*- coding: utf-8 -*-
"""
total_non_operation_time_realtime.py

요구사항 반영:
1) SOURCE의 (end_day, station, from_time, to_time)은 항상 유일(한 행)
2) 5초마다 window 범위 전체 재스캔
3) 신규 행 + (reason/sparepart 값이 변한 행)만 다시 UPSERT
4) DB 접속은 기존 connect_blocking 방식 그대로
5) Streamlit에서 나중에 입력한 reason/sparepart가 덮이지 않도록:
   - source 값이 NULL이면 기존값 유지 (COALESCE)
"""

from __future__ import annotations

import os
import time as time_mod
import traceback
import threading
import queue
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from collections import deque
from typing import Optional, Tuple, List, Dict, Any

import psycopg2
from psycopg2.extras import execute_batch


# =========================
# Config
# =========================
SLEEP_SEC = 5
FETCH_MANY_ROWS = 2000
UPSERT_BATCH_ROWS = 200

SRC_SCHEMA = "g_production_film"
SRC_FCT_TABLE = "fct_non_operation_time"
SRC_VISION_TABLE = "vision_non_operation_time"

DST_SCHEMA = "i_daily_report"
DST_TABLE = "total_non_operation_time"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "total_non_operation"

DAY_START = dtime(8, 30, 0)
NIGHT_START = dtime(20, 30, 0)

STATEMENT_TIMEOUT_MS = 30_000
DEFAULT_WORK_MEM = "4MB"

QUEUE_MAX = 20_000

# (source_table,end_day,station,from_ts,to_ts) 단위로 reason/sparepart 변경 감지 캐시
SIG_CACHE_MAX = 300_000  # 필요하면 조정

DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),#보안
}

LOCAL_LOG_DIR = r"C:\AptivAgent\_logs"
LOCAL_LOG_FILE = os.path.join(LOCAL_LOG_DIR, "total_non_operation_time_realtime.log")


# =========================
# Logging
# =========================
def _ensure_local_log_dir():
    try:
        os.makedirs(LOCAL_LOG_DIR, exist_ok=True)
    except Exception:
        pass


def _append_local_log(line: str):
    try:
        _ensure_local_log_dir()
        with open(LOCAL_LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str):
    lvl = (level or "info").lower()
    line = f"{_ts()} [{lvl}] {msg}"
    print(line, flush=True)
    _append_local_log(line)


# =========================
# DB connect (기존 방식 유지)
# =========================
def _work_mem_value() -> str:
    v = os.getenv("PG_WORK_MEM", "").strip()
    return v if v else DEFAULT_WORK_MEM


def connect_blocking(role: str):
    """
    role: 'READ' or 'WRITE'
    - READ: autocommit False
    - WRITE: autocommit True
    """
    assert role in ("READ", "WRITE")
    work_mem = _work_mem_value()

    while True:
        try:
            conn = psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                dbname=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                connect_timeout=10,
                keepalives=1,
                keepalives_idle=30,
                keepalives_interval=10,
                keepalives_count=3,
                application_name=f"total_nonop_{role.lower()}",
            )
            conn.autocommit = (role == "WRITE")

            cur = conn.cursor()
            cur.execute(f"SET work_mem = '{work_mem}';")
            cur.execute(f"SET statement_timeout = '{int(STATEMENT_TIMEOUT_MS)}';")
            cur.close()

            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchall()
            cur.close()

            log("info", f"[DB] connected ({role}) host={DB_CONFIG['host']}:{DB_CONFIG['port']} work_mem={work_mem} stmt_timeout={STATEMENT_TIMEOUT_MS}ms")
            return conn
        except Exception as e:
            log("down", f"[DB][RETRY] connect({role}) failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)


def _close_silent(conn):
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


def _rollback_silent(conn):
    try:
        if conn is not None and not conn.autocommit:
            conn.rollback()
    except Exception:
        pass


# =========================
# DDL ensure
# =========================
def ensure_target_table(write_conn):
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {DST_SCHEMA};

    CREATE TABLE IF NOT EXISTS {DST_SCHEMA}.{DST_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        prod_day TEXT NOT NULL,
        shift_type TEXT NOT NULL,
        station TEXT NOT NULL,
        from_ts TIMESTAMPTZ NOT NULL,
        to_ts   TIMESTAMPTZ NOT NULL,
        reason TEXT,
        sparepart TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        source_table TEXT,
        end_day TEXT
    );

    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS prod_day TEXT;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS shift_type TEXT;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS station TEXT;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS from_ts TIMESTAMPTZ;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS to_ts TIMESTAMPTZ;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS reason TEXT;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS sparepart TEXT;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS source_table TEXT;
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ADD COLUMN IF NOT EXISTS end_day TEXT;

    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ALTER COLUMN created_at SET DEFAULT now();
    ALTER TABLE {DST_SCHEMA}.{DST_TABLE} ALTER COLUMN updated_at SET DEFAULT now();

    CREATE UNIQUE INDEX IF NOT EXISTS ux_total_nonop_pk
    ON {DST_SCHEMA}.{DST_TABLE} (source_table, end_day, station, from_ts);

    CREATE INDEX IF NOT EXISTS idx_total_nonop_query
    ON {DST_SCHEMA}.{DST_TABLE} (prod_day, shift_type, from_ts DESC);

    CREATE OR REPLACE FUNCTION {DST_SCHEMA}.trg_set_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_total_nonop_updated_at') THEN
            CREATE TRIGGER trg_total_nonop_updated_at
            BEFORE UPDATE ON {DST_SCHEMA}.{DST_TABLE}
            FOR EACH ROW
            EXECUTE FUNCTION {DST_SCHEMA}.trg_set_updated_at();
        END IF;
    END$$;
    """
    cur = write_conn.cursor()
    cur.execute(ddl)
    cur.close()


def ensure_progress_table(write_conn):
    """
    기존 테이블이 있어도 컬럼을 ADD COLUMN IF NOT EXISTS로 보정만 한다(DELETE/DROP 없음)
    """
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        log_ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        level TEXT NOT NULL,
        phase TEXT NOT NULL,
        src TEXT,
        message TEXT,
        window_start_ts TIMESTAMPTZ,
        window_end_ts TIMESTAMPTZ,
        fetched_rows INT,
        upsert_rows INT,
        bad_rows INT,
        skipped_rows INT
    );

    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS log_ts TIMESTAMPTZ;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS level TEXT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS phase TEXT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS src TEXT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS message TEXT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS window_start_ts TIMESTAMPTZ;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS window_end_ts TIMESTAMPTZ;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS fetched_rows INT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS upsert_rows INT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS bad_rows INT;
    ALTER TABLE {LOG_SCHEMA}.{LOG_TABLE} ADD COLUMN IF NOT EXISTS skipped_rows INT;

    CREATE INDEX IF NOT EXISTS idx_total_nonop_log_ts
    ON {LOG_SCHEMA}.{LOG_TABLE} (log_ts DESC);
    """
    cur = write_conn.cursor()
    cur.execute(ddl)
    cur.close()


def progress_log(write_conn, level: str, phase: str, src: Optional[str], message: str,
                 win_start: Optional[datetime] = None, win_end: Optional[datetime] = None,
                 fetched_rows: Optional[int] = None, upsert_rows: Optional[int] = None,
                 bad_rows: Optional[int] = None, skipped_rows: Optional[int] = None):
    try:
        ensure_progress_table(write_conn)
        cur = write_conn.cursor()
        cur.execute(
            f"""
            INSERT INTO {LOG_SCHEMA}.{LOG_TABLE} (
                level, phase, src, message,
                window_start_ts, window_end_ts,
                fetched_rows, upsert_rows, bad_rows, skipped_rows
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                (level or "info").lower(),
                phase,
                src,
                message,
                win_start,
                win_end,
                fetched_rows,
                upsert_rows,
                bad_rows,
                skipped_rows,
            ),
        )
        cur.close()
    except Exception as e:
        log("down", f"[LOG][WARN] progress_log failed (ignored) | {type(e).__name__}: {repr(e)}")


# =========================
# Window(day/night)
# =========================
@dataclass
class Window:
    prod_day: str
    shift_type: str
    start_ts: datetime
    end_ts: datetime


def compute_window(now: datetime) -> Window:
    today = now.date()
    dt_day_start = datetime.combine(today, DAY_START)
    dt_night_start = datetime.combine(today, NIGHT_START)

    if now >= dt_night_start:
        return Window(prod_day=today.strftime("%Y%m%d"), shift_type="night", start_ts=dt_night_start, end_ts=now)

    if now >= dt_day_start:
        return Window(prod_day=today.strftime("%Y%m%d"), shift_type="day", start_ts=dt_day_start, end_ts=now)

    y = today - timedelta(days=1)
    return Window(prod_day=y.strftime("%Y%m%d"), shift_type="night", start_ts=datetime.combine(y, NIGHT_START), end_ts=now)


def _window_days(win: Window) -> Tuple[str, str]:
    end_day2 = win.end_ts.strftime("%Y%m%d")
    d1 = win.prod_day
    d2 = end_day2
    if d1 > d2:
        d1, d2 = d2, d1
    return d1, d2


# =========================
# Helpers: time parse
# =========================
def _parse_hms_text(s: Any) -> Optional[dtime]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        if "." in t:
            base, frac = t.split(".", 1)
            dt = datetime.strptime(base, "%H:%M:%S")
            frac_digits = "".join([c for c in frac if c.isdigit()])[:6]
            micro = int(frac_digits.ljust(6, "0")) if frac_digits else 0
            return dt.time().replace(microsecond=micro)
        dt = datetime.strptime(t, "%H:%M:%S")
        return dt.time()
    except Exception:
        return None


def make_from_to_dt(end_day_text: str, from_time_text: Any, to_time_text: Any) -> Tuple[Optional[datetime], Optional[datetime]]:
    ft = _parse_hms_text(from_time_text)
    tt = _parse_hms_text(to_time_text)
    if ft is None or tt is None:
        return None, None
    try:
        d = datetime.strptime(str(end_day_text), "%Y%m%d").date()
    except Exception:
        return None, None

    from_dt = datetime.combine(d, ft)
    to_dt = datetime.combine(d, tt)
    if to_dt <= from_dt:
        to_dt = to_dt + timedelta(days=1)
    return from_dt, to_dt


def _norm_text(x: Any) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    return s if s else None


# =========================
# Change-detection cache (key -> (reason, sparepart))
# =========================
class SigCache:
    def __init__(self, max_size: int):
        self.max_size = int(max_size)
        self.q = deque()
        self.s: Dict[tuple, tuple] = {}

    def upsert_if_changed(self, k: tuple, sig: tuple) -> bool:
        """
        return True if (new key) or (sig changed)
        """
        old = self.s.get(k)
        if old == sig:
            return False
        self.s[k] = sig
        self.q.append(k)
        if len(self.q) > self.max_size:
            oldk = self.q.popleft()
            self.s.pop(oldk, None)
        return True


# =========================
# SQL
# =========================
def make_window_scan_sql(src_table: str) -> str:
    # window overlap은 파이썬에서 판정
    return f"""
    SELECT end_day, station, from_time, to_time, reason, sparepart
    FROM {SRC_SCHEMA}.{src_table}
    WHERE end_day BETWEEN %s AND %s
    ORDER BY end_day ASC, station ASC, from_time ASC
    """


# =========================
# Upsert
# - Streamlit 나중 입력값 보호: source 값이 NULL이면 기존값 유지
# =========================
UPSERT_SQL = f"""
INSERT INTO {DST_SCHEMA}.{DST_TABLE} (
    source_table, end_day, prod_day, shift_type, station, from_ts, to_ts, reason, sparepart
) VALUES (
    %(source_table)s,
    %(end_day)s,
    %(prod_day)s,
    %(shift_type)s,
    %(station)s,
    %(from_ts)s::timestamptz,
    %(to_ts)s::timestamptz,
    %(reason)s,
    %(sparepart)s
)
ON CONFLICT (source_table, end_day, station, from_ts)
DO UPDATE SET
    prod_day   = EXCLUDED.prod_day,
    shift_type = EXCLUDED.shift_type,
    to_ts      = EXCLUDED.to_ts,
    reason     = COALESCE(EXCLUDED.reason, {DST_SCHEMA}.{DST_TABLE}.reason),
    sparepart  = COALESCE(EXCLUDED.sparepart, {DST_SCHEMA}.{DST_TABLE}.sparepart),
    updated_at = now();
"""


def upsert_batch_keepfirst(write_conn, rows: List[dict], src_tag: str, win: Window):
    """
    execute_batch가 UniqueViolation에서 통째로 롤백될 수 있으니:
    - 1) 일단 batch 시도
    - 2) UniqueViolation이면 row-by-row로 degrade 해서
         첫 행만 남기고(이미 있는/중복은 skip) 진행
    """
    if not rows:
        return 0, 0  # upserted, skipped

    while True:
        try:
            cur = write_conn.cursor()
            execute_batch(cur, UPSERT_SQL, rows, page_size=len(rows))
            cur.close()
            return len(rows), 0

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log("down", f"[DB][RETRY] upsert conn lost -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            _close_silent(write_conn)
            time_mod.sleep(SLEEP_SEC)
            write_conn = connect_blocking("WRITE")
            ensure_target_table(write_conn)
            ensure_progress_table(write_conn)

        except psycopg2.errors.UniqueViolation:
            # keep-first: row-by-row로 돌면서 중복은 skip
            try:
                # batch 실패로 트랜잭션 깨졌을 수 있으니 정리
                if not write_conn.autocommit:
                    write_conn.rollback()
            except Exception:
                pass

            up_ok = 0
            skipped = 0
            for r in rows:
                try:
                    c2 = write_conn.cursor()
                    c2.execute(UPSERT_SQL, r)
                    c2.close()
                    up_ok += 1
                except psycopg2.errors.UniqueViolation:
                    skipped += 1
                    try:
                        if not write_conn.autocommit:
                            write_conn.rollback()
                    except Exception:
                        pass
                    continue
                except Exception as ee:
                    # 기타 오류는 재시도 루프로
                    try:
                        if not write_conn.autocommit:
                            write_conn.rollback()
                    except Exception:
                        pass
                    log("down", f"[DB][RETRY] upsert row failed -> retry in {SLEEP_SEC}s | {type(ee).__name__}: {repr(ee)}")
                    time_mod.sleep(SLEEP_SEC)
                    break

            return up_ok, skipped

        except Exception as e:
            log("down", f"[DB][RETRY] upsert failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)


# =========================
# Stream select (READ) - 안정판(기존 유지)
# =========================
def stream_select(read_conn, sql: str, params: Tuple, fetchmany_rows: int):
    while True:
        try:
            try:
                read_conn.rollback()
            except Exception:
                pass

            cur = read_conn.cursor()
            cur.execute(sql, params)

            if cur.description is None:
                raise RuntimeError("cursor.description is None (non-select executed?)")

            cols = [d[0] for d in cur.description]

            while True:
                rows = cur.fetchmany(int(fetchmany_rows))
                if not rows:
                    break
                yield cols, rows

            cur.close()
            read_conn.commit()
            return

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log("down", f"[DB][RETRY] stream_select conn lost -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            _close_silent(read_conn)
            time_mod.sleep(SLEEP_SEC)
            read_conn = connect_blocking("READ")

        except Exception as e:
            pgerr = ""
            try:
                pgerr = getattr(e, "pgerror", "") or ""
            except Exception:
                pass

            try:
                read_conn.rollback()
            except Exception:
                pass

            log("down", f"[DB][RETRY] stream_select failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)} {pgerr}")
            time_mod.sleep(SLEEP_SEC)
            _close_silent(read_conn)
            read_conn = connect_blocking("READ")


# =========================
# Pipeline items
# =========================
@dataclass
class UpsertItem:
    src_tag: str
    win: Window
    rows: List[dict]
    fetched: int
    bad: int
    skipped: int


# =========================
# Reader: 5초마다 window 재스캔
# =========================
def reader_main(q_out: "queue.Queue[UpsertItem]", stop_evt: threading.Event):
    log("info", "[BOOT] reader start")

    read_conn = connect_blocking("READ")
    log_conn = connect_blocking("WRITE")  # progress log 전용

    sig_cache = SigCache(SIG_CACHE_MAX)

    sources = [
        (SRC_FCT_TABLE, "FCT"),
        (SRC_VISION_TABLE, "VISION"),
    ]

    while not stop_evt.is_set():
        now = datetime.now()
        win = compute_window(now)
        d1, d2 = _window_days(win)

        total_fetched = 0
        total_bad = 0
        total_changed = 0

        for (src_table, src_tag) in sources:
            sql = make_window_scan_sql(src_table)
            params = (d1, d2)

            changed_rows: List[dict] = []
            fetched = 0
            bad = 0
            skipped = 0

            for cols, rows in stream_select(read_conn, sql, params, FETCH_MANY_ROWS):
                idx = {c: i for i, c in enumerate(cols)}
                fetched += len(rows)

                for row in rows:
                    end_day = str(row[idx["end_day"]])
                    station = str(row[idx["station"]])

                    from_dt, to_dt = make_from_to_dt(
                        end_day,
                        row[idx["from_time"]],
                        row[idx["to_time"]],
                    )
                    if from_dt is None or to_dt is None:
                        bad += 1
                        continue

                    # window overlap
                    if not (from_dt <= win.end_ts and to_dt >= win.start_ts):
                        continue

                    # 유일키: (end_day, station, from_dt, to_dt) 까지 포함
                    key = (src_table, end_day, station, from_dt, to_dt)

                    reason = _norm_text(row[idx["reason"]])
                    sparepart = _norm_text(row[idx["sparepart"]])
                    sig = (reason, sparepart)

                    if not sig_cache.upsert_if_changed(key, sig):
                        skipped += 1
                        continue

                    changed_rows.append(
                        {
                            "source_table": src_table,
                            "end_day": end_day,
                            "prod_day": win.prod_day,
                            "shift_type": win.shift_type,
                            "station": station,
                            "from_ts": from_dt.strftime("%Y-%m-%d %H:%M:%S.%f+09:00"),
                            "to_ts": to_dt.strftime("%Y-%m-%d %H:%M:%S.%f+09:00"),
                            "reason": reason,
                            "sparepart": sparepart,
                        }
                    )

            total_fetched += fetched
            total_bad += bad
            total_changed += len(changed_rows)

            # 큐에 넣기(배치 쪼개기)
            if changed_rows:
                for i in range(0, len(changed_rows), UPSERT_BATCH_ROWS):
                    chunk = changed_rows[i:i + UPSERT_BATCH_ROWS]
                    q_out.put(UpsertItem(src_tag=src_tag, win=win, rows=chunk, fetched=fetched, bad=bad, skipped=skipped))

            msg = f"scan window={win.shift_type} {win.start_ts:%F %T}~{win.end_ts:%F %T} end_day_range={d1}~{d2} fetched={fetched} changed={len(changed_rows)} bad={bad} skipped={skipped}"
            log("info", f"[SCAN][{src_tag}] {msg}")
            progress_log(log_conn, "info", "SCAN", src_tag, msg, win.start_ts, win.end_ts,
                         fetched_rows=fetched, upsert_rows=len(changed_rows), bad_rows=bad, skipped_rows=skipped)

        time_mod.sleep(SLEEP_SEC)

    _close_silent(read_conn)
    _close_silent(log_conn)


# =========================
# Writer
# =========================
def writer_main(q_in: "queue.Queue[UpsertItem]", stop_evt: threading.Event):
    log("info", "[BOOT] writer start")

    write_conn = connect_blocking("WRITE")
    ensure_target_table(write_conn)
    ensure_progress_table(write_conn)

    while not stop_evt.is_set():
        try:
            item = q_in.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            log("info", f"[UPSERT][{item.src_tag}] batch={len(item.rows)} -> {DST_SCHEMA}.{DST_TABLE}")
            up_ok, skipped = upsert_batch_keepfirst(write_conn, item.rows, item.src_tag, item.win)

            msg = f"upserted={up_ok} skipped_keepfirst={skipped} batch={len(item.rows)}"
            progress_log(write_conn, "info", "UPSERT", item.src_tag, msg,
                         item.win.start_ts, item.win.end_ts,
                         fetched_rows=item.fetched, upsert_rows=up_ok, bad_rows=item.bad, skipped_rows=item.skipped + skipped)

        except Exception as e:
            log("down", f"[UPSERT][ERR] {type(e).__name__}: {repr(e)}")
            progress_log(write_conn, "down", "UPSERT_ERR", item.src_tag, f"{type(e).__name__}: {repr(e)}",
                         item.win.start_ts, item.win.end_ts)

        finally:
            q_in.task_done()

    _close_silent(write_conn)


# =========================
# main
# =========================
def main():
    _ensure_local_log_dir()

    log("info", f"[BOOT] start (READ=1, WRITE=1) interval={SLEEP_SEC}s")
    log("info", f"[CONF] SOURCE=({SRC_SCHEMA}.{SRC_FCT_TABLE}, {SRC_SCHEMA}.{SRC_VISION_TABLE})")
    log("info", f"[CONF] TARGET(i)={DST_SCHEMA}.{DST_TABLE} | LOG(k)={LOG_SCHEMA}.{LOG_TABLE}")
    log("info", f"[CONF] scan=window_rescan_every_{SLEEP_SEC}s | change_detect=(reason,sparepart) per unique (end_day,station,from,to)")

    # 초기 DDL 보장
    w = connect_blocking("WRITE")
    ensure_target_table(w)
    ensure_progress_table(w)
    progress_log(w, "info", "BOOT", None, "daemon booted")
    _close_silent(w)

    q_upsert: "queue.Queue[UpsertItem]" = queue.Queue(maxsize=QUEUE_MAX)
    stop_evt = threading.Event()

    th_r = threading.Thread(target=reader_main, args=(q_upsert, stop_evt), daemon=True)
    th_w = threading.Thread(target=writer_main, args=(q_upsert, stop_evt), daemon=True)
    th_r.start()
    th_w.start()

    try:
        while True:
            time_mod.sleep(1)
    except KeyboardInterrupt:
        log("info", "KeyboardInterrupt -> stopping...")
        stop_evt.set()
        time_mod.sleep(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log("error", f"fatal: {type(e).__name__}: {e}")
        _append_local_log(traceback.format_exc())
        raise