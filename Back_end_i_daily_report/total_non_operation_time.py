# -*- coding: utf-8 -*-
"""
total_non_operation_time_realtime.py

최종 구조
1) SOURCE는 created_at / updated_at 기준 "신규 또는 갱신 대상만" 증분 조회
2) 주/야간 window는 조회 범위용이 아니라 "분류용"으로만 사용
3) FCT 원본은 g_production_film.fct_non_operation_time 에서 읽어 i_daily_report.total_non_operation_time 로 적재
4) Vision은 i_daily_report.total_non_operation_time 안의 FCT1~4 행끼리 교집합을 계산해 같은 테이블에 Vision1 / Vision2 로 적재
5) reason / sparepart 는 SOURCE에서 읽지 않음
6) Streamlit이 i_daily_report.total_non_operation_time 에 직접 입력/수정
7) UPSERT 시 기존 reason / sparepart 는 절대 덮어쓰지 않음
8) WRITE(UPSERT) / WRITE(LOG) 분리
9) Heartbeat 1분 1회, DB 로그 최소화
10) 워터마크는 DB state table + 메모리 공유로 관리
11) 재시작 시 최근 row가 아닌 "최근 row 이전 행의 to_ts timestamp" 기준으로 다시 읽음

주/야간 분류 규칙
- 주간 : [D-DAY] 08:30:00 ~ 20:29:59
- 야간 : [D-DAY] 20:30:00 ~ [D+1] 08:29:59
- row가 주/야간에 걸치면 더 많이 겹친 shift에 귀속
- 겹치는 시간이 동일하면 day
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
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values


# =========================
# Config
# =========================
FCT_SLEEP_SEC = 5
VISION_SLEEP_SEC = 6

FETCH_MANY_ROWS = 2000
UPSERT_BATCH_ROWS = 300

SRC_SCHEMA = "g_production_film"
SRC_FCT_TABLE = "fct_non_operation_time"

DST_SCHEMA = "i_daily_report"
DST_TABLE = "total_non_operation_time"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "total_non_operation"
STATE_TABLE = "total_non_operation_state"

FCT_THRESH_SCHEMA = "e1_FCT_ct"
FCT_THRESH_TABLE = "fct_op_ct"
FCT_THRESH_COL = "q3"
FCT_THRESH_FALLBACK_SEC = 44.0

DAY_START = dtime(8, 30, 0)
NIGHT_START = dtime(20, 30, 0)
KST = ZoneInfo("Asia/Seoul")

STATEMENT_TIMEOUT_MS = 30_000
DEFAULT_WORK_MEM = "16MB"

QUEUE_MAX = 20_000
SIG_CACHE_MAX = 300_000
LOG_QUEUE_MAX = 10_000

LOOKBACK_SEC = 3
MANUAL_FORCE_VALUE = "o"

FCT_STATE_KEY = SRC_FCT_TABLE
VISION_STATE_KEY = "VISION_OVERLAP"
VISION_SOURCE_TABLE = "vision_overlap_from_total_non_operation_time"

VISION_PAIR_MAP = {
    "FCT1": ("FCT2", "Vision1"),
    "FCT2": ("FCT1", "Vision1"),
    "FCT3": ("FCT4", "Vision2"),
    "FCT4": ("FCT3", "Vision2"),
}

DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
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
# SQL identifier quote
# =========================
def qi(name: str) -> str:
    return '"' + str(name).replace('"', '""') + '"'


# =========================
# DB connect
# =========================
def _work_mem_value() -> str:
    v = os.getenv("PG_WORK_MEM", "").strip()
    return v if v else DEFAULT_WORK_MEM


def connect_blocking(role: str):
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

            log(
                "info",
                f"[DB] connected ({role}) host={DB_CONFIG['host']}:{DB_CONFIG['port']} "
                f"work_mem={work_mem} stmt_timeout={STATEMENT_TIMEOUT_MS}ms"
            )
            return conn

        except Exception as e:
            log("down", f"[DB][RETRY] connect({role}) failed -> retry in 5s | {type(e).__name__}: {repr(e)}")
            time_mod.sleep(5)


def ensure_connection(conn, role: str):
    try:
        if conn is None or getattr(conn, "closed", 1) != 0:
            return connect_blocking(role)

        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchall()
        cur.close()
        return conn
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        return connect_blocking(role)


def _close_silent(conn):
    try:
        if conn is not None:
            conn.close()
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

    UPDATE {DST_SCHEMA}.{DST_TABLE}
       SET created_at = COALESCE(created_at, now()),
           updated_at = COALESCE(updated_at, created_at, now())
     WHERE created_at IS NULL
        OR updated_at IS NULL;

    CREATE UNIQUE INDEX IF NOT EXISTS ux_total_nonop_pk
    ON {DST_SCHEMA}.{DST_TABLE} (source_table, end_day, station, from_ts);

    CREATE INDEX IF NOT EXISTS idx_total_nonop_query
    ON {DST_SCHEMA}.{DST_TABLE} (prod_day, shift_type, from_ts DESC);

    CREATE INDEX IF NOT EXISTS idx_total_nonop_station_time
    ON {DST_SCHEMA}.{DST_TABLE} (station, from_ts, to_ts);

    CREATE INDEX IF NOT EXISTS idx_total_nonop_updated
    ON {DST_SCHEMA}.{DST_TABLE} (updated_at DESC);

    CREATE INDEX IF NOT EXISTS idx_total_nonop_created
    ON {DST_SCHEMA}.{DST_TABLE} (created_at DESC);

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


def ensure_state_table(write_conn):
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{STATE_TABLE} (
        src_table TEXT PRIMARY KEY,
        last_created_at TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    ALTER TABLE {LOG_SCHEMA}.{STATE_TABLE} ADD COLUMN IF NOT EXISTS src_table TEXT;
    ALTER TABLE {LOG_SCHEMA}.{STATE_TABLE} ADD COLUMN IF NOT EXISTS last_created_at TIMESTAMPTZ;
    ALTER TABLE {LOG_SCHEMA}.{STATE_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;

    CREATE OR REPLACE FUNCTION {LOG_SCHEMA}.trg_set_nonop_state_updated_at()
    RETURNS TRIGGER AS $$
    BEGIN
        NEW.updated_at = now();
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;

    DO $$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = 'trg_total_nonop_state_updated_at') THEN
            CREATE TRIGGER trg_total_nonop_state_updated_at
            BEFORE UPDATE ON {LOG_SCHEMA}.{STATE_TABLE}
            FOR EACH ROW
            EXECUTE FUNCTION {LOG_SCHEMA}.trg_set_nonop_state_updated_at();
        END IF;
    END$$;
    """
    cur = write_conn.cursor()
    cur.execute(ddl)
    cur.close()


# =========================
# Async DB log worker
# =========================
@dataclass
class LogItem:
    level: str
    phase: str
    src: Optional[str]
    message: str
    win_start: Optional[datetime] = None
    win_end: Optional[datetime] = None
    fetched_rows: Optional[int] = None
    upsert_rows: Optional[int] = None
    bad_rows: Optional[int] = None
    skipped_rows: Optional[int] = None


class AsyncDbLogger:
    def __init__(self, q_out: "queue.Queue[LogItem]"):
        self.q_out = q_out

    def write(
        self,
        level: str,
        phase: str,
        src: Optional[str],
        message: str,
        win_start: Optional[datetime] = None,
        win_end: Optional[datetime] = None,
        fetched_rows: Optional[int] = None,
        upsert_rows: Optional[int] = None,
        bad_rows: Optional[int] = None,
        skipped_rows: Optional[int] = None,
    ):
        item = LogItem(
            level=(level or "info").lower(),
            phase=phase,
            src=src,
            message=message,
            win_start=win_start,
            win_end=win_end,
            fetched_rows=fetched_rows,
            upsert_rows=upsert_rows,
            bad_rows=bad_rows,
            skipped_rows=skipped_rows,
        )
        try:
            self.q_out.put_nowait(item)
        except queue.Full:
            log("warn", f"[LOG][DROP] queue full phase={phase} src={src}")


def log_worker_main(q_log: "queue.Queue[LogItem]", stop_evt: threading.Event):
    log("info", "[BOOT] log_worker start")

    write_conn = connect_blocking("WRITE")
    ensure_progress_table(write_conn)

    while not stop_evt.is_set():
        try:
            item = q_log.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            while True:
                try:
                    write_conn = ensure_connection(write_conn, "WRITE")
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
                            item.level,
                            item.phase,
                            item.src,
                            item.message,
                            item.win_start,
                            item.win_end,
                            item.fetched_rows,
                            item.upsert_rows,
                            item.bad_rows,
                            item.skipped_rows,
                        ),
                    )
                    cur.close()
                    break
                except Exception as e:
                    log("down", f"[LOG][RETRY] insert failed -> reconnect | {type(e).__name__}: {repr(e)}")
                    _close_silent(write_conn)
                    write_conn = connect_blocking("WRITE")
                    ensure_progress_table(write_conn)
        finally:
            q_log.task_done()

    _close_silent(write_conn)


# =========================
# Window(day/night) - 분류용
# =========================
@dataclass
class Window:
    prod_day: str
    shift_type: str
    start_ts: datetime
    end_ts: datetime


def compute_window(now: datetime) -> Window:
    now = now.astimezone(KST) if now.tzinfo else now.replace(tzinfo=KST)
    today = now.date()
    dt_day_start = datetime.combine(today, DAY_START, tzinfo=KST)
    dt_night_start = datetime.combine(today, NIGHT_START, tzinfo=KST)

    if now >= dt_night_start:
        return Window(
            prod_day=today.strftime("%Y%m%d"),
            shift_type="night",
            start_ts=dt_night_start,
            end_ts=now,
        )

    if now >= dt_day_start:
        return Window(
            prod_day=today.strftime("%Y%m%d"),
            shift_type="day",
            start_ts=dt_day_start,
            end_ts=now,
        )

    y = today - timedelta(days=1)
    return Window(
        prod_day=y.strftime("%Y%m%d"),
        shift_type="night",
        start_ts=datetime.combine(y, NIGHT_START, tzinfo=KST),
        end_ts=now,
    )


# =========================
# Helpers
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
        return datetime.strptime(t, "%H:%M:%S").time()
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

    from_dt = datetime.combine(d, ft, tzinfo=KST)
    to_dt = datetime.combine(d, tt, tzinfo=KST)
    if to_dt <= from_dt:
        to_dt += timedelta(days=1)
    return from_dt, to_dt


def _to_kst_aware(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def _month_key(dt: datetime) -> str:
    dt = _to_kst_aware(dt)
    return dt.strftime("%Y%m")


def _overlap_seconds(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> float:
    s = max(a_start, b_start)
    e = min(a_end, b_end)
    if e <= s:
        return 0.0
    return (e - s).total_seconds()


def classify_shift_by_dt(from_dt: datetime, to_dt: datetime) -> Tuple[Optional[str], Optional[str]]:
    if from_dt is None or to_dt is None:
        return None, None

    from_dt = _to_kst_aware(from_dt)
    to_dt = _to_kst_aware(to_dt)

    if to_dt <= from_dt:
        return None, None

    tz = from_dt.tzinfo
    start_date = from_dt.date() - timedelta(days=1)
    end_date = to_dt.date() + timedelta(days=1)

    best_overlap = -1.0
    best_prod_day = None
    best_shift_type = None

    cur = start_date
    while cur <= end_date:
        day_s = datetime.combine(cur, DAY_START, tzinfo=tz)
        day_e = datetime.combine(cur, NIGHT_START, tzinfo=tz)
        day_sec = _overlap_seconds(from_dt, to_dt, day_s, day_e)
        if day_sec > 0:
            if (day_sec > best_overlap) or (day_sec == best_overlap and best_shift_type != "day"):
                best_overlap = day_sec
                best_prod_day = cur.strftime("%Y%m%d")
                best_shift_type = "day"

        night_s = datetime.combine(cur, NIGHT_START, tzinfo=tz)
        night_e = datetime.combine(cur + timedelta(days=1), DAY_START, tzinfo=tz)
        night_sec = _overlap_seconds(from_dt, to_dt, night_s, night_e)
        if night_sec > 0:
            if (night_sec > best_overlap) or (night_sec == best_overlap and best_shift_type != "day"):
                best_overlap = night_sec
                best_prod_day = cur.strftime("%Y%m%d")
                best_shift_type = "night"

        cur += timedelta(days=1)

    return best_prod_day, best_shift_type


def classify_shift_by_overlap(end_day_text: str, from_time_text: Any, to_time_text: Any) -> Tuple[Optional[str], Optional[str], Optional[datetime], Optional[datetime]]:
    from_dt, to_dt = make_from_to_dt(end_day_text, from_time_text, to_time_text)
    if from_dt is None or to_dt is None:
        return None, None, None, None
    prod_day, shift_type = classify_shift_by_dt(from_dt, to_dt)
    return prod_day, shift_type, from_dt, to_dt


def second_largest_or_none(values: List[datetime]) -> Optional[datetime]:
    uniq = sorted(set(values))
    if len(uniq) >= 2:
        return uniq[-2]
    return None


# =========================
# Monthly threshold cache
# =========================
class MonthlyThresholdStore:
    def __init__(self, schema_name: str, table_name: str, col_name: str, fallback_sec: float):
        self.schema_name = schema_name
        self.table_name = table_name
        self.col_name = col_name
        self.fallback_sec = float(fallback_sec)
        self._lock = threading.Lock()
        self._month: Optional[str] = None
        self._value: float = float(fallback_sec)

    def refresh_if_needed(self, read_conn, now: datetime) -> Tuple[Any, str, float, bool]:
        want_month = _month_key(now)

        with self._lock:
            if self._month == want_month:
                return read_conn, self._month, float(self._value), False

        read_conn, val = fetch_monthly_threshold(
            read_conn=read_conn,
            schema_name=self.schema_name,
            table_name=self.table_name,
            col_name=self.col_name,
            fallback_sec=self.fallback_sec,
        )

        with self._lock:
            self._month = want_month
            self._value = float(val)
            return read_conn, self._month, float(self._value), True


def fetch_monthly_threshold(read_conn, schema_name: str, table_name: str, col_name: str, fallback_sec: float) -> Tuple[Any, float]:
    sql = f"""
    SELECT COALESCE(MAX({qi(col_name)}), %s)::float8 AS threshold_sec
    FROM {qi(schema_name)}.{qi(table_name)}
    """
    read_conn, cols, rows = fetch_all_rows_persistent(read_conn, sql, (fallback_sec,))
    if not rows:
        return read_conn, float(fallback_sec)

    idx = {c: i for i, c in enumerate(cols)}
    try:
        v = rows[0][idx["threshold_sec"]]
        if v is None:
            return read_conn, float(fallback_sec)
        return read_conn, float(v)
    except Exception:
        return read_conn, float(fallback_sec)


# =========================
# Sig cache
# =========================
class SigCache:
    def __init__(self, max_size: int):
        self.max_size = int(max_size)
        self.q = deque()
        self.s: Dict[tuple, tuple] = {}

    def upsert_if_changed(self, k: tuple, sig: tuple) -> bool:
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
# Shared watermark store
# =========================
class WatermarkStore:
    def __init__(self):
        self._lock = threading.Lock()
        self._m: Dict[str, datetime] = {}

    def load_from_db(self, write_conn):
        ensure_state_table(write_conn)
        cur = write_conn.cursor()
        cur.execute(f"SELECT src_table, last_created_at FROM {LOG_SCHEMA}.{STATE_TABLE}")
        rows = cur.fetchall()
        cur.close()

        with self._lock:
            self._m = {}
            for src_table, last_created_at in rows:
                if src_table and last_created_at is not None:
                    self._m[str(src_table)] = _to_kst_aware(last_created_at)

    def get(self, src_table: str) -> Optional[datetime]:
        with self._lock:
            return self._m.get(src_table)

    def set(self, src_table: str, last_created_at: Optional[datetime]):
        if last_created_at is None:
            return
        last_created_at = _to_kst_aware(last_created_at)
        with self._lock:
            self._m[src_table] = last_created_at


def persist_watermark(write_conn, src_table: str, last_created_at: datetime):
    last_created_at = _to_kst_aware(last_created_at)
    while True:
        try:
            write_conn = ensure_connection(write_conn, "WRITE")
            cur = write_conn.cursor()
            cur.execute(
                f"""
                INSERT INTO {LOG_SCHEMA}.{STATE_TABLE} (src_table, last_created_at)
                VALUES (%s, %s)
                ON CONFLICT (src_table)
                DO UPDATE SET
                    last_created_at = EXCLUDED.last_created_at,
                    updated_at = now()
                """,
                (src_table, last_created_at),
            )
            cur.close()
            return write_conn
        except Exception as e:
            log("down", f"[DB][RETRY] persist_watermark failed -> reconnect | {type(e).__name__}: {repr(e)}")
            _close_silent(write_conn)
            write_conn = connect_blocking("WRITE")
            ensure_state_table(write_conn)


# =========================
# SQL
# =========================
def make_incremental_scan_sql(src_table: str) -> str:
    return f"""
    SELECT
        end_day,
        station,
        from_time,
        to_time,
        created_at,
        COALESCE(updated_at, created_at) AS changed_ts,
        manual
    FROM {SRC_SCHEMA}.{src_table}
    WHERE COALESCE(updated_at, created_at) >= %s
      AND (
            COALESCE(no_operation_time, 0) > %s
            OR manual = %s
          )
    ORDER BY COALESCE(updated_at, created_at) ASC, end_day ASC, station ASC, from_time ASC, to_time ASC
    """


def make_incremental_scan_params(query_from: datetime, threshold_sec: float) -> Tuple:
    return (query_from, threshold_sec, MANUAL_FORCE_VALUE)


VISION_CHANGED_SQL = f"""
SELECT
    station,
    from_ts,
    to_ts,
    COALESCE(updated_at, created_at) AS changed_ts
FROM {DST_SCHEMA}.{DST_TABLE}
WHERE COALESCE(updated_at, created_at) >= %s
  AND station IN ('FCT1', 'FCT2', 'FCT3', 'FCT4')
ORDER BY COALESCE(updated_at, created_at) ASC, station ASC, from_ts ASC, to_ts ASC
"""

VISION_PARTNER_SQL = f"""
SELECT
    station,
    from_ts,
    to_ts,
    COALESCE(updated_at, created_at) AS changed_ts
FROM {DST_SCHEMA}.{DST_TABLE}
WHERE station = %s
  AND to_ts > %s
  AND from_ts < %s
ORDER BY from_ts ASC, to_ts ASC
"""


# =========================
# Dedup helper
# =========================
def dedup_upsert_rows(rows: List[dict]) -> List[dict]:
    best: Dict[tuple, dict] = {}
    for r in rows:
        k = (r["source_table"], r["end_day"], r["station"], r["from_ts"])
        prev = best.get(k)
        if prev is None:
            best[k] = r
            continue
        if str(r["to_ts"]) >= str(prev["to_ts"]):
            best[k] = r
    return list(best.values())


# =========================
# Upsert
# =========================
def upsert_batch_keepfirst(write_conn, rows: List[dict]):
    rows = dedup_upsert_rows(rows)
    if not rows:
        return write_conn, 0, 0

    values = [
        (
            r["source_table"],
            r["end_day"],
            r["prod_day"],
            r["shift_type"],
            r["station"],
            r["from_ts"],
            r["to_ts"],
        )
        for r in rows
    ]

    sql = f"""
    INSERT INTO {DST_SCHEMA}.{DST_TABLE} (
        source_table,
        end_day,
        prod_day,
        shift_type,
        station,
        from_ts,
        to_ts,
        reason,
        sparepart
    )
    SELECT
        v.source_table,
        v.end_day,
        v.prod_day,
        v.shift_type,
        v.station,
        v.from_ts::timestamptz,
        v.to_ts::timestamptz,
        NULL,
        NULL
    FROM (
        VALUES %s
    ) AS v (
        source_table,
        end_day,
        prod_day,
        shift_type,
        station,
        from_ts,
        to_ts
    )
    WHERE NOT EXISTS (
        SELECT 1
        FROM {DST_SCHEMA}.{DST_TABLE} t
        WHERE t.prod_day = v.prod_day
          AND t.shift_type = v.shift_type
          AND t.station = v.station
          AND t.from_ts = v.from_ts::timestamptz
          AND t.to_ts = v.to_ts::timestamptz
    )
    ON CONFLICT (source_table, end_day, station, from_ts)
    DO UPDATE SET
        prod_day   = EXCLUDED.prod_day,
        shift_type = EXCLUDED.shift_type,
        to_ts      = EXCLUDED.to_ts
    WHERE {DST_SCHEMA}.{DST_TABLE}.prod_day   IS DISTINCT FROM EXCLUDED.prod_day
       OR {DST_SCHEMA}.{DST_TABLE}.shift_type IS DISTINCT FROM EXCLUDED.shift_type
       OR {DST_SCHEMA}.{DST_TABLE}.to_ts      IS DISTINCT FROM EXCLUDED.to_ts
    """

    while True:
        try:
            write_conn = ensure_connection(write_conn, "WRITE")
            cur = write_conn.cursor()
            execute_values(
                cur,
                sql,
                values,
                template="(%s, %s, %s, %s, %s, %s, %s)",
                page_size=len(values),
            )
            affected = cur.rowcount if cur.rowcount is not None else 0
            cur.close()
            skipped = max(0, len(rows) - affected)
            return write_conn, affected, skipped
        except Exception as e:
            log("down", f"[DB][RETRY] upsert failed -> reconnect | {type(e).__name__}: {repr(e)}")
            try:
                if write_conn is not None and not write_conn.autocommit:
                    write_conn.rollback()
            except Exception:
                pass
            _close_silent(write_conn)
            write_conn = connect_blocking("WRITE")
            ensure_target_table(write_conn)


# =========================
# DB reads using persistent conn
# =========================
def stream_select_persistent(read_conn, sql: str, params: Tuple, fetchmany_rows: int):
    while True:
        try:
            read_conn = ensure_connection(read_conn, "READ")
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
                yield read_conn, cols, rows

            cur.close()
            read_conn.commit()
            return
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
            log("down", f"[DB][RETRY] stream_select failed -> reconnect | {type(e).__name__}: {repr(e)} {pgerr}")
            _close_silent(read_conn)
            read_conn = connect_blocking("READ")


def fetch_all_rows_persistent(read_conn, sql: str, params: Tuple) -> Tuple[Any, List[str], List[tuple]]:
    while True:
        try:
            read_conn = ensure_connection(read_conn, "READ")
            try:
                read_conn.rollback()
            except Exception:
                pass

            cur = read_conn.cursor()
            cur.execute(sql, params)

            if cur.description is None:
                raise RuntimeError("cursor.description is None (non-select executed?)")

            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            cur.close()
            read_conn.commit()
            return read_conn, cols, rows
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
            log("down", f"[DB][RETRY] fetch_all_rows failed -> reconnect | {type(e).__name__}: {repr(e)} {pgerr}")
            _close_silent(read_conn)
            read_conn = connect_blocking("READ")


# =========================
# Pipeline items
# =========================
@dataclass
class UpsertItem:
    src_table: str
    src_tag: str
    rows: List[dict]
    fetched: int
    bad: int
    skipped: int
    restart_watermark_ts: Optional[datetime]
    win_start: Optional[datetime]
    win_end: Optional[datetime]


# =========================
# Reader helpers
# =========================
def _initial_watermark(now: datetime) -> datetime:
    now = _to_kst_aware(now)
    win = compute_window(now)
    return win.start_ts - timedelta(seconds=LOOKBACK_SEC)


def _restart_watermark_from_to_ts_list(to_ts_list: List[datetime], old_watermark: Optional[datetime]) -> Optional[datetime]:
    if not to_ts_list:
        return old_watermark
    second_latest = second_largest_or_none(to_ts_list)
    if second_latest is not None:
        return second_latest
    return old_watermark


def scan_fct_source_once(
    read_conn,
    wm_store: WatermarkStore,
    sig_cache: SigCache,
    fct_threshold_store: MonthlyThresholdStore,
    now: datetime,
):
    read_conn, month_key, threshold_sec, refreshed = fct_threshold_store.refresh_if_needed(read_conn, now)
    if refreshed:
        log("info", f"[THRESHOLD][FCT] month={month_key} threshold_sec={threshold_sec}")

    last_wm = wm_store.get(FCT_STATE_KEY)
    if last_wm is None:
        last_wm = _initial_watermark(now)

    query_from = last_wm - timedelta(seconds=LOOKBACK_SEC)
    sql = make_incremental_scan_sql(SRC_FCT_TABLE)
    params = make_incremental_scan_params(query_from, threshold_sec)

    changed_rows: List[dict] = []
    fetched = 0
    bad = 0
    skipped = 0
    to_ts_seen: List[datetime] = []
    win_start_for_log: Optional[datetime] = None
    win_end_for_log: Optional[datetime] = None

    for read_conn, cols, rows in stream_select_persistent(read_conn, sql, params, FETCH_MANY_ROWS):
        idx = {c: i for i, c in enumerate(cols)}
        fetched += len(rows)

        for row in rows:
            end_day = str(row[idx["end_day"]])
            station = str(row[idx["station"]])

            prod_day, shift_type, from_dt, to_dt = classify_shift_by_overlap(
                end_day,
                row[idx["from_time"]],
                row[idx["to_time"]],
            )
            if from_dt is None or to_dt is None or prod_day is None or shift_type is None:
                bad += 1
                continue

            key = (SRC_FCT_TABLE, end_day, station, from_dt, to_dt)
            sig = (_to_kst_aware(row[idx["changed_ts"]]),)

            if not sig_cache.upsert_if_changed(key, sig):
                skipped += 1
                continue

            to_ts_seen.append(to_dt)

            if win_start_for_log is None or from_dt < win_start_for_log:
                win_start_for_log = from_dt
            if win_end_for_log is None or to_dt > win_end_for_log:
                win_end_for_log = to_dt

            changed_rows.append(
                {
                    "source_table": SRC_FCT_TABLE,
                    "end_day": end_day,
                    "prod_day": prod_day,
                    "shift_type": shift_type,
                    "station": station,
                    "from_ts": from_dt.isoformat(),
                    "to_ts": to_dt.isoformat(),
                }
            )

    restart_wm_ts = _restart_watermark_from_to_ts_list(to_ts_seen, last_wm)

    return read_conn, {
        "src_table": FCT_STATE_KEY,
        "src_tag": "FCT",
        "rows": changed_rows,
        "fetched": fetched,
        "bad": bad,
        "skipped": skipped,
        "restart_watermark_ts": restart_wm_ts,
        "win_start": win_start_for_log,
        "win_end": win_end_for_log,
        "threshold_sec": threshold_sec,
        "threshold_month": month_key,
    }


def _vision_overlap_rows_for_changed(
    read_conn,
    changed_station: str,
    changed_from_ts: datetime,
    changed_to_ts: datetime,
    changed_updated_at: datetime,
    sig_cache: SigCache,
):
    changed_from_ts = _to_kst_aware(changed_from_ts)
    changed_to_ts = _to_kst_aware(changed_to_ts)
    changed_updated_at = _to_kst_aware(changed_updated_at)

    partner_station, vision_station = VISION_PAIR_MAP[changed_station]
    read_conn, cols, partner_rows = fetch_all_rows_persistent(
        read_conn,
        VISION_PARTNER_SQL,
        (partner_station, changed_from_ts, changed_to_ts),
    )
    idx = {c: i for i, c in enumerate(cols)}

    out_rows: List[dict] = []
    bad = 0
    skipped = 0
    win_start = None
    win_end = None
    to_ts_out: List[datetime] = []

    for prow in partner_rows:
        partner_from = _to_kst_aware(prow[idx["from_ts"]])
        partner_to = _to_kst_aware(prow[idx["to_ts"]])
        partner_updated_at = _to_kst_aware(prow[idx["changed_ts"]])

        overlap_from = max(changed_from_ts, partner_from)
        overlap_to = min(changed_to_ts, partner_to)
        if overlap_to <= overlap_from:
            continue

        prod_day, shift_type = classify_shift_by_dt(overlap_from, overlap_to)
        if prod_day is None or shift_type is None:
            bad += 1
            continue

        end_day = overlap_from.strftime("%Y%m%d")
        sig_key = (VISION_SOURCE_TABLE, vision_station, overlap_from, overlap_to)
        sig_val = (changed_updated_at, partner_updated_at)

        if not sig_cache.upsert_if_changed(sig_key, sig_val):
            skipped += 1
            continue

        to_ts_out.append(overlap_to)

        if win_start is None or overlap_from < win_start:
            win_start = overlap_from
        if win_end is None or overlap_to > win_end:
            win_end = overlap_to

        out_rows.append(
            {
                "source_table": VISION_SOURCE_TABLE,
                "end_day": end_day,
                "prod_day": prod_day,
                "shift_type": shift_type,
                "station": vision_station,
                "from_ts": overlap_from.isoformat(),
                "to_ts": overlap_to.isoformat(),
            }
        )

    return read_conn, out_rows, bad, skipped, win_start, win_end, to_ts_out


def scan_vision_overlap_once(
    read_conn,
    wm_store: WatermarkStore,
    sig_cache: SigCache,
    now: datetime,
):
    last_wm = wm_store.get(VISION_STATE_KEY)
    if last_wm is None:
        last_wm = _initial_watermark(now)

    query_from = last_wm - timedelta(seconds=LOOKBACK_SEC)
    read_conn, changed_cols, changed_rows = fetch_all_rows_persistent(read_conn, VISION_CHANGED_SQL, (query_from,))
    changed_idx = {c: i for i, c in enumerate(changed_cols)}

    out_rows: List[dict] = []
    fetched = len(changed_rows)
    bad = 0
    skipped = 0
    to_ts_seen: List[datetime] = []
    win_start_for_log: Optional[datetime] = None
    win_end_for_log: Optional[datetime] = None

    for crow in changed_rows:
        station = str(crow[changed_idx["station"]])
        from_ts = _to_kst_aware(crow[changed_idx["from_ts"]])
        to_ts = _to_kst_aware(crow[changed_idx["to_ts"]])
        changed_ts = _to_kst_aware(crow[changed_idx["changed_ts"]])

        if station not in VISION_PAIR_MAP:
            continue

        read_conn, rows_one, bad_one, skipped_one, win_start_one, win_end_one, to_ts_one = _vision_overlap_rows_for_changed(
            read_conn=read_conn,
            changed_station=station,
            changed_from_ts=from_ts,
            changed_to_ts=to_ts,
            changed_updated_at=changed_ts,
            sig_cache=sig_cache,
        )

        out_rows.extend(rows_one)
        bad += bad_one
        skipped += skipped_one
        to_ts_seen.extend(to_ts_one)

        if win_start_one is not None and (win_start_for_log is None or win_start_one < win_start_for_log):
            win_start_for_log = win_start_one
        if win_end_one is not None and (win_end_for_log is None or win_end_one > win_end_for_log):
            win_end_for_log = win_end_one

    restart_wm_ts = _restart_watermark_from_to_ts_list(to_ts_seen, last_wm)

    return read_conn, {
        "src_table": VISION_STATE_KEY,
        "src_tag": "VISION",
        "rows": dedup_upsert_rows(out_rows),
        "fetched": fetched,
        "bad": bad,
        "skipped": skipped,
        "restart_watermark_ts": restart_wm_ts,
        "win_start": win_start_for_log,
        "win_end": win_end_for_log,
    }


# =========================
# Readers
# =========================
def fct_reader_main(
    q_out: "queue.Queue[UpsertItem]",
    q_log: "queue.Queue[LogItem]",
    stop_evt: threading.Event,
    wm_store: WatermarkStore,
    fct_threshold_store: MonthlyThresholdStore,
):
    logger = AsyncDbLogger(q_log)
    sig_cache = SigCache(SIG_CACHE_MAX)
    read_conn = connect_blocking("READ")

    heartbeat_last = 0.0
    scan_log_last = 0.0

    log("info", "[BOOT] fct_reader start")

    while not stop_evt.is_set():
        now_ts = time_mod.time()
        now = datetime.now(KST)
        active_win = compute_window(now)

        if now_ts - heartbeat_last >= 60:
            heartbeat_last = now_ts
            hb_msg = f"fct_reader alive queue_size={q_out.qsize()} active={active_win.prod_day}/{active_win.shift_type}"
            log("info", f"[HEARTBEAT] {hb_msg}")
            logger.write("info", "HEARTBEAT", "FCT_READER", hb_msg, active_win.start_ts, active_win.end_ts)

        try:
            read_conn, result = scan_fct_source_once(
                read_conn=read_conn,
                wm_store=wm_store,
                sig_cache=sig_cache,
                fct_threshold_store=fct_threshold_store,
                now=now,
            )

            rows = result["rows"]
            if rows:
                for i in range(0, len(rows), UPSERT_BATCH_ROWS):
                    chunk = rows[i:i + UPSERT_BATCH_ROWS]
                    q_out.put(
                        UpsertItem(
                            src_table=result["src_table"],
                            src_tag=result["src_tag"],
                            rows=chunk,
                            fetched=result["fetched"],
                            bad=result["bad"],
                            skipped=result["skipped"],
                            restart_watermark_ts=result["restart_watermark_ts"],
                            win_start=result["win_start"],
                            win_end=result["win_end"],
                        )
                    )

            if len(rows) > 0 or (now_ts - scan_log_last >= 60):
                scan_log_last = now_ts
                total_msg = (
                    f"scan src=FCT fetched={result['fetched']} changed={len(rows)} "
                    f"bad={result['bad']} skipped={result['skipped']} "
                    f"threshold_sec={result['threshold_sec']} "
                    f"threshold_month={result['threshold_month']} "
                    f"restart_wm={result['restart_watermark_ts']} "
                    f"queue_size={q_out.qsize()}"
                )
                log("info", f"[SCAN] {total_msg}")
                logger.write(
                    "info",
                    "SCAN_SRC",
                    "FCT",
                    total_msg,
                    active_win.start_ts,
                    active_win.end_ts,
                    fetched_rows=result["fetched"],
                    upsert_rows=len(rows),
                    bad_rows=result["bad"],
                    skipped_rows=result["skipped"],
                )

        except Exception as e:
            log("down", f"[SCAN][ERR] fct_reader failed | {type(e).__name__}: {repr(e)}")
            _close_silent(read_conn)
            read_conn = connect_blocking("READ")
            time_mod.sleep(FCT_SLEEP_SEC)

        time_mod.sleep(FCT_SLEEP_SEC)

    _close_silent(read_conn)


def vision_reader_main(
    q_out: "queue.Queue[UpsertItem]",
    q_log: "queue.Queue[LogItem]",
    stop_evt: threading.Event,
    wm_store: WatermarkStore,
):
    logger = AsyncDbLogger(q_log)
    sig_cache = SigCache(SIG_CACHE_MAX)
    read_conn = connect_blocking("READ")

    heartbeat_last = 0.0
    scan_log_last = 0.0

    log("info", "[BOOT] vision_reader start")

    while not stop_evt.is_set():
        now_ts = time_mod.time()
        now = datetime.now(KST)
        active_win = compute_window(now)

        if now_ts - heartbeat_last >= 60:
            heartbeat_last = now_ts
            hb_msg = f"vision_reader alive queue_size={q_out.qsize()} active={active_win.prod_day}/{active_win.shift_type}"
            log("info", f"[HEARTBEAT] {hb_msg}")
            logger.write("info", "HEARTBEAT", "VISION_READER", hb_msg, active_win.start_ts, active_win.end_ts)

        try:
            read_conn, result = scan_vision_overlap_once(
                read_conn=read_conn,
                wm_store=wm_store,
                sig_cache=sig_cache,
                now=now,
            )

            rows = result["rows"]
            if rows:
                for i in range(0, len(rows), UPSERT_BATCH_ROWS):
                    chunk = rows[i:i + UPSERT_BATCH_ROWS]
                    q_out.put(
                        UpsertItem(
                            src_table=result["src_table"],
                            src_tag=result["src_tag"],
                            rows=chunk,
                            fetched=result["fetched"],
                            bad=result["bad"],
                            skipped=result["skipped"],
                            restart_watermark_ts=result["restart_watermark_ts"],
                            win_start=result["win_start"],
                            win_end=result["win_end"],
                        )
                    )

            if len(rows) > 0 or (now_ts - scan_log_last >= 60):
                scan_log_last = now_ts
                total_msg = (
                    f"scan src=VISION fetched={result['fetched']} changed={len(rows)} "
                    f"bad={result['bad']} skipped={result['skipped']} "
                    f"restart_wm={result['restart_watermark_ts']} "
                    f"queue_size={q_out.qsize()}"
                )
                log("info", f"[SCAN] {total_msg}")
                logger.write(
                    "info",
                    "SCAN_SRC",
                    "VISION",
                    total_msg,
                    active_win.start_ts,
                    active_win.end_ts,
                    fetched_rows=result["fetched"],
                    upsert_rows=len(rows),
                    bad_rows=result["bad"],
                    skipped_rows=result["skipped"],
                )

        except Exception as e:
            log("down", f"[SCAN][ERR] vision_reader failed | {type(e).__name__}: {repr(e)}")
            _close_silent(read_conn)
            read_conn = connect_blocking("READ")
            time_mod.sleep(VISION_SLEEP_SEC)

        time_mod.sleep(VISION_SLEEP_SEC)

    _close_silent(read_conn)


# =========================
# Writer
# =========================
def writer_main(
    q_in: "queue.Queue[UpsertItem]",
    q_log: "queue.Queue[LogItem]",
    stop_evt: threading.Event,
    wm_store: WatermarkStore,
):
    log("info", "[BOOT] writer start")

    write_conn = connect_blocking("WRITE")
    ensure_target_table(write_conn)
    ensure_state_table(write_conn)

    logger = AsyncDbLogger(q_log)

    heartbeat_last = 0.0
    summary_last = 0.0

    agg_batches = 0
    agg_up_ok = 0
    agg_skipped = 0
    agg_fetched = 0
    agg_bad = 0
    last_win_start = None
    last_win_end = None

    while not stop_evt.is_set():
        now_ts = time_mod.time()

        if now_ts - heartbeat_last >= 60:
            heartbeat_last = now_ts
            hb_msg = f"writer alive queue_size={q_in.qsize()}"
            log("info", f"[HEARTBEAT] {hb_msg}")
            logger.write("info", "HEARTBEAT", "WRITER", hb_msg)

        if now_ts - summary_last >= 60:
            summary_last = now_ts
            if agg_batches > 0:
                msg = (
                    f"writer_summary batches={agg_batches} upserted={agg_up_ok} "
                    f"skipped={agg_skipped} fetched={agg_fetched} bad={agg_bad} queue_size={q_in.qsize()}"
                )
                log("info", f"[UPSERT][SUMMARY] {msg}")
                logger.write(
                    "info",
                    "UPSERT_SUMMARY",
                    "ALL",
                    msg,
                    last_win_start,
                    last_win_end,
                    fetched_rows=agg_fetched,
                    upsert_rows=agg_up_ok,
                    bad_rows=agg_bad,
                    skipped_rows=agg_skipped,
                )
                agg_batches = 0
                agg_up_ok = 0
                agg_skipped = 0
                agg_fetched = 0
                agg_bad = 0
                last_win_start = None
                last_win_end = None

        try:
            item = q_in.get(timeout=1.0)
        except queue.Empty:
            continue

        try:
            write_conn, up_ok, skipped_keepfirst = upsert_batch_keepfirst(write_conn, item.rows)

            if item.restart_watermark_ts is not None:
                wm_store.set(item.src_table, item.restart_watermark_ts)
                write_conn = persist_watermark(write_conn, item.src_table, item.restart_watermark_ts)

            agg_batches += 1
            agg_up_ok += up_ok
            agg_skipped += (item.skipped + skipped_keepfirst)
            agg_fetched += item.fetched
            agg_bad += item.bad
            last_win_start = item.win_start
            last_win_end = item.win_end

        except Exception as e:
            log("down", f"[UPSERT][ERR] {type(e).__name__}: {repr(e)}")
            _close_silent(write_conn)
            write_conn = connect_blocking("WRITE")
            ensure_target_table(write_conn)
            ensure_state_table(write_conn)

        finally:
            q_in.task_done()

    _close_silent(write_conn)


# =========================
# main
# =========================
def main():
    _ensure_local_log_dir()

    log("info", "[BOOT] start")
    log("info", f"[CONF] FCT_SOURCE={SRC_SCHEMA}.{SRC_FCT_TABLE}")
    log("info", f"[CONF] TARGET(i)={DST_SCHEMA}.{DST_TABLE} | LOG(k)={LOG_SCHEMA}.{LOG_TABLE} | STATE(k)={LOG_SCHEMA}.{STATE_TABLE}")
    log("info", f"[CONF] fct_interval={FCT_SLEEP_SEC}s")
    log("info", f"[CONF] vision_interval={VISION_SLEEP_SEC}s")
    log("info", "[CONF] read_connection=reuse_until_broken")
    log("info", "[CONF] reconnect=retry_until_connected")
    log("info", "[CONF] classify=overlap_majority(day_vs_night), tie=>day")
    log("info", f"[CONF] fct_threshold_source={FCT_THRESH_SCHEMA}.{FCT_THRESH_TABLE}.{FCT_THRESH_COL}")
    log("info", f"[CONF] fct_threshold_rule=no_operation_time > monthly_max_q3 fallback={FCT_THRESH_FALLBACK_SEC}")
    log("info", "[CONF] vision_threshold_rule=removed")
    log("info", "[CONF] vision_overlap_rule=overlap_to > overlap_from only")
    log("info", "[CONF] restart_recovery=use second-latest processed to_ts")
    log("info", f"[CONF] fct_manual_force_include=manual='{MANUAL_FORCE_VALUE}'")
    log("info", f"[CONF] default_work_mem={DEFAULT_WORK_MEM}")

    w = connect_blocking("WRITE")
    ensure_target_table(w)
    ensure_progress_table(w)
    ensure_state_table(w)

    wm_store = WatermarkStore()
    wm_store.load_from_db(w)
    _close_silent(w)

    fct_threshold_store = MonthlyThresholdStore(
        schema_name=FCT_THRESH_SCHEMA,
        table_name=FCT_THRESH_TABLE,
        col_name=FCT_THRESH_COL,
        fallback_sec=FCT_THRESH_FALLBACK_SEC,
    )

    q_upsert: "queue.Queue[UpsertItem]" = queue.Queue(maxsize=QUEUE_MAX)
    q_log: "queue.Queue[LogItem]" = queue.Queue(maxsize=LOG_QUEUE_MAX)
    stop_evt = threading.Event()

    boot_logger = AsyncDbLogger(q_log)
    boot_logger.write("info", "BOOT", None, "daemon booted")

    th_log = threading.Thread(target=log_worker_main, args=(q_log, stop_evt), daemon=True)
    th_fct = threading.Thread(target=fct_reader_main, args=(q_upsert, q_log, stop_evt, wm_store, fct_threshold_store), daemon=True)
    th_vis = threading.Thread(target=vision_reader_main, args=(q_upsert, q_log, stop_evt, wm_store), daemon=True)
    th_wri = threading.Thread(target=writer_main, args=(q_upsert, q_log, stop_evt, wm_store), daemon=True)

    th_log.start()
    th_fct.start()
    th_vis.start()
    th_wri.start()

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