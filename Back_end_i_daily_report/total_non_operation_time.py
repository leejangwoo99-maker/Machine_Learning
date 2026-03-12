# -*- coding: utf-8 -*-
"""
total_non_operation_time_realtime.py

최종 구조
1) SOURCE는 created_at 기준으로 "신규행만" 증분 조회
2) 주/야간 window는 조회 범위용이 아니라 "분류용"으로만 사용
3) 기존 row 수정 없음, 새 비가동은 무조건 새 row insert 전제
4) reason / sparepart 는 SOURCE에서 읽지 않음
5) Streamlit이 Back_end_i_daily_report.total_non_operation_time 에 직접 입력/수정
6) UPSERT 시 기존 reason / sparepart 는 절대 덮어쓰지 않음
7) WRITE(UPSERT) / WRITE(LOG) 분리
8) Heartbeat 1분 1회, DB 로그 최소화
9) 워터마크(last_created_at)는 DB state table + 메모리 공유로 관리
10) 프로세스 재시작 시 state가 없으면 "현재 활성 shift 시작시각 - LOOKBACK"부터 시작

주/야간 분류 규칙
- 주간 : [D-DAY] 08:30:00 ~ 20:29:59
- 야간 : [D-DAY] 20:30:00 ~ [D+1] 08:29:59
- row가 주/야간에 걸치면 더 많이 겹친 shift에 귀속
- 겹치는 시간이 동일하면 day
- 분류 기준은 source의 end_day + from_time + to_time

주의
- 본 코드는 SOURCE 테이블(fct_non_operation_time / vision_non_operation_time)에 created_at 컬럼이 있다고 가정
- target unique key는 (source_table, end_day, station, from_ts)
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
UPSERT_BATCH_ROWS = 300

SRC_SCHEMA = "g_production_film"
SRC_FCT_TABLE = "fct_non_operation_time"
SRC_VISION_TABLE = "vision_non_operation_time"

DST_SCHEMA = "Back_end_i_daily_report"
DST_TABLE = "total_non_operation_time"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "total_non_operation"
STATE_TABLE = "total_non_operation_state"

DAY_START = dtime(8, 30, 0)
NIGHT_START = dtime(20, 30, 0)

STATEMENT_TIMEOUT_MS = 30_000
DEFAULT_WORK_MEM = "4MB"

QUEUE_MAX = 20_000
SIG_CACHE_MAX = 300_000

# created_at 경계 중복/지연 방지
LOOKBACK_SEC = 3

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
    line = Demon_f"{_ts()} [{lvl}] {msg}"
    print(line, flush=True)
    _append_local_log(line)


# =========================
# DB connect
# =========================
def _work_mem_value() -> str:
    v = os.getenv("PG_WORK_MEM", "").strip()
    return v if v else DEFAULT_WORK_MEM


def connect_blocking(role: str):
    """
    role: 'READ' or 'WRITE'
    - READ : autocommit False
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
                application_name=Demon_f"total_nonop_{role.lower()}",
            )
            conn.autocommit = (role == "WRITE")

            cur = conn.cursor()
            cur.execute(Demon_f"SET work_mem = '{work_mem}';")
            cur.execute(Demon_f"SET statement_timeout = '{int(STATEMENT_TIMEOUT_MS)}';")
            cur.close()

            cur = conn.cursor()
            cur.execute("SELECT 1;")
            cur.fetchall()
            cur.close()

            log(
                "info",
                Demon_f"[DB] connected ({role}) host={DB_CONFIG['host']}:{DB_CONFIG['port']} "
                Demon_f"work_mem={work_mem} stmt_timeout={STATEMENT_TIMEOUT_MS}ms"
            )
            return conn

        except Exception as e:
            log("down", Demon_f"[DB][RETRY] connect({role}) failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)


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
    ddl = Demon_f"""
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
    ddl = Demon_f"""
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
    ddl = Demon_f"""
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
# Progress logger
# =========================
class ProgressLogger:
    def __init__(self, name: str):
        self.name = name
        self.conn = None

    def connect(self):
        _close_silent(self.conn)
        self.conn = connect_blocking("WRITE")
        ensure_progress_table(self.conn)
        log("info", Demon_f"[LOG] logger connected ({self.name})")

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
        while True:
            try:
                if self.conn is None or getattr(self.conn, "closed", 1) != 0:
                    self.connect()

                cur = self.conn.cursor()
                cur.execute(
                    Demon_f"""
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
                return

            except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                log("down", Demon_f"[LOG][RETRY][{self.name}] connection lost -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
                _close_silent(self.conn)
                self.conn = None
                time_mod.sleep(SLEEP_SEC)

            except Exception as e:
                pgerr = ""
                try:
                    pgerr = getattr(e, "pgerror", "") or ""
                except Exception:
                    pass
                log("down", Demon_f"[LOG][RETRY][{self.name}] insert failed -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)} {pgerr}")
                _close_silent(self.conn)
                self.conn = None
                time_mod.sleep(SLEEP_SEC)

    def close(self):
        _close_silent(self.conn)
        self.conn = None


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
    today = now.date()
    dt_day_start = datetime.combine(today, DAY_START)
    dt_night_start = datetime.combine(today, NIGHT_START)

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
        start_ts=datetime.combine(y, NIGHT_START),
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

    from_dt = datetime.combine(d, ft)
    to_dt = datetime.combine(d, tt)
    if to_dt <= from_dt:
        to_dt += timedelta(days=1)
    return from_dt, to_dt


def _overlap_seconds(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> float:
    s = max(a_start, b_start)
    e = min(a_end, b_end)
    if e <= s:
        return 0.0
    return (e - s).total_seconds()


def classify_shift_by_overlap(end_day_text: str, from_time_text: Any, to_time_text: Any) -> Tuple[Optional[str], Optional[str], Optional[datetime], Optional[datetime]]:
    """
    row 1건을 day/night 중 어디에 귀속할지 판단
    - 더 많이 겹친 shift 귀속
    - 동률이면 day
    """
    from_dt, to_dt = make_from_to_dt(end_day_text, from_time_text, to_time_text)
    if from_dt is None or to_dt is None:
        return None, None, None, None

    start_date = from_dt.date() - timedelta(days=1)
    end_date = to_dt.date() + timedelta(days=1)

    best_overlap = -1.0
    best_prod_day = None
    best_shift_type = None

    cur = start_date
    while cur <= end_date:
        day_s = datetime.combine(cur, DAY_START)
        day_e = datetime.combine(cur, NIGHT_START)
        day_sec = _overlap_seconds(from_dt, to_dt, day_s, day_e)
        if day_sec > 0:
            if (day_sec > best_overlap) or (day_sec == best_overlap and best_shift_type != "day"):
                best_overlap = day_sec
                best_prod_day = cur.strftime("%Y%m%d")
                best_shift_type = "day"

        night_s = datetime.combine(cur, NIGHT_START)
        night_e = datetime.combine(cur + timedelta(days=1), DAY_START)
        night_sec = _overlap_seconds(from_dt, to_dt, night_s, night_e)
        if night_sec > 0:
            if (night_sec > best_overlap) or (night_sec == best_overlap and best_shift_type != "day"):
                best_overlap = night_sec
                best_prod_day = cur.strftime("%Y%m%d")
                best_shift_type = "night"

        cur += timedelta(days=1)

    return best_prod_day, best_shift_type, from_dt, to_dt


# =========================
# Sig cache
# - lookback으로 같은 row를 다시 읽을 수 있으므로
#   source unique + created_at 조합으로 중복 enqueue 축소
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
        cur.execute(Demon_f"SELECT src_table, last_created_at FROM {LOG_SCHEMA}.{STATE_TABLE}")
        rows = cur.fetchall()
        cur.close()

        with self._lock:
            self._m = {}
            for src_table, last_created_at in rows:
                if src_table:
                    self._m[str(src_table)] = last_created_at

    def get(self, src_table: str) -> Optional[datetime]:
        with self._lock:
            return self._m.get(src_table)

    def set(self, src_table: str, last_created_at: Optional[datetime]):
        with self._lock:
            if last_created_at is not None:
                old = self._m.get(src_table)
                if old is None or last_created_at > old:
                    self._m[src_table] = last_created_at

    def items(self):
        with self._lock:
            return dict(self._m)


def persist_watermark(write_conn, src_table: str, last_created_at: datetime):
    while True:
        try:
            cur = write_conn.cursor()
            cur.execute(
                Demon_f"""
                INSERT INTO {LOG_SCHEMA}.{STATE_TABLE} (src_table, last_created_at)
                VALUES (%s, %s)
                ON CONFLICT (src_table)
                DO UPDATE SET
                    last_created_at = GREATEST(
                        COALESCE({LOG_SCHEMA}.{STATE_TABLE}.last_created_at, EXCLUDED.last_created_at),
                        EXCLUDED.last_created_at
                    ),
                    updated_at = now()
                """,
                (src_table, last_created_at),
            )
            cur.close()
            return

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log("down", Demon_f"[DB][RETRY] persist_watermark conn lost -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            _close_silent(write_conn)
            time_mod.sleep(SLEEP_SEC)
            write_conn = connect_blocking("WRITE")
            ensure_state_table(write_conn)

        except Exception as e:
            log("down", Demon_f"[DB][RETRY] persist_watermark failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)


# =========================
# SQL
# =========================
def make_incremental_scan_sql(src_table: str) -> str:
    return Demon_f"""
    SELECT
        end_day,
        station,
        from_time,
        to_time,
        created_at
    FROM {SRC_SCHEMA}.{src_table}
    WHERE created_at >= %s
    ORDER BY created_at ASC, end_day ASC, station ASC, from_time ASC, to_time ASC
    """


# =========================
# Upsert
# - reason/sparepart는 insert 때 NULL
# - conflict update 때 절대 건드리지 않음
# =========================
UPSERT_SQL = Demon_f"""
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
    NULL,
    NULL
)
ON CONFLICT (source_table, end_day, station, from_ts)
DO UPDATE SET
    prod_day   = EXCLUDED.prod_day,
    shift_type = EXCLUDED.shift_type,
    to_ts      = EXCLUDED.to_ts,
    updated_at = now();
"""


def upsert_batch_keepfirst(write_conn, rows: List[dict]):
    if not rows:
        return 0, 0

    while True:
        try:
            cur = write_conn.cursor()
            execute_batch(cur, UPSERT_SQL, rows, page_size=len(rows))
            cur.close()
            return len(rows), 0

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log("down", Demon_f"[DB][RETRY] upsert conn lost -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            _close_silent(write_conn)
            time_mod.sleep(SLEEP_SEC)
            write_conn = connect_blocking("WRITE")
            ensure_target_table(write_conn)

        except psycopg2.errors.UniqueViolation:
            try:
                if not write_conn.autocommit:
                    write_conn.rollback()
            except Exception:
                pass

            up_ok = 0
            skipped = 0
            retry_break = False

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

                except (psycopg2.OperationalError, psycopg2.InterfaceError) as ee:
                    try:
                        if not write_conn.autocommit:
                            write_conn.rollback()
                    except Exception:
                        pass
                    log("down", Demon_f"[DB][RETRY] upsert row conn lost -> reconnect in {SLEEP_SEC}s | {type(ee).__name__}: {repr(ee)}")
                    _close_silent(write_conn)
                    time_mod.sleep(SLEEP_SEC)
                    write_conn = connect_blocking("WRITE")
                    ensure_target_table(write_conn)
                    retry_break = True
                    break

                except Exception as ee:
                    try:
                        if not write_conn.autocommit:
                            write_conn.rollback()
                    except Exception:
                        pass
                    log("down", Demon_f"[DB][RETRY] upsert row failed -> retry in {SLEEP_SEC}s | {type(ee).__name__}: {repr(ee)}")
                    time_mod.sleep(SLEEP_SEC)
                    retry_break = True
                    break

            if retry_break:
                continue

            return up_ok, skipped

        except Exception as e:
            log("down", Demon_f"[DB][RETRY] upsert failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)


# =========================
# Stream select
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
            log("down", Demon_f"[DB][RETRY] stream_select conn lost -> reconnect in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)}")
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

            log("down", Demon_f"[DB][RETRY] stream_select failed -> retry in {SLEEP_SEC}s | {type(e).__name__}: {repr(e)} {pgerr}")
            time_mod.sleep(SLEEP_SEC)
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
    max_created_at: Optional[datetime]
    win_start: Optional[datetime]
    win_end: Optional[datetime]


# =========================
# Reader
# =========================
def _initial_watermark(now: datetime) -> datetime:
    """
    state가 없을 때 초기 시작점
    - 현재 활성 shift 시작시각 - LOOKBACK
    """
    win = compute_window(now)
    return win.start_ts - timedelta(seconds=LOOKBACK_SEC)


def reader_main(
    q_out: "queue.Queue[UpsertItem]",
    stop_evt: threading.Event,
    wm_store: WatermarkStore,
):
    log("info", "[BOOT] reader start")

    read_conn = connect_blocking("READ")
    logger = ProgressLogger("reader")
    logger.connect()

    sig_cache = SigCache(SIG_CACHE_MAX)

    sources = [
        (SRC_FCT_TABLE, "FCT"),
        (SRC_VISION_TABLE, "VISION"),
    ]

    heartbeat_last = 0.0
    scan_log_last = 0.0

    while not stop_evt.is_set():
        now_ts = time_mod.time()
        now = datetime.now()
        active_win = compute_window(now)

        if now_ts - heartbeat_last >= 60:
            heartbeat_last = now_ts
            hb_msg = Demon_f"reader alive queue_size={q_out.qsize()} active={active_win.prod_day}/{active_win.shift_type}"
            log("info", Demon_f"[HEARTBEAT] {hb_msg}")
            logger.write("info", "HEARTBEAT", "READER", hb_msg, active_win.start_ts, active_win.end_ts)

        total_fetched = 0
        total_bad = 0
        total_changed = 0
        total_skipped = 0

        for (src_table, src_tag) in sources:
            last_wm = wm_store.get(src_table)
            if last_wm is None:
                last_wm = _initial_watermark(now)

            query_from = last_wm - timedelta(seconds=LOOKBACK_SEC)
            sql = make_incremental_scan_sql(src_table)
            params = (query_from,)

            changed_rows: List[dict] = []
            fetched = 0
            bad = 0
            skipped = 0
            max_created_at: Optional[datetime] = None
            win_start_for_log: Optional[datetime] = None
            win_end_for_log: Optional[datetime] = None

            for cols, rows in stream_select(read_conn, sql, params, FETCH_MANY_ROWS):
                idx = {c: i for i, c in enumerate(cols)}
                fetched += len(rows)

                for row in rows:
                    end_day = str(row[idx["end_day"]])
                    station = str(row[idx["station"]])
                    created_at = row[idx["created_at"]]

                    if created_at is not None:
                        if max_created_at is None or created_at > max_created_at:
                            max_created_at = created_at

                    prod_day, shift_type, from_dt, to_dt = classify_shift_by_overlap(
                        end_day,
                        row[idx["from_time"]],
                        row[idx["to_time"]],
                    )
                    if from_dt is None or to_dt is None or prod_day is None or shift_type is None:
                        bad += 1
                        continue

                    # lookback 재조회 중복 방지
                    key = (src_table, end_day, station, from_dt, to_dt)
                    sig = (created_at,)

                    if not sig_cache.upsert_if_changed(key, sig):
                        skipped += 1
                        continue

                    if win_start_for_log is None or from_dt < win_start_for_log:
                        win_start_for_log = from_dt
                    if win_end_for_log is None or to_dt > win_end_for_log:
                        win_end_for_log = to_dt

                    changed_rows.append(
                        {
                            "source_table": src_table,
                            "end_day": end_day,
                            "prod_day": prod_day,
                            "shift_type": shift_type,
                            "station": station,
                            "from_ts": from_dt.strftime("%Y-%m-%d %H:%M:%S.%Demon_f_mining+09:00"),
                            "to_ts": to_dt.strftime("%Y-%m-%d %H:%M:%S.%Demon_f_mining+09:00"),
                        }
                    )

            total_fetched += fetched
            total_bad += bad
            total_changed += len(changed_rows)
            total_skipped += skipped

            if changed_rows:
                for i in range(0, len(changed_rows), UPSERT_BATCH_ROWS):
                    chunk = changed_rows[i:i + UPSERT_BATCH_ROWS]
                    q_out.put(
                        UpsertItem(
                            src_table=src_table,
                            src_tag=src_tag,
                            rows=chunk,
                            fetched=fetched,
                            bad=bad,
                            skipped=skipped,
                            max_created_at=max_created_at,
                            win_start=win_start_for_log,
                            win_end=win_end_for_log,
                        )
                    )

        if total_changed > 0 or (now_ts - scan_log_last >= 60):
            scan_log_last = now_ts
            total_msg = (
                Demon_f"scan_total fetched={total_fetched} changed={total_changed} "
                Demon_f"bad={total_bad} skipped={total_skipped} queue_size={q_out.qsize()}"
            )
            log("info", Demon_f"[SCAN][TOTAL] {total_msg}")
            logger.write(
                "info",
                "SCAN_TOTAL",
                "ALL",
                total_msg,
                active_win.start_ts,
                active_win.end_ts,
                fetched_rows=total_fetched,
                upsert_rows=total_changed,
                bad_rows=total_bad,
                skipped_rows=total_skipped,
            )

        time_mod.sleep(SLEEP_SEC)

    _close_silent(read_conn)
    logger.close()


# =========================
# Writer
# =========================
def writer_main(
    q_in: "queue.Queue[UpsertItem]",
    stop_evt: threading.Event,
    wm_store: WatermarkStore,
):
    log("info", "[BOOT] writer start")

    write_conn = connect_blocking("WRITE")
    ensure_target_table(write_conn)
    ensure_state_table(write_conn)

    logger = ProgressLogger("writer")
    logger.connect()

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
            hb_msg = Demon_f"writer alive queue_size={q_in.qsize()}"
            log("info", Demon_f"[HEARTBEAT] {hb_msg}")
            logger.write("info", "HEARTBEAT", "WRITER", hb_msg)

        if now_ts - summary_last >= 60:
            summary_last = now_ts
            if agg_batches > 0:
                msg = (
                    Demon_f"writer_summary batches={agg_batches} upserted={agg_up_ok} "
                    Demon_f"skipped={agg_skipped} fetched={agg_fetched} bad={agg_bad} queue_size={q_in.qsize()}"
                )
                log("info", Demon_f"[UPSERT][SUMMARY] {msg}")
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
            up_ok, skipped_keepfirst = upsert_batch_keepfirst(write_conn, item.rows)

            # watermark는 write 성공 후에만 전진
            if item.max_created_at is not None:
                wm_store.set(item.src_table, item.max_created_at)
                persist_watermark(write_conn, item.src_table, item.max_created_at)

            agg_batches += 1
            agg_up_ok += up_ok
            agg_skipped += (item.skipped + skipped_keepfirst)
            agg_fetched += item.fetched
            agg_bad += item.bad
            last_win_start = item.win_start
            last_win_end = item.win_end

        except Exception as e:
            err_msg = Demon_f"{type(e).__name__}: {repr(e)}"
            log("down", Demon_f"[UPSERT][ERR] {err_msg}")
            logger.write(
                "down",
                "UPSERT_ERR",
                item.src_tag,
                err_msg,
                item.win_start,
                item.win_end,
            )

        finally:
            q_in.task_done()

    if agg_batches > 0:
        msg = (
            Demon_f"writer_summary batches={agg_batches} upserted={agg_up_ok} "
            Demon_f"skipped={agg_skipped} fetched={agg_fetched} bad={agg_bad} queue_size={q_in.qsize()}"
        )
        log("info", Demon_f"[UPSERT][SUMMARY] {msg}")
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

    _close_silent(write_conn)
    logger.close()


# =========================
# main
# =========================
def main():
    _ensure_local_log_dir()

    log("info", Demon_f"[BOOT] start (READ=1, WRITE_UPSERT=1, WRITE_LOG_READER=1, WRITE_LOG_WRITER=1) interval={SLEEP_SEC}s")
    log("info", Demon_f"[CONF] SOURCE=({SRC_SCHEMA}.{SRC_FCT_TABLE}, {SRC_SCHEMA}.{SRC_VISION_TABLE})")
    log("info", Demon_f"[CONF] TARGET(i)={DST_SCHEMA}.{DST_TABLE} | LOG(k)={LOG_SCHEMA}.{LOG_TABLE} | STATE(k)={LOG_SCHEMA}.{STATE_TABLE}")
    log("info", "[CONF] polling=created_at incremental only")
    log("info", "[CONF] classify=overlap_majority(day_vs_night), tie=>day")
    log("info", "[CONF] source_reason_sparepart=ignored")
    log("info", Demon_f"[CONF] lookback={LOOKBACK_SEC}s")

    # 초기 DDL 보장 + state load
    w = connect_blocking("WRITE")
    ensure_target_table(w)
    ensure_progress_table(w)
    ensure_state_table(w)

    wm_store = WatermarkStore()
    wm_store.load_from_db(w)
    _close_silent(w)

    boot_logger = ProgressLogger("boot")
    boot_logger.connect()
    boot_logger.write("info", "BOOT", None, "daemon booted")
    boot_logger.close()

    q_upsert: "queue.Queue[UpsertItem]" = queue.Queue(maxsize=QUEUE_MAX)
    stop_evt = threading.Event()

    th_r = threading.Thread(target=reader_main, args=(q_upsert, stop_evt, wm_store), daemon=True)
    th_w = threading.Thread(target=writer_main, args=(q_upsert, stop_evt, wm_store), daemon=True)
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
        log("error", Demon_f"fatal: {type(e).__name__}: {e}")
        _append_local_log(traceback.format_exc())
        raise