# -*- coding: utf-8 -*-
"""
d4_afa_fail_wasted_time_factory.py

AFA FAIL wasted time (NG -> ON) 실시간 계산/저장
- 5초 루프
- DB 접속 실패/끊김 시 무한 재시도
- 메인 엔진 / 헬스 로그 엔진 분리
- health log는 queue + worker thread 로 비동기 적재
- txt 파일 로그 없음
- 콘솔 + DB health log만 사용
- pandas 제거
- warm-start backfill 유지
- 증분 fetch는 station별 마지막 저장 to_ts 기준
- 저장 테이블에 to_ts TIMESTAMP 컬럼 사용
- last cursor 조회는 현재 운영 window 내부만 조회
- traceback는 최대 100행까지만 적재
"""

from __future__ import annotations

import os
import re
import time as time_mod
import threading
import queue
import traceback
import urllib.parse
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, Optional, Tuple, List, Any

from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from zoneinfo import ZoneInfo

try:
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore
except Exception:
    psycopg2 = None  # type: ignore


# =========================================================
# 환경 / 상수
# =========================================================
KST = ZoneInfo("Asia/Seoul")

os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",  # 비밀번호 입력
}

SCHEMA = "d1_machine_log"
SAVE_TABLE = "afa_fail_wasted_time"

HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "d4_log"

SRC_TABLES = [
    ("FCT1_machine_log", "FCT1"),
    ("FCT2_machine_log", "FCT2"),
    ("FCT3_machine_log", "FCT3"),
    ("FCT4_machine_log", "FCT4"),
]

NG_PHRASE = "제품 감지 ng"
OFF_PHRASE = "제품 검사 투입요구 on"
MANUAL_PHRASE = "manual mode 전환"
AUTO_PHRASE = "auto mode 전환"

NG_PHRASE_RAW = "제품 감지 NG"
OFF_PHRASE_RAW = "제품 검사 투입요구 ON"

LOOP_INTERVAL_SEC = int(os.getenv("D4_LOOP_INTERVAL_SEC", "5"))
DB_RETRY_INTERVAL_SEC = int(os.getenv("DB_RETRY_INTERVAL_SEC", "5"))
CURSOR_OVERLAP_SEC = int(os.getenv("AFA_CURSOR_OVERLAP_SEC", "3"))

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# 10초 -> 20초로 상향
STMT_TIMEOUT_MS = int(os.getenv("D4_STMT_TIMEOUT_MS", "20000"))
LOCK_TIMEOUT_MS = int(os.getenv("D4_LOCK_TIMEOUT_MS", "3000"))
IDLE_TX_TIMEOUT_MS = int(os.getenv("D4_IDLE_TX_TIMEOUT_MS", "5000"))

PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

DB_LOG_QUEUE_MAX = int(os.getenv("DB_LOG_QUEUE_MAX", "10000"))
DB_LOG_BATCH_SIZE = int(os.getenv("DB_LOG_BATCH_SIZE", "300"))
DB_LOG_FLUSH_INTERVAL_SEC = float(os.getenv("DB_LOG_FLUSH_INTERVAL_SEC", "2.0"))
DB_LOG_CONTENTS_MAXLEN = int(os.getenv("DB_LOG_CONTENTS_MAXLEN", "2000"))

TRACEBACK_MAX_LINES = int(os.getenv("TRACEBACK_MAX_LINES", "100"))

_ENGINE_MAIN = None
_ENGINE_HEALTH = None

db_log_queue: queue.Queue = queue.Queue(maxsize=max(1000, DB_LOG_QUEUE_MAX))
_db_log_enabled = True


# =========================================================
# 유틸
# =========================================================
def _masked_db_info(cfg=DB_CONFIG) -> str:
    return f"postgresql://{cfg['user']}:***@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"


def _normalize_info(info: str) -> str:
    if not info:
        return "info"
    s = re.sub(r"[^a-z0-9_]+", "_", info.strip().lower())
    s = s.strip("_")
    return s or "info"


def _infer_info_from_msg(msg: str) -> str:
    m = (msg or "").lower()
    if "[error]" in m or "trace" in m or "fatal" in m or "[unhandled]" in m:
        return "error"
    if "[retry]" in m or "failed" in m or "conn error" in m or "down" in m:
        return "down"
    if "[boot]" in m or "[ok]" in m:
        return "boot"
    if "[stop]" in m:
        return "stop"
    if "sleep" in m:
        return "sleep"
    if "[perf]" in m:
        return "perf"
    if "[warn]" in m:
        return "warn"
    return "info"


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, OperationalError):
        return True
    if isinstance(e, DBAPIError):
        if getattr(e, "connection_invalidated", False):
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
        "'nonetype' object has no attribute 'connect'",
    ]
    return any(k in msg for k in keywords)


_ZWSP_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")


def norm_text(s: object) -> str:
    if s is None:
        return ""
    t = str(s)
    t = unicodedata.normalize("NFKC", t)
    t = _ZWSP_RE.sub("", t)
    t = t.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t.lower()


def is_ng(c: str) -> bool:
    return NG_PHRASE in c


def is_off(c: str) -> bool:
    return OFF_PHRASE in c


def is_manual(c: str) -> bool:
    return MANUAL_PHRASE in c


def is_auto(c: str) -> bool:
    return AUTO_PHRASE in c


def to_ts_kst(end_day: str, hhmmss_like: str) -> Optional[datetime]:
    try:
        base = str(hhmmss_like).split(".")[0]
        dt = datetime.strptime(f"{end_day} {base}", "%Y%m%d %H:%M:%S")
        return dt.replace(tzinfo=KST)
    except Exception:
        return None


def format_hhmmss_ff(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%H:%M:%S.%f")[:11]


def to_naive_kst(dt: datetime) -> datetime:
    return dt.astimezone(KST).replace(tzinfo=None)


def _format_traceback_limited(max_lines: int = TRACEBACK_MAX_LINES) -> List[str]:
    try:
        tb_lines = traceback.format_exc().rstrip().splitlines()
        if not tb_lines:
            return []

        if max_lines > 0 and len(tb_lines) > max_lines:
            omitted = len(tb_lines) - max_lines
            tb_lines = [f"... traceback truncated, omitted {omitted} lines ..."] + tb_lines[-max_lines:]

        return tb_lines
    except Exception:
        return []


# =========================================================
# 로그
# =========================================================
def _enqueue_db_log(info: str, contents: str):
    global _db_log_enabled
    if not _db_log_enabled:
        return

    now = datetime.now(tz=KST)
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": (contents or "")[:DB_LOG_CONTENTS_MAXLEN],
    }

    try:
        db_log_queue.put_nowait(row)
    except queue.Full:
        ts = now.strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] [WARN] db_log_queue full. health log dropped.", flush=True)


def log(msg: str, info: str | None = None):
    ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)

    try:
        tag = _normalize_info(info) if info else _infer_info_from_msg(msg)
        _enqueue_db_log(tag, msg)
    except Exception:
        pass


def log_exc(prefix: str, e: Exception):
    log(f"{prefix}: {type(e).__name__}: {repr(e)}", info="error")

    tb_lines = _format_traceback_limited()
    for line in tb_lines:
        log(f"{prefix} TRACE: {line}", info="error")


# =========================================================
# Engine 생성 / 관리
# =========================================================
def _build_engine(cfg=DB_CONFIG, application_name: str = "d4_afa_fail"):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]

    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?connect_timeout=5"
    options = (
        f"-c work_mem={WORK_MEM} "
        f"-c statement_timeout={STMT_TIMEOUT_MS} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS} "
        f"-c idle_in_transaction_session_timeout={IDLE_TX_TIMEOUT_MS}"
    )

    eng = create_engine(
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
            "application_name": application_name,
            "options": options,
        },
    )
    return eng


def _dispose_engine_main():
    global _ENGINE_MAIN
    try:
        if _ENGINE_MAIN is not None:
            _ENGINE_MAIN.dispose()
    except Exception:
        pass
    _ENGINE_MAIN = None


def _dispose_engine_health():
    global _ENGINE_HEALTH
    try:
        if _ENGINE_HEALTH is not None:
            _ENGINE_HEALTH.dispose()
    except Exception:
        pass
    _ENGINE_HEALTH = None


def get_engine_main_blocking():
    global _ENGINE_MAIN

    while _ENGINE_MAIN is not None:
        try:
            with _ENGINE_MAIN.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE_MAIN
        except Exception as e:
            log("[DB][RETRY] main engine ping failed -> rebuild", info="down")
            log_exc("[DB][RETRY] main ping error", e)
            _dispose_engine_main()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE_MAIN = _build_engine(DB_CONFIG, "d4_afa_fail_main")
            with _ENGINE_MAIN.connect() as conn:
                conn.execute(text("SELECT 1"))
            log(
                f"[DB][OK] main engine ready pool_size=1 max_overflow=0 "
                f"work_mem={WORK_MEM} stmt_timeout_ms={STMT_TIMEOUT_MS} "
                f"lock_timeout_ms={LOCK_TIMEOUT_MS} idle_tx_timeout_ms={IDLE_TX_TIMEOUT_MS}",
                info="boot",
            )
            log(f"[INFO] MAIN DB = {_masked_db_info(DB_CONFIG)}", info="info")
            return _ENGINE_MAIN
        except Exception as e:
            log("[DB][RETRY] main engine create/connect failed", info="down")
            log_exc("[DB][RETRY] main connect error", e)
            _dispose_engine_main()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def get_engine_health_blocking():
    global _ENGINE_HEALTH

    while _ENGINE_HEALTH is not None:
        try:
            with _ENGINE_HEALTH.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE_HEALTH
        except Exception as e:
            ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] health engine ping failed: {type(e).__name__}: {e}", flush=True)
            _dispose_engine_health()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE_HEALTH = _build_engine(DB_CONFIG, "d4_afa_fail_health")
            with _ENGINE_HEALTH.connect() as conn:
                conn.execute(text("SELECT 1"))
            log("[DB][OK] health engine ready dedicated for health log", info="boot")
            return _ENGINE_HEALTH
        except Exception as e:
            ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] health engine create/connect failed: {type(e).__name__}: {e}", flush=True)
            _dispose_engine_health()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================================================
# DDL
# =========================================================
CREATE_SAVE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {SCHEMA}.{SAVE_TABLE} (
    end_day       TEXT,
    station       TEXT,
    from_contents TEXT,
    from_time     TEXT,
    to_contents   TEXT,
    to_time       TEXT,
    to_ts         TIMESTAMP,
    wasted_time   NUMERIC(10,2),
    created_at    TIMESTAMP DEFAULT now()
);
"""

ALTER_SAVE_ADD_TO_TS_SQL = f"""
ALTER TABLE {SCHEMA}.{SAVE_TABLE}
ADD COLUMN IF NOT EXISTS to_ts TIMESTAMP;
"""

CREATE_SAVE_UNIQUE_SQL = f"""
CREATE UNIQUE INDEX IF NOT EXISTS ux_{SCHEMA}_{SAVE_TABLE}_pk
ON {SCHEMA}.{SAVE_TABLE} (end_day, station, from_time, to_time);
"""

CREATE_SAVE_TO_TS_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{SCHEMA}_{SAVE_TABLE}_station_to_ts
ON {SCHEMA}.{SAVE_TABLE} (station, to_ts DESC);
"""

BACKFILL_TO_TS_SQL = f"""
UPDATE {SCHEMA}.{SAVE_TABLE}
SET to_ts = to_timestamp(end_day || ' ' || split_part(to_time, '.', 1), 'YYYYMMDD HH24:MI:SS')
WHERE to_ts IS NULL
  AND end_day IS NOT NULL
  AND to_time IS NOT NULL;
"""

CREATE_HEALTH_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {HEALTH_SCHEMA}.{HEALTH_TABLE} (
    id         BIGSERIAL PRIMARY KEY,
    end_day    VARCHAR(8)  NOT NULL,
    end_time   VARCHAR(8)  NOT NULL,
    info       VARCHAR(32) NOT NULL,
    contents   TEXT,
    created_at TIMESTAMP DEFAULT now()
);
"""

CREATE_HEALTH_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS ix_{HEALTH_SCHEMA}_{HEALTH_TABLE}_day_time
ON {HEALTH_SCHEMA}.{HEALTH_TABLE} (end_day, end_time);
"""


def ensure_schema_and_tables(engine_main, engine_health):
    while True:
        try:
            with engine_main.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))
                conn.execute(text(CREATE_SAVE_TABLE_SQL))
                conn.execute(text(ALTER_SAVE_ADD_TO_TS_SQL))
                conn.execute(text(CREATE_SAVE_UNIQUE_SQL))
                conn.execute(text(CREATE_SAVE_TO_TS_INDEX_SQL))
                conn.execute(text(BACKFILL_TO_TS_SQL))
                log(f"[INFO] Save table ensured: {SCHEMA}.{SAVE_TABLE}", info="info")

            with engine_health.begin() as conn:
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA}"))
                conn.execute(text(CREATE_HEALTH_TABLE_SQL))
                conn.execute(text(CREATE_HEALTH_INDEX_SQL))
                log(f"[INFO] Health log table ensured: {HEALTH_SCHEMA}.{HEALTH_TABLE}", info="info")

            return

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] ensure_schema_and_tables conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] ensure_schema_and_tables", e)
                _dispose_engine_main()
                _dispose_engine_health()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine_main = get_engine_main_blocking()
                engine_health = get_engine_health_blocking()
                continue

            log("[DB][RETRY] ensure_schema_and_tables failed", info="down")
            log_exc("[DB][RETRY] ensure_schema_and_tables", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine_main = get_engine_main_blocking()
            engine_health = get_engine_health_blocking()


# =========================================================
# Health log worker
# =========================================================
class HealthLogWorker:
    def __init__(self):
        self.stop_event = threading.Event()
        self.thread = threading.Thread(target=self._run, name="HealthLogWorker", daemon=True)
        self._local_buffer: list[dict] = []

    def start(self):
        self.thread.start()

    def stop(self):
        self.stop_event.set()

    def join(self, timeout: float | None = None):
        self.thread.join(timeout=timeout)

    def _flush_batch(self, batch: list[dict]):
        if not batch:
            return

        while True:
            try:
                engine = get_engine_health_blocking()
                with engine.begin() as conn:
                    dbapi_conn = getattr(conn.connection, "driver_connection", None)
                    if dbapi_conn is not None and psycopg2 is not None:
                        cur = dbapi_conn.cursor()
                        sql = f"""
                            INSERT INTO {HEALTH_SCHEMA}.{HEALTH_TABLE}
                            (end_day, end_time, info, contents)
                            VALUES %s
                        """
                        values = [
                            (r["end_day"], r["end_time"], r["info"], r["contents"])
                            for r in batch
                        ]
                        psycopg2.extras.execute_values(cur, sql, values, page_size=1000)  # type: ignore
                    else:
                        conn.execute(
                            text(
                                f"""
                                INSERT INTO {HEALTH_SCHEMA}.{HEALTH_TABLE}
                                (end_day, end_time, info, contents)
                                VALUES
                                (:end_day, :end_time, :info, :contents)
                                """
                            ),
                            batch,
                        )
                return

            except Exception as e:
                ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [WARN] health log flush failed: {type(e).__name__}: {e}", flush=True)
                if _is_connection_error(e):
                    _dispose_engine_health()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    def _drain_queue(self, max_items: int):
        drained = 0
        while drained < max_items:
            try:
                item = db_log_queue.get_nowait()
                self._local_buffer.append(item)
                drained += 1
            except queue.Empty:
                break

    def _run(self):
        last_flush_ts = time_mod.time()

        while not self.stop_event.is_set():
            try:
                timeout = max(0.2, DB_LOG_FLUSH_INTERVAL_SEC / 2.0)

                try:
                    item = db_log_queue.get(timeout=timeout)
                    self._local_buffer.append(item)
                except queue.Empty:
                    pass

                self._drain_queue(max_items=max(0, DB_LOG_BATCH_SIZE - len(self._local_buffer)))

                now_ts = time_mod.time()
                need_flush = False

                if len(self._local_buffer) >= DB_LOG_BATCH_SIZE:
                    need_flush = True
                elif self._local_buffer and (now_ts - last_flush_ts) >= DB_LOG_FLUSH_INTERVAL_SEC:
                    need_flush = True

                if need_flush:
                    batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                    del self._local_buffer[:len(batch)]
                    self._flush_batch(batch)
                    last_flush_ts = now_ts

            except Exception as e:
                ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
                print(f"[{ts}] [WARN] HealthLogWorker loop error: {type(e).__name__}: {e}", flush=True)
                time_mod.sleep(1.0)

        try:
            while True:
                try:
                    item = db_log_queue.get_nowait()
                    self._local_buffer.append(item)
                except queue.Empty:
                    break

            while self._local_buffer:
                batch = self._local_buffer[:DB_LOG_BATCH_SIZE]
                del self._local_buffer[:len(batch)]
                self._flush_batch(batch)

        except Exception as e:
            ts = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{ts}] [WARN] HealthLogWorker final flush error: {type(e).__name__}: {e}", flush=True)


# =========================================================
# Window 계산
# =========================================================
def calc_window(now_kst: datetime) -> Tuple[str, str, datetime, datetime]:
    t = now_kst.time()
    day_start = dtime(8, 30, 0)
    day_end = dtime(20, 29, 59)
    night_start = dtime(20, 30, 0)

    if day_start <= t <= day_end:
        ws = now_kst.replace(hour=8, minute=30, second=0, microsecond=0)
        return "day", ws.strftime("%Y%m%d"), ws, now_kst

    if t >= night_start:
        ws = now_kst.replace(hour=20, minute=30, second=0, microsecond=0)
        return "night", ws.strftime("%Y%m%d"), ws, now_kst

    yday = (now_kst - timedelta(days=1)).date()
    ws = datetime(yday.year, yday.month, yday.day, 20, 30, 0, tzinfo=KST)
    return "night", ws.strftime("%Y%m%d"), ws, now_kst


# =========================================================
# station별 마지막 저장 to_ts 조회
# 현재 window 안에서만 station별 최신 1건 조회
# =========================================================
def read_last_pk_per_station(engine, ws: datetime, we: datetime) -> Dict[str, Optional[datetime]]:
    out: Dict[str, Optional[datetime]] = {st: None for _, st in SRC_TABLES}

    sql = text(
        f"""
        SELECT DISTINCT ON (station)
               station,
               to_ts
        FROM {SCHEMA}.{SAVE_TABLE}
        WHERE station IS NOT NULL
          AND to_ts IS NOT NULL
          AND to_ts >= :ws
          AND to_ts <= :we
        ORDER BY station, to_ts DESC
        """
    )

    params = {
        "ws": to_naive_kst(ws),
        "we": to_naive_kst(we),
    }

    while True:
        try:
            with engine.connect() as conn:
                rows = conn.execute(sql, params).fetchall()

            for station, to_ts in rows:
                if station in out and to_ts is not None:
                    if to_ts.tzinfo is None:
                        to_ts = to_ts.replace(tzinfo=KST)
                    out[station] = to_ts
            return out

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] read_last_pk_per_station conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] read_last_pk_per_station", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue

            log("[DB][RETRY] read_last_pk_per_station failed", info="down")
            log_exc("[DB][RETRY] read_last_pk_per_station", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_main_blocking()


# =========================================================
# Source fetch
# =========================================================
def fetch_src_logs(
    engine,
    table: str,
    station: str,
    ws: datetime,
    we: datetime,
    cursor_ts: datetime,
    overlap_sec: int = CURSOR_OVERLAP_SEC,
) -> List[Dict[str, Any]]:
    cursor_eff = cursor_ts - timedelta(seconds=int(overlap_sec))

    ws_day = ws.astimezone(KST).strftime("%Y%m%d")
    we_day = we.astimezone(KST).strftime("%Y%m%d")
    ws_time = ws.astimezone(KST).strftime("%H:%M:%S")
    we_time = we.astimezone(KST).strftime("%H:%M:%S")
    cursor_day = cursor_eff.astimezone(KST).strftime("%Y%m%d")
    cursor_time = cursor_eff.astimezone(KST).strftime("%H:%M:%S")

    params = {
        "ws_day": ws_day,
        "we_day": we_day,
        "ws_time": ws_time,
        "we_time": we_time,
        "cursor_day": cursor_day,
        "cursor_time": cursor_time,
    }

    sql = text(
        f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {SCHEMA}."{table}"
        WHERE
            end_day BETWEEN :ws_day AND :we_day

            AND (
                end_day > :cursor_day
                OR (end_day = :cursor_day AND split_part(end_time, '.', 1) >= :cursor_time)
            )

            AND (
                end_day > :ws_day
                OR (end_day = :ws_day AND split_part(end_time, '.', 1) >= :ws_time)
            )

            AND (
                end_day < :we_day
                OR (end_day = :we_day AND split_part(end_time, '.', 1) <= :we_time)
            )

            AND (
                contents ILIKE '%제품 감지 NG%'
                OR contents ILIKE '%제품 검사 투입요구 ON%'
                OR contents ILIKE '%Manual mode 전환%'
                OR contents ILIKE '%Auto mode 전환%'
            )
        ORDER BY
            end_day ASC,
            end_time ASC
        """
    )

    while True:
        try:
            with engine.connect() as conn:
                rows = conn.execute(sql, params).fetchall()

            dedup = {}
            for end_day, end_time, contents in rows:
                key = (str(end_day), str(end_time), str(contents))
                dedup[key] = {
                    "end_day": str(end_day),
                    "end_time": str(end_time),
                    "contents": str(contents),
                    "contents_norm": norm_text(contents),
                    "station": station,
                    "end_ts": to_ts_kst(str(end_day), str(end_time)),
                }

            out = [v for v in dedup.values() if v["end_ts"] is not None]
            out.sort(key=lambda x: x["end_ts"])
            return out

        except Exception as e:
            if _is_connection_error(e):
                log(f"[DB][RETRY] fetch_src_logs conn error -> rebuild table={table} station={station}", info="down")
                log_exc("[DB][RETRY] fetch_src_logs", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue

            log(f"[DB][RETRY] fetch_src_logs failed table={table} station={station}", info="down")
            log_exc("[DB][RETRY] fetch_src_logs", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = get_engine_main_blocking()


# =========================================================
# Event pairing
# =========================================================
@dataclass
class StationState:
    in_manual: bool = False
    pending_ng_ts: Optional[datetime] = None


def pair_events(rows: List[Dict[str, Any]], state: StationState) -> Tuple[List[Dict[str, Any]], StationState]:
    pairs: List[Dict[str, Any]] = []

    for r in rows:
        c = r["contents_norm"]
        ts: datetime = r["end_ts"]
        station = r["station"]

        if is_manual(c):
            state.in_manual = True
            continue

        if is_auto(c):
            state.in_manual = False
            continue

        if is_ng(c):
            if (not state.in_manual) and (state.pending_ng_ts is None):
                state.pending_ng_ts = ts
            continue

        if is_off(c):
            if state.pending_ng_ts is not None:
                from_ts = state.pending_ng_ts
                to_ts = ts

                if to_ts >= from_ts:
                    pairs.append(
                        {
                            "end_day": from_ts.astimezone(KST).strftime("%Y%m%d"),
                            "station": station,
                            "from_contents": NG_PHRASE_RAW,
                            "from_time": format_hhmmss_ff(from_ts),
                            "to_contents": OFF_PHRASE_RAW,
                            "to_time": format_hhmmss_ff(to_ts),
                            "to_ts": to_naive_kst(to_ts),
                            "wasted_time": round((to_ts - from_ts).total_seconds(), 2),
                        }
                    )

                state.pending_ng_ts = None

    return pairs, state


# =========================================================
# Save insert
# =========================================================
def insert_rows(engine, rows: List[Dict[str, Any]]) -> int:
    if not rows:
        return 0

    sql = text(
        f"""
        INSERT INTO {SCHEMA}.{SAVE_TABLE}
            (end_day, station, from_contents, from_time, to_contents, to_time, to_ts, wasted_time)
        VALUES
            (:end_day, :station, :from_contents, :from_time, :to_contents, :to_time, :to_ts, :wasted_time)
        ON CONFLICT (end_day, station, from_time, to_time)
        DO NOTHING
        """
    )

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(sql, rows)
            return len(rows)

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] insert_rows conn error -> rebuild", info="down")
                log_exc("[DB][RETRY] insert_rows", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_main_blocking()
                continue

            log("[ERROR] insert_rows failed", info="error")
            log_exc("[ERROR] insert_rows", e)
            return 0


# =========================================================
# Warm-start Backfill
# =========================================================
def run_backfill_for_window(engine, ws: datetime, we: datetime, states: Dict[str, StationState]) -> None:
    log(f"[INFO] [backfill] start window={ws:%Y-%m-%d %H:%M:%S}~{we:%Y-%m-%d %H:%M:%S}", info="info")

    total_fetched = 0
    total_pairs = 0
    total_ins = 0

    for table, station in SRC_TABLES:
        rows = fetch_src_logs(engine, table, station, ws, we, cursor_ts=ws, overlap_sec=0)
        fetched = len(rows)
        total_fetched += fetched
        log(f"[INFO] [backfill][fetch] {station} rows={fetched}", info="info")

        if fetched == 0:
            continue

        pairs, states[station] = pair_events(rows, states[station])
        pcount = len(pairs)
        total_pairs += pcount

        log(
            f"[INFO] [backfill][pair] {station} pairs={pcount} "
            f"pending_ng={'y' if states[station].pending_ng_ts else 'n'} "
            f"in_manual={'y' if states[station].in_manual else 'n'}",
            info="info",
        )

        ins_n = insert_rows(engine, pairs) if pcount > 0 else 0
        total_ins += ins_n
        log(f"[INFO] [backfill][ins] {station} attempted_insert={ins_n}", info="info")

    log(
        f"[INFO] [backfill] done fetched={total_fetched} pairs={total_pairs} attempted_insert={total_ins}",
        info="info",
    )


# =========================================================
# main
# =========================================================
def main():
    log("[BOOT] d4_afa_fail_wasted_time realtime starting", info="boot")
    log(f"[INFO] MAIN DB = {_masked_db_info(DB_CONFIG)}", info="info")
    log(f"[INFO] save = {SCHEMA}.{SAVE_TABLE}", info="info")
    log(f"[INFO] health = {HEALTH_SCHEMA}.{HEALTH_TABLE}", info="info")
    log(f"[INFO] work_mem={WORK_MEM}", info="info")
    log(
        f"[INFO] stmt_timeout_ms={STMT_TIMEOUT_MS} lock_timeout_ms={LOCK_TIMEOUT_MS} "
        f"idle_tx_timeout_ms={IDLE_TX_TIMEOUT_MS}",
        info="info",
    )
    log(f"[INFO] cursor_overlap_sec={CURSOR_OVERLAP_SEC}", info="info")
    log(f"[INFO] health queue max={DB_LOG_QUEUE_MAX} batch={DB_LOG_BATCH_SIZE}", info="info")
    log(f"[INFO] traceback_max_lines={TRACEBACK_MAX_LINES}", info="info")

    engine_main = get_engine_main_blocking()
    engine_health = get_engine_health_blocking()
    ensure_schema_and_tables(engine_main, engine_health)

    health_worker = HealthLogWorker()
    health_worker.start()
    log("[INFO] HealthLogWorker started", info="info")

    states: Dict[str, StationState] = {st: StationState() for _, st in SRC_TABLES}
    last_backfill_key: Optional[Tuple[str, datetime]] = None

    while True:
        loop_t0 = time_mod.perf_counter()

        try:
            now_kst = datetime.now(tz=KST)
            shift, prod_day, ws, we = calc_window(now_kst)

            engine_main = get_engine_main_blocking()

            backfill_key = (shift, ws)
            if last_backfill_key != backfill_key:
                states = {st: StationState() for _, st in SRC_TABLES}
                run_backfill_for_window(engine_main, ws, we, states)
                last_backfill_key = backfill_key

            # 현재 window 내에서만 last to_ts 조회
            last_to_ts = read_last_pk_per_station(engine_main, ws, we)

            last_pk_str = ", ".join(
                [f"{k}={(v.strftime('%Y-%m-%d %H:%M:%S') if v else 'None')}" for k, v in last_to_ts.items()]
            )
            log(
                f"[INFO] [last_pk] shift={shift} prod_day={prod_day} "
                f"window={ws:%Y-%m-%d %H:%M:%S}~{we:%Y-%m-%d %H:%M:%S} | {last_pk_str}",
                info="info",
            )

            total_fetched = 0
            total_pairs = 0
            total_attempted = 0

            for table, station in SRC_TABLES:
                cursor_base = last_to_ts.get(station) or ws
                if cursor_base < ws:
                    cursor_base = ws

                rows = fetch_src_logs(
                    engine_main,
                    table,
                    station,
                    ws,
                    we,
                    cursor_base,
                    overlap_sec=CURSOR_OVERLAP_SEC,
                )
                fetched = len(rows)
                total_fetched += fetched
                log(f"[INFO] [fetch] {station} cursor={cursor_base:%Y-%m-%d %H:%M:%S} rows={fetched}", info="info")

                if fetched == 0:
                    continue

                pairs, states[station] = pair_events(rows, states[station])
                pcount = len(pairs)
                total_pairs += pcount

                log(
                    f"[INFO] [pair] {station} pairs={pcount} "
                    f"pending_ng={'y' if states[station].pending_ng_ts else 'n'} "
                    f"in_manual={'y' if states[station].in_manual else 'n'}",
                    info="info",
                )

                attempted = insert_rows(engine_main, pairs) if pcount > 0 else 0
                total_attempted += attempted
                log(f"[INFO] [ins] {station} attempted_insert={attempted}", info="info")

            loop_t1 = time_mod.perf_counter()
            log(
                f"[PERF] shift={shift} prod_day={prod_day} "
                f"fetched={total_fetched} pairs={total_pairs} attempted_insert={total_attempted} "
                f"loop={loop_t1 - loop_t0:.3f}s qsize={db_log_queue.qsize()}",
                info="perf",
            )

        except KeyboardInterrupt:
            log("[STOP] Interrupted by user", info="stop")
            try:
                health_worker.stop()
                health_worker.join(timeout=15.0)
                log("[INFO] HealthLogWorker stopped", info="info")
            except Exception as e:
                log_exc("[WARN] HealthLogWorker stop failed", e)
            break

        except Exception as e:
            if _is_connection_error(e):
                log("[DB][RETRY] loop-level conn error -> rebuild main engine", info="down")
                log_exc("[DB][RETRY] loop-level", e)
                _dispose_engine_main()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                engine_main = get_engine_main_blocking()
            else:
                log("[ERROR] Loop error continue", info="error")
                log_exc("[ERROR] Loop error", e)

        elapsed = time_mod.perf_counter() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[STOP] user interrupted Ctrl+C", info="stop")
    except Exception as e:
        log("[UNHANDLED] fatal error", info="error")
        log_exc("[UNHANDLED]", e)
        raise