# -*- coding: utf-8 -*-
"""
g_inspect_vision_non_operation_daemon.py

[기능 유지]
- 소스: g_production_film.fct_non_operation_time
- 대상 station:
  - (FCT1, FCT2) 교집합 -> Vision1
  - (FCT3, FCT4) 교집합 -> Vision2
- 교집합 판정:
  overlap_start = max(a.from, b.from)
  overlap_end   = min(a.to, b.to)
  overlap_start < overlap_end 이면 유효
- 저장:
  g_production_film.vision_non_operation_time
  (end_day, station, from_time, to_time) PK UPSERT
  no_operation_time = 교집합 초(소수 둘째자리)

[운영 기준]
- stable cut 구조 유지
- 신규 PK 즉시 upsert
- 값 변경 PK만 update
- 값 같으면 update 안 함
- today + yesterday 2일 처리 유지

[재실행 복구]
- vision_non_operation_time 의 station별 최신 바로 이전 행 기준
- end_day + to_time - 30분 부터만 다시 계산
- 첫 루프 bootstrap 후에는 DB의 기존 today/yesterday 상태를 읽어 seen_state를 채움
- 이후 루프에서 과거 PK 대량 delta 재발 방지
"""

from __future__ import annotations

import os
import signal
import time
import traceback
import urllib.parse
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

import psycopg2
from psycopg2.extras import execute_values
from zoneinfo import ZoneInfo


# -----------------------------
# 설정
# -----------------------------
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

DAEMON_NAME = "g_inspect_vision_non_operation_daemon"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "gv_log"

HEARTBEAT_SCHEMA = "k_demon_heath_check"
HEARTBEAT_TABLE = "daemon_heartbeat"

SRC_SCHEMA = "g_production_film"
SRC_TABLE = "fct_non_operation_time"

SAVE_SCHEMA = "g_production_film"
SAVE_TABLE = "vision_non_operation_time"

LOOP_SEC = 2
RETRY_SEC = 5
STABLE_DATA_SEC = 1.0
RESTART_LOOKBACK_MINUTES = 30

PG_WORK_MEM = (os.getenv("PG_WORK_MEM", "4MB") or "4MB").strip()
APP_NAME = DAEMON_NAME
ADVISORY_LOCK_KEY = 2026021102

DB_HEARTBEAT_INTERVAL_SEC = 60.0
IDLE_LOG_INTERVAL_SEC = 60.0

KST = ZoneInfo("Asia/Seoul")
_shutdown_requested = False
_last_db_heartbeat_epoch = 0.0
_last_idle_log_epoch = 0.0


# -----------------------------
# 시그널
# -----------------------------
def _signal_handler(signum, _frame):
    global _shutdown_requested
    _shutdown_requested = True
    print(f"{now_str()} [INFO] shutdown signal received: {signum}", flush=True)


signal.signal(signal.SIGINT, _signal_handler)
if hasattr(signal, "SIGTERM"):
    signal.signal(signal.SIGTERM, _signal_handler)


# -----------------------------
# 공통 유틸
# -----------------------------
def now_kst() -> datetime:
    return datetime.now(tz=KST)


def now_str() -> str:
    return now_kst().strftime("%Y-%m-%d %H:%M:%S")


def p(level: str, msg: str) -> None:
    print(f"{now_str()} [{level}] {msg}", flush=True)


def current_window() -> Tuple[str, str]:
    n = now_kst()
    return n.strftime("%Y%m%d"), n.strftime("%H:%M:%S")


def ymd_add(ymd: str, days: int) -> str:
    dt = datetime.strptime(ymd, "%Y%m%d")
    dt2 = dt + timedelta(days=days)
    return dt2.strftime("%Y%m%d")


def normalize_info(info: str) -> str:
    return (str(info).strip().lower() or "info")


def should_write_db_log(info: str) -> bool:
    global _last_db_heartbeat_epoch

    info_n = normalize_info(info)
    allowed = {"boot", "error", "recover", "save", "fatal", "exit", "heartbeat", "start", "stop"}

    if info_n not in allowed:
        return False

    if info_n == "heartbeat":
        now_epoch = time.time()
        if now_epoch - _last_db_heartbeat_epoch < DB_HEARTBEAT_INTERVAL_SEC:
            return False
        _last_db_heartbeat_epoch = now_epoch

    return True


def parse_dt(end_day: str, hms: str) -> pd.Timestamp:
    d = str(end_day).strip()
    t = str(hms).strip()

    ts = pd.to_datetime(f"{d} {t}", format="%Y%m%d %H:%M:%S.%f", errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime(f"{d} {t}", format="%Y%m%d %H:%M:%S", errors="coerce")
    return ts


def _format_cs(ts: pd.Timestamp, cs: int) -> str:
    base = ts.strftime("%H:%M:%S")
    return f"{base}.{cs:02d}"


def fmt_time_floor_cs(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return ""
    cs = int(ts.microsecond // 10000)
    return _format_cs(ts, cs)


def fmt_time_ceil_cs(ts: pd.Timestamp) -> str:
    if pd.isna(ts):
        return ""
    us = int(ts.microsecond)
    cs_floor = us // 10000
    rem = us % 10000
    cs = cs_floor if rem == 0 else cs_floor + 1

    if cs >= 100:
        ts2 = ts + pd.Timedelta(seconds=1)
        cs = 0
        return _format_cs(ts2, cs)

    return _format_cs(ts, cs)


def pk_tuple(end_day: str, station: str, from_time: str, to_time: str) -> Tuple[str, str, str, str]:
    return (str(end_day), str(station), str(from_time), str(to_time))


def pk_of_row(r) -> Tuple[str, str, str, str]:
    return pk_tuple(r["end_day"], r["station"], r["from_time"], r["to_time"])


def value_of_row(r) -> Optional[float]:
    v = r["no_operation_time"]
    if pd.isna(v):
        return None
    return round(float(v), 2)


def df_last_pk_for_display(df: pd.DataFrame) -> Optional[Tuple[str, str, str, str]]:
    if df.empty:
        return None
    dpk = df[["end_day", "station", "from_time", "to_time"]].copy()
    dpk = dpk.sort_values(["end_day", "station", "from_time", "to_time"]).reset_index(drop=True)
    lr = dpk.iloc[-1]
    return (str(lr["end_day"]), str(lr["station"]), str(lr["from_time"]), str(lr["to_time"]))


# -----------------------------
# DB 엔진/연결
# -----------------------------
def build_db_url() -> str:
    pw = urllib.parse.quote_plus(DB_CONFIG["password"])
    return (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{pw}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )


def build_engine() -> Engine:
    engine = create_engine(
        build_db_url(),
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_recycle=1800,
        connect_args={
            "application_name": APP_NAME,
            "options": (
                "-c lock_timeout=5000 "
                "-c statement_timeout=120000 "
                "-c idle_in_transaction_session_timeout=60000"
            ),
        },
        future=True,
    )

    @event.listens_for(engine, "connect")
    def _set_work_mem(dbapi_conn, _):
        try:
            with dbapi_conn.cursor() as cur:
                cur.execute(f"SET work_mem TO '{PG_WORK_MEM}'")
        except Exception:
            pass

    return engine


def connect_blocking() -> Engine:
    while True:
        if _shutdown_requested:
            raise KeyboardInterrupt("shutdown requested during connect")
        try:
            eng = build_engine()
            with eng.connect() as conn:
                conn.execute(text("SELECT 1"))
                conn.execute(text(f"SET work_mem TO '{PG_WORK_MEM}'"))
            p("INFO", f"DB connected (work_mem={PG_WORK_MEM})")
            return eng
        except Exception as e:
            p("RETRY", f"DB connect failed: {repr(e)}; retry in {RETRY_SEC}s")
            time.sleep(RETRY_SEC)


# -----------------------------
# 테이블 보장
# -----------------------------
def ensure_log_table(conn) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA}"))
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
                end_day  TEXT NOT NULL,
                end_time TEXT NOT NULL,
                info     TEXT NOT NULL,
                contents TEXT
            )
            """
        )
    )


def ensure_heartbeat_table(conn) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {HEARTBEAT_SCHEMA}"))
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} (
                daemon_name TEXT PRIMARY KEY,
                status TEXT NOT NULL,
                end_day TEXT,
                last_pk_end_day TEXT,
                last_pk_station TEXT,
                last_pk_from_time TEXT,
                last_pk_to_time TEXT,
                last_message TEXT,
                last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
    )
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS last_pk_end_day TEXT"))
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS last_pk_station TEXT"))
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS last_pk_from_time TEXT"))
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS last_pk_to_time TEXT"))
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS last_message TEXT"))
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMPTZ"))
    conn.execute(text(f"ALTER TABLE {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ"))


def ensure_save_table(conn) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA}"))
    conn.execute(
        text(
            f"""
            CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{SAVE_TABLE} (
                end_day TEXT NOT NULL,
                station TEXT NOT NULL,
                from_time TEXT NOT NULL,
                to_time TEXT NOT NULL,
                no_operation_time NUMERIC(12,2),
                created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (end_day, station, from_time, to_time)
            )
            """
        )
    )
    conn.execute(text(f"ALTER TABLE {SAVE_SCHEMA}.{SAVE_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()"))
    conn.execute(text(f"ALTER TABLE {SAVE_SCHEMA}.{SAVE_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()"))


# -----------------------------
# 로그/heartbeat
# -----------------------------
def log_db(engine: Engine, info: str, contents: str) -> None:
    info_n = normalize_info(info)
    if not should_write_db_log(info_n):
        return

    n = now_kst()
    payload = {
        "end_day": n.strftime("%Y%m%d"),
        "end_time": n.strftime("%H:%M:%S"),
        "info": info_n,
        "contents": str(contents),
    }

    sql = text(
        f"""
        INSERT INTO {LOG_SCHEMA}.{LOG_TABLE}
        (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
        """
    )

    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout='2s'"))
            conn.execute(text("SET LOCAL statement_timeout='3s'"))
            conn.execute(sql, payload)
    except Exception as e:
        p("WARN", f"log_db skipped: {repr(e)}")


def maybe_log_idle(engine: Optional[Engine], message: str) -> None:
    global _last_idle_log_epoch

    now_epoch = time.time()
    if now_epoch - _last_idle_log_epoch < IDLE_LOG_INTERVAL_SEC:
        return

    _last_idle_log_epoch = now_epoch
    p("INFO", message)

    try:
        if engine is not None:
            log_db(engine, "heartbeat", message)
    except Exception:
        pass


def heartbeat_upsert(
    engine: Engine,
    status: str,
    end_day: Optional[str],
    last_pk: Optional[Tuple[str, str, str, str]],
    message: str,
) -> None:
    pk_day = pk_station = pk_from = pk_to = None
    if last_pk is not None:
        pk_day, pk_station, pk_from, pk_to = last_pk

    sql = text(
        f"""
        INSERT INTO {HEARTBEAT_SCHEMA}.{HEARTBEAT_TABLE}
        (daemon_name, status, end_day, last_pk_end_day, last_pk_station, last_pk_from_time, last_pk_to_time, last_message, last_seen_at, updated_at)
        VALUES
        (:daemon_name, :status, :end_day, :pk_day, :pk_station, :pk_from, :pk_to, :msg, now(), now())
        ON CONFLICT (daemon_name)
        DO UPDATE SET
            status = EXCLUDED.status,
            end_day = EXCLUDED.end_day,
            last_pk_end_day = EXCLUDED.last_pk_end_day,
            last_pk_station = EXCLUDED.last_pk_station,
            last_pk_from_time = EXCLUDED.last_pk_from_time,
            last_pk_to_time = EXCLUDED.last_pk_to_time,
            last_message = EXCLUDED.last_message,
            last_seen_at = now(),
            updated_at = now()
        """
    )

    params = {
        "daemon_name": DAEMON_NAME,
        "status": status,
        "end_day": end_day,
        "pk_day": pk_day,
        "pk_station": pk_station,
        "pk_from": pk_from,
        "pk_to": pk_to,
        "msg": (message or "")[:1000],
    }

    try:
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL lock_timeout='2s'"))
            conn.execute(text("SET LOCAL statement_timeout='3s'"))
            conn.execute(sql, params)
    except Exception as e:
        p("WARN", f"heartbeat skipped: {repr(e)}")


# -----------------------------
# advisory lock
# -----------------------------
def acquire_singleton_lock_blocking():
    while True:
        if _shutdown_requested:
            raise KeyboardInterrupt("shutdown requested during advisory lock acquire")

        conn = None
        try:
            conn = psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                dbname=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                application_name=f"{APP_NAME}_lock",
            )
            conn.autocommit = True

            with conn.cursor() as cur:
                cur.execute("SET lock_timeout = '5s'")
                cur.execute("SET statement_timeout = '10s'")
                cur.execute("SELECT pg_try_advisory_lock(%s)", (ADVISORY_LOCK_KEY,))
                got = cur.fetchone()[0]
                if got:
                    p("INFO", "advisory lock acquired")
                    return conn

            try:
                conn.close()
            except Exception:
                pass

            p("RETRY", "another instance already running; waiting advisory lock...")
            slept = 0
            while slept < RETRY_SEC and not _shutdown_requested:
                time.sleep(1)
                slept += 1

        except Exception as e:
            p("WARN", f"advisory lock acquire failed: {repr(e)}")
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

            slept = 0
            while slept < RETRY_SEC and not _shutdown_requested:
                time.sleep(1)
                slept += 1


def release_singleton_lock(lock_conn) -> None:
    if lock_conn is None:
        return
    try:
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_KEY,))
    except Exception:
        pass
    try:
        lock_conn.close()
    except Exception:
        pass


# -----------------------------
# 재시작 anchor 조회
# -----------------------------
def load_restart_anchors(engine: Engine) -> Dict[str, Optional[pd.Timestamp]]:
    out: Dict[str, Optional[pd.Timestamp]] = {"Vision1": None, "Vision2": None}

    for station in ("Vision1", "Vision2"):
        q = text(f"""
            WITH ranked AS (
                SELECT
                    end_day,
                    to_time,
                    row_number() OVER (
                        PARTITION BY station
                        ORDER BY
                            to_timestamp(end_day || ' ' || to_time, 'YYYYMMDD HH24:MI:SS.MS') DESC
                    ) AS rn
                FROM {SAVE_SCHEMA}.{SAVE_TABLE}
                WHERE station = :station
            )
            SELECT end_day, to_time
            FROM ranked
            WHERE rn = 2
        """)
        with engine.connect() as conn:
            row = conn.execute(q, {"station": station}).mappings().first()

        if row:
            ts = parse_dt(row["end_day"], row["to_time"])
            if pd.notna(ts):
                out[station] = ts - pd.Timedelta(minutes=RESTART_LOOKBACK_MINUTES)

    return out


def load_existing_state_for_days(
    engine: Engine,
    days: List[str],
) -> Dict[str, Dict[Tuple[str, str, str, str], Optional[float]]]:
    out: Dict[str, Dict[Tuple[str, str, str, str], Optional[float]]] = {}
    for d in days:
        q = text(f"""
            SELECT end_day, station, from_time, to_time, no_operation_time
            FROM {SAVE_SCHEMA}.{SAVE_TABLE}
            WHERE end_day = :end_day
              AND station IN ('Vision1', 'Vision2')
        """)
        with engine.connect() as conn:
            rows = conn.execute(q, {"end_day": d}).mappings().all()

        state: Dict[Tuple[str, str, str, str], Optional[float]] = {}
        for r in rows:
            pk = pk_tuple(r["end_day"], r["station"], r["from_time"], r["to_time"])
            val = r["no_operation_time"]
            state[pk] = None if val is None else round(float(val), 2)
        out[d] = state
    return out


# -----------------------------
# 소스 로드
# -----------------------------
def load_fct_day(engine: Engine, end_day: str) -> pd.DataFrame:
    sql = text(
        f"""
        SELECT end_day, station, from_time, to_time
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE end_day = :end_day
          AND station IN ('FCT1','FCT2','FCT3','FCT4')
          AND from_time IS NOT NULL
          AND to_time IS NOT NULL
        ORDER BY station, from_time, to_time
        """
    )
    with engine.connect() as conn:
        df = pd.read_sql(sql, conn, params={"end_day": end_day})
    return df


def cap_df_by_ts(
    df: pd.DataFrame,
    end_day: str,
    cap_ts: pd.Timestamp,
    min_ts: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    if df.empty:
        return df

    tmp = df.copy()
    tmp["from_ts"] = [parse_dt(end_day, x) for x in tmp["from_time"]]
    tmp["to_ts"] = [parse_dt(end_day, x) for x in tmp["to_time"]]

    tmp = tmp[
        (tmp["from_ts"].notna()) &
        (tmp["to_ts"].notna()) &
        (tmp["from_ts"] <= tmp["to_ts"])
    ].copy()

    tmp = tmp[tmp["to_ts"] <= cap_ts].copy()

    if min_ts is not None:
        tmp = tmp[tmp["to_ts"] >= min_ts].copy()

    return tmp[["end_day", "station", "from_time", "to_time"]].copy()


# -----------------------------
# 교집합 계산
# -----------------------------
def build_intersections(
    df_day: pd.DataFrame,
    end_day: str,
    left_station: str,
    right_station: str,
    out_station: str,
) -> List[dict]:
    if df_day.empty:
        return []

    ldf = df_day[df_day["station"] == left_station].copy()
    rdf = df_day[df_day["station"] == right_station].copy()
    if ldf.empty or rdf.empty:
        return []

    ldf["from_ts"] = [parse_dt(end_day, x) for x in ldf["from_time"]]
    ldf["to_ts"] = [parse_dt(end_day, x) for x in ldf["to_time"]]
    rdf["from_ts"] = [parse_dt(end_day, x) for x in rdf["from_time"]]
    rdf["to_ts"] = [parse_dt(end_day, x) for x in rdf["to_time"]]

    ldf = ldf[(ldf["from_ts"].notna()) & (ldf["to_ts"].notna()) & (ldf["from_ts"] <= ldf["to_ts"])].copy()
    rdf = rdf[(rdf["from_ts"].notna()) & (rdf["to_ts"].notna()) & (rdf["from_ts"] <= rdf["to_ts"])].copy()
    if ldf.empty or rdf.empty:
        return []

    ldf = ldf.sort_values(["from_ts", "to_ts"]).reset_index(drop=True)
    rdf = rdf.sort_values(["from_ts", "to_ts"]).reset_index(drop=True)

    out_rows: List[dict] = []

    for _, la in ldf.iterrows():
        a_from = la["from_ts"]
        a_to = la["to_ts"]

        cand = rdf[(rdf["to_ts"] >= a_from) & (rdf["from_ts"] <= a_to)]
        if cand.empty:
            continue

        for _, rb in cand.iterrows():
            b_from = rb["from_ts"]
            b_to = rb["to_ts"]

            ov_from = a_from if a_from >= b_from else b_from
            ov_to = a_to if a_to <= b_to else b_to

            if not (ov_from < ov_to):
                continue

            diff_sec = float((ov_to - ov_from).total_seconds())
            diff_sec_2 = round(diff_sec, 2)
            if diff_sec_2 <= 0.0:
                continue

            out_rows.append(
                {
                    "end_day": str(end_day),
                    "station": out_station,
                    "from_time": fmt_time_floor_cs(ov_from),
                    "to_time": fmt_time_ceil_cs(ov_to),
                    "no_operation_time": diff_sec_2,
                }
            )

    return out_rows


def compute_vision_rows_from_fct(df_fct: pd.DataFrame, end_day: str) -> pd.DataFrame:
    if df_fct.empty:
        return pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    rows_v1 = build_intersections(df_fct, end_day, "FCT1", "FCT2", "Vision1")
    rows_v2 = build_intersections(df_fct, end_day, "FCT3", "FCT4", "Vision2")

    rows = rows_v1 + rows_v2
    if not rows:
        return pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = pd.DataFrame(rows, columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])
    df["end_day"] = df["end_day"].astype(str)
    df["station"] = df["station"].astype(str)
    df["from_time"] = df["from_time"].astype(str)
    df["to_time"] = df["to_time"].astype(str)
    df["no_operation_time"] = pd.to_numeric(df["no_operation_time"], errors="coerce").round(2)

    df = df.drop_duplicates(subset=["end_day", "station", "from_time", "to_time"], keep="last").reset_index(drop=True)
    return df


# -----------------------------
# delta 계산
# -----------------------------
def build_delta_rows(
    df_out: pd.DataFrame,
    state_map: Dict[Tuple[str, str, str, str], Optional[float]],
) -> List[dict]:
    if df_out.empty:
        return []

    delta_rows: List[dict] = []

    dpk = df_out.sort_values(["end_day", "station", "from_time", "to_time"]).reset_index(drop=True)
    for _, r in dpk.iterrows():
        pk = pk_of_row(r)
        new_val = value_of_row(r)
        old_val = state_map.get(pk, "__MISSING__")

        if old_val == "__MISSING__" or old_val != new_val:
            delta_rows.append(
                {
                    "end_day": str(r["end_day"]),
                    "station": str(r["station"]),
                    "from_time": str(r["from_time"]),
                    "to_time": str(r["to_time"]),
                    "no_operation_time": new_val,
                }
            )

    return delta_rows


def snapshot_state_from_df(
    df_out: pd.DataFrame,
) -> Dict[Tuple[str, str, str, str], Optional[float]]:
    state: Dict[Tuple[str, str, str, str], Optional[float]] = {}
    if df_out.empty:
        return state

    for _, r in df_out.iterrows():
        state[pk_of_row(r)] = value_of_row(r)
    return state


# -----------------------------
# 저장
# -----------------------------
def upsert_rows(
    rows: List[dict],
) -> Tuple[int, Dict[Tuple[str, str, str, str], Optional[float]]]:
    if not rows:
        return 0, {}

    conn = None
    done = 0
    saved_state: Dict[Tuple[str, str, str, str], Optional[float]] = {}

    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            dbname=DB_CONFIG["dbname"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            application_name=f"{APP_NAME}_upsert",
        )
        conn.autocommit = True

        with conn.cursor() as cur:
            cur.execute("SET lock_timeout = '5s'")
            cur.execute("SET statement_timeout = '120s'")
            cur.execute("SET idle_in_transaction_session_timeout = '60s'")
            cur.execute(f"SET work_mem TO '{PG_WORK_MEM}'")

            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA}")
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{SAVE_TABLE} (
                    end_day TEXT NOT NULL,
                    station TEXT NOT NULL,
                    from_time TEXT NOT NULL,
                    to_time TEXT NOT NULL,
                    no_operation_time NUMERIC(12,2),
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (end_day, station, from_time, to_time)
                )
                """
            )
            cur.execute(f"ALTER TABLE {SAVE_SCHEMA}.{SAVE_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()")
            cur.execute(f"ALTER TABLE {SAVE_SCHEMA}.{SAVE_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now()")

            sql = f"""
                INSERT INTO {SAVE_SCHEMA}.{SAVE_TABLE}
                (end_day, station, from_time, to_time, no_operation_time)
                VALUES %s
                ON CONFLICT (end_day, station, from_time, to_time)
                DO UPDATE SET
                    no_operation_time = EXCLUDED.no_operation_time,
                    updated_at = now()
                WHERE
                    {SAVE_SCHEMA}.{SAVE_TABLE}.no_operation_time IS DISTINCT FROM EXCLUDED.no_operation_time
            """

            vals = []
            state_list = []
            for r in rows:
                pk = pk_tuple(r["end_day"], r["station"], r["from_time"], r["to_time"])
                val = None if r["no_operation_time"] is None else round(float(r["no_operation_time"]), 2)
                state_list.append((pk, val))
                vals.append((
                    str(r["end_day"]),
                    str(r["station"]),
                    str(r["from_time"]),
                    str(r["to_time"]),
                    val,
                ))

            total = len(vals)
            page_size = 500

            for s in range(0, total, page_size):
                if _shutdown_requested:
                    p("INFO", "shutdown requested during upsert; stopping early")
                    break

                e = min(s + page_size, total)
                chunk = vals[s:e]
                chunk_state = state_list[s:e]

                execute_values(cur, sql, chunk, page_size=len(chunk))

                done += len(chunk)
                for pk, val in chunk_state:
                    saved_state[pk] = val

                p("INFO", f"upsert progress {done}/{total}")

        return done, saved_state

    finally:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass


# -----------------------------
# 메인
# -----------------------------
def run_daemon():
    global _last_idle_log_epoch

    p("BOOT", f"{DAEMON_NAME} starting")
    _last_idle_log_epoch = 0.0

    engine: Optional[Engine] = None
    lock_conn = None

    seen_state_by_day: Dict[str, Dict[Tuple[str, str, str, str], Optional[float]]] = {}
    last_pk: Optional[Tuple[str, str, str, str]] = None

    restart_anchor_by_station: Dict[str, Optional[pd.Timestamp]] = {"Vision1": None, "Vision2": None}
    first_loop_after_restart = True

    try:
        while True:
            if _shutdown_requested:
                p("INFO", "graceful shutdown requested")
                break

            try:
                if engine is None:
                    engine = connect_blocking()

                    with engine.begin() as conn:
                        ensure_log_table(conn)
                        ensure_heartbeat_table(conn)
                        ensure_save_table(conn)

                    if lock_conn is None:
                        lock_conn = acquire_singleton_lock_blocking()

                    restart_anchor_by_station = load_restart_anchors(engine)
                    p(
                        "INFO",
                        f"restart anchors: Vision1={restart_anchor_by_station['Vision1']} "
                        f"Vision2={restart_anchor_by_station['Vision2']}",
                    )

                    log_db(engine, "start", "daemon connected and ready")
                    heartbeat_upsert(engine, "running", None, last_pk, "connected and ready")

                today_ymd, now_time = current_window()
                yday_ymd = ymd_add(today_ymd, -1)

                now_ts_val = parse_dt(today_ymd, now_time)
                stable_today_ts = now_ts_val - pd.Timedelta(seconds=STABLE_DATA_SEC)
                cap_today = stable_today_ts
                cap_yday = parse_dt(yday_ymd, "23:59:59.99")

                target_days = [today_ymd, yday_ymd]

                loop_msgs: List[str] = []
                total_src = 0
                total_out = 0
                total_delta = 0
                total_saved = 0

                for d in target_days:
                    if _shutdown_requested:
                        break

                    cap_ts = cap_today if d == today_ymd else cap_yday

                    df_src_full = load_fct_day(engine, d)

                    min_ts_for_day: Optional[pd.Timestamp] = None
                    if first_loop_after_restart:
                        anchors = [x for x in restart_anchor_by_station.values() if x is not None]
                        if anchors:
                            min_anchor = min(anchors)
                            if min_anchor.strftime("%Y%m%d") == d:
                                min_ts_for_day = min_anchor

                    df_src = cap_df_by_ts(df_src_full, d, cap_ts, min_ts=min_ts_for_day)

                    src_n = len(df_src)
                    skipped_n = max(0, len(df_src_full) - src_n)
                    total_src += src_n

                    p(
                        "INFO",
                        f"[{d}] source rows={src_n} skipped_by_cap={skipped_n} "
                        f"(cap<= {cap_ts}, min_ts={min_ts_for_day})",
                    )

                    df_out = compute_vision_rows_from_fct(df_src, d)
                    out_n = len(df_out)
                    total_out += out_n
                    p("INFO", f"[{d}] computed output rows={out_n}")

                    disp = df_last_pk_for_display(df_out)
                    if disp is not None:
                        last_pk = disp

                    if d not in seen_state_by_day:
                        seen_state_by_day[d] = {}

                    if first_loop_after_restart:
                        rows = df_out.to_dict(orient="records")
                        p("INFO", f"[{d}] phase=bootstrap before_upsert rows={len(rows)}")
                        saved_cnt, saved_state = upsert_rows(rows)
                        p("INFO", f"[{d}] phase=bootstrap after_upsert saved={saved_cnt}")

                        total_saved += saved_cnt
                        loop_msgs.append(
                            f"[{d}] bootstrap computed={out_n}, saved={saved_cnt}"
                        )
                    else:
                        delta_rows = build_delta_rows(df_out, seen_state_by_day[d])
                        delta_n = len(delta_rows)
                        total_delta += delta_n

                        p("INFO", f"[{d}] phase=incremental before_upsert delta_rows={delta_n}")
                        saved_cnt, saved_state = upsert_rows(delta_rows) if delta_rows else (0, {})
                        p("INFO", f"[{d}] phase=incremental after_upsert saved={saved_cnt}")

                        seen_state_by_day[d].update(saved_state)
                        total_saved += saved_cnt
                        loop_msgs.append(
                            f"[{d}] inc computed={out_n}, delta={delta_n}, saved={saved_cnt}"
                        )

                if first_loop_after_restart:
                    existing_state = load_existing_state_for_days(engine, target_days)
                    for d in target_days:
                        seen_state_by_day[d] = existing_state.get(d, {})
                    first_loop_after_restart = False
                    p(
                        "INFO",
                        f"state hydrated from DB: "
                        f"{today_ymd}={len(seen_state_by_day.get(today_ymd, {}))}, "
                        f"{yday_ymd}={len(seen_state_by_day.get(yday_ymd, {}))}"
                    )
                else:
                    for d in target_days:
                        df_src_full = load_fct_day(engine, d)
                        cap_ts = cap_today if d == today_ymd else cap_yday
                        df_src = cap_df_by_ts(df_src_full, d, cap_ts, min_ts=None)
                        df_out = compute_vision_rows_from_fct(df_src, d)
                        seen_state_by_day[d] = snapshot_state_from_df(df_out)

                msg = (
                    f"loop days={target_days} total_src={total_src}, total_out={total_out}, "
                    f"total_delta={total_delta}, total_saved={total_saved} | " + " ; ".join(loop_msgs)
                )

                is_idle = (total_delta == 0 and total_saved == 0)

                if is_idle:
                    maybe_log_idle(
                        engine,
                        f"idle heartbeat | days={target_days} total_src={total_src}, total_out={total_out}, "
                        f"total_delta={total_delta}, total_saved={total_saved}",
                    )
                else:
                    p("INFO", msg)
                    log_db(engine, "save", msg)

                heartbeat_upsert(engine, "running", today_ymd, last_pk, msg)

                slept = 0
                while slept < LOOP_SEC and not _shutdown_requested:
                    time.sleep(1)
                    slept += 1

            except (OperationalError, DBAPIError) as e:
                p("RETRY", f"DB disconnected: {repr(e)}")
                try:
                    if engine is not None:
                        log_db(engine, "recover", f"db disconnected: {repr(e)}")
                        heartbeat_upsert(engine, "degraded", None, last_pk, f"db disconnected: {repr(e)}")
                except Exception:
                    pass

                try:
                    if engine is not None:
                        engine.dispose()
                except Exception:
                    pass

                try:
                    if lock_conn is not None:
                        release_singleton_lock(lock_conn)
                except Exception:
                    pass

                engine = None
                lock_conn = None
                first_loop_after_restart = True

                slept = 0
                while slept < RETRY_SEC and not _shutdown_requested:
                    time.sleep(1)
                    slept += 1

            except Exception as e:
                p("ERROR", f"Unhandled: {repr(e)}")
                traceback.print_exc()
                try:
                    if engine is not None:
                        log_db(engine, "error", f"Unhandled: {repr(e)}")
                        heartbeat_upsert(engine, "error", None, last_pk, f"unhandled: {repr(e)}")
                except Exception:
                    pass

                slept = 0
                while slept < LOOP_SEC and not _shutdown_requested:
                    time.sleep(1)
                    slept += 1

    finally:
        try:
            if engine is not None:
                log_db(engine, "stop", "graceful shutdown")
                heartbeat_upsert(engine, "stopped", None, last_pk, "graceful shutdown")
        except Exception:
            pass

        try:
            if lock_conn is not None:
                release_singleton_lock(lock_conn)
        except Exception:
            pass

        try:
            if engine is not None:
                engine.dispose()
        except Exception:
            pass

        p("INFO", "shutdown complete")


if __name__ == "__main__":
    run_daemon()