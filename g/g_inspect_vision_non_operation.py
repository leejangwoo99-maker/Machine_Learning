# -*- coding: utf-8 -*-
"""
g_inspect_vision_non_operation_daemon.py

[신규 로직]
- 소스: g_production_film.fct_non_operation_time
- 대상 station:
  - (FCT1, FCT2) 교집합 -> Vision1
  - (FCT3, FCT4) 교집합 -> Vision2
- 교집합 판정: (수정) 0초 구간 제거를 위해 엄격 비교
  overlap_start = max(a.from, b.from)
  overlap_end   = min(a.to, b.to)
  overlap_start < overlap_end 이면 유효 (0초/경계 접점은 저장 금지)
- 저장:
  g_production_film.vision_non_operation_time
  (end_day, station, from_time, to_time) PK UPSERT
  no_operation_time = 교집합 초(소수 둘째자리)

[핵심 보강]
- today + yesterday 2일 처리(UPSERT 안전)
- SQL TEXT 비교 제거 -> pandas timestamp 파싱 후 cap_ts로 컷
- (수정) HH:MM:SS.ss 포맷 반올림으로 인한 from/to 동일 표기 문제 완화:
  - from_time = centisecond floor
  - to_time   = centisecond ceil
"""

from __future__ import annotations

import os
import signal
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Set

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
    "password": "",  # 비번은 보완 사항
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

LOOP_SEC = 5
RETRY_SEC = 5

PG_WORK_MEM = (os.getenv("PG_WORK_MEM", "4MB") or "4MB").strip()
APP_NAME = DAEMON_NAME
ADVISORY_LOCK_KEY = 2026021102

KST = ZoneInfo("Asia/Seoul")
_shutdown_requested = False


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


def parse_dt(end_day: str, hms: str) -> pd.Timestamp:
    """
    end_day='YYYYMMDD', time='HH:MM:SS' or 'HH:MM:SS.ss' or 'HH:MM:SS.sss...' 혼합 대응
    """
    d = str(end_day)
    t = str(hms)

    ts = pd.to_datetime(f"{d} {t}", format="%Y%m%d %H:%M:%S.%f", errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime(f"{d} {t}", format="%Y%m%d %H:%M:%S", errors="coerce")
    return ts


def _format_cs(ts: pd.Timestamp, cs: int) -> str:
    """
    base는 ts의 시:분:초, cs는 0~99
    """
    base = ts.strftime("%H:%M:%S")
    return f"{base}.{cs:02d}"


def fmt_time_floor_cs(ts: pd.Timestamp) -> str:
    """
    from_time용: 센티초(0.01s) 단위 내림(floor)
    """
    if pd.isna(ts):
        return ""
    cs = int(ts.microsecond // 10000)  # 0..99
    return _format_cs(ts, cs)


def fmt_time_ceil_cs(ts: pd.Timestamp) -> str:
    """
    to_time용: 센티초(0.01s) 단위 올림(ceil)
    - 이미 딱 떨어지면 그대로
    - 올림으로 인해 +1초가 될 수 있으니 보정
    """
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


def pk_of_row(r) -> Tuple[str, str, str, str]:
    return (str(r["end_day"]), str(r["station"]), str(r["from_time"]), str(r["to_time"]))


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
def build_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    engine = create_engine(
        url,
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
    # 구버전 호환
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
                created_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (end_day, station, from_time, to_time)
            )
            """
        )
    )


# -----------------------------
# 로그/heartbeat
# -----------------------------
def log_db(engine: Engine, info: str, contents: str) -> None:
    n = now_kst()
    payload = [
        {
            "end_day": n.strftime("%Y%m%d"),
            "end_time": n.strftime("%H:%M:%S"),
            "info": normalize_info(info),
            "contents": str(contents),
        }
    ]
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
def acquire_singleton_lock(engine: Engine) -> bool:
    try:
        with engine.connect() as conn:
            got = conn.execute(text("SELECT pg_try_advisory_lock(:k)"), {"k": ADVISORY_LOCK_KEY}).scalar()
            return bool(got)
    except Exception as e:
        p("WARN", f"advisory lock check failed: {repr(e)}")
        return False


def release_singleton_lock(engine: Engine) -> None:
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": ADVISORY_LOCK_KEY})
    except Exception:
        pass


# -----------------------------
# 소스 로드 (TEXT 비교 제거 버전)
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


def cap_df_by_ts(df: pd.DataFrame, end_day: str, cap_ts: pd.Timestamp) -> pd.DataFrame:
    if df.empty:
        return df
    tmp = df.copy()
    tmp["from_ts"] = [parse_dt(end_day, x) for x in tmp["from_time"]]
    tmp["to_ts"] = [parse_dt(end_day, x) for x in tmp["to_time"]]
    tmp = tmp[(tmp["from_ts"].notna()) & (tmp["to_ts"].notna()) & (tmp["from_ts"] <= tmp["to_ts"])].copy()
    tmp = tmp[tmp["from_ts"] <= cap_ts].copy()
    return tmp[["end_day", "station", "from_time", "to_time"]].copy()


# -----------------------------
# 교집합 계산 (0초 구간 제거 + from/to 포맷 분리)
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

            # ✅ 핵심 수정: 0초(경계만 닿는) 구간 저장 금지
            if not (ov_from < ov_to):
                continue

            diff_sec = float((ov_to - ov_from).total_seconds())
            # ✅ 반올림 결과가 0.00이면 저장하지 않음(안전장치)
            diff_sec_2 = round(diff_sec, 2)
            if diff_sec_2 <= 0.0:
                continue

            out_rows.append(
                {
                    "end_day": str(end_day),
                    "station": out_station,
                    # ✅ 핵심 수정: from=내림, to=올림 (HH:MM:SS.ss)
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

    # 같은 PK 중복 방지
    df = df.drop_duplicates(subset=["end_day", "station", "from_time", "to_time"], keep="last").reset_index(drop=True)
    return df


# -----------------------------
# 저장
# -----------------------------
def upsert_rows(rows: List[dict]) -> int:
    if not rows:
        return 0

    conn = None
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
                    created_at TIMESTAMPTZ DEFAULT now(),
                    PRIMARY KEY (end_day, station, from_time, to_time)
                )
                """
            )

            sql = f"""
                INSERT INTO {SAVE_SCHEMA}.{SAVE_TABLE}
                (end_day, station, from_time, to_time, no_operation_time)
                VALUES %s
                ON CONFLICT (end_day, station, from_time, to_time)
                DO UPDATE SET
                    no_operation_time = EXCLUDED.no_operation_time
            """

            vals = []
            for r in rows:
                vals.append(
                    (
                        str(r["end_day"]),
                        str(r["station"]),
                        str(r["from_time"]),
                        str(r["to_time"]),
                        None if r["no_operation_time"] is None else float(r["no_operation_time"]),
                    )
                )

            total = len(vals)
            page_size = 500
            done = 0

            for s in range(0, total, page_size):
                if _shutdown_requested:
                    p("INFO", "shutdown requested during upsert; stopping early")
                    break
                e = min(s + page_size, total)
                chunk = vals[s:e]
                execute_values(cur, sql, chunk, page_size=len(chunk))
                done += len(chunk)
                p("INFO", f"upsert progress {done}/{total}")

        return done

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
    p("BOOT", f"{DAEMON_NAME} starting")

    engine: Optional[Engine] = None
    lock_acquired = False

    seen_pk_by_day: Dict[str, Set[Tuple[str, str, str, str]]] = {}
    bootstrapped_by_day: Set[str] = set()

    last_pk: Optional[Tuple[str, str, str, str]] = None

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

                    lock_acquired = acquire_singleton_lock(engine)
                    if not lock_acquired:
                        p("RETRY", "another instance already running; waiting lock...")
                        while not lock_acquired:
                            if _shutdown_requested:
                                break
                            time.sleep(RETRY_SEC)
                            lock_acquired = acquire_singleton_lock(engine)
                            if not lock_acquired:
                                p("RETRY", "waiting advisory lock...")
                        if _shutdown_requested:
                            break
                        p("INFO", "advisory lock acquired")

                    log_db(engine, "start", "daemon connected and ready")
                    heartbeat_upsert(engine, "running", None, last_pk, "connected and ready")

                today_ymd, now_time = current_window()
                yday_ymd = ymd_add(today_ymd, -1)

                cap_today = parse_dt(today_ymd, now_time)
                cap_yday = parse_dt(yday_ymd, "23:59:59.99")

                target_days = [today_ymd, yday_ymd]

                loop_msgs: List[str] = []
                total_src = 0
                total_out = 0
                total_new = 0
                total_saved = 0

                for d in target_days:
                    if _shutdown_requested:
                        break

                    cap_ts = cap_today if d == today_ymd else cap_yday

                    df_src_full = load_fct_day(engine, d)
                    df_src = cap_df_by_ts(df_src_full, d, cap_ts)

                    src_n = len(df_src)
                    total_src += src_n
                    p("INFO", f"[{d}] source rows={src_n} (cap<= {cap_ts})")
                    log_db(engine, "info", f"[{d}] source rows={src_n} cap<= {cap_ts}")

                    df_out = compute_vision_rows_from_fct(df_src, d)
                    out_n = len(df_out)
                    total_out += out_n
                    p("INFO", f"[{d}] computed output rows={out_n}")
                    log_db(engine, "info", f"[{d}] computed output rows={out_n}")

                    disp = df_last_pk_for_display(df_out)
                    if disp is not None:
                        last_pk = disp

                    if d not in seen_pk_by_day:
                        seen_pk_by_day[d] = set()

                    if d not in bootstrapped_by_day:
                        rows = df_out.to_dict(orient="records")
                        p("INFO", f"[{d}] phase=bootstrap before_upsert rows={len(rows)}")
                        saved = upsert_rows(rows)
                        p("INFO", f"[{d}] phase=bootstrap after_upsert saved={saved}")
                        log_db(engine, "info", f"[{d}] bootstrap saved={saved}")

                        pkset = set()
                        for _, r in df_out.iterrows():
                            pkset.add(pk_of_row(r))
                        seen_pk_by_day[d] = pkset
                        bootstrapped_by_day.add(d)

                        total_saved += saved
                        loop_msgs.append(f"[{d}] bootstrap saved={saved}, seen_pk={len(pkset)}")
                        continue

                    new_rows: List[dict] = []
                    if not df_out.empty:
                        dpk = df_out.sort_values(["end_day", "station", "from_time", "to_time"]).reset_index(drop=True)
                        for _, r in dpk.iterrows():
                            pk = pk_of_row(r)
                            if pk in seen_pk_by_day[d]:
                                continue
                            new_rows.append(
                                {
                                    "end_day": str(r["end_day"]),
                                    "station": str(r["station"]),
                                    "from_time": str(r["from_time"]),
                                    "to_time": str(r["to_time"]),
                                    "no_operation_time": None
                                    if pd.isna(r["no_operation_time"])
                                    else float(r["no_operation_time"]),
                                }
                            )

                    new_n = len(new_rows)
                    total_new += new_n

                    p("INFO", f"[{d}] phase=incremental before_upsert new_rows={new_n}")
                    saved = upsert_rows(new_rows) if new_rows else 0
                    p("INFO", f"[{d}] phase=incremental after_upsert saved={saved}")

                    for nr in new_rows:
                        seen_pk_by_day[d].add((nr["end_day"], nr["station"], nr["from_time"], nr["to_time"]))

                    total_saved += saved
                    loop_msgs.append(
                        f"[{d}] inc computed={out_n}, new_pk={new_n}, saved={saved}, seen_pk={len(seen_pk_by_day[d])}"
                    )

                msg = (
                    f"loop days={target_days} total_src={total_src}, total_out={total_out}, "
                    f"total_new={total_new}, total_saved={total_saved} | " + " ; ".join(loop_msgs)
                )
                p("INFO", msg)
                log_db(engine, "info", msg)
                heartbeat_upsert(engine, "running", today_ymd, last_pk, msg)

                slept = 0
                while slept < LOOP_SEC and not _shutdown_requested:
                    time.sleep(1)
                    slept += 1

            except (OperationalError, DBAPIError) as e:
                p("RETRY", f"DB disconnected: {repr(e)}")
                try:
                    if engine is not None:
                        heartbeat_upsert(engine, "degraded", None, last_pk, f"db disconnected: {repr(e)}")
                except Exception:
                    pass

                try:
                    if engine is not None and lock_acquired:
                        release_singleton_lock(engine)
                except Exception:
                    pass
                try:
                    if engine is not None:
                        engine.dispose()
                except Exception:
                    pass

                engine = None
                lock_acquired = False

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
                heartbeat_upsert(engine, "stopped", None, last_pk, "graceful shutdown")
        except Exception:
            pass

        try:
            if engine is not None and lock_acquired:
                release_singleton_lock(engine)
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