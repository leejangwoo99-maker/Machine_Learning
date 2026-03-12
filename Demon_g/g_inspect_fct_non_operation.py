# -*- coding: utf-8 -*-
"""
Factory Realtime - FCT Non Operation Time Inspector (final)

[최종 판정 규칙]
1) 기존 조건만 사용
   - TEST RESULT -> TEST AUTO MODE START
   - 중간 로그는 기본적으로 무시

2) TEST RESULT 인식
   - 대소문자 섞임 허용
   - 콜론 개수/공백 차이 허용
   - 예:
     TEST RESULT :: OK
     TEST Result : NG

3) RESULT 선택 규칙
   - Manual mode 전환이 없는 경우:
     여러 TEST RESULT 중 마지막 TEST RESULT를 사용
   - Manual mode 전환이 나온 경우:
     그 이후 TEST AUTO MODE START가 나오기 전까지 TEST RESULT는 무시
     즉 Manual 이전의 살아있는 pending RESULT 유지

4) TEST AUTO MODE START가 나오면
   - 현재 살아있는 pending RESULT와 짝지음
   - diff > 0
   - diff > threshold
   - 저장 후 pending 초기화

5) incremental batch 경계 보완
   - pending_result_time 저장
   - manual_block 저장

6) 재시작 안전
   - 프로그램 시작 시 오늘 cursor 초기화
   - 첫 RUN은 DB cursor 무시 + 오늘 full scan
   - UPSERT로 중복 방지

7) DB 로그 최소화
   - heartbeat는 1분 1회만 DB 저장
"""

import os
import sys
import re
import time as time_mod
import socket
import traceback
import subprocess
import multiprocessing as mp
from datetime import datetime
from zoneinfo import ZoneInfo
import urllib.parse

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SRC_SCHEMA = "d1_machine_log"
FCT_TABLES = {
    "FCT1": "FCT1_machine_log",
    "FCT2": "FCT2_machine_log",
    "FCT3": "FCT3_machine_log",
    "FCT4": "FCT4_machine_log",
}

SAVE_SCHEMA = "g_production_film"
SAVE_TABLE = "fct_non_operation_time"

CURSOR_SCHEMA = "g_production_film"
CURSOR_TABLE = "fct_non_operation_time_cursor"

LOOP_INTERVAL_SEC = 2.0
STABLE_DATA_SEC = 1.0

DB_CONNECT_TIMEOUT_SEC = 5
RETRY_SLEEP_SEC = 10

WORK_MEM = "4MB"
STATEMENT_TIMEOUT_MS = 60000

STARTUP_FULL_RESCAN_TODAY = True
STARTUP_FORCE_FIRST_RUN_FULLSCAN = True

DB_HEARTBEAT_INTERVAL_SEC = 60.0

KST = ZoneInfo("Asia/Seoul")


# =========================
# 0-1) 로그 DB 설정
# =========================
LOG_DB_SCHEMA = "k_demon_heath_check"
LOG_DB_TABLE = "gf_log"
DB_LOG_ENGINE = None
_DB_LOG_GUARD = False
_LAST_DB_HEARTBEAT_TS = 0.0

INFO_ALLOW = {
    "info", "error", "down", "sleep", "warn", "boot", "run", "done", "load", "save",
    "skip", "evt", "cursor", "recover", "retry", "exit", "fatal", "diag", "socket",
    "ping", "db", "heartbeat"
}

DB_LOG_ALLOWED = {"boot", "error", "recover", "save", "fatal", "exit", "heartbeat"}


# =========================
# 0-2) 이벤트 판정
# =========================
RE_RESULT = re.compile(r"^\s*TEST\s+RESULT\s*:+\s*(OK|NG)\b", re.IGNORECASE)
RE_AUTO_START = re.compile(r"^\s*TEST\s+AUTO\s+MODE\s+START\b", re.IGNORECASE)

MANUAL_MODE_TEXT = "Manual mode 전환"


def classify_event(contents: str) -> str | None:
    s = str(contents or "").strip()
    if s == MANUAL_MODE_TEXT:
        return "MANUAL"
    if RE_RESULT.match(s):
        return "RESULT"
    if RE_AUTO_START.match(s):
        return "AUTO_START"
    return None


def _infer_info(msg: str) -> str:
    if not msg:
        return "info"

    m = re.match(r"^\[([A-Za-z0-9_ -]+)\]", str(msg).strip())
    if m:
        tag = m.group(1).strip().lower().replace(" ", "_")
        if tag in ("err", "error"):
            return "error"
        if tag in ("fatal",):
            return "fatal"
        if tag in ("warning",):
            return "warn"
        if tag in INFO_ALLOW:
            return tag
        return "info"

    low = str(msg).lower()
    if "heartbeat" in low:
        return "heartbeat"
    if "sleep" in low:
        return "sleep"
    if "down" in low:
        return "down"
    if "error" in low or "exception" in low:
        return "error"
    return "info"


def _default_log_path():
    try:
        base = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else __file__)
    except Exception:
        base = os.getcwd()
    return os.path.join(base, "diag_{0}.log".format(datetime.now().strftime("%Y%m%d_%H%M%S")))


LOG_PATH = _default_log_path()


def _session_guard(conn):
    try:
        conn.execute(text("SET work_mem TO '{0}';".format(WORK_MEM)))
        if STATEMENT_TIMEOUT_MS is not None:
            conn.execute(text("SET statement_timeout TO {0};".format(int(STATEMENT_TIMEOUT_MS))))
    except Exception:
        pass


def _ensure_log_table(engine):
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_DB_SCHEMA};
    CREATE TABLE IF NOT EXISTS {LOG_DB_SCHEMA}.{LOG_DB_TABLE} (
        end_day   TEXT NOT NULL,
        end_time  TEXT NOT NULL,
        info      TEXT NOT NULL,
        contents  TEXT NOT NULL
    );
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl)


def _should_write_db_log(info_val: str) -> bool:
    global _LAST_DB_HEARTBEAT_TS

    if info_val not in DB_LOG_ALLOWED:
        return False

    if info_val == "heartbeat":
        now_epoch = time_mod.time()
        if now_epoch - _LAST_DB_HEARTBEAT_TS < DB_HEARTBEAT_INTERVAL_SEC:
            return False
        _LAST_DB_HEARTBEAT_TS = now_epoch

    return True


def _save_log_to_db(msg: str, info: str = None):
    global _DB_LOG_GUARD, DB_LOG_ENGINE

    if DB_LOG_ENGINE is None or _DB_LOG_GUARD:
        return

    info_val = (info or _infer_info(msg) or "info").strip().lower() or "info"
    if not _should_write_db_log(info_val):
        return

    try:
        _DB_LOG_GUARD = True
        dt = datetime.now()

        ins = text(f"""
            INSERT INTO {LOG_DB_SCHEMA}.{LOG_DB_TABLE}
            (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """)

        row = {
            "end_day": dt.strftime("%Y%m%d"),
            "end_time": dt.strftime("%H:%M:%S"),
            "info": info_val,
            "contents": str(msg),
        }

        with DB_LOG_ENGINE.begin() as conn:
            _session_guard(conn)
            conn.execute(ins, row)

    except Exception:
        pass
    finally:
        _DB_LOG_GUARD = False


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(msg: str, info: str = None):
    s = "[{0}] {1}".format(_now_str(), msg)
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(s + "\n")
    except Exception:
        pass
    _save_log_to_db(msg=msg, info=info)


def hold_console(exit_code: int = 0):
    log("[HOLD] press Enter to exit (exit_code={0})".format(exit_code), info="info")
    try:
        input()
    except Exception:
        time_mod.sleep(999999)


def run_cmd(cmd):
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, shell=False)
        return p.returncode, (p.stdout or "").strip(), (p.stderr or "").strip()
    except Exception as e:
        return -1, "", "{0}: {1}".format(type(e).__name__, e)


def safe_ping(host: str):
    rc, out, err = run_cmd(["ping", "-n", "1", "-w", "1000", host])
    log("[PING] host={0} rc={1}".format(host, rc), info="ping")
    if out:
        log("[PING-OUT] {0}".format(out[:500]), info="ping")
    if err:
        log("[PING-ERR] {0}".format(err[:500]), info="ping")
    return rc == 0


def socket_port_check(host: str, port: int, timeout_sec: float = 3.0):
    t0 = time_mod.time()
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            dt = time_mod.time() - t0
            log("[SOCKET] {0}:{1} OK ({2:.2f}s)".format(host, port, dt), info="socket")
            return True
    except Exception as e:
        dt = time_mod.time() - t0
        log("[SOCKET] {0}:{1} FAIL ({2:.2f}s) err={3}: {4}".format(host, port, dt, type(e).__name__, e), info="down")
        return False


def build_db_url(cfg):
    pw = urllib.parse.quote_plus(cfg["password"])
    return "postgresql+psycopg2://{u}:{p}@{h}:{pt}/{d}".format(
        u=cfg["user"], p=pw, h=cfg["host"], pt=cfg["port"], d=cfg["dbname"]
    )


def print_env_header():
    log("=" * 110, info="boot")
    log("[START] DIAG HEADER", info="boot")
    log("frozen={0} | exe={1}".format(getattr(sys, "frozen", False), sys.executable), info="boot")
    log("cwd={0}".format(os.getcwd()), info="boot")
    try:
        log("hostname={0}".format(socket.gethostname()), info="boot")
    except Exception:
        pass
    log("log_file={0}".format(LOG_PATH), info="boot")
    safe_url = build_db_url(DB_CONFIG).replace(urllib.parse.quote_plus(DB_CONFIG["password"]), "***")
    log("[DB] host={0} port={1} db={2} user={3}".format(
        DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"], DB_CONFIG["user"]), info="db")
    log("[DB_URL] {0}".format(safe_url), info="db")
    log("work_mem={0} statement_timeout_ms={1}".format(WORK_MEM, STATEMENT_TIMEOUT_MS), info="db")
    log("=" * 110, info="boot")


def run_diagnostics():
    print_env_header()
    host = DB_CONFIG["host"]
    port = int(DB_CONFIG["port"])
    safe_ping(host)
    socket_port_check(host, port, timeout_sec=3.0)


# =========================
# 1) 공통 유틸
# =========================
def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")


def now_ts() -> datetime:
    return datetime.now()


def _print_run_banner(tag: str, start_dt: datetime):
    log("=" * 110, info="run")
    log("[{0}] {1}".format(tag, start_dt.strftime("%Y-%m-%d %H:%M:%S")), info="run")
    log("=" * 110, info="run")


def _print_end_banner(tag: str, start_dt: datetime, end_dt: datetime):
    log("-" * 110, info="done")
    log("[{0}] {1} | elapsed={2}".format(tag, end_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt - start_dt), info="done")
    log("-" * 110, info="done")


def _safe_ts_from_day_time(end_day: str, end_time: str):
    try:
        if end_day is None or end_time is None:
            return None
        ts = pd.to_datetime(str(end_day) + " " + str(end_time), errors="coerce")
        if pd.isna(ts):
            return None
        return ts
    except Exception:
        return None


def _fresh_empty_state():
    return {
        "last_id": None,
        "pending_result_time": None,
        "manual_block": 0,
    }


def _fresh_all_station_states():
    return {
        "FCT1": _fresh_empty_state().copy(),
        "FCT2": _fresh_empty_state().copy(),
        "FCT3": _fresh_empty_state().copy(),
        "FCT4": _fresh_empty_state().copy(),
    }


# =========================
# 1-1) 엔진/연결
# =========================
def is_db_disconnect_error(e: Exception) -> bool:
    if isinstance(e, OperationalError):
        return True
    if isinstance(e, DBAPIError) and getattr(e, "connection_invalidated", False):
        return True

    msg = (str(e) or "").lower()
    hints = [
        "server closed the connection",
        "terminating connection",
        "connection refused",
        "connection reset",
        "could not connect",
        "network is unreachable",
        "broken pipe",
        "ssl syscall error",
        "timeout",
        "connection timed out",
    ]
    return any(h in msg for h in hints)


def _make_engine_one_pool():
    db_url = build_db_url(DB_CONFIG)

    opt = "-c work_mem={0}".format(WORK_MEM)
    if STATEMENT_TIMEOUT_MS is not None:
        opt += " -c statement_timeout={0}".format(int(STATEMENT_TIMEOUT_MS))

    connect_args = {
        "connect_timeout": int(DB_CONNECT_TIMEOUT_SEC),
        "application_name": "fct_nonop_realtime",
        "options": opt,
    }

    eng = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        connect_args=connect_args,
    )
    return eng


def init_engine_blocking():
    while True:
        eng = None
        try:
            eng = _make_engine_one_pool()
            with eng.connect() as conn:
                _session_guard(conn)
                conn.execute(text("SELECT 1"))
            log("[OK] engine connect test passed (single-pool fixed)", info="db")
            return eng
        except Exception as e:
            log("[ERROR] DB connect failed: {0}: {1}".format(type(e).__name__, e), info="error")
            log(traceback.format_exc(), info="error")
            try:
                if eng is not None:
                    eng.dispose()
            except Exception:
                pass
            log("[RETRY] sleep {0}s then retry DB connect".format(RETRY_SLEEP_SEC), info="retry")
            time_mod.sleep(RETRY_SLEEP_SEC)


def engine_health_check_blocking(engine):
    with engine.connect() as conn:
        _session_guard(conn)
        conn.execute(text("SELECT 1"))
    return True


def recover_engine_and_thresholds_blocking():
    global DB_LOG_ENGINE

    while True:
        engine = init_engine_blocking()

        try:
            _ensure_log_table(engine)
            DB_LOG_ENGINE = engine
            log("[OK] db log table ensured: {0}.{1}".format(LOG_DB_SCHEMA, LOG_DB_TABLE), info="boot")
        except Exception as e:
            log("[WARN] log table ensure failed (will retry later): {0}: {1}".format(type(e).__name__, e), info="warn")

        th_map = None
        while th_map is None:
            try:
                th_map = load_thresholds(engine)
                log("[OK] thresholds loaded: value={0} sec | month={1} | fallback={2}".format(
                    th_map.get("ALL"), th_map.get("month"), th_map.get("fallback")
                ), info="load")
            except Exception as e:
                if is_db_disconnect_error(e):
                    log("[WARN] thresholds load hit DB disconnect -> re-init engine (blocking)", info="down")
                    try:
                        engine.dispose()
                    except Exception:
                        pass
                    engine = None
                    DB_LOG_ENGINE = None
                    break
                log("[ERROR] load_thresholds failed: {0}: {1}".format(type(e).__name__, e), info="error")
                log(traceback.format_exc(), info="error")
                log("[RETRY] sleep {0}s then retry thresholds".format(RETRY_SLEEP_SEC), info="retry")
                time_mod.sleep(RETRY_SLEEP_SEC)

        if engine is not None and th_map is not None:
            return engine, th_map


# =========================
# 2) cursor table
# =========================
def ensure_cursor_table(engine):
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {CURSOR_SCHEMA};
    CREATE TABLE IF NOT EXISTS {CURSOR_SCHEMA}.{CURSOR_TABLE} (
        end_day              TEXT NOT NULL,
        station              TEXT NOT NULL,
        last_id              BIGINT NULL,
        pending_result_time  TEXT NULL,
        manual_block         INTEGER NOT NULL DEFAULT 0,
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (end_day, station)
    );
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl)

    ddls = [
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='{CURSOR_SCHEMA}'
                  AND table_name='{CURSOR_TABLE}'
                  AND column_name='last_id'
            ) THEN
                ALTER TABLE {CURSOR_SCHEMA}.{CURSOR_TABLE} ADD COLUMN last_id BIGINT NULL;
            END IF;
        END $$;
        """,
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='{CURSOR_SCHEMA}'
                  AND table_name='{CURSOR_TABLE}'
                  AND column_name='pending_result_time'
            ) THEN
                ALTER TABLE {CURSOR_SCHEMA}.{CURSOR_TABLE} ADD COLUMN pending_result_time TEXT NULL;
            END IF;
        END $$;
        """,
        f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_schema='{CURSOR_SCHEMA}'
                  AND table_name='{CURSOR_TABLE}'
                  AND column_name='manual_block'
            ) THEN
                ALTER TABLE {CURSOR_SCHEMA}.{CURSOR_TABLE} ADD COLUMN manual_block INTEGER NOT NULL DEFAULT 0;
            END IF;
        END $$;
        """,
    ]

    with engine.begin() as conn:
        _session_guard(conn)
        for ddlx in ddls:
            conn.execute(text(ddlx))


def reset_today_cursors(engine, end_day: str):
    ensure_cursor_table(engine)
    q = text(f"""
        DELETE FROM {CURSOR_SCHEMA}.{CURSOR_TABLE}
        WHERE end_day = :end_day
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(q, {"end_day": end_day})


def load_cursors(engine, end_day: str) -> dict:
    ensure_cursor_table(engine)
    q = text(f"""
        SELECT station, last_id, pending_result_time, manual_block
        FROM {CURSOR_SCHEMA}.{CURSOR_TABLE}
        WHERE end_day = :end_day
    """)
    df = pd.read_sql(q, engine, params={"end_day": end_day})

    cur = _fresh_all_station_states()

    for _, r in df.iterrows():
        st = str(r["station"])
        cur[st] = {
            "last_id": r["last_id"],
            "pending_result_time": r["pending_result_time"] if pd.notna(r["pending_result_time"]) else None,
            "manual_block": int(r["manual_block"]) if pd.notna(r["manual_block"]) else 0,
        }
    return cur


def upsert_cursor_states_batch(engine, end_day: str, state_updates: list[tuple[str, dict]]):
    if not state_updates:
        return

    ensure_cursor_table(engine)

    q = text(f"""
        INSERT INTO {CURSOR_SCHEMA}.{CURSOR_TABLE}
        (end_day, station, last_id, pending_result_time, manual_block, updated_at)
        VALUES
        (:end_day, :station, :last_id, :pending_result_time, :manual_block, now())
        ON CONFLICT (end_day, station)
        DO UPDATE SET
            last_id = EXCLUDED.last_id,
            pending_result_time = EXCLUDED.pending_result_time,
            manual_block = EXCLUDED.manual_block,
            updated_at = now()
    """)

    rows = []
    for station, state in state_updates:
        rows.append({
            "end_day": end_day,
            "station": station,
            "last_id": int(state["last_id"]) if state.get("last_id") is not None else None,
            "pending_result_time": state.get("pending_result_time"),
            "manual_block": int(state.get("manual_block") or 0),
        })

    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(q, rows)


# =========================
# 3) threshold
# =========================
def prev_month_yyyymm_kst(now: datetime | None = None) -> str:
    if now is None:
        now = datetime.now(tz=KST)
    else:
        if now.tzinfo is None:
            now = now.replace(tzinfo=KST)
        else:
            now = now.astimezone(KST)

    y = now.year
    m = now.month
    if m == 1:
        y -= 1
        m = 12
    else:
        m -= 1
    return f"{y:04d}{m:02d}"


def load_thresholds(engine):
    target_month = prev_month_yyyymm_kst()

    q_max = text("""
        SELECT MAX(upper_outlier) AS mx
        FROM g_production_film.fct_op_criteria
        WHERE month = :month
    """)
    df = pd.read_sql(q_max, engine, params={"month": target_month})
    mx = df.loc[0, "mx"] if (df is not None and not df.empty) else None
    if mx is not None and pd.notna(mx):
        return {"ALL": float(mx), "month": str(target_month), "fallback": False}

    q_latest_month = text("""SELECT MAX(month) AS latest_month FROM g_production_film.fct_op_criteria""")
    dfm = pd.read_sql(q_latest_month, engine)
    latest_month = dfm.loc[0, "latest_month"] if (dfm is not None and not dfm.empty) else None
    if latest_month is None or str(latest_month).strip() == "":
        raise RuntimeError("[ERROR] g_production_film.fct_op_criteria 테이블에 month 데이터가 없습니다.")

    latest_month = str(latest_month).strip()
    df2 = pd.read_sql(q_max, engine, params={"month": latest_month})
    mx2 = df2.loc[0, "mx"] if (df2 is not None and not df2.empty) else None
    if mx2 is None or pd.isna(mx2):
        raise RuntimeError(f"[ERROR] fct_op_criteria month={latest_month} 행은 있으나 upper_outlier MAX가 NULL 입니다.")

    return {"ALL": float(mx2), "month": latest_month, "fallback": True}


def threshold_for_station(th_map: dict, station: str) -> float:
    return float(th_map["ALL"])


# =========================
# 4) source load
# =========================
def _load_station_max_id(engine, end_day: str, station: str, last_id):
    tbl = FCT_TABLES[station]

    if last_id is None or (isinstance(last_id, float) and pd.isna(last_id)):
        q = text(f"""
            SELECT MAX(id) AS mx
            FROM {SRC_SCHEMA}."{tbl}"
            WHERE end_day = :end_day
        """)
        params = {"end_day": end_day}
    else:
        q = text(f"""
            SELECT MAX(id) AS mx
            FROM {SRC_SCHEMA}."{tbl}"
            WHERE end_day = :end_day
              AND id > :last_id
        """)
        params = {"end_day": end_day, "last_id": int(last_id)}

    df = pd.read_sql(q, engine, params=params)
    if df is None or df.empty:
        return None
    mx = df.loc[0, "mx"]
    return int(mx) if pd.notna(mx) else None


def load_fct_incremental(engine, end_day: str, station: str, last_id):
    """
    필요한 후보 로그만 가져오고 최종 판정은 Python classify_event 에서 처리
    """
    tbl = FCT_TABLES[station]
    stable_cut = datetime.now() - pd.Timedelta(seconds=STABLE_DATA_SEC)

    fetched_max_id = _load_station_max_id(engine, end_day=end_day, station=station, last_id=last_id)

    params = {
        "end_day": end_day,
        "result_prefix": "TEST RESULT%",
        "auto_start_prefix": "TEST AUTO MODE START%",
        "manual_text": MANUAL_MODE_TEXT,
    }

    if last_id is None or (isinstance(last_id, float) and pd.isna(last_id)):
        q = text(f"""
            SELECT id, end_day, station, end_time, contents
            FROM {SRC_SCHEMA}."{tbl}"
            WHERE end_day = :end_day
              AND (
                    contents ILIKE :result_prefix
                 OR contents ILIKE :auto_start_prefix
                 OR contents = :manual_text
              )
            ORDER BY id ASC
        """)
    else:
        q = text(f"""
            SELECT id, end_day, station, end_time, contents
            FROM {SRC_SCHEMA}."{tbl}"
            WHERE end_day = :end_day
              AND id > :last_id
              AND (
                    contents ILIKE :result_prefix
                 OR contents ILIKE :auto_start_prefix
                 OR contents = :manual_text
              )
            ORDER BY id ASC
        """)
        params["last_id"] = int(last_id)

    df = pd.read_sql(q, engine, params=params)

    if df.empty:
        return df, fetched_max_id

    df["contents"] = df["contents"].astype(str)
    ts = pd.to_datetime(df["end_day"].astype(str) + " " + df["end_time"].astype(str), errors="coerce")
    df["_ts"] = ts
    df = df[df["_ts"].notna()].copy()
    df = df[df["_ts"] <= stable_cut].copy()

    df["_event_type"] = df["contents"].apply(classify_event)
    df = df[df["_event_type"].notna()].copy()

    return df.reset_index(drop=True), fetched_max_id


# =========================
# 5) event calc
# =========================
def _empty_event_df():
    return pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])


def _append_event(events: list, end_day: str, station: str, from_time: str, to_time: str, diff_sec: float):
    if diff_sec is None:
        return
    try:
        diff_val = float(round(float(diff_sec), 2))
    except Exception:
        return
    if diff_val <= 0:
        return
    events.append({
        "end_day": str(end_day),
        "station": str(station),
        "from_time": str(from_time),
        "to_time": str(to_time),
        "no_operation_time": diff_val,
    })


def compute_events_for_station(
    station: str,
    end_day: str,
    df_station: pd.DataFrame,
    th_map: dict,
    prev_state: dict,
    fetched_max_id,
):
    max_id = fetched_max_id if fetched_max_id is not None else prev_state.get("last_id")

    pending_result_time = prev_state.get("pending_result_time")
    pending_result_ts = _safe_ts_from_day_time(end_day, pending_result_time)
    manual_block = int(prev_state.get("manual_block") or 0)

    if df_station is None or df_station.empty:
        new_state = {
            "last_id": max_id,
            "pending_result_time": pending_result_time,
            "manual_block": manual_block,
        }
        return station, new_state, _empty_event_df()

    df = df_station.copy()
    df = df.sort_values(["_ts", "id"], ascending=True).reset_index(drop=True)

    th_val = threshold_for_station(th_map, station)
    events = []

    for i in range(len(df)):
        event_type = str(df.at[i, "_event_type"])
        cur_ts = df.at[i, "_ts"]
        cur_end_time = str(df.at[i, "end_time"])

        if event_type == "MANUAL":
            manual_block = 1
            continue

        if event_type == "RESULT":
            if pending_result_ts is None:
                pending_result_time = cur_end_time
                pending_result_ts = cur_ts
            else:
                if manual_block == 0:
                    # Manual mode 전환이 없으면 마지막 RESULT 사용
                    pending_result_time = cur_end_time
                    pending_result_ts = cur_ts
                else:
                    # Manual 이후 AUTO START 전 RESULT는 무시
                    pass
            continue

        if event_type == "AUTO_START":
            if pending_result_ts is not None and pd.notna(pending_result_ts) and pd.notna(cur_ts) and (cur_ts > pending_result_ts):
                diff = (cur_ts - pending_result_ts).total_seconds()
                if diff > 0 and diff > th_val:
                    _append_event(
                        events=events,
                        end_day=end_day,
                        station=station,
                        from_time=pending_result_time,
                        to_time=cur_end_time,
                        diff_sec=diff,
                    )

            pending_result_time = None
            pending_result_ts = None
            manual_block = 0
            continue

    out = pd.DataFrame(events) if len(events) > 0 else _empty_event_df()

    if not out.empty:
        out["no_operation_time"] = pd.to_numeric(out["no_operation_time"], errors="coerce")
        out = out[out["no_operation_time"].notna() & (out["no_operation_time"] > 0)].copy()

        if not out.empty:
            out["end_day"] = out["end_day"].astype(str)
            out["station"] = out["station"].astype(str)
            out["from_time"] = out["from_time"].astype(str)
            out["to_time"] = out["to_time"].astype(str)
            out = out.drop_duplicates(
                subset=["end_day", "station", "from_time", "to_time"],
                keep="first",
            ).reset_index(drop=True)

    new_state = {
        "last_id": max_id,
        "pending_result_time": pending_result_time,
        "manual_block": manual_block,
    }

    return station, new_state, out


# =========================
# 6) save
# =========================
def ensure_target_table(engine):
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA};
    CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{SAVE_TABLE} (
        end_day           TEXT NOT NULL,
        station           TEXT NOT NULL,
        from_time         TEXT NOT NULL,
        to_time           TEXT NOT NULL,
        no_operation_time NUMERIC(12,2),
        created_at        TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (end_day, station, from_time, to_time)
    );
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl)


def upsert_events(engine, df_events: pd.DataFrame) -> int:
    if df_events is None or df_events.empty:
        return 0

    df_events = df_events.copy()
    df_events["no_operation_time"] = pd.to_numeric(df_events["no_operation_time"], errors="coerce")
    df_events = df_events[df_events["no_operation_time"].notna() & (df_events["no_operation_time"] > 0)].copy()
    if df_events.empty:
        return 0

    ensure_target_table(engine)

    upsert_sql = text(f"""
        INSERT INTO {SAVE_SCHEMA}.{SAVE_TABLE}
        (end_day, station, from_time, to_time, no_operation_time)
        VALUES (:end_day, :station, :from_time, :to_time, :no_operation_time)
        ON CONFLICT (end_day, station, from_time, to_time)
        DO UPDATE SET no_operation_time = EXCLUDED.no_operation_time;
    """)

    rows = df_events.to_dict(orient="records")
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(upsert_sql, rows)

    return len(rows)


# =========================
# 7) main once
# =========================
def main_once(engine, th_map, force_first_fullscan: bool = False):
    run_start = now_ts()
    _print_run_banner("RUN", run_start)

    end_day = today_yyyymmdd()

    if force_first_fullscan:
        cursors = _fresh_all_station_states()
        log("[BOOT] first run forced fullscan -> ignore DB cursor end_day={0}".format(end_day), info="boot")
    else:
        cursors = load_cursors(engine, end_day=end_day)

    station_payloads = {}

    log("[HEARTBEAT] loop begin end_day={0}".format(end_day), info="heartbeat")

    for st, state in cursors.items():
        last_id = state.get("last_id")
        log("[LOAD-START] {0} {1} last_id={2}".format(end_day, st, last_id), info="load")
        df_st, fetched_max_id = load_fct_incremental(engine, end_day=end_day, station=st, last_id=last_id)
        station_payloads[st] = (df_st, fetched_max_id)
        log("[LOAD-END] {0} {1} rows={2} max_id={3}".format(end_day, st, len(df_st), fetched_max_id), info="load")

    all_events = []
    state_updates = []

    for st, payload in station_payloads.items():
        st_df, fetched_max_id = payload
        prev_state = cursors.get(st, _fresh_empty_state())

        st_name, new_state, ev = compute_events_for_station(
            station=st,
            end_day=end_day,
            df_station=st_df,
            th_map=th_map,
            prev_state=prev_state,
            fetched_max_id=fetched_max_id,
        )

        if ev is not None and not ev.empty:
            all_events.append(ev)
            log("[EVT] {0}: events={1}".format(st_name, len(ev)), info="evt")
        else:
            log("[EVT] {0}: events=0".format(st_name), info="evt")

        state_updates.append((st_name, new_state))

    if len(all_events) > 0:
        df_save = pd.concat(all_events, ignore_index=True)
        df_save["no_operation_time"] = pd.to_numeric(df_save["no_operation_time"], errors="coerce")
        df_save = df_save[df_save["no_operation_time"].notna() & (df_save["no_operation_time"] > 0)].copy()

        inserted = upsert_events(engine, df_save)
        log("[SAVE] {0}.{1}: upsert rows={2}".format(SAVE_SCHEMA, SAVE_TABLE, inserted), info="save")
    else:
        log("[SAVE] {0}.{1}: no events -> skip".format(SAVE_SCHEMA, SAVE_TABLE), info="save")

    upsert_cursor_states_batch(engine, end_day=end_day, state_updates=state_updates)
    for st, state in state_updates:
        log(
            "[CURSOR] {0} {1} -> last_id={2} pending_result_time={3} manual_block={4}".format(
                end_day,
                st,
                state.get("last_id"),
                state.get("pending_result_time"),
                state.get("manual_block"),
            ),
            info="cursor"
        )

    run_end = now_ts()
    _print_end_banner("DONE", run_start, run_end)


# =========================
# 8) realtime loop
# =========================
def realtime_loop():
    log("[BOOT] realtime_loop start", info="boot")
    run_diagnostics()

    engine, th_map = recover_engine_and_thresholds_blocking()

    if STARTUP_FULL_RESCAN_TODAY:
        try:
            boot_end_day = today_yyyymmdd()
            reset_today_cursors(engine, boot_end_day)
            log("[BOOT] startup full rescan enabled -> reset today cursors end_day={0}".format(boot_end_day), info="boot")
        except Exception as e:
            log("[ERROR] startup cursor reset failed: {0}: {1}".format(type(e).__name__, e), info="error")
            log(traceback.format_exc(), info="error")
            raise

    first_run_pending = bool(STARTUP_FORCE_FIRST_RUN_FULLSCAN)

    while True:
        loop_t0 = now_ts()

        try:
            engine_health_check_blocking(engine)
            main_once(engine, th_map, force_first_fullscan=first_run_pending)

            if first_run_pending:
                first_run_pending = False

        except Exception as e:
            log("[ERROR] loop failed: {0}: {1}".format(type(e).__name__, e), info="error")
            log(traceback.format_exc(), info="error")

            try:
                if engine is not None:
                    engine.dispose()
            except Exception:
                pass

            global DB_LOG_ENGINE
            DB_LOG_ENGINE = None

            log("[RECOVER] db error suspected -> blocking reconnect ...", info="recover")
            engine, th_map = recover_engine_and_thresholds_blocking()

        loop_t1 = now_ts()
        elapsed = (loop_t1 - loop_t0).total_seconds()
        sleep_sec = LOOP_INTERVAL_SEC - elapsed
        if sleep_sec < 0.0:
            sleep_sec = 0.0

        log("[LOOP] end | elapsed={0:.3f}s | sleep={1:.3f}s".format(elapsed, sleep_sec), info="sleep")
        if sleep_sec > 0:
            time_mod.sleep(sleep_sec)


# =========================
# 9) entry
# =========================
def main():
    try:
        realtime_loop()
    except KeyboardInterrupt:
        log("[EXIT] KeyboardInterrupt", info="exit")
        hold_console(exit_code=0)
    except Exception as e:
        log("[FATAL] {0}: {1}".format(type(e).__name__, e), info="fatal")
        log(traceback.format_exc(), info="fatal")
        hold_console(exit_code=1)


if __name__ == "__main__":
    try:
        mp.freeze_support()
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    main()