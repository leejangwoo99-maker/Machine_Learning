# -*- coding: utf-8 -*-
"""
Factory Realtime - FCT Non Operation Time Inspector (ID cursor incremental)

[0초 이벤트 방지 보강]
- "TEST RESULT" 직후 "TEST AUTO MODE START"가 같은 타임스탬프(또는 순서상 같게 파싱)인 경우
  no_operation_time=0.00 이 발생할 수 있음
- 정책: no_operation_time <= 0 인 이벤트는 "비정상 이벤트"로 보지 않고 적재하지 않음(skip)

구현:
1) diff 계산 시 t_b == t_a 또는 역전/파싱 문제 -> no_operation_time 미부여(NaN 유지)
2) real_no_operation_time 판정에 (no_operation_time > 0) 조건 추가
3) out 구성 전에 no_operation_time > 0 행만 필터링
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

import numpy as np
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
    "password": "",  # 보안 개선 대상
}

SRC_SCHEMA = "d1_machine_log"
FCT_TABLES = {
    "FCT1": "FCT1_machine_log",
    "FCT2": "FCT2_machine_log",
    "FCT3": "FCT3_machine_log",
    "FCT4": "FCT4_machine_log",
}

# 저장 대상
SAVE_SCHEMA = "g_production_film"
SAVE_TABLE = "fct_non_operation_time"

# 커서(중복방지) - ID 기반
CURSOR_SCHEMA = "g_production_film"
CURSOR_TABLE = "fct_non_operation_time_cursor"

# Realtime
MAX_WORKERS = 1
LOOP_INTERVAL_SEC = 5.0
STABLE_DATA_SEC = 1.0  # 아주 최근 insert row 회피(선택)

# DB 재연결/재시도
DB_CONNECT_TIMEOUT_SEC = 5
RETRY_SLEEP_SEC = 10

# work_mem/timeout
WORK_MEM = "4MB"
STATEMENT_TIMEOUT_MS = 60000  # ✅ 영원 대기 방지 (60초 권장)

# KST
KST = ZoneInfo("Asia/Seoul")

# =========================
# 0-1) 로그 DB 설정
# =========================
LOG_DB_SCHEMA = "k_demon_heath_check"
LOG_DB_TABLE = "gf_log"
DB_LOG_ENGINE = None
_DB_LOG_GUARD = False

INFO_ALLOW = {
    "info", "error", "down", "sleep", "warn", "boot", "run", "done", "load", "save",
    "skip", "evt", "cursor", "recover", "retry", "exit", "fatal", "diag", "socket",
    "ping", "db", "heartbeat"
}


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


def _save_log_to_db(msg: str, info: str = None):
    global _DB_LOG_GUARD, DB_LOG_ENGINE
    if DB_LOG_ENGINE is None or _DB_LOG_GUARD:
        return

    try:
        _DB_LOG_GUARD = True
        dt = datetime.now()
        info_val = (info or _infer_info(msg) or "info").strip().lower() or "info"

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
    log("[DB] host={0} port={1} db={2} user={3}".format(DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"], DB_CONFIG["user"]), info="db")
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


# =========================
# 1-1) 엔진/연결: 상시 1개 고정 + 무한 재시도(블로킹)
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
# 2) 커서 테이블 (ID 기반)
# =========================
def ensure_cursor_table(engine):
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {CURSOR_SCHEMA};
    CREATE TABLE IF NOT EXISTS {CURSOR_SCHEMA}.{CURSOR_TABLE} (
        end_day      TEXT NOT NULL,
        station      TEXT NOT NULL,
        last_id      BIGINT NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (end_day, station)
    );
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl)

    ddl2 = text(f"""
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema='{CURSOR_SCHEMA}'
              AND table_name='{CURSOR_TABLE}'
              AND column_name='last_id'
        ) THEN
            ALTER TABLE {CURSOR_SCHEMA}.{CURSOR_TABLE} ADD COLUMN last_id BIGINT NULL;
        END IF;
    END $$;
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl2)


def load_cursors(engine, end_day: str) -> dict:
    ensure_cursor_table(engine)
    q = text(f"""
        SELECT station, last_id
        FROM {CURSOR_SCHEMA}.{CURSOR_TABLE}
        WHERE end_day = :end_day
    """)
    df = pd.read_sql(q, engine, params={"end_day": end_day})

    cur = {"FCT1": None, "FCT2": None, "FCT3": None, "FCT4": None}
    for _, r in df.iterrows():
        st = str(r["station"])
        cur[st] = r["last_id"]
    return cur


def upsert_cursor(engine, end_day: str, station: str, last_id: int):
    ensure_cursor_table(engine)
    q = text(f"""
        INSERT INTO {CURSOR_SCHEMA}.{CURSOR_TABLE} (end_day, station, last_id, updated_at)
        VALUES (:end_day, :station, :last_id, now())
        ON CONFLICT (end_day, station)
        DO UPDATE SET last_id = EXCLUDED.last_id,
                      updated_at = now()
    """)
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(q, {"end_day": end_day, "station": station, "last_id": int(last_id)})


# =========================
# 3) 임계값 로드 (fct_op_criteria)
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
# 4) 소스 로딩(증분) - ID 기반
# =========================
RESULT_PREFIXES = ("TEST RESULT :: OK", "TEST RESULT :: NG")
AUTO_START_PREFIX = "TEST AUTO MODE START"
VALID_PREFIXES = RESULT_PREFIXES + (AUTO_START_PREFIX,)


def load_fct_incremental(engine, end_day: str, station: str, last_id):
    tbl = FCT_TABLES[station]

    stable_cut = datetime.now() - pd.Timedelta(seconds=STABLE_DATA_SEC)

    if last_id is None or (isinstance(last_id, float) and pd.isna(last_id)):
        q = text(f"""
            SELECT id, end_day, station, end_time, contents
            FROM {SRC_SCHEMA}."{tbl}"
            WHERE end_day = :end_day
            ORDER BY id ASC
        """)
        params = {"end_day": end_day}
    else:
        q = text(f"""
            SELECT id, end_day, station, end_time, contents
            FROM {SRC_SCHEMA}."{tbl}"
            WHERE end_day = :end_day
              AND id > :last_id
            ORDER BY id ASC
        """)
        params = {"end_day": end_day, "last_id": int(last_id)}

    df = pd.read_sql(q, engine, params=params)
    if df.empty:
        return df

    df["contents"] = df["contents"].astype(str)

    ts = pd.to_datetime(df["end_day"].astype(str) + " " + df["end_time"].astype(str), errors="coerce")
    df["_ts"] = ts
    df = df[df["_ts"].notna()].copy()

    df = df[df["_ts"] <= stable_cut].copy()

    return df.reset_index(drop=True)


# =========================
# 5) 이벤트 계산(Station 1개 처리)
# =========================
def compute_events_for_station(station: str, df_station: pd.DataFrame, th_map: dict):
    if df_station is None or df_station.empty:
        return station, None, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df_station.copy()
    df["contents"] = df["contents"].astype(str)
    df = df[df["contents"].str.startswith(VALID_PREFIXES)].copy()

    if df.empty:
        max_id = int(df_station["id"].max()) if ("id" in df_station.columns and not df_station.empty) else None
        return station, max_id, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df.sort_values(["_ts", "id"], ascending=True).reset_index(drop=True)
    df["no_operation_time"] = np.nan

    last_a_idx = None
    i = 0
    while i < len(df):
        c = df.at[i, "contents"]
        if c.startswith(RESULT_PREFIXES):
            last_a_idx = i
            i += 1
            continue

        if c.startswith(AUTO_START_PREFIX):
            if last_a_idx is not None:
                t_a = df.at[last_a_idx, "_ts"]
                t_b = df.at[i, "_ts"]
                # ✅ 0초/역전은 이벤트로 보지 않음
                if pd.notna(t_a) and pd.notna(t_b) and (t_b > t_a):
                    diff = (t_b - t_a).total_seconds()
                    if diff > 0:
                        df.at[i, "no_operation_time"] = float(round(diff, 2))
            last_a_idx = None
        i += 1

    th_val = threshold_for_station(th_map, station)
    df["threshold"] = th_val

    # ✅ 0초/NaN은 이벤트 불가
    df["real_no_operation_time"] = np.where(
        df["no_operation_time"].notna() & (df["no_operation_time"] > 0) & (df["no_operation_time"] > df["threshold"]),
        1, 0
    )

    df["end_time_str"] = df["end_time"].astype(str)
    df["to_time"] = np.where(df["real_no_operation_time"] == 1, df["end_time_str"], np.nan)

    df["from_time"] = np.where(df["real_no_operation_time"] == 1, df["end_time_str"].shift(1), np.nan)

    out = df.loc[df["real_no_operation_time"] == 1, ["end_day", "station", "from_time", "to_time", "no_operation_time"]].copy()

    # ✅ 최종 안전망: 혹시라도 0초가 남아있으면 제거
    out["no_operation_time"] = pd.to_numeric(out["no_operation_time"], errors="coerce")
    out = out[out["no_operation_time"].notna() & (out["no_operation_time"] > 0)].copy()

    out["end_day"] = out["end_day"].astype(str)
    out["station"] = out["station"].astype(str)
    out["from_time"] = out["from_time"].astype(str)
    out["to_time"] = out["to_time"].astype(str)

    max_id = int(df_station["id"].max()) if ("id" in df_station.columns and not df_station.empty) else None
    return station, max_id, out.reset_index(drop=True)


# =========================
# 6) 저장(UPSERT)
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

    # ✅ 여기서도 0초 방어 (이중 안전)
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
# 7) 1회 실행
# =========================
def main_once(engine, th_map):
    run_start = now_ts()
    _print_run_banner("RUN", run_start)

    end_day = today_yyyymmdd()
    cursors = load_cursors(engine, end_day=end_day)

    total_loaded = 0
    station_dfs = {}

    log(f"[HEARTBEAT] loop begin end_day={end_day}", info="heartbeat")

    for st, last_id in cursors.items():
        log(f"[LOAD-START] {end_day} {st} last_id={last_id}", info="load")
        df_st = load_fct_incremental(engine, end_day=end_day, station=st, last_id=last_id)
        station_dfs[st] = df_st
        total_loaded += len(df_st)
        log(f"[LOAD-END] {end_day} {st} rows={len(df_st)}", info="load")

    if total_loaded == 0:
        run_end = now_ts()
        log("[SKIP] no new rows for all stations", info="skip")
        _print_end_banner("DONE", run_start, run_end)
        return

    all_events = []
    cursor_updates = []

    for st, st_df in station_dfs.items():
        st_name, max_id, ev = compute_events_for_station(st, st_df, th_map)

        if ev is not None and not ev.empty:
            all_events.append(ev)
            log("[EVT] {0}: events={1}".format(st_name, len(ev)), info="evt")
        else:
            log("[EVT] {0}: events=0".format(st_name), info="evt")

        if max_id is not None:
            cursor_updates.append((st_name, max_id))

    if len(all_events) > 0:
        df_save = pd.concat(all_events, ignore_index=True)

        # ✅ 최종 방어 (0초 제거)
        df_save["no_operation_time"] = pd.to_numeric(df_save["no_operation_time"], errors="coerce")
        df_save = df_save[df_save["no_operation_time"].notna() & (df_save["no_operation_time"] > 0)].copy()

        inserted = upsert_events(engine, df_save)
        log("[SAVE] {0}.{1}: upsert rows={2}".format(SAVE_SCHEMA, SAVE_TABLE, inserted), info="save")
    else:
        log("[SAVE] {0}.{1}: no events -> skip".format(SAVE_SCHEMA, SAVE_TABLE), info="skip")

    for st, max_id in cursor_updates:
        upsert_cursor(engine, end_day=end_day, station=st, last_id=max_id)
        log("[CURSOR] {0} {1} -> last_id={2}".format(end_day, st, max_id), info="cursor")

    run_end = now_ts()
    _print_end_banner("DONE", run_start, run_end)


# =========================
# 8) Realtime loop
# =========================
def realtime_loop():
    log("[BOOT] realtime_loop start", info="boot")
    run_diagnostics()

    engine, th_map = recover_engine_and_thresholds_blocking()

    while True:
        loop_t0 = now_ts()

        try:
            engine_health_check_blocking(engine)
            main_once(engine, th_map)

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
# 9) Entry
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