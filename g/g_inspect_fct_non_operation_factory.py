# -*- coding: utf-8 -*-
"""
Factory Realtime - FCT Non Operation Time Inspector (cursor incremental)

목적
- d1_machine_log.FCT1~4_machine_log 에서
  a) "TEST RESULT :: OK|NG" 이후
  b) "TEST AUTO MODE START" 까지의 시간차를 no_operation_time(초)로 계산
- (변경) g_production_film.fct_op_criteria 의 임계값(upper_outlier max)과 비교하여
  no_operation_time > threshold 인 경우만 이벤트로 확정(real_no_operation_time=1)
- 이벤트를 g_production_film.fct_non_operation_time 로 UPSERT 저장
- (end_day, station)별로 마지막 처리 end_ts 를 커서 테이블에 저장하여 중복 처리 방지

임계값 사양(확정 반영)
- 스키마: g_production_film, 테이블: fct_op_criteria
- 컬럼 month: TEXT(yyyymm)
- 1) KST 기준 "이전 달" month를 우선 선택
- 2) 해당 month 행들의 upper_outlier(double precision) 중 최댓값을 임계값으로 사용
- 3) (C) 이전 달이 없으면 테이블에 존재하는 최신 month(MAX(month))로 fallback 후,
       그 month의 upper_outlier 최댓값을 사용
- 4) station 컬럼 없음 → FCT1~4 모두 동일 임계값 적용(초 단위)

요청 반영(수정/추가)
- DB: 100.105.75.47:5432/postgres
- 멀티프로세스: 1개 (단일 프로세스 순차 처리)
- 무한 루프: 5초 주기
- ✅ 실행 중간에 서버가 끊겨도, "연결 성공할 때까지" 무한 재접속(블로킹) + 정상 복구 후 재개
- 풀 최소화: pool_size=1, max_overflow=0
- work_mem 폭증 방지: 세션 단위 SET work_mem 적용 (+옵션 statement_timeout)
- 콘솔: 매 루프 시작/종료 시간 출력, EXE 실행 시 콘솔 자동 종료 방지

추가(로그 DB 저장)
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: gf_log (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 시 컬럼 순서: end_day, end_time, info, contents
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
    "host": "100.105.75.47",   # ✅ 여기로 고정
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
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
SAVE_TABLE  = "fct_non_operation_time"

# 커서(중복방지)
CURSOR_SCHEMA = "g_production_film"
CURSOR_TABLE  = "fct_non_operation_time_cursor"

# Realtime
MAX_WORKERS = 1              # ✅ 멀티프로세스=1 (단일 프로세스/순차 처리)
LOOP_INTERVAL_SEC = 5.0      # ✅ 무한 루프 5초
STABLE_DATA_SEC = 2.0        # 방금 적재된 row 회피용(선택적)

# DB 재연결/재시도
DB_CONNECT_TIMEOUT_SEC = 5
RETRY_SLEEP_SEC = 10

# ✅ work_mem 폭증 방지
WORK_MEM = "4MB"             # 필요 시 2MB~16MB 등으로 조정
STATEMENT_TIMEOUT_MS = None  # 예: 60000 (1분). 미사용이면 None

# ✅ 임계값 기준 시간대: KST
KST = ZoneInfo("Asia/Seoul")

# =========================
# 0-1) 로그 DB 설정
# =========================
LOG_DB_SCHEMA = "k_demon_heath_check"
LOG_DB_TABLE = "gf_log"
DB_LOG_ENGINE = None
_DB_LOG_GUARD = False  # 재귀 방지

INFO_ALLOW = {"info", "error", "down", "sleep", "warn", "boot", "run", "done", "load", "save", "skip", "evt", "cursor", "recover", "retry", "exit", "fatal", "diag", "socket", "ping", "db"}

def _infer_info(msg: str) -> str:
    """
    msg prefix에서 info 추론. 반드시 소문자 반환.
    예) [ERROR] -> error, [RETRY] -> retry
    """
    if not msg:
        return "info"
    m = re.match(r"^\[([A-Za-z0-9_ -]+)\]", str(msg).strip())
    if m:
        tag = m.group(1).strip().lower().replace(" ", "_")
        # 대표 매핑
        if tag in ("err", "error", "fatal"):
            return "error" if tag != "fatal" else "fatal"
        if tag in ("warning",):
            return "warn"
        if tag in INFO_ALLOW:
            return tag
        # 허용 외 태그는 info로 표준화
        return "info"
    # 키워드 기반 보정
    low = str(msg).lower()
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
    """
    6) end_day, end_time, info, contents 순서 dataframe화 후 저장
    """
    global _DB_LOG_GUARD, DB_LOG_ENGINE

    if DB_LOG_ENGINE is None:
        return
    if _DB_LOG_GUARD:
        return

    try:
        _DB_LOG_GUARD = True
        dt = datetime.now()
        info_val = (info or _infer_info(msg) or "info").strip().lower()
        if not info_val:
            info_val = "info"

        df = pd.DataFrame(
            [{
                "end_day": dt.strftime("%Y%m%d"),
                "end_time": dt.strftime("%H:%M:%S"),
                "info": info_val,
                "contents": str(msg),
            }],
            columns=["end_day", "end_time", "info", "contents"]  # ✅ 컬럼 순서 고정
        )

        ins = text(f"""
            INSERT INTO {LOG_DB_SCHEMA}.{LOG_DB_TABLE}
            (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """)

        rows = df.to_dict(orient="records")
        with DB_LOG_ENGINE.begin() as conn:
            _session_guard(conn)
            conn.execute(ins, rows)

    except Exception:
        # DB 로그 실패는 콘솔/파일만 유지 (재귀 방지)
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

    # DB 저장 (요청 반영)
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


def psycopg2_quick_test(host, port, dbname, user, password, timeout_sec=5):
    try:
        import psycopg2
        t0 = time_mod.time()
        conn = psycopg2.connect(
            host=host, port=port, dbname=dbname, user=user, password=password,
            connect_timeout=int(timeout_sec),
            application_name="diag_psycopg2_probe",
        )
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        r = cur.fetchone()
        conn.close()
        log("[PSYCOPG2] OK result={0} ({1:.2f}s)".format(r, time_mod.time() - t0), info="diag")
        return True
    except Exception as e:
        log("[PSYCOPG2] FAIL err={0}: {1}".format(type(e).__name__, e), info="error")
        return False


def sqlalchemy_quick_test(db_url: str, timeout_sec=5):
    try:
        from sqlalchemy import create_engine as _create_engine, text as _text
        t0 = time_mod.time()
        eng = _create_engine(
            db_url,
            pool_pre_ping=True,
            pool_recycle=300,
            pool_size=1,
            max_overflow=0,
            pool_timeout=30,
            connect_args={
                "connect_timeout": int(timeout_sec),
                "application_name": "diag_sqla_probe",
                "options": "-c work_mem={0}".format(WORK_MEM),
            },
        )
        with eng.connect() as conn:
            r = conn.execute(_text("SELECT 1")).scalar()
        log("[SQLA] OK result={0} ({1:.2f}s)".format(r, time_mod.time() - t0), info="diag")
        return True
    except Exception as e:
        log("[SQLA] FAIL err={0}: {1}".format(type(e).__name__, e), info="error")
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
    try:
        rc, out, err = run_cmd(["whoami"])
        log("whoami rc={0} out={1} err={2}".format(rc, out, err), info="boot")
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
    psycopg2_quick_test(**DB_CONFIG, timeout_sec=DB_CONNECT_TIMEOUT_SEC)
    sqlalchemy_quick_test(build_db_url(DB_CONFIG), timeout_sec=DB_CONNECT_TIMEOUT_SEC)


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


def parse_ts(end_day: str, end_time: str) -> pd.Timestamp:
    d = str(end_day).strip()
    t = str(end_time).strip()
    ts = pd.to_datetime("{0} {1}".format(d, t), format="%Y%m%d %H:%M:%S.%f", errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime("{0} {1}".format(d, t), format="%Y%m%d %H:%M:%S", errors="coerce")
    return ts


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


def _session_guard(conn):
    try:
        conn.execute(text("SET work_mem TO '{0}';".format(WORK_MEM)))
        if STATEMENT_TIMEOUT_MS is not None:
            conn.execute(text("SET statement_timeout TO {0};".format(int(STATEMENT_TIMEOUT_MS))))
    except Exception:
        pass


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

        # 로그 테이블 준비 + DB_LOG_ENGINE 바인딩
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
# 2) 커서 테이블
# =========================
def ensure_cursor_table(engine):
    ddl = text("""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        end_day      TEXT NOT NULL,
        station      TEXT NOT NULL,
        last_end_ts  TIMESTAMP NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (end_day, station)
    );
    """.format(schema=CURSOR_SCHEMA, table=CURSOR_TABLE))
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl)


def load_cursors(engine, end_day: str) -> dict:
    ensure_cursor_table(engine)
    q = text("""
        SELECT station, last_end_ts
        FROM {schema}.{table}
        WHERE end_day = :end_day
    """.format(schema=CURSOR_SCHEMA, table=CURSOR_TABLE))
    df = pd.read_sql(q, engine, params={"end_day": end_day})

    cur = {"FCT1": None, "FCT2": None, "FCT3": None, "FCT4": None}
    for _, r in df.iterrows():
        st = str(r["station"])
        cur[st] = r["last_end_ts"]
    return cur


def upsert_cursor(engine, end_day: str, station: str, last_end_ts: datetime):
    ensure_cursor_table(engine)
    q = text("""
        INSERT INTO {schema}.{table} (end_day, station, last_end_ts, updated_at)
        VALUES (:end_day, :station, :last_end_ts, now())
        ON CONFLICT (end_day, station)
        DO UPDATE SET last_end_ts = EXCLUDED.last_end_ts,
                      updated_at  = now()
    """.format(schema=CURSOR_SCHEMA, table=CURSOR_TABLE))
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(q, {"end_day": end_day, "station": station, "last_end_ts": last_end_ts})


# =========================
# 3) 임계값 로드 (NEW: fct_op_criteria)
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
    mx = None
    if not df.empty:
        mx = df.loc[0, "mx"]

    if mx is not None and pd.notna(mx):
        return {"ALL": float(mx), "month": str(target_month), "fallback": False}

    q_latest_month = text("""
        SELECT MAX(month) AS latest_month
        FROM g_production_film.fct_op_criteria
    """)
    dfm = pd.read_sql(q_latest_month, engine)
    latest_month = None
    if not dfm.empty:
        latest_month = dfm.loc[0, "latest_month"]

    if latest_month is None or str(latest_month).strip() == "":
        raise RuntimeError("[ERROR] g_production_film.fct_op_criteria 테이블에 month 데이터가 없습니다.")

    latest_month = str(latest_month).strip()

    df2 = pd.read_sql(q_max, engine, params={"month": latest_month})
    mx2 = None
    if not df2.empty:
        mx2 = df2.loc[0, "mx"]

    if mx2 is None or pd.isna(mx2):
        raise RuntimeError(
            f"[ERROR] fct_op_criteria month={latest_month} 행은 있으나 upper_outlier MAX가 NULL 입니다."
        )

    return {"ALL": float(mx2), "month": latest_month, "fallback": True}


def threshold_for_station(th_map: dict, station: str) -> float:
    return float(th_map["ALL"])


# =========================
# 4) 소스 로딩(증분)
# =========================
def load_fct_incremental(engine, end_day: str, station: str, last_end_ts):
    tbl = FCT_TABLES[station]
    q = text("""
        SELECT end_day, :station AS station, contents, end_time
        FROM {schema}."{tbl}"
        WHERE end_day = :end_day
        ORDER BY end_time ASC
    """.format(schema=SRC_SCHEMA, tbl=tbl))

    df = pd.read_sql(q, engine, params={"end_day": end_day, "station": station})
    if df.empty:
        return df

    ts_list = []
    for d, t in zip(df["end_day"], df["end_time"]):
        ts_list.append(parse_ts(d, t))
    df["_ts"] = ts_list
    df = df[df["_ts"].notna()].copy()

    stable_cut = pd.Timestamp(now_ts() - pd.Timedelta(seconds=STABLE_DATA_SEC))
    df = df[df["_ts"] <= stable_cut].copy()

    if last_end_ts is not None and pd.notna(last_end_ts):
        df = df[df["_ts"] > pd.Timestamp(last_end_ts)].copy()

    return df.reset_index(drop=True)


# =========================
# 5) 이벤트 계산(Station 1개 처리)
# =========================
RESULT_PREFIXES = ("TEST RESULT :: OK", "TEST RESULT :: NG")
AUTO_START_PREFIX = "TEST AUTO MODE START"
VALID_PREFIXES = RESULT_PREFIXES + (AUTO_START_PREFIX,)


def compute_events_for_station(station: str, df_station: pd.DataFrame, th_map: dict):
    if df_station is None or df_station.empty:
        return station, None, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df_station.copy()
    df["contents"] = df["contents"].astype(str)
    df = df[df["contents"].str.startswith(VALID_PREFIXES)].copy()

    if df.empty:
        mx = None
        if "_ts" in df_station.columns and not df_station.empty:
            mx = df_station["_ts"].max()
        mx_out = mx.to_pydatetime() if (mx is not None and pd.notna(mx)) else None
        return station, mx_out, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df.sort_values(["_ts"], ascending=True).reset_index(drop=True)
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
                if pd.notna(t_a) and pd.notna(t_b) and t_b >= t_a:
                    df.at[i, "no_operation_time"] = float(round((t_b - t_a).total_seconds(), 2))
            last_a_idx = None
        i += 1

    th_val = threshold_for_station(th_map, station)
    df["threshold"] = th_val
    df["real_no_operation_time"] = np.where(
        df["no_operation_time"].notna() & (df["no_operation_time"] > df["threshold"]),
        1, 0
    )

    df["end_time_str"] = df["end_time"].astype(str)
    df["to_time"] = np.where(df["real_no_operation_time"] == 1, df["end_time_str"], np.nan)
    df["from_time"] = np.where(df["real_no_operation_time"] == 1, df["end_time_str"].shift(1), np.nan)

    out = df.loc[df["real_no_operation_time"] == 1, ["end_day", "station", "from_time", "to_time", "no_operation_time"]].copy()
    out["end_day"] = out["end_day"].astype(str)
    out["station"] = out["station"].astype(str)
    out["from_time"] = out["from_time"].astype(str)
    out["to_time"] = out["to_time"].astype(str)

    max_ts = df_station["_ts"].max()
    max_ts_out = max_ts.to_pydatetime() if (pd.notna(max_ts)) else None
    return station, max_ts_out, out.reset_index(drop=True)


# =========================
# 6) 저장(UPSERT)
# =========================
def ensure_target_table(engine):
    ddl = text("""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        end_day           TEXT NOT NULL,
        station           TEXT NOT NULL,
        from_time         TEXT NOT NULL,
        to_time           TEXT NOT NULL,
        no_operation_time NUMERIC(12,2),
        created_at        TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (end_day, station, from_time, to_time)
    );
    """.format(schema=SAVE_SCHEMA, table=SAVE_TABLE))
    with engine.begin() as conn:
        _session_guard(conn)
        conn.execute(ddl)


def upsert_events(engine, df_events: pd.DataFrame) -> int:
    if df_events is None or df_events.empty:
        return 0

    ensure_target_table(engine)
    upsert_sql = text("""
        INSERT INTO {schema}.{table}
        (end_day, station, from_time, to_time, no_operation_time)
        VALUES (:end_day, :station, :from_time, :to_time, :no_operation_time)
        ON CONFLICT (end_day, station, from_time, to_time)
        DO UPDATE SET no_operation_time = EXCLUDED.no_operation_time;
    """.format(schema=SAVE_SCHEMA, table=SAVE_TABLE))

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

    station_dfs = {}
    total_loaded = 0

    for st, last_ts in cursors.items():
        df_st = load_fct_incremental(engine, end_day=end_day, station=st, last_end_ts=last_ts)
        station_dfs[st] = df_st
        total_loaded += len(df_st)
        if len(df_st) > 0:
            log("[LOAD] {0} {1}: rows={2} (cursor={3})".format(end_day, st, len(df_st), last_ts), info="load")
        else:
            log("[SKIP] {0} {1}: 신규 없음 (cursor={2})".format(end_day, st, last_ts), info="skip")

    if total_loaded == 0:
        run_end = now_ts()
        _print_end_banner("DONE", run_start, run_end)
        return

    all_events = []
    cursor_updates = []

    for st in station_dfs.keys():
        st_df = station_dfs.get(st)
        st_name, max_ts, ev = compute_events_for_station(st, st_df, th_map)
        if ev is not None and not ev.empty:
            all_events.append(ev)
            log("[EVT] {0}: events={1}".format(st_name, len(ev)), info="evt")
        else:
            log("[EVT] {0}: events=0".format(st_name), info="evt")
        if max_ts is not None:
            cursor_updates.append((st_name, max_ts))

    if len(all_events) > 0:
        df_save = pd.concat(all_events, ignore_index=True)
        inserted = upsert_events(engine, df_save)
        log("[SAVE] {0}.{1}: upsert rows={2}".format(SAVE_SCHEMA, SAVE_TABLE, inserted), info="save")
    else:
        log("[SAVE] {0}.{1}: no events -> skip".format(SAVE_SCHEMA, SAVE_TABLE), info="skip")

    for st, max_ts in cursor_updates:
        upsert_cursor(engine, end_day=end_day, station=st, last_end_ts=max_ts)
        log("[CURSOR] {0} {1} -> {2}".format(end_day, st, max_ts), info="cursor")

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

            # DB 로그 엔진도 끊김으로 간주
            global DB_LOG_ENGINE
            DB_LOG_ENGINE = None

            log("[RECOVER] server disconnect or db error suspected -> blocking reconnect ...", info="recover")
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
