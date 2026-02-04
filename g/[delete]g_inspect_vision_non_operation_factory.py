# -*- coding: utf-8 -*-
"""
Factory Realtime - Vision Non Operation Time Inspector (cursor incremental)

목적
- d1_machine_log.Vision1~2_machine_log 에서
  a) "검사 양품 신호 출력" 또는 "검사 불량 신호 출력" 이후
  b) "바코드 스캔 신호 수신" 까지의 시간차를 no_operation_time(초)로 계산
- g_production_film.op_ct_gap 의 del_out_av(Vision1/Vision2) 임계값과 비교하여
  no_operation_time > del_out_av 인 경우만 이벤트로 확정(real_no_operation_time=1)
- 이벤트를 g_production_film.vision_non_operation_time 로 UPSERT 저장
- (end_day, station)별 커서(last_end_ts) 테이블로 중복 처리 방지

요청 반영(수정/추가)
- DB: 100.105.75.47:5432/postgres
- 멀티프로세스: 1개 (병렬 계산 제거/단일 프로세스 순차 처리)
- 무한 루프: 5초 주기
- ✅ 실행 중간에 서버가 끊겨도, "연결 성공할 때까지" 무한 재접속(블로킹) + 정상 복구 후 재개
- 백엔드별 상시 연결을 1개로 고정(풀 최소화: pool_size=1, max_overflow=0)
- work_mem 폭증 방지: 세션 단위 SET work_mem 적용 (+옵션 statement_timeout)
- 콘솔: 매 루프 시작/종료 시간 출력, EXE 실행 시 콘솔 자동 종료 방지

추가(진단/디버그)
- 실행 폴더에 diag_YYYYMMDD_HHMMSS.log 로그 파일 생성
- 시작 시 ping/port/psycopg2/SQLAlchemy 진단
- EXE 환경에서 multiprocessing 안정화(freeze_support)
"""

import os
import sys
import time as time_mod
import socket
import traceback
import subprocess
import multiprocessing as mp
from datetime import datetime
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
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "d1_machine_log"
VISION_TABLES = {
    "Vision1": "Vision1_machine_log",
    "Vision2": "Vision2_machine_log",
}

SAVE_SCHEMA = "g_production_film"
SAVE_TABLE  = "vision_non_operation_time"

CURSOR_SCHEMA = "g_production_film"
CURSOR_TABLE  = "vision_non_operation_time_cursor"

# ✅ 요청 반영
MAX_WORKERS = 1             # 멀티프로세스 1개 (사실상 순차 처리)
LOOP_INTERVAL_SEC = 5.0     # 무한 루프 5초
STABLE_DATA_SEC = 2.0

# DB 재연결/재시도
DB_CONNECT_TIMEOUT_SEC = 5
RETRY_SLEEP_SEC = 10

# ✅ work_mem 폭증 방지 (필요 시 조정)
WORK_MEM = "4MB"
STATEMENT_TIMEOUT_MS = None  # 예: 60000 (1분) / 미사용이면 None


# =========================
# 0-1) DIAG + FILE LOG
# =========================
def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def _default_log_path():
    try:
        base = os.path.dirname(sys.executable if getattr(sys, "frozen", False) else __file__)
    except Exception:
        base = os.getcwd()
    return os.path.join(base, "diag_{0}.log".format(datetime.now().strftime("%Y%m%d_%H%M%S")))

LOG_PATH = _default_log_path()

def log(msg: str):
    s = "[{0}] {1}".format(_now_str(), msg)
    print(s, flush=True)
    try:
        with open(LOG_PATH, "a", encoding="utf-8") as f:
            f.write(s + "\n")
    except Exception:
        pass

def hold_console(exit_code: int = 0):
    log("[HOLD] press Enter to exit (exit_code={0})".format(exit_code))
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
    log("[PING] host={0} rc={1}".format(host, rc))
    if out:
        log("[PING-OUT] {0}".format(out[:500]))
    if err:
        log("[PING-ERR] {0}".format(err[:500]))
    return rc == 0

def socket_port_check(host: str, port: int, timeout_sec: float = 3.0):
    t0 = time_mod.time()
    try:
        with socket.create_connection((host, port), timeout=timeout_sec):
            dt = time_mod.time() - t0
            log("[SOCKET] {0}:{1} OK ({2:.2f}s)".format(host, port, dt))
            return True
    except Exception as e:
        dt = time_mod.time() - t0
        log("[SOCKET] {0}:{1} FAIL ({2:.2f}s) err={3}: {4}".format(host, port, dt, type(e).__name__, e))
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
        log("[PSYCOPG2] OK result={0} ({1:.2f}s)".format(r, time_mod.time() - t0))
        return True
    except Exception as e:
        log("[PSYCOPG2] FAIL err={0}: {1}".format(type(e).__name__, e))
        return False

def build_db_url(cfg):
    pw = urllib.parse.quote_plus(cfg["password"])
    return "postgresql+psycopg2://{u}:{p}@{h}:{pt}/{d}".format(
        u=cfg["user"], p=pw, h=cfg["host"], pt=cfg["port"], d=cfg["dbname"]
    )

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
        log("[SQLA] OK result={0} ({1:.2f}s)".format(r, time_mod.time() - t0))
        return True
    except Exception as e:
        log("[SQLA] FAIL err={0}: {1}".format(type(e).__name__, e))
        return False

def print_env_header():
    log("=" * 110)
    log("[START] DIAG HEADER")
    log("frozen={0} | exe={1}".format(getattr(sys, "frozen", False), sys.executable))
    log("cwd={0}".format(os.getcwd()))
    try:
        log("hostname={0}".format(socket.gethostname()))
    except Exception:
        pass
    try:
        rc, out, err = run_cmd(["whoami"])
        log("whoami rc={0} out={1} err={2}".format(rc, out, err))
    except Exception:
        pass
    log("log_file={0}".format(LOG_PATH))
    safe_url = build_db_url(DB_CONFIG).replace(urllib.parse.quote_plus(DB_CONFIG["password"]), "***")
    log("[DB] host={0} port={1} db={2} user={3}".format(DB_CONFIG["host"], DB_CONFIG["port"], DB_CONFIG["dbname"], DB_CONFIG["user"]))
    log("[DB_URL] {0}".format(safe_url))
    log("work_mem={0} statement_timeout_ms={1}".format(WORK_MEM, STATEMENT_TIMEOUT_MS))
    log("=" * 110)

def run_diagnostics():
    print_env_header()
    host = DB_CONFIG["host"]
    port = int(DB_CONFIG["port"])
    safe_ping(host)
    socket_port_check(host, port, timeout_sec=3.0)
    psycopg2_quick_test(**DB_CONFIG, timeout_sec=DB_CONNECT_TIMEOUT_SEC)
    sqlalchemy_quick_test(build_db_url(DB_CONFIG), timeout_sec=DB_CONNECT_TIMEOUT_SEC)


# =========================
# 1) 유틸
# =========================
def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")

def now_ts() -> datetime:
    return datetime.now()

def _print_run_banner(tag: str, start_dt: datetime):
    log("=" * 110)
    log("[{0}] {1}".format(tag, start_dt.strftime("%Y-%m-%d %H:%M:%S")))
    log("=" * 110)

def _print_end_banner(tag: str, start_dt: datetime, end_dt: datetime):
    log("-" * 110)
    log("[{0}] {1} | elapsed={2}".format(tag, end_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt - start_dt))
    log("-" * 110)

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
    """
    ✅ 실행 중간 연결 끊김/네트워크 단절/서버 재시작 등을 최대한 넓게 감지.
    - OperationalError
    - DBAPIError(connection_invalidated)
    - 메시지 기반(일부 환경)
    """
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

def _session_guard(conn):
    """
    work_mem/timeout을 세션에서 한번 더 고정(옵션 무시/세션변경 대비).
    """
    try:
        conn.execute(text("SET work_mem TO '{0}';".format(WORK_MEM)))
        if STATEMENT_TIMEOUT_MS is not None:
            conn.execute(text("SET statement_timeout TO {0};".format(int(STATEMENT_TIMEOUT_MS))))
    except Exception:
        pass

def _make_engine_one_pool():
    """
    pool_size=1, max_overflow=0 로 상시 연결 1개 수준으로 제한.
    work_mem은 options로 기본값을 낮춤.
    """
    db_url = build_db_url(DB_CONFIG)

    opt = "-c work_mem={0}".format(WORK_MEM)
    if STATEMENT_TIMEOUT_MS is not None:
        opt = opt + " -c statement_timeout={0}".format(int(STATEMENT_TIMEOUT_MS))

    eng = create_engine(
        db_url,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        connect_args={
            "connect_timeout": int(DB_CONNECT_TIMEOUT_SEC),
            "application_name": "vision_nonop_realtime",
            "options": opt,
        },
    )
    return eng

def init_engine_blocking():
    """
    ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
    """
    while True:
        eng = None
        try:
            eng = _make_engine_one_pool()
            with eng.connect() as conn:
                _session_guard(conn)
                conn.execute(text("SELECT 1"))
            log("[OK] engine connect test passed (single-pool fixed)")
            return eng
        except Exception as e:
            log("[ERROR] DB connect failed: {0}: {1}".format(type(e).__name__, e))
            log(traceback.format_exc())
            try:
                if eng is not None:
                    eng.dispose()
            except Exception:
                pass
            log("[RETRY] sleep {0}s then retry DB connect".format(RETRY_SLEEP_SEC))
            time_mod.sleep(RETRY_SLEEP_SEC)

def engine_health_check_blocking(engine):
    """
    ✅ 루프 중간/업무 중간에 연결이 끊긴 경우를 빠르게 감지.
    끊김/장애면 예외를 발생시켜 상위 복구 루틴을 타게 함.
    """
    try:
        with engine.connect() as conn:
            _session_guard(conn)
            conn.execute(text("SELECT 1"))
        return True
    except Exception:
        raise

def recover_engine_and_thresholds_blocking():
    """
    ✅ '연결 성공할 때까지' 엔진 재생성 + thresholds 로드도 성공할 때까지 반복.
    """
    while True:
        engine = init_engine_blocking()

        th_map = None
        while th_map is None:
            try:
                th_map = load_thresholds(engine)
                log("[OK] thresholds loaded: {0}".format(th_map))
            except Exception as e:
                if is_db_disconnect_error(e):
                    log("[WARN] thresholds load hit DB disconnect -> re-init engine (blocking)")
                    try:
                        engine.dispose()
                    except Exception:
                        pass
                    engine = None
                    break  # 바깥 while로 재접속
                log("[ERROR] load_thresholds failed: {0}: {1}".format(type(e).__name__, e))
                log(traceback.format_exc())
                log("[RETRY] sleep {0}s then retry thresholds".format(RETRY_SLEEP_SEC))
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

    cur = {"Vision1": None, "Vision2": None}
    for _, r in df.iterrows():
        cur[str(r["station"])] = r["last_end_ts"]
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
# 3) 임계값 로드(op_ct_gap)
# =========================
def load_thresholds(engine):
    q = text("""
        SELECT station, del_out_av
        FROM g_production_film.op_ct_gap
        WHERE station IN ('Vision1', 'Vision2')
    """)
    df = pd.read_sql(q, engine)
    if df.empty:
        raise RuntimeError("[ERROR] g_production_film.op_ct_gap 에서 ('Vision1','Vision2') 데이터를 찾지 못했습니다.")
    df["del_out_av"] = pd.to_numeric(df["del_out_av"], errors="coerce")
    th_map = {}
    for st, v in zip(df["station"].astype(str), df["del_out_av"]):
        try:
            th_map[st] = float(v)
        except Exception:
            th_map[st] = np.nan
    return th_map


# =========================
# 4) 소스 로딩(증분)
# =========================
A_PREFIXES = ("검사 양품 신호 출력", "검사 불량 신호 출력")
B_PREFIX = "바코드 스캔 신호 수신"
VALID_PREFIXES = A_PREFIXES + (B_PREFIX,)

def load_vision_incremental(engine, end_day: str, station: str, last_end_ts):
    tbl = VISION_TABLES[station]
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
# 5) 이벤트 계산(Station 1개 처리) - 순차 처리
# =========================
def compute_events_for_station(station: str, df_station: pd.DataFrame, th_map: dict):
    """
    return:
      station, max_ts_in_input(or None), events_df
      events_df columns: end_day, station, from_time, to_time, no_operation_time
    """
    if df_station is None or df_station.empty:
        return station, None, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df_station.copy()
    df["contents"] = df["contents"].astype(str)
    df = df[df["contents"].str.startswith(VALID_PREFIXES)].copy()

    if df.empty:
        mx = df_station["_ts"].max() if ("_ts" in df_station.columns and not df_station.empty) else None
        mx_out = mx.to_pydatetime() if (mx is not None and pd.notna(mx)) else None
        return station, mx_out, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df.sort_values(["_ts"], ascending=True).reset_index(drop=True)
    df["no_operation_time"] = np.nan

    last_a_idx = None
    i = 0
    while i < len(df):
        c = df.at[i, "contents"]
        if c.startswith(A_PREFIXES):
            last_a_idx = i
            i += 1
            continue

        if c.startswith(B_PREFIX):
            if last_a_idx is not None:
                t_a = df.at[last_a_idx, "_ts"]
                t_b = df.at[i, "_ts"]
                if pd.notna(t_a) and pd.notna(t_b) and t_b >= t_a:
                    df.at[i, "no_operation_time"] = float(round((t_b - t_a).total_seconds(), 2))
            last_a_idx = None
        i += 1

    thr = th_map.get(station, np.nan)
    try:
        thr_val = float(thr)
    except Exception:
        thr_val = np.nan

    df["real_no_operation_time"] = np.where(
        df["no_operation_time"].notna() & (df["no_operation_time"] > thr_val),
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
        df_st = load_vision_incremental(engine, end_day=end_day, station=st, last_end_ts=last_ts)
        station_dfs[st] = df_st
        total_loaded += len(df_st)

        if len(df_st) > 0:
            log("[LOAD] {0} {1}: rows={2} (cursor={3})".format(end_day, st, len(df_st), last_ts))
        else:
            log("[SKIP] {0} {1}: 신규 없음 (cursor={2})".format(end_day, st, last_ts))

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
            log("[EVT] {0}: events={1}".format(st_name, len(ev)))
        else:
            log("[EVT] {0}: events=0".format(st_name))

        if max_ts is not None:
            cursor_updates.append((st_name, max_ts))

    if len(all_events) > 0:
        df_save = pd.concat(all_events, ignore_index=True)
        inserted = upsert_events(engine, df_save)
        log("[SAVE] {0}.{1}: upsert rows={2}".format(SAVE_SCHEMA, SAVE_TABLE, inserted))
    else:
        log("[SAVE] {0}.{1}: no events -> skip".format(SAVE_SCHEMA, SAVE_TABLE))

    for st, max_ts in cursor_updates:
        upsert_cursor(engine, end_day=end_day, station=st, last_end_ts=max_ts)
        log("[CURSOR] {0} {1} -> {2}".format(end_day, st, max_ts))

    run_end = now_ts()
    _print_end_banner("DONE", run_start, run_end)


# =========================
# 8) Realtime loop (DB 끊김 포함: 블로킹 재접속)
# =========================
def realtime_loop():
    log("[BOOT] realtime_loop start")
    run_diagnostics()

    # ✅ 최초 엔진/threshold: 연결 성공까지 블로킹
    engine, th_map = recover_engine_and_thresholds_blocking()

    while True:
        loop_t0 = now_ts()

        try:
            # ✅ 루프 시작 시점 헬스체크(중간 끊김 즉시 감지)
            engine_health_check_blocking(engine)

            # 1) 본 실행
            main_once(engine, th_map)

        except Exception as e:
            log("[ERROR] loop failed: {0}: {1}".format(type(e).__name__, e))
            log(traceback.format_exc())

            # ✅ DB 끊김/장애 포함: 여기서 "연결 성공할 때까지" 복구
            try:
                if engine is not None:
                    engine.dispose()
            except Exception:
                pass

            log("[RECOVER] server disconnect or db error suspected -> blocking reconnect ...")
            engine, th_map = recover_engine_and_thresholds_blocking()

        # 2) 루프 주기 맞추기 (5초)
        loop_t1 = now_ts()
        elapsed = (loop_t1 - loop_t0).total_seconds()
        sleep_sec = LOOP_INTERVAL_SEC - elapsed
        if sleep_sec < 0.0:
            sleep_sec = 0.0
        log("[LOOP] end | elapsed={0:.3f}s | sleep={1:.3f}s".format(elapsed, sleep_sec))
        if sleep_sec > 0:
            time_mod.sleep(sleep_sec)


# =========================
# 9) Entry
# =========================
def main():
    try:
        realtime_loop()
    except KeyboardInterrupt:
        log("[EXIT] KeyboardInterrupt")
        hold_console(exit_code=0)
    except Exception as e:
        log("[FATAL] {0}: {1}".format(type(e).__name__, e))
        log(traceback.format_exc())
        hold_console(exit_code=1)

if __name__ == "__main__":
    # Windows EXE + multiprocessing 안정화
    try:
        mp.freeze_support()
        mp.set_start_method("spawn", force=True)
    except Exception:
        pass

    main()
