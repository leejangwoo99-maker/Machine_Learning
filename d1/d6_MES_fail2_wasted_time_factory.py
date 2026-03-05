# -*- coding: utf-8 -*-
"""
d6_MES_fail2_wasted_time.py  (REPLACEMENT / OPTION-A 안정화 교체본)
-------------------------------------------------
핵심 목표: "멈춘 것처럼 보이는 현상" 방지 + DB/네트워크 불안정에도 계속 동작

✅ 적용 사항(요청 반영 + 안정화):
- 무한루프 5초
- DB 접속 실패/실행 중 끊김 시: engine dispose 후 "연결 성공까지" 무한 재시도(블로킹)
- pool 최소화: pool_size=1, max_overflow=0, pool_pre_ping=True
- work_mem 폭증 방지: ✅ SET work_mem 제거, connect_args options로 고정
- ✅ statement_timeout / lock_timeout / idle_in_tx_timeout 강제 (hang 방지)
- 증분 커서: DEST last_pk 이후만(>)
- Warm-start Backfill:
  - 부팅 직후 1회
  - recover(재연결) 직후 1회
  - lookback(기본 60분) 되감기
  - ON CONFLICT DO NOTHING (덮어쓰기/0초기화 없음)

✅ 데몬 헬스 로그 DB 저장(버퍼링 후 주기 flush):
- schema: k_demon_heath_check (없으면 생성)
- table : d6_log (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 순서: end_day, end_time, info, contents

주의:
- DB_CONFIG.password는 운영 환경에서 외부 주입 권장(.env 등)
"""

from __future__ import annotations

import os
import time
import urllib.parse
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError


# =========================
# 0) CONFIG
# =========================
KST = ZoneInfo("Asia/Seoul")

os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",  # ⚠ 운영에서는 환경변수/시크릿으로 주입 권장
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE = "fct_vision_testlog_txt_processing_history"

CT_FQN = '"e1_FCT_ct"."fct_whole_op_ct"'  # 대문자 스키마 대응

DST_SCHEMA = "d1_machine_log"
DST_TABLE = "mes_fail2_wasted_time"
DST_FQN = f'{DST_SCHEMA}."{DST_TABLE}"' if DST_TABLE.lower() != DST_TABLE else f"{DST_SCHEMA}.{DST_TABLE}"

LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "d6_log"
LOG_FQN = f"{LOG_SCHEMA}.{LOG_TABLE}"

STATIONS = ("FCT1", "FCT2", "FCT3", "FCT4")
GOODORBAD_VALUE = "BadFile"

LOOP_SLEEP_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

# 세션 옵션(핵심: hang 방지)
WORK_MEM = os.getenv("PG_WORK_MEM", "64MB")
STMT_TIMEOUT_MS = int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "15000"))          # 15s
LOCK_TIMEOUT_MS = int(os.getenv("PG_LOCK_TIMEOUT_MS", "5000"))               # 5s
IDLE_IN_TX_TIMEOUT_MS = int(os.getenv("PG_IDLE_IN_TX_TIMEOUT_MS", "15000"))  # 15s

# keepalive (환경/망 불안 대비)
ENABLE_PG_KEEPALIVE = os.getenv("ENABLE_PG_KEEPALIVE", "1").strip().lower() not in ("0", "false", "off")
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

# Warm-start backfill lookback
WARM_START_LOOKBACK_MIN = int(os.getenv("WARM_START_LOOKBACK_MIN", "60"))

# 헬스 로그: 버퍼 flush 주기
HEALTH_FLUSH_INTERVAL_SEC = int(os.getenv("D6_HEALTH_FLUSH_SEC", "5"))
HEALTH_BUFFER_MAX = int(os.getenv("D6_HEALTH_BUFFER_MAX", "500"))


# =========================
# 1) GLOBALS
# =========================
_ENGINE: Optional[Engine] = None
_health_buf: List[Dict[str, str]] = []
_last_health_flush_ts: float = 0.0


# =========================
# 2) LOG (console + DB buffer)
# =========================
def _now_kst() -> datetime:
    return datetime.now(tz=KST)


def _masked_db() -> str:
    c = DB_CONFIG
    return f"postgresql://{c['user']}:***@{c['host']}:{c['port']}/{c['dbname']}"


def log_console(msg: str) -> None:
    ts = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def _health_row(info: str, contents: str) -> Dict[str, str]:
    now = _now_kst()
    return {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "info").strip().lower(),
        "contents": str(contents)[:4000],
    }


def logx(engine: Optional[Engine], info: str, contents: str) -> None:
    # console
    log_console(f"[{(info or '').strip().lower()}] {contents}")

    # buffer (DB best-effort, flush는 주기적으로)
    try:
        _health_buf.append(_health_row(info, contents))
        if len(_health_buf) >= HEALTH_BUFFER_MAX:
            flush_health_logs(engine, force=True)
    except Exception:
        pass


# =========================
# 3) DB ENGINE (옵션 강제 + 무한 재접속)
# =========================
def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError)):
        return True
    msg = (str(e) or "").lower()
    keys = [
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
    ]
    return any(k in msg for k in keys)


def _dispose_engine() -> None:
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


def _build_engine() -> Engine:
    user = DB_CONFIG["user"]
    password = urllib.parse.quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    dbname = DB_CONFIG["dbname"]

    # ✅ 핵심: SET work_mem 제거하고 options로 세션 파라미터 고정
    options = (
        f"-c work_mem={WORK_MEM} "
        f"-c statement_timeout={STMT_TIMEOUT_MS} "
        f"-c lock_timeout={LOCK_TIMEOUT_MS} "
        f"-c idle_in_transaction_session_timeout={IDLE_IN_TX_TIMEOUT_MS} "
        f"-c client_encoding=UTF8"
    )

    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        "?connect_timeout=5"
    )

    connect_args = {
        "connect_timeout": 5,
        "application_name": "d6_MES_fail2_wasted_time",
        "options": options,
    }

    if ENABLE_PG_KEEPALIVE:
        connect_args.update({
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
        })

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args=connect_args,
    )


def get_engine_blocking() -> Engine:
    """
    ✅ DB 연결 성공까지 무한 재시도(블로킹)
    ✅ 엔진 1개 고정, ping 실패하면 dispose 후 재생성
    """
    global _ENGINE

    # 1) existing ping
    while _ENGINE is not None:
        try:
            with _ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            return _ENGINE
        except Exception as e:
            log_console(f"[DB][RETRY] ping failed -> rebuild | {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)

    # 2) create & connect
    while True:
        try:
            _ENGINE = _build_engine()
            with _ENGINE.connect() as conn:
                conn.execute(text("SELECT 1"))
            ka = (
                f"{PG_KEEPALIVES}/{PG_KEEPALIVES_IDLE}/{PG_KEEPALIVES_INTERVAL}/{PG_KEEPALIVES_COUNT}"
                if ENABLE_PG_KEEPALIVE else "off"
            )
            log_console(
                "[DB][OK] engine ready "
                f"(pool=1/0 work_mem={WORK_MEM} stmt_timeout={STMT_TIMEOUT_MS}ms lock_timeout={LOCK_TIMEOUT_MS}ms "
                f"idle_in_tx={IDLE_IN_TX_TIMEOUT_MS}ms keepalive={ka})"
            )
            return _ENGINE
        except Exception as e:
            log_console(f"[DB][RETRY] connect failed | {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 4) DDL
# =========================
def ensure_dest_table(engine: Engine) -> None:
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {DST_SCHEMA};

    CREATE TABLE IF NOT EXISTS {DST_FQN} (
        barcode_information  TEXT NOT NULL,
        end_day              TEXT NOT NULL,   -- YYYYMMDD
        end_time             TEXT NOT NULL,   -- HH:MM:SS

        station              TEXT,
        remark               TEXT,
        result               TEXT,
        goodorbad            TEXT,
        final_ct             NUMERIC,

        df_updated_at        TIMESTAMPTZ,

        CONSTRAINT pk_mes_fail2_wasted_time
            PRIMARY KEY (barcode_information, end_day, end_time)
    );
    """)
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(ddl)
            return
        except Exception as e:
            if _is_connection_error(e):
                log_console(f"[DB][RETRY] ensure_dest_table conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_console(f"[DB][RETRY] ensure_dest_table failed | {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)


def ensure_demon_log_table(engine: Engine) -> None:
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {LOG_FQN} (
        end_day   TEXT NOT NULL,
        end_time  TEXT NOT NULL,
        info      TEXT NOT NULL,
        contents  TEXT
    );
    """)
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(ddl)
            return
        except Exception as e:
            if _is_connection_error(e):
                log_console(f"[DB][RETRY] ensure_demon_log_table conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_console(f"[DB][RETRY] ensure_demon_log_table failed | {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 5) HEALTH LOG FLUSH (buffered)
# =========================
def flush_health_logs(engine: Optional[Engine], force: bool = False) -> None:
    global _last_health_flush_ts, _health_buf
    if engine is None:
        return
    if not _health_buf:
        return

    now_ts = time.time()
    if (not force) and (now_ts - _last_health_flush_ts) < HEALTH_FLUSH_INTERVAL_SEC:
        return

    df = pd.DataFrame(_health_buf, columns=["end_day", "end_time", "info", "contents"])
    if df.empty:
        _last_health_flush_ts = now_ts
        _health_buf = []
        return

    ins = text(f"""
        INSERT INTO {LOG_FQN} (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
    """)

    rows = df.to_dict("records")

    try:
        with engine.begin() as conn:
            conn.execute(ins, rows)
        _health_buf = []
        _last_health_flush_ts = now_ts
    except Exception as e:
        # 헬스 로그는 best-effort: 실패해도 메인 루프를 막지 않음
        log_console(f"[LOG-DB][SKIP] {type(e).__name__}: {e}")
        _last_health_flush_ts = now_ts


# =========================
# 6) CURSOR
# =========================
LastPK = Tuple[str, str, str]  # (end_day, end_time, barcode_information)


def get_last_pk(engine: Engine) -> Optional[LastPK]:
    sql = text(f"""
    SELECT end_day, end_time, barcode_information
    FROM {DST_FQN}
    ORDER BY end_day DESC, end_time DESC, barcode_information DESC
    LIMIT 1
    """)
    while True:
        try:
            with engine.connect() as conn:
                row = conn.execute(sql).mappings().first()
            if not row:
                return None
            return (str(row["end_day"]), str(row["end_time"]), str(row["barcode_information"]))
        except Exception as e:
            if _is_connection_error(e):
                log_console(f"[DB][RETRY] get_last_pk conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
                continue
            # 비연결 오류는 그대로 raise (상위에서 recover)
            raise


# =========================
# 7) SQL builder + FETCH
# =========================
def build_fetch_sql(extra_where: str) -> str:
    return f"""
    WITH src_raw AS (
        SELECT
            s.barcode_information,
            s.station,
            s.end_day,
            s.end_time,
            s.remark,
            s.result,
            s.goodorbad,

            to_timestamp(s.end_day || ' ' || s.end_time, 'YYYYMMDD HH24:MI:SS') AS end_ts,
            to_char((to_date(s.end_day, 'YYYYMMDD') - interval '1 month'), 'YYYYMM') AS prev_month,

            CASE
                WHEN upper(regexp_replace(trim(s.remark), '[\\s_]+', '', 'g')) = 'PD' THEN 'PD'
                WHEN upper(replace(replace(regexp_replace(trim(s.remark), '[\\s_]+', '', 'g'), '–', '-'), '—', '-'))
                     IN ('NON-PD','NONPD') THEN 'Non-PD'
                ELSE NULL
            END AS remark_norm
        FROM {SRC_SCHEMA}.{SRC_TABLE} s
        WHERE s.station = ANY(:stations)
          AND s.goodorbad = :goodorbad
          {extra_where}
    ),
    ct AS (
        SELECT month::text AS month, remark::text AS remark, final_ct
        FROM {CT_FQN}
        WHERE station = 'whole'
          AND remark IN ('PD','Non-PD')
    )
    SELECT
        x.barcode_information,
        x.end_day,
        x.end_time,
        x.station,
        x.remark,
        x.result,
        x.goodorbad,
        c.final_ct
    FROM src_raw x
    LEFT JOIN ct c
      ON c.month  = x.prev_month
     AND c.remark = x.remark_norm
    ORDER BY x.end_day, x.end_time, x.barcode_information
    """


def fetch_new_rows(engine: Engine, last_pk: Optional[LastPK]) -> pd.DataFrame:
    params = {"stations": list(STATIONS), "goodorbad": GOODORBAD_VALUE}
    extra_where = ""

    if last_pk is not None:
        last_end_day, last_end_time, last_barcode = last_pk
        extra_where = """
          AND (s.end_day, s.end_time, s.barcode_information)
              > (:last_end_day, :last_end_time, :last_barcode)
        """
        params.update({
            "last_end_day": last_end_day,
            "last_end_time": last_end_time,
            "last_barcode": last_barcode,
        })

    sql = build_fetch_sql(extra_where)

    while True:
        try:
            with engine.connect() as conn:
                df = pd.read_sql_query(text(sql), conn, params=params)
            return df
        except Exception as e:
            if _is_connection_error(e):
                log_console(f"[DB][RETRY] fetch_new_rows conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
                continue
            raise


# =========================
# 8) INSERT (executemany, SKIP DUP)
# =========================
def insert_rows(engine: Engine, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    df_updated_at = _now_kst()

    insert_cols = [
        "barcode_information",
        "end_day",
        "end_time",
        "station",
        "remark",
        "result",
        "goodorbad",
        "final_ct",
        "df_updated_at",
    ]

    df2 = df.copy()
    df2["df_updated_at"] = df_updated_at

    # 최소 유효성 필터
    df2 = df2[
        df2["barcode_information"].notna()
        & (df2["barcode_information"].astype(str).str.len() > 0)
        & df2["end_day"].notna()
        & (df2["end_day"].astype(str).str.len() == 8)
        & df2["end_time"].notna()
        & (df2["end_time"].astype(str).str.len() > 0)
    ].copy()

    if df2.empty:
        return 0

    rows = df2[insert_cols].to_dict(orient="records")

    ins = text(f"""
    INSERT INTO {DST_FQN}
        (barcode_information, end_day, end_time, station, remark, result, goodorbad, final_ct, df_updated_at)
    VALUES
        (:barcode_information, :end_day, :end_time, :station, :remark, :result, :goodorbad, :final_ct, :df_updated_at)
    ON CONFLICT (barcode_information, end_day, end_time) DO NOTHING
    """)

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(ins, rows)
            return len(rows)
        except Exception as e:
            if _is_connection_error(e):
                log_console(f"[DB][RETRY] insert_rows conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
                continue
            raise


# =========================
# 9) WARM-START BACKFILL
# =========================
def warm_start_backfill(engine: Engine, info_engine: Optional[Engine]) -> int:
    last_pk = get_last_pk(engine)

    if last_pk is None:
        now_kst = _now_kst()
        start_dt = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts_str = start_dt.strftime("%Y%m%d %H:%M:%S")
        logx(info_engine, "info", f"[WARM] no last_pk -> bootstrap from KST today 00:00:00 ({start_ts_str})")
    else:
        last_end_day, last_end_time, _ = last_pk
        last_dt = datetime.strptime(f"{last_end_day} {last_end_time}", "%Y%m%d %H:%M:%S").replace(tzinfo=KST)
        start_dt = last_dt - timedelta(minutes=WARM_START_LOOKBACK_MIN)
        start_ts_str = start_dt.strftime("%Y%m%d %H:%M:%S")
        logx(
            info_engine,
            "info",
            f"[WARM] last_pk={last_pk} -> backfill from {start_ts_str} (lookback={WARM_START_LOOKBACK_MIN}m)",
        )

    params = {
        "stations": list(STATIONS),
        "goodorbad": GOODORBAD_VALUE,
        "start_ts": start_ts_str,
    }

    extra_where = """
      AND to_timestamp(s.end_day || ' ' || s.end_time, 'YYYYMMDD HH24:MI:SS')
          >= to_timestamp(:start_ts, 'YYYYMMDD HH24:MI:SS')
    """

    sql = build_fetch_sql(extra_where)

    while True:
        try:
            with engine.connect() as conn:
                df = pd.read_sql_query(text(sql), conn, params=params)
            inserted = insert_rows(engine, df)
            logx(info_engine, "info", f"[WARM] fetched={len(df)} inserted_attempt={inserted} (duplicates skipped)")
            return inserted
        except Exception as e:
            if _is_connection_error(e):
                logx(info_engine, "down", f"[WARM][RETRY] conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
                continue
            # 비연결 오류는 warm만 스킵하고 0 리턴
            logx(info_engine, "error", f"[WARM][SKIP] {type(e).__name__}: {e}")
            return 0


# =========================
# 10) MAIN
# =========================
def main() -> None:
    log_console("[BOOT] start d6_MES_fail2_wasted_time (replacement)")
    log_console(f"[CFG ] db={_masked_db()} sleep={LOOP_SLEEP_SEC}s work_mem={WORK_MEM}")
    log_console(f"[CFG ] stmt_timeout={STMT_TIMEOUT_MS}ms lock_timeout={LOCK_TIMEOUT_MS}ms idle_in_tx={IDLE_IN_TX_TIMEOUT_MS}ms")
    log_console(f"[CFG ] warm_start_lookback_min={WARM_START_LOOKBACK_MIN} stations={STATIONS} goodorbad={GOODORBAD_VALUE}")
    log_console(f"[CFG ] dst={DST_FQN} log={LOG_FQN}")

    engine = get_engine_blocking()

    # DDL ensure (blocking)
    ensure_dest_table(engine)
    ensure_demon_log_table(engine)
    logx(engine, "info", "[DDL] ensured dest table + demon log table")

    # Warm-start 1회
    try:
        warm_start_backfill(engine, engine)
    except Exception as e:
        logx(engine, "error", f"[WARM][ERROR] {type(e).__name__}: {e}")

    while True:
        loop_t0 = time.perf_counter()

        try:
            # ping (필요 시 rebuild)
            engine = get_engine_blocking()

            logx(engine, "info", "[LOOP] tick")

            last_pk = get_last_pk(engine)
            logx(engine, "info", f"[CURSOR] last_pk={last_pk}")

            df_new = fetch_new_rows(engine, last_pk)
            logx(engine, "info", f"[FETCH] rows={len(df_new)}")

            n = insert_rows(engine, df_new)
            logx(engine, "info", f"[WRITE] inserted_attempt={n} (duplicates skipped by PK)")

            # health flush
            flush_health_logs(engine, force=False)

        except KeyboardInterrupt:
            logx(engine, "down", "[STOP] KeyboardInterrupt")
            flush_health_logs(engine, force=True)
            return

        except Exception as e:
            # 어떤 에러든 recover는 "재연결"로 통일
            logx(engine, "error", f"[ERROR] {type(e).__name__}: {e}")
            flush_health_logs(engine, force=False)

            logx(engine, "down", "[RECOVER] dispose engine -> reconnect blocking...")
            try:
                _dispose_engine()
            except Exception:
                pass

            engine = get_engine_blocking()

            try:
                ensure_dest_table(engine)
                ensure_demon_log_table(engine)
                logx(engine, "info", "[DDL] ensured tables (after recover)")
            except Exception as e2:
                logx(engine, "error", f"[RECOVER][DDL][ERROR] {type(e2).__name__}: {e2}")

            # recover 직후 누락 보정용 warm 1회
            try:
                warm_start_backfill(engine, engine)
            except Exception as e3:
                logx(engine, "error", f"[RECOVER][WARM][ERROR] {type(e3).__name__}: {e3}")

        # pacing
        elapsed = time.perf_counter() - loop_t0
        sleep_left = max(0.0, LOOP_SLEEP_SEC - elapsed)
        logx(engine, "sleep", f"sleep {sleep_left:.2f}s")
        flush_health_logs(engine, force=False)
        time.sleep(sleep_left)


if __name__ == "__main__":
    main()