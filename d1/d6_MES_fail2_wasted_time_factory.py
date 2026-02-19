# -*- coding: utf-8 -*-
"""
d6_MES_fail2_wasted_time.py
-------------------------------------------------
추가 반영: Warm-start Backfill (부팅 시 1회)
- DEST(last_pk) 기준으로 일정 시간(기본 60분) 되감기하여 소스 재조회 -> 누락 보충
- DEST 비어있으면: KST 오늘 00:00:00부터 부트스트랩
- 저장은 ON CONFLICT DO NOTHING -> 기존 데이터 덮어쓰기/0초기화 없음

운영 사양:
- 무한루프 5초
- DB 접속 실패 시 무한 재시도(블로킹)
- 실행 중 DB 끊김 발생 시에도 무한 재접속 후 계속 진행
- pool 최소화: pool_size=1, max_overflow=0, pool_pre_ping=True
- work_mem 폭증 방지: 세션마다 SET work_mem
- 증분 커서: DEST의 last_pk 이후 데이터만(>) 조회

[추가] 데몬 헬스 로그 DB 저장
- schema: k_demon_heath_check (없으면 생성)
- table : d6_log (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 순서: end_day, end_time, info, contents
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError, OperationalError, InterfaceError
from psycopg2.extras import execute_values


# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE = "fct_vision_testlog_txt_processing_history"

CT_FQN = '"e1_FCT_ct"."fct_whole_op_ct"'  # 대문자 스키마 대응

DST_SCHEMA = "d1_machine_log"
DST_TABLE = "mes_fail2_wasted_time"
DST_FQN = f"{DST_SCHEMA}.{DST_TABLE}"

# ✅ 데몬 로그 저장 대상
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "d6_log"
LOG_FQN = f"{LOG_SCHEMA}.{LOG_TABLE}"

STATIONS = ("FCT1", "FCT2", "FCT3", "FCT4")
GOODORBAD_VALUE = "BadFile"

LOOP_SLEEP_SEC = 5
KST = ZoneInfo("Asia/Seoul")

# work_mem (원하면 환경변수 PG_WORK_MEM로 조절)
PG_WORK_MEM = os.getenv("PG_WORK_MEM", "64MB")

# Warm-start backfill 되감기(분): 재시작/장애 시 누락 보충용
WARM_START_LOOKBACK_MIN = int(os.getenv("WARM_START_LOOKBACK_MIN", "60"))


# =========================
# LOG (console)
# =========================
def log(msg: str) -> None:
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


# =========================
# ENGINE + SESSION GUARD
# =========================
def make_engine() -> Engine:
    return create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
    )


def set_session_guards(conn) -> None:
    conn.execute(text(f"SET work_mem TO '{PG_WORK_MEM}';"))


def get_engine_blocking() -> Engine:
    """DB 붙을 때까지 무한 재시도"""
    while True:
        eng = None
        try:
            log("[DB] creating engine...")
            eng = make_engine()
            log("[DB] connect test...")
            with eng.connect() as conn:
                set_session_guards(conn)
                conn.execute(text("SELECT 1"))
            log("[DB] connected OK")
            return eng
        except Exception as e:
            log(f"[RETRY] DB connect failed: {type(e).__name__}: {e}")
            try:
                if eng is not None:
                    eng.dispose()
            except Exception:
                pass
            time.sleep(LOOP_SLEEP_SEC)


def is_disconnect_error(e: Exception) -> bool:
    """
    SQLAlchemy가 감지한 disconnect 계열이면 True.
    (DBAPIError.connection_invalidated 활용)
    """
    if isinstance(e, DBAPIError):
        return bool(getattr(e, "connection_invalidated", False))
    if isinstance(e, (OperationalError, InterfaceError)):
        return True
    return False


# =========================
# DDL: DEST TABLE + PK
# =========================
def ensure_dest_table(engine: Engine) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {DST_SCHEMA};

    CREATE TABLE IF NOT EXISTS {DST_FQN} (
        barcode_information  TEXT NOT NULL,
        end_day              TEXT NOT NULL,   -- YYYYMMDD
        end_time             TEXT NOT NULL,   -- HH:MM:SS (text)

        station              TEXT,
        remark               TEXT,
        result               TEXT,
        goodorbad            TEXT,
        final_ct             NUMERIC,

        df_updated_at        TIMESTAMPTZ,

        CONSTRAINT pk_mes_fail2_wasted_time
            PRIMARY KEY (barcode_information, end_day, end_time)
    );
    """
    with engine.begin() as conn:
        set_session_guards(conn)
        conn.execute(text(ddl))


# =========================
# DDL: DEMON HEALTH LOG TABLE
# =========================
def ensure_demon_log_table(engine: Engine) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {LOG_FQN} (
        end_day   TEXT NOT NULL,   -- yyyymmdd
        end_time  TEXT NOT NULL,   -- hh:mi:ss
        info      TEXT NOT NULL,   -- 반드시 소문자 저장
        contents  TEXT
    );
    """
    with engine.begin() as conn:
        set_session_guards(conn)
        conn.execute(text(ddl))


def write_demon_log(engine: Engine | None, info: str, contents: str) -> None:
    """
    DB 헬스 로그 저장.
    - info는 강제로 소문자화
    - 컬럼 순서(end_day, end_time, info, contents)로 DataFrame 생성 후 저장
    - 본 함수 실패는 상위 로직 중단시키지 않음
    """
    if engine is None:
        return

    try:
        now = datetime.now(KST)
        row = {
            "end_day": now.strftime("%Y%m%d"),
            "end_time": now.strftime("%H:%M:%S"),
            "info": (info or "").strip().lower(),
            "contents": str(contents) if contents is not None else None,
        }

        df_log = pd.DataFrame([row], columns=["end_day", "end_time", "info", "contents"])
        records = [tuple(r) for r in df_log.itertuples(index=False, name=None)]
        if not records:
            return

        sql_ins = f"""
        INSERT INTO {LOG_FQN} (end_day, end_time, info, contents)
        VALUES %s
        """

        with engine.begin() as conn:
            set_session_guards(conn)
            raw = conn.connection
            with raw.cursor() as cur:
                execute_values(
                    cur,
                    sql_ins,
                    records,
                    template="(%s, %s, %s, %s)",
                    page_size=1000,
                )
    except Exception as e:
        # 로그 저장 실패는 콘솔만 남기고 무시
        log(f"[LOG-DB][ERROR] {type(e).__name__}: {e}")


def logx(engine: Engine | None, info: str, contents: str) -> None:
    """
    콘솔 + DB 로그 동시 기록
    """
    msg = f"[{info.lower()}] {contents}"
    log(msg)
    write_demon_log(engine, info, contents)


# =========================
# CURSOR: last saved PK
# =========================
def get_last_pk(engine: Engine) -> tuple[str, str, str] | None:
    sql = f"""
    SELECT end_day, end_time, barcode_information
    FROM {DST_FQN}
    ORDER BY end_day DESC, end_time DESC, barcode_information DESC
    LIMIT 1
    """
    with engine.connect() as conn:
        set_session_guards(conn)
        row = conn.execute(text(sql)).mappings().first()
    if not row:
        return None
    return (str(row["end_day"]), str(row["end_time"]), str(row["barcode_information"]))


# =========================
# SQL builder (공통)
# =========================
def build_fetch_sql(extra_where: str) -> str:
    """
    extra_where: src_raw WHERE 절에 추가할 조건 문자열(앞에 AND ... 형태)
    """
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

            -- warm-start/backfill 비교용 (end_day/end_time은 text지만 비교 안정성 위해 ts도 만들어 둠)
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


# =========================
# FETCH: incremental (last_pk 이후만)
# =========================
def fetch_new_rows(engine: Engine, last_pk: tuple[str, str, str] | None) -> pd.DataFrame:
    params = {"stations": list(STATIONS), "goodorbad": GOODORBAD_VALUE}
    extra_where = ""

    if last_pk is not None:
        last_end_day, last_end_time, last_barcode = last_pk
        extra_where = """
          AND (s.end_day, s.end_time, s.barcode_information)
              > (:last_end_day, :last_end_time, :last_barcode)
        """
        params.update(
            {
                "last_end_day": last_end_day,
                "last_end_time": last_end_time,
                "last_barcode": last_barcode,
            }
        )

    sql = build_fetch_sql(extra_where)

    with engine.connect() as conn:
        set_session_guards(conn)
        df = pd.read_sql_query(text(sql), conn, params=params)

    return df


# =========================
# WARM-START BACKFILL (부팅 시 1회)
# =========================
def warm_start_backfill(engine: Engine) -> int:
    last_pk = get_last_pk(engine)

    if last_pk is None:
        now_kst = datetime.now(KST)
        start_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0)
        start_ts_str = start_kst.strftime("%Y%m%d %H:%M:%S")
        logx(engine, "info", f"[WARM] no last_pk -> bootstrap from KST today 00:00:00 ({start_ts_str})")
    else:
        last_end_day, last_end_time, _ = last_pk
        last_dt = datetime.strptime(f"{last_end_day} {last_end_time}", "%Y%m%d %H:%M:%S").replace(tzinfo=KST)
        start_dt = last_dt - timedelta(minutes=WARM_START_LOOKBACK_MIN)
        start_ts_str = start_dt.strftime("%Y%m%d %H:%M:%S")
        logx(
            engine,
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

    with engine.connect() as conn:
        set_session_guards(conn)
        df = pd.read_sql_query(text(sql), conn, params=params)

    inserted = insert_rows(engine, df)
    logx(engine, "info", f"[WARM] fetched={len(df)} inserted_attempt={inserted} (duplicates skipped)")
    return inserted


# =========================
# INSERT (SKIP DUP)
# =========================
def insert_rows(engine: Engine, df: pd.DataFrame) -> int:
    if df.empty:
        return 0

    df_updated_at = datetime.now(KST)

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

    df2 = df2[
        df2["barcode_information"].notna()
        & (df2["barcode_information"].astype(str).str.len() > 0)
        & df2["end_day"].notna()
        & (df2["end_day"].astype(str).str.len() > 0)
        & df2["end_time"].notna()
        & (df2["end_time"].astype(str).str.len() > 0)
    ].copy()

    if df2.empty:
        return 0

    def _py(v):
        if v is None:
            return None
        if pd.isna(v):
            return None
        return v

    records = [tuple(_py(v) for v in row) for row in df2[insert_cols].itertuples(index=False, name=None)]
    if not records:
        return 0

    sql_ins = f"""
    INSERT INTO {DST_FQN} ({", ".join(insert_cols)})
    VALUES %s
    ON CONFLICT (barcode_information, end_day, end_time) DO NOTHING;
    """

    with engine.begin() as conn:
        set_session_guards(conn)
        raw = conn.connection
        with raw.cursor() as cur:
            execute_values(
                cur,
                sql_ins,
                records,
                template=f"({', '.join(['%s'] * len(insert_cols))})",
                page_size=2000,
            )

    return len(records)


# =========================
# MAIN
# =========================
def main() -> None:
    # 엔진 전 콘솔 부트로그
    log("[BOOT] start d6_MES_fail2_wasted_time")
    log(f"[CFG] DB={DB_CONFIG['host']}:{DB_CONFIG['port']} db={DB_CONFIG['dbname']} user={DB_CONFIG['user']}")
    log(f"[CFG] sleep={LOOP_SLEEP_SEC}s work_mem={PG_WORK_MEM} dst={DST_FQN}")
    log(f"[CFG] warm_start_lookback_min={WARM_START_LOOKBACK_MIN}")

    engine = get_engine_blocking()

    # 테이블 보장
    ensure_dest_table(engine)
    ensure_demon_log_table(engine)

    logx(engine, "info", "[DDL] ensured dest table")
    logx(engine, "info", f"[DDL] ensured demon log table: {LOG_FQN}")

    # ✅ Warm-start backfill (부팅 시 1회)
    try:
        warm_start_backfill(engine)
    except Exception as e:
        logx(engine, "error", f"[WARM][ERROR] {type(e).__name__}: {e}")
        # warm-start 실패해도 데몬은 계속 돈다

    while True:
        try:
            logx(engine, "info", "[LOOP] tick")

            last_pk = get_last_pk(engine)
            logx(engine, "info", f"[CURSOR] last_pk={last_pk}")

            logx(engine, "info", "[FETCH] querying new rows...")
            df_new = fetch_new_rows(engine, last_pk)
            logx(engine, "info", f"[FETCH] rows={len(df_new)}")

            logx(engine, "info", "[WRITE] inserting (duplicates skipped by PK)...")
            n = insert_rows(engine, df_new)
            logx(engine, "info", f"[WRITE] inserted_attempt={n}")

            logx(engine, "sleep", f"sleep {LOOP_SLEEP_SEC}s")
            time.sleep(LOOP_SLEEP_SEC)

        except KeyboardInterrupt:
            logx(engine, "down", "[STOP] KeyboardInterrupt")
            return

        except Exception as e:
            logx(engine, "error", f"[ERROR] {type(e).__name__}: {e}")

            if is_disconnect_error(e):
                logx(engine, "down", "[RECOVER] detected disconnect -> reconnect blocking...")
            else:
                logx(engine, "down", "[RECOVER] unexpected error -> reconnect anyway...")

            try:
                engine.dispose()
            except Exception:
                pass

            engine = get_engine_blocking()

            try:
                ensure_dest_table(engine)
                ensure_demon_log_table(engine)
                logx(engine, "info", "[DDL] ensured tables (after reconnect)")
            except Exception as e2:
                logx(engine, "error", f"[RETRY] DDL ensure failed after reconnect: {type(e2).__name__}: {e2}")

            # reconnect 동안 누락 보정용 가벼운 backfill 1회
            try:
                warm_start_backfill(engine)
            except Exception as e3:
                logx(engine, "error", f"[WARM][RETRY-ERROR] {type(e3).__name__}: {e3}")

            logx(engine, "sleep", f"sleep {LOOP_SLEEP_SEC}s after recover")
            time.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    main()
