# -*- coding: utf-8 -*-
"""
d6_MES_fail2_wasted_time.py
- 무한루프 5초
- DB 접속 실패 무한 재시도(블로킹)
- pool 최소화(1)
- work_mem 제한
- PK 이후 데이터만 증분 조회
- 중복은 PK로 SKIP(ON CONFLICT DO NOTHING)
"""

from __future__ import annotations

import os
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from psycopg2.extras import execute_values


# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE = "fct_vision_testlog_txt_processing_history"

CT_FQN = '"e1_FCT_ct"."fct_whole_op_ct"'  # 대문자 스키마 대응

DST_SCHEMA = "d1_machine_log"
DST_TABLE = "mes_fail2_wasted_time"
DST_FQN = f"{DST_SCHEMA}.{DST_TABLE}"

STATIONS = ("FCT1", "FCT2", "FCT3", "FCT4")
GOODORBAD_VALUE = "BadFile"

LOOP_SLEEP_SEC = 5
KST = ZoneInfo("Asia/Seoul")

PG_WORK_MEM = os.getenv("PG_WORK_MEM", "64MB")


# =========================
# LOG
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
    """DB 붙을 때까지 무한 재시도 + 매번 INFO 출력"""
    while True:
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
                eng.dispose()
            except Exception:
                pass
            time.sleep(LOOP_SLEEP_SEC)


# =========================
# DDL
# =========================
def ensure_dest_table(engine: Engine) -> None:
    log("[DDL] ensure schema/table/PK ...")
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {DST_SCHEMA};

    CREATE TABLE IF NOT EXISTS {DST_FQN} (
        barcode_information  TEXT NOT NULL,
        end_day              TEXT NOT NULL,
        end_time             TEXT NOT NULL,

        station              TEXT,
        remark               TEXT,
        result               TEXT,
        goodorbad            TEXT,
        file_path            TEXT,
        final_ct             NUMERIC,

        df_updated_at        TIMESTAMPTZ,

        created_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),

        CONSTRAINT pk_mes_fail2_wasted_time
            PRIMARY KEY (barcode_information, end_day, end_time)
    );
    """
    with engine.begin() as conn:
        set_session_guards(conn)
        conn.execute(text(ddl))
    log("[DDL] OK")


# =========================
# CURSOR
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
# FETCH NEW
# =========================
def fetch_new_rows(engine: Engine, last_pk: tuple[str, str, str] | None) -> pd.DataFrame:
    inc_where = ""
    params = {"stations": list(STATIONS), "goodorbad": GOODORBAD_VALUE}

    if last_pk is not None:
        last_end_day, last_end_time, last_barcode = last_pk
        inc_where = """
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

    sql = f"""
    WITH src AS (
        SELECT
            s.barcode_information,
            s.station,
            s.end_day,
            s.end_time,
            s.remark,
            s.result,
            s.goodorbad,
            s.file_path,

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
          {inc_where}
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
        x.file_path,
        c.final_ct
    FROM src x
    LEFT JOIN ct c
      ON c.month  = x.prev_month
     AND c.remark = x.remark_norm
    ORDER BY x.end_day, x.end_time, x.barcode_information
    """

    with engine.connect() as conn:
        set_session_guards(conn)
        df = pd.read_sql_query(text(sql), conn, params=params)

    return df


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
        "file_path",
        "final_ct",
        "df_updated_at",
    ]

    df2 = df.copy()
    df2["df_updated_at"] = df_updated_at

    # PK 비어있는 행 제외
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

    # NaN -> None
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
    log("[BOOT] start d6_MES_fail2_wasted_time")
    log(f"[CFG] DB={DB_CONFIG['host']}:{DB_CONFIG['port']} db={DB_CONFIG['dbname']} user={DB_CONFIG['user']}")
    log(f"[CFG] sleep={LOOP_SLEEP_SEC}s work_mem={PG_WORK_MEM} dst={DST_FQN}")

    engine = get_engine_blocking()
    ensure_dest_table(engine)

    while True:
        try:
            log("[LOOP] tick")

            last_pk = get_last_pk(engine)
            log(f"[CURSOR] last_pk={last_pk}")

            log("[FETCH] querying new rows...")
            df_new = fetch_new_rows(engine, last_pk)
            log(f"[FETCH] rows={len(df_new)}")

            log("[WRITE] inserting (duplicates skipped by PK)...")
            n = insert_rows(engine, df_new)
            log(f"[WRITE] inserted_attempt={n}")

            time.sleep(LOOP_SLEEP_SEC)

        except KeyboardInterrupt:
            log("[STOP] KeyboardInterrupt")
            return
        except Exception as e:
            log(f"[ERROR] loop error: {type(e).__name__}: {e}")
            # 연결 깨졌을 가능성: 엔진 재연결(무한재시도)
            try:
                engine.dispose()
            except Exception:
                pass
            engine = get_engine_blocking()
            ensure_dest_table(engine)


if __name__ == "__main__":
    main()
