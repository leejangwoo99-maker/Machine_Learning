# -*- coding: utf-8 -*-
"""
backend_station_daily_percentage_daemon.py
------------------------------------------
Station PASS percentage daemon (incremental PK, 5s loop)

요구사항 반영:
1) dataframe 콘솔 출력 없음
2) 현재 시각(KST) 기준으로 prod_day/shift 자동 전환
3) 멀티프로세스 없음 (single process)
4) 무한 루프, 5초 간격
5) DB 접속 실패 시 무한 재시도(연결될 때까지 블로킹)
6) 실행 중 DB 끊김 발생 시에도 무한 재접속 후 계속 진행
7) 연결 풀 최소화 (pool_size=1, max_overflow=0)
8) work_mem 설정
9) 증분 조건 PK=(end_day, end_time, barcode_information)
10) BOOT 로그 즉시 출력, DB 미접속 시 RETRY 로그 5초마다
11) LAST_PK / FETCH / UPSERT 단계마다 INFO 로그
12~14) (요구 제거됨) -> 대신 DB 기존 데이터 DELETE/초기화 절대 없음
15) day -> i_daily_report.b_station_day_daily_percentage
16) night -> i_daily_report.b_station_night_daily_percentage

✅ 추가 조건:
- goodorbad = 'GoodFile' 인 row만 집계 대상으로 포함

✅ 추가(로그 DB 모니터링):
- 로그를 DB에 저장
- 컬럼: end_day, end_time, info, contents
- info는 반드시 소문자
- 저장 위치: schema k_demon_heath_check, table "2_log"
"""

from __future__ import annotations

import os
import time as time_mod
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / Const
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5
FETCH_LIMIT = 5000  # 한 루프 최대 fetch row (너무 크면 조정)

PG_WORK_MEM = os.getenv("PG_WORK_MEM", "64MB")

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"]

SAVE_SCHEMA = "i_daily_report"
DAY_TABLE   = "b_station_day_daily_percentage"
NIGHT_TABLE = "b_station_night_daily_percentage"
KEY_COLS    = ["prod_day", "shift_type"]  # UPSERT 키 고정

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"
SRC_FQN    = f"{SRC_SCHEMA}.{SRC_TABLE}"

# ✅ 추가 조건
GOODORBAD_FILTER_VALUE = "GoodFile"

# ✅ 로그 저장 위치
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "2_log"  # 숫자 시작 -> 반드시 쿼팅

# =========================
# DB Config (환경변수 권장)
# =========================
DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    # ⚠️ 운영에서는 환경변수 PGPASSWORD 권장
    "password": os.getenv("PGPASSWORD", ""),#비번은 보완 사항
}

# =========================
# Time / Logging helpers
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)

def ts() -> str:
    return now_kst().strftime("%Y-%m-%d %H:%M:%S")

def to_log_row(level: str, contents: str) -> pd.DataFrame:
    """
    요구사항:
    - end_day: yyyymmdd
    - end_time: hh:mi:ss
    - info: 소문자
    - contents: 나머지 내용
    - 컬럼 순서 고정
    """
    n = now_kst()
    return pd.DataFrame([{
        "end_day": n.strftime("%Y%m%d"),
        "end_time": n.strftime("%H:%M:%S"),
        "info": str(level).lower(),
        "contents": str(contents),
    }], columns=["end_day", "end_time", "info", "contents"])

def console_log(level: str, msg: str) -> None:
    print(f"{ts()} [{level.upper()}] {msg}")

def set_session_params(conn) -> None:
    conn.execute(text(f"SET work_mem = '{PG_WORK_MEM}'"))
    conn.execute(text("SET client_encoding = 'UTF8'"))

def ensure_log_table(engine: Engine) -> None:
    with engine.begin() as conn:
        set_session_params(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};'))
        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}."{LOG_TABLE}" (
                end_day  text,
                end_time text,
                info     text,
                contents text
            );
        '''))

def write_log_db(engine: Optional[Engine], level: str, msg: str) -> None:
    """
    로그를 DataFrame화 후 DB 저장.
    - engine 없거나 DB 오류면 조용히 스킵(콘솔 로그는 유지)
    """
    if engine is None:
        return
    try:
        df = to_log_row(level, msg)  # 컬럼 순서 보장
        row = df.iloc[0].to_dict()
        with engine.begin() as conn:
            set_session_params(conn)
            conn.execute(
                text(f'''
                    INSERT INTO {LOG_SCHEMA}."{LOG_TABLE}"
                    (end_day, end_time, info, contents)
                    VALUES (:end_day, :end_time, :info, :contents)
                '''),
                row
            )
    except Exception:
        # 로그 저장 실패로 메인 루프 중단 금지
        pass

def emit_log(level: str, msg: str, engine: Optional[Engine] = None) -> None:
    """
    콘솔 + DB 동시 로그
    """
    lvl = str(level).lower()
    console_log(lvl, msg)
    write_log_db(engine, lvl, msg)

# =========================
# Engine / Connection
# =========================
def make_engine() -> Engine:
    user = urllib.parse.quote_plus(DB_CONFIG["user"])
    pw   = urllib.parse.quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    db   = DB_CONFIG["dbname"]
    url  = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_recycle=1800,
    )

def connect_with_retry() -> Engine:
    """엔진 생성 + 첫 연결 확인까지 무한 재시도."""
    while True:
        try:
            engine = make_engine()
            with engine.begin() as conn:
                set_session_params(conn)
                conn.execute(text("SELECT 1")).scalar()
            return engine
        except Exception as e:
            # 이 시점엔 engine이 없거나 불안정할 수 있으므로 콘솔만
            console_log("retry", f"DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

# =========================
# Window logic
# =========================
@dataclass(frozen=True)
class Window:
    prod_day: str       # YYYYMMDD
    shift_type: str     # day|night
    start_dt: datetime  # tz-aware KST
    end_dt: datetime    # tz-aware KST (= now)

def current_window(now: datetime) -> Window:
    """
    확정 규칙:
    - 08:30~20:29:59 => day,  prod_day=오늘
    - 20:30~23:59:59 => night, prod_day=오늘
    - 00:00~08:29:59 => night, prod_day=어제
    window_end = now
    """
    t = now.timetz()
    day_start = dtime(8, 30, 0, tzinfo=KST)
    day_end   = dtime(20, 29, 59, tzinfo=KST)
    night_start = dtime(20, 30, 0, tzinfo=KST)

    # day
    if day_start <= t <= day_end:
        prod = now.strftime("%Y%m%d")
        start = now.replace(hour=8, minute=30, second=0, microsecond=0)
        return Window(prod, "day", start, now)

    # night (evening)
    if t >= night_start:
        prod = now.strftime("%Y%m%d")
        start = now.replace(hour=20, minute=30, second=0, microsecond=0)
        return Window(prod, "night", start, now)

    # night (morning) 00:00~08:29:59 => prod_day=어제
    prod_date = (now.date() - timedelta(days=1))
    prod = prod_date.strftime("%Y%m%d")
    start = datetime(prod_date.year, prod_date.month, prod_date.day, 20, 30, 0, tzinfo=KST)
    return Window(prod, "night", start, now)

# =========================
# PK helpers
# =========================
PK = Tuple[str, str, str]  # (end_day, end_time, barcode_information)

def pk_str(pk: Optional[PK]) -> str:
    if not pk:
        return "None"
    return f"({pk[0]}, {pk[1]}, {pk[2]})"

# =========================
# Queries (✅ goodorbad='GoodFile' 필터 추가)
# =========================
BOOTSTRAP_AGG_SQL = f"""
WITH base AS (
  SELECT station, result
  FROM {SRC_FQN}
  WHERE
    station = ANY(:stations)
    AND result IN ('PASS','FAIL')
    AND goodorbad = :goodorbad
    AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')
        BETWEEN :start_dt AND :end_dt
)
SELECT
  station,
  SUM(CASE WHEN result='PASS' THEN 1 ELSE 0 END)::int AS pass_cnt,
  COUNT(*)::int AS total_cnt
FROM base
GROUP BY station
ORDER BY station
"""

BOOTSTRAP_LASTPK_SQL = f"""
SELECT end_day, end_time, barcode_information
FROM {SRC_FQN}
WHERE
  station = ANY(:stations)
  AND result IN ('PASS','FAIL')
  AND goodorbad = :goodorbad
  AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')
      BETWEEN :start_dt AND :end_dt
ORDER BY end_day DESC, end_time DESC, barcode_information DESC
LIMIT 1
"""

INCR_FETCH_SQL = f"""
SELECT end_day, end_time, barcode_information, station, result
FROM {SRC_FQN}
WHERE
  station = ANY(:stations)
  AND result IN ('PASS','FAIL')
  AND goodorbad = :goodorbad
  AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')
      BETWEEN :start_dt AND :end_dt
  AND (end_day, end_time, barcode_information) > (:pk_day, :pk_time, :pk_barcode)
ORDER BY end_day ASC, end_time ASC, barcode_information ASC
LIMIT :limit
"""

# =========================
# Aggregation / DF
# =========================
def fmt_cell(pass_cnt: int, total_cnt: int) -> str:
    pct = (pass_cnt / total_cnt * 100.0) if total_cnt > 0 else 0.0
    return f"PASS: {pass_cnt}, total: {total_cnt}, PASS_pct:{pct:.2f}"

def build_df(prod_day: str, shift_type: str, agg: Dict[str, Dict[str, int]]) -> pd.DataFrame:
    v_pass  = agg["Vision1"]["pass"]  + agg["Vision2"]["pass"]
    v_total = agg["Vision1"]["total"] + agg["Vision2"]["total"]

    updated_at = now_kst()  # timestamptz

    out = {
        "prod_day": prod_day,
        "shift_type": shift_type,
        "FCT1": fmt_cell(agg["FCT1"]["pass"], agg["FCT1"]["total"]),
        "FCT2": fmt_cell(agg["FCT2"]["pass"], agg["FCT2"]["total"]),
        "FCT3": fmt_cell(agg["FCT3"]["pass"], agg["FCT3"]["total"]),
        "FCT4": fmt_cell(agg["FCT4"]["pass"], agg["FCT4"]["total"]),
        "Vision1": fmt_cell(agg["Vision1"]["pass"], agg["Vision1"]["total"]),
        "Vision2": fmt_cell(agg["Vision2"]["pass"], agg["Vision2"]["total"]),
        "FCT>Vision Total": fmt_cell(v_pass, v_total),
        "updated_at": updated_at,
    }
    return pd.DataFrame([out])

# =========================
# Save (schema/table ensure + safe UPSERT)
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        set_session_params(conn)
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def ensure_table(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> None:
    cols = list(df.columns)
    with engine.begin() as conn:
        set_session_params(conn)

        exists = conn.execute(text("""
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.tables
              WHERE table_schema = :schema AND table_name = :table
            );
        """), {"schema": schema, "table": table}).scalar()

        if not exists:
            ddl_cols = []
            for c in cols:
                if c == "updated_at":
                    ddl_cols.append(f'"{c}" timestamptz')
                else:
                    ddl_cols.append(f'"{c}" text')

            ddl_cols_sql = ",\n  ".join(ddl_cols)
            uq_sql = ", ".join([f'"{c}"' for c in key_cols])

            conn.execute(text(f"""
                CREATE TABLE {schema}.{table} (
                  {ddl_cols_sql},
                  CONSTRAINT uq_{table}_key UNIQUE ({uq_sql})
                );
            """))
        else:
            existing = conn.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :schema AND table_name = :table
            """), {"schema": schema, "table": table}).fetchall()
            existing_cols = {r[0] for r in existing}

            for c in cols:
                if c in existing_cols:
                    continue
                col_type = "timestamptz" if c == "updated_at" else "text"
                conn.execute(text(f'ALTER TABLE {schema}.{table} ADD COLUMN "{c}" {col_type};'))

def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> None:
    """
    - 컬럼명에 특수문자/공백이 있어도 안전
    - NULL 덮어쓰기 방지 (COALESCE) + updated_at은 최신으로
    """
    assert len(df) == 1
    row0 = df.iloc[0].to_dict()
    cols = list(df.columns)

    bind_map = {c: f"v{i}" for i, c in enumerate(cols)}
    params = {bind_map[c]: row0.get(c) for c in cols}

    insert_cols_sql = ", ".join([f'"{c}"' for c in cols])
    values_sql = ", ".join([f":{bind_map[c]}" for c in cols])
    conflict_cols_sql = ", ".join([f'"{c}"' for c in key_cols])

    set_parts = []
    for c in cols:
        if c in key_cols:
            continue
        if c == "updated_at":
            set_parts.append(f'"{c}" = EXCLUDED."{c}"')
        else:
            set_parts.append(f'"{c}" = COALESCE(EXCLUDED."{c}", {schema}.{table}."{c}")')
    set_sql = ", ".join(set_parts)

    sql = f"""
        INSERT INTO {schema}.{table} ({insert_cols_sql})
        VALUES ({values_sql})
        ON CONFLICT ({conflict_cols_sql})
        DO UPDATE SET {set_sql};
    """

    with engine.begin() as conn:
        set_session_params(conn)
        conn.execute(text(sql), params)

# =========================
# Runtime state
# =========================
@dataclass
class State:
    window: Window
    last_pk: Optional[PK]
    agg: Dict[str, Dict[str, int]]

def empty_agg() -> Dict[str, Dict[str, int]]:
    return {s: {"pass": 0, "total": 0} for s in STATIONS}

# =========================
# Core steps
# =========================
def bootstrap(engine: Engine, w: Window) -> Tuple[Dict[str, Dict[str, int]], Optional[PK]]:
    params = {
        "stations": STATIONS,
        "goodorbad": GOODORBAD_FILTER_VALUE,
        "start_dt": w.start_dt.replace(tzinfo=None),
        "end_dt": w.end_dt.replace(tzinfo=None),
    }

    agg = empty_agg()
    with engine.begin() as conn:
        set_session_params(conn)
        rows = conn.execute(text(BOOTSTRAP_AGG_SQL), params).mappings().all()

        for r in rows:
            s = r.get("station")
            if s in agg:
                agg[s]["pass"] = int(r.get("pass_cnt") or 0)
                agg[s]["total"] = int(r.get("total_cnt") or 0)

        last_row = conn.execute(text(BOOTSTRAP_LASTPK_SQL), params).mappings().first()

    last_pk: Optional[PK] = None
    if last_row:
        last_pk = (str(last_row["end_day"]), str(last_row["end_time"]), str(last_row["barcode_information"]))

    return agg, last_pk

def fetch_incremental(engine: Engine, w: Window, last_pk: PK) -> Tuple[List[dict], Optional[PK]]:
    params = {
        "stations": STATIONS,
        "goodorbad": GOODORBAD_FILTER_VALUE,
        "start_dt": w.start_dt.replace(tzinfo=None),
        "end_dt": w.end_dt.replace(tzinfo=None),
        "pk_day": last_pk[0],
        "pk_time": last_pk[1],
        "pk_barcode": last_pk[2],
        "limit": FETCH_LIMIT,
    }

    with engine.begin() as conn:
        set_session_params(conn)
        rows = conn.execute(text(INCR_FETCH_SQL), params).mappings().all()

    if not rows:
        return [], None

    rlast = rows[-1]
    new_last_pk: PK = (str(rlast["end_day"]), str(rlast["end_time"]), str(rlast["barcode_information"]))
    return list(rows), new_last_pk

def apply_rows(agg: Dict[str, Dict[str, int]], rows: List[dict]) -> None:
    for r in rows:
        s = r.get("station")
        if s not in agg:
            continue
        result = r.get("result")
        agg[s]["total"] += 1
        if result == "PASS":
            agg[s]["pass"] += 1

def target_table(shift_type: str) -> str:
    return DAY_TABLE if shift_type == "day" else NIGHT_TABLE

# =========================
# Main loop
# =========================
def main() -> None:
    # DB 연결 전 로그는 콘솔만
    emit_log("boot", f"backend station daily percentage daemon starting (goodorbad='{GOODORBAD_FILTER_VALUE}')", None)

    engine = connect_with_retry()
    ensure_log_table(engine)  # 로그 테이블 보장

    emit_log("info", f"DB connected (work_mem={PG_WORK_MEM})", engine)
    emit_log("info", f"log table ready => {LOG_SCHEMA}.\"{LOG_TABLE}\"", engine)

    ensure_schema(engine, SAVE_SCHEMA)

    # 초기 window/state
    w = current_window(now_kst())
    agg, last_pk = bootstrap(engine, w)
    state = State(window=w, last_pk=last_pk, agg=agg)

    # BOOT 직후 1회 저장(예외)
    df0 = build_df(state.window.prod_day, state.window.shift_type, state.agg)
    t0 = target_table(state.window.shift_type)
    ensure_table(engine, SAVE_SCHEMA, t0, df0, KEY_COLS)
    upsert_df(engine, SAVE_SCHEMA, t0, df0, KEY_COLS)
    emit_log(
        "info",
        f"[upsert] bootstrap saved => {SAVE_SCHEMA}.{t0} prod_day={state.window.prod_day} shift={state.window.shift_type}",
        engine,
    )
    emit_log(
        "info",
        f"[last_pk] {pk_str(state.last_pk)} window={state.window.start_dt}~{state.window.end_dt}",
        engine,
    )

    while True:
        loop_start = time_mod.time()

        try:
            now = now_kst()
            w_new = current_window(now)

            # 윈도우 변경 시 bootstrap rebuild (DB 삭제/초기화 금지, in-memory만 재구성)
            if (w_new.prod_day != state.window.prod_day) or (w_new.shift_type != state.window.shift_type):
                emit_log(
                    "info",
                    f"[window] changed => prod_day={w_new.prod_day} shift={w_new.shift_type} "
                    f"(start={w_new.start_dt}, end={w_new.end_dt})",
                    engine,
                )
                agg, last_pk = bootstrap(engine, w_new)
                state.window = w_new
                state.agg = agg
                state.last_pk = last_pk

                # 전환 직후 1회 저장
                dfw = build_df(state.window.prod_day, state.window.shift_type, state.agg)
                tw = target_table(state.window.shift_type)
                ensure_table(engine, SAVE_SCHEMA, tw, dfw, KEY_COLS)
                upsert_df(engine, SAVE_SCHEMA, tw, dfw, KEY_COLS)
                emit_log(
                    "info",
                    f"[upsert] window-change saved => {SAVE_SCHEMA}.{tw} prod_day={state.window.prod_day} shift={state.window.shift_type}",
                    engine,
                )
                emit_log("info", f"[last_pk] {pk_str(state.last_pk)}", engine)
            else:
                # 같은 윈도우면 end_dt(now)만 갱신
                state.window = Window(state.window.prod_day, state.window.shift_type, state.window.start_dt, now)

                if state.last_pk is None:
                    emit_log("info", "[last_pk] none => bootstrap rebuild (no rows yet)", engine)
                    agg, last_pk = bootstrap(engine, state.window)
                    state.agg = agg
                    state.last_pk = last_pk
                    emit_log("info", f"[last_pk] {pk_str(state.last_pk)}", engine)
                else:
                    emit_log("info", f"[last_pk] {pk_str(state.last_pk)}", engine)
                    new_rows, new_last_pk = fetch_incremental(engine, state.window, state.last_pk)

                    if new_rows:
                        emit_log("info", f"[fetch] rows={len(new_rows)}", engine)
                        apply_rows(state.agg, new_rows)
                        state.last_pk = new_last_pk

                        # 신규 row 있었던 루프에만 UPSERT
                        df = build_df(state.window.prod_day, state.window.shift_type, state.agg)
                        tbl = target_table(state.window.shift_type)
                        ensure_table(engine, SAVE_SCHEMA, tbl, df, KEY_COLS)
                        upsert_df(engine, SAVE_SCHEMA, tbl, df, KEY_COLS)
                        emit_log(
                            "info",
                            f"[upsert] done => {SAVE_SCHEMA}.{tbl} prod_day={state.window.prod_day} "
                            f"shift={state.window.shift_type} last_pk={pk_str(state.last_pk)}",
                            engine,
                        )
                    else:
                        emit_log("sleep", "[fetch] rows=0 (no update)", engine)

        except (OperationalError, DBAPIError) as e:
            emit_log("down", f"db error: {type(e).__name__}: {e}", engine)
            try:
                engine.dispose()
            except Exception:
                pass

            # 재연결 중에는 engine 불안정할 수 있으므로 콘솔 우선
            engine = connect_with_retry()
            ensure_log_table(engine)
            emit_log("info", "db reconnected", engine)

        except Exception as e:
            emit_log("error", f"unhandled error: {type(e).__name__}: {e}", engine)

        elapsed = time_mod.time() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            emit_log("sleep", f"loop sleep {sleep_sec:.2f}s", engine)
        time_mod.sleep(sleep_sec)

if __name__ == "__main__":
    main()
