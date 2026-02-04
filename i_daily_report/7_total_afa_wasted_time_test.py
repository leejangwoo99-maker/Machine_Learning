# -*- coding: utf-8 -*-
"""
7_total_afa_wasted_time_TEST_20260128_DAY.py
-------------------------------------------------
Backend-7 (TEST FIXED): AFA wasted time (day) daemon (window/now 고정)

테스트 요구:
- prod_day = 20260128 고정
- shift    = day 고정
- window   = 2026-01-28 08:30:00 ~ 2026-01-28 20:29:59 (KST, 고정)
- 무한루프 5초 유지(원하면 마지막에 LOOP=False로 단발 실행 가능)

그 외 요구사항은 원본과 동일:
- dataframe 콘솔 출력 없음
- DB 접속 실패/끊김 무한 재시도
- pool_size=1, max_overflow=0
- work_mem 세션 SET
- 증분 PK: (end_day, station, from_time) 사전식
- seen_pk: (end_day, station, from_time, to_time)
- 재실행 시 bootstrap(윈도우 start~window_end 전체 재집계) 후 UPSERT overwrite
- DELETE/TRUNCATE 금지
- UPSERT는 prod_day UNIQUE 필요(ensure_dest_table에서 UNIQUE INDEX 보장)
"""

from __future__ import annotations

import os
import re
import time as time_mod
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple, Set

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# =========================
# 0) 테스트 고정 입력
# =========================
TEST_PROD_DAY = "20260128"
TEST_SHIFT = "day"
TEST_WS = datetime(2026, 1, 28, 8, 30, 0, tzinfo=KST)
TEST_WE = datetime(2026, 1, 28, 20, 29, 59, tzinfo=KST)

# 무한루프 유지(테스트 단발이면 False로 변경)
LOOP = True

# =========================
# DB / 테이블 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

AFA_SCHEMA = "d1_machine_log"
AFA_TABLE  = "afa_fail_wasted_time"

CT_SCHEMA  = "e1_FCT_ct"
CT_TABLE   = "fct_whole_op_ct"

SAVE_SCHEMA = "i_daily_report"
T_DAY   = "g_afa_wasted_time_day_daily"
T_NIGHT = "g_afa_wasted_time_night_daily"

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

COL_TOTAL = "Total 조립 불량 손실 시간"

# =========================
# 1) 로그
# =========================
def log(level: str, msg: str) -> None:
    now = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}", flush=True)

# =========================
# 2) DB 엔진/재접속
# =========================
def make_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

def ensure_session(conn) -> None:
    conn.execute(text("SET work_mem = :wm"), {"wm": WORK_MEM})

def connect_with_retry() -> Engine:
    log("BOOT", "backend7 afa wasted time TEST daemon starting")
    while True:
        try:
            engine = make_engine()
            with engine.begin() as conn:
                conn.execute(text("SELECT 1"))
                ensure_session(conn)
            log("INFO", f"DB connected (work_mem={WORK_MEM})")
            return engine
        except Exception as e:
            log("RETRY", f"DB connect failed: {type(e).__name__}: {e} | retry in {DB_RETRY_INTERVAL_SEC}s")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

# =========================
# 3) 식별자 resolve (대소문자/따옴표 이슈 방지)
# =========================
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")

def quote_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'

def resolve_table_fqn(engine: Engine, preferred_schema: str, preferred_table: str) -> str:
    q = text("""
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE lower(table_name) = lower(:table_name)
          AND lower(table_schema) = lower(:schema_name)
        LIMIT 1
    """)
    with engine.begin() as conn:
        ensure_session(conn)
        row = conn.execute(q, {"schema_name": preferred_schema, "table_name": preferred_table}).fetchone()
        if row is None:
            q2 = text("""
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE lower(table_name) = lower(:table_name)
                ORDER BY table_schema, table_name
                LIMIT 1
            """)
            row = conn.execute(q2, {"table_name": preferred_table}).fetchone()

    if row is None:
        raise RuntimeError(f"Table not found: preferred={preferred_schema}.{preferred_table}")

    schema_actual, table_actual = row[0], row[1]
    return f'{quote_ident(schema_actual)}.{quote_ident(table_actual)}'

# =========================
# 4) 시간/윈도우 유틸
# =========================
def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))

def yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def fixed_shift_bounds(prod_day: str, shift: str) -> Tuple[datetime, datetime]:
    d = parse_yyyymmdd(prod_day)
    if shift == "day":
        return (
            datetime(d.year, d.month, d.day, 8, 30, 0, tzinfo=KST),
            datetime(d.year, d.month, d.day, 20, 29, 59, tzinfo=KST),
        )
    if shift == "night":
        ws = datetime(d.year, d.month, d.day, 20, 30, 0, tzinfo=KST)
        d1 = d + timedelta(days=1)
        we = datetime(d1.year, d1.month, d1.day, 8, 29, 59, tzinfo=KST)
        return ws, we
    raise ValueError("shift must be day/night")

def prev_month_by_window_start(prod_day: str, shift: str) -> str:
    from dateutil.relativedelta import relativedelta
    ws, _ = fixed_shift_bounds(prod_day, shift)
    prev = (ws.date().replace(day=1) - relativedelta(months=1))
    return f"{prev.year:04d}{prev.month:02d}"

def _time_to_decimal_seconds(t: str) -> Decimal:
    hh, mm, ss = t.split(":")
    return Decimal(hh) * 3600 + Decimal(mm) * 60 + Decimal(ss)

def normalize_time_to_dt(end_day: str, t: str) -> datetime:
    base = parse_yyyymmdd(end_day)
    sec = _time_to_decimal_seconds(t)
    sec_i = int(sec.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    day_add = 0
    if sec_i >= 86400:
        day_add = sec_i // 86400
        sec_i = sec_i % 86400
    if sec_i < 0:
        day_add = -((-sec_i + 86399) // 86400)
        sec_i = sec_i % 86400

    hh = sec_i // 3600
    mm = (sec_i % 3600) // 60
    ss = sec_i % 60
    return datetime(base.year, base.month, base.day, hh, mm, ss, tzinfo=KST) + timedelta(days=day_add)

def overlap_seconds_tick(from_dt: datetime, to_dt: datetime, ws: datetime, we: datetime) -> int:
    if to_dt <= from_dt:
        return 0
    start_tick = from_dt + timedelta(seconds=1)  # open at from
    end_tick = to_dt  # closed at to
    if start_tick > end_tick:
        return 0

    lo = max(start_tick, ws)
    hi = min(end_tick, we)
    if lo > hi:
        return 0
    return int((hi - lo).total_seconds()) + 1

def format_kor_hms(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    parts = []
    if h:
        parts.append(f"{h}시간")
    if m:
        parts.append(f"{m}분")
    if s or (not parts):
        parts.append(f"{s}초")
    return " ".join(parts)

# =========================
# 5) 저장 테이블 DDL/UPSERT
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        ensure_session(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

def ensure_dest_table(engine: Engine, schema: str, table: str) -> None:
    idx_name = f"{table}__ux_prod_day"
    with engine.begin() as conn:
        ensure_session(conn)

        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))

        conn.execute(text(f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
              "prod_day" text
            );
        '''))

        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "shift_type" text;'))
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{COL_TOTAL}" text;'))
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "updated_at" timestamptz;'))

        conn.execute(text(f'''
            CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}"
            ON "{schema}"."{table}" ("prod_day");
        '''))

def upsert_result(engine: Engine, schema: str, table: str, prod_day: str, shift_type: str, total_str: str, updated_at: datetime) -> None:
    sql = text(f"""
        INSERT INTO "{schema}"."{table}" ("prod_day","shift_type","{COL_TOTAL}","updated_at")
        VALUES (:prod_day, :shift_type, :total, :updated_at)
        ON CONFLICT ("prod_day") DO UPDATE
        SET
          "shift_type" = EXCLUDED."shift_type",
          "{COL_TOTAL}" = EXCLUDED."{COL_TOTAL}",
          "updated_at" = EXCLUDED."updated_at";
    """)
    with engine.begin() as conn:
        ensure_session(conn)
        conn.execute(sql, {"prod_day": prod_day, "shift_type": shift_type, "total": total_str, "updated_at": updated_at})

# =========================
# 6) fetch / compute
# =========================
def end_days_for_window(prod_day: str, shift: str) -> list[str]:
    d = parse_yyyymmdd(prod_day)
    if shift == "day":
        return [yyyymmdd(d)]
    return [yyyymmdd(d), yyyymmdd(d + timedelta(days=1))]

def fetch_all(engine: Engine, afa_fqn: str, prod_day: str, shift: str) -> pd.DataFrame:
    end_days = end_days_for_window(prod_day, shift)
    sql = text(f"""
        SELECT end_day, station, from_time, to_time, wasted_time
        FROM {afa_fqn}
        WHERE end_day = ANY(:end_days)
        ORDER BY end_day, station, from_time, to_time
    """)
    with engine.begin() as conn:
        ensure_session(conn)
        return pd.read_sql(sql, conn, params={"end_days": end_days})

def fetch_incremental(engine: Engine, afa_fqn: str, prod_day: str, shift: str, last_pk3: Optional[Tuple[str, str, str]]) -> pd.DataFrame:
    end_days = end_days_for_window(prod_day, shift)
    if last_pk3 is None:
        sql = text(f"""
            SELECT end_day, station, from_time, to_time, wasted_time
            FROM {afa_fqn}
            WHERE end_day = ANY(:end_days)
            ORDER BY end_day, station, from_time, to_time
        """)
        params = {"end_days": end_days}
    else:
        ld, ls, lf = last_pk3
        sql = text(f"""
            SELECT end_day, station, from_time, to_time, wasted_time
            FROM {afa_fqn}
            WHERE end_day = ANY(:end_days)
              AND (end_day, station, from_time) >= (:ld, :ls, :lf)
            ORDER BY end_day, station, from_time, to_time
        """)
        params = {"end_days": end_days, "ld": ld, "ls": ls, "lf": lf}
    with engine.begin() as conn:
        ensure_session(conn)
        return pd.read_sql(sql, conn, params=params)

def load_final_ct(engine: Engine, ct_fqn: str, prod_day: str, shift: str) -> float:
    prev_month = prev_month_by_window_start(prod_day, shift)
    sql = text(f"""
        SELECT final_ct
        FROM {ct_fqn}
        WHERE month = :month
          AND station = 'whole'
          AND remark = 'PD'
        LIMIT 1
    """)
    with engine.begin() as conn:
        ensure_session(conn)
        row = conn.execute(sql, {"month": prev_month}).fetchone()
    if row is None:
        raise RuntimeError(f"final_ct not found for month={prev_month}, station='whole', remark='PD'")
    return float(row[0])

def compute_increment_day(
    df: pd.DataFrame,
    prod_day: str,
    window_end_fixed: datetime,
    seen_pk: Set[Tuple[str, str, str, str]],
) -> Tuple[int, int, Optional[Tuple[str, str, str]]]:
    """
    TEST 고정: prod_day=20260128, shift=day, window_end=20:29:59 고정
    return: (added_loss_sec, added_cnt, new_last_pk3_max)
    """
    day_ws, day_we = fixed_shift_bounds(prod_day, "day")
    nig_ws, nig_we = fixed_shift_bounds(prod_day, "night")

    # 이번 테스트는 window_end를 고정(TEST_WE)으로 사용
    day_we = min(day_we, window_end_fixed)
    nig_we = min(nig_we, window_end_fixed)

    if df.empty:
        return 0, 0, None

    df = df.copy()
    df["wasted_time"] = pd.to_numeric(df["wasted_time"], errors="coerce").fillna(0).astype(int)

    added_loss = 0
    added_cnt = 0
    last_pk3_max: Optional[Tuple[str, str, str]] = None

    for r in df.itertuples(index=False):
        end_day = str(r.end_day)
        station = str(r.station)
        from_t = str(r.from_time)
        to_t = str(r.to_time)
        wasted = int(r.wasted_time)

        pk4 = (end_day, station, from_t, to_t)
        if pk4 in seen_pk:
            continue

        fdt = normalize_time_to_dt(end_day, from_t)
        tdt = normalize_time_to_dt(end_day, to_t)

        if tdt < fdt:
            tdt = tdt + timedelta(days=1)

        # window_end_fixed 이후는 클립
        if fdt > window_end_fixed:
            continue
        if tdt > window_end_fixed:
            tdt = window_end_fixed

        # day/night overlap (고정 window end)
        sec_day = overlap_seconds_tick(fdt, tdt, day_ws, day_we)
        sec_nig = overlap_seconds_tick(fdt, tdt, nig_ws, nig_we)
        sec_total = sec_day + sec_nig
        if sec_total <= 0:
            continue

        # row shift 결정(동률 day)
        row_shift = "day" if sec_day >= sec_nig else "night"

        # day window overlap만 계산(테스트는 day만)
        sec_cur = sec_day
        if sec_cur <= 0:
            continue

        # wasted_time 비율 배분(half-up) - day에 할당
        if wasted <= 0:
            alloc_day = 0
        else:
            alloc_day = int((Decimal(wasted) * Decimal(sec_cur) / Decimal(sec_total)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            alloc_day = max(0, min(wasted, alloc_day))

        added_loss += alloc_day

        # count: row_shift가 day일 때만
        if row_shift == "day":
            added_cnt += 1

        seen_pk.add(pk4)
        pk3 = (end_day, station, from_t)
        if last_pk3_max is None or pk3 > last_pk3_max:
            last_pk3_max = pk3

    return added_loss, added_cnt, last_pk3_max

# =========================
# 7) 메인
# =========================
def main() -> None:
    engine = connect_with_retry()

    try:
        afa_fqn = resolve_table_fqn(engine, AFA_SCHEMA, AFA_TABLE)
        ct_fqn  = resolve_table_fqn(engine, CT_SCHEMA, CT_TABLE)
        log("INFO", f"AFA_FQN={afa_fqn}")
        log("INFO", f"CT_FQN={ct_fqn}")
    except Exception as e:
        log("RETRY", f"Resolve table failed: {type(e).__name__}: {e}")
        engine.dispose()
        return main()

    try:
        ensure_schema(engine, SAVE_SCHEMA)
        ensure_dest_table(engine, SAVE_SCHEMA, T_DAY)
        ensure_dest_table(engine, SAVE_SCHEMA, T_NIGHT)
    except Exception as e:
        log("RETRY", f"Ensure dest failed: {type(e).__name__}: {e}")
        engine.dispose()
        return main()

    # 테스트 고정 상태
    prod_day = TEST_PROD_DAY
    shift = TEST_SHIFT
    ws = TEST_WS
    we = TEST_WE

    # state
    seen_pk: Set[Tuple[str, str, str, str]] = set()
    last_pk3: Optional[Tuple[str, str, str]] = None
    loss_sec = 0
    cnt = 0

    # bootstrap 1회
    try:
        log("INFO", f"[TEST WINDOW] prod_day={prod_day} shift={shift} start={ws} end={we}")
        log("INFO", "bootstrap start (fixed window full scan)")
        final_ct = load_final_ct(engine, ct_fqn, prod_day, shift)
        log("INFO", f"final_ct loaded: {final_ct} (prev_month={prev_month_by_window_start(prod_day, shift)})")

        df_all = fetch_all(engine, afa_fqn, prod_day, shift)
        log("INFO", f"bootstrap fetched rows={len(df_all)}")

        add_loss, add_cnt, new_last_pk = compute_increment_day(df_all, prod_day, we, seen_pk)
        loss_sec += add_loss
        cnt += add_cnt
        last_pk3 = new_last_pk

        rework_sec = int((Decimal(str(final_ct)) * Decimal(cnt)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
        total_sec = loss_sec + rework_sec
        total_str = format_kor_hms(total_sec)
        updated_at = datetime.now(tz=KST)

        upsert_result(engine, SAVE_SCHEMA, T_DAY, prod_day, "day", total_str, updated_at)
        log("INFO", f"bootstrap done | loss_sec={loss_sec} cnt={cnt} rework_sec={rework_sec} total={total_str}")
        log("INFO", f"upsert => {SAVE_SCHEMA}.{T_DAY} (prod_day={prod_day})")

    except (OperationalError, DBAPIError) as e:
        log("RETRY", f"DB error during bootstrap: {type(e).__name__}: {e}")
        try:
            engine.dispose()
        except Exception:
            pass
        return main()

    if not LOOP:
        log("INFO", "LOOP=False, exit after bootstrap.")
        return

    # incremental loop
    while True:
        try:
            log("INFO", f"[LAST_PK] {last_pk3}")
            df_new = fetch_incremental(engine, afa_fqn, prod_day, shift, last_pk3)
            log("INFO", f"[FETCH] candidates={len(df_new)}")

            if df_new.empty:
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            add_loss, add_cnt, new_last_pk = compute_increment_day(df_new, prod_day, we, seen_pk)
            if add_loss == 0 and add_cnt == 0 and new_last_pk is None:
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            loss_sec += add_loss
            cnt += add_cnt
            if new_last_pk is not None:
                if last_pk3 is None or new_last_pk > last_pk3:
                    last_pk3 = new_last_pk

            # final_ct는 테스트 중 변하지 않으니 재조회 안 함
            # (그래도 안전하게 유지하려면 위에서 final_ct를 outer scope로 올려도 됨)
            final_ct = load_final_ct(engine, ct_fqn, prod_day, shift)

            rework_sec = int((Decimal(str(final_ct)) * Decimal(cnt)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            total_sec = loss_sec + rework_sec
            total_str = format_kor_hms(total_sec)
            updated_at = datetime.now(tz=KST)

            upsert_result(engine, SAVE_SCHEMA, T_DAY, prod_day, "day", total_str, updated_at)

            log("INFO", f"[UPDATE] add_loss={add_loss} add_cnt={add_cnt} | loss_sec={loss_sec} cnt={cnt} rework_sec={rework_sec} total={total_str}")
            log("INFO", f"[UPSERT] {SAVE_SCHEMA}.{T_DAY} prod_day={prod_day}")

            time_mod.sleep(LOOP_INTERVAL_SEC)

        except (OperationalError, DBAPIError) as e:
            log("RETRY", f"DB error: {type(e).__name__}: {e} | reconnect")
            try:
                engine.dispose()
            except Exception:
                pass

            engine = connect_with_retry()
            afa_fqn = resolve_table_fqn(engine, AFA_SCHEMA, AFA_TABLE)
            ct_fqn  = resolve_table_fqn(engine, CT_SCHEMA, CT_TABLE)

            ensure_schema(engine, SAVE_SCHEMA)
            ensure_dest_table(engine, SAVE_SCHEMA, T_DAY)
            ensure_dest_table(engine, SAVE_SCHEMA, T_NIGHT)

            # 테스트도 재부팅 정책: 상태 리셋 후 bootstrap 재수행
            seen_pk.clear()
            last_pk3 = None
            loss_sec = 0
            cnt = 0
            log("INFO", "reconnected, state reset -> will re-bootstrap next loop")
            # 즉시 bootstrap 재수행을 원하면 여기서 main() 재호출해도 됨
            # 여기서는 다음 루프에서 bootstrap을 다시 하지 않으므로, 간단히 main() 재호출
            return main()

        except Exception as e:
            log("RETRY", f"Unhandled error: {type(e).__name__}: {e}")
            time_mod.sleep(LOOP_INTERVAL_SEC)

if __name__ == "__main__":
    main()
