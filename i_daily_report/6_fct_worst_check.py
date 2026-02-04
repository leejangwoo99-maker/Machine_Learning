# -*- coding: utf-8 -*-
"""
backend6_worst_case_daemon.py

[추가 실행 조건 반영]
- DB 연결된 상태로 프로그램 상시 대기
- 매일 08:20:00~08:30:00 실행
- 매일 20:20:00~20:30:00 실행
- 그 외 시간은 sleep (다음 실행 구간 시작까지)
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Optional, Tuple, List, Dict, Set

import pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# Timezone / constants
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5
LOOKBACK_SEC = 180  # 3 minutes
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

SWITCH_GRACE_SEC = LOOKBACK_SEC  # window 전환 시 미처리분 마저 처리

# ===== 실행 스케줄 (KST) =====
RUN_WINDOWS = [
    (time(8, 20, 0), time(8, 30, 0)),
    (time(20, 20, 0), time(20, 30, 0)),
]

# =========================
# DB config
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# Source tables
SRC_SCHEMA = "e4_predictive_maintenance"
T_PD = "pd_worst"
T_NPD = "non_pd_worst"

# Mapping table
MAP_SCHEMA = "g_production_film"
MAP_TABLE = "remark_info"

# Destination tables
SAVE_SCHEMA = "i_daily_report"
T_SAVE_DAY = "f_worst_case_day_daily"
T_SAVE_NIGHT = "f_worst_case_night_daily"

SAVE_COLS = [
    "prod_day", "shift_type", "barcode_information", "pn", "remark", "station",
    "end_day", "end_time", "run_time", "test_contents", "file_path", "updated_at"
]

UNIQUE_COLS = ["end_day", "end_time", "barcode_information", "test_contents"]


# =========================
# Logging
# =========================
def log(level: str, msg: str) -> None:
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


# =========================
# Scheduling helpers
# =========================
def _in_run_window(now_kst: datetime) -> bool:
    t0 = now_kst.timetz().replace(tzinfo=None)
    for s, e in RUN_WINDOWS:
        if s <= t0 <= e:
            return True
    return False


def _next_run_start(now_kst: datetime) -> datetime:
    """
    now 이후 가장 가까운 run window 시작 시각을 반환.
    오늘 남은 start가 없으면 내일 첫 start.
    """
    d0 = now_kst.date()
    starts = [datetime.combine(d0, s, tzinfo=KST) for s, _ in RUN_WINDOWS]
    starts.sort()
    for st in starts:
        if st > now_kst:
            return st
    # tomorrow
    d1 = d0 + timedelta(days=1)
    return datetime.combine(d1, RUN_WINDOWS[0][0], tzinfo=KST)


def _sleep_until_next_run(now_kst: datetime) -> None:
    nxt = _next_run_start(now_kst)
    sec = (nxt - now_kst).total_seconds()
    sec = max(1.0, sec)
    log("INFO", f"[SLEEP] outside run window. next_start={nxt} (sleep {int(sec)}s)")
    time_mod.sleep(sec)


# =========================
# Window logic
# =========================
@dataclass(frozen=True)
class WindowCtx:
    prod_day: str
    shift_type: str
    start_dt: datetime
    end_dt: datetime
    dest_table: str


def _yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def current_window_ctx(now_kst: Optional[datetime] = None) -> WindowCtx:
    if now_kst is None:
        now_kst = datetime.now(KST)

    d0 = now_kst.date()
    t0 = now_kst.timetz().replace(tzinfo=None)

    day_start_t = time(8, 30, 0)
    day_end_t = time(20, 29, 59)
    night_start_t = time(20, 30, 0)
    night_end_t = time(8, 29, 59)

    if day_start_t <= t0 <= day_end_t:
        prod = _yyyymmdd(d0)
        start_dt = datetime.combine(d0, day_start_t, tzinfo=KST)
        end_dt = datetime.combine(d0, day_end_t, tzinfo=KST)
        return WindowCtx(prod, "day", start_dt, end_dt, T_SAVE_DAY)

    # night
    if t0 >= night_start_t:
        prod_day_date = d0
        start_dt = datetime.combine(d0, night_start_t, tzinfo=KST)
        end_dt = datetime.combine(d0 + timedelta(days=1), night_end_t, tzinfo=KST)
    else:
        prod_day_date = d0 - timedelta(days=1)
        start_dt = datetime.combine(prod_day_date, night_start_t, tzinfo=KST)
        end_dt = datetime.combine(d0, night_end_t, tzinfo=KST)

    prod = _yyyymmdd(prod_day_date)
    return WindowCtx(prod, "night", start_dt, end_dt, T_SAVE_NIGHT)


# =========================
# DB helpers
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


def configure_session(conn: Connection) -> None:
    conn.execute(text("SET work_mem = :wm;"), {"wm": WORK_MEM})
    conn.execute(text("SET TIME ZONE 'Asia/Seoul';"))


def connect_with_retry() -> Engine:
    log("BOOT", "backend6 worst-case daemon starting")
    while True:
        try:
            eng = make_engine()
            with eng.connect() as conn:
                configure_session(conn)
            log("INFO", f"DB connected (work_mem={WORK_MEM})")
            return eng
        except Exception as e:
            log("RETRY", f"DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def ensure_save_objects(conn: Connection, schema: str, table: str) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    cols_ddl = []
    for c in SAVE_COLS:
        if c == "updated_at":
            cols_ddl.append(f'"{c}" timestamptz')
        else:
            cols_ddl.append(f'"{c}" text')
    cols_sql = ",\n  ".join(cols_ddl)

    uq_name = f"uq_{table}_pk"
    uq_cols_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      {cols_sql},
      CONSTRAINT {uq_name} UNIQUE ({uq_cols_sql})
    );
    """
    conn.execute(text(ddl))


@dataclass
class LastPK:
    end_ts: datetime
    barcode: str
    test_contents: str
    end_day: str
    end_time_norm: str


def read_last_pk(conn: Connection, ctx: WindowCtx) -> Optional[LastPK]:
    ensure_save_objects(conn, SAVE_SCHEMA, ctx.dest_table)

    sql = f"""
    WITH x AS (
      SELECT
        end_day,
        end_time,
        barcode_information,
        test_contents,
        (end_day::date::timestamp + (end_time::time)) AS end_ts,
        to_char((end_time::time), 'HH24:MI:SS') AS end_time_norm
      FROM {SAVE_SCHEMA}.{ctx.dest_table}
      WHERE prod_day = :prod_day
        AND shift_type = :shift_type
    )
    SELECT end_day, end_time_norm, barcode_information, test_contents, end_ts
    FROM x
    ORDER BY end_ts DESC, barcode_information DESC, test_contents DESC
    LIMIT 1;
    """
    row = conn.execute(
        text(sql),
        {"prod_day": ctx.prod_day, "shift_type": ctx.shift_type},
    ).fetchone()

    if not row:
        return None

    end_day, end_time_norm, barcode, test_contents, end_ts = row
    return LastPK(
        end_ts=end_ts,
        barcode=str(barcode),
        test_contents=str(test_contents),
        end_day=str(end_day),
        end_time_norm=str(end_time_norm),
    )


# =========================
# Fetch SQL
# =========================
FETCH_SQL = """
WITH src AS (
  SELECT
    'PD'::text AS src_type,
    t.barcode_information,
    t.station,
    t.end_day,
    t.end_time,
    t.run_time,
    t.test_contents,
    t.file_path
  FROM {src_schema}.{pd_table} t
  UNION ALL
  SELECT
    'Non-PD'::text AS src_type,
    t.barcode_information,
    t.station,
    t.end_day,
    t.end_time,
    t.run_time,
    t.test_contents,
    t.file_path
  FROM {src_schema}.{npd_table} t
),
base AS (
  SELECT
    s.*,
    (s.end_day::timestamp + (s.end_time::time)) AS end_ts,
    to_char((s.end_time::time), 'HH24:MI:SS') AS end_time_norm,
    substring(s.barcode_information from 18 for 1) AS key18
  FROM src s
),
mapped AS (
  SELECT
    b.*,
    ri.pn AS mapped_pn,
    ri.remark AS mapped_remark
  FROM base b
  LEFT JOIN {map_schema}.{map_table} ri
    ON ri.barcode_information = b.key18
)
SELECT
  :prod_day AS prod_day,
  :shift_type AS shift_type,
  barcode_information,
  COALESCE(mapped_pn, 'Unknown') AS pn,
  (COALESCE(mapped_remark, 'Unknown') || ' ' || src_type) AS remark,
  station,
  end_day,
  end_time_norm AS end_time,
  run_time,
  test_contents,
  file_path,
  :updated_at AS updated_at,
  end_ts
FROM mapped
WHERE end_ts BETWEEN :fetch_start_ts AND :window_end_ts
  AND (
    :has_last = FALSE
    OR end_ts > :last_end_ts
    OR (end_ts = :last_end_ts AND (barcode_information, test_contents) > (:last_barcode, :last_test_contents))
  )
ORDER BY end_ts ASC, barcode_information ASC, test_contents ASC;
"""


def fetch_rows_incremental(
    conn: Connection,
    ctx: WindowCtx,
    last_pk: Optional[LastPK],
    lookback_sec: int,
) -> List[Dict]:
    now_kst = datetime.now(KST)
    updated_at = now_kst  # timestamptz (KST aware)

    window_end_naive = ctx.end_dt.replace(tzinfo=None)

    if last_pk:
        fetch_start = max(
            ctx.start_dt.replace(tzinfo=None),
            last_pk.end_ts - timedelta(seconds=lookback_sec),
        )
        params = {
            "has_last": True,
            "last_end_ts": last_pk.end_ts,
            "last_barcode": last_pk.barcode,
            "last_test_contents": last_pk.test_contents,
        }
    else:
        fetch_start = ctx.start_dt.replace(tzinfo=None)
        params = {
            "has_last": False,
            "last_end_ts": datetime(1970, 1, 1),
            "last_barcode": "",
            "last_test_contents": "",
        }

    params.update({
        "prod_day": ctx.prod_day,
        "shift_type": ctx.shift_type,
        "fetch_start_ts": fetch_start,
        "window_end_ts": window_end_naive,
        "updated_at": updated_at,
    })

    sql = FETCH_SQL.format(
        src_schema=SRC_SCHEMA,
        pd_table=T_PD,
        npd_table=T_NPD,
        map_schema=MAP_SCHEMA,
        map_table=MAP_TABLE,
    )
    rows = conn.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d.pop("end_ts", None)
        out.append(d)
    return out


# =========================
# UPSERT
# =========================
def upsert_rows(conn: Connection, ctx: WindowCtx, rows: List[Dict]) -> int:
    if not rows:
        return 0

    ensure_save_objects(conn, SAVE_SCHEMA, ctx.dest_table)

    cols = SAVE_COLS
    col_sql = ", ".join([f'"{c}"' for c in cols])
    val_sql = ", ".join([f":{c}" for c in cols])
    conflict_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])

    set_parts = []
    for c in cols:
        if c in UNIQUE_COLS:
            continue
        if c == "updated_at":
            set_parts.append(f'"{c}" = EXCLUDED."{c}"')
        elif c in ("pn", "remark"):
            set_parts.append(f'"{c}" = COALESCE({SAVE_SCHEMA}.{ctx.dest_table}."{c}", EXCLUDED."{c}")')
        else:
            set_parts.append(f'"{c}" = COALESCE(EXCLUDED."{c}", {SAVE_SCHEMA}.{ctx.dest_table}."{c}")')

    set_sql = ",\n  ".join(set_parts)

    upsert_sql = f"""
    INSERT INTO {SAVE_SCHEMA}.{ctx.dest_table} ({col_sql})
    VALUES ({val_sql})
    ON CONFLICT ({conflict_sql})
    DO UPDATE SET
      {set_sql};
    """

    conn.execute(text(upsert_sql), rows)
    return len(rows)


# =========================
# seen_pk + last_pk update
# =========================
def make_seen_key(r: Dict) -> Tuple[str, str, str, str]:
    end_day = str(r["end_day"])
    end_time_norm = str(r["end_time"])
    barcode = str(r["barcode_information"])
    testc = str(r["test_contents"])
    return (end_day, end_time_norm, barcode, testc)


def update_last_pk_from_rows(rows: List[Dict]) -> Optional[LastPK]:
    if not rows:
        return None
    last = rows[-1]
    end_day_str = str(last["end_day"])
    end_time_norm = str(last["end_time"])
    barcode = str(last["barcode_information"])
    testc = str(last["test_contents"])

    y, m, d = map(int, end_day_str.split("-"))
    hh, mm, ss = map(int, end_time_norm.split(":"))
    end_ts = datetime(y, m, d, hh, mm, ss)

    return LastPK(
        end_ts=end_ts,
        barcode=barcode,
        test_contents=testc,
        end_day=end_day_str,
        end_time_norm=end_time_norm,
    )


# =========================
# Main loop
# =========================
def run() -> None:
    engine = connect_with_retry()

    active_ctx: Optional[WindowCtx] = None
    pending_ctx: Optional[WindowCtx] = None

    seen_pk: Set[Tuple[str, str, str, str]] = set()
    last_pk: Optional[LastPK] = None

    while True:
        now_kst = datetime.now(KST)

        # ====== 스케줄: 실행 구간이 아니면 다음 시작까지 sleep ======
        if not _in_run_window(now_kst):
            _sleep_until_next_run(now_kst)
            # sleep에서 깨어나면 즉시 다음 loop로 (바로 처리 시작)
            continue

        loop_t0 = time_mod.time()

        # 실행 구간 안에서는 "현재 시각이 속한 window" 1개만 처리
        cur_ctx = current_window_ctx(now_kst)

        # initialize active on first run-in-window
        if active_ctx is None:
            active_ctx = cur_ctx
            pending_ctx = None
            seen_pk.clear()
            last_pk = None
            log("INFO", f"[WINDOW] init => {active_ctx.prod_day}:{active_ctx.shift_type}")

        # window change detection (within run windows too)
        if (cur_ctx.prod_day != active_ctx.prod_day) or (cur_ctx.shift_type != active_ctx.shift_type):
            if pending_ctx is None:
                pending_ctx = cur_ctx
                log("INFO", f"[WINDOW] change detected => pending {pending_ctx.prod_day}:{pending_ctx.shift_type} "
                            f"(active={active_ctx.prod_day}:{active_ctx.shift_type})")

        try:
            with engine.begin() as conn:
                configure_session(conn)
                ensure_save_objects(conn, SAVE_SCHEMA, active_ctx.dest_table)

                # warm-start last_pk per active window
                if last_pk is None:
                    log("INFO", f"[LAST_PK] reading dest ({SAVE_SCHEMA}.{active_ctx.dest_table}) "
                                f"prod_day={active_ctx.prod_day} shift={active_ctx.shift_type}")
                    last_pk = read_last_pk(conn, active_ctx)
                    if last_pk:
                        log("INFO", f"[LAST_PK] found end_ts={last_pk.end_ts} barcode={last_pk.barcode} "
                                    f"test_contents={last_pk.test_contents}")
                    else:
                        log("INFO", "[LAST_PK] none")

                # fetch incremental
                log("INFO", f"[FETCH] prod_day={active_ctx.prod_day} shift={active_ctx.shift_type} "
                            f"window={active_ctx.start_dt}~{active_ctx.end_dt} lookback={LOOKBACK_SEC}s")
                rows = fetch_rows_incremental(conn, active_ctx, last_pk, LOOKBACK_SEC)

                # seen_pk filter
                new_rows: List[Dict] = []
                for r in rows:
                    key = make_seen_key(r)
                    if key in seen_pk:
                        continue
                    seen_pk.add(key)
                    for c in SAVE_COLS:
                        if c not in r:
                            r[c] = None
                    new_rows.append({c: r[c] for c in SAVE_COLS})

                log("INFO", f"[FETCH] got={len(rows)} new_after_seen={len(new_rows)}")

                # upsert
                if new_rows:
                    n = upsert_rows(conn, active_ctx, new_rows)
                    log("INFO", f"[UPSERT] table={SAVE_SCHEMA}.{active_ctx.dest_table} rows={n}")

                    new_last = update_last_pk_from_rows(new_rows)
                    if new_last:
                        last_pk = new_last
                        log("INFO", f"[LAST_PK] advanced => end_ts={last_pk.end_ts} "
                                    f"barcode={last_pk.barcode} test_contents={last_pk.test_contents}")

                # switch finalize: "이전 shift 미처리분 마저 처리 후 전환"
                if pending_ctx is not None:
                    grace_end = active_ctx.end_dt + timedelta(seconds=SWITCH_GRACE_SEC)
                    # now는 active window end + lookback 이후이고, 더 이상 new가 없으면 스위치
                    if now_kst > grace_end and len(new_rows) == 0:
                        log("INFO", f"[WINDOW] switching => {pending_ctx.prod_day}:{pending_ctx.shift_type}")
                        active_ctx = pending_ctx
                        pending_ctx = None
                        seen_pk.clear()
                        last_pk = None

        except (OperationalError, DBAPIError) as e:
            log("RETRY", f"DB error: {type(e).__name__}: {e} (reconnect)")
            try:
                engine.dispose()
            except Exception:
                pass
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = connect_with_retry()
        except Exception as e:
            log("RETRY", f"Unhandled error: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

        # loop pacing (run window 안에서만 5초)
        elapsed = time_mod.time() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    run()
