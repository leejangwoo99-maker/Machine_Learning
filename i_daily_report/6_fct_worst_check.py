# -*- coding: utf-8 -*-
"""
backend6_worst_case_daemon.py

요구사항 반영:
1) remark 중복(PD PD) 방지 -> remark는 PD/Non-PD 로만 저장
2) 24시간 5초 루프
3) 최근 36시간 late-arrival backfill
4) 기존 테이블에도 UNIQUE 인덱스 강제 보장
5) 타입 안정화:
   - end_day: YYYY-MM-DD 문자열 저장
   - prod_day: YYYYMMDD 문자열 저장
"""

from __future__ import annotations

import os
import time as time_mod
import traceback
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from pathlib import Path
from typing import Optional, Tuple, List, Dict, Set
from collections import deque

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
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

LOOKBACK_SEC = 180
BACKFILL_HOURS = 36
BACKFILL_EVERY_SEC = 300  # 5분

# =========================
# DB config
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
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
# DB Log table config
# =========================
LOG_DB_SCHEMA = "k_demon_heath_check"
LOG_DB_TABLE = "6_log"
LOG_DB_COLS = ["end_day", "end_time", "info", "contents"]  # 순서 고정
LOG_BUFFER_MAX = 10000

# =========================
# Fixed log path
# =========================
LOG_DIR = Path(r"C:\AptivAgent\_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# Dataclasses
# =========================
@dataclass(frozen=True)
class WindowCtx:
    prod_day: str          # YYYYMMDD
    shift_type: str
    start_dt: datetime     # aware(KST)
    end_dt: datetime       # aware(KST)
    dest_table: str


@dataclass
class LastPK:
    end_ts: datetime       # naive (KST local clock)
    barcode: str
    test_contents: str
    end_day: str           # YYYY-MM-DD
    end_time_norm: str     # HH:MM:SS


# =========================
# Log buffer
# =========================
DB_LOG_BUFFER = deque(maxlen=LOG_BUFFER_MAX)


def _mk_db_log_row(now_kst: datetime, level: str, msg: str) -> Dict[str, str]:
    return {
        "end_day": now_kst.strftime("%Y%m%d"),
        "end_time": now_kst.strftime("%H:%M:%S"),
        "info": str(level).lower(),
        "contents": str(msg),
    }


def _enqueue_db_log(level: str, msg: str) -> None:
    DB_LOG_BUFFER.append(_mk_db_log_row(datetime.now(KST), level, msg))


# =========================
# Logging
# =========================
def _log_file_path(now_kst: datetime) -> Path:
    return LOG_DIR / f"backend6_worst_case_daemon_{now_kst.strftime('%Y%m%d')}.log"


def log(level: str, msg: str, exc: Optional[BaseException] = None) -> None:
    now_kst = datetime.now(KST)
    ts = now_kst.strftime("%Y-%m-%d %H:%M:%S")
    lvl = str(level).lower()
    line = f"{ts} [{lvl}] {msg}"

    print(line, flush=True)

    try:
        fp = _log_file_path(now_kst)
        with fp.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            if exc is not None:
                f.write(traceback.format_exc() + "\n")
    except Exception:
        pass

    _enqueue_db_log(lvl, msg)
    if exc is not None:
        _enqueue_db_log("error", traceback.format_exc())


# =========================
# Window helpers
# =========================
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
        return WindowCtx(
            prod_day=_yyyymmdd(d0),
            shift_type="day",
            start_dt=datetime.combine(d0, day_start_t, tzinfo=KST),
            end_dt=datetime.combine(d0, day_end_t, tzinfo=KST),
            dest_table=T_SAVE_DAY,
        )

    if t0 >= night_start_t:
        prod_day_date = d0
        start_dt = datetime.combine(d0, night_start_t, tzinfo=KST)
        end_dt = datetime.combine(d0 + timedelta(days=1), night_end_t, tzinfo=KST)
    else:
        prod_day_date = d0 - timedelta(days=1)
        start_dt = datetime.combine(prod_day_date, night_start_t, tzinfo=KST)
        end_dt = datetime.combine(d0, night_end_t, tzinfo=KST)

    return WindowCtx(
        prod_day=_yyyymmdd(prod_day_date),
        shift_type="night",
        start_dt=start_dt,
        end_dt=end_dt,
        dest_table=T_SAVE_NIGHT,
    )


def build_windows_for_backfill(now_kst: datetime, hours: int) -> List[WindowCtx]:
    lb = now_kst - timedelta(hours=hours)
    ub = now_kst

    d_start = lb.date() - timedelta(days=2)
    d_end = ub.date() + timedelta(days=1)

    out: List[WindowCtx] = []
    d = d_start
    while d <= d_end:
        day_ws = datetime.combine(d, time(8, 30, 0), tzinfo=KST)
        day_we = datetime.combine(d, time(20, 29, 59), tzinfo=KST)
        if day_we >= lb and day_ws <= ub:
            out.append(WindowCtx(_yyyymmdd(d), "day", day_ws, day_we, T_SAVE_DAY))

        n_ws = datetime.combine(d, time(20, 30, 0), tzinfo=KST)
        n_we = datetime.combine(d + timedelta(days=1), time(8, 29, 59), tzinfo=KST)
        if n_we >= lb and n_ws <= ub:
            out.append(WindowCtx(_yyyymmdd(d), "night", n_ws, n_we, T_SAVE_NIGHT))
        d += timedelta(days=1)

    out.sort(key=lambda w: (w.start_dt, w.shift_type))
    uniq, seen = [], set()
    for w in out:
        k = (w.prod_day, w.shift_type)
        if k in seen:
            continue
        seen.add(k)
        uniq.append(w)
    return uniq


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


def ensure_log_table(conn: Connection) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {LOG_DB_SCHEMA};"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {LOG_DB_SCHEMA}."{LOG_DB_TABLE}" (
          end_day  text,
          end_time text,
          info     text,
          contents text
        );
    """))


def flush_db_logs(conn: Connection) -> int:
    if not DB_LOG_BUFFER:
        return 0
    df = pd.DataFrame(list(DB_LOG_BUFFER), columns=LOG_DB_COLS)
    if df.empty:
        return 0
    conn.execute(
        text(f"""
            INSERT INTO {LOG_DB_SCHEMA}."{LOG_DB_TABLE}" (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """),
        df.to_dict(orient="records"),
    )
    n = len(df)
    DB_LOG_BUFFER.clear()
    return n


def ensure_save_objects(conn: Connection, table: str) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA};"))

    cols_ddl = []
    for c in SAVE_COLS:
        if c == "updated_at":
            cols_ddl.append(f'"{c}" timestamptz')
        else:
            cols_ddl.append(f'"{c}" text')
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{table} (
          {", ".join(cols_ddl)}
        );
    """))

    # 기존 테이블 포함 UNIQUE 인덱스 강제 보장
    uq_idx = f"ux_{table}_dedup"
    uq_cols_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])
    conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {uq_idx}
        ON {SAVE_SCHEMA}.{table} ({uq_cols_sql});
    """))


def connect_with_retry() -> Engine:
    log("boot", "backend6 worst-case daemon starting")
    while True:
        try:
            eng = make_engine()
            with eng.begin() as conn:
                configure_session(conn)
                ensure_log_table(conn)
                ensure_save_objects(conn, T_SAVE_DAY)
                ensure_save_objects(conn, T_SAVE_NIGHT)
                flush_db_logs(conn)
            log("info", f"DB connected (work_mem={WORK_MEM})")
            return eng
        except Exception as e:
            log("down", f"DB connect failed: {type(e).__name__}: {e}", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# Read last pk
# =========================
def read_last_pk(conn: Connection, ctx: WindowCtx) -> Optional[LastPK]:
    ensure_save_objects(conn, ctx.dest_table)

    sql = f"""
    WITH x AS (
      SELECT
        end_day,
        end_time,
        barcode_information,
        test_contents,
        (to_date(end_day, 'YYYY-MM-DD')::timestamp + (end_time::time)) AS end_ts,
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
    row = conn.execute(text(sql), {
        "prod_day": ctx.prod_day,
        "shift_type": ctx.shift_type
    }).fetchone()

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
# - end_day: YYYY-MM-DD 문자열로 정규화
# - prod_day: 바인딩 YYYYMMDD
# - remark: PD/Non-PD만 저장
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
    to_char(s.end_day::date, 'YYYY-MM-DD') AS end_day_norm,
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
  :prod_day AS prod_day,         -- YYYYMMDD
  :shift_type AS shift_type,
  barcode_information,
  COALESCE(mapped_pn, 'Unknown') AS pn,
  CASE
    WHEN src_type = 'PD' THEN 'PD'
    ELSE 'Non-PD'
  END AS remark,
  station,
  end_day_norm AS end_day,       -- YYYY-MM-DD 문자열
  end_time_norm AS end_time,     -- HH:MM:SS
  run_time::text AS run_time,
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


def fetch_rows_incremental(conn: Connection, ctx: WindowCtx, last_pk: Optional[LastPK]) -> List[Dict]:
    params = {
        "prod_day": ctx.prod_day,
        "shift_type": ctx.shift_type,
        "fetch_start_ts": ctx.start_dt.replace(tzinfo=None) if last_pk is None else max(
            ctx.start_dt.replace(tzinfo=None),
            last_pk.end_ts - timedelta(seconds=LOOKBACK_SEC),
        ),
        "window_end_ts": ctx.end_dt.replace(tzinfo=None),
        "updated_at": datetime.now(KST),
        "has_last": last_pk is not None,
        "last_end_ts": datetime(1970, 1, 1) if last_pk is None else last_pk.end_ts,
        "last_barcode": "" if last_pk is None else last_pk.barcode,
        "last_test_contents": "" if last_pk is None else last_pk.test_contents,
    }

    sql = FETCH_SQL.format(
        src_schema=SRC_SCHEMA, pd_table=T_PD, npd_table=T_NPD,
        map_schema=MAP_SCHEMA, map_table=MAP_TABLE
    )
    rows = conn.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d.pop("end_ts", None)
        out.append(d)
    return out


def fetch_rows_full_window(conn: Connection, ctx: WindowCtx) -> List[Dict]:
    params = {
        "prod_day": ctx.prod_day,
        "shift_type": ctx.shift_type,
        "fetch_start_ts": ctx.start_dt.replace(tzinfo=None),
        "window_end_ts": ctx.end_dt.replace(tzinfo=None),
        "updated_at": datetime.now(KST),
        "has_last": False,
        "last_end_ts": datetime(1970, 1, 1),
        "last_barcode": "",
        "last_test_contents": "",
    }

    sql = FETCH_SQL.format(
        src_schema=SRC_SCHEMA, pd_table=T_PD, npd_table=T_NPD,
        map_schema=MAP_SCHEMA, map_table=MAP_TABLE
    )
    rows = conn.execute(text(sql), params).mappings().all()

    out = []
    for r in rows:
        d = dict(r)
        d.pop("end_ts", None)
        out.append(d)
    return out


# =========================
# Upsert
# =========================
def upsert_rows(conn: Connection, ctx: WindowCtx, rows: List[Dict]) -> int:
    if not rows:
        return 0

    ensure_save_objects(conn, ctx.dest_table)

    col_sql = ", ".join([f'"{c}"' for c in SAVE_COLS])
    val_sql = ", ".join([f":{c}" for c in SAVE_COLS])
    conflict_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])

    set_parts = []
    for c in SAVE_COLS:
        if c in UNIQUE_COLS:
            continue
        if c == "updated_at":
            set_parts.append(f'"{c}" = EXCLUDED."{c}"')
        elif c in ("pn", "remark"):
            set_parts.append(
                f'''"{c}" = CASE
                    WHEN EXCLUDED."{c}" IS NULL OR btrim(EXCLUDED."{c}") = '' THEN {SAVE_SCHEMA}.{ctx.dest_table}."{c}"
                    WHEN lower(btrim(EXCLUDED."{c}")) = 'unknown' THEN COALESCE({SAVE_SCHEMA}.{ctx.dest_table}."{c}", EXCLUDED."{c}")
                    ELSE EXCLUDED."{c}"
                END'''
            )
        else:
            set_parts.append(f'"{c}" = COALESCE(EXCLUDED."{c}", {SAVE_SCHEMA}.{ctx.dest_table}."{c}")')

    upsert_sql = f"""
    INSERT INTO {SAVE_SCHEMA}.{ctx.dest_table} ({col_sql})
    VALUES ({val_sql})
    ON CONFLICT ({conflict_sql})
    DO UPDATE SET
      {", ".join(set_parts)};
    """
    conn.execute(text(upsert_sql), rows)
    return len(rows)


# =========================
# Helpers
# =========================
def make_seen_key(r: Dict) -> Tuple[str, str, str, str]:
    return (
        str(r["end_day"]),
        str(r["end_time"]),
        str(r["barcode_information"]),
        str(r["test_contents"]),
    )


def _parse_end_day_to_ymd(end_day_val: str) -> Tuple[int, int, int]:
    s = str(end_day_val).strip()
    if "-" in s:
        y, m, d = map(int, s.split("-"))
        return y, m, d
    if len(s) == 8 and s.isdigit():
        return int(s[0:4]), int(s[4:6]), int(s[6:8])
    raise ValueError(f"Unsupported end_day format: {s}")


def update_last_pk_from_rows(rows: List[Dict]) -> Optional[LastPK]:
    if not rows:
        return None
    last = rows[-1]
    end_day = str(last["end_day"]).strip()      # YYYY-MM-DD
    end_time = str(last["end_time"]).strip()    # HH:MM:SS
    y, m, d = _parse_end_day_to_ymd(end_day)
    hh, mm, ss = map(int, end_time.split(":"))
    return LastPK(
        end_ts=datetime(y, m, d, hh, mm, ss),
        barcode=str(last["barcode_information"]),
        test_contents=str(last["test_contents"]),
        end_day=end_day,
        end_time_norm=end_time,
    )


def run_backfill_windows(conn: Connection, now_kst: datetime) -> int:
    windows = build_windows_for_backfill(now_kst, BACKFILL_HOURS)
    total = 0
    log("info", f"[BACKFILL] start hours={BACKFILL_HOURS}, windows={len(windows)}")

    for w in windows:
        ensure_save_objects(conn, w.dest_table)
        rows = fetch_rows_full_window(conn, w)
        if not rows:
            continue

        dedup_local: Set[Tuple[str, str, str, str]] = set()
        prepared: List[Dict] = []

        for r in rows:
            k = make_seen_key(r)
            if k in dedup_local:
                continue
            dedup_local.add(k)

            for c in SAVE_COLS:
                if c not in r:
                    r[c] = None

            # 타입/포맷 강제 안정화
            r["prod_day"] = str(r["prod_day"]).replace("-", "")[:8]  # YYYYMMDD
            if len(r["prod_day"]) != 8:
                # fail-safe
                r["prod_day"] = w.prod_day
            # end_day는 YYYY-MM-DD 유지
            if isinstance(r["end_day"], date):
                r["end_day"] = r["end_day"].strftime("%Y-%m-%d")
            else:
                r["end_day"] = str(r["end_day"])

            prepared.append({c: r[c] for c in SAVE_COLS})

        if prepared:
            n = upsert_rows(conn, w, prepared)
            total += n
            log("info", f"[BACKFILL] {w.prod_day}:{w.shift_type} rows={n}")

    log("info", f"[BACKFILL] done total_rows={total}")
    return total


# =========================
# Main loop
# =========================
def run() -> None:
    engine = connect_with_retry()

    active_ctx: Optional[WindowCtx] = None
    seen_pk: Set[Tuple[str, str, str, str]] = set()
    last_pk: Optional[LastPK] = None
    last_backfill_at: Optional[datetime] = None

    log("info", f"[CFG] LOOP={LOOP_INTERVAL_SEC}s, LOOKBACK={LOOKBACK_SEC}s")
    log("info", f"[CFG] BACKFILL_HOURS={BACKFILL_HOURS}, BACKFILL_EVERY_SEC={BACKFILL_EVERY_SEC}")
    log("info", f"[CFG] SRC={SRC_SCHEMA}.{T_PD}/{T_NPD}, MAP={MAP_SCHEMA}.{MAP_TABLE}")
    log("info", f"[CFG] DST={SAVE_SCHEMA}.{T_SAVE_DAY}/{T_SAVE_NIGHT}")
    log("info", f"[CFG] LOG_DIR={LOG_DIR}")
    log("info", f"[CFG] DB_LOG_TABLE={LOG_DB_SCHEMA}.\"{LOG_DB_TABLE}\"")

    while True:
        t0 = time_mod.time()
        now_kst = datetime.now(KST)
        cur_ctx = current_window_ctx(now_kst)

        try:
            with engine.begin() as conn:
                configure_session(conn)
                ensure_log_table(conn)
                ensure_save_objects(conn, T_SAVE_DAY)
                ensure_save_objects(conn, T_SAVE_NIGHT)
                flush_db_logs(conn)

                if active_ctx is None or (active_ctx.prod_day != cur_ctx.prod_day or active_ctx.shift_type != cur_ctx.shift_type):
                    active_ctx = cur_ctx
                    seen_pk.clear()
                    last_pk = None
                    log("info", f"[WINDOW] active => {active_ctx.prod_day}:{active_ctx.shift_type}")

                if last_pk is None:
                    last_pk = read_last_pk(conn, active_ctx)
                    log("info", f"[LAST_PK] {active_ctx.prod_day}:{active_ctx.shift_type} {'none' if last_pk is None else last_pk.end_ts}")

                rows = fetch_rows_incremental(conn, active_ctx, last_pk)
                new_rows: List[Dict] = []

                for r in rows:
                    k = make_seen_key(r)
                    if k in seen_pk:
                        continue
                    seen_pk.add(k)

                    for c in SAVE_COLS:
                        if c not in r:
                            r[c] = None

                    # 타입/포맷 강제
                    r["prod_day"] = str(r["prod_day"]).replace("-", "")[:8]  # YYYYMMDD
                    if len(r["prod_day"]) != 8:
                        r["prod_day"] = active_ctx.prod_day

                    if isinstance(r["end_day"], date):
                        r["end_day"] = r["end_day"].strftime("%Y-%m-%d")
                    else:
                        r["end_day"] = str(r["end_day"])

                    r["remark"] = "PD" if str(r.get("remark", "")).upper() == "PD" else "Non-PD"

                    new_rows.append({c: r[c] for c in SAVE_COLS})

                if new_rows:
                    n = upsert_rows(conn, active_ctx, new_rows)
                    log("info", f"[UPSERT] active {active_ctx.prod_day}:{active_ctx.shift_type} rows={n}")
                    last_pk = update_last_pk_from_rows(new_rows)
                else:
                    log("sleep", f"[IDLE] no new rows in active window {active_ctx.prod_day}:{active_ctx.shift_type}")

                do_backfill = (last_backfill_at is None) or ((now_kst - last_backfill_at).total_seconds() >= BACKFILL_EVERY_SEC)
                if do_backfill:
                    run_backfill_windows(conn, now_kst)
                    last_backfill_at = now_kst

                flush_db_logs(conn)

        except (OperationalError, DBAPIError) as e:
            log("down", f"DB error: {type(e).__name__}: {e} (reconnect)", e)
            try:
                engine.dispose()
            except Exception:
                pass
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = connect_with_retry()

        except Exception as e:
            log("error", f"Unhandled error: {type(e).__name__}: {e}", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

        time_mod.sleep(max(0.0, LOOP_INTERVAL_SEC - (time_mod.time() - t0)))


if __name__ == "__main__":
    run()
