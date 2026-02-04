# -*- coding: utf-8 -*-
"""
backend6_worst_case_daemon_TEST_20260128_DAY.py

[TEST 고정 버전]
- prod_day = 20260128 (고정)
- shift_type = day (고정)
- window = 2026-01-28 08:30:00 ~ 2026-01-28 20:29:59 (KST, 고정)

- 스케줄(08:20~08:30 / 20:20~20:30) 비활성화: 테스트 즉시 실행 가능
- window 자동 전환 로직 비활성화: 고정 window만 증분 처리
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from typing import Optional, Tuple, List, Dict, Set

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

# =========================
# TEST FIXED WINDOW
# =========================
TEST_PROD_DAY = "20260128"
TEST_SHIFT_TYPE = "day"
TEST_WINDOW_START = datetime(2026, 1, 28, 8, 30, 0, tzinfo=KST)
TEST_WINDOW_END = datetime(2026, 1, 28, 20, 29, 59, tzinfo=KST)

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

# UNIQUE 확정(확장)
UNIQUE_COLS = ["end_day", "end_time", "barcode_information", "test_contents"]


# =========================
# Logging
# =========================
def log(level: str, msg: str) -> None:
    ts = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


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
    log("BOOT", f"backend6 TEST starting (prod_day={TEST_PROD_DAY}, shift={TEST_SHIFT_TYPE})")
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
    """
    ✅ 핵심 수정:
    - 테이블이 이미 존재하면 'CONSTRAINT UNIQUE'는 추가되지 않음
    - 따라서 ON CONFLICT 타겟을 만족하는 UNIQUE INDEX를 항상 보장해야 함
    """
    # schema
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

    # table (컬럼만 보장)
    cols_ddl = []
    for c in SAVE_COLS:
        if c == "updated_at":
            cols_ddl.append(f'"{c}" timestamptz')
        else:
            cols_ddl.append(f'"{c}" text')
    cols_sql = ",\n  ".join(cols_ddl)

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      {cols_sql}
    );
    """
    conn.execute(text(ddl))

    # ✅ UNIQUE INDEX 보장 (ON CONFLICT용)
    idx_name = f"ux_{table}_worst_pk"
    idx_cols_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])

    conn.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {idx_name}
        ON {schema}.{table} ({idx_cols_sql});
    """))


# =========================
# Window context (fixed)
# =========================
@dataclass(frozen=True)
class WindowCtx:
    prod_day: str
    shift_type: str
    start_dt: datetime
    end_dt: datetime
    dest_table: str


FIXED_CTX = WindowCtx(
    prod_day=TEST_PROD_DAY,
    shift_type=TEST_SHIFT_TYPE,
    start_dt=TEST_WINDOW_START,
    end_dt=TEST_WINDOW_END,
    dest_table=T_SAVE_DAY,  # day 고정
)


# =========================
# last_pk warm-start
# =========================
@dataclass
class LastPK:
    end_ts: datetime            # naive timestamp
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
# Fetch SQL (매 루프 JOIN)
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
  src_type AS remark,
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
    updated_at = datetime.now(KST)  # timestamptz aware

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
# UPSERT (DELETE 금지)
# - updated_at: always overwrite
# - pn/remark: freeze once saved (target 우선)
# - others: COALESCE(excluded, target)
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
# seen_pk + last_pk
# =========================
def make_seen_key(r: Dict) -> Tuple[str, str, str, str]:
    end_day = str(r["end_day"])
    end_time_norm = str(r["end_time"])  # already HH:MI:SS
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

    # end_day_str가 'YYYY-MM-DD' 형태라고 가정(SELECT에서 date로 내려옴)
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
# Main loop (fixed window)
# =========================
def run() -> None:
    engine = connect_with_retry()

    ctx = FIXED_CTX
    seen_pk: Set[Tuple[str, str, str, str]] = set()
    last_pk: Optional[LastPK] = None

    log("INFO", f"[TEST] fixed window => prod_day={ctx.prod_day} shift={ctx.shift_type} "
                f"start={ctx.start_dt} end={ctx.end_dt} dest={SAVE_SCHEMA}.{ctx.dest_table}")

    while True:
        loop_t0 = time_mod.time()
        try:
            with engine.begin() as conn:
                configure_session(conn)
                ensure_save_objects(conn, SAVE_SCHEMA, ctx.dest_table)

                # warm-start once
                if last_pk is None:
                    log("INFO", f"[LAST_PK] reading dest ({SAVE_SCHEMA}.{ctx.dest_table}) "
                                f"prod_day={ctx.prod_day} shift={ctx.shift_type}")
                    last_pk = read_last_pk(conn, ctx)
                    if last_pk:
                        log("INFO", f"[LAST_PK] found end_ts={last_pk.end_ts} barcode={last_pk.barcode} "
                                    f"test_contents={last_pk.test_contents}")
                    else:
                        log("INFO", "[LAST_PK] none")

                # fetch
                log("INFO", f"[FETCH] window={ctx.start_dt}~{ctx.end_dt} lookback={LOOKBACK_SEC}s")
                rows = fetch_rows_incremental(conn, ctx, last_pk, LOOKBACK_SEC)

                # ✅ 핵심 수정: UPSERT 성공 후에만 seen_pk 반영 (실패 시 유실 방지)
                candidate_rows: List[Dict] = []
                candidate_keys: List[Tuple[str, str, str, str]] = []

                for r in rows:
                    key = make_seen_key(r)
                    if key in seen_pk:
                        continue

                    for c in SAVE_COLS:
                        if c not in r:
                            r[c] = None

                    candidate_rows.append({c: r[c] for c in SAVE_COLS})
                    candidate_keys.append(key)

                log("INFO", f"[FETCH] got={len(rows)} new_after_seen={len(candidate_rows)}")

                # upsert
                if candidate_rows:
                    n = upsert_rows(conn, ctx, candidate_rows)
                    log("INFO", f"[UPSERT] table={SAVE_SCHEMA}.{ctx.dest_table} rows={n}")

                    # ✅ UPSERT 성공 후 seen 반영
                    for k in candidate_keys:
                        seen_pk.add(k)

                    new_last = update_last_pk_from_rows(candidate_rows)
                    if new_last:
                        last_pk = new_last
                        log("INFO", f"[LAST_PK] advanced => end_ts={last_pk.end_ts} "
                                    f"barcode={last_pk.barcode} test_contents={last_pk.test_contents}")

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

        # 5s loop
        elapsed = time_mod.time() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    run()
