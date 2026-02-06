# -*- coding: utf-8 -*-
"""
backend6_worst_case_daemon.py

[추가 실행 조건 반영 + 안정화 + DB 로그 적재]
- DB 연결된 상태로 프로그램 상시 대기
- 매일 08:20:00~08:30:00 실행
- 매일 20:20:00~20:30:00 실행
- 그 외 시간은 sleep (다음 실행 구간 시작까지, 5초 heartbeat)
- 로그: 콘솔 + 파일 동시 기록
  고정 경로: C:\\AptivAgent\\_logs\\backend6_worst_case_daemon_YYYYMMDD.log

[DB 로그 사양]
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: "6_log" (없으면 생성)
- 컬럼:
  1) end_day  : yyyymmdd
  2) end_time : hh:mi:ss
  3) info     : 예) error, down, sleep (소문자)
  4) contents : 로그 본문
- 저장 컬럼 순서: end_day, end_time, info, contents
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
# DB Log table config
# =========================
LOG_DB_SCHEMA = "k_demon_heath_check"
LOG_DB_TABLE = "6_log"  # 반드시 쿼리에서 "6_log"로 quote 처리

LOG_DB_COLS = ["end_day", "end_time", "info", "contents"]  # 순서 고정
LOG_BUFFER_MAX = 10000  # 메모리 큐 상한

# =========================
# Fixed log path
# =========================
LOG_DIR = Path(r"C:\AptivAgent\_logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)


# =========================
# In-memory DB log buffer
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
    now_kst = datetime.now(KST)
    row = _mk_db_log_row(now_kst, level, msg)
    DB_LOG_BUFFER.append(row)


# =========================
# Logging (console + file + queue for DB)
# =========================
def _log_file_path(now_kst: datetime) -> Path:
    return LOG_DIR / f"backend6_worst_case_daemon_{now_kst.strftime('%Y%m%d')}.log"


def log(level: str, msg: str, exc: Optional[BaseException] = None) -> None:
    now_kst = datetime.now(KST)
    ts = now_kst.strftime("%Y-%m-%d %H:%M:%S")
    level_lc = str(level).lower()
    line = f"{ts} [{level_lc}] {msg}"

    # console
    print(line, flush=True)

    # file
    try:
        fp = _log_file_path(now_kst)
        with fp.open("a", encoding="utf-8") as f:
            f.write(line + "\n")
            if exc is not None:
                f.write(traceback.format_exc() + "\n")
    except Exception:
        # 파일 기록 실패해도 메인 루프는 유지
        pass

    # DB queue
    _enqueue_db_log(level_lc, msg)
    if exc is not None:
        _enqueue_db_log("error", traceback.format_exc())


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
    d1 = d0 + timedelta(days=1)
    return datetime.combine(d1, RUN_WINDOWS[0][0], tzinfo=KST)


def _sleep_until_next_run_with_heartbeat() -> None:
    while True:
        now_kst = datetime.now(KST)
        if _in_run_window(now_kst):
            return
        nxt = _next_run_start(now_kst)
        sec = max(1, int((nxt - now_kst).total_seconds()))
        log("sleep", f"[SLEEP] outside run window. next_start={nxt} remain={sec}s")
        time_mod.sleep(min(5, sec))


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
    log("boot", "backend6 worst-case daemon starting")
    while True:
        try:
            eng = make_engine()
            with eng.connect() as conn:
                configure_session(conn)
                ensure_log_table(conn)     # DB 로그 테이블 먼저 보장
                flush_db_logs(conn)        # 접속 복구 시 버퍼 플러시
            log("info", f"DB connected (work_mem={WORK_MEM})")
            return eng
        except Exception as e:
            log("down", f"DB connect failed: {type(e).__name__}: {e}", e)
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


def ensure_log_table(conn: Connection) -> None:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {LOG_DB_SCHEMA};"))
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {LOG_DB_SCHEMA}."{LOG_DB_TABLE}" (
      end_day  text,
      end_time text,
      info     text,
      contents text
    );
    """
    conn.execute(text(ddl))


def flush_db_logs(conn: Connection) -> int:
    if not DB_LOG_BUFFER:
        return 0

    # 버퍼 스냅샷
    rows = list(DB_LOG_BUFFER)

    # 사양: end_day, end_time, info, contents 순서 dataframe화
    df = pd.DataFrame(rows, columns=LOG_DB_COLS)
    if df.empty:
        return 0

    sql = text(f"""
        INSERT INTO {LOG_DB_SCHEMA}."{LOG_DB_TABLE}" (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
    """)

    payload = df.to_dict(orient="records")
    conn.execute(sql, payload)

    # 성공 시 버퍼 비우기
    DB_LOG_BUFFER.clear()
    return len(payload)


@dataclass
class LastPK:
    end_ts: datetime           # naive datetime (KST 기준)
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
    updated_at = datetime.now(KST)  # timestamptz (aware)

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
            set_parts.append(
                f'"{c}" = COALESCE({SAVE_SCHEMA}.{ctx.dest_table}."{c}", EXCLUDED."{c}")'
            )
        else:
            set_parts.append(
                f'"{c}" = COALESCE(EXCLUDED."{c}", {SAVE_SCHEMA}.{ctx.dest_table}."{c}")'
            )

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


def _parse_end_day_to_ymd(end_day_val: str) -> Tuple[int, int, int]:
    s = str(end_day_val).strip()
    # YYYY-MM-DD
    if "-" in s:
        y, m, d = map(int, s.split("-"))
        return y, m, d
    # YYYYMMDD
    if len(s) == 8 and s.isdigit():
        return int(s[0:4]), int(s[4:6]), int(s[6:8])
    raise ValueError(f"Unsupported end_day format: {s}")


def update_last_pk_from_rows(rows: List[Dict]) -> Optional[LastPK]:
    if not rows:
        return None

    last = rows[-1]
    end_day_str = str(last["end_day"]).strip()
    end_time_norm = str(last["end_time"]).strip()
    barcode = str(last["barcode_information"])
    testc = str(last["test_contents"])

    y, m, d = _parse_end_day_to_ymd(end_day_str)
    hh, mm, ss = map(int, end_time_norm.split(":"))
    end_ts = datetime(y, m, d, hh, mm, ss)  # naive

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

    log("info", f"[CFG] RUN_WINDOWS={RUN_WINDOWS}, LOOP={LOOP_INTERVAL_SEC}s, LOOKBACK={LOOKBACK_SEC}s")
    log("info", f"[CFG] SRC={SRC_SCHEMA}.{T_PD}/{T_NPD}, MAP={MAP_SCHEMA}.{MAP_TABLE}, DST={SAVE_SCHEMA}.{T_SAVE_DAY}/{T_SAVE_NIGHT}")
    log("info", f"[CFG] LOG_DIR={LOG_DIR}")
    log("info", f"[CFG] DB_LOG_TABLE={LOG_DB_SCHEMA}.\"{LOG_DB_TABLE}\"")

    while True:
        now_kst = datetime.now(KST)

        # ====== 스케줄: 실행 구간이 아니면 다음 시작까지 대기 ======
        if not _in_run_window(now_kst):
            _sleep_until_next_run_with_heartbeat()
            continue

        loop_t0 = time_mod.time()
        cur_ctx = current_window_ctx(now_kst)

        # initialize active on first run-in-window
        if active_ctx is None:
            active_ctx = cur_ctx
            pending_ctx = None
            seen_pk.clear()
            last_pk = None
            log("info", f"[WINDOW] init => {active_ctx.prod_day}:{active_ctx.shift_type}")

        # window change detection
        if (cur_ctx.prod_day != active_ctx.prod_day) or (cur_ctx.shift_type != active_ctx.shift_type):
            if pending_ctx is None:
                pending_ctx = cur_ctx
                log("info",
                    f"[WINDOW] change detected => pending {pending_ctx.prod_day}:{pending_ctx.shift_type} "
                    f"(active={active_ctx.prod_day}:{active_ctx.shift_type})")

        try:
            with engine.begin() as conn:
                configure_session(conn)
                ensure_log_table(conn)
                flush_db_logs(conn)  # 주기적으로 flush

                ensure_save_objects(conn, SAVE_SCHEMA, active_ctx.dest_table)

                # warm-start last_pk per active window
                if last_pk is None:
                    log("info",
                        f"[LAST_PK] read dest={SAVE_SCHEMA}.{active_ctx.dest_table} "
                        f"prod_day={active_ctx.prod_day} shift={active_ctx.shift_type}")
                    last_pk = read_last_pk(conn, active_ctx)
                    if last_pk:
                        log("info",
                            f"[LAST_PK] found end_ts={last_pk.end_ts} "
                            f"barcode={last_pk.barcode} test_contents={last_pk.test_contents}")
                    else:
                        log("info", "[LAST_PK] none")

                # fetch incremental
                log("info",
                    f"[FETCH] prod_day={active_ctx.prod_day} shift={active_ctx.shift_type} "
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

                log("info", f"[FETCH] got={len(rows)} new_after_seen={len(new_rows)}")

                # upsert
                if new_rows:
                    n = upsert_rows(conn, active_ctx, new_rows)
                    log("info", f"[UPSERT] table={SAVE_SCHEMA}.{active_ctx.dest_table} rows={n}")

                    new_last = update_last_pk_from_rows(new_rows)
                    if new_last:
                        last_pk = new_last
                        log("info",
                            f"[LAST_PK] advanced end_ts={last_pk.end_ts} "
                            f"barcode={last_pk.barcode} test_contents={last_pk.test_contents}")

                # switch finalize: "이전 shift 미처리분 마저 처리 후 전환"
                if pending_ctx is not None:
                    grace_end = active_ctx.end_dt + timedelta(seconds=SWITCH_GRACE_SEC)
                    if now_kst > grace_end and len(new_rows) == 0:
                        log("info", f"[WINDOW] switching => {pending_ctx.prod_day}:{pending_ctx.shift_type}")
                        active_ctx = pending_ctx
                        pending_ctx = None
                        seen_pk.clear()
                        last_pk = None

                # 루프 말미 한 번 더 flush (현재 루프 로그 반영)
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

        # loop pacing (run window 안에서만 5초)
        elapsed = time_mod.time() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    run()
