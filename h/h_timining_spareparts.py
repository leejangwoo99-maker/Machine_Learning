# -*- coding: utf-8 -*-
"""
h_timing_spareparts_v5_threshold_crossing.py
============================================================
- 기존 v4에서 "준비/권고/긴급이 안 저장되는 문제"를 해결:
  배치로 added가 크게 증가해 ratio 임계(0.5/0.8/0.9/1.0)를 건너뛰면
  최종 단계(교체)만 저장되는 현상이 발생.

해결:
- 신규 테스트 end_ts 목록(정렬)을 가져오고,
- 한 건씩 amount를 증가시키며 임계값을 "넘는 순간" 알람을 저장.

알람 저장 시간 규칙:
- alarm_record.end_day/end_time: 알람을 유발한 TEST의 end_ts(= end_day+end_time) 사용
- alarm_record.created_at: inspect 시간(now)

추가 조건:
- added=0 AND amount=0은 DBG 출력하지 않음
- "교체" 알람 이후 next_repl_ts(다음 교체)가 없으면 더 이상 amount 집계하지 않음
  (단, next_repl_ts 재탐지는 계속하여, 생기면 정상 롤링/재개)
"""

from __future__ import annotations

import time
import pickle
import traceback
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2


# =================================================
# 0) CONFIG
# =================================================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
SPAREPARTS = ["usb_a", "usb_c", "mini_b"]

LOOP_INTERVAL_SEC = 5
BATCH_LIMIT_TEST_ROWS = 5000  # 한 루프당 신규 테스트 최대 fetch

USE_FALLBACK_PREV_REPL = True

SESSION_GUARDS_SQL = """
SET application_name = 'h_timing_spareparts_v5';
SET statement_timeout = '30s';
SET lock_timeout = '5s';
SET idle_in_transaction_session_timeout = '30s';
SET work_mem = '64MB';
SET maintenance_work_mem = '64MB';
SET temp_buffers = '16MB';
"""

MODEL_SCHEMA = "h_machine_learning"
MODEL_TABLE = '3_machine_learning_model'

REPL_SCHEMA = "g_production_film"
REPL_TABLE = "fct_non_operation_time"

TEST_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
TEST_TABLE = "fct_vision_testlog_txt_processing_history"

LIFE_SCHEMA = "e3_sparepart_replacement"
LIFE_TABLE = "sparepart_life_amount"

ALARM_SCHEMA = "g_production_film"
ALARM_TABLE = "alarm_record"

STATE_SCHEMA = "h_machine_learning"
STATE_TABLE = "sparepart_interval_state_v4"


# =================================================
# 1) UTIL
# =================================================
def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def connect_forever() -> psycopg2.extensions.connection:
    while True:
        try:
            conn = psycopg2.connect(
                host=DB_CONFIG["host"],
                port=DB_CONFIG["port"],
                dbname=DB_CONFIG["dbname"],
                user=DB_CONFIG["user"],
                password=DB_CONFIG["password"],
                connect_timeout=5,
            )
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(SESSION_GUARDS_SQL)
            log("[OK] DB connected")
            return conn
        except Exception as e:
            log(f"[ERROR] DB connect failed: {type(e).__name__}: {e}")
            time.sleep(2)


def safe_close(conn: Optional[psycopg2.extensions.connection]) -> None:
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


def get_column_udt(conn, schema: str, table: str, column: str) -> Optional[str]:
    sql = """
    SELECT c.udt_name
    FROM information_schema.columns c
    WHERE c.table_schema=%s AND c.table_name=%s AND c.column_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table, column))
        row = cur.fetchone()
    return row[0] if row else None


def has_column(conn, schema: str, table: str, column: str) -> bool:
    sql = """
    SELECT 1
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s AND column_name=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table, column))
        return cur.fetchone() is not None


def require_column(conn, schema: str, table: str, column: str) -> None:
    if not has_column(conn, schema, table, column):
        raise RuntimeError(f"Required column missing: {schema}.{table}.{column}")


def parse_ymd(s: str) -> datetime:
    return datetime.strptime(s, "%Y-%m-%d")


def prompt_date_range_console() -> Tuple[str, str]:
    today = datetime.now().strftime("%Y-%m-%d")

    def _read_one(prompt: str) -> str:
        while True:
            v = input(prompt).strip()
            if v == "":
                return ""
            try:
                parse_ymd(v)
                return v
            except Exception:
                print("  형식 오류: YYYY-MM-DD 로 다시 입력하세요.")

    print("\n[DATE RANGE INPUT]")
    print(" - Enter만 누르면 오늘 날짜로 설정됩니다.")
    print(" - 형식: YYYY-MM-DD\n")

    s = _read_one(f"Start date (YYYY-MM-DD) [default {today}]: ")
    e = _read_one(f"End date   (YYYY-MM-DD) [default {today}]: ")

    if s == "" and e == "":
        s = today
        e = today
    elif s != "" and e == "":
        e = s
    elif s == "" and e != "":
        s = e

    if parse_ymd(s) > parse_ymd(e):
        s, e = e, s

    print(f"\n[DATE RANGE SET] {s} ~ {e}\n")
    return s, e


def window_end_timestamp(date_to: str) -> datetime:
    today = datetime.now().date()
    dt_to = parse_ymd(date_to).date()
    if dt_to >= today:
        return datetime.now()
    return datetime.combine(dt_to, datetime.max.time()).replace(microsecond=0)


def round_dt_to_sec(dt: datetime) -> datetime:
    if dt.microsecond >= 500_000:
        dt = dt + timedelta(seconds=1)
    return dt.replace(microsecond=0)


# =================================================
# 2) SQL EXPRESSIONS (end_day + time -> timestamp)
# =================================================
def end_day_as_date_expr(col: str = "end_day") -> str:
    return rf"""
    (
      CASE
        WHEN pg_typeof({col})::text IN ('date','timestamp without time zone','timestamp with time zone')
          THEN ({col})::date
        ELSE
          to_date(
            lpad(
              substring(regexp_replace({col}::text, '[^0-9]', '', 'g') from 1 for 8),
              8, '0'
            ),
            'YYYYMMDD'
          )
      END
    )
    """


def time_as_time_expr(col: str) -> str:
    return rf"""
    (
      CASE
        WHEN pg_typeof({col})::text IN ('time without time zone','time with time zone')
          THEN ({col})::time
        WHEN pg_typeof({col})::text IN ('timestamp without time zone','timestamp with time zone')
          THEN ({col})::time
        ELSE
          NULLIF(({col})::text, '')::time
      END
    )
    """


def ts_expr(day_col: str, time_col: str) -> str:
    d = end_day_as_date_expr(day_col)
    t = time_as_time_expr(time_col)
    return rf"(({d})::timestamp + ({t}))"


# =================================================
# 3) TABLE SETUP
# =================================================
def ensure_tables(conn) -> None:
    create_state = f"""
    CREATE TABLE IF NOT EXISTS {STATE_SCHEMA}.{STATE_TABLE} (
        date_from DATE NOT NULL,
        date_to   DATE NOT NULL,
        station   TEXT NOT NULL,
        sparepart TEXT NOT NULL,

        current_repl_ts TIMESTAMP,
        next_repl_ts    TIMESTAMP,
        last_test_ts    TIMESTAMP,
        amount          BIGINT NOT NULL DEFAULT 0,

        last_alarm_type TEXT,
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (date_from, date_to, station, sparepart)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_state)

    create_alarm = f"""
    CREATE TABLE IF NOT EXISTS {ALARM_SCHEMA}.{ALARM_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        end_day TEXT NOT NULL,
        end_time TEXT NOT NULL,
        station TEXT NOT NULL,
        sparepart TEXT NOT NULL,
        type_alarm TEXT NOT NULL,
        amount BIGINT NOT NULL DEFAULT 0,
        min_prob DOUBLE PRECISION NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_alarm)

    alter_cols = []
    for col_sql, col_name in [
        ("ADD COLUMN end_day TEXT NOT NULL DEFAULT ''", "end_day"),
        ("ADD COLUMN end_time TEXT NOT NULL DEFAULT '00:00:00'", "end_time"),
        ("ADD COLUMN station TEXT NOT NULL DEFAULT ''", "station"),
        ("ADD COLUMN sparepart TEXT NOT NULL DEFAULT ''", "sparepart"),
        ("ADD COLUMN type_alarm TEXT NOT NULL DEFAULT ''", "type_alarm"),
        ("ADD COLUMN amount BIGINT NOT NULL DEFAULT 0", "amount"),
        ("ADD COLUMN min_prob DOUBLE PRECISION NOT NULL DEFAULT 0", "min_prob"),
        ("ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now()", "created_at"),
    ]:
        if not has_column(conn, ALARM_SCHEMA, ALARM_TABLE, col_name):
            alter_cols.append(col_sql)

    if alter_cols:
        with conn.cursor() as cur:
            cur.execute(f"ALTER TABLE {ALARM_SCHEMA}.{ALARM_TABLE} " + ", ".join(alter_cols) + ";")

    log("[OK] ensure_tables done")


# =================================================
# 4) MODEL (max id)
# =================================================
def detect_model_blob_column(conn) -> Optional[str]:
    sql = """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema=%s AND table_name=%s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (MODEL_SCHEMA, MODEL_TABLE))
        cols = [r[0] for r in cur.fetchall()]

    bytea_cols: List[str] = []
    for c in cols:
        udt = get_column_udt(conn, MODEL_SCHEMA, MODEL_TABLE, c)
        if udt == "bytea":
            bytea_cols.append(c)

    if not bytea_cols:
        return None

    prefer = ["model_pickle", "pickle", "model", "blob", "model_blob", "pkl", "bin"]
    for p in prefer:
        for bc in bytea_cols:
            if p in bc.lower():
                return bc
    return bytea_cols[0]


def load_model_max_id(conn) -> Tuple[Any, int]:
    blob_col = detect_model_blob_column(conn)
    if blob_col is None:
        raise RuntimeError(f'No BYTEA column found in {MODEL_SCHEMA}."{MODEL_TABLE}"')

    sql = f"""
    SELECT id, "{blob_col}" AS blob
    FROM {MODEL_SCHEMA}."{MODEL_TABLE}"
    ORDER BY id DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    if not row:
        raise RuntimeError(f'No rows in {MODEL_SCHEMA}."{MODEL_TABLE}"')

    mid, blob = row
    model = pickle.loads(blob)
    return model, int(mid)


def get_model_feature_names(model: Any) -> Optional[List[str]]:
    if hasattr(model, "feature_names_in_"):
        try:
            return list(model.feature_names_in_)
        except Exception:
            pass
    if hasattr(model, "feature_name"):
        try:
            fn = model.feature_name()
            if isinstance(fn, list) and fn:
                return fn
        except Exception:
            pass
    return None


def _to_scalar_float(y: Any) -> float:
    try:
        if hasattr(y, "item"):
            try:
                return float(y.item())
            except Exception:
                pass
        if isinstance(y, (list, tuple)):
            return _to_scalar_float(y[0]) if y else 0.0
        return float(y)
    except Exception:
        return 0.0


def predict_proba_1(model: Any, X: List[List[float]]) -> float:
    if hasattr(model, "predict_proba"):
        proba = model.predict_proba(X)
        try:
            return float(proba[0][1])
        except Exception:
            return _to_scalar_float(proba)
    if hasattr(model, "predict"):
        y = model.predict(X)
        return _to_scalar_float(y)
    raise RuntimeError("Unsupported model object: no predict_proba / predict")


# =================================================
# 5) LIFE(p25)
# =================================================
def load_life_p25_map(conn) -> Dict[str, float]:
    sql = f"""
    SELECT sparepart::text AS sparepart, p25::float8 AS p25
    FROM {LIFE_SCHEMA}.{LIFE_TABLE}
    WHERE sparepart::text = ANY(%s)
    """
    mp: Dict[str, float] = {}
    with conn.cursor() as cur:
        cur.execute(sql, (SPAREPARTS,))
        for sp, p25 in cur.fetchall():
            if sp:
                try:
                    mp[str(sp)] = float(p25)
                except Exception:
                    pass
    for sp in SPAREPARTS:
        mp.setdefault(sp, 0.0)
    return mp


# =================================================
# 6) POLICY
# =================================================
def policy_by_ratio(ratio: float) -> Tuple[Optional[str], float]:
    if ratio < 0.3:
        return None, 0.95
    if ratio < 0.5:
        return None, 0.90
    if ratio < 0.8:
        return "준비", 0.60
    if ratio < 0.9:
        return "권고", 0.30
    if ratio < 1.0:
        return "긴급", 0.0
    return "교체", 0.0


def alarm_rank(t: Optional[str]) -> int:
    order = {"준비": 1, "권고": 2, "긴급": 3, "교체": 4}
    return order.get(t or "", 0)


# =================================================
# 7) ALARM INSERT
# =================================================
def convert_end_day_for_alarm(conn, yyyy_mm_dd: str) -> Any:
    udt = (get_column_udt(conn, ALARM_SCHEMA, ALARM_TABLE, "end_day") or "text").lower()
    if udt == "date":
        y = int(yyyy_mm_dd[:4]); m = int(yyyy_mm_dd[5:7]); d = int(yyyy_mm_dd[8:10])
        return date(y, m, d)
    if udt in ("int2", "int4", "int8", "numeric"):
        return int(yyyy_mm_dd.replace("-", ""))
    if "timestamp" in udt:
        return datetime.strptime(yyyy_mm_dd, "%Y-%m-%d")
    return yyyy_mm_dd


def convert_end_time_for_alarm(conn, hhmmss: str, end_day_yyyy_mm_dd: str) -> Any:
    udt = (get_column_udt(conn, ALARM_SCHEMA, ALARM_TABLE, "end_time") or "text").lower()
    if "time" in udt:
        return hhmmss
    if udt in ("int2", "int4", "int8", "numeric"):
        return int(hhmmss.replace(":", ""))
    if "timestamp" in udt:
        return datetime.strptime(end_day_yyyy_mm_dd + " " + hhmmss, "%Y-%m-%d %H:%M:%S")
    return hhmmss


def insert_alarm(
    conn,
    station: str,
    sparepart: str,
    alarm_type: str,
    amount: int,
    min_prob: float,
    event_end_ts: datetime,
    inspect_ts: datetime,
) -> None:
    evt = round_dt_to_sec(event_end_ts)
    end_day_str = evt.strftime("%Y-%m-%d")
    end_time_str = evt.strftime("%H:%M:%S")

    end_day_val = convert_end_day_for_alarm(conn, end_day_str)
    end_time_val = convert_end_time_for_alarm(conn, end_time_str, end_day_str)

    sql = f"""
    INSERT INTO {ALARM_SCHEMA}.{ALARM_TABLE}
      (end_day, end_time, station, sparepart, type_alarm, amount, min_prob, created_at)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                end_day_val,
                end_time_val,
                station,
                sparepart,
                alarm_type,
                int(amount),
                float(min_prob),
                inspect_ts,
            ),
        )


# =================================================
# 8) FEATURES
# =================================================
def build_features_for_model(
    model: Any,
    station: str,
    sparepart: str,
    amount: int,
    max_tests: float,
    ratio: float,
) -> List[List[float]]:
    fn = get_model_feature_names(model)

    feat: Dict[str, float] = {
        "amount": float(amount),
        "max_tests": float(max_tests),
        "ratio": float(ratio),
    }
    for st in STATIONS:
        feat[f"station_{st}"] = 1.0 if station == st else 0.0
    for sp in SPAREPARTS:
        feat[f"sparepart_{sp}"] = 1.0 if sparepart == sp else 0.0

    if fn:
        return [[float(feat.get(name, 0.0)) for name in fn]]
    return [[feat["amount"], feat["max_tests"], feat["ratio"]]]


# =================================================
# 9) STATE
# =================================================
@dataclass
class State:
    date_from: str
    date_to: str
    station: str
    sparepart: str
    current_repl_ts: Optional[datetime]
    next_repl_ts: Optional[datetime]
    last_test_ts: Optional[datetime]
    amount: int
    last_alarm_type: Optional[str]


def load_state(conn, date_from: str, date_to: str, station: str, sparepart: str) -> State:
    sql = f"""
    SELECT date_from, date_to, station, sparepart, current_repl_ts, next_repl_ts, last_test_ts, amount, last_alarm_type
    FROM {STATE_SCHEMA}.{STATE_TABLE}
    WHERE date_from=%s::date AND date_to=%s::date AND station=%s AND sparepart=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (date_from, date_to, station, sparepart))
        row = cur.fetchone()

    if row:
        return State(
            date_from=str(row[0]),
            date_to=str(row[1]),
            station=row[2],
            sparepart=row[3],
            current_repl_ts=row[4],
            next_repl_ts=row[5],
            last_test_ts=row[6],
            amount=int(row[7] or 0),
            last_alarm_type=row[8],
        )

    ins = f"""
    INSERT INTO {STATE_SCHEMA}.{STATE_TABLE}
    (date_from, date_to, station, sparepart, current_repl_ts, next_repl_ts, last_test_ts, amount, last_alarm_type)
    VALUES (%s::date,%s::date,%s,%s,NULL,NULL,NULL,0,NULL)
    ON CONFLICT (date_from, date_to, station, sparepart) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(ins, (date_from, date_to, station, sparepart))

    return State(date_from, date_to, station, sparepart, None, None, None, 0, None)


def save_state(conn, st: State) -> None:
    sql = f"""
    INSERT INTO {STATE_SCHEMA}.{STATE_TABLE}
    (date_from, date_to, station, sparepart, current_repl_ts, next_repl_ts, last_test_ts, amount, last_alarm_type, updated_at)
    VALUES (%s::date,%s::date,%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (date_from, date_to, station, sparepart) DO UPDATE
      SET current_repl_ts=EXCLUDED.current_repl_ts,
          next_repl_ts=EXCLUDED.next_repl_ts,
          last_test_ts=EXCLUDED.last_test_ts,
          amount=EXCLUDED.amount,
          last_alarm_type=EXCLUDED.last_alarm_type,
          updated_at=now()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                st.date_from, st.date_to, st.station, st.sparepart,
                st.current_repl_ts, st.next_repl_ts, st.last_test_ts,
                int(st.amount), st.last_alarm_type,
            ),
        )


# =================================================
# 10) QUERIES
# =================================================
def preflight_required_columns(conn) -> None:
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "end_day")
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "to_time")
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "station")
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "sparepart")

    require_column(conn, TEST_SCHEMA, TEST_TABLE, "end_day")
    require_column(conn, TEST_SCHEMA, TEST_TABLE, "end_time")
    require_column(conn, TEST_SCHEMA, TEST_TABLE, "station")

    require_column(conn, LIFE_SCHEMA, LIFE_TABLE, "sparepart")
    require_column(conn, LIFE_SCHEMA, LIFE_TABLE, "p25")


def find_latest_repl_in_window(
    conn,
    station: str,
    sparepart: str,
    date_from: str,
    date_to: str,
    window_end_ts: datetime,
) -> Optional[datetime]:
    repl_ts = ts_expr("end_day", "to_time")
    sql = f"""
    SELECT {repl_ts} AS t
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND sparepart::text=%s
      AND {repl_ts} IS NOT NULL
      AND {end_day_as_date_expr("end_day")} BETWEEN %s::date AND %s::date
      AND {repl_ts} <= %s
    ORDER BY {repl_ts} DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, sparepart, date_from, date_to, window_end_ts))
        row = cur.fetchone()
    return row[0] if row else None


def find_prev_repl_before_window(conn, station: str, sparepart: str, date_from: str) -> Optional[datetime]:
    repl_ts = ts_expr("end_day", "to_time")
    sql = f"""
    SELECT {repl_ts} AS t
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND sparepart::text=%s
      AND {repl_ts} IS NOT NULL
      AND {repl_ts} <= (%s::date)::timestamp
    ORDER BY {repl_ts} DESC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, sparepart, date_from))
        row = cur.fetchone()
    return row[0] if row else None


def find_next_repl_after_current(
    conn,
    station: str,
    sparepart: str,
    current_ts: datetime,
    date_from: str,
    date_to: str,
) -> Optional[datetime]:
    repl_ts = ts_expr("end_day", "to_time")
    sql = f"""
    SELECT {repl_ts} AS t
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND sparepart::text=%s
      AND {repl_ts} IS NOT NULL
      AND {end_day_as_date_expr("end_day")} BETWEEN %s::date AND %s::date
      AND {repl_ts} > %s
    ORDER BY {repl_ts} ASC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, sparepart, date_from, date_to, current_ts))
        row = cur.fetchone()
    return row[0] if row else None


def fetch_new_tests_ts_list(
    conn,
    station: str,
    after_ts: datetime,
    upper_ts: datetime,
    date_from: str,
    date_to: str,
    limit: int,
) -> List[datetime]:
    """
    (after_ts, upper_ts] 범위의 테스트 end_ts를 오름차순으로 가져옴.
    임계값 crossing을 잡기 위해 리스트를 반환.
    """
    end_ts = ts_expr("end_day", "end_time")
    sql = f"""
    SELECT {end_ts} AS tts
    FROM {TEST_SCHEMA}.{TEST_TABLE}
    WHERE station=%s
      AND {end_ts} IS NOT NULL
      AND {end_day_as_date_expr("end_day")} BETWEEN %s::date AND %s::date
      AND {end_ts} > %s
      AND {end_ts} <= %s
    ORDER BY {end_ts} ASC
    LIMIT {int(limit)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, date_from, date_to, after_ts, upper_ts))
        rows = cur.fetchall()
    return [r[0] for r in rows] if rows else []


def get_max_end_ts(conn, station: str, date_from: str, date_to: str) -> Optional[datetime]:
    end_ts = ts_expr("end_day", "end_time")
    sql = f"""
    SELECT MAX({end_ts}) AS mx
    FROM {TEST_SCHEMA}.{TEST_TABLE}
    WHERE station=%s
      AND {end_ts} IS NOT NULL
      AND {end_day_as_date_expr("end_day")} BETWEEN %s::date AND %s::date
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, date_from, date_to))
        row = cur.fetchone()
    return row[0] if row else None


# =================================================
# 11) MAIN
# =================================================
def main() -> None:
    date_from, date_to = prompt_date_range_console()
    log(f"[CFG] date_range = {date_from} ~ {date_to}")

    conn: Optional[psycopg2.extensions.connection] = None
    cached_model_id: Optional[int] = None
    cached_model: Any = None

    life_map: Dict[str, float] = {}
    last_life_reload = 0.0
    last_hb = 0.0

    while True:
        try:
            if conn is None or conn.closed != 0:
                conn = connect_forever()
                ensure_tables(conn)
                preflight_required_columns(conn)

            now_ts = time.time()
            if now_ts - last_hb >= 60:
                log(f"[HEARTBEAT] now={datetime.now().strftime('%Y-%m-%d %H:%M:%S')} range={date_from}~{date_to}")
                last_hb = now_ts

            win_end = window_end_timestamp(date_to)

            if not life_map or (now_ts - last_life_reload >= 300):
                life_map = load_life_p25_map(conn)
                last_life_reload = now_ts
                log(f"[OK] life(p25) loaded: {life_map}")

            model, mid = load_model_max_id(conn)
            if cached_model_id != mid:
                cached_model_id = mid
                cached_model = model
                log(f"[OK] model loaded (id={mid})")

            station_max_map: Dict[str, Optional[datetime]] = {}
            for station in STATIONS:
                station_max_map[station] = get_max_end_ts(conn, station, date_from, date_to)

            for station in STATIONS:
                for sparepart in SPAREPARTS:
                    st = load_state(conn, date_from, date_to, station, sparepart)

                    # -------------------------
                    # current_repl_ts 초기화
                    # -------------------------
                    if st.current_repl_ts is None:
                        current = find_latest_repl_in_window(conn, station, sparepart, date_from, date_to, win_end)
                        if current is None and USE_FALLBACK_PREV_REPL:
                            current = find_prev_repl_before_window(conn, station, sparepart, date_from)
                        if current is None:
                            continue

                        st.current_repl_ts = current
                        st.last_test_ts = current
                        st.amount = 0
                        st.last_alarm_type = None
                        st.next_repl_ts = find_next_repl_after_current(conn, station, sparepart, current, date_from, date_to)
                        save_state(conn, st)
                        log(f"[INIT] {station}/{sparepart} current={st.current_repl_ts} next={st.next_repl_ts}")

                    # next_repl_ts 재탐지
                    if st.next_repl_ts is None:
                        st.next_repl_ts = find_next_repl_after_current(conn, station, sparepart, st.current_repl_ts, date_from, date_to)
                        save_state(conn, st)

                    # ============================================================
                    # "교체" 이후 next가 없으면 집계 중단(단, next 재탐지는 계속)
                    # ============================================================
                    if st.last_alarm_type == "교체" and st.next_repl_ts is None:
                        nxt = find_next_repl_after_current(conn, station, sparepart, st.current_repl_ts, date_from, date_to)
                        if nxt is not None:
                            st.next_repl_ts = nxt
                            save_state(conn, st)
                        else:
                            continue

                    upper_ts = st.next_repl_ts if st.next_repl_ts is not None else win_end
                    after_ts = st.last_test_ts or st.current_repl_ts

                    ts_list: List[datetime] = []
                    if after_ts < upper_ts:
                        ts_list = fetch_new_tests_ts_list(
                            conn=conn,
                            station=station,
                            after_ts=after_ts,
                            upper_ts=upper_ts,
                            date_from=date_from,
                            date_to=date_to,
                            limit=BATCH_LIMIT_TEST_ROWS,
                        )

                    added = len(ts_list)
                    max_seen: Optional[datetime] = ts_list[-1] if ts_list else None

                    # -------------------------
                    # DBG (added=0 & amount=0이면 미출력)
                    # -------------------------
                    if not (added == 0 and st.amount == 0):
                        log(
                            f"[DBG] {station}/{sparepart} current={st.current_repl_ts} next={st.next_repl_ts} "
                            f"upper={upper_ts} last_test_ts={st.last_test_ts} "
                            f"max_end_ts={station_max_map.get(station)} added={added} amount={st.amount}"
                        )

                    # -------------------------
                    # 임계 crossing 처리: 1건씩 누적하며 알람 저장
                    # -------------------------
                    max_tests = float(life_map.get(sparepart, 0.0) or 0.0)

                    stop_after_replace_no_next = False

                    for tts in ts_list:
                        st.amount += 1
                        st.last_test_ts = tts

                        ratio = (float(st.amount) / max_tests) if max_tests > 0 else 0.0
                        alarm_type, min_prob = policy_by_ratio(ratio)

                        if alarm_type in ("준비", "권고", "긴급", "교체"):
                            prev_rank = alarm_rank(st.last_alarm_type)
                            curr_rank = alarm_rank(alarm_type)

                            if curr_rank > prev_rank:
                                X = build_features_for_model(cached_model, station, sparepart, st.amount, max_tests, ratio)
                                prob = predict_proba_1(cached_model, X)
                                pass_prob = (prob >= min_prob) if alarm_type in ("준비", "권고") else True

                                log(
                                    f"[EVAL] {station}/{sparepart} amount={st.amount} max={max_tests:.0f} "
                                    f"ratio={ratio:.3f} -> {alarm_type} min_prob={min_prob} prob={prob:.4f} pass={pass_prob}"
                                )

                                # 저장은 항상 수행(요청: 교체 외 알람도 저장)
                                inspect_ts = datetime.now()
                                insert_alarm(
                                    conn=conn,
                                    station=station,
                                    sparepart=sparepart,
                                    alarm_type=alarm_type,
                                    amount=st.amount,
                                    min_prob=min_prob,
                                    event_end_ts=tts,      # 알람을 유발한 TEST end_ts
                                    inspect_ts=inspect_ts, # inspect 시간
                                )
                                st.last_alarm_type = alarm_type
                                log(f"[ALARM-SAVED] {station}/{sparepart} {alarm_type} (event_ts={tts})")

                                # "교체" 이후 next가 없으면 더 이상 집계하지 않음
                                if alarm_type == "교체" and st.next_repl_ts is None:
                                    stop_after_replace_no_next = True
                                    break

                    # 상태 저장(배치 처리 후)
                    if added > 0:
                        save_state(conn, st)

                    if stop_after_replace_no_next:
                        continue

                    # -------------------------
                    # 예1 롤링
                    # -------------------------
                    if st.next_repl_ts is not None:
                        if win_end >= st.next_repl_ts:
                            if (st.last_test_ts or st.current_repl_ts) >= st.next_repl_ts:
                                st.current_repl_ts = st.next_repl_ts
                                st.last_test_ts = st.current_repl_ts
                                st.amount = 0
                                st.last_alarm_type = None
                                st.next_repl_ts = find_next_repl_after_current(
                                    conn, station, sparepart, st.current_repl_ts, date_from, date_to
                                )
                                save_state(conn, st)
                                log(
                                    f"[ROLL] {station}/{sparepart} new_current={st.current_repl_ts} "
                                    f"new_next={st.next_repl_ts}"
                                )

            time.sleep(LOOP_INTERVAL_SEC)

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log(f"[ERROR] DB disconnected: {type(e).__name__}: {e}")
            safe_close(conn)
            conn = None
            time.sleep(2)

        except Exception as e:
            log(f"[ERROR] runtime: {type(e).__name__}: {e}")
            log(traceback.format_exc())
            time.sleep(2)


if __name__ == "__main__":
    main()
