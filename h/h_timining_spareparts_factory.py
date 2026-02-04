# -*- coding: utf-8 -*-
"""
h_timing_spareparts_v9_2_backfill_then_realtime.py
============================================================
목표
- fct_non_operation_time(sparepart 교체 이력) 기반으로 구간을 나눠서
  기존 데이터(과거)부터 처리(backfill)한 뒤, 실시간으로 계속 추적한다.

핵심 규칙
1) 교체 기준(reset 기준): fct_non_operation_time.to_time (교체 완료 시각)
2) 구간: (current_repl_end_ts, next_repl_end_ts]  (다음 교체 완료시각이 리셋 포인트)
   - next가 없으면 upper_ts = now
3) 각 구간 내에서 testlog end_ts를 오름차순으로 1건씩 누적하며
   ratio 임계 crossing 순간에 알람 저장(준비/권고/긴급/교체 모두 가능)
4) "교체" 알람 이후 next 교체가 없으면 더 이상 집계하지 않음(단, next 탐지는 계속)
5) 테이블은 DROP 금지. 1회성 CREATE/ALTER/INDEX만 수행.
6) backlog 처리 시에는 sleep 최소화(adaptive loop)

주의
- backfill을 하면 alarm_record.end_day/end_time은 과거(TEST end_ts)가 들어가고,
  created_at은 현재(inspect 시각)가 들어간다.

[중요 수정(v9.2 FIX)]
- 교체(next_repl_end_ts)가 있는데 그 사이에 테스트가 없으면(ts_list empty)
  기존 로직은 ROLL이 영원히 안 되어 다음 사이클(교체 이후)을 집계하지 못함.
- 해결:
  1) now >= next_repl_end_ts 이면 upper_ts=next_repl_end_ts로 고정
  2) ts_list가 비어도 now >= next_repl_end_ts이면 ROLL 수행
"""

from __future__ import annotations

import time
import pickle
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
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
BATCH_LIMIT_TEST_ROWS = 5000

SESSION_GUARDS_SQL = """
SET application_name = 'h_timing_spareparts_v9_2';
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
STATE_TABLE = "sparepart_interval_state_rt_v9_2"


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


def has_schema(conn, schema: str) -> bool:
    sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name=%s"
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        return cur.fetchone() is not None


def ensure_schema(conn, schema: str) -> None:
    if not has_schema(conn, schema):
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


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
# 3) ONE-TIME DDL (NO DROP)
# =================================================
def ensure_tables(conn) -> None:
    ensure_schema(conn, ALARM_SCHEMA)
    ensure_schema(conn, STATE_SCHEMA)

    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {STATE_SCHEMA}.{STATE_TABLE} (
            station   TEXT NOT NULL,
            sparepart TEXT NOT NULL,

            current_repl_end_ts TIMESTAMP,  -- 현재 교체 완료(to_time)
            next_repl_end_ts    TIMESTAMP,  -- 다음 교체 완료(to_time)

            last_test_ts    TIMESTAMP,
            amount          BIGINT NOT NULL DEFAULT 0,
            last_alarm_type TEXT,

            updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (station, sparepart)
        );
        """)

    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {ALARM_SCHEMA}.{ALARM_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            end_day TEXT NOT NULL,
            end_time TEXT NOT NULL,
            station TEXT NOT NULL,
            sparepart TEXT NOT NULL,
            type_alarm TEXT NOT NULL,
            amount BIGINT NOT NULL DEFAULT 0,
            min_prob DOUBLE PRECISION NOT NULL DEFAULT 0,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

            run_id TEXT NOT NULL DEFAULT '',
            algo_ver TEXT NOT NULL DEFAULT '',
            reset_reason TEXT NOT NULL DEFAULT '',
            reset_repl_ts TIMESTAMP NULL
        );
        """)

    alarm_cols = [
        ("run_id", "TEXT NOT NULL DEFAULT ''"),
        ("algo_ver", "TEXT NOT NULL DEFAULT ''"),
        ("reset_reason", "TEXT NOT NULL DEFAULT ''"),
        ("reset_repl_ts", "TIMESTAMP NULL"),
    ]
    for col, ddl in alarm_cols:
        if not has_column(conn, ALARM_SCHEMA, ALARM_TABLE, col):
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {ALARM_SCHEMA}.{ALARM_TABLE} ADD COLUMN {col} {ddl};")

    with conn.cursor() as cur:
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_alarm_station_spare_created ON {ALARM_SCHEMA}.{ALARM_TABLE} (station, sparepart, created_at DESC);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_alarm_runid ON {ALARM_SCHEMA}.{ALARM_TABLE} (run_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_state_updated ON {STATE_SCHEMA}.{STATE_TABLE} (updated_at DESC);")

    log("[OK] ensure_tables done (NO DROP)")


# =================================================
# 4) MODEL
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
# 7) INSERT ALARM (meta 포함)
# =================================================
def insert_alarm(
    conn,
    station: str,
    sparepart: str,
    alarm_type: str,
    amount: int,
    min_prob: float,
    event_end_ts: datetime,
    inspect_ts: datetime,
    run_id: str,
    algo_ver: str,
    reset_reason: str,
    reset_repl_ts: Optional[datetime],
) -> None:
    evt = round_dt_to_sec(event_end_ts)
    end_day_str = evt.strftime("%Y-%m-%d")
    end_time_str = evt.strftime("%H:%M:%S")

    sql = f"""
    INSERT INTO {ALARM_SCHEMA}.{ALARM_TABLE}
      (end_day, end_time, station, sparepart, type_alarm, amount, min_prob, created_at,
       run_id, algo_ver, reset_reason, reset_repl_ts)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s)
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                end_day_str,
                end_time_str,
                station,
                sparepart,
                alarm_type,
                int(amount),
                float(min_prob),
                inspect_ts,
                run_id,
                algo_ver,
                reset_reason,
                reset_repl_ts,
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
    station: str
    sparepart: str
    current_repl_end_ts: Optional[datetime]
    next_repl_end_ts: Optional[datetime]
    last_test_ts: Optional[datetime]
    amount: int
    last_alarm_type: Optional[str]


def load_state(conn, station: str, sparepart: str) -> State:
    sql = f"""
    SELECT station, sparepart, current_repl_end_ts, next_repl_end_ts, last_test_ts, amount, last_alarm_type
    FROM {STATE_SCHEMA}.{STATE_TABLE}
    WHERE station=%s AND sparepart=%s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, sparepart))
        row = cur.fetchone()

    if row:
        return State(
            station=row[0],
            sparepart=row[1],
            current_repl_end_ts=row[2],
            next_repl_end_ts=row[3],
            last_test_ts=row[4],
            amount=int(row[5] or 0),
            last_alarm_type=row[6],
        )

    ins = f"""
    INSERT INTO {STATE_SCHEMA}.{STATE_TABLE}
      (station, sparepart, current_repl_end_ts, next_repl_end_ts, last_test_ts, amount, last_alarm_type)
    VALUES (%s,%s,NULL,NULL,NULL,0,NULL)
    ON CONFLICT (station, sparepart) DO NOTHING
    """
    with conn.cursor() as cur:
        cur.execute(ins, (station, sparepart))

    return State(station, sparepart, None, None, None, 0, None)


def save_state(conn, st: State) -> None:
    sql = f"""
    INSERT INTO {STATE_SCHEMA}.{STATE_TABLE}
      (station, sparepart, current_repl_end_ts, next_repl_end_ts, last_test_ts, amount, last_alarm_type, updated_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,now())
    ON CONFLICT (station, sparepart) DO UPDATE
      SET current_repl_end_ts=EXCLUDED.current_repl_end_ts,
          next_repl_end_ts=EXCLUDED.next_repl_end_ts,
          last_test_ts=EXCLUDED.last_test_ts,
          amount=EXCLUDED.amount,
          last_alarm_type=EXCLUDED.last_alarm_type,
          updated_at=now()
    """
    with conn.cursor() as cur:
        cur.execute(
            sql,
            (
                st.station, st.sparepart,
                st.current_repl_end_ts, st.next_repl_end_ts,
                st.last_test_ts, int(st.amount), st.last_alarm_type,
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


def list_repl_end_ts(conn, station: str, sparepart: str) -> List[datetime]:
    repl_end_ts = ts_expr("end_day", "to_time")
    sql = f"""
    SELECT {repl_end_ts} AS e
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND sparepart::text=%s
      AND {repl_end_ts} IS NOT NULL
    ORDER BY {repl_end_ts} ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, sparepart))
        rows = cur.fetchall()
    return [r[0] for r in rows] if rows else []


def find_next_repl_end_after(conn, station: str, sparepart: str, current_end: datetime) -> Optional[datetime]:
    repl_end_ts = ts_expr("end_day", "to_time")
    sql = f"""
    SELECT {repl_end_ts} AS e
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND sparepart::text=%s
      AND {repl_end_ts} IS NOT NULL
      AND {repl_end_ts} > %s
    ORDER BY {repl_end_ts} ASC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, sparepart, current_end))
        row = cur.fetchone()
    return row[0] if row else None


def fetch_new_tests_ts_list(conn, station: str, after_ts: datetime, upper_ts: datetime, limit: int) -> List[datetime]:
    end_ts = ts_expr("end_day", "end_time")
    sql = f"""
    SELECT {end_ts} AS tts
    FROM {TEST_SCHEMA}.{TEST_TABLE}
    WHERE station=%s
      AND {end_ts} IS NOT NULL
      AND {end_ts} > %s
      AND {end_ts} <= %s
    ORDER BY {end_ts} ASC
    LIMIT {int(limit)}
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, after_ts, upper_ts))
        rows = cur.fetchall()
    return [r[0] for r in rows] if rows else []


def roll_state_to_next(conn, st: State, station: str, sparepart: str) -> None:
    """next_repl_end_ts를 current로 승격하고 사이클 리셋"""
    prev_current = st.current_repl_end_ts
    st.current_repl_end_ts = st.next_repl_end_ts
    st.last_test_ts = st.current_repl_end_ts
    st.amount = 0
    st.last_alarm_type = None
    st.next_repl_end_ts = find_next_repl_end_after(conn, station, sparepart, st.current_repl_end_ts)
    save_state(conn, st)
    log(f"[ROLL] {station}/{sparepart} {prev_current} -> {st.current_repl_end_ts} next={st.next_repl_end_ts}")


# =================================================
# 11) MAIN
# =================================================
def main() -> None:
    RUN_ID = datetime.now().strftime("%Y%m%d_%H%M%S") + "_v9_2_fix"
    ALGO_VER = "v9_2_fix"

    conn: Optional[psycopg2.extensions.connection] = None
    cached_model_id: Optional[int] = None
    cached_model: Any = None

    life_map: Dict[str, float] = {}
    last_life_reload = 0.0
    last_hb = 0.0

    log(f"[START] run_id={RUN_ID} algo_ver={ALGO_VER}")

    while True:
        try:
            if conn is None or conn.closed != 0:
                conn = connect_forever()
                ensure_tables(conn)  # NO DROP
                preflight_required_columns(conn)

            now_ts = time.time()
            now_dt = datetime.now()

            if now_ts - last_hb >= 60:
                log(f"[HEARTBEAT] now={now_dt.strftime('%Y-%m-%d %H:%M:%S')} run_id={RUN_ID}")
                last_hb = now_ts

            if not life_map or (now_ts - last_life_reload >= 300):
                life_map = load_life_p25_map(conn)
                last_life_reload = now_ts
                log(f"[OK] life(p25) loaded: {life_map}")

            model, mid = load_model_max_id(conn)
            if cached_model_id != mid:
                cached_model_id = mid
                cached_model = model
                log(f"[OK] model loaded (id={mid})")

            need_immediate = False  # backlog 있으면 sleep 0

            for station in STATIONS:
                for sparepart in SPAREPARTS:
                    max_tests = float(life_map.get(sparepart, 0.0) or 0.0)
                    if max_tests <= 0:
                        continue

                    st = load_state(conn, station, sparepart)

                    # ============================================================
                    # (1) 초기화: 가장 이른 교체부터
                    # ============================================================
                    if st.current_repl_end_ts is None:
                        repl_list = list_repl_end_ts(conn, station, sparepart)
                        if not repl_list:
                            continue
                        st.current_repl_end_ts = repl_list[0]
                        st.next_repl_end_ts = repl_list[1] if len(repl_list) >= 2 else None
                        st.last_test_ts = st.current_repl_end_ts
                        st.amount = 0
                        st.last_alarm_type = None
                        save_state(conn, st)
                        need_immediate = True
                        log(f"[INIT] {station}/{sparepart} current_end={st.current_repl_end_ts} next_end={st.next_repl_end_ts}")

                    # station/sparepart 당 한 루프에서 연속 롤링/처리를 허용(무한 방지)
                    for _guard in range(50):
                        # ========================================================
                        # (2) next 갱신(새 교체행 생길 수 있음)
                        # ========================================================
                        if st.current_repl_end_ts is not None:
                            nxt = find_next_repl_end_after(conn, station, sparepart, st.current_repl_end_ts)
                            if nxt is not None and (st.next_repl_end_ts is None or nxt != st.next_repl_end_ts):
                                st.next_repl_end_ts = nxt
                                save_state(conn, st)
                                need_immediate = True
                                log(f"[NEXT-UPDATE] {station}/{sparepart} next_end={st.next_repl_end_ts}")

                        # ========================================================
                        # (3) 교체까지 갔고 next가 없으면 집계 중단(단, next 탐지는 계속)
                        # ========================================================
                        if st.last_alarm_type == "교체" and st.next_repl_end_ts is None:
                            break

                        if st.current_repl_end_ts is None:
                            break

                        # ========================================================
                        # (4) upper_ts 결정 (FIX)
                        # - next가 존재하고 now가 next를 지났으면 upper를 next로 고정해 사이클 닫기
                        # ========================================================
                        if st.next_repl_end_ts is not None and now_dt >= st.next_repl_end_ts:
                            upper_ts = st.next_repl_end_ts
                        else:
                            upper_ts = now_dt

                        after_ts = st.last_test_ts or st.current_repl_end_ts
                        if after_ts is None:
                            break

                        # 이미 upper를 넘어섰으면(또는 같으면) 롤링 여부 확인
                        if after_ts >= upper_ts:
                            if st.next_repl_end_ts is not None and now_dt >= st.next_repl_end_ts:
                                # next 교체시각이 이미 지났으면 무조건 롤링
                                roll_state_to_next(conn, st, station, sparepart)
                                need_immediate = True
                                continue
                            break

                        # ========================================================
                        # (5) 테스트 ts fetch
                        # ========================================================
                        ts_list = fetch_new_tests_ts_list(conn, station, after_ts, upper_ts, BATCH_LIMIT_TEST_ROWS)

                        # ========================================================
                        # (FIX 핵심) ts_list가 비어도 교체시각이 지났으면 롤링
                        # ========================================================
                        if not ts_list:
                            if st.next_repl_end_ts is not None and now_dt >= st.next_repl_end_ts and upper_ts == st.next_repl_end_ts:
                                log(f"[ROLL-NO-TEST] {station}/{sparepart} gap_no_tests (after={after_ts} <= next={st.next_repl_end_ts})")
                                roll_state_to_next(conn, st, station, sparepart)
                                need_immediate = True
                                continue
                            break

                        need_immediate = True

                        # ========================================================
                        # (6) 1건씩 누적하며 crossing 알람 저장
                        # ========================================================
                        for tts in ts_list:
                            st.amount += 1
                            st.last_test_ts = tts

                            ratio = float(st.amount) / max_tests if max_tests > 0 else 0.0
                            alarm_type, min_prob = policy_by_ratio(ratio)

                            if alarm_type in ("준비", "권고", "긴급", "교체"):
                                if alarm_rank(alarm_type) > alarm_rank(st.last_alarm_type):
                                    prob = 0.0
                                    pass_prob = True
                                    try:
                                        X = build_features_for_model(cached_model, station, sparepart, st.amount, max_tests, ratio)
                                        prob = predict_proba_1(cached_model, X)
                                        pass_prob = (prob >= min_prob) if alarm_type in ("준비", "권고") else True
                                    except Exception:
                                        pass

                                    log(f"[EVAL] {station}/{sparepart} amount={st.amount} ratio={ratio:.3f} -> {alarm_type} prob={prob:.4f} pass={pass_prob}")

                                    insert_alarm(
                                        conn=conn,
                                        station=station,
                                        sparepart=sparepart,
                                        alarm_type=alarm_type,
                                        amount=st.amount,
                                        min_prob=float(min_prob),
                                        event_end_ts=tts,  # ★ 알람 시각은 TEST end_ts
                                        inspect_ts=datetime.now(),
                                        run_id=RUN_ID,
                                        algo_ver=ALGO_VER,
                                        reset_reason="NORMAL_CROSS",
                                        reset_repl_ts=st.current_repl_end_ts,
                                    )
                                    st.last_alarm_type = alarm_type
                                    log(f"[ALARM-SAVED] {station}/{sparepart} {alarm_type} (event_ts={tts})")

                                    if alarm_type == "교체" and st.next_repl_end_ts is None:
                                        break

                        save_state(conn, st)

                        # 이번 사이클에서 upper가 next_repl_end_ts였고, now가 그 이상이면
                        # 다음 루프에서 자연스럽게 ROLL 처리로 넘어가도록 continue
                        continue

            # backlog 있으면 즉시 재루프, 없으면 5초
            if need_immediate:
                time.sleep(0)
            else:
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
