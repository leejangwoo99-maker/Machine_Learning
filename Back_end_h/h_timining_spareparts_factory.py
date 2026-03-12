# -*- coding: utf-8 -*-
"""
h_timing_spareparts_v9_3_backfill_then_realtime_logdb.py
============================================================
목표
- (REPL) Back_end_i_daily_report.total_non_operation_time(to_ts) 기반으로 구간을 나눠서
  기존 데이터(과거)부터 처리(backfill)한 뒤, 실시간으로 계속 추적한다.

핵심 규칙
1) 교체 기준(reset 기준): total_non_operation_time.to_ts (교체 완료 시각)
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

[v9.2 FIX]
- 교체(next_repl_end_ts)가 있는데 그 사이에 테스트가 없으면(ts_list empty)
  기존 로직은 ROLL이 영원히 안 되어 다음 사이클(교체 이후)을 집계하지 못함.
- 해결:
  1) now >= next_repl_end_ts 이면 upper_ts=next_repl_end_ts로 고정
  2) ts_list가 비어도 now >= next_repl_end_ts이면 ROLL 수행

[v9.2 추가 FIX]
- "확률이 기준 이상일 때만 알람 저장"을 위해
  준비/권고 알람은 pass_prob=True일 때만 insert_alarm 수행.

[v9.3 추가]
- 실행 로그를 DB에도 저장:
  schema: k_demon_heath_check (없으면 생성)
  table : h_log (없으면 생성)

[중요 변경]
- REPL 소스:
  g_production_film.fct_non_operation_time(end_day,to_time) -> Back_end_i_daily_report.total_non_operation_time(to_ts)
- to_ts는 timestamptz(오프셋-aware) 이므로, 코드 전체를 "aware datetime(KST)"로 통일

[이번 요청 반영 - alarm_record 중복 방지]
- alarm_record에서 (end_day, end_time, station, sparepart) 가 같으면 "중복 INSERT 불가"
- 방법:
  1) UNIQUE INDEX 생성(없으면 생성)
  2) insert_alarm 을 ON CONFLICT(...) DO UPDATE로 변경 (즉, 행은 1개만 유지)
"""

from __future__ import annotations

import time
import pickle
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# =================================================
# 0) CONFIG
# =================================================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",  # 비번은 보완 사항
}

KST = ZoneInfo("Asia/Seoul")

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
SPAREPARTS = ["usb_a", "usb_c", "mini_b"]

# REPL(교체 이력) 소스 변경
REPL_SCHEMA = "Back_end_i_daily_report"
REPL_TABLE = "total_non_operation_time"
REPL_EXCLUDE_STATIONS = ("Vision1", "Vision2")

LOOP_INTERVAL_SEC = 5
BATCH_LIMIT_TEST_ROWS = 5000

SESSION_GUARDS_SQL = """
SET application_name = 'h_timing_spareparts_v9_3_logdb';
SET statement_timeout = '30s';
SET lock_timeout = '5s';
SET idle_in_transaction_session_timeout = '30s';
SET work_mem = '64MB';
SET maintenance_work_mem = '64MB';
SET temp_buffers = '16MB';
"""

MODEL_SCHEMA = "h_machine_learning"
MODEL_TABLE = '3_machine_learning_model'

TEST_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
TEST_TABLE = "fct_vision_testlog_txt_processing_history"

LIFE_SCHEMA = "e3_sparepart_replacement"
LIFE_TABLE = "sparepart_life_amount"

ALARM_SCHEMA = "g_production_film"
ALARM_TABLE = "alarm_record"

STATE_SCHEMA = "h_machine_learning"
STATE_TABLE = "sparepart_interval_state_rt_v9_2"

# DB 로그 저장 대상
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "h_log"

# 메모리 버퍼(DB 다운 시 로그 유실 방지)
PENDING_DB_LOGS: List[Tuple[str, str, str, str]] = []


# =================================================
# 1) TIME UTIL (aware 통일)
# =================================================
def now_kst() -> datetime:
    return datetime.now(tz=KST)


def ensure_aware_kst(dt: Optional[datetime]) -> Optional[datetime]:
    """
    - None => None
    - naive => KST로 로컬라이즈(운영 DB가 KST 기준으로 기록된다는 가정)
    - aware => KST로 변환
    """
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=KST)
    return dt.astimezone(KST)


def round_dt_to_sec(dt: datetime) -> datetime:
    dt = ensure_aware_kst(dt) or dt
    if dt.microsecond >= 500_000:
        dt = dt + timedelta(seconds=1)
    return dt.replace(microsecond=0)


# =================================================
# 2) LOG UTIL
# =================================================
def _normalize_info(info: str) -> str:
    if not info:
        return "info"
    return str(info).strip().lower()


def _make_log_row(info: str, contents: str, now: Optional[datetime] = None) -> Tuple[str, str, str, str]:
    d = now or now_kst()
    end_day = d.strftime("%Y%m%d")  # yyyymmdd
    end_time = d.strftime("%H:%M:%S")  # hh:mi:ss
    return end_day, end_time, _normalize_info(info), str(contents)


def print_log(msg: str) -> None:
    ts = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def enqueue_db_log(info: str, contents: str) -> None:
    PENDING_DB_LOGS.append(_make_log_row(info, contents))


def has_schema(conn, schema: str) -> bool:
    sql = "SELECT 1 FROM information_schema.schemata WHERE schema_name=%s"
    with conn.cursor() as cur:
        cur.execute(sql, (schema,))
        return cur.fetchone() is not None


def ensure_schema(conn, schema: str) -> None:
    if not has_schema(conn, schema):
        with conn.cursor() as cur:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")


def ensure_log_table(conn) -> None:
    ensure_schema(conn, LOG_SCHEMA)
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {LOG_SCHEMA}.{LOG_TABLE} (
                id BIGSERIAL PRIMARY KEY,
                end_day  TEXT NOT NULL,
                end_time TEXT NOT NULL,
                info     TEXT NOT NULL,
                contents TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            """
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE}_day_time "
            f"ON {LOG_SCHEMA}.{LOG_TABLE} (end_day, end_time);"
        )
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_{LOG_TABLE}_info "
            f"ON {LOG_SCHEMA}.{LOG_TABLE} (info);"
        )


def flush_db_logs(conn, max_batch: int = 1000) -> None:
    """
    end_day, end_time, info, contents 순서로 DataFrame화 후 저장.
    """
    if not PENDING_DB_LOGS:
        return

    rows = PENDING_DB_LOGS[:max_batch]
    df = pd.DataFrame(rows, columns=["end_day", "end_time", "info", "contents"])
    values = [tuple(x) for x in df[["end_day", "end_time", "info", "contents"]].to_records(index=False)]

    sql = f"""
    INSERT INTO {LOG_SCHEMA}.{LOG_TABLE}
      (end_day, end_time, info, contents)
    VALUES %s
    """

    with conn.cursor() as cur:
        execute_values(cur, sql, values, page_size=500)

    del PENDING_DB_LOGS[: len(rows)]


def log(msg: str, info: str = "info", conn: Optional[psycopg2.extensions.connection] = None) -> None:
    """
    콘솔 출력 + DB 로그 버퍼 적재.
    conn이 가능하면 즉시 flush 시도.
    """
    info_n = _normalize_info(info)
    print_log(msg)
    enqueue_db_log(info_n, msg)

    if conn is not None and conn.closed == 0:
        try:
            flush_db_logs(conn)
        except Exception:
            pass


# =================================================
# 3) DB UTIL
# =================================================
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

            ensure_log_table(conn)
            log("[OK] DB connected", info="info", conn=conn)
            return conn
        except Exception as e:
            log(f"[ERROR] DB connect failed: {type(e).__name__}: {e}", info="error", conn=None)
            time.sleep(2)


def safe_close(conn: Optional[psycopg2.extensions.connection]) -> None:
    try:
        if conn is not None:
            conn.close()
    except Exception:
        pass


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


# =================================================
# 4) ONE-TIME DDL (NO DROP)
# =================================================
def ensure_tables(conn) -> None:
    ensure_schema(conn, ALARM_SCHEMA)
    ensure_schema(conn, STATE_SCHEMA)
    ensure_log_table(conn)

    with conn.cursor() as cur:
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {STATE_SCHEMA}.{STATE_TABLE} (
            station   TEXT NOT NULL,
            sparepart TEXT NOT NULL,

            current_repl_end_ts TIMESTAMPTZ,
            next_repl_end_ts    TIMESTAMPTZ,

            last_test_ts    TIMESTAMPTZ,
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
            reset_repl_ts TIMESTAMPTZ NULL
        );
        """)

    alarm_cols = [
        ("run_id", "TEXT NOT NULL DEFAULT ''"),
        ("algo_ver", "TEXT NOT NULL DEFAULT ''"),
        ("reset_reason", "TEXT NOT NULL DEFAULT ''"),
        ("reset_repl_ts", "TIMESTAMPTZ NULL"),
    ]
    for col, ddl in alarm_cols:
        if not has_column(conn, ALARM_SCHEMA, ALARM_TABLE, col):
            with conn.cursor() as cur:
                cur.execute(f"ALTER TABLE {ALARM_SCHEMA}.{ALARM_TABLE} ADD COLUMN {col} {ddl};")

    # ✅ (핵심) 중복 방지 UNIQUE INDEX: (end_day, end_time, station, sparepart)
    # - 이미 데이터가 중복되어 있으면 생성이 실패할 수 있음.
    #   그 경우 먼저 중복 정리(최신 id만 남기기) 후 재시도 필요.
    with conn.cursor() as cur:
        cur.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_alarm_record_dts
            ON {ALARM_SCHEMA}.{ALARM_TABLE} (end_day, end_time, station, sparepart);
            """
        )

    with conn.cursor() as cur:
        cur.execute(
            f"CREATE INDEX IF NOT EXISTS idx_alarm_station_spare_created "
            f"ON {ALARM_SCHEMA}.{ALARM_TABLE} (station, sparepart, created_at DESC);"
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_alarm_runid ON {ALARM_SCHEMA}.{ALARM_TABLE} (run_id);")
        cur.execute(f"CREATE INDEX IF NOT EXISTS idx_state_updated ON {STATE_SCHEMA}.{STATE_TABLE} (updated_at DESC);")

    log("[OK] ensure_tables done (NO DROP)", info="info", conn=conn)


# =================================================
# 5) MODEL
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
# 6) LIFE(p25)
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
# 7) POLICY
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
# 8) INSERT ALARM (중복 방지 UPSERT)
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
    end_day_str = evt.strftime("%Y-%m-%d")   # 기존 유지 (alarm_record end_day 포맷)
    end_time_str = evt.strftime("%H:%M:%S")

    # ✅ 핵심: (end_day, end_time, station, sparepart) 동일이면 INSERT가 아니라 UPDATE
    # - “중복 insert 금지” 요건 충족(행은 1개만 유지)
    # - 재시도/네트워크 단절에도 멱등(idempotent)
    sql = f"""
    INSERT INTO {ALARM_SCHEMA}.{ALARM_TABLE}
      (end_day, end_time, station, sparepart, type_alarm, amount, min_prob, created_at,
       run_id, algo_ver, reset_reason, reset_repl_ts)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
            %s,%s,%s,%s)
    ON CONFLICT (end_day, end_time, station, sparepart)
    DO UPDATE SET
        type_alarm   = EXCLUDED.type_alarm,
        amount       = EXCLUDED.amount,
        min_prob     = EXCLUDED.min_prob,
        created_at   = EXCLUDED.created_at,
        run_id       = EXCLUDED.run_id,
        algo_ver     = EXCLUDED.algo_ver,
        reset_reason = EXCLUDED.reset_reason,
        reset_repl_ts= EXCLUDED.reset_repl_ts
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
                ensure_aware_kst(inspect_ts) or inspect_ts,
                run_id,
                algo_ver,
                reset_reason,
                ensure_aware_kst(reset_repl_ts),
            ),
        )


# =================================================
# 9) FEATURES
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
# 10) STATE
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
            current_repl_end_ts=ensure_aware_kst(row[2]),
            next_repl_end_ts=ensure_aware_kst(row[3]),
            last_test_ts=ensure_aware_kst(row[4]),
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
                st.station,
                st.sparepart,
                ensure_aware_kst(st.current_repl_end_ts),
                ensure_aware_kst(st.next_repl_end_ts),
                ensure_aware_kst(st.last_test_ts),
                int(st.amount),
                st.last_alarm_type,
            ),
        )


# =================================================
# 11) QUERIES
# =================================================
def preflight_required_columns(conn) -> None:
    # REPL (Back_end_i_daily_report.total_non_operation_time)
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "station")
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "sparepart")
    require_column(conn, REPL_SCHEMA, REPL_TABLE, "to_ts")

    # TEST
    require_column(conn, TEST_SCHEMA, TEST_TABLE, "end_day")
    require_column(conn, TEST_SCHEMA, TEST_TABLE, "end_time")
    require_column(conn, TEST_SCHEMA, TEST_TABLE, "station")

    # LIFE
    require_column(conn, LIFE_SCHEMA, LIFE_TABLE, "sparepart")
    require_column(conn, LIFE_SCHEMA, LIFE_TABLE, "p25")


def list_repl_end_ts(conn, station: str, sparepart: str) -> List[datetime]:
    sql = f"""
    SELECT to_ts AS e
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND station NOT IN %s
      AND sparepart::text=%s
      AND to_ts IS NOT NULL
    ORDER BY to_ts ASC
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, REPL_EXCLUDE_STATIONS, sparepart))
        rows = cur.fetchall()
    out = [ensure_aware_kst(r[0]) for r in rows] if rows else []
    return [x for x in out if x is not None]


def find_next_repl_end_after(conn, station: str, sparepart: str, current_end: datetime) -> Optional[datetime]:
    current_end = ensure_aware_kst(current_end) or current_end
    sql = f"""
    SELECT to_ts AS e
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND station NOT IN %s
      AND sparepart::text=%s
      AND to_ts IS NOT NULL
      AND to_ts > %s
    ORDER BY to_ts ASC
    LIMIT 1
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, REPL_EXCLUDE_STATIONS, sparepart, current_end))
        row = cur.fetchone()
    return ensure_aware_kst(row[0]) if row else None


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


def fetch_new_tests_ts_list(conn, station: str, after_ts: datetime, upper_ts: datetime, limit: int) -> List[datetime]:
    """
    TEST 테이블은 end_day/end_time 기반 -> ts_expr는 naive(timestamp)로 만들어질 수 있음.
    비교/저장은 KST-aware로 통일해야 하므로,
    - 쿼리는 naive로 비교 수행(동일 표현)
    - 결과를 받아서 KST-aware로 변환해 반환
    """
    after_ts = ensure_aware_kst(after_ts) or after_ts
    upper_ts = ensure_aware_kst(upper_ts) or upper_ts

    after_naive = after_ts.replace(tzinfo=None)
    upper_naive = upper_ts.replace(tzinfo=None)

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
        cur.execute(sql, (station, after_naive, upper_naive))
        rows = cur.fetchall()

    out: List[datetime] = []
    for r in rows or []:
        dt = r[0]
        dt = ensure_aware_kst(dt)
        if dt is not None:
            out.append(dt)
    return out


def roll_state_to_next(conn, st: State, station: str, sparepart: str) -> None:
    prev_current = st.current_repl_end_ts
    st.current_repl_end_ts = ensure_aware_kst(st.next_repl_end_ts)
    st.last_test_ts = st.current_repl_end_ts
    st.amount = 0
    st.last_alarm_type = None
    if st.current_repl_end_ts is not None:
        st.next_repl_end_ts = find_next_repl_end_after(conn, station, sparepart, st.current_repl_end_ts)
    else:
        st.next_repl_end_ts = None
    save_state(conn, st)
    log(
        f"[ROLL] {station}/{sparepart} {prev_current} -> {st.current_repl_end_ts} next={st.next_repl_end_ts}",
        info="info",
        conn=conn,
    )


def repl_count_debug(conn, station: str, sparepart: str) -> Tuple[int, Optional[datetime], Optional[datetime]]:
    sql = f"""
    SELECT COUNT(*)::bigint AS n,
           MIN(to_ts) AS first_to,
           MAX(to_ts) AS last_to
    FROM {REPL_SCHEMA}.{REPL_TABLE}
    WHERE station=%s
      AND station NOT IN %s
      AND sparepart::text=%s
      AND to_ts IS NOT NULL
    """
    with conn.cursor() as cur:
        cur.execute(sql, (station, REPL_EXCLUDE_STATIONS, sparepart))
        n, first_to, last_to = cur.fetchone()
    return int(n or 0), ensure_aware_kst(first_to), ensure_aware_kst(last_to)


# =================================================
# 12) MAIN
# =================================================
def main() -> None:
    RUN_ID = now_kst().strftime("%Y%m%d_%H%M%S") + "_v9_3_logdb"
    ALGO_VER = "v9_3_logdb"

    conn: Optional[psycopg2.extensions.connection] = None
    cached_model_id: Optional[int] = None
    cached_model: Any = None

    life_map: Dict[str, float] = {}
    last_life_reload = 0.0
    last_hb = 0.0

    log(f"[START] run_id={RUN_ID} algo_ver={ALGO_VER}", info="info", conn=None)

    while True:
        try:
            if conn is None or conn.closed != 0:
                conn = connect_forever()
                ensure_tables(conn)
                preflight_required_columns(conn)
                flush_db_logs(conn)

                log(f"[SRC] REPL={REPL_SCHEMA}.{REPL_TABLE} reset_ts=to_ts exclude={list(REPL_EXCLUDE_STATIONS)}", info="info", conn=conn)
                log(f"[SRC] STATE={STATE_SCHEMA}.{STATE_TABLE} (timestamptz aware; KST normalize)", info="info", conn=conn)

            now_ts = time.time()
            now_dt = now_kst()

            if now_ts - last_hb >= 60:
                log(
                    f"[HEARTBEAT] now={now_dt.strftime('%Y-%m-%d %H:%M:%S %z')} run_id={RUN_ID}",
                    info="info",
                    conn=conn,
                )
                last_hb = now_ts

            if not life_map or (now_ts - last_life_reload >= 300):
                life_map = load_life_p25_map(conn)
                last_life_reload = now_ts
                log(f"[OK] life(p25) loaded: {life_map}", info="info", conn=conn)

            model, mid = load_model_max_id(conn)
            if cached_model_id != mid:
                cached_model_id = mid
                cached_model = model
                log(f"[OK] model loaded (id={mid})", info="info", conn=conn)

            need_immediate = False

            for station in STATIONS:
                for sparepart in SPAREPARTS:
                    max_tests = float(life_map.get(sparepart, 0.0) or 0.0)
                    if max_tests <= 0:
                        continue

                    try:
                        repl_n, repl_first, repl_last = repl_count_debug(conn, station, sparepart)
                        log(f"[REPL-CHECK] {station}/{sparepart} repl_n={repl_n} first={repl_first} last={repl_last}", info="info", conn=conn)
                    except Exception as e:
                        log(f"[REPL-CHECK-ERROR] {station}/{sparepart} {type(e).__name__}: {e}", info="error", conn=conn)

                    st = load_state(conn, station, sparepart)

                    if st.current_repl_end_ts is None:
                        repl_list = list_repl_end_ts(conn, station, sparepart)
                        if not repl_list:
                            continue
                        st.current_repl_end_ts = ensure_aware_kst(repl_list[0])
                        st.next_repl_end_ts = ensure_aware_kst(repl_list[1]) if len(repl_list) >= 2 else None
                        st.last_test_ts = st.current_repl_end_ts
                        st.amount = 0
                        st.last_alarm_type = None
                        save_state(conn, st)
                        need_immediate = True
                        log(
                            f"[INIT] {station}/{sparepart} current_end={st.current_repl_end_ts} next_end={st.next_repl_end_ts}",
                            info="info",
                            conn=conn,
                        )

                    for _guard in range(50):
                        if st.current_repl_end_ts is not None:
                            nxt = find_next_repl_end_after(conn, station, sparepart, st.current_repl_end_ts)
                            if nxt is not None and (st.next_repl_end_ts is None or nxt != st.next_repl_end_ts):
                                st.next_repl_end_ts = nxt
                                save_state(conn, st)
                                need_immediate = True
                                log(
                                    f"[NEXT-UPDATE] {station}/{sparepart} next_end={st.next_repl_end_ts}",
                                    info="info",
                                    conn=conn,
                                )

                        if st.last_alarm_type == "교체" and st.next_repl_end_ts is None:
                            break

                        if st.current_repl_end_ts is None:
                            break

                        if st.next_repl_end_ts is not None and now_dt >= st.next_repl_end_ts:
                            upper_ts = st.next_repl_end_ts
                        else:
                            upper_ts = now_dt

                        after_ts = st.last_test_ts or st.current_repl_end_ts
                        after_ts = ensure_aware_kst(after_ts)
                        upper_ts = ensure_aware_kst(upper_ts)

                        if after_ts is None or upper_ts is None:
                            break

                        if after_ts >= upper_ts:
                            if st.next_repl_end_ts is not None and now_dt >= st.next_repl_end_ts:
                                roll_state_to_next(conn, st, station, sparepart)
                                need_immediate = True
                                continue
                            break

                        ts_list = fetch_new_tests_ts_list(conn, station, after_ts, upper_ts, BATCH_LIMIT_TEST_ROWS)

                        if not ts_list:
                            if st.next_repl_end_ts is not None and now_dt >= st.next_repl_end_ts and upper_ts == st.next_repl_end_ts:
                                log(
                                    f"[ROLL-NO-TEST] {station}/{sparepart} gap_no_tests (after={after_ts} <= next={st.next_repl_end_ts})",
                                    info="info",
                                    conn=conn,
                                )
                                roll_state_to_next(conn, st, station, sparepart)
                                need_immediate = True
                                continue
                            break

                        need_immediate = True

                        for tts in ts_list:
                            tts = ensure_aware_kst(tts) or tts
                            st.amount += 1
                            st.last_test_ts = tts

                            ratio = float(st.amount) / max_tests if max_tests > 0 else 0.0
                            alarm_type, min_prob = policy_by_ratio(ratio)

                            if alarm_type in ("준비", "권고", "긴급", "교체"):
                                if alarm_rank(alarm_type) > alarm_rank(st.last_alarm_type):
                                    prob = 0.0

                                    if alarm_type in ("준비", "권고"):
                                        pass_prob = False
                                        try:
                                            X = build_features_for_model(
                                                cached_model, station, sparepart, st.amount, max_tests, ratio
                                            )
                                            prob = predict_proba_1(cached_model, X)
                                            pass_prob = (prob >= float(min_prob))
                                        except Exception:
                                            pass_prob = False

                                        log(
                                            f"[EVAL] {station}/{sparepart} amount={st.amount} ratio={ratio:.3f} -> {alarm_type} prob={prob:.4f} pass={pass_prob}",
                                            info="info",
                                            conn=conn,
                                        )

                                        if pass_prob:
                                            insert_alarm(
                                                conn=conn,
                                                station=station,
                                                sparepart=sparepart,
                                                alarm_type=alarm_type,
                                                amount=st.amount,
                                                min_prob=float(min_prob),
                                                event_end_ts=tts,
                                                inspect_ts=now_kst(),
                                                run_id=RUN_ID,
                                                algo_ver=ALGO_VER,
                                                reset_reason="NORMAL_CROSS",
                                                reset_repl_ts=st.current_repl_end_ts,
                                            )
                                            st.last_alarm_type = alarm_type
                                            log(
                                                f"[ALARM-SAVED] {station}/{sparepart} {alarm_type} (event_ts={tts})",
                                                info="info",
                                                conn=conn,
                                            )
                                        else:
                                            log(
                                                f"[ALARM-SKIP] {station}/{sparepart} {alarm_type} (prob<{min_prob})",
                                                info="info",
                                                conn=conn,
                                            )

                                    else:
                                        try:
                                            X = build_features_for_model(
                                                cached_model, station, sparepart, st.amount, max_tests, ratio
                                            )
                                            prob = predict_proba_1(cached_model, X)
                                        except Exception:
                                            prob = 0.0

                                        log(
                                            f"[EVAL] {station}/{sparepart} amount={st.amount} ratio={ratio:.3f} -> {alarm_type} prob={prob:.4f} pass=True",
                                            info="info",
                                            conn=conn,
                                        )

                                        insert_alarm(
                                            conn=conn,
                                            station=station,
                                            sparepart=sparepart,
                                            alarm_type=alarm_type,
                                            amount=st.amount,
                                            min_prob=float(min_prob),
                                            event_end_ts=tts,
                                            inspect_ts=now_kst(),
                                            run_id=RUN_ID,
                                            algo_ver=ALGO_VER,
                                            reset_reason="NORMAL_CROSS",
                                            reset_repl_ts=st.current_repl_end_ts,
                                        )
                                        st.last_alarm_type = alarm_type
                                        log(
                                            f"[ALARM-SAVED] {station}/{sparepart} {alarm_type} (event_ts={tts})",
                                            info="info",
                                            conn=conn,
                                        )

                                        if alarm_type == "교체" and st.next_repl_end_ts is None:
                                            break

                        save_state(conn, st)
                        continue

            if need_immediate:
                log("[LOOP] sleep 0 (backfill/realtime immediate)", info="sleep", conn=conn)
                time.sleep(0)
            else:
                log(f"[LOOP] sleep {LOOP_INTERVAL_SEC}s", info="sleep", conn=conn)
                time.sleep(LOOP_INTERVAL_SEC)

            flush_db_logs(conn)

        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            log(f"[ERROR] DB disconnected: {type(e).__name__}: {e}", info="down", conn=None)
            safe_close(conn)
            conn = None
            time.sleep(2)

        except Exception as e:
            log(f"[ERROR] runtime: {type(e).__name__}: {e}", info="error", conn=conn)
            log(traceback.format_exc(), info="error", conn=conn)
            time.sleep(2)


if __name__ == "__main__":
    main()