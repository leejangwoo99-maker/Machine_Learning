# -*- coding: utf-8 -*-
"""
backend4_repeat_fail_daemon.py  (A안: DATA/LOG 엔진 분리, 단일커넥션 풀 충돌 제거)
------------------------------------------------------------
Backend-4: Vision FAIL repeat step_description daily daemon

✅ 이번 핵심 수정(backend3와 동일 계열 버그 제거):
- main loop에서 매 루프 계산된 win(current_window(now))는 upper_dt(=now로 clip)가 "현재"로 갱신됨
- 그런데 실수로 예전 win(bootstrap 시점 upper_dt 고정)을 계속 넘기면 window filter의 upper_t가 고정되어
  fetch-count/rows가 0으로 떨어지는 증상이 발생할 수 있음
- 이 파일은 구조상 win을 매 루프 재계산해서 incremental_step/upsert_counts에 넘기고 있으나,
  "data_engine 재연결 후 기존 data_engine 변수/전역 DATA_ENGINE이 꼬이거나,
   bootstrap 이후 prev_win_key는 같지만 win.upper_dt가 갱신되지 않는 객체를 재사용"하는 류의 실수를
  원천 봉쇄하도록,
  ✅ State에 anchor_win(=prod_day/shift_type/start/end)만 유지하고,
  ✅ fetch용 win은 매 루프 새로 생성한 win을 그대로 사용(upper_dt 최신),
  ✅ incremental_step에서 st.win_key와 불일치 시 즉시 bootstrap하도록 가드 추가,
  ✅ reconnect 이후 반드시 data_engine 변수와 전역 DATA_ENGINE을 동기화.

추가로 안정화(기능 동일):
- ensure_log_table의 CREATE SCHEMA 쿼트 오류 수정(기존: CREATE SCHEMA IF NOT EXISTS "schema"; 는 OK지만
  quote_ident를 중복 적용/SQL 형태가 이상해질 여지가 있어 정리)
- build_window_filter 로직은 그대로, upper_dt 최신값만 보장
- fetch_rows_incremental: pk 조건은 기존대로 '>' 유지
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Optional, Set, List, Iterable

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

DB_RETRY_INTERVAL_SEC = 5
LOOP_INTERVAL_SEC = 5
FETCH_BATCH_LIMIT = 5000  # 루프당 과도 fetch 방지

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",  # 비번은 보안사항
}

VISION_SCHEMA = "a3_vision_table"
VISION_TABLE = "vision_table"

REMARK_SCHEMA = "g_production_film"
REMARK_TABLE = "remark_info"

SAVE_SCHEMA = "i_daily_report"

TABLES = {
    ("day", "1"): "d_vs_1time_step_decription_day_daily",
    ("day", "2"): "d_vs_2time_step_decription_day_daily",
    ("day", "3+"): "d_vs_3time_over_step_decription_day_daily",
    ("night", "1"): "d_vs_1time_step_decription_night_daily",
    ("night", "2"): "d_vs_2time_step_decription_night_daily",
    ("night", "3+"): "d_vs_3time_over_step_decription_night_daily",
}

BUCKET_COL = {
    "1": "1회 FAIL_step_description",
    "2": "2회 반복_FAIL_step_description",
    "3+": "3회 이상 반복_FAIL_step_description",
}

# 로그 DB
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "4_log"  # 숫자 시작 테이블명은 쌍따옴표 필요

# 엔진 분리 (A안)
DATA_ENGINE: Optional[Engine] = None
LOG_ENGINE: Optional[Engine] = None

# 로그 재귀 방지
_LOG_DB_REENTRANT_GUARD = False


# =========================
# Utils
# =========================
def _ts_kst() -> datetime:
    return datetime.now(tz=KST)


def _fmt_now() -> str:
    return _ts_kst().strftime("%Y-%m-%d %H:%M:%S")


def quote_ident(name: str) -> str:
    # 스키마/테이블/컬럼 안전용
    return '"' + name.replace('"', '""') + '"'


def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def get_barcode_key_18th(barcode: Optional[str]) -> Optional[str]:
    if not barcode or len(barcode) < 18:
        return None
    return barcode[17]


def bucket_for(cnt: int) -> Optional[str]:
    if cnt <= 0:
        return None
    if cnt == 1:
        return "1"
    if cnt == 2:
        return "2"
    return "3+"


# =========================
# Health Log Table (DB)
# =========================
def ensure_log_table(engine: Engine) -> None:
    # schema/table/col은 식별자이므로 quote_ident 적용
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(LOG_SCHEMA)};"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {quote_ident(LOG_SCHEMA)}.{quote_ident(LOG_TABLE)} (
                    {quote_ident("end_day")}  text,
                    {quote_ident("end_time")} text,
                    {quote_ident("info")}     text,
                    {quote_ident("contents")} text
                );
                """
            )
        )


def _insert_health_log(engine: Engine, info: str, contents: str) -> None:
    """
    end_day, end_time, info, contents 순서로 INSERT (to_sql 금지)
    """
    now = _ts_kst()
    payload = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "").lower(),
        "contents": str(contents),
    }

    sql = text(
        f"""
        INSERT INTO {quote_ident(LOG_SCHEMA)}.{quote_ident(LOG_TABLE)}
        ({quote_ident("end_day")}, {quote_ident("end_time")}, {quote_ident("info")}, {quote_ident("contents")})
        VALUES (:end_day, :end_time, :info, :contents)
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, payload)


def _emit(level_tag: str, info: str, msg: str, persist: bool = True) -> None:
    """
    level_tag: 콘솔 표기용 [BOOT]/[INFO]/[RETRY]/[WARN]
    info     : DB 저장용 소문자 코드 (boot/info/down/sleep/warn/error ...)
    """
    print(f"{_fmt_now()} [{level_tag}] {msg}", flush=True)

    if not persist:
        return

    global LOG_ENGINE, _LOG_DB_REENTRANT_GUARD
    if LOG_ENGINE is None:
        return
    if _LOG_DB_REENTRANT_GUARD:
        return

    try:
        _LOG_DB_REENTRANT_GUARD = True
        _insert_health_log(LOG_ENGINE, info=info.lower(), contents=msg)
    except Exception as e:
        # 로그 저장 실패는 본 로직을 절대 막지 않음
        print(f"{_fmt_now()} [WARN] health-log insert failed: {type(e).__name__}: {e}", flush=True)
    finally:
        _LOG_DB_REENTRANT_GUARD = False


def log_boot(msg: str) -> None:
    _emit("BOOT", "boot", msg, persist=True)


def log_info(msg: str) -> None:
    _emit("INFO", "info", msg, persist=True)


def log_retry(msg: str) -> None:
    _emit("RETRY", "down", msg, persist=True)


def log_warn(msg: str) -> None:
    _emit("WARN", "warn", msg, persist=True)


def log_sleep(msg: str) -> None:
    _emit("INFO", "sleep", msg, persist=True)


def log_error(msg: str) -> None:
    _emit("WARN", "error", msg, persist=True)


# =========================
# DB Identity logger
# =========================
def log_db_identity(engine: Engine) -> None:
    sql = """
    SELECT
      current_database()  AS db,
      current_user        AS usr,
      inet_server_addr()::text AS server_ip,
      inet_server_port()  AS server_port,
      inet_client_addr()::text AS client_ip
    """
    with engine.connect() as conn:
        row = conn.execute(text(sql)).mappings().first()

    log_info(
        f"[DB_ID] db={row['db']} usr={row['usr']} "
        f"server={row['server_ip']}:{row['server_port']} client={row['client_ip']}"
    )


# =========================
# Engines (A안: 분리)
# =========================
def _make_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

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


def _ensure_db_ready(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))


def connect_both_with_retry() -> Tuple[Engine, Engine]:
    """
    DATA_ENGINE / LOG_ENGINE 각각 별도 engine 생성.
    - 둘 다 성공할 때까지 블로킹 재시도.
    """
    global DATA_ENGINE, LOG_ENGINE

    log_boot("backend4 repeat-fail daemon starting")

    while True:
        data_engine = _make_engine()
        log_engine = _make_engine()
        try:
            _ensure_db_ready(data_engine)
            _ensure_db_ready(log_engine)

            ensure_log_table(log_engine)
            LOG_ENGINE = log_engine

            log_info(f"DB connected (work_mem={WORK_MEM})")
            log_db_identity(data_engine)

            DATA_ENGINE = data_engine
            log_info(f'health log ready -> {LOG_SCHEMA}."{LOG_TABLE}"')
            return data_engine, log_engine
        except Exception as e:
            LOG_ENGINE = None
            DATA_ENGINE = None
            print(f"{_fmt_now()} [RETRY] DB connect failed: {type(e).__name__}: {e}", flush=True)
            print(f"{_fmt_now()} [INFO] sleep {DB_RETRY_INTERVAL_SEC}s before reconnect", flush=True)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# WINDOW 자동 결정
# =========================
@dataclass(frozen=True)
class WindowKey:
    prod_day: str
    shift_type: str  # 'day' or 'night'
    start_dt: datetime
    end_dt: datetime
    upper_dt: datetime  # ✅ now로 clip된 "현재까지" 상한 (여기가 매 루프 최신이어야 함)


def current_window(now: datetime) -> WindowKey:
    now = now.astimezone(KST)
    t = now.timetz()

    day_start = time(8, 30, 0, tzinfo=KST)
    day_end = time(20, 29, 59, tzinfo=KST)
    night_end = time(8, 29, 59, tzinfo=KST)

    today = now.date()

    if t <= night_end:
        prod_d = today - timedelta(days=1)
        shift_type = "night"
        start_dt = datetime.combine(prod_d, time(20, 30, 0), tzinfo=KST)
        end_dt = datetime.combine(prod_d + timedelta(days=1), time(8, 29, 59), tzinfo=KST)
    elif day_start <= t <= day_end:
        prod_d = today
        shift_type = "day"
        start_dt = datetime.combine(prod_d, time(8, 30, 0), tzinfo=KST)
        end_dt = datetime.combine(prod_d, time(20, 29, 59), tzinfo=KST)
    else:
        prod_d = today
        shift_type = "night"
        start_dt = datetime.combine(prod_d, time(20, 30, 0), tzinfo=KST)
        end_dt = datetime.combine(prod_d + timedelta(days=1), time(8, 29, 59), tzinfo=KST)

    upper_dt = min(now, end_dt)

    return WindowKey(
        prod_day=date_to_yyyymmdd(prod_d),
        shift_type=shift_type,
        start_dt=start_dt,
        end_dt=end_dt,
        upper_dt=upper_dt,
    )


# =========================
# Schema/Tables (SAVE)
# =========================
def ensure_schema_and_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(SAVE_SCHEMA)};"))

    for (shift_type, bucket), table in TABLES.items():
        bucket_col = BUCKET_COL[bucket]
        cols = [
            ("prod_day", "text"),
            ("shift_type", "text"),
            ("pn", "text"),
            (bucket_col, "text"),
            ("count", "text"),
            ("updated_at", "timestamptz"),
        ]
        ddl_cols = ", ".join(f"{quote_ident(c)} {t}" for c, t in cols)

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {quote_ident(SAVE_SCHEMA)}.{quote_ident(table)} (
            {ddl_cols}
        );
        """

        idx_name = f"uq_{table}_key"
        create_idx_sql = f"""
        CREATE UNIQUE INDEX IF NOT EXISTS {quote_ident(idx_name)}
        ON {quote_ident(SAVE_SCHEMA)}.{quote_ident(table)}
        ({quote_ident("prod_day")}, {quote_ident("shift_type")}, {quote_ident("pn")}, {quote_ident(bucket_col)});
        """

        with engine.begin() as conn:
            conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
            conn.execute(text(create_table_sql))
            conn.execute(text(create_idx_sql))


# =========================
# remark_info -> pn_map
# =========================
def load_pn_map(engine: Engine) -> Dict[str, str]:
    sql = text(
        f"""
        SELECT barcode_information, pn
        FROM {REMARK_SCHEMA}.{REMARK_TABLE}
        """
    )
    pn_map: Dict[str, str] = {}
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
        rows = conn.execute(sql).fetchall()
    for r in rows:
        k = (r[0] or "").strip()
        v = (r[1] or "").strip()
        if k and v:
            pn_map[k] = v
    return pn_map


# =========================
# vision_table fetch
# =========================
def build_window_filter(win: WindowKey) -> Tuple[str, Dict[str, str]]:
    prod_d = yyyymmdd_to_date(win.prod_day)
    d0 = prod_d
    d1 = prod_d + timedelta(days=1)
    d0s = date_to_yyyymmdd(d0)
    d1s = date_to_yyyymmdd(d1)

    upper_date = win.upper_dt.date()
    upper_time_str = win.upper_dt.strftime("%H:%M:%S")

    if win.shift_type == "day":
        where_sql = """
            end_day = :d0
            AND end_time >= '08:30:00'
            AND end_time <= :upper_t
        """
        params = {"d0": d0s, "upper_t": upper_time_str}
        return where_sql, params

    if upper_date == d0:
        where_sql = """
            (end_day = :d0 AND end_time >= '20:30:00' AND end_time <= :upper_t)
        """
        params = {"d0": d0s, "upper_t": upper_time_str}
        return where_sql, params

    where_sql = """
        (
            (end_day = :d0 AND end_time >= '20:30:00' AND end_time <= '23:59:59')
            OR
            (end_day = :d1 AND end_time >= '00:00:00' AND end_time <= :upper_t)
        )
    """
    params = {"d0": d0s, "d1": d1s, "upper_t": upper_time_str}
    return where_sql, params


def fetch_rows_incremental(
    engine: Engine,
    win: WindowKey,
    last_pk: Optional[Tuple[str, str, str]],
    limit: int = FETCH_BATCH_LIMIT,
) -> List[Tuple[str, str, str, str]]:
    where_win, params = build_window_filter(win)

    pk_cond = ""
    if last_pk is not None:
        pk_cond = """
        AND (end_day, end_time, barcode_information) > (:pk_day, :pk_time, :pk_bar)
        """
        params = dict(params)
        params.update({"pk_day": last_pk[0], "pk_time": last_pk[1], "pk_bar": last_pk[2]})

    sql = text(
        f"""
        SELECT
            end_day,
            end_time,
            barcode_information,
            step_description
        FROM {VISION_SCHEMA}.{VISION_TABLE}
        WHERE
            result = 'FAIL'
            AND {where_win}
            {pk_cond}
        ORDER BY end_day, end_time, barcode_information, step_description
        LIMIT :lim
        """
    )
    params["lim"] = limit

    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
        rows = conn.execute(sql, params).fetchall()

    return [(r[0], r[1], r[2], r[3]) for r in rows]


def fetch_rows_bootstrap(engine: Engine, win: WindowKey) -> Iterable[Tuple[str, str, str, str]]:
    last_pk: Optional[Tuple[str, str, str]] = None
    while True:
        rows = fetch_rows_incremental(engine, win, last_pk, limit=FETCH_BATCH_LIMIT)
        if not rows:
            break
        for r in rows:
            yield r
        last = rows[-1]
        last_pk = (last[0], last[1], last[2])


# =========================
# In-memory state / aggregation
# =========================
@dataclass
class State:
    # ✅ anchor: 윈도우의 "정체성"만 보관(upper_dt는 저장하지 않음)
    win_key: Optional[Tuple[str, str]] = None  # (prod_day, shift)
    last_pk: Optional[Tuple[str, str, str]] = None
    pn_map: Dict[str, str] = field(default_factory=dict)

    seen_pk: Set[Tuple[str, str, str, str]] = field(default_factory=set)  # (end_day, end_time, barcode, step)
    pair_cnt: Dict[Tuple[str, str], int] = field(default_factory=dict)     # (barcode, step) -> cnt
    pair_bucket: Dict[Tuple[str, str], str] = field(default_factory=dict)  # (barcode, step) -> bucket
    barcode_pn: Dict[str, str] = field(default_factory=dict)              # barcode -> pn

    agg: Dict[str, Dict[Tuple[str, str], int]] = field(default_factory=lambda: {"1": {}, "2": {}, "3+": {}})
    dirty: Set[Tuple[str, str, str]] = field(default_factory=set)         # (bucket, pn, step)

    def reset_for_new_window(self, win: WindowKey, pn_map: Dict[str, str]) -> None:
        self.win_key = (win.prod_day, win.shift_type)
        self.last_pk = None
        self.pn_map = pn_map

        self.seen_pk.clear()
        self.pair_cnt.clear()
        self.pair_bucket.clear()
        self.barcode_pn.clear()
        self.agg = {"1": {}, "2": {}, "3+": {}}
        self.dirty.clear()


def apply_event(st: State, end_day: str, end_time: str, barcode: str, step: str) -> None:
    pk4 = (end_day, end_time, barcode, step)
    if pk4 in st.seen_pk:
        return
    st.seen_pk.add(pk4)

    if barcode not in st.barcode_pn:
        k = get_barcode_key_18th(barcode)
        pn = st.pn_map.get(k, "Unknown") if k else "Unknown"
        st.barcode_pn[barcode] = pn
    pn = st.barcode_pn[barcode]

    pair = (barcode, step)
    old_cnt = st.pair_cnt.get(pair, 0)
    new_cnt = old_cnt + 1
    st.pair_cnt[pair] = new_cnt

    old_bucket = st.pair_bucket.get(pair) if old_cnt > 0 else None
    new_bucket = bucket_for(new_cnt)
    if new_bucket is None:
        return

    if old_bucket == new_bucket:
        return

    # old bucket decrement
    if old_bucket is not None:
        key = (pn, step)
        prev = st.agg[old_bucket].get(key, 0)
        st.agg[old_bucket][key] = max(prev - 1, 0)
        st.dirty.add((old_bucket, pn, step))

    # new bucket increment
    key = (pn, step)
    st.agg[new_bucket][key] = st.agg[new_bucket].get(key, 0) + 1
    st.dirty.add((new_bucket, pn, step))

    st.pair_bucket[pair] = new_bucket


# =========================
# UPSERT
# =========================
def upsert_counts(engine: Engine, win: WindowKey, st: State) -> None:
    if not st.dirty:
        return

    prod_day = win.prod_day
    shift_type = win.shift_type
    now_ts = _ts_kst()

    by_bucket: Dict[str, List[Tuple[str, str]]] = {"1": [], "2": [], "3+": []}
    for bucket, pn, step in st.dirty:
        by_bucket[bucket].append((pn, step))

    total_updates = 0
    total_upserts = 0

    for bucket, items in by_bucket.items():
        if not items:
            continue

        table = TABLES[(shift_type, bucket)]
        bucket_col = BUCKET_COL[bucket]

        to_update_only = []
        to_upsert = []

        for pn, step in items:
            cnt = st.agg[bucket].get((pn, step), 0)
            if cnt <= 0:
                to_update_only.append((pn, step, 0))
            else:
                to_upsert.append((pn, step, cnt))

        # cnt=0인 경우 update만 시도
        if to_update_only:
            upd_sql = text(
                f"""
                UPDATE {quote_ident(SAVE_SCHEMA)}.{quote_ident(table)}
                   SET {quote_ident("count")} = :cnt,
                       {quote_ident("updated_at")} = :updated_at
                 WHERE {quote_ident("prod_day")} = :prod_day
                   AND {quote_ident("shift_type")} = :shift_type
                   AND {quote_ident("pn")} = :pn
                   AND {quote_ident(bucket_col)} = :step
                """
            )
            payload = [
                {
                    "prod_day": prod_day,
                    "shift_type": shift_type,
                    "pn": pn,
                    "step": step,
                    "cnt": "0",
                    "updated_at": now_ts,
                }
                for pn, step, _cnt in to_update_only
            ]
            with engine.begin() as conn:
                conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
                res = conn.execute(upd_sql, payload)
                total_updates += (res.rowcount or 0)

        # cnt>0인 경우 upsert
        if to_upsert:
            ins_sql = text(
                f"""
                INSERT INTO {quote_ident(SAVE_SCHEMA)}.{quote_ident(table)}
                ({quote_ident("prod_day")}, {quote_ident("shift_type")}, {quote_ident("pn")},
                 {quote_ident(bucket_col)}, {quote_ident("count")}, {quote_ident("updated_at")})
                VALUES (:prod_day, :shift_type, :pn, :step, :cnt, :updated_at)
                ON CONFLICT ({quote_ident("prod_day")}, {quote_ident("shift_type")}, {quote_ident("pn")}, {quote_ident(bucket_col)})
                DO UPDATE SET
                    {quote_ident("count")} = EXCLUDED.{quote_ident("count")},
                    {quote_ident("updated_at")} = EXCLUDED.{quote_ident("updated_at")}
                """
            )
            payload = [
                {
                    "prod_day": prod_day,
                    "shift_type": shift_type,
                    "pn": pn,
                    "step": step,
                    "cnt": str(cnt),
                    "updated_at": now_ts,
                }
                for pn, step, cnt in to_upsert
            ]
            with engine.begin() as conn:
                conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
                conn.execute(ins_sql, payload)
                total_upserts += len(payload)

    st.dirty.clear()
    log_info(f"[UPSERT] window={win.prod_day}:{win.shift_type} upserted={total_upserts}, updated={total_updates}")


# =========================
# Bootstrap + Incremental Loop
# =========================
def bootstrap(engine: Engine, win: WindowKey, st: State) -> None:
    log_info(f"[BOOTSTRAP] start window={win.prod_day}:{win.shift_type} ({win.start_dt} ~ {win.upper_dt})")

    pn_map = load_pn_map(engine)
    st.reset_for_new_window(win, pn_map)

    n = 0
    last_pk: Optional[Tuple[str, str, str]] = None

    for end_day, end_time, barcode, step in fetch_rows_bootstrap(engine, win):
        if barcode is None or step is None:
            continue
        apply_event(st, str(end_day), str(end_time), str(barcode), str(step))
        n += 1
        last_pk = (str(end_day), str(end_time), str(barcode))

    st.last_pk = last_pk

    # bootstrap 후에는 현재 agg 전체를 dirty로 올려서 1회 upsert
    for bucket in ("1", "2", "3+"):
        for (pn, step), _cnt in st.agg[bucket].items():
            st.dirty.add((bucket, pn, step))

    log_info(f"[BOOTSTRAP] done fetched={n} last_pk={st.last_pk}")


def incremental_step(engine: Engine, win: WindowKey, st: State) -> int:
    # ✅ 안전가드: state가 다른 윈도우를 보고 있으면 fetch하지 말고 bootstrap 유도
    if st.win_key != (win.prod_day, win.shift_type):
        log_warn(f"[GUARD] state.win_key={st.win_key} != current={win.prod_day}:{win.shift_type} -> need bootstrap")
        return 0

    log_info(f"[LAST_PK] {st.last_pk}")

    total_new = 0
    while True:
        rows = fetch_rows_incremental(engine, win, st.last_pk, limit=FETCH_BATCH_LIMIT)
        if not rows:
            break

        for end_day, end_time, barcode, step in rows:
            if barcode is None or step is None:
                continue
            apply_event(st, str(end_day), str(end_time), str(barcode), str(step))
            total_new += 1

        last = rows[-1]
        st.last_pk = (str(last[0]), str(last[1]), str(last[2]))

        if len(rows) < FETCH_BATCH_LIMIT:
            break

    log_info(f"[FETCH] new_rows={total_new} last_pk={st.last_pk}")
    return total_new


# =========================
# Main
# =========================
def main() -> None:
    global DATA_ENGINE, LOG_ENGINE

    # DATA/LOG 엔진 모두 성공할 때까지 블로킹
    data_engine, _log_engine = connect_both_with_retry()
    # ✅ 전역 동기화(재연결 후 꼬임 방지)
    DATA_ENGINE = data_engine

    # 저장 테이블 준비 (DATA 엔진)
    ensure_schema_and_tables(data_engine)

    st = State()
    prev_win_key: Optional[Tuple[str, str]] = None

    # 첫 bootstrap
    while True:
        try:
            win0 = current_window(datetime.now(tz=KST))
            bootstrap(data_engine, win0, st)
            upsert_counts(data_engine, win0, st)
            prev_win_key = (win0.prod_day, win0.shift_type)
            break
        except (OperationalError, DBAPIError) as e:
            log_retry(f"bootstrap DB error: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            data_engine, _log_engine = connect_both_with_retry()
            DATA_ENGINE = data_engine
            ensure_schema_and_tables(data_engine)
        except Exception as e:
            log_error(f"bootstrap unhandled: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s and retry bootstrap")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    # main loop
    while True:
        loop_t0 = time_mod.time()
        try:
            # ✅ 매 루프 win은 새로 계산(upper_dt 최신)
            now = datetime.now(tz=KST)
            win = current_window(now)
            win_key = (win.prod_day, win.shift_type)

            if prev_win_key != win_key:
                log_info(
                    f"[WINDOW] changed => {win.prod_day}:{win.shift_type} "
                    f"(start={win.start_dt}, end={win.end_dt}, upper={win.upper_dt})"
                )
                bootstrap(data_engine, win, st)
                upsert_counts(data_engine, win, st)
                prev_win_key = win_key
            else:
                # ✅ 핵심: incremental_step/upsert_counts에 'win'을 그대로 넘겨 upper_dt 최신 보장
                new_n = incremental_step(data_engine, win, st)

                # guard로 0이 내려왔는데 state 윈도우가 다른 경우(윈도우 불일치) -> bootstrap
                if new_n == 0 and st.win_key != win_key:
                    bootstrap(data_engine, win, st)

                if st.dirty:
                    upsert_counts(data_engine, win, st)

            # 안전장치: seen_pk 과도 증가 시 강제 bootstrap
            if len(st.seen_pk) > 2_000_000:
                log_warn(f"seen_pk too large ({len(st.seen_pk)}). force bootstrap.")
                win2 = current_window(datetime.now(tz=KST))
                bootstrap(data_engine, win2, st)
                upsert_counts(data_engine, win2, st)
                prev_win_key = (win2.prod_day, win2.shift_type)

        except (OperationalError, DBAPIError) as e:
            log_retry(f"DB error: {type(e).__name__}: {e}")

            # 재연결 무한 재시도 + bootstrap으로 정상화
            while True:
                try:
                    log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)

                    data_engine, _log_engine = connect_both_with_retry()
                    DATA_ENGINE = data_engine
                    ensure_schema_and_tables(data_engine)

                    log_info("DB reconnected (data/log engines)")

                    winr = current_window(datetime.now(tz=KST))
                    bootstrap(data_engine, winr, st)
                    upsert_counts(data_engine, winr, st)
                    prev_win_key = (winr.prod_day, winr.shift_type)
                    break
                except Exception as e2:
                    print(f"{_fmt_now()} [RETRY] reconnect failed: {type(e2).__name__}: {e2}", flush=True)
                    continue

        except Exception as e:
            log_error(f"Unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_t0
        sleep_s = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_s > 0:
            log_sleep(f"loop sleep {sleep_s:.2f}s")
            time_mod.sleep(sleep_s)


if __name__ == "__main__":
    main()