# -*- coding: utf-8 -*-
"""
backend4_repeat_fail_daemon.py
------------------------------------------------------------
Backend-4: Vision FAIL repeat step_description daily daemon

[기존 요구사항 유지]
1) dataframe 콘솔 출력 없음
2) 날짜는 WINDOW 기준 현재날짜/현재시각으로 자동 전환
3) 멀티프로세스 1개
4) 무한루프 5초
5) DB 접속 실패 시 무한 재시도(연결될 때까지 블로킹)
6) 실행 중 DB 끊김 발생 시 무한 재접속 후 계속
7) 백엔드 상시 연결 1개(pool 최소화)
8) work_mem 폭증 방지(PG_WORK_MEM, default 4MB)
9) 증분 PK: (end_day, end_time, barcode_information) 기준
10) seen_pk 캐시: (end_day, end_time_norm, barcode_information, step_description)
11) [BOOT] 즉시 출력, DB 미접속 시 [RETRY] 5초마다 출력
12) last_pk 읽기/신규 fetch/insert 단계마다 [INFO]
    - fetch된 신규 row만 반영하여 in-memory 집계 증분 업데이트
    - last_pk는 메모리만 사용
13) 재실행 시 DELETE/TRUNCATE 금지
    - last_pk가 날아가므로 현재 윈도우(start~now) 전체 bootstrap 후 UPSERT

저장 테이블(스키마 i_daily_report):
- day 1회:  d_vs_1time_step_decription_day_daily
- day 2회:  d_vs_2time_step_decription_day_daily
- day 3+:   d_vs_3time_over_step_decription_day_daily
- night 1회:d_vs_1time_step_decription_night_daily
- night 2회:d_vs_2time_step_decription_night_daily
- night 3+: d_vs_3time_over_step_decription_night_daily

주의:
- 컬럼명은 bucket별로 그대로 사용(한글/공백 포함)
- 유일키: (prod_day, shift_type, pn, <bucket_col>)

[추가된 로그 DB 사양]
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: "4_log" (없으면 생성)
- 컬럼:
  1) end_day   : yyyymmdd
  2) end_time  : hh:mi:ss
  3) info      : 소문자
  4) contents  : 나머지 로그 내용
- 저장 시 컬럼 순서: end_day, end_time, info, contents
- DataFrame화 후 append 저장
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass, field
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Tuple, Optional, Set, List, Iterable

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

KST = ZoneInfo("Asia/Seoul")

# =========================
# 0) 환경 설정
# =========================
DB_RETRY_INTERVAL_SEC = 5
LOOP_INTERVAL_SEC = 5
FETCH_BATCH_LIMIT = 5000  # 루프당 과도 fetch 방지

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
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
LOG_TABLE = "4_log"

# 전역 로깅 엔진 핸들(연결 성공 후 활성화)
LOG_ENGINE: Optional[Engine] = None
_LOG_DB_REENTRANT_GUARD = False


# =========================
# 1) 공통 유틸
# =========================
def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def yyyymmdd_to_date(s: str) -> date:
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


def date_to_yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def get_barcode_key_18th(barcode: Optional[str]) -> Optional[str]:
    if not barcode:
        return None
    if len(barcode) < 18:
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
# 2) 로그 DB 저장
# =========================
def ensure_log_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(LOG_SCHEMA)};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {quote_ident(LOG_SCHEMA)}.{quote_ident(LOG_TABLE)} (
                {quote_ident("end_day")}  text,
                {quote_ident("end_time")} text,
                {quote_ident("info")}     text,
                {quote_ident("contents")} text
            );
        """))


def write_log_to_db(level: str, msg: str) -> None:
    """
    컬럼 순서(end_day, end_time, info, contents)로 DataFrame화 후 append 저장.
    info는 반드시 소문자 저장.
    """
    global _LOG_DB_REENTRANT_GUARD

    if LOG_ENGINE is None:
        return
    if _LOG_DB_REENTRANT_GUARD:
        return

    ts = datetime.now(tz=KST)
    end_day = ts.strftime("%Y%m%d")
    end_time = ts.strftime("%H:%M:%S")
    info = (level or "").strip().lower()
    contents = str(msg)

    df = pd.DataFrame(
        [[end_day, end_time, info, contents]],
        columns=["end_day", "end_time", "info", "contents"],
    )

    try:
        _LOG_DB_REENTRANT_GUARD = True
        # pandas.to_sql 사용 (append)
        df.to_sql(
            name=LOG_TABLE,
            con=LOG_ENGINE,
            schema=LOG_SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
        )
    except Exception:
        # 로그 저장 실패는 콘솔 동작에 영향 주지 않음
        pass
    finally:
        _LOG_DB_REENTRANT_GUARD = False


def log(level: str, msg: str) -> None:
    now = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
    lv = (level or "").upper()
    print(f"{now} [{lv}] {msg}", flush=True)

    # DB에도 저장 (info는 소문자 변환)
    write_log_to_db(level=level, msg=msg)


# =========================
# 3) WINDOW 자동 결정
# =========================
@dataclass(frozen=True)
class WindowKey:
    prod_day: str      # YYYYMMDD (window 기준)
    shift_type: str    # 'day' or 'night'
    start_dt: datetime
    end_dt: datetime   # 고정 window end
    upper_dt: datetime # min(now, end_dt) (bootstrap/fetch 상한)


def current_window(now: datetime) -> WindowKey:
    """
    now 기준 자동 전환
    - day:   D 08:30:00 ~ D 20:29:59
    - night: D 20:30:00 ~ D+1 08:29:59
      (00:00~08:29:59 구간은 '어제 prod_day'의 night)
    """
    now = now.astimezone(KST)
    t = now.timetz()

    day_start = time(8, 30, 0, tzinfo=KST)
    day_end = time(20, 29, 59, tzinfo=KST)
    night_end = time(8, 29, 59, tzinfo=KST)

    today = now.date()

    # 00:00~08:29:59 => 어제 night
    if t <= night_end:
        prod_d = today - timedelta(days=1)
        shift_type = "night"
        start_dt = datetime.combine(prod_d, time(20, 30, 0), tzinfo=KST)
        end_dt = datetime.combine(prod_d + timedelta(days=1), time(8, 29, 59), tzinfo=KST)
    # 08:30~20:29:59 => today day
    elif day_start <= t <= day_end:
        prod_d = today
        shift_type = "day"
        start_dt = datetime.combine(prod_d, time(8, 30, 0), tzinfo=KST)
        end_dt = datetime.combine(prod_d, time(20, 29, 59), tzinfo=KST)
    # 20:30~23:59:59 => today night
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
# 4) DB 연결/세션
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


def connect_with_retry() -> Engine:
    global LOG_ENGINE

    engine = make_engine()
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))

            # 로그 테이블 먼저 보장 후 로깅 엔진 활성화
            ensure_log_table(engine)
            LOG_ENGINE = engine

            log("info", f"db connected (work_mem={WORK_MEM})")
            return engine
        except Exception as e:
            # 연결 자체 실패 시에는 콘솔 출력만(아직 LOG_ENGINE 미보장 가능)
            now = datetime.now(tz=KST).strftime("%Y-%m-%d %H:%M:%S")
            msg = f"db connect failed: {type(e).__name__}: {e} (retry in {DB_RETRY_INTERVAL_SEC}s)"
            print(f"{now} [RETRY] {msg}", flush=True)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = make_engine()


def ensure_schema_and_tables(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {quote_ident(SAVE_SCHEMA)};"))

    # 테이블 + unique index(UPSERT 키용) 준비
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
# 5) remark_info -> pn_map
# =========================
def load_pn_map(engine: Engine) -> Dict[str, str]:
    sql = text(f"""
        SELECT barcode_information, pn
        FROM {REMARK_SCHEMA}.{REMARK_TABLE}
    """)
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
# 6) vision_table fetch
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

    sql = text(f"""
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
    """)
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
# 7) in-memory 상태/증분 집계
# =========================
@dataclass
class State:
    win_key: Optional[Tuple[str, str]] = None
    last_pk: Optional[Tuple[str, str, str]] = None
    pn_map: Dict[str, str] = field(default_factory=dict)

    seen_pk: Set[Tuple[str, str, str, str]] = field(default_factory=set)
    pair_cnt: Dict[Tuple[str, str], int] = field(default_factory=dict)
    pair_bucket: Dict[Tuple[str, str], str] = field(default_factory=dict)
    barcode_pn: Dict[str, str] = field(default_factory=dict)

    agg: Dict[str, Dict[Tuple[str, str], int]] = field(default_factory=lambda: {"1": {}, "2": {}, "3+": {}})
    dirty: Set[Tuple[str, str, str]] = field(default_factory=set)

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
    end_time_norm = end_time
    pk4 = (end_day, end_time_norm, barcode, step)
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

    if old_bucket is not None:
        key = (pn, step)
        prev = st.agg[old_bucket].get(key, 0)
        st.agg[old_bucket][key] = max(prev - 1, 0)
        st.dirty.add((old_bucket, pn, step))

    key = (pn, step)
    st.agg[new_bucket][key] = st.agg[new_bucket].get(key, 0) + 1
    st.dirty.add((new_bucket, pn, step))

    st.pair_bucket[pair] = new_bucket


# =========================
# 8) DB UPSERT
# =========================
def upsert_counts(engine: Engine, win: WindowKey, st: State) -> None:
    if not st.dirty:
        return

    prod_day = win.prod_day
    shift_type = win.shift_type
    now_ts = datetime.now(tz=KST)

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

        if to_update_only:
            upd_sql = text(f"""
                UPDATE {quote_ident(SAVE_SCHEMA)}.{quote_ident(table)}
                   SET {quote_ident("count")} = :cnt,
                       {quote_ident("updated_at")} = :updated_at
                 WHERE {quote_ident("prod_day")} = :prod_day
                   AND {quote_ident("shift_type")} = :shift_type
                   AND {quote_ident("pn")} = :pn
                   AND {quote_ident(bucket_col)} = :step
            """)
            payload = [{
                "prod_day": prod_day,
                "shift_type": shift_type,
                "pn": pn,
                "step": step,
                "cnt": str(cnt),
                "updated_at": now_ts,
            } for pn, step, cnt in to_update_only]

            with engine.begin() as conn:
                conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
                res = conn.execute(upd_sql, payload)
                total_updates += (res.rowcount or 0)

        if to_upsert:
            ins_sql = text(f"""
                INSERT INTO {quote_ident(SAVE_SCHEMA)}.{quote_ident(table)}
                ({quote_ident("prod_day")}, {quote_ident("shift_type")}, {quote_ident("pn")},
                 {quote_ident(bucket_col)}, {quote_ident("count")}, {quote_ident("updated_at")})
                VALUES (:prod_day, :shift_type, :pn, :step, :cnt, :updated_at)
                ON CONFLICT ({quote_ident("prod_day")}, {quote_ident("shift_type")}, {quote_ident("pn")}, {quote_ident(bucket_col)})
                DO UPDATE SET
                    {quote_ident("count")} = EXCLUDED.{quote_ident("count")},
                    {quote_ident("updated_at")} = EXCLUDED.{quote_ident("updated_at")}
            """)
            payload = [{
                "prod_day": prod_day,
                "shift_type": shift_type,
                "pn": pn,
                "step": step,
                "cnt": str(cnt),
                "updated_at": now_ts,
            } for pn, step, cnt in to_upsert]

            with engine.begin() as conn:
                conn.execute(text(f"SET work_mem = '{WORK_MEM}';"))
                conn.execute(ins_sql, payload)
                total_upserts += len(payload)

    st.dirty.clear()
    log("info", f"[UPSERT] window={win.prod_day}:{win.shift_type} upserted={total_upserts}, updated={total_updates}")


# =========================
# 9) Bootstrap + Incremental Loop
# =========================
def bootstrap(engine: Engine, win: WindowKey, st: State) -> None:
    log("info", f"[BOOTSTRAP] start window={win.prod_day}:{win.shift_type} ({win.start_dt} ~ {win.upper_dt})")

    pn_map = load_pn_map(engine)
    st.reset_for_new_window(win, pn_map)

    n = 0
    last_pk: Optional[Tuple[str, str, str]] = None

    for end_day, end_time, barcode, step in fetch_rows_bootstrap(engine, win):
        if barcode is None or step is None:
            continue
        apply_event(st, end_day, end_time, str(barcode), str(step))
        n += 1
        last_pk = (end_day, end_time, str(barcode))

    st.last_pk = last_pk
    for bucket in ("1", "2", "3+"):
        for (pn, step), _cnt in st.agg[bucket].items():
            st.dirty.add((bucket, pn, step))

    log("info", f"[BOOTSTRAP] done fetched={n} last_pk={st.last_pk}")


def incremental_step(engine: Engine, win: WindowKey, st: State) -> int:
    log("info", f"[LAST_PK] {st.last_pk}")

    total_new = 0
    while True:
        rows = fetch_rows_incremental(engine, win, st.last_pk, limit=FETCH_BATCH_LIMIT)
        if not rows:
            break

        for end_day, end_time, barcode, step in rows:
            if barcode is None or step is None:
                continue
            apply_event(st, end_day, end_time, str(barcode), str(step))
            total_new += 1

        last = rows[-1]
        st.last_pk = (last[0], last[1], str(last[2]))

        if len(rows) < FETCH_BATCH_LIMIT:
            break

    log("info", f"[FETCH] new_rows={total_new} last_pk={st.last_pk}")
    return total_new


def main() -> None:
    log("boot", "backend4 repeat-fail daemon starting")

    engine = connect_with_retry()
    ensure_schema_and_tables(engine)

    st = State()
    prev_win_key: Optional[Tuple[str, str]] = None

    while True:
        loop_t0 = time_mod.time()
        try:
            now = datetime.now(tz=KST)
            win = current_window(now)
            win_key = (win.prod_day, win.shift_type)

            if prev_win_key != win_key:
                log(
                    "info",
                    f"[WINDOW] changed => {win.prod_day}:{win.shift_type} "
                    f"(start={win.start_dt}, end={win.end_dt}, upper={win.upper_dt})"
                )
                bootstrap(engine, win, st)
                upsert_counts(engine, win, st)
                prev_win_key = win_key
            else:
                win = current_window(now)
                _ = incremental_step(engine, win, st)
                if st.dirty:
                    upsert_counts(engine, win, st)

            if len(st.seen_pk) > 2_000_000:
                log("down", f"seen_pk too large ({len(st.seen_pk)}). force bootstrap.")
                now2 = datetime.now(tz=KST)
                win2 = current_window(now2)
                bootstrap(engine, win2, st)
                upsert_counts(engine, win2, st)

        except (OperationalError, DBAPIError) as e:
            log("down", f"db error: {type(e).__name__}: {e} (reconnect in {DB_RETRY_INTERVAL_SEC}s)")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            engine = connect_with_retry()
            ensure_schema_and_tables(engine)

            win = current_window(datetime.now(tz=KST))
            bootstrap(engine, win, st)
            upsert_counts(engine, win, st)
            prev_win_key = (win.prod_day, win.shift_type)

        except Exception as e:
            log("error", f"unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_t0
        sleep_s = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_s > 0:
            log("sleep", f"loop sleep {sleep_s:.2f}s")
        time_mod.sleep(sleep_s)


if __name__ == "__main__":
    main()
