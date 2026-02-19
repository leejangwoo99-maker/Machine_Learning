# -*- coding: utf-8 -*-
"""
d4_afa_fail_wasted_time_factory_FIXED_20260130_DAY.py

✅ 테스트 고정 버전:
- prod_day = 20260130 (고정)
- shift    = day (고정)
- window   = 2026-01-30 08:30:00 ~ 2026-01-30 20:29:59 (KST, 고정)

기능:
- NG는 오로지 contents ILIKE '%제품 감지 NG%' 만 인정 (Python에서도 '제품 감지 NG' 포함만 NG로 확정)
- NG -> ON 페어링 + wasted_time 계산
- save: d1_machine_log.afa_fail_wasted_time (누적 insert, ON CONFLICT DO NOTHING)
- save PK/UNIQUE: (end_day, station, from_time, to_time)
- DB 접속 실패/끊김 시 무한 재시도(5초마다 [RETRY])
- 엔진 1개 고정(pool_size=1, max_overflow=0), work_mem cap
- ✅ Warm-start Backfill: 시작 시(또는 강제 재백필 옵션) window 전체를 1회 스캔 후 누락 채움
- 이후 루프는 last_pk 기반 증분

로그:
- [BOOT] 즉시
- [INFO] LAST_PK / BACKFILL / FETCH / PAIR / INS 단계별
"""

from __future__ import annotations

import os
import re
import time
import unicodedata
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from zoneinfo import ZoneInfo


# =========================
# 0) 고정 설정 (테스트용)
# =========================
KST = ZoneInfo("Asia/Seoul")

FIXED_PROD_DAY = "20260131"
FIXED_SHIFT = "night"
FIXED_WS = datetime(2026, 1, 31, 20, 30, 0, tzinfo=KST)
FIXED_WE = datetime(2026, 2, 1, 8, 29, 59, tzinfo=KST)

# (선택) 매 실행마다 backfill을 강제로 다시 하고 싶으면 1로
FORCE_BACKFILL_EVERY_BOOT = int(os.getenv("AFA_FORCE_BACKFILL_EVERY_BOOT", "1"))  # 테스트용 default=1


# =========================
# 1) DB / 테이블
# =========================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SCHEMA = "d1_machine_log"

SRC_TABLES = [
    ("FCT1_machine_log", "FCT1"),
    ("FCT2_machine_log", "FCT2"),
    ("FCT3_machine_log", "FCT3"),
    ("FCT4_machine_log", "FCT4"),
]

SAVE_TABLE = "afa_fail_wasted_time"

# 타겟 문구(이거만 NG 인정)
NG_PHRASE = "제품 감지 NG"
OFF_PHRASE = "제품 검사 투입요구 ON"
MANUAL_PHRASE = "Manual mode 전환"
AUTO_PHRASE = "Auto mode 전환"

# Loop / Retry
LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

# work_mem cap
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# Cursor overlap(초) - 증분에서 누락 방지
CURSOR_OVERLAP_SEC = int(os.getenv("AFA_CURSOR_OVERLAP_SEC", "3"))

# keepalive (선택)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

_ENGINE = None


# =========================
# 2) 로깅
# =========================
def log_boot(msg: str) -> None:
    print(f"[BOOT] {msg}", flush=True)

def log_info(msg: str) -> None:
    print(f"[INFO] {msg}", flush=True)

def log_retry(msg: str) -> None:
    print(f"[RETRY] {msg}", flush=True)

def log_error(msg: str) -> None:
    print(f"[ERROR] {msg}", flush=True)


# =========================
# 3) 텍스트 정규화/매칭
# =========================
_ZWSP_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")  # zero-width chars

def norm_text(s: object) -> str:
    if s is None:
        return ""
    t = str(s)
    t = unicodedata.normalize("NFKC", t)
    t = _ZWSP_RE.sub("", t)
    t = t.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t

def is_ng(c: str) -> bool:
    return NG_PHRASE in c  # 오로지 이 문구 포함만 NG

def is_off(c: str) -> bool:
    return OFF_PHRASE in c

def is_manual(c: str) -> bool:
    return MANUAL_PHRASE in c

def is_auto(c: str) -> bool:
    return AUTO_PHRASE in c


# =========================
# 4) DB 연결/재접속(엔진 1개 고정)
# =========================
def _masked_db() -> str:
    c = DB_CONFIG
    return f"postgresql://{c['user']}:***@{c['host']}:{c['port']}/{c['dbname']}"

def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, OperationalError):
        return True
    if isinstance(e, DBAPIError) and getattr(e, "connection_invalidated", False):
        return True
    msg = (str(e) or "").lower()
    keys = [
        "server closed the connection",
        "connection not open",
        "terminating connection",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
    ]
    return any(k in msg for k in keys)

def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None

def _build_engine():
    user = DB_CONFIG["user"]
    password = urllib.parse.quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    dbname = DB_CONFIG["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}?connect_timeout=5"

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args={
            "connect_timeout": 5,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "afa_fail_wasted_time_fixed_20260130_day",
        },
    )

def get_engine_blocking():
    global _ENGINE

    if _ENGINE is not None:
        while True:
            try:
                with _ENGINE.connect() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    conn.execute(text("SELECT 1"))
                return _ENGINE
            except Exception as e:
                log_retry(f"DB ping failed -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                time.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE = _build_engine()
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            log_info(f"DB connected | {_masked_db()} | pool_size=1 max_overflow=0 work_mem={WORK_MEM}")
            return _ENGINE
        except Exception as e:
            log_retry(f"DB connect failed | {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 5) 결과 테이블/UNIQUE 보장 (PK: 4컬럼)
# =========================
def init_save_table_blocking(engine):
    ddl_create = text(f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    CREATE TABLE IF NOT EXISTS {SCHEMA}.{SAVE_TABLE} (
        end_day       TEXT,
        station       TEXT,
        from_contents TEXT,
        from_time     TEXT,
        to_contents   TEXT,
        to_time       TEXT,
        wasted_time   NUMERIC(10,2),
        created_at    TIMESTAMP DEFAULT now()
    );
    """)
    ddl_unique = text(f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_afa_fail_wasted_time_pk
    ON {SCHEMA}.{SAVE_TABLE} (end_day, station, from_time, to_time);
    """)

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(ddl_create)
                conn.execute(ddl_unique)
            log_info("bootstrap OK: save table + UNIQUE INDEX(end_day,station,from_time,to_time) ensured")
            return
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"init_save_table conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"init_save_table failed | {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 6) station별 마지막 PK(to_ts) 읽기 (✅ prod_day 고정 필터)
# =========================
def read_last_pk_per_station(engine, prod_day: str) -> Dict[str, Optional[datetime]]:
    out: Dict[str, Optional[datetime]] = {st: None for _, st in SRC_TABLES}

    sql = text(f"""
        SELECT station,
               to_timestamp(end_day || ' ' || split_part(to_time, '.', 1), 'YYYYMMDD HH24:MI:SS') AS to_ts
        FROM {SCHEMA}.{SAVE_TABLE}
        WHERE end_day = :prod_day
          AND station IS NOT NULL
          AND to_time IS NOT NULL
        ORDER BY to_ts DESC
    """)

    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                rows = conn.execute(sql, {"prod_day": prod_day}).fetchall()

            seen = set()
            for station, to_ts in rows:
                if station in out and station not in seen and to_ts is not None:
                    if to_ts.tzinfo is None:
                        to_ts = to_ts.replace(tzinfo=KST)
                    out[station] = to_ts
                    seen.add(station)
                if len(seen) == len(out):
                    break
            return out

        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"read_last_pk conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"read_last_pk failed | {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 7) 로그 fetch
# =========================
def fetch_src_logs(
    engine,
    table: str,
    station: str,
    ws: datetime,
    we: datetime,
    cursor_ts: datetime,
    overlap_sec: int = CURSOR_OVERLAP_SEC,
) -> pd.DataFrame:
    cursor_eff = cursor_ts - timedelta(seconds=int(overlap_sec))

    params = {
        "ws": ws.astimezone(KST).replace(tzinfo=None),
        "we": we.astimezone(KST).replace(tzinfo=None),
        "cursor": cursor_eff.astimezone(KST).replace(tzinfo=None),
    }

    sql = text(f"""
        SELECT
            end_day,
            end_time,
            contents
        FROM {SCHEMA}."{table}"
        WHERE
            to_timestamp(end_day || ' ' || split_part(end_time, '.', 1), 'YYYYMMDD HH24:MI:SS')
                BETWEEN :ws AND :we
            AND to_timestamp(end_day || ' ' || split_part(end_time, '.', 1), 'YYYYMMDD HH24:MI:SS')
                >= :cursor
            AND (
                contents ILIKE '%제품 감지 NG%'
                OR contents ILIKE '%제품 검사 투입요구 ON%'
                OR contents ILIKE '%Manual mode 전환%'
                OR contents ILIKE '%Auto mode 전환%'
            )
        ORDER BY
            to_timestamp(end_day || ' ' || split_part(end_time, '.', 1), 'YYYYMMDD HH24:MI:SS') ASC
    """)

    while True:
        try:
            df = pd.read_sql(sql, engine, params=params)
            if df.empty:
                return pd.DataFrame(columns=["end_day", "end_time", "contents", "contents_norm", "station", "end_ts"])

            df["end_day"] = df["end_day"].astype(str)
            df["end_time"] = df["end_time"].astype(str)
            df["contents"] = df["contents"].astype(str)

            # source PK dedup: (end_day, end_time, contents)
            df = df.drop_duplicates(subset=["end_day", "end_time", "contents"], keep="last")

            df["station"] = station
            df["contents_norm"] = df["contents"].map(norm_text)

            end_ts = pd.to_datetime(
                df["end_day"] + " " + df["end_time"].str.split(".").str[0],
                format="%Y%m%d %H:%M:%S",
                errors="coerce",
            )
            df["end_ts"] = end_ts.dt.tz_localize(KST, nonexistent="shift_forward", ambiguous="NaT")
            df = df.dropna(subset=["end_ts"]).sort_values("end_ts").reset_index(drop=True)

            return df[["end_day", "end_time", "contents", "contents_norm", "station", "end_ts"]]

        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"fetch_src_logs conn error -> rebuild | {table}/{station} | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"fetch_src_logs failed | {table}/{station} | {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 8) 이벤트 페어링(stateful)
# =========================
@dataclass
class StationState:
    in_manual: bool = False
    pending_ng_ts: Optional[datetime] = None

def pair_events(df: pd.DataFrame, state: StationState) -> Tuple[pd.DataFrame, StationState]:
    rows = []

    for _, r in df.iterrows():
        c = r["contents_norm"]
        ts: datetime = r["end_ts"]

        if is_manual(c):
            state.in_manual = True
            continue
        if is_auto(c):
            state.in_manual = False
            continue

        if is_ng(c):
            if (not state.in_manual) and (state.pending_ng_ts is None):
                state.pending_ng_ts = ts
            continue

        if is_off(c):
            if state.pending_ng_ts is not None:
                from_ts = state.pending_ng_ts
                to_ts = ts
                end_day = from_ts.astimezone(KST).strftime("%Y%m%d")

                rows.append({
                    "end_day": end_day,
                    "station": r["station"],
                    "from_contents": NG_PHRASE,
                    "from_time": from_ts.astimezone(KST).strftime("%H:%M:%S.%f")[:-4],
                    "to_contents": OFF_PHRASE,
                    "to_time": to_ts.astimezone(KST).strftime("%H:%M:%S.%f")[:-4],
                    "wasted_time": round(abs((to_ts - from_ts).total_seconds()), 2),
                })
                state.pending_ng_ts = None

    out = pd.DataFrame(rows, columns=[
        "end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"
    ])
    return out, state


# =========================
# 9) INSERT (PK/UNIQUE: 4컬럼)
# =========================
def insert_rows(engine, df_pairs: pd.DataFrame) -> int:
    if df_pairs.empty:
        return 0

    ins = text(f"""
        INSERT INTO {SCHEMA}.{SAVE_TABLE}
            (end_day, station, from_contents, from_time, to_contents, to_time, wasted_time)
        VALUES
            (:end_day, :station, :from_contents, :from_time, :to_contents, :to_time, :wasted_time)
        ON CONFLICT (end_day, station, from_time, to_time)
        DO NOTHING
    """)

    rows = df_pairs.to_dict("records")

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(ins, rows)
            return len(rows)
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"insert conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
                time.sleep(DB_RETRY_INTERVAL_SEC)
                continue
            log_error(f"insert failed (non-conn) | {type(e).__name__}: {repr(e)}")
            return 0


# =========================
# 10) Warm-start Backfill (고정 window 전체)
# =========================
def run_backfill_for_window(engine, ws: datetime, we: datetime, states: Dict[str, StationState]) -> None:
    """
    고정 window(ws~we) 구간을 station별로 한번 전체 스캔(backfill)
    - cursor=ws, overlap=0
    - dedup은 DB PK/UNIQUE로 처리(ON CONFLICT DO NOTHING)
    """
    log_info(f"[BACKFILL] start window={ws:%Y-%m-%d %H:%M:%S}~{we:%Y-%m-%d %H:%M:%S}")

    total_fetched = 0
    total_pairs = 0
    total_ins = 0

    for table, station in SRC_TABLES:
        df = fetch_src_logs(engine, table, station, ws, we, cursor_ts=ws, overlap_sec=0)
        fetched = len(df)
        total_fetched += fetched
        log_info(f"[BACKFILL][FETCH] {station} cursor=ws -> rows={fetched}")

        if fetched == 0:
            continue

        pairs, states[station] = pair_events(df, states[station])
        pcount = len(pairs)
        total_pairs += pcount
        log_info(f"[BACKFILL][PAIR ] {station} pairs={pcount} (pending_ng={'Y' if states[station].pending_ng_ts else 'N'}, in_manual={'Y' if states[station].in_manual else 'N'})")

        ins = insert_rows(engine, pairs) if pcount > 0 else 0
        total_ins += ins
        log_info(f"[BACKFILL][INS  ] {station} attempted_insert={ins} (dedup by PK/UNIQUE)")

    log_info(f"[BACKFILL] done | fetched={total_fetched} | pairs={total_pairs} | attempted_insert={total_ins}")


# =========================
# 11) main (prod_day/shift 고정)
# =========================
def main():
    log_boot(f"backend afa_fail_wasted_time FIXED starting | prod_day={FIXED_PROD_DAY} shift={FIXED_SHIFT} (MP=1, loop=5s, warm-start backfill=ON)")
    log_info(f"DB = {_masked_db()}")
    log_info(f"work_mem cap = {WORK_MEM}")
    log_info(f"SRC = {SCHEMA}.FCT1~4_machine_log | PK(end_day,end_time,contents) assumed")
    log_info(f"SAVE = {SCHEMA}.{SAVE_TABLE} | PK/UNIQUE(end_day,station,from_time,to_time)")
    log_info(f"CURSOR_OVERLAP_SEC = {CURSOR_OVERLAP_SEC}")
    log_info(f"FIXED window = {FIXED_WS:%Y-%m-%d %H:%M:%S} ~ {FIXED_WE:%Y-%m-%d %H:%M:%S} (KST)")

    engine = get_engine_blocking()
    init_save_table_blocking(engine)

    # state는 window 단위로만 의미 있으니, 고정 window 테스트에서는 1회 초기화
    states: Dict[str, StationState] = {st: StationState() for _, st in SRC_TABLES}

    # ✅ 시작 시 backfill 1회 (테스트 default=항상 실행)
    if FORCE_BACKFILL_EVERY_BOOT:
        run_backfill_for_window(engine, FIXED_WS, FIXED_WE, states)

    while True:
        loop_t0 = time.perf_counter()

        try:
            engine = get_engine_blocking()

            # 1) 마지막 PK 읽기 (✅ 20260130만 기준)
            last_to_ts = read_last_pk_per_station(engine, FIXED_PROD_DAY)
            last_pk_str = ", ".join([f"{k}={(v.strftime('%Y-%m-%d %H:%M:%S') if v else 'None')}" for k, v in last_to_ts.items()])
            log_info(f"[LAST_PK] shift={FIXED_SHIFT} prod_day={FIXED_PROD_DAY} window={FIXED_WS:%Y-%m-%d %H:%M:%S}~{FIXED_WE:%Y-%m-%d %H:%M:%S} | {last_pk_str}")

            total_fetched = 0
            total_pairs = 0
            total_attempted = 0

            # 2) station별 증분 fetch/pair/insert
            for table, station in SRC_TABLES:
                cursor_base = last_to_ts.get(station) or FIXED_WS
                if cursor_base < FIXED_WS:
                    cursor_base = FIXED_WS
                if cursor_base > FIXED_WE:
                    # window 끝을 넘으면 더 볼 게 없음
                    log_info(f"[FETCH] {station} cursor>{FIXED_WE:%H:%M:%S} -> skip")
                    continue

                df = fetch_src_logs(engine, table, station, FIXED_WS, FIXED_WE, cursor_base, overlap_sec=CURSOR_OVERLAP_SEC)
                fetched = len(df)
                total_fetched += fetched
                log_info(f"[FETCH] {station} cursor={cursor_base:%Y-%m-%d %H:%M:%S} -> rows={fetched}")

                if fetched == 0:
                    continue

                pairs, states[station] = pair_events(df, states[station])
                pcount = len(pairs)
                total_pairs += pcount
                log_info(f"[PAIR ] {station} pairs={pcount} (pending_ng={'Y' if states[station].pending_ng_ts else 'N'}, in_manual={'Y' if states[station].in_manual else 'N'})")

                attempted = insert_rows(engine, pairs) if pcount > 0 else 0
                total_attempted += attempted
                log_info(f"[INS  ] {station} attempted_insert={attempted} (dedup by PK/UNIQUE)")

            log_info(f"[LOOP ] FIXED {FIXED_PROD_DAY} {FIXED_SHIFT} | fetched={total_fetched} | pairs={total_pairs} | attempted_insert={total_attempted}")

        except KeyboardInterrupt:
            log_info("KeyboardInterrupt -> exit")
            break
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"loop conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                time.sleep(DB_RETRY_INTERVAL_SEC)
                engine = get_engine_blocking()
            else:
                log_error(f"{type(e).__name__}: {repr(e)}")

        elapsed = time.perf_counter() - loop_t0
        sleep_sec = LOOP_INTERVAL_SEC - elapsed
        if sleep_sec > 0:
            time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
