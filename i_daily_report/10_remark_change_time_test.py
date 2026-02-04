# -*- coding: utf-8 -*-
"""
backend10_remark_change_daemon_TEST_20260128_DAY.py

테스트 고정 버전:
- prod_day  = 20260128 (고정)
- shift     = day      (고정)
- window    = 2026-01-28 08:30:00 ~ 2026-01-28 20:29:59 (KST, 고정)

변경점(요청 반영):
- station 대상 확장: Vision1, Vision2 + FCT1~FCT4 (총 6개)

요구사항 반영:
1) dataframe 콘솔 출력 제외
3) 멀티프로세스=1
4) 무한루프 5초
5) DB 접속 실패 시 무한 재시도(블로킹)
6) 접속 후 중간 끊김도 무한 재접속
7) 연결 1개 고정(pool 최소화)
8) work_mem 폭증 방지: env(PG_WORK_MEM) 읽고 없으면 4MB, 연결 시 SET work_mem
9) 증분 조건: (end_day, station, end_time) 기반 (내부 비교는 end_ts + station 정렬/커서)
10) seen_pk: set[(end_day, station, end_time)] 중복 방지 캐시
11) 실행 즉시 [BOOT], DB 미접속 시 [RETRY] 5초마다
12) 단계별 INFO: 마지막 PK / 신규 fetch / insert
13) 재실행 시 삭제/초기화 금지
    - bootstrap: 고정 윈도우(start~end) 전체 스캔하여 상태 복구
    - bootstrap 전에 이미 저장된 이벤트를 DB에서 읽어 seen_event 로딩 후 신규만 insert
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from datetime import datetime, date, time as dtime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Optional, Tuple, Set

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

KST = ZoneInfo("Asia/Seoul")

# =========================
# 0) 테스트 고정 파라미터
# =========================
PROD_DAY_FIXED = "20260128"   # YYYYMMDD (고정)
SHIFT_FIXED = "day"          # 'day' 고정

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

SAVE_SCHEMA = "i_daily_report"
T_DAY   = "j_remark_change_day_daily"
T_NIGHT = "j_remark_change_night_daily"

# ✅ station 대상 확장 (총 6개)
STATIONS = ("Vision1", "Vision2", "FCT1", "FCT2", "FCT3", "FCT4")

REMARKS  = ("PD", "Non-PD")

SLEEP_SEC = 5
WORK_MEM_DEFAULT = "4MB"


# =========================
# 1) 로깅
# =========================
def log(level: str, msg: str) -> None:
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}")


# =========================
# 2) DB 엔진/세션 설정
# =========================
def get_work_mem() -> str:
    v = os.getenv("PG_WORK_MEM", "").strip()
    return v if v else WORK_MEM_DEFAULT

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
    work_mem = get_work_mem()
    while True:
        try:
            engine = make_engine()
            with engine.begin() as conn:
                conn.execute(text(f"SET work_mem = '{work_mem}'"))
            log("INFO", f"DB connected (work_mem={work_mem})")
            return engine
        except Exception as e:
            log("RETRY", f"DB connect failed: {type(e).__name__}: {e} -> sleep {SLEEP_SEC}s")
            time.sleep(SLEEP_SEC)

def src_fqn() -> str:
    return f'"{SRC_SCHEMA}"."{SRC_TABLE}"'

def day_fqn() -> str:
    return f'"{SAVE_SCHEMA}"."{T_DAY}"'

def night_fqn() -> str:
    return f'"{SAVE_SCHEMA}"."{T_NIGHT}"'


# =========================
# 3) 고정 윈도우 (KST)
# =========================
def _to_naive_kst(dt: datetime) -> datetime:
    return dt.astimezone(KST).replace(tzinfo=None)

def prod_day_to_date(prod_day: str) -> date:
    return datetime.strptime(prod_day, "%Y%m%d").date()

@dataclass(frozen=True)
class Window:
    prod_day: str
    shift_type: str
    start_dt: datetime  # naive (KST local clock)
    end_dt: datetime    # naive (KST local clock) fixed

def calc_fixed_window() -> Window:
    d = prod_day_to_date(PROD_DAY_FIXED)
    start = datetime.combine(d, dtime(8, 30, 0), tzinfo=KST)
    end   = datetime.combine(d, dtime(20, 29, 59), tzinfo=KST)
    return Window(
        prod_day=PROD_DAY_FIXED,
        shift_type="day",
        start_dt=_to_naive_kst(start),
        end_dt=_to_naive_kst(end),
    )


# =========================
# 4) 저장 테이블 보장 + INSERT(중복 방지)
# =========================
COLS = [
    "prod_day",
    "station",
    "shift_type",
    "at_time",
    "from_remark",
    "to_remark",
    "updated_at",
]
UNIQUE_COLS = ["prod_day", "station", "shift_type", "at_time", "from_remark", "to_remark"]

def ensure_schema_and_tables(engine: Engine) -> None:
    unique_day = f'ux_{SAVE_SCHEMA}_{T_DAY}_dedup'
    unique_night = f'ux_{SAVE_SCHEMA}_{T_NIGHT}_dedup'
    uniq_cols_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])

    cols_ddl = """
        "prod_day"    text NOT NULL,
        "station"     text NOT NULL,
        "shift_type"  text NOT NULL,
        "at_time"     text NOT NULL,
        "from_remark" text NOT NULL,
        "to_remark"   text NOT NULL,
        "updated_at"  timestamptz NOT NULL
    """

    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SAVE_SCHEMA}";'))

        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {day_fqn()} ({cols_ddl});"))
        conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{unique_day}" ON {day_fqn()} ({uniq_cols_sql});'))

        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {night_fqn()} ({cols_ddl});"))
        conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "{unique_night}" ON {night_fqn()} ({uniq_cols_sql});'))

def insert_events(engine: Engine, table_fqn: str, rows: list[dict]) -> int:
    if not rows:
        return 0

    cols_sql = ", ".join([f'"{c}"' for c in COLS])
    values_sql = ", ".join([f":{c}" for c in COLS])
    conflict_sql = ", ".join([f'"{c}"' for c in UNIQUE_COLS])

    ins = text(f"""
        INSERT INTO {table_fqn} ({cols_sql})
        VALUES ({values_sql})
        ON CONFLICT ({conflict_sql}) DO NOTHING
    """)

    with engine.begin() as conn:
        conn.execute(ins, rows)
    return len(rows)


# =========================
# 5) 소스 fetch (bootstrap / incremental)
# =========================
def fetch_source_range(engine: Engine, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    sql = text(f"""
        SELECT
            station,
            remark,
            end_day,
            end_time,
            to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')::timestamp AS end_ts
        FROM {src_fqn()}
        WHERE station = ANY(:stations)
          AND remark  = ANY(:remarks)
          AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')::timestamp
              BETWEEN :start_dt AND :end_dt
        ORDER BY end_ts, station
    """)
    with engine.begin() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "stations": list(STATIONS),
                "remarks": list(REMARKS),
                "start_dt": start_dt,
                "end_dt": end_dt,
            },
        )
    if df.empty:
        return df

    df = df.dropna(subset=["station", "remark", "end_day", "end_time", "end_ts"]).copy()
    df["remark"] = df["remark"].astype(str)
    df = df[df["remark"].isin(REMARKS)].copy()
    df = df.sort_values(["end_ts", "station"], ascending=True).reset_index(drop=True)
    return df

@dataclass
class LastCursor:
    last_ts: Optional[datetime] = None   # naive timestamp
    last_station: str = ""               # tie-breaker

def fetch_source_incremental(engine: Engine, start_dt: datetime, end_dt: datetime, cur: LastCursor) -> pd.DataFrame:
    if cur.last_ts is None:
        return fetch_source_range(engine, start_dt, end_dt)

    sql = text(f"""
        SELECT
            station,
            remark,
            end_day,
            end_time,
            to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')::timestamp AS end_ts
        FROM {src_fqn()}
        WHERE station = ANY(:stations)
          AND remark  = ANY(:remarks)
          AND to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')::timestamp
              BETWEEN :start_dt AND :end_dt
          AND (
                to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')::timestamp > :last_ts
                OR (
                    to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS')::timestamp = :last_ts
                    AND station > :last_station
                )
          )
        ORDER BY end_ts, station
    """)
    with engine.begin() as conn:
        df = pd.read_sql(
            sql,
            conn,
            params={
                "stations": list(STATIONS),
                "remarks": list(REMARKS),
                "start_dt": start_dt,
                "end_dt": end_dt,
                "last_ts": cur.last_ts,
                "last_station": cur.last_station,
            },
        )
    if df.empty:
        return df

    df = df.dropna(subset=["station", "remark", "end_day", "end_time", "end_ts"]).copy()
    df["remark"] = df["remark"].astype(str)
    df = df[df["remark"].isin(REMARKS)].copy()
    df = df.sort_values(["end_ts", "station"], ascending=True).reset_index(drop=True)
    return df


# =========================
# 6) 기존 이벤트 로딩 (bootstrap 전 seen_event 세팅)
# =========================
def load_existing_events(engine: Engine, table_fqn: str, prod_day: str, shift_type: str) -> Set[Tuple[str, str, str, str, str, str]]:
    sql = text(f"""
        SELECT prod_day, station, shift_type, at_time, from_remark, to_remark
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
    """)
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type})

    keys: Set[Tuple[str, str, str, str, str, str]] = set()
    if df.empty:
        return keys

    for r in df.itertuples(index=False):
        keys.add((r.prod_day, r.station, r.shift_type, r.at_time, r.from_remark, r.to_remark))
    return keys


# =========================
# 7) 상태/이벤트 생성
# =========================
@dataclass
class StationState:
    last_remark: Optional[str] = None
    last_end_time: Optional[str] = None

def event_key(prod_day: str, station: str, shift_type: str, at_time: str, from_r: str, to_r: str):
    return (prod_day, station, shift_type, at_time, from_r, to_r)

def source_pk(end_day: str, station: str, end_time: str):
    return (end_day, station, end_time)


# =========================
# 8) 메인 루프 (고정 윈도우)
# =========================
def run():
    log("BOOT", f"backend10 remark-change TEST starting (prod_day={PROD_DAY_FIXED}, shift={SHIFT_FIXED}, stations={len(STATIONS)})")

    engine = connect_with_retry()
    ensure_schema_and_tables(engine)

    w = calc_fixed_window()
    table_fqn = day_fqn()  # day 고정
    log("INFO", f"[WINDOW][FIXED] prod_day={w.prod_day} shift={w.shift_type} start={w.start_dt} end={w.end_dt}")

    cur = LastCursor()
    seen_pk: Set[Tuple[str, str, str]] = set()
    state: Dict[str, StationState] = {s: StationState() for s in STATIONS}

    # bootstrap
    seen_event = load_existing_events(engine, table_fqn, w.prod_day, w.shift_type)
    log("INFO", f"[BOOTSTRAP] existing events loaded = {len(seen_event):,}")

    df = fetch_source_range(engine, w.start_dt, w.end_dt)
    log("INFO", f"[BOOTSTRAP] fetch rows = {len(df):,} (range {w.start_dt} ~ {w.end_dt})")

    new_rows: list[dict] = []
    updated_at = pd.Timestamp.now(tz=KST)

    for r in df.itertuples(index=False):
        spk = source_pk(r.end_day, r.station, r.end_time)
        if spk in seen_pk:
            continue
        seen_pk.add(spk)

        st = state.get(r.station) or StationState()

        if st.last_remark is None:
            st.last_remark = r.remark
            st.last_end_time = r.end_time
            state[r.station] = st
        else:
            if r.remark != st.last_remark:
                at_time = st.last_end_time
                if at_time:
                    ek = event_key(w.prod_day, r.station, w.shift_type, at_time, st.last_remark, r.remark)
                    if ek not in seen_event:
                        seen_event.add(ek)
                        new_rows.append(
                            {
                                "prod_day": w.prod_day,
                                "station": r.station,
                                "shift_type": w.shift_type,
                                "at_time": at_time,
                                "from_remark": st.last_remark,
                                "to_remark": r.remark,
                                "updated_at": updated_at,
                            }
                        )

            st.last_remark = r.remark
            st.last_end_time = r.end_time
            state[r.station] = st

        cur.last_ts = r.end_ts
        cur.last_station = r.station

    if new_rows:
        insert_events(engine, table_fqn, new_rows)
    log("INFO", f"[BOOTSTRAP] inserted new events = {len(new_rows):,}")
    if cur.last_ts is not None:
        log("INFO", f"[LAST_PK] (end_ts={cur.last_ts}, station={cur.last_station})")
    else:
        log("INFO", "[LAST_PK] None (no source rows in fixed window)")

    # incremental loop
    while True:
        try:
            log("INFO", f"[LAST_PK] (end_ts={cur.last_ts}, station={cur.last_station})")
            log("INFO", f"[FETCH] range ({w.start_dt} ~ {w.end_dt}) after last_pk")

            df_new = fetch_source_incremental(engine, w.start_dt, w.end_dt, cur)
            if df_new.empty:
                log("INFO", "[FETCH] new rows = 0")
                time.sleep(SLEEP_SEC)
                continue

            log("INFO", f"[FETCH] new rows = {len(df_new):,}")

            new_events: list[dict] = []
            updated_at = pd.Timestamp.now(tz=KST)

            for r in df_new.itertuples(index=False):
                spk = source_pk(r.end_day, r.station, r.end_time)
                if spk in seen_pk:
                    cur.last_ts = r.end_ts
                    cur.last_station = r.station
                    continue
                seen_pk.add(spk)

                st = state.get(r.station) or StationState()

                if st.last_remark is None:
                    st.last_remark = r.remark
                    st.last_end_time = r.end_time
                    state[r.station] = st
                else:
                    if r.remark != st.last_remark:
                        at_time = st.last_end_time
                        if at_time:
                            ek = event_key(w.prod_day, r.station, w.shift_type, at_time, st.last_remark, r.remark)
                            if ek not in seen_event:
                                seen_event.add(ek)
                                new_events.append(
                                    {
                                        "prod_day": w.prod_day,
                                        "station": r.station,
                                        "shift_type": w.shift_type,
                                        "at_time": at_time,
                                        "from_remark": st.last_remark,
                                        "to_remark": r.remark,
                                        "updated_at": updated_at,
                                    }
                                )

                    st.last_remark = r.remark
                    st.last_end_time = r.end_time
                    state[r.station] = st

                cur.last_ts = r.end_ts
                cur.last_station = r.station

            if new_events:
                insert_events(engine, table_fqn, new_events)
                log("INFO", f"[INSERT] events inserted = {len(new_events):,}")
            else:
                log("INFO", "[INSERT] events inserted = 0")

            time.sleep(SLEEP_SEC)

        except (OperationalError, DBAPIError) as e:
            log("RETRY", f"DB error: {type(e).__name__}: {e} -> reconnect")
            try:
                engine.dispose(close=True)
            except Exception:
                pass
            engine = connect_with_retry()
            ensure_schema_and_tables(engine)
            log("INFO", "[RECOVER] reconnect OK -> re-bootstrap fixed window")
            return run()  # 테스트용 재진입

        except Exception as e:
            log("RETRY", f"Unhandled error: {type(e).__name__}: {e} -> sleep {SLEEP_SEC}s")
            time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    run()
