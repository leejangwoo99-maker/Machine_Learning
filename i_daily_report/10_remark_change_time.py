# -*- coding: utf-8 -*-
"""
backend10_remark_change_daemon.py

요구사항 반영:
1) dataframe 콘솔 출력 제외
2) 날짜는 [WINDOW] 기준 현재날짜/현재시각(KST) 자동 전환
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
    - last_pk는 메모리만 사용
    - 신규 row만 반영하여 station별 "마지막 remark + 마지막 end_time" 상태를 유지하며 이벤트 생성
13) 재실행 시 삭제/초기화 금지
    - bootstrap: 현재 윈도우(start~now) 전체 스캔하여 상태 복구
    - bootstrap 전에 이미 저장된 이벤트를 DB에서 읽어 seen_event 로딩 후 신규만 insert

변경점(요청 반영):
- station 대상 확장: Vision1, Vision2 + FCT1~FCT4 (총 6개)
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
# 0) 설정
# =========================
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

# ✅ station 6개로 확장
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
# 3) 윈도우 계산 (KST)
# =========================
@dataclass(frozen=True)
class Window:
    prod_day: str         # YYYYMMDD (night는 전날 D-DAY로 저장)
    shift_type: str       # 'day' or 'night'
    start_dt: datetime    # naive timestamp (KST local clock)
    end_dt: datetime      # naive timestamp (KST local clock) = now

def _to_naive_kst(dt: datetime) -> datetime:
    # Postgres timestamp(without tz)과 맞추기 위해 naive로 전달
    return dt.astimezone(KST).replace(tzinfo=None)

def _yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")

def calc_window(now_kst: datetime) -> Window:
    now_kst = now_kst.astimezone(KST)
    t = now_kst.time()
    today = now_kst.date()

    # day: 08:30:00 ~ 20:29:59
    if dtime(8, 30, 0) <= t <= dtime(20, 29, 59):
        prod_day = _yyyymmdd(today)
        start = datetime.combine(today, dtime(8, 30, 0), tzinfo=KST)
        return Window(
            prod_day=prod_day,
            shift_type="day",
            start_dt=_to_naive_kst(start),
            end_dt=_to_naive_kst(now_kst),
        )

    # night: 20:30:00 ~ next day 08:29:59
    # 20:30~23:59:59 => prod_day=today
    if dtime(20, 30, 0) <= t <= dtime(23, 59, 59):
        prod_day = _yyyymmdd(today)
        start = datetime.combine(today, dtime(20, 30, 0), tzinfo=KST)
        return Window(
            prod_day=prod_day,
            shift_type="night",
            start_dt=_to_naive_kst(start),
            end_dt=_to_naive_kst(now_kst),
        )

    # 00:00~08:29:59 => prod_day=전날(D-DAY)
    prev = today - timedelta(days=1)
    prod_day = _yyyymmdd(prev)
    start = datetime.combine(prev, dtime(20, 30, 0), tzinfo=KST)
    return Window(
        prod_day=prod_day,
        shift_type="night",
        start_dt=_to_naive_kst(start),
        end_dt=_to_naive_kst(now_kst),
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
        conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_i_daily_report_j_remark_change_day_daily_dedup" ON {day_fqn()} ({uniq_cols_sql});'))

        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {night_fqn()} ({cols_ddl});"))
        conn.execute(text(f'CREATE UNIQUE INDEX IF NOT EXISTS "ux_i_daily_report_j_remark_change_night_daily_dedup" ON {night_fqn()} ({uniq_cols_sql});'))

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

    # 안전: 결측 제거
    df = df.dropna(subset=["station", "remark", "end_day", "end_time", "end_ts"]).copy()
    df["remark"] = df["remark"].astype(str)
    df = df[df["remark"].isin(REMARKS)].copy()
    df = df.sort_values(["end_ts", "station"], ascending=True).reset_index(drop=True)
    return df

@dataclass
class LastCursor:
    last_ts: Optional[datetime] = None   # naive timestamp
    last_station: str = ""              # tie-breaker

def fetch_source_incremental(engine: Engine, start_dt: datetime, end_dt: datetime, cur: LastCursor) -> pd.DataFrame:
    # 첫 루프(커서 없음)는 bootstrap에서 처리함. 여기선 안전하게 start_dt~end_dt로 제한.
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
    # key = (prod_day, station, shift_type, at_time, from_remark, to_remark)
    sql = text(f"""
        SELECT prod_day, station, shift_type, at_time, from_remark, to_remark
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
    """)
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type})
    if df.empty:
        return set()

    keys = set()
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
# 8) 메인 루프
# =========================
def run():
    log("BOOT", "backend10 remark-change daemon starting")
    engine = connect_with_retry()
    ensure_schema_and_tables(engine)

    current_window_id: Optional[Tuple[str, str]] = None  # (prod_day, shift_type)
    cur = LastCursor()
    seen_pk: Set[Tuple[str, str, str]] = set()
    seen_event: Set[Tuple[str, str, str, str, str, str]] = set()
    state: Dict[str, StationState] = {s: StationState() for s in STATIONS}

    while True:
        try:
            now = datetime.now(KST)
            w = calc_window(now)
            window_id = (w.prod_day, w.shift_type)
            table_fqn = day_fqn() if w.shift_type == "day" else night_fqn()

            # 윈도우가 바뀌었거나 첫 시작이면 bootstrap
            if window_id != current_window_id:
                log("INFO", f"[WINDOW] changed => prod_day={w.prod_day} shift={w.shift_type} start={w.start_dt} end(now)={w.end_dt}")

                # 캐시/커서 리셋 (윈도우 단위)
                cur = LastCursor()
                seen_pk.clear()
                state = {s: StationState() for s in STATIONS}

                # (B) bootstrap 전에 기존 이벤트를 읽어 seen_event 로딩
                seen_event = load_existing_events(engine, table_fqn, w.prod_day, w.shift_type)
                log("INFO", f"[BOOTSTRAP] existing events loaded = {len(seen_event):,}")

                # bootstrap 스캔
                df = fetch_source_range(engine, w.start_dt, w.end_dt)
                log("INFO", f"[BOOTSTRAP] fetch rows = {len(df):,} (range {w.start_dt} ~ {w.end_dt})")

                # bootstrap 처리(순차)
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
                    log("INFO", "[LAST_PK] None (no source rows in window)")

                current_window_id = window_id
                time.sleep(SLEEP_SEC)
                continue

            # incremental loop
            if cur.last_ts is None:
                log("INFO", "[INCR] last_pk is None -> fallback bootstrap scan")
                current_window_id = None
                continue

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
            current_window_id = None
            time.sleep(SLEEP_SEC)

        except Exception as e:
            log("RETRY", f"Unhandled error: {type(e).__name__}: {e} -> sleep {SLEEP_SEC}s")
            time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    run()
