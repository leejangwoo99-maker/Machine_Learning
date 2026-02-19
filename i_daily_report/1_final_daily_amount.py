# -*- coding: utf-8 -*-
"""
backend1_daily_final_amount_daemon.py

요구사항 반영
1) dataframe 콘솔 출력 제외
2) [WINDOW] 기준 현재 날짜/현재 시각으로 자동 전환
3) 멀티프로세스 = 1개(단일 프로세스)
4) 무한 루프, 인터벌 5초
5) DB 서버 접속 실패 시 무한 재시도(블로킹)
6) DB 서버 접속 후 끊김 시 무한 재시도
7) 상시 연결 1개 고정(풀 최소화)
8) work_mem 폭증 방지(세션마다 SET)
9) “PK 이후 데이터” 증분 조건: (end_day, end_time, barcode_information)
10) 실행 즉시 [BOOT] 로그, DB 미연결 시 [RETRY] 5초마다 출력
11) “마지막 PK / 신규 fetch / insert(or upsert)” 단계마다 [INFO]
12) 재실행(프로세스 재시작) 시 PK 무시 + 윈도우 초기화 후 처음부터 처리(=full rebuild)
13) 주간 리셋: 14:22:00, 20:22:00 (±30s 윈도우 내 최초 1회)
14) 야간 리셋: 02:22:00, 08:22:00 (±30s 윈도우 내 최초 1회)
15~16) 결과 저장 테이블 4개(스키마 i_daily_report)
  - a_day_daily_final_amount
  - a_station_day_daily_final_amount
  - a_night_daily_final_amount
  - a_station_night_daily_final_amount

✅ 추가 사양(누락 없이 반영)
- station 후보는 Vision1/2 "포함" + FCT1~4 "포함" (합집합)
- barcode_information 중복 dedup 규칙은 범위를 분리:
  * FCT군(FCT1~4) 내부에서만: barcode별 최신 1건(end_ts 최신) 유지
  * Vision군(Vision1~2) 내부에서만: barcode별 최신 1건(end_ts 최신) 유지
  (FCT군과 Vision군은 서로 dedup 영향을 주지 않음)
- 저장 규칙:
  * overall 테이블(a_day / a_night): "최종" 성격으로 Vision군만 집계 저장
  * station 테이블(a_station_day / a_station_night): FCT군 + Vision군 모두 station별 집계 저장

중요 구현 포인트
- “증분 fetch”는 DB에서 last_pk 이후만 가져오지만,
  집계는 in-memory(바코드당 최신 1건) 상태를 유지하면서 갱신
- 리셋/재시작/shift 변경 시: window_start~now 전체를 다시 로드하여 in-memory 재구성 + DELETE+INSERT
- 일반 루프 시: last_pk 이후만 fetch하여 in-memory 갱신 + UPSERT

[추가] 데몬 헬스 로그 저장
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: "1_log" (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 순서: end_day, end_time, info, contents
"""

from __future__ import annotations

import os
import time as time_mod
import math
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, Tuple, Optional, List, Any, Iterable

import pandas as pd
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / 기본 설정
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

FCT_STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
VISION_STATIONS = ["Vision1", "Vision2"]
STATIONS = FCT_STATIONS + VISION_STATIONS

GOODORBAD_VALUE = "GoodFile"

# =========================
# DB 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

TESTLOG_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
TESTLOG_TABLE  = "fct_vision_testlog_txt_processing_history"

REMARK_SCHEMA = "g_production_film"
REMARK_TABLE  = "remark_info"

SAVE_SCHEMA = "i_daily_report"
T_DAY_OVERALL   = "a_day_daily_final_amount"
T_DAY_STATION   = "a_station_day_daily_final_amount"
T_NIGHT_OVERALL = "a_night_daily_final_amount"
T_NIGHT_STATION = "a_station_night_daily_final_amount"

# =========================
# [추가] DB 로그 저장 대상
# =========================
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "1_log"  # 숫자로 시작하므로 식별자 quoting 필수

# 메모리 버퍼(일시적 DB 다운 시 로그 유실 최소화)
PENDING_LOG_ROWS: List[Dict[str, str]] = []

# =========================
# 시간대(한 줄 라벨)
# =========================
DAY_BANDS = [
    ("A시간대(08:30:00 ~ 10:29:59)", 0,  2*3600 - 1),
    ("B시간대(10:30:00 ~ 12:29:59)", 2*3600,  4*3600 - 1),
    ("C시간대(12:30:00 ~ 14:29:59)", 4*3600,  6*3600 - 1),
    ("D시간대(14:30:00 ~ 16:29:59)", 6*3600,  8*3600 - 1),
    ("E시간대(16:30:00 ~ 18:29:59)", 8*3600, 10*3600 - 1),
    ("F시간대(18:30:00 ~ 20:29:59)",10*3600, 12*3600 - 1),
]
NIGHT_BANDS = [
    ("A'시간대(20:30:00 ~ 22:29:59)", 0,  2*3600 - 1),
    ("B'시간대(22:30:00 ~ 00:29:59)", 2*3600,  4*3600 - 1),
    ("C'시간대(00:30:00 ~ 02:29:59)", 4*3600,  6*3600 - 1),
    ("D'시간대(02:30:00 ~ 04:29:59)", 6*3600,  8*3600 - 1),
    ("E'시간대(04:30:00 ~ 06:29:59)", 8*3600, 10*3600 - 1),
    ("F'시간대(06:30:00 ~ 08:29:59)",10*3600, 12*3600 - 1),
]

# =========================
# SQL 식별자 quoting
# =========================
def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

# =========================
# DB 엔진
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

def ensure_db_ready(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("SELECT 1;"))
        conn.execute(text("SET work_mem = :wm;"), {"wm": WORK_MEM})

def with_work_mem(conn):
    conn.execute(text("SET work_mem = :wm;"), {"wm": WORK_MEM})

# =========================
# [추가] 헬스 로그 테이블 보장/적재
# =========================
def ensure_log_table(engine: Engine):
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {qident(LOG_SCHEMA)};"))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {qident(LOG_SCHEMA)}.{qident(LOG_TABLE)} (
                end_day  text,
                end_time text,
                info     text,
                contents text
            );
        """))

def _mk_log_row(info: str, contents: str, now: Optional[datetime] = None) -> Dict[str, str]:
    ts = now or datetime.now(KST)
    return {
        "end_day": ts.strftime("%Y%m%d"),
        "end_time": ts.strftime("%H:%M:%S"),
        "info": (info or "").strip().lower(),   # 반드시 소문자
        "contents": str(contents or ""),
    }

def flush_pending_logs(engine: Optional[Engine]):
    if engine is None:
        return
    if not PENDING_LOG_ROWS:
        return
    try:
        ensure_log_table(engine)
        # 사양: end_day, end_time, info, contents 순서로 dataframe화하여 저장
        df = pd.DataFrame(PENDING_LOG_ROWS, columns=["end_day", "end_time", "info", "contents"])
        if df.empty:
            return

        sql = text(f"""
            INSERT INTO {qident(LOG_SCHEMA)}.{qident(LOG_TABLE)}
            (end_day, end_time, info, contents)
            VALUES (:end_day, :end_time, :info, :contents)
        """)
        records = df.to_dict(orient="records")
        with engine.begin() as conn:
            with_work_mem(conn)
            conn.execute(sql, records)

        PENDING_LOG_ROWS.clear()
    except Exception:
        # 로그 저장 실패는 데몬 로직에 영향 주지 않음
        pass

def log(level: str, msg: str, engine: Optional[Engine] = None):
    """
    콘솔 출력 + DB 로그 버퍼 적재(가능 시 즉시 flush)
    info: error/down/sleep/... 처럼 소문자 저장
    """
    now = datetime.now(KST)
    print(f"{now.strftime('%Y-%m-%d %H:%M:%S')} [{level}] {msg}", flush=True)

    # level을 info 컬럼으로 사용 (소문자 강제)
    PENDING_LOG_ROWS.append(_mk_log_row(level, msg, now=now))

    # 엔진이 살아있으면 즉시 flush 시도
    flush_pending_logs(engine)

# =========================
# SHIFT WINDOW
# =========================
@dataclass(frozen=True)
class ShiftWindow:
    prod_day: str
    shift_type: str   # day/night
    start_dt: datetime
    end_dt: datetime  # now (inclusive)

def current_shift_window(now: datetime) -> ShiftWindow:
    """
    규칙:
    - day: 오늘 08:30:00 ~ 20:29:59 (now가 이 범위면 day)
    - night: 20:30:00 ~ 익일 08:29:59
      - now < 08:30 이면 prod_day=어제, shift=night
      - now >= 20:30 이면 prod_day=오늘, shift=night
    window_end = now (inclusive)
    """
    assert now.tzinfo is not None
    today = now.date()

    day_start = datetime(today.year, today.month, today.day, 8, 30, 0, tzinfo=KST)
    day_end   = datetime(today.year, today.month, today.day, 20, 29, 59, tzinfo=KST)

    if day_start <= now <= day_end:
        prod_day = today.strftime("%Y%m%d")
        return ShiftWindow(prod_day, "day", day_start, now)

    if now >= datetime(today.year, today.month, today.day, 20, 30, 0, tzinfo=KST):
        prod_day = today.strftime("%Y%m%d")
        night_start = datetime(today.year, today.month, today.day, 20, 30, 0, tzinfo=KST)
        return ShiftWindow(prod_day, "night", night_start, now)

    prev = today - timedelta(days=1)
    prod_day = prev.strftime("%Y%m%d")
    night_start = datetime(prev.year, prev.month, prev.day, 20, 30, 0, tzinfo=KST)
    return ShiftWindow(prod_day, "night", night_start, now)

# =========================
# 리셋 트리거(±30s)
# =========================
RESET_TOL_SEC = 30
DAY_RESET_TIMES = [dtime(14, 22, 0), dtime(20, 22, 0)]
NIGHT_RESET_TIMES = [dtime(2, 22, 0), dtime(8, 22, 0)]

def reset_tag(window: ShiftWindow, now: datetime) -> Optional[str]:
    targets = DAY_RESET_TIMES if window.shift_type == "day" else NIGHT_RESET_TIMES
    for tt in targets:
        target_dt = datetime(now.year, now.month, now.day, tt.hour, tt.minute, tt.second, tzinfo=KST)
        if abs((now - target_dt).total_seconds()) <= RESET_TOL_SEC:
            return f"{window.prod_day}_{window.shift_type}_{tt.strftime('%H%M%S')}"
    return None

# =========================
# PASS/FAIL 정규화
# =========================
def normalize_passfail_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.upper()
    is_pass = x.isin(["PASS", "P", "OK", "TRUE", "1"]) | x.str.contains("PASS", na=False)
    is_fail = x.isin(["FAIL", "F", "NG", "FALSE", "0"]) | x.str.contains("FAIL", na=False)
    out = pd.Series(pd.NA, index=s.index, dtype="object")
    out.loc[is_pass] = "PASS"
    out.loc[is_fail] = "FAIL"
    return out

def passfail_cell(pass_cnt: int, fail_cnt: int) -> str:
    return f"PASS: {int(pass_cnt)}, FAIL: {int(fail_cnt)}"

# =========================
# time_band 부여
# =========================
def assign_band(end_ts: pd.Series, window: ShiftWindow) -> pd.Series:
    delta_sec = (end_ts - window.start_dt).dt.total_seconds()
    bands = DAY_BANDS if window.shift_type == "day" else NIGHT_BANDS
    out = pd.Series(pd.NA, index=end_ts.index, dtype="object")
    for label, lo, hi in bands:
        mask = (delta_sec >= lo) & (delta_sec <= hi)
        out.loc[mask] = label
    return out

# =========================
# remark_info 로드 (키->(pn,remark))
# =========================
def load_remark_map(engine: Engine) -> Dict[str, Tuple[str, str]]:
    q = text(f"""
        SELECT barcode_information, pn, remark
        FROM {REMARK_SCHEMA}.{REMARK_TABLE}
    """)
    with engine.begin() as conn:
        with_work_mem(conn)
        m = pd.read_sql(q, conn)

    m["barcode_information"] = m["barcode_information"].astype(str).str.strip()
    m["pn"] = m["pn"].astype(str).str.strip()
    m["remark"] = m["remark"].astype(str).str.strip()

    mp: Dict[str, Tuple[str, str]] = {}
    for _, r in m.iterrows():
        k = r["barcode_information"]
        if k and k != "nan":
            mp[k] = (r["pn"], r["remark"])
    return mp

def barcode_key18(s: str) -> Optional[str]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if len(s) < 18:
        return None
    return s[17]

# =========================
# 결과 컬럼 감지
# =========================
RESULT_CANDIDATES = ["result", "passfail", "pass_fail", "test_result", "pf", "judge", "status"]

def detect_result_col(engine: Engine) -> str:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
        ORDER BY ordinal_position
    """)
    with engine.begin() as conn:
        with_work_mem(conn)
        cols = [r[0] for r in conn.execute(q, {"s": TESTLOG_SCHEMA, "t": TESTLOG_TABLE}).fetchall()]

    lower = {c.lower(): c for c in cols}
    for cand in RESULT_CANDIDATES:
        if cand.lower() in lower:
            return lower[cand.lower()]

    raise RuntimeError(f"PASS/FAIL 컬럼을 찾지 못함. candidates={RESULT_CANDIDATES}, table_cols={cols}")

# =========================
# DB fetch (full / incremental)
# =========================
@dataclass
class LastPK:
    end_day: str
    end_time: str
    barcode: str

def fetch_rows(
    engine: Engine,
    window: ShiftWindow,
    result_col: str,
    last_pk: Optional[LastPK],
) -> pd.DataFrame:
    end_days = sorted({window.start_dt.strftime("%Y%m%d"), window.end_dt.strftime("%Y%m%d")})
    rc = qident(result_col)

    sql = f"""
    WITH base AS (
        SELECT
            station,
            barcode_information,
            end_day::text AS end_day,
            regexp_replace(end_time::text, '\\\\..*$', '') AS end_time,
            {rc} AS result_raw
        FROM {TESTLOG_SCHEMA}.{TESTLOG_TABLE}
        WHERE station = ANY(:stations)
          AND goodorbad = :goodorbad
          AND end_day = ANY(:end_days)
          AND barcode_information IS NOT NULL
          AND barcode_information <> ''
    ),
    enriched AS (
        SELECT
            station,
            barcode_information,
            end_day,
            end_time,
            (to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS') AT TIME ZONE 'Asia/Seoul') AS end_ts,
            result_raw
        FROM base
        WHERE end_time IS NOT NULL AND end_time <> ''
    )
    SELECT station, barcode_information, end_day, end_time, end_ts, result_raw
    FROM enriched
    WHERE end_ts >= :win_start
      AND end_ts <= :win_end
      AND (
            :lp_end_day IS NULL
            OR (end_day, end_time, barcode_information) > (:lp_end_day, :lp_end_time, :lp_barcode)
          )
    ORDER BY end_day, end_time, barcode_information
    """

    params = {
        "stations": STATIONS,
        "goodorbad": GOODORBAD_VALUE,
        "end_days": end_days,
        "win_start": window.start_dt.replace(tzinfo=None),
        "win_end": window.end_dt.replace(tzinfo=None),
        "lp_end_day": None if last_pk is None else last_pk.end_day,
        "lp_end_time": None if last_pk is None else last_pk.end_time,
        "lp_barcode": None if last_pk is None else last_pk.barcode,
    }

    with engine.begin() as conn:
        with_work_mem(conn)
        df = pd.read_sql(text(sql), conn, params=params)

    if not df.empty:
        # DB 타입/드라이버 케이스 대응
        ts = pd.to_datetime(df["end_ts"], errors="coerce")
        if getattr(ts.dt, "tz", None) is None:
            df["end_ts"] = ts.dt.tz_localize(KST, ambiguous="NaT", nonexistent="NaT")
        else:
            df["end_ts"] = ts.dt.tz_convert(KST)
    return df

def compute_last_pk_from_df(df: pd.DataFrame) -> Optional[LastPK]:
    if df is None or df.empty:
        return None
    last = df.iloc[-1]
    return LastPK(end_day=str(last["end_day"]), end_time=str(last["end_time"]), barcode=str(last["barcode_information"]))

# =========================
# In-memory 최신 바코드 1건 유지 구조 (dedup 분리)
# =========================
@dataclass
class LatestRow:
    station: str
    barcode: str
    end_day: str
    end_time: str
    end_ts: datetime
    passfail: str

def _is_fct_station(st: str) -> bool:
    return st in FCT_STATIONS

def _is_vision_station(st: str) -> bool:
    return st in VISION_STATIONS

def update_latest_maps(
    latest_fct: Dict[str, LatestRow],
    latest_vis: Dict[str, LatestRow],
    df_new: pd.DataFrame,
) -> Tuple[int, int]:
    if df_new is None or df_new.empty:
        return 0, 0

    df_new = df_new.copy()
    df_new["passfail_norm"] = normalize_passfail_series(df_new["result_raw"])
    df_new = df_new.dropna(subset=["passfail_norm", "end_ts"])

    upd_fct = 0
    upd_vis = 0

    for r in df_new.itertuples(index=False):
        station = str(r.station)
        barcode = str(r.barcode_information).strip()
        if not barcode:
            continue

        row = LatestRow(
            station=station,
            barcode=barcode,
            end_day=str(r.end_day),
            end_time=str(r.end_time),
            end_ts=r.end_ts,
            passfail=str(r.passfail_norm),
        )

        if _is_fct_station(station):
            prev = latest_fct.get(barcode)
            if prev is None or row.end_ts > prev.end_ts:
                latest_fct[barcode] = row
                upd_fct += 1
        elif _is_vision_station(station):
            prev = latest_vis.get(barcode)
            if prev is None or row.end_ts > prev.end_ts:
                latest_vis[barcode] = row
                upd_vis += 1

    return upd_fct, upd_vis

# =========================
# 집계 DF 생성
# =========================
def build_overall_df_from_latest(
    latest_rows: Iterable[LatestRow],
    window: ShiftWindow,
    remark_map: Dict[str, Tuple[str, str]],
    updated_at: datetime,
) -> pd.DataFrame:
    bands = DAY_BANDS if window.shift_type == "day" else NIGHT_BANDS
    band_cols = [b[0] for b in bands]
    group_overall = ["prod_day", "shift_type", "pn", "remark"]

    rows = []
    for v in latest_rows:
        k = barcode_key18(v.barcode)
        mapped = remark_map.get(k) if k else None
        if not mapped:
            continue
        pn, remark = mapped
        rows.append({
            "barcode_information": v.barcode,
            "end_ts": v.end_ts,
            "passfail_norm": v.passfail,
            "pn": pn,
            "remark": remark,
            "prod_day": window.prod_day,
            "shift_type": window.shift_type,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        cols = ["prod_day", "shift_type", "pn", "remark"] + band_cols + ["합계", "updated_at"]
        return pd.DataFrame(columns=cols)

    df["time_band"] = assign_band(df["end_ts"], window)
    df = df.dropna(subset=["time_band"])

    g = df.groupby(group_overall + ["time_band", "passfail_norm"]).size().rename("cnt").reset_index()
    piv = g.pivot_table(index=group_overall + ["time_band"], columns="passfail_norm",
                        values="cnt", aggfunc="sum", fill_value=0).reset_index()
    if "PASS" not in piv.columns: piv["PASS"] = 0
    if "FAIL" not in piv.columns: piv["FAIL"] = 0
    piv["cell"] = piv.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)

    wide = piv.pivot_table(index=group_overall, columns="time_band", values="cell", aggfunc="first").reset_index()
    for c in band_cols:
        if c not in wide.columns:
            wide[c] = passfail_cell(0, 0)
    wide = wide[group_overall + band_cols]

    tot = (
        df.groupby(group_overall + ["passfail_norm"]).size().rename("cnt").reset_index()
          .pivot_table(index=group_overall, columns="passfail_norm", values="cnt", aggfunc="sum", fill_value=0)
          .reset_index()
    )
    if "PASS" not in tot.columns: tot["PASS"] = 0
    if "FAIL" not in tot.columns: tot["FAIL"] = 0
    tot["합계"] = tot.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)
    tot = tot[group_overall + ["합계"]]

    out = wide.merge(tot, on=group_overall, how="left")
    out["updated_at"] = updated_at
    out = out[["prod_day","shift_type","pn","remark"] + band_cols + ["합계","updated_at"]]
    out = out.sort_values(["prod_day","shift_type","pn","remark"], kind="mergesort").reset_index(drop=True)
    return out

def build_station_df_from_latest(
    latest_rows: Iterable[LatestRow],
    window: ShiftWindow,
    remark_map: Dict[str, Tuple[str, str]],
    updated_at: datetime,
) -> pd.DataFrame:
    bands = DAY_BANDS if window.shift_type == "day" else NIGHT_BANDS
    band_cols = [b[0] for b in bands]
    group_station = ["prod_day", "shift_type", "station", "pn", "remark"]

    rows = []
    for v in latest_rows:
        k = barcode_key18(v.barcode)
        mapped = remark_map.get(k) if k else None
        if not mapped:
            continue
        pn, remark = mapped
        rows.append({
            "station": v.station,
            "barcode_information": v.barcode,
            "end_ts": v.end_ts,
            "passfail_norm": v.passfail,
            "pn": pn,
            "remark": remark,
            "prod_day": window.prod_day,
            "shift_type": window.shift_type,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        cols = ["prod_day", "shift_type", "station", "pn", "remark"] + band_cols + ["합계", "updated_at"]
        return pd.DataFrame(columns=cols)

    df["time_band"] = assign_band(df["end_ts"], window)
    df = df.dropna(subset=["time_band"])

    g2 = df.groupby(group_station + ["time_band", "passfail_norm"]).size().rename("cnt").reset_index()
    piv2 = g2.pivot_table(index=group_station + ["time_band"], columns="passfail_norm",
                          values="cnt", aggfunc="sum", fill_value=0).reset_index()
    if "PASS" not in piv2.columns: piv2["PASS"] = 0
    if "FAIL" not in piv2.columns: piv2["FAIL"] = 0
    piv2["cell"] = piv2.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)

    wide2 = piv2.pivot_table(index=group_station, columns="time_band", values="cell", aggfunc="first").reset_index()
    for c in band_cols:
        if c not in wide2.columns:
            wide2[c] = passfail_cell(0, 0)
    wide2 = wide2[group_station + band_cols]

    tot2 = (
        df.groupby(group_station + ["passfail_norm"]).size().rename("cnt").reset_index()
          .pivot_table(index=group_station, columns="passfail_norm", values="cnt", aggfunc="sum", fill_value=0)
          .reset_index()
    )
    if "PASS" not in tot2.columns: tot2["PASS"] = 0
    if "FAIL" not in tot2.columns: tot2["FAIL"] = 0
    tot2["합계"] = tot2.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)
    tot2 = tot2[group_station + ["합계"]]

    out = wide2.merge(tot2, on=group_station, how="left")
    out["updated_at"] = updated_at
    out = out[["prod_day","shift_type","station","pn","remark"] + band_cols + ["합계","updated_at"]]

    station_order = FCT_STATIONS + VISION_STATIONS
    out["station"] = pd.Categorical(out["station"], categories=station_order, ordered=True)
    out = out.sort_values(["prod_day","shift_type","pn","station","remark"], kind="mergesort").reset_index(drop=True)
    return out

# =========================
# 저장(테이블 생성 + UPSERT/DELETE+INSERT)
# =========================
def ensure_schema(engine: Engine, schema: str):
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def ensure_table(engine: Engine, schema: str, table: str, columns: List[str], key_cols: List[str]):
    ddl_cols = []
    for c in columns:
        ddl_cols.append(f'{qident(c)} {"timestamptz" if c=="updated_at" else "text"}')
    ddl_cols_sql = ",\n  ".join(ddl_cols)
    key_sql = ", ".join([qident(c) for c in key_cols])

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      {ddl_cols_sql},
      UNIQUE ({key_sql})
    );
    """
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(create_sql))

def _to_py(v: Any):
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v

def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> int:
    if df is None or df.empty:
        return 0

    cols = df.columns.tolist()
    col_sql = ", ".join([qident(c) for c in cols])

    bind_names = [f"p{i}" for i in range(len(cols))]
    val_sql = ", ".join([f":{bn}" for bn in bind_names])

    key_sql = ", ".join([qident(c) for c in key_cols])

    non_keys = [c for c in cols if c not in key_cols]
    set_sql = ", ".join([f'{qident(c)} = EXCLUDED.{qident(c)}' for c in non_keys])

    sql = f"""
    INSERT INTO {schema}.{table} ({col_sql})
    VALUES ({val_sql})
    ON CONFLICT ({key_sql})
    DO UPDATE SET {set_sql};
    """

    records = []
    for row in df.itertuples(index=False, name=None):
        records.append({bn: _to_py(v) for bn, v in zip(bind_names, row)})

    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(sql), records)

    return len(records)

def delete_shift_rows(engine: Engine, schema: str, table: str, prod_day: str, shift_type: str):
    sql = text(f"""
        DELETE FROM {schema}.{table}
        WHERE prod_day = :pd AND shift_type = :sh
    """)
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(sql, {"pd": prod_day, "sh": shift_type})

def insert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    cols = df.columns.tolist()
    col_sql = ", ".join([qident(c) for c in cols])

    bind_names = [f"p{i}" for i in range(len(cols))]
    val_sql = ", ".join([f":{bn}" for bn in bind_names])

    sql = f"""
    INSERT INTO {schema}.{table} ({col_sql})
    VALUES ({val_sql})
    """

    records = []
    for row in df.itertuples(index=False, name=None):
        records.append({bn: _to_py(v) for bn, v in zip(bind_names, row)})

    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(sql), records)
    return len(records)

# =========================
# 메인 루프
# =========================
def main():
    # 부팅 로그(아직 DB 엔진 없으므로 버퍼만 적재)
    log("boot", "backend1 daily final amount daemon starting")
    log("info", f"stations={STATIONS} (FCT+Vision), dedup scope = FCT-group / Vision-group separated")

    engine: Optional[Engine] = None

    latest_fct: Dict[str, LatestRow] = {}
    latest_vis: Dict[str, LatestRow] = {}
    last_pk: Optional[LastPK] = None

    current_ctx: Optional[Tuple[str, str]] = None
    last_reset_fired: Optional[str] = None

    remark_map: Dict[str, Tuple[str, str]] = {}
    result_col: Optional[str] = None
    last_remark_refresh: Optional[datetime] = None

    while True:
        try:
            engine = make_engine()
            ensure_db_ready(engine)
            ensure_log_table(engine)
            flush_pending_logs(engine)
            log("info", f"db connected (work_mem={WORK_MEM}, pool_size=1)", engine)
            break
        except Exception as e:
            log("retry", f"db connect failed: {type(e).__name__}: {e}", engine=None)
            log("sleep", f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect", engine=None)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            assert engine is not None
            result_col = detect_result_col(engine)
            remark_map = load_remark_map(engine)
            last_remark_refresh = datetime.now(KST)

            ensure_schema(engine, SAVE_SCHEMA)
            ensure_log_table(engine)
            flush_pending_logs(engine)
            log("info", f"bootstrap ok (result_col={result_col})", engine)
            break
        except Exception as e:
            log("retry", f"bootstrap failed: {type(e).__name__}: {e}", engine)
            log("sleep", f"sleep {DB_RETRY_INTERVAL_SEC}s before bootstrap retry", engine)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        loop_t0 = time_mod.time()
        now = datetime.now(KST)

        try:
            assert engine is not None
            assert result_col is not None

            flush_pending_logs(engine)

            window = current_shift_window(now)
            ctx = (window.prod_day, window.shift_type)

            tag = reset_tag(window, now)
            need_full_rebuild = False

            if current_ctx is None or ctx != current_ctx:
                need_full_rebuild = True
                last_reset_fired = None
                log("info", f"[window] prod_day={window.prod_day} shift={window.shift_type} {window.start_dt} -> {window.end_dt} (ctx change)", engine)

            if tag is not None and tag != last_reset_fired:
                need_full_rebuild = True
                last_reset_fired = tag
                log("info", f"[reset] trigger={tag} (full rebuild & delete+insert)", engine)

            if last_remark_refresh is None or (now - last_remark_refresh).total_seconds() >= 600:
                remark_map = load_remark_map(engine)
                last_remark_refresh = now
                log("info", "remark_map refreshed", engine)

            if window.shift_type == "day":
                t_overall = T_DAY_OVERALL
                t_station = T_DAY_STATION
            else:
                t_overall = T_NIGHT_OVERALL
                t_station = T_NIGHT_STATION

            KEY_OVERALL = ["prod_day", "shift_type", "pn"]
            KEY_STATION = ["prod_day", "shift_type", "station", "pn"]

            if need_full_rebuild:
                latest_fct = {}
                latest_vis = {}
                last_pk = None

                log("info", "[last_pk] (reset) last_pk=None -> full fetch", engine)

                df_all = fetch_rows(engine, window, result_col, last_pk=None)
                log("info", f"[fetch] full rows={len(df_all)}", engine)

                upd_fct, upd_vis = update_latest_maps(latest_fct, latest_vis, df_all)
                last_pk = compute_last_pk_from_df(df_all)

                log(
                    "info",
                    f"[build] latest_fct={len(latest_fct)}(upd={upd_fct}) latest_vis={len(latest_vis)}(upd={upd_vis}) "
                    f"last_pk={None if last_pk is None else (last_pk.end_day, last_pk.end_time, last_pk.barcode)}",
                    engine
                )

                updated_at = now
                overall_df = build_overall_df_from_latest(latest_vis.values(), window, remark_map, updated_at)
                station_rows = list(latest_fct.values()) + list(latest_vis.values())
                station_df = build_station_df_from_latest(station_rows, window, remark_map, updated_at)

                ensure_table(engine, SAVE_SCHEMA, t_overall, overall_df.columns.tolist(), KEY_OVERALL)
                ensure_table(engine, SAVE_SCHEMA, t_station, station_df.columns.tolist(), KEY_STATION)

                log("info", f"[insert] delete+insert into {SAVE_SCHEMA}.{t_overall} / {SAVE_SCHEMA}.{t_station}", engine)
                delete_shift_rows(engine, SAVE_SCHEMA, t_overall, window.prod_day, window.shift_type)
                delete_shift_rows(engine, SAVE_SCHEMA, t_station, window.prod_day, window.shift_type)

                ins1 = insert_df(engine, SAVE_SCHEMA, t_overall, overall_df)
                ins2 = insert_df(engine, SAVE_SCHEMA, t_station, station_df)

                log("info", f"[insert] inserted overall(vision)={ins1}, station(fct+vision)={ins2}", engine)
                current_ctx = ctx

            else:
                log("info", f"[last_pk] last_pk={None if last_pk is None else (last_pk.end_day, last_pk.end_time, last_pk.barcode)}", engine)

                df_new = fetch_rows(engine, window, result_col, last_pk=last_pk)
                log("info", f"[fetch] new rows={len(df_new)}", engine)

                if not df_new.empty:
                    upd_fct, upd_vis = update_latest_maps(latest_fct, latest_vis, df_new)

                    last_pk_new = compute_last_pk_from_df(df_new)
                    if last_pk_new is not None:
                        last_pk = last_pk_new

                    log(
                        "info",
                        f"[build] latest_fct={len(latest_fct)}(upd={upd_fct}) latest_vis={len(latest_vis)}(upd={upd_vis}) "
                        f"last_pk={(last_pk.end_day, last_pk.end_time, last_pk.barcode) if last_pk else None}",
                        engine
                    )

                    updated_at = now
                    overall_df = build_overall_df_from_latest(latest_vis.values(), window, remark_map, updated_at)
                    station_rows = list(latest_fct.values()) + list(latest_vis.values())
                    station_df = build_station_df_from_latest(station_rows, window, remark_map, updated_at)

                    ensure_table(engine, SAVE_SCHEMA, t_overall, overall_df.columns.tolist(), KEY_OVERALL)
                    ensure_table(engine, SAVE_SCHEMA, t_station, station_df.columns.tolist(), KEY_STATION)

                    log("info", f"[upsert] saving to {SAVE_SCHEMA}.{t_overall} / {SAVE_SCHEMA}.{t_station}", engine)
                    up1 = upsert_df(engine, SAVE_SCHEMA, t_overall, overall_df, KEY_OVERALL)
                    up2 = upsert_df(engine, SAVE_SCHEMA, t_station, station_df, KEY_STATION)
                    log("info", f"[upsert] upserted overall(vision)={up1}, station(fct+vision)={up2}", engine)

                current_ctx = ctx

        except (OperationalError, DBAPIError) as e:
            log("down", f"db error: {type(e).__name__}: {e}", engine=None)
            while True:
                try:
                    engine = make_engine()
                    ensure_db_ready(engine)
                    ensure_log_table(engine)
                    flush_pending_logs(engine)
                    log("info", f"db reconnected (work_mem={WORK_MEM})", engine)

                    result_col = detect_result_col(engine)
                    remark_map = load_remark_map(engine)
                    last_remark_refresh = datetime.now(KST)
                    log("info", f"re-bootstrap ok (result_col={result_col})", engine)
                    break
                except Exception as e2:
                    log("retry", f"db reconnect failed: {type(e2).__name__}: {e2}", engine=None)
                    log("sleep", f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect retry", engine=None)
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)

        except Exception as e:
            log("error", f"unhandled error: {type(e).__name__}: {e}", engine)

        elapsed = time_mod.time() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        log("sleep", f"loop sleep {sleep_sec:.3f}s", engine)
        time_mod.sleep(sleep_sec)

if __name__ == "__main__":
    main()
