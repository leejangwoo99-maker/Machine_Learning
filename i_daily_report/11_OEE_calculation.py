# -*- coding: utf-8 -*-
"""
oee_daemon.py
- OEE calc daemon based on Jupyter Cells 1~16 (final)
- 5-thread parallel fetch
- window auto switch (KST)
- incremental remark_change (PK: prod_day, station, at_time) + seen_pk cache (last 5)
- planned_stop full rescan each loop (can be updated)
- non_time / quality full rescan each loop (prod_day row keeps updating)
- final_amount full rescan each loop (base remark fallback)
- No dataframe print (logs only)
- CREATE IF NOT EXISTS + ENSURE UNIQUE INDEX + UPSERT to:
  - i_daily_report.k_total_oee_{day|night}_daily   (UNIQUE: prod_day)
  - i_daily_report.k_line_oee_{day|night}_daily    (UNIQUE: prod_day, line)   line in {left,right}
  - i_daily_report.k_station_oee_{day|night}_daily (UNIQUE: prod_day, station)
"""

from __future__ import annotations

import os
import re
import math
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple
from collections import deque

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError
from concurrent.futures import ThreadPoolExecutor

# -----------------------------
# Config
# -----------------------------
KST = ZoneInfo("Asia/Seoul")

WINDOW_SECONDS = 43200
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"]
REMARKS  = ["PD", "Non-PD"]

SAVE_SCHEMA = "i_daily_report"

# input tables
T_REMARK_CHANGE_DAY   = "j_remark_change_day_daily"
T_REMARK_CHANGE_NIGHT = "j_remark_change_night_daily"

T_PLANNED_STOP_DAY    = "i_planned_stop_time_day_daily"
T_PLANNED_STOP_NIGHT  = "i_planned_stop_time_night_daily"

T_NON_TIME_DAY        = "i_non_time_day_daily"
T_NON_TIME_NIGHT      = "i_non_time_night_daily"

T_QUALITY_DAY         = "b_station_day_daily_percentage"
T_QUALITY_NIGHT       = "b_station_night_daily_percentage"

T_FINAL_AMT_DAY       = "a_station_day_daily_final_amount"
T_FINAL_AMT_NIGHT     = "a_station_night_daily_final_amount"

# ideal ct tables
IDEAL_SCHEMA_VISION = "e1_FCT_ct"
IDEAL_TABLE_VISION  = "fct_whole_op_ct"

IDEAL_SCHEMA_FCT = "e1_FCT_ct"
IDEAL_TABLE_FCT  = "fct_op_ct"

# output tables
T_TOTAL_DAY   = "k_total_oee_day_daily"
T_TOTAL_NIGHT = "k_total_oee_night_daily"
T_LINE_DAY    = "k_line_oee_day_daily"
T_LINE_NIGHT  = "k_line_oee_night_daily"
T_ST_DAY      = "k_station_oee_day_daily"
T_ST_NIGHT    = "k_station_oee_night_daily"

SLEEP_SEC = 5

# work_mem
DEFAULT_WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# -----------------------------
# Logging
# -----------------------------
def _ts() -> str:
    return f"{datetime.now(tz=KST):%Y-%m-%d %H:%M:%S}"

def log_boot(msg: str) -> None:
    print(f"[{_ts()}] [BOOT] {msg}")

def log_info(msg: str) -> None:
    print(f"[{_ts()}] [INFO] {msg}")

def log_warn(msg: str) -> None:
    print(f"[{_ts()}] [WARN] {msg}")

def log_retry(msg: str) -> None:
    print(f"[{_ts()}] [RETRY] {msg}")

# -----------------------------
# Engine
# -----------------------------
def make_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    eng = create_engine(
        url,
        pool_size=5,         # ✅ 5 threads => 5 conns
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            # keepalive (psycopg2)
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        }
    )

    @event.listens_for(eng, "connect")
    def _on_connect(dbapi_conn, _conn_record):
        # set work_mem once per DBAPI connection
        try:
            cur = dbapi_conn.cursor()
            cur.execute(f"SET work_mem = '{DEFAULT_WORK_MEM}';")
            cur.close()
            dbapi_conn.commit()
        except Exception:
            pass

    return eng

def connect_blocking() -> Engine:
    while True:
        try:
            eng = make_engine()
            with eng.begin() as conn:
                conn.exec_driver_sql("SELECT 1;")
            log_info(f"DB engine OK (pool_size=5, work_mem={DEFAULT_WORK_MEM})")
            return eng
        except Exception as e:
            log_retry(f"DB connect failed: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)

# -----------------------------
# Time / window
# -----------------------------
def now_kst() -> datetime:
    return datetime.now(tz=KST)

def yyyymmdd(d: date) -> str:
    return f"{d:%Y%m%d}"

def current_window(now: datetime) -> Tuple[str, str, datetime, datetime]:
    """
    Returns (prod_day, shift_type, window_start, window_end)
    half-open [start, end)
    day   = [08:30, 20:30)
    night = [20:30, next 08:30)
    """
    t = now.timetz().replace(tzinfo=None)

    day_start = time(8, 30, 0)
    day_end   = time(20, 30, 0)

    if day_start <= t < day_end:
        shift = "day"
        pd = yyyymmdd(now.date())
        start = datetime.combine(now.date(), day_start, tzinfo=KST)
        end   = datetime.combine(now.date(), day_end,   tzinfo=KST)
    else:
        shift = "night"
        if t >= day_end:
            pd_date = now.date()
            start = datetime.combine(pd_date, day_end, tzinfo=KST)
            end   = datetime.combine(pd_date + timedelta(days=1), day_start, tzinfo=KST)
        else:
            pd_date = now.date() - timedelta(days=1)
            start = datetime.combine(pd_date, day_end, tzinfo=KST)
            end   = datetime.combine(pd_date + timedelta(days=1), day_start, tzinfo=KST)
        pd = yyyymmdd(pd_date)

    dur = int((end - start).total_seconds())
    if dur != WINDOW_SECONDS:
        raise ValueError(f"window seconds mismatch: {dur} != {WINDOW_SECONDS}")
    return pd, shift, start, end

# -----------------------------
# Interval utils (half-open)
# -----------------------------
@dataclass(frozen=True)
class Interval:
    start: datetime
    end: datetime  # exclusive

def merge_intervals(intervals: List[Interval]) -> List[Interval]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: x.start)
    merged = [intervals[0]]
    for cur in intervals[1:]:
        last = merged[-1]
        if cur.start <= last.end:
            merged[-1] = Interval(last.start, max(last.end, cur.end))
        else:
            merged.append(cur)
    return [iv for iv in merged if iv.end > iv.start]

def overlap_seconds(a: Interval, b: Interval) -> int:
    s = max(a.start, b.start)
    e = min(a.end, b.end)
    if e <= s:
        return 0
    return int((e - s).total_seconds())

def total_seconds(intervals: List[Interval]) -> int:
    return sum(int((iv.end - iv.start).total_seconds()) for iv in intervals)

# -----------------------------
# Parsers
# -----------------------------
PASS_RE = re.compile(r"PASS\s*[:=]\s*(\d+)", re.IGNORECASE)
FAIL_RE = re.compile(r"FAIL\s*[:=]\s*(\d+)", re.IGNORECASE)

def parse_korean_duration_to_sec(v) -> int:
    if v is None:
        return 0
    s = str(v).strip()
    if s == "" or s.lower() in ("nan","none","null"):
        return 0
    h = m = sec = 0
    mh = re.search(r"(\d+)\s*시간", s)
    mm = re.search(r"(\d+)\s*분", s)
    ms = re.search(r"(\d+)\s*초", s)
    if mh: h = int(mh.group(1))
    if mm: m = int(mm.group(1))
    if ms: sec = int(ms.group(1))
    return h*3600 + m*60 + sec

def parse_quality_text(v) -> Tuple[int,int,float]:
    if v is None:
        return 0, 0, np.nan
    s = str(v)
    mp = re.search(r"PASS\s*:\s*(\d+)", s, re.IGNORECASE)
    mt = re.search(r"total\s*:\s*(\d+)", s, re.IGNORECASE)
    mpct = re.search(r"PASS_pct\s*:\s*([0-9]+(?:\.[0-9]+)?)", s, re.IGNORECASE)
    p = int(mp.group(1)) if mp else 0
    t = int(mt.group(1)) if mt else 0
    q = float(mpct.group(1))/100.0 if mpct else (p/t if t>0 else np.nan)
    return p, t, q

def _safe_div(num, den):
    den = float(den)
    if np.isnan(den) or den <= 0:
        return np.nan
    return float(num) / den

def to_pct_str(x):
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return None
    return f"{x*100:.2f}%"

# time parser (HH:MM:SS string guaranteed)
def parse_hms(v: Any) -> time:
    s = str(v).strip()
    if "." in s:
        s = s.split(".", 1)[0].strip()
    hh, mm, ss = s.split(":")
    return time(int(hh), int(mm), int(ss))

def clean_remark(v) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() in ("nan","null","none"):
        return None
    if s.upper() == "PD":
        return "PD"
    if s.replace(" ", "").replace("_","").replace("-","").lower() in ("nonpd","non-pd","non_pd"):
        return "Non-PD"
    return s

# -----------------------------
# PK caches (recent 5)
# -----------------------------
class SeenPK:
    def __init__(self, maxlen: int = 5):
        self.maxlen = maxlen
        self.q = deque()   # type: deque[str]
        self.s = set()     # type: set[str]

    def add(self, key: str) -> None:
        if key in self.s:
            return
        self.q.append(key)
        self.s.add(key)
        while len(self.q) > self.maxlen:
            old = self.q.popleft()
            self.s.discard(old)

    def has(self, key: str) -> bool:
        return key in self.s

# -----------------------------
# Column introspection
# -----------------------------
def get_columns(engine: Engine, schema: str, table: str) -> List[str]:
    sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
        ORDER BY ordinal_position
    """)
    with engine.begin() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table}).fetchall()
    return [r[0] for r in rows]

def pick_col(cols: List[str], candidates: List[str], required=True) -> Optional[str]:
    for c in candidates:
        if c in cols:
            return c
    if required:
        raise ValueError(f"Missing columns, tried={candidates}, have={cols}")
    return None

# -----------------------------
# Data Fetch
# -----------------------------
def fqn(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'

def load_remark_changes_incremental(
    engine: Engine,
    table_fqn: str,
    prod_day: str,
    shift_type: str,
    stations: List[str],
    last_at_time_by_station: Dict[str, str],
    seen_pk: SeenPK,
) -> pd.DataFrame:
    """
    Incremental: fetch at_time > last_at_time[station]
    (string compare ok, HH:MM:SS)
    """
    conds = []
    params: Dict[str, Any] = {"prod_day": prod_day, "shift_type": shift_type, "stations": stations}
    for i, st in enumerate(stations):
        last_t = last_at_time_by_station.get(st)
        if last_t:
            conds.append(f"(station = :st{i} AND at_time > :t{i})")
            params[f"st{i}"] = st
            params[f"t{i}"] = last_t
        else:
            conds.append(f"(station = :st{i})")
            params[f"st{i}"] = st

    where_extra = " OR ".join(conds)
    sql = text(f"""
        SELECT prod_day, shift_type, station, at_time, from_remark, to_remark
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
          AND station = ANY(:stations)
          AND ({where_extra})
        ORDER BY station, at_time
    """)
    with engine.begin() as conn:
        df = pd.read_sql(sql, conn, params=params)

    if df.empty:
        return df

    out_rows = []
    for _, r in df.iterrows():
        key = f"{r['prod_day']}|{r['station']}|{r['at_time']}"
        if seen_pk.has(key):
            continue
        seen_pk.add(key)
        out_rows.append(r)

    if not out_rows:
        return df.iloc[0:0].copy()

    df2 = pd.DataFrame(out_rows)

    for st in df2["station"].unique().tolist():
        s = df2[df2["station"] == st].sort_values("at_time")
        last_at_time_by_station[st] = str(s.iloc[-1]["at_time"])

    return df2.reset_index(drop=True)

def load_remark_changes_full(engine: Engine, table_fqn: str, prod_day: str, shift_type: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT prod_day, shift_type, station, at_time, from_remark, to_remark
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
          AND station = ANY(:stations)
        ORDER BY station, at_time
    """)
    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type, "stations": STATIONS})

def load_planned_stops_full(engine: Engine, table_fqn: str, prod_day: str, shift_type: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT *
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
    """)
    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type})

def load_non_time_row(engine: Engine, table_fqn: str, prod_day: str, shift_type: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT *
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
    """)
    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type})

def load_quality_row(engine: Engine, table_fqn: str, prod_day: str, shift_type: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT *
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
    """)
    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type})

def load_final_amount_full(engine: Engine, table_fqn: str, prod_day: str, shift_type: str) -> pd.DataFrame:
    sql = text(f"""
        SELECT prod_day, shift_type, station, remark, pn, "합계"
        FROM {table_fqn}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
          AND station = ANY(:stations)
    """)
    with engine.begin() as conn:
        return pd.read_sql(sql, conn, params={"prod_day": prod_day, "shift_type": shift_type, "stations": STATIONS})

# -----------------------------
# Ideal CT load (once per window)
# -----------------------------
SIDE_TO_STATION = {"left": "Vision1", "right": "Vision2"}

def load_ideal_ct(engine: Engine) -> pd.DataFrame:
    cols_vision = get_columns(engine, IDEAL_SCHEMA_VISION, IDEAL_TABLE_VISION)
    cols_fct    = get_columns(engine, IDEAL_SCHEMA_FCT, IDEAL_TABLE_FCT)

    vis_col_side   = pick_col(cols_vision, ["station"])
    vis_col_remark = pick_col(cols_vision, ["remark"])
    vis_col_ct     = pick_col(cols_vision, ["ct_eq"])

    sql_vision = text(f"""
        SELECT {vis_col_side}   AS side,
               {vis_col_remark} AS remark,
               MIN({vis_col_ct}) AS ideal_ct_sec
        FROM {fqn(IDEAL_SCHEMA_VISION, IDEAL_TABLE_VISION)}
        WHERE {vis_col_side} = ANY(:sides)
          AND {vis_col_remark} = ANY(:remarks)
        GROUP BY {vis_col_side}, {vis_col_remark}
    """)
    with engine.begin() as conn:
        df_vision = pd.read_sql(sql_vision, conn, params={"sides": ["left","right"], "remarks": REMARKS})

    df_vision["side"] = df_vision["side"].astype(str).str.strip().str.lower()
    df_vision["station"] = df_vision["side"].map(SIDE_TO_STATION)
    df_vision = df_vision.dropna(subset=["station"])[["station","remark","ideal_ct_sec"]].copy()

    if "del_out_op_ct_av" not in cols_fct:
        raise ValueError(f"Missing del_out_op_ct_av in {IDEAL_SCHEMA_FCT}.{IDEAL_TABLE_FCT}. cols={cols_fct}")

    fct_col_station = pick_col(cols_fct, ["station"])
    fct_col_remark  = pick_col(cols_fct, ["remark"])

    sql_fct = text(f"""
        SELECT {fct_col_station} AS station,
               {fct_col_remark}  AS remark,
               MIN(del_out_op_ct_av) AS ideal_ct_sec
        FROM {fqn(IDEAL_SCHEMA_FCT, IDEAL_TABLE_FCT)}
        WHERE {fct_col_station} = ANY(:stations)
          AND {fct_col_remark}  = ANY(:remarks)
        GROUP BY {fct_col_station}, {fct_col_remark}
    """)
    with engine.begin() as conn:
        df_fct = pd.read_sql(sql_fct, conn, params={"stations": ["FCT1","FCT2","FCT3","FCT4"], "remarks": REMARKS})

    df_fct = df_fct[["station","remark","ideal_ct_sec"]].copy()
    return pd.concat([df_vision, df_fct], ignore_index=True)

# -----------------------------
# Remark segments
# -----------------------------
def base_remark_for_station(df_final: pd.DataFrame, station: str) -> str:
    s = df_final[df_final["station"] == station].copy()
    if s.empty:
        return "Non-PD"
    cleaned = []
    for v in s["remark"].tolist():
        cv = clean_remark(v)
        if cv is not None:
            cleaned.append(cv)
    vals = sorted(set(cleaned))
    if not vals:
        return "Non-PD"
    if len(vals) > 1:
        log_warn(f"station={station} has multiple base remarks in final_amount: {vals} -> using first")
    return vals[0]

def at_time_to_dt(window_start: datetime, shift_type: str, at_time_val: Any) -> datetime:
    at_t = parse_hms(at_time_val)
    at_dt = datetime.combine(window_start.date(), at_t, tzinfo=KST)
    if shift_type == "night" and at_dt < window_start:
        at_dt += timedelta(days=1)
    return at_dt

def build_segments_for_station(
    df_remark_all: pd.DataFrame,
    station: str,
    window_start: datetime,
    window_end: datetime,
    shift_type: str,
    base_remark: str,
) -> List[Tuple[str, Interval]]:
    """
    half-open [start,end)
    boundary = at_time + 1sec (at_time included in previous segment)
    """
    events = df_remark_all[df_remark_all["station"] == station].copy()
    events = events.sort_values("at_time")

    if events.empty:
        return [(base_remark, Interval(window_start, window_end))]

    segs: List[Tuple[str, Interval]] = []
    cur_start = window_start
    cur_remark = str(events.iloc[0]["from_remark"])

    for _, row in events.iterrows():
        at_dt = at_time_to_dt(window_start, shift_type, row["at_time"])
        boundary = at_dt + timedelta(seconds=1)

        if boundary < window_start:
            boundary = window_start
        if boundary > window_end:
            boundary = window_end

        if boundary > cur_start:
            segs.append((cur_remark, Interval(cur_start, boundary)))

        cur_start = boundary
        cur_remark = str(row["to_remark"])
        if cur_start >= window_end:
            break

    if cur_start < window_end:
        segs.append((cur_remark, Interval(cur_start, window_end)))

    segs = [(r, iv) for (r, iv) in segs if iv.end > iv.start]
    merged: List[Tuple[str, Interval]] = []
    for r, iv in segs:
        if not merged:
            merged.append((r, iv))
            continue
        pr, piv = merged[-1]
        if pr == r and piv.end == iv.start:
            merged[-1] = (pr, Interval(piv.start, iv.end))
        else:
            merged.append((r, iv))
    return merged

def remark_window_seconds(segments: Dict[str, List[Tuple[str, Interval]]], station: str, remark: str) -> int:
    sec = 0
    for rr, iv in segments[station]:
        if str(rr) == remark:
            sec += int((iv.end - iv.start).total_seconds())
    return sec

# -----------------------------
# Planned stop intervals
# -----------------------------
def _is_missing_time(v) -> bool:
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return True
    if s in ("0","0초","00","0000","000000"):
        return True
    return False

def stops_to_intervals(
    df_stop: pd.DataFrame,
    window_start: datetime,
    window_end: datetime,
    shift_type: str,
    station: Optional[str],
) -> List[Interval]:
    d = df_stop.copy()
    if station is not None and "station" in d.columns:
        d = d[d["station"] == station]

    ivs: List[Interval] = []
    for _, row in d.iterrows():
        ft = row.get("from_time")
        tt = row.get("to_time")
        if _is_missing_time(ft) or _is_missing_time(tt):
            continue

        ft_t = parse_hms(ft)
        tt_t = parse_hms(tt)

        sdt = datetime.combine(window_start.date(), ft_t, tzinfo=KST)
        edt = datetime.combine(window_start.date(), tt_t, tzinfo=KST)

        if shift_type == "night":
            if sdt < window_start:
                sdt += timedelta(days=1)
            if edt < window_start:
                edt += timedelta(days=1)

        if edt < sdt:
            sdt, edt = edt, sdt

        if edt <= sdt:
            continue

        sdt = max(sdt, window_start)
        edt = min(edt, window_end)
        if edt > sdt:
            ivs.append(Interval(sdt, edt))

    return merge_intervals(ivs)

def get_total_planned_time_if_available(df_stop: pd.DataFrame) -> Optional[int]:
    if "total_planned_time" not in df_stop.columns:
        return None
    s = df_stop.dropna(subset=["total_planned_time"])
    if s.empty:
        return None
    try:
        return int(float(s.iloc[0]["total_planned_time"]))
    except Exception:
        return None

# -----------------------------
# OEE calc (Cell 15 logic)
# -----------------------------
def ideal_ct_for(df_ideal: pd.DataFrame, station: str, remark: str) -> Optional[float]:
    s = df_ideal[(df_ideal["station"] == station) & (df_ideal["remark"] == remark)]
    if s.empty:
        return None
    try:
        v = float(s.iloc[0]["ideal_ct_sec"])
        return v if v > 0 else None
    except Exception:
        return None

def calc_oee(
    prod_day: str,
    shift_type: str,
    window_start: datetime,
    window_end: datetime,
    df_remark_all: pd.DataFrame,
    df_stop: pd.DataFrame,
    df_non: pd.DataFrame,
    df_q: pd.DataFrame,
    df_final: pd.DataFrame,
    df_ideal: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    base_remark_map = {st: base_remark_for_station(df_final, st) for st in STATIONS}

    segments = {
        st: build_segments_for_station(
            df_remark_all=df_remark_all,
            station=st,
            window_start=window_start,
            window_end=window_end,
            shift_type=shift_type,
            base_remark=base_remark_map[st],
        )
        for st in STATIONS
    }

    stop_union: Dict[str, List[Interval]] = {}
    if "station" in df_stop.columns:
        for st in STATIONS:
            stop_union[st] = stops_to_intervals(df_stop, window_start, window_end, shift_type, st)
    else:
        common = stops_to_intervals(df_stop, window_start, window_end, shift_type, None)
        for st in STATIONS:
            stop_union[st] = common

    total_planned_time_shift = get_total_planned_time_if_available(df_stop)

    planned_sec_by_station_remark: Dict[Tuple[str,str], int] = {}
    for st in STATIONS:
        for r in REMARKS:
            planned_sec_by_station_remark[(st, r)] = 0

        has_events = not df_remark_all[df_remark_all["station"] == st].empty
        if not has_events:
            r0 = base_remark_map[st]
            if total_planned_time_shift is not None:
                planned_sec_by_station_remark[(st, r0)] = int(total_planned_time_shift)
            else:
                planned_sec_by_station_remark[(st, r0)] = int(total_seconds(stop_union[st]))
            continue

        for r, seg_iv in segments[st]:
            sec = 0
            for ps in stop_union[st]:
                sec += overlap_seconds(ps, seg_iv)
            planned_sec_by_station_remark[(st, r)] += sec

    if df_non.empty:
        raise ValueError("non_time returned 0 rows")
    if df_q.empty:
        raise ValueError("quality returned 0 rows")

    non_row = df_non.iloc[0].to_dict()
    q_row   = df_q.iloc[0].to_dict()

    def non_time_sec(station: str) -> int:
        col = f"비가동 {station}"
        if col not in df_non.columns:
            raise ValueError(f"non_time column not found: {col}")
        return parse_korean_duration_to_sec(non_row.get(col))

    def quality_triplet(station: str) -> Dict[str, Any]:
        if station not in df_q.columns:
            raise ValueError(f"quality column not found: {station}")
        p, t, q = parse_quality_text(q_row.get(station))
        return {"pass": p, "total": t, "Q": q}

    # ---- Station OEE ----
    station_rows = []
    station_intermediate: Dict[str, Dict[str, Any]] = {}

    for st in STATIONS:
        planned_total = sum(int(planned_sec_by_station_remark.get((st, r), 0)) for r in REMARKS)
        PPT = max(WINDOW_SECONDS - planned_total, 0)

        unplanned = int(non_time_sec(st))
        run = max(PPT - unplanned, 0)

        A = _safe_div(run, PPT)

        ideal_total_qty = 0.0
        for r in REMARKS:
            rw = remark_window_seconds(segments, st, r)
            if rw <= 0:
                continue
            ps_r = min(int(planned_sec_by_station_remark.get((st, r), 0)), rw)
            PPT_r = max(rw - ps_r, 0)
            ct = ideal_ct_for(df_ideal, st, r)
            if ct is None:
                continue
            ideal_total_qty += (PPT_r / ct)

        mix_ct = (PPT / ideal_total_qty) if (PPT > 0 and ideal_total_qty > 0) else np.nan

        qt = quality_triplet(st)
        total_cnt = int(qt["total"])
        good_cnt  = int(qt["pass"])
        Q = float(qt["Q"])

        P = _safe_div(mix_ct * total_cnt, run) if run > 0 else np.nan
        OEE = (A * P * Q) if (not np.isnan(A) and not np.isnan(P) and not np.isnan(Q)) else np.nan

        station_rows.append({
            "prod_day": prod_day,
            "shift_type": shift_type,
            "station": st,
            "Station별 OEE": to_pct_str(OEE),
        })
        station_intermediate[st] = dict(PPT=PPT, run=run, ideal_qty=ideal_total_qty, total_cnt=total_cnt, good_cnt=good_cnt)

    df_oee_by_station = pd.DataFrame(station_rows)

    # ---- Line OEE (Vision-based) ----
    LINES = [
        {"line": "left",  "vision": "Vision1"},
        {"line": "right", "vision": "Vision2"},
    ]

    line_rows = []
    line_intermediate: Dict[str, Dict[str, Any]] = {}

    for L in LINES:
        line_name = L["line"]
        vis = L["vision"]
        v = station_intermediate[vis]

        PPT = v["PPT"]
        run = v["run"]
        ideal_qty = v["ideal_qty"]
        total_cnt = v["total_cnt"]
        good_cnt  = v["good_cnt"]

        A = _safe_div(run, PPT)
        mix_ct = (PPT / ideal_qty) if (PPT > 0 and ideal_qty > 0) else np.nan
        P = _safe_div(mix_ct * total_cnt, run) if run > 0 else np.nan
        Q = _safe_div(good_cnt, total_cnt)

        OEE = (A * P * Q) if (not np.isnan(A) and not np.isnan(P) and not np.isnan(Q)) else np.nan

        line_rows.append({
            "prod_day": prod_day,
            "shift_type": shift_type,
            "line": line_name,
            "Line별 OEE": to_pct_str(OEE),
        })
        line_intermediate[line_name] = dict(PPT=PPT, run=run, ideal_qty=ideal_qty, total_cnt=total_cnt, good_cnt=good_cnt)

    df_oee_by_line = pd.DataFrame(line_rows)

    # ---- Total OEE (sum line APQ) ----
    PPT_tot   = sum(v["PPT"] for v in line_intermediate.values())
    run_tot   = sum(v["run"] for v in line_intermediate.values())
    ideal_tot = sum(v["ideal_qty"] for v in line_intermediate.values())
    total_cnt_tot = int(sum(v["total_cnt"] for v in line_intermediate.values()))
    good_cnt_tot  = int(sum(v["good_cnt"]  for v in line_intermediate.values()))

    Q_tot = _safe_div(good_cnt_tot, total_cnt_tot)
    A_tot = _safe_div(run_tot, PPT_tot)
    mix_ct_tot = (PPT_tot / ideal_tot) if (PPT_tot > 0 and ideal_tot > 0) else np.nan
    P_tot = _safe_div(mix_ct_tot * total_cnt_tot, run_tot) if run_tot > 0 else np.nan
    OEE_tot = (A_tot * P_tot * Q_tot) if (not np.isnan(A_tot) and not np.isnan(P_tot) and not np.isnan(Q_tot)) else np.nan

    df_oee_total = pd.DataFrame([{
        "prod_day": prod_day,
        "shift_type": shift_type,
        "전체 OEE": to_pct_str(OEE_tot),
    }])

    return df_oee_total, df_oee_by_line, df_oee_by_station

# -----------------------------
# Save to DB (CREATE + ENSURE UNIQUE INDEX + UPSERT)
# UNIQUE:
#  - total   : (prod_day)
#  - line    : (prod_day, line)
#  - station : (prod_day, station)
# -----------------------------
def ensure_output_tables(engine: Engine, shift_type: str) -> Tuple[str, str, str]:
    t_total   = T_TOTAL_DAY   if shift_type == "day" else T_TOTAL_NIGHT
    t_line    = T_LINE_DAY    if shift_type == "day" else T_LINE_NIGHT
    t_station = T_ST_DAY      if shift_type == "day" else T_ST_NIGHT

    f_total   = fqn(SAVE_SCHEMA, t_total)
    f_line    = fqn(SAVE_SCHEMA, t_line)
    f_station = fqn(SAVE_SCHEMA, t_station)

    ddl_schema = text(f'CREATE SCHEMA IF NOT EXISTS "{SAVE_SCHEMA}";')

    # ✅ 테이블은 "컬럼만" 보장 (기존 테이블에 PK 없던 케이스를 커버)
    ddl_total = text(f"""
    CREATE TABLE IF NOT EXISTS {f_total} (
        prod_day   text NOT NULL,
        shift_type text NOT NULL,
        "전체 OEE"  text,
        updated_at timestamptz NOT NULL DEFAULT now()
    );
    """)

    ddl_line = text(f"""
    CREATE TABLE IF NOT EXISTS {f_line} (
        prod_day     text NOT NULL,
        shift_type   text NOT NULL,
        line         text NOT NULL,
        "Line별 OEE" text,
        updated_at   timestamptz NOT NULL DEFAULT now()
    );
    """)

    ddl_station = text(f"""
    CREATE TABLE IF NOT EXISTS {f_station} (
        prod_day        text NOT NULL,
        shift_type      text NOT NULL,
        station         text NOT NULL,
        "Station별 OEE" text,
        updated_at      timestamptz NOT NULL DEFAULT now()
    );
    """)

    # ✅ ON CONFLICT 가 동작하려면 UNIQUE/PK가 반드시 필요
    ux_total = text(f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{t_total}_prod_day
    ON {f_total} (prod_day);
    """)

    ux_line = text(f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{t_line}_prod_day_line
    ON {f_line} (prod_day, line);
    """)

    ux_station = text(f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{t_station}_prod_day_station
    ON {f_station} (prod_day, station);
    """)

    with engine.begin() as conn:
        conn.execute(ddl_schema)
        conn.execute(ddl_total)
        conn.execute(ddl_line)
        conn.execute(ddl_station)

        # unique index ensure (중복 데이터가 있으면 여기서 실패할 수 있음)
        conn.execute(ux_total)
        conn.execute(ux_line)
        conn.execute(ux_station)

    return f_total, f_line, f_station

def upsert_outputs(
    engine: Engine,
    f_total: str,
    f_line: str,
    f_station: str,
    df_total: pd.DataFrame,
    df_line: pd.DataFrame,
    df_station: pd.DataFrame,
) -> None:
    sql_total = text(f"""
        INSERT INTO {f_total} (prod_day, shift_type, "전체 OEE", updated_at)
        VALUES (:prod_day, :shift_type, :전체_OEE, now())
        ON CONFLICT (prod_day)
        DO UPDATE SET
            shift_type = EXCLUDED.shift_type,
            "전체 OEE" = EXCLUDED."전체 OEE",
            updated_at = now()
    """)

    sql_line = text(f"""
        INSERT INTO {f_line} (prod_day, shift_type, line, "Line별 OEE", updated_at)
        VALUES (:prod_day, :shift_type, :line, :Line별_OEE, now())
        ON CONFLICT (prod_day, line)
        DO UPDATE SET
            shift_type   = EXCLUDED.shift_type,
            "Line별 OEE" = EXCLUDED."Line별 OEE",
            updated_at   = now()
    """)

    sql_station = text(f"""
        INSERT INTO {f_station} (prod_day, shift_type, station, "Station별 OEE", updated_at)
        VALUES (:prod_day, :shift_type, :station, :Station별_OEE, now())
        ON CONFLICT (prod_day, station)
        DO UPDATE SET
            shift_type      = EXCLUDED.shift_type,
            "Station별 OEE" = EXCLUDED."Station별 OEE",
            updated_at      = now()
    """)

    payload_total = df_total.rename(columns={"전체 OEE": "전체_OEE"}).to_dict(orient="records")
    payload_line  = df_line.rename(columns={"Line별 OEE": "Line별_OEE"}).to_dict(orient="records")
    payload_st    = df_station.rename(columns={"Station별 OEE": "Station별_OEE"}).to_dict(orient="records")

    with engine.begin() as conn:
        conn.execute(sql_total, payload_total)
        conn.execute(sql_line,  payload_line)
        conn.execute(sql_station, payload_st)

# -----------------------------
# Main daemon
# -----------------------------
def run_daemon():
    log_boot("OEE daemon starting (5-thread fetch, 5s loop)")
    engine = connect_blocking()

    last_window_key = None  # (prod_day, shift_type)
    df_ideal: Optional[pd.DataFrame] = None

    # remark incremental state
    last_at_time_by_station: Dict[str, str] = {}
    seen_remark_pk = SeenPK(maxlen=5)

    # in-memory full remark events (accumulate incremental)
    df_remark_all = pd.DataFrame(columns=["prod_day","shift_type","station","at_time","from_remark","to_remark"])

    while True:
        try:
            now = now_kst()
            prod_day, shift_type, window_start, window_end = current_window(now)
            window_key = (prod_day, shift_type)

            remark_table = T_REMARK_CHANGE_DAY if shift_type == "day" else T_REMARK_CHANGE_NIGHT
            stop_table   = T_PLANNED_STOP_DAY  if shift_type == "day" else T_PLANNED_STOP_NIGHT
            non_table    = T_NON_TIME_DAY      if shift_type == "day" else T_NON_TIME_NIGHT
            q_table      = T_QUALITY_DAY       if shift_type == "day" else T_QUALITY_NIGHT
            final_table  = T_FINAL_AMT_DAY     if shift_type == "day" else T_FINAL_AMT_NIGHT

            remark_fqn = fqn(SAVE_SCHEMA, remark_table)
            stop_fqn   = fqn(SAVE_SCHEMA, stop_table)
            non_fqn    = fqn(SAVE_SCHEMA, non_table)
            q_fqn      = fqn(SAVE_SCHEMA, q_table)
            final_fqn  = fqn(SAVE_SCHEMA, final_table)

            # window changed => bootstrap
            if window_key != last_window_key:
                log_info(
                    f"WINDOW switch => prod_day={prod_day}, shift={shift_type}, "
                    f"window=[{window_start:%Y-%m-%d %H:%M:%S} ~ {window_end:%Y-%m-%d %H:%M:%S})"
                )
                last_window_key = window_key

                last_at_time_by_station = {}
                seen_remark_pk = SeenPK(maxlen=5)

                log_info("bootstrap: load full remark_change + ideal_ct")
                df_remark_all = load_remark_changes_full(engine, remark_fqn, prod_day, shift_type)

                for st in STATIONS:
                    s = df_remark_all[df_remark_all["station"] == st]
                    if not s.empty:
                        last_at_time_by_station[st] = str(s.sort_values("at_time").iloc[-1]["at_time"])

                df_ideal = load_ideal_ct(engine)

            # ---------- parallel fetch (5 tables) ----------
            with ThreadPoolExecutor(max_workers=5) as ex:
                fut_remark = ex.submit(
                    load_remark_changes_incremental,
                    engine, remark_fqn, prod_day, shift_type, STATIONS,
                    last_at_time_by_station, seen_remark_pk
                )
                fut_stop   = ex.submit(load_planned_stops_full, engine, stop_fqn, prod_day, shift_type)
                fut_non    = ex.submit(load_non_time_row, engine, non_fqn, prod_day, shift_type)
                fut_q      = ex.submit(load_quality_row, engine, q_fqn, prod_day, shift_type)
                fut_final  = ex.submit(load_final_amount_full, engine, final_fqn, prod_day, shift_type)

                df_remark_inc = fut_remark.result()
                df_stop       = fut_stop.result()
                df_non        = fut_non.result()
                df_q          = fut_q.result()
                df_final      = fut_final.result()

            # apply incremental remark updates
            if not df_remark_inc.empty:
                before = len(df_remark_all)
                df_remark_all = pd.concat([df_remark_all, df_remark_inc], ignore_index=True)

                df_remark_all["_pk"] = (
                    df_remark_all["prod_day"].astype(str) + "|" +
                    df_remark_all["station"].astype(str) + "|" +
                    df_remark_all["at_time"].astype(str)
                )
                df_remark_all = df_remark_all.drop_duplicates("_pk", keep="last").drop(columns=["_pk"])
                df_remark_all = df_remark_all.sort_values(["station","at_time"]).reset_index(drop=True)

                log_info(f"remark_change fetch: +{len(df_remark_inc)} new (mem {before}->{len(df_remark_all)})")
            else:
                log_info("remark_change fetch: 0 new")

            # ---------- compute ----------
            if df_ideal is None or df_ideal.empty:
                raise ValueError("ideal_ct not loaded")

            df_oee_total, df_oee_by_line, df_oee_by_station = calc_oee(
                prod_day=prod_day,
                shift_type=shift_type,
                window_start=window_start,
                window_end=window_end,
                df_remark_all=df_remark_all,
                df_stop=df_stop,
                df_non=df_non,
                df_q=df_q,
                df_final=df_final,
                df_ideal=df_ideal,
            )

            # ---------- save ----------
            try:
                f_total, f_line, f_station = ensure_output_tables(engine, shift_type)
            except Exception as e:
                # UNIQUE INDEX 생성 실패(대부분 기존 중복 데이터)면 삭제 없이 해결 불가 -> 저장만 스킵
                log_warn(
                    "ensure_output_tables failed (likely duplicates prevent UNIQUE index). "
                    f"Need manual cleanup. err={repr(e)}"
                )
                time_mod.sleep(SLEEP_SEC)
                continue

            upsert_outputs(engine, f_total, f_line, f_station, df_oee_total, df_oee_by_line, df_oee_by_station)
            log_info(f"UPSERT OK => {f_total}, {f_line}, {f_station}")

            time_mod.sleep(SLEEP_SEC)

        except (OperationalError, DBAPIError) as e:
            log_retry(f"DB error: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)
            engine = connect_blocking()

        except Exception as e:
            log_warn(f"loop error: {repr(e)}")
            time_mod.sleep(SLEEP_SEC)

if __name__ == "__main__":
    run_daemon()