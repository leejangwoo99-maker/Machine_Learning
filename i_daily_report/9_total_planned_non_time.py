# -*- coding: utf-8 -*-
"""
backend9_planned_and_nonop_daemon.py

[Trigger Snapshot Build + snapshot_rows (edge-based) + FIX(end_day 보정)]
- ✅ 기존 기능 100% 유지: 윈도우/증분캐시/계산/UPSERT/재접속/health log
- ✅ 이상 패턴 감지 시: 스냅샷을 파일 + DB(k_demon_heath_check.9_log)에 저장
  * snapshot / snapshot_cache / snapshot_inc / snapshot_planned / snapshot_calc / snapshot_db / snapshot_rows

[핵심 수정(이상치 원인 해결)]
- end_day가 간헐적으로 다음날(예: prod_day+1)로 찍혀도 계산이 0으로 리셋되지 않도록
  record_interval_to_offsets()를 shift-aware로 보정
  - day shift: day_delta = 0 강제 (end_day 무시)
  - night shift: from_time 기준으로 20:30~23:59 => 0, 00:00~08:29 => 1

[Snapshot rows sampling]
- snapshot_rows: 윈도우 경계(w0, w1) ±60분(SNAP_EDGE_SEC) 구간에서
  station별로 w0_edge / w1_edge 샘플을 뽑아 원본+offsets+intersect를 저장

환경변수
- BACKEND9_SNAP_EDGE_SEC=3600 (기본 ±60분)
- BACKEND9_SNAP_ROWS_PER_EDGE=5 (기본 edge당 station별 5개)
"""

from __future__ import annotations

import os
import re
import time
import traceback
import logging
from logging.handlers import RotatingFileHandler
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Tuple, Optional, Set, Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError, DBAPIError, OperationalError
from zoneinfo import ZoneInfo

# =========================
# 0) 설정
# =========================
KST = ZoneInfo("Asia/Seoul")

SLEEP_SEC = int(os.getenv("BACKEND9_SLEEP_SEC", "5"))
FETCH_LIMIT = int(os.getenv("BACKEND9_FETCH_LIMIT", "5000"))
PG_WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# 세션 타임아웃(밀리초)
PG_STATEMENT_TIMEOUT_MS = int(os.getenv("PG_STATEMENT_TIMEOUT_MS", "60000"))          # 60s
PG_LOCK_TIMEOUT_MS = int(os.getenv("PG_LOCK_TIMEOUT_MS", "10000"))                    # 10s
PG_IDLE_IN_TX_TIMEOUT_MS = int(os.getenv("PG_IDLE_IN_TX_TIMEOUT_MS", "60000"))        # 60s

# heartbeat
HEARTBEAT_SEC = int(os.getenv("BACKEND9_HEARTBEAT_SEC", "60"))

# 로그 경로 고정
LOG_DIR = r"C:\AptivAgent\_logs"
LOG_FILE = os.path.join(LOG_DIR, "backend9_planned_and_nonop_daemon.log")
LOG_MAX_BYTES = int(os.getenv("BACKEND9_LOG_MAX_BYTES", str(20 * 1024 * 1024)))  # 20MB
LOG_BACKUP_COUNT = int(os.getenv("BACKEND9_LOG_BACKUP_COUNT", "10"))

# Snapshot control
SNAP_MAX_CONTENTS = int(os.getenv("BACKEND9_SNAP_MAX_CONTENTS", "1800"))  # DB health contents 최대 길이
SNAP_COOLDOWN_SEC = int(os.getenv("BACKEND9_SNAP_COOLDOWN_SEC", "300"))   # 스냅샷 최소 간격(초)
SNAP_FCT4_MIN_SEC = int(os.getenv("BACKEND9_SNAP_FCT4_MIN_SEC", "60"))    # FCT4가 이 이상이면 트리거 후보
SNAP_FCT123_MAX_SEC = int(os.getenv("BACKEND9_SNAP_FCT123_MAX_SEC", "5")) # FCT1~3 합이 이 이하면 트리거 후보
SNAP_DROP_RATIO = float(os.getenv("BACKEND9_SNAP_DROP_RATIO", "0.90"))    # 직전 대비 90% 이상 감소시 트리거
SNAP_SAMPLE_PKS = int(os.getenv("BACKEND9_SNAP_SAMPLE_PKS", "10"))

# snapshot_rows: edge-based sampling
SNAP_EDGE_SEC = int(os.getenv("BACKEND9_SNAP_EDGE_SEC", "3600"))          # ±60분
SNAP_ROWS_PER_EDGE = int(os.getenv("BACKEND9_SNAP_ROWS_PER_EDGE", "5"))   # edge당 station별 샘플 개수

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "100.105.75.47"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DBNAME", "postgres"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", ""),  # TODO: 보안
}

SRC_SCHEMA = "g_production_film"
T_PLANNED = "planned_time"
T_FCT_NONOP = "fct_non_operation_time"

SAVE_SCHEMA = "i_daily_report"
T_PLAN_DAY    = "i_planned_stop_time_day_daily"
T_PLAN_NIGHT  = "i_planned_stop_time_night_daily"
T_NONOP_DAY   = "i_non_time_day_daily"
T_NONOP_NIGHT = "i_non_time_night_daily"

FCT_STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]

# DB 로그 저장 대상
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "9_log"

# =========================
# 1) 로깅
# =========================
logger = logging.getLogger("backend9")
logger.setLevel(logging.INFO)
logger.propagate = False


def setup_logger():
    fmt = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)

    if not logger.handlers:
        logger.addHandler(sh)

    try:
        os.makedirs(LOG_DIR, exist_ok=True)
        fh = RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        fh.setFormatter(fmt)
        fh.setLevel(logging.INFO)
        logger.addHandler(fh)
        logger.info(f"[LOG] file logging enabled: {LOG_FILE}")
    except Exception as e:
        logger.warning(f"[LOG] file logging disabled (cannot open {LOG_FILE}): {type(e).__name__}: {e}")


def _normalize_info(info: str) -> str:
    s = (info or "").strip().lower()
    s = re.sub(r"[^a-z0-9_]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return s or "info"


def _truncate(s: str, n: int) -> str:
    if s is None:
        return ""
    s = str(s)
    if len(s) <= n:
        return s
    return s[: n - 20] + " ...[truncated]"


def _health_row(info: str, contents: str, now: Optional[datetime] = None) -> Dict[str, str]:
    ts = now or datetime.now(KST)
    return {
        "end_day": ts.strftime("%Y%m%d"),
        "end_time": ts.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": _truncate(str(contents), SNAP_MAX_CONTENTS),
    }


def _ensure_health_table(engine: Engine):
    ddl_schema = f'CREATE SCHEMA IF NOT EXISTS "{HEALTH_SCHEMA}";'
    ddl_table = f"""
    CREATE TABLE IF NOT EXISTS "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (
        "end_day" text,
        "end_time" text,
        "info" text,
        "contents" text
    );
    """
    with engine.begin() as conn:
        set_session(conn)
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))


def _save_health_logs_df(engine: Engine, rows: List[Dict[str, str]]):
    if not rows:
        return
    cols = ["end_day", "end_time", "info", "contents"]
    df = pd.DataFrame(rows, columns=cols)
    recs = df.to_dict(orient="records")

    sql = text(f"""
        INSERT INTO "{HEALTH_SCHEMA}"."{HEALTH_TABLE}"
        ("end_day","end_time","info","contents")
        VALUES (:end_day, :end_time, :info, :contents)
    """)
    with engine.begin() as conn:
        set_session(conn)
        conn.execute(sql, recs)


def log(level: str, msg: str):
    if level == "DEBUG":
        logger.debug(msg)
    elif level == "WARNING":
        logger.warning(msg)
    elif level == "ERROR":
        logger.error(msg)
    elif level == "RETRY":
        logger.warning(msg)
    else:
        logger.info(msg)


def log_exception(prefix: str, e: Exception):
    tb = traceback.format_exc()
    logger.error(f"{prefix}: {type(e).__name__}: {e}\n{tb}")


# =========================
# 2) DB 엔진 / 재시도
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
        connect_args={
            "connect_timeout": 10,
            "options": "-c timezone=Asia/Seoul",
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        },
    )


def set_session(conn):
    conn.execute(text(f"SET work_mem TO '{PG_WORK_MEM}';"))
    conn.execute(text(f"SET statement_timeout TO {PG_STATEMENT_TIMEOUT_MS};"))
    conn.execute(text(f"SET lock_timeout TO {PG_LOCK_TIMEOUT_MS};"))
    conn.execute(text(f"SET idle_in_transaction_session_timeout TO {PG_IDLE_IN_TX_TIMEOUT_MS};"))


def connect_with_retry() -> Engine:
    while True:
        try:
            eng = make_engine()
            with eng.begin() as conn:
                set_session(conn)
                conn.execute(text("SELECT 1;"))
            log("INFO", f"DB connected (work_mem={PG_WORK_MEM}, stmt_timeout={PG_STATEMENT_TIMEOUT_MS}ms)")
            return eng
        except Exception as e:
            log("RETRY", f"DB connect failed: {type(e).__name__}: {e} (sleep {SLEEP_SEC}s)")
            time.sleep(SLEEP_SEC)


# =========================
# 3) Window 계산 (KST, end=now)
# =========================
@dataclass(frozen=True)
class WindowState:
    prod_day: str
    shift_type: str
    start_ts: datetime
    end_ts: datetime


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def window_state_now(now: datetime) -> WindowState:
    assert now.tzinfo is not None

    d_today = now.date()
    day_start = datetime(d_today.year, d_today.month, d_today.day, 8, 30, 0, tzinfo=KST)
    night_start = datetime(d_today.year, d_today.month, d_today.day, 20, 30, 0, tzinfo=KST)

    if now >= night_start:
        return WindowState(yyyymmdd(d_today), "night", night_start, now)

    if now >= day_start:
        return WindowState(yyyymmdd(d_today), "day", day_start, now)

    d_yest = d_today - timedelta(days=1)
    start_ts = datetime(d_yest.year, d_yest.month, d_yest.day, 20, 30, 0, tzinfo=KST)
    return WindowState(yyyymmdd(d_yest), "night", start_ts, now)


# =========================
# 4) Decimal/Interval 유틸
# =========================
def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def parse_hms_decimal(hms: str) -> Decimal:
    h, m, s = str(hms).strip().split(":")
    return Decimal(int(h)) * Decimal(3600) + Decimal(int(m)) * Decimal(60) + Decimal(str(s))


def round_half_up_sec(x: Decimal) -> int:
    return int(x.quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def sec_to_kor_str(total_sec: int) -> str:
    if total_sec <= 0:
        return "0초"
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    s = total_sec % 60
    parts = []
    if h:
        parts.append(f"{h}시간")
    if m:
        parts.append(f"{m}분")
    if s or not parts:
        parts.append(f"{s}초")
    return " ".join(parts)


def shift_start_end_offsets(prod_day: str, shift_type: str) -> Tuple[Decimal, Decimal]:
    if shift_type == "day":
        return parse_hms_decimal("08:30:00"), parse_hms_decimal("20:29:59")
    if shift_type == "night":
        return parse_hms_decimal("20:30:00"), Decimal(86400) + parse_hms_decimal("08:29:59")
    raise ValueError("shift_type must be 'day' or 'night'")


def now_offset_from_prod_day(prod_day: str, now_ts: datetime) -> Decimal:
    base_d = parse_yyyymmdd(prod_day)
    now_d = now_ts.date()
    day_delta = (now_d - base_d).days
    now_hms = now_ts.replace(microsecond=0).strftime("%H:%M:%S")
    return Decimal(86400) * Decimal(day_delta) + parse_hms_decimal(now_hms)


def window_offsets_now(prod_day: str, shift_type: str, now_ts: datetime) -> Tuple[Decimal, Decimal]:
    s0, smax = shift_start_end_offsets(prod_day, shift_type)
    n1 = now_offset_from_prod_day(prod_day, now_ts)
    w1 = min(n1, smax)
    w1 = max(w1, s0)
    return s0, w1


# =========================
# 4-1) ✅ FIX: shift-aware record_interval_to_offsets
# =========================
def record_interval_to_offsets(base_prod_day: str, shift_type: str, end_day: str, from_time: str, to_time: str) -> Tuple[Decimal, Decimal]:
    """
    end_day가 간헐적으로 다음날로 찍히는 품질 이슈를 방어.
    - day: 무조건 day_delta=0 (당일 윈도우)
    - night: from_time 기준으로 20:30~23:59 => 0, 00:00~08:29 => 1
    """
    ft = parse_hms_decimal(from_time)
    tt = parse_hms_decimal(to_time)

    if shift_type == "day":
        day_delta = 0
    elif shift_type == "night":
        day_delta = 0 if ft >= parse_hms_decimal("20:30:00") else 1
    else:
        raise ValueError("shift_type must be 'day' or 'night'")

    a = Decimal(86400) * Decimal(day_delta) + ft
    b = Decimal(86400) * Decimal(day_delta) + tt
    if tt < ft:
        b += Decimal(86400)
    return a, b


def intersect(a: Decimal, b: Decimal, w0: Decimal, w1: Decimal) -> Optional[Tuple[Decimal, Decimal]]:
    s = max(a, w0)
    e = min(b, w1)
    if e <= s:
        return None
    return s, e


def merge_intervals_decimal(intervals: List[Tuple[Decimal, Decimal]]) -> List[Tuple[Decimal, Decimal]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: (x[0], x[1]))
    merged = [intervals[0]]
    for s, e in intervals[1:]:
        ps, pe = merged[-1]
        if s <= pe:
            merged[-1] = (ps, max(pe, e))
        else:
            merged.append((s, e))
    return merged


def subtract_intervals_decimal(base: Tuple[Decimal, Decimal], blockers: List[Tuple[Decimal, Decimal]]) -> List[Tuple[Decimal, Decimal]]:
    if not blockers:
        return [base]
    s0, e0 = base
    cur = [(s0, e0)]
    for bs, be in blockers:
        nxt = []
        for s, e in cur:
            if be <= s or bs >= e:
                nxt.append((s, e))
                continue
            if s < bs:
                nxt.append((s, min(bs, e)))
            if e > be:
                nxt.append((max(be, s), e))
        cur = nxt
        if not cur:
            break
    return [(s, e) for s, e in cur if e > s]


def subtract_many_decimal(intervals: List[Tuple[Decimal, Decimal]], blockers: List[Tuple[Decimal, Decimal]]) -> List[Tuple[Decimal, Decimal]]:
    if not intervals:
        return []
    out: List[Tuple[Decimal, Decimal]] = []
    for itv in intervals:
        out.extend(subtract_intervals_decimal(itv, blockers))
    return merge_intervals_decimal(out)


def to_int_intervals_half_up(intervals: List[Tuple[Decimal, Decimal]]) -> List[Tuple[int, int]]:
    out = []
    for s, e in intervals:
        si = round_half_up_sec(s)
        ei = round_half_up_sec(e)
        if ei <= si:
            continue
        out.append((si, ei))
    return out


def merge_intervals_int(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    if not intervals:
        return []
    intervals = sorted(intervals, key=lambda x: (x[0], x[1]))
    merged = [intervals[0]]
    for s, e in intervals[1:]:
        ps, pe = merged[-1]
        if s <= pe:
            merged[-1] = (ps, max(pe, e))
        else:
            merged.append((s, e))
    return merged


def sum_int_durations(intervals: List[Tuple[int, int]]) -> int:
    return int(sum(e - s for s, e in intervals))


def intersect_two_decimal_lists(a: List[Tuple[Decimal, Decimal]], b: List[Tuple[Decimal, Decimal]]) -> List[Tuple[Decimal, Decimal]]:
    i = j = 0
    out: List[Tuple[Decimal, Decimal]] = []
    while i < len(a) and j < len(b):
        as_, ae = a[i]
        bs_, be = b[j]
        s = max(as_, bs_)
        e = min(ae, be)
        if e > s:
            out.append((s, e))
        if ae <= be:
            i += 1
        else:
            j += 1
    return merge_intervals_decimal(out)


def offset_to_hms(off: Decimal) -> str:
    total = int(off.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    sec_in_day = total % 86400
    hh = sec_in_day // 3600
    mm = (sec_in_day % 3600) // 60
    ss = sec_in_day % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"


# =========================
# 5) Fetch
# =========================
def planned_fetch_for_window(engine: Engine, prod_day: str) -> pd.DataFrame:
    d0 = parse_yyyymmdd(prod_day)
    d1 = d0 + timedelta(days=1)
    sql = text(f"""
        SELECT end_day, from_time, to_time, reason
        FROM {SRC_SCHEMA}.{T_PLANNED}
        WHERE end_day IN (:day0, :day1)
    """)
    with engine.begin() as conn:
        set_session(conn)
        return pd.read_sql(sql, conn, params={"day0": yyyymmdd(d0), "day1": yyyymmdd(d1)})


def fct_nonop_bootstrap(engine: Engine, prod_day: str) -> pd.DataFrame:
    d0 = parse_yyyymmdd(prod_day)
    d1 = d0 + timedelta(days=1)
    sql = text(f"""
        SELECT end_day, station, from_time, to_time
        FROM {SRC_SCHEMA}.{T_FCT_NONOP}
        WHERE end_day IN (:day0, :day1)
          AND station IN ('FCT1','FCT2','FCT3','FCT4')
        ORDER BY end_day, station, from_time
    """)
    with engine.begin() as conn:
        set_session(conn)
        return pd.read_sql(sql, conn, params={"day0": yyyymmdd(d0), "day1": yyyymmdd(d1)})


def fct_nonop_incremental(engine: Engine, prod_day: str, last_pk: Tuple[str, str, str]) -> pd.DataFrame:
    d0 = parse_yyyymmdd(prod_day)
    d1 = d0 + timedelta(days=1)
    e, s, f = last_pk
    sql = text(f"""
        SELECT end_day, station, from_time, to_time
        FROM {SRC_SCHEMA}.{T_FCT_NONOP}
        WHERE end_day IN (:day0, :day1)
          AND station IN ('FCT1','FCT2','FCT3','FCT4')
          AND (end_day, station, from_time) > (:e, :s, :f)
        ORDER BY end_day, station, from_time
        LIMIT :lim
    """)
    with engine.begin() as conn:
        set_session(conn)
        return pd.read_sql(
            sql, conn,
            params={"day0": yyyymmdd(d0), "day1": yyyymmdd(d1), "e": e, "s": s, "f": f, "lim": FETCH_LIMIT}
        )


def db_station_stats_for_days(engine: Engine, day0: str, day1: str) -> Dict[str, Any]:
    sql = text(f"""
        SELECT station,
               count(*) AS cnt,
               min(from_time) AS min_from,
               max(from_time) AS max_from,
               min(to_time) AS min_to,
               max(to_time) AS max_to
        FROM {SRC_SCHEMA}.{T_FCT_NONOP}
        WHERE end_day IN (:day0, :day1)
          AND station IN ('FCT1','FCT2','FCT3','FCT4')
        GROUP BY station
        ORDER BY station
    """)
    with engine.begin() as conn:
        set_session(conn)
        df = pd.read_sql(sql, conn, params={"day0": day0, "day1": day1})
    out: Dict[str, Any] = {}
    for st in FCT_STATIONS:
        sub = df[df["station"].astype(str) == st]
        if sub.empty:
            out[st] = {"cnt": 0}
        else:
            r = sub.iloc[0]
            out[st] = {
                "cnt": int(r["cnt"]),
                "min_from": str(r["min_from"]),
                "max_from": str(r["max_from"]),
                "min_to": str(r["min_to"]),
                "max_to": str(r["max_to"]),
            }
    return out


# =========================
# 6) 계산
# =========================
def build_planned_intervals_for_shift(planned_df: pd.DataFrame, prod_day: str, shift_type: str, now_ts: datetime) -> List[Tuple[Decimal, Decimal]]:
    w0, w1 = window_offsets_now(prod_day, shift_type, now_ts)
    intervals: List[Tuple[Decimal, Decimal]] = []
    for r in planned_df.itertuples(index=False):
        a, b = record_interval_to_offsets(prod_day, shift_type, str(r.end_day), str(r.from_time), str(r.to_time))
        inter = intersect(a, b, w0, w1)
        if inter:
            intervals.append(inter)
    return merge_intervals_decimal(intervals)


def planned_rows_per_interval(planned_df: pd.DataFrame, prod_day: str, shift_type: str, now_ts: datetime, include_total_row: bool = True) -> pd.DataFrame:
    intervals = build_planned_intervals_for_shift(planned_df, prod_day, shift_type, now_ts)
    total_dec = sum(((e - s) for s, e in intervals), Decimal("0"))
    total_sec = round_half_up_sec(total_dec)

    rows: List[Dict[str, object]] = []
    if include_total_row:
        rows.append({
            "prod_day": prod_day,
            "shift_type": shift_type,
            "from_time": "",
            "to_time": "",
            "Total 계획 정지 시간": sec_to_kor_str(total_sec),
            "total_planned_time": int(total_sec),
            "updated_at": now_ts,
        })

    for s, e in intervals:
        rows.append({
            "prod_day": prod_day,
            "shift_type": shift_type,
            "from_time": offset_to_hms(s),
            "to_time": offset_to_hms(e),
            "Total 계획 정지 시간": sec_to_kor_str(total_sec),
            "total_planned_time": int(total_sec),
            "updated_at": now_ts,
        })

    if not rows:
        return pd.DataFrame(columns=["prod_day", "shift_type", "from_time", "to_time",
                                     "Total 계획 정지 시간", "total_planned_time", "updated_at"])
    return pd.DataFrame(rows)


def build_fct_station_raw_decimal_from_rows(rows_df: pd.DataFrame, prod_day: str, shift_type: str, now_ts: datetime) -> Dict[str, List[Tuple[Decimal, Decimal]]]:
    w0, w1 = window_offsets_now(prod_day, shift_type, now_ts)
    tmp: Dict[str, List[Tuple[Decimal, Decimal]]] = {st: [] for st in FCT_STATIONS}

    for r in rows_df.itertuples(index=False):
        st = str(r.station)
        if st not in tmp:
            continue
        a, b = record_interval_to_offsets(prod_day, shift_type, str(r.end_day), str(r.from_time), str(r.to_time))
        inter = intersect(a, b, w0, w1)
        if inter:
            tmp[st].append(inter)

    return {st: merge_intervals_decimal(tmp[st]) for st in FCT_STATIONS}


def fct_display_seconds_after_planned(raw_station_dec: Dict[str, List[Tuple[Decimal, Decimal]]], planned_dec: List[Tuple[Decimal, Decimal]]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for st in FCT_STATIONS:
        dec_after = subtract_many_decimal(raw_station_dec[st], planned_dec)
        int_intervals = merge_intervals_int(to_int_intervals_half_up(dec_after))
        out[st] = sum_int_durations(int_intervals)
    return out


def vision_seconds_overlap_then_planned(raw_a: List[Tuple[Decimal, Decimal]], raw_b: List[Tuple[Decimal, Decimal]], planned_dec: List[Tuple[Decimal, Decimal]]) -> Tuple[int, int, int]:
    overlap_dec = intersect_two_decimal_lists(raw_a, raw_b)
    before = sum_int_durations(merge_intervals_int(to_int_intervals_half_up(overlap_dec)))
    after_planned_dec = subtract_many_decimal(overlap_dec, planned_dec)
    after = sum_int_durations(merge_intervals_int(to_int_intervals_half_up(after_planned_dec)))
    removed = max(0, before - after)
    return before, after, removed


def nonop_summary_overlap_spec_from_rows(rows_df: pd.DataFrame, planned_df: pd.DataFrame, prod_day: str, shift_type: str, now_ts: datetime) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    diag: Dict[str, Any] = {}

    w0, w1 = window_offsets_now(prod_day, shift_type, now_ts)
    diag["window"] = {
        "prod_day": prod_day,
        "shift_type": shift_type,
        "w0": float(w0),
        "w1": float(w1),
        "span_sec": float(w1 - w0),
        "w0_hms": offset_to_hms(w0),
        "w1_hms": offset_to_hms(w1),
    }

    raw_station_dec = build_fct_station_raw_decimal_from_rows(rows_df, prod_day, shift_type, now_ts)
    planned_dec = build_planned_intervals_for_shift(planned_df, prod_day, shift_type, now_ts)

    planned_sec = sum_int_durations(merge_intervals_int(to_int_intervals_half_up(planned_dec)))
    diag["planned"] = {"intervals": len(planned_dec), "planned_sec": planned_sec}

    raw_before: Dict[str, int] = {}
    for st in FCT_STATIONS:
        raw_before[st] = sum_int_durations(merge_intervals_int(to_int_intervals_half_up(raw_station_dec[st])))

    fct_after = fct_display_seconds_after_planned(raw_station_dec, planned_dec)
    removed_by_planned = {st: max(0, raw_before[st] - fct_after[st]) for st in FCT_STATIONS}

    v1_before, v1_after, v1_removed = vision_seconds_overlap_then_planned(raw_station_dec["FCT1"], raw_station_dec["FCT2"], planned_dec)
    v2_before, v2_after, v2_removed = vision_seconds_overlap_then_planned(raw_station_dec["FCT3"], raw_station_dec["FCT4"], planned_dec)
    total_vision = v1_after + v2_after

    diag["fct_raw_before_sec"] = raw_before
    diag["fct_after_sec"] = fct_after
    diag["fct_removed_by_planned_sec"] = removed_by_planned
    diag["vision"] = {
        "v1_before": v1_before, "v1_after": v1_after, "v1_removed": v1_removed,
        "v2_before": v2_before, "v2_after": v2_after, "v2_removed": v2_removed,
        "total_after": total_vision,
    }

    row = {
        "prod_day": prod_day,
        "shift_type": shift_type,
        "비가동 FCT1": sec_to_kor_str(fct_after["FCT1"]),
        "비가동 FCT2": sec_to_kor_str(fct_after["FCT2"]),
        "비가동 Vision1": sec_to_kor_str(v1_after),
        "비가동 FCT3": sec_to_kor_str(fct_after["FCT3"]),
        "비가동 FCT4": sec_to_kor_str(fct_after["FCT4"]),
        "비가동 Vision2": sec_to_kor_str(v2_after),
        "Total Vision 비가동 시간": sec_to_kor_str(total_vision),
        "vision1_non_time": int(v1_after),
        "vision2_non_time": int(v2_after),
        "total_vision_non_time": int(total_vision),
        "updated_at": now_ts,
    }
    return pd.DataFrame([row]), diag


# =========================
# 7) 저장(UPSERT)
# =========================
def ensure_schema(engine: Engine, schema: str):
    with engine.begin() as conn:
        set_session(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))


def _col_type(col: str) -> str:
    if col == "updated_at":
        return "timestamptz"
    if col in ("total_planned_time", "vision1_non_time", "vision2_non_time", "total_vision_non_time"):
        return "integer"
    return "text"


def ensure_table(engine: Engine, schema: str, table: str, columns: List[str], key_cols: List[str]):
    ddl_cols = [f'"{c}" {_col_type(c)}' for c in columns]
    ddl_cols_sql = ",\n  ".join(ddl_cols)
    key_sql = ", ".join([f'"{c}"' for c in key_cols])
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
      {ddl_cols_sql},
      CONSTRAINT "{table}__uk" UNIQUE ({key_sql})
    );
    """
    with engine.begin() as conn:
        set_session(conn)
        conn.execute(text(ddl))


def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]):
    if df is None or df.empty:
        log("INFO", f"[SKIP] upsert {schema}.{table} (df empty)")
        return

    cols = list(df.columns)
    non_keys = [c for c in cols if c not in key_cols]

    col_sql = ", ".join([f'"{c}"' for c in cols])
    bind_keys = [f"p{i}" for i in range(len(cols))]
    val_sql = ", ".join([f":{bk}" for bk in bind_keys])
    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in non_keys])

    sql = text(f"""
        INSERT INTO "{schema}"."{table}" ({col_sql})
        VALUES ({val_sql})
        ON CONFLICT ({", ".join([f'"{c}"' for c in key_cols])})
        DO UPDATE SET
            {set_sql};
    """)

    records = []
    for rec in df.to_dict(orient="records"):
        records.append({bk: rec.get(c) for c, bk in zip(cols, bind_keys)})

    with engine.begin() as conn:
        set_session(conn)
        conn.execute(sql, records)

    log("INFO", f"[UPSERT] {schema}.{table} rows={len(df)}")


# =========================
# 8) Snapshot helpers
# =========================
def station_counts(df: Optional[pd.DataFrame]) -> Dict[str, int]:
    if df is None or df.empty or "station" not in df.columns:
        return {}
    vc = df["station"].astype(str).value_counts()
    return {k: int(v) for k, v in vc.to_dict().items()}


def sample_last_pks(df: Optional[pd.DataFrame], n: int) -> List[Tuple[str, str, str]]:
    if df is None or df.empty:
        return []
    tmp = df.copy()
    tmp["end_day"] = tmp["end_day"].astype(str)
    tmp["station"] = tmp["station"].astype(str)
    tmp["from_time"] = tmp["from_time"].astype(str)
    tmp = tmp.sort_values(["end_day", "station", "from_time"])
    tail = tmp.tail(n)
    return [(str(r.end_day), str(r.station), str(r.from_time)) for r in tail.itertuples(index=False)]


def should_snapshot(diag: Dict[str, Any], prev_total_vision: Optional[int], last_snapshot_ts: float) -> Tuple[bool, str]:
    now_ts = time.time()
    if now_ts - last_snapshot_ts < SNAP_COOLDOWN_SEC:
        return False, "cooldown"

    fct_after = diag.get("fct_after_sec", {}) or {}
    vision = diag.get("vision", {}) or {}
    fct1 = int(fct_after.get("FCT1", 0))
    fct2 = int(fct_after.get("FCT2", 0))
    fct3 = int(fct_after.get("FCT3", 0))
    fct4 = int(fct_after.get("FCT4", 0))
    v1_after = int(vision.get("v1_after", 0))
    v2_after = int(vision.get("v2_after", 0))
    total_now = int(vision.get("total_after", v1_after + v2_after))

    if fct4 >= SNAP_FCT4_MIN_SEC and (fct1 + fct2 + fct3) <= SNAP_FCT123_MAX_SEC:
        return True, f"pattern_a_fct4_big_fct123_small fct4={fct4} fct123={fct1+fct2+fct3}"

    if prev_total_vision is not None and prev_total_vision > 0:
        drop = (prev_total_vision - total_now) / float(prev_total_vision)
        if drop >= SNAP_DROP_RATIO:
            return True, f"pattern_b_total_drop prev={prev_total_vision} now={total_now} drop={drop:.2f}"

    planned = diag.get("planned", {}) or {}
    planned_sec = int(planned.get("planned_sec", 0))
    if planned_sec > 0 and ((v1_after == 0 and fct1 > 0 and fct2 > 0) or (v2_after == 0 and fct3 > 0 and fct4 > 0)):
        return True, f"pattern_c_vision_zero_with_planned planned_sec={planned_sec} v1={v1_after} v2={v2_after}"

    return False, "no_trigger"


def _edge_sample_rows_offsets(nonop_cache_df: pd.DataFrame, prod_day: str, shift_type: str, w0: Decimal, w1: Decimal) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if nonop_cache_df is None or nonop_cache_df.empty:
        return out

    w0_low = w0 - Decimal(SNAP_EDGE_SEC)
    w0_high = w0 + Decimal(SNAP_EDGE_SEC)
    w1_low = w1 - Decimal(SNAP_EDGE_SEC)
    w1_high = w1 + Decimal(SNAP_EDGE_SEC)

    tmp = nonop_cache_df.copy()
    tmp["end_day"] = tmp["end_day"].astype(str)
    tmp["station"] = tmp["station"].astype(str)
    tmp["from_time"] = tmp["from_time"].astype(str)
    tmp["to_time"] = tmp["to_time"].astype(str)

    a_list: List[Decimal] = []
    b_list: List[Decimal] = []
    for r in tmp.itertuples(index=False):
        a, b = record_interval_to_offsets(prod_day, shift_type, r.end_day, r.from_time, r.to_time)
        a_list.append(a)
        b_list.append(b)

    tmp["a"] = a_list
    tmp["b"] = b_list

    def _dump(df: pd.DataFrame) -> List[Dict[str, Any]]:
        dumped: List[Dict[str, Any]] = []
        if df is None or df.empty:
            return dumped
        for rr in df.itertuples(index=False):
            a = rr.a
            b = rr.b
            inter = intersect(a, b, w0, w1)
            dumped.append({
                "end_day": rr.end_day,
                "from_time": rr.from_time,
                "to_time": rr.to_time,
                "a": float(a),
                "b": float(b),
                "a_hms": offset_to_hms(a),
                "b_hms": offset_to_hms(b),
                "inter": None if not inter else {
                    "s": float(inter[0]), "e": float(inter[1]),
                    "s_hms": offset_to_hms(inter[0]), "e_hms": offset_to_hms(inter[1]),
                },
            })
        return dumped

    by_station: Dict[str, Any] = {}

    for st in FCT_STATIONS:
        sub = tmp[tmp["station"] == st].copy()
        if sub.empty:
            by_station[st] = {"w0_edge": [], "w1_edge": []}
            continue

        c0 = sub[(sub["a"] >= w0_low) & (sub["a"] <= w0_high)].copy()
        if not c0.empty:
            c0["dist"] = (c0["a"] - w0).abs()
            c0 = c0.sort_values(["dist", "a"]).head(SNAP_ROWS_PER_EDGE)

        c1 = sub[(sub["a"] >= w1_low) & (sub["a"] <= w1_high)].copy()
        if not c1.empty:
            c1["dist"] = (c1["a"] - w1).abs()
            c1 = c1.sort_values(["dist", "a"]).head(SNAP_ROWS_PER_EDGE)

        by_station[st] = {"w0_edge": _dump(c0), "w1_edge": _dump(c1)}

    meta = {
        "edge_sec": SNAP_EDGE_SEC,
        "rows_per_edge": SNAP_ROWS_PER_EDGE,
        "w0": float(w0), "w1": float(w1),
        "w0_hms": offset_to_hms(w0), "w1_hms": offset_to_hms(w1),
        "w0_range": [float(w0_low), float(w0_high)],
        "w1_range": [float(w1_low), float(w1_high)],
    }
    return {"meta": meta, "by_station": by_station}


def snapshot_dump(
    engine: Engine,
    now: datetime,
    loop_no: int,
    cur_window: WindowState,
    last_pk: Tuple[str, str, str],
    planned_df: pd.DataFrame,
    nonop_cache_df: pd.DataFrame,
    inc_df: pd.DataFrame,
    new_station_counts: Dict[str, int],
    diag: Dict[str, Any],
    reason: str,
):
    day0 = cur_window.prod_day
    day1 = yyyymmdd(parse_yyyymmdd(day0) + timedelta(days=1))

    cache_sc = station_counts(nonop_cache_df)
    inc_sc = station_counts(inc_df)
    pks_tail = sample_last_pks(nonop_cache_df, SNAP_SAMPLE_PKS)

    try:
        db_stats = db_station_stats_for_days(engine, day0, day1)
    except Exception as e:
        db_stats = {"error": f"{type(e).__name__}: {e}"}

    w0, w1 = window_offsets_now(cur_window.prod_day, cur_window.shift_type, now)

    try:
        edge_samples = _edge_sample_rows_offsets(nonop_cache_df, cur_window.prod_day, cur_window.shift_type, w0, w1)
    except Exception as e:
        edge_samples = {"error": f"{type(e).__name__}: {e}"}

    header = {
        "ts": now.isoformat(),
        "loop": loop_no,
        "reason": reason,
        "window": {"prod_day": cur_window.prod_day, "shift": cur_window.shift_type,
                   "w0_hms": offset_to_hms(w0), "w1_hms": offset_to_hms(w1)},
        "last_pk": last_pk,
        "sleep_sec": SLEEP_SEC,
        "fetch_limit": FETCH_LIMIT,
    }

    payloads = [
        ("snapshot", f"HEADER {header}"),
        ("snapshot_cache", f"cache_rows={len(nonop_cache_df)} cache_station_counts={cache_sc} pks_tail={pks_tail}"),
        ("snapshot_inc", f"inc_raw_rows={len(inc_df)} inc_station_counts={inc_sc} new_station_counts={new_station_counts}"),
        ("snapshot_planned", f"planned_rows={len(planned_df)} planned_diag={diag.get('planned')}"),
        ("snapshot_calc", f"calc_diag={{window:{diag.get('window')}, "
                          f"fct_raw_before_sec:{diag.get('fct_raw_before_sec')}, "
                          f"fct_after_sec:{diag.get('fct_after_sec')}, "
                          f"fct_removed_by_planned_sec:{diag.get('fct_removed_by_planned_sec')}, "
                          f"vision:{diag.get('vision')}}}"),
        ("snapshot_db", f"db_station_stats(day0={day0},day1={day1})={db_stats}"),
        ("snapshot_rows", f"edge_samples={edge_samples}"),
    ]

    log("WARNING", f"[SNAPSHOT] TRIGGERED reason={reason} header={header}")
    rows = [_health_row(k, v, now) for k, v in payloads]
    _save_health_logs_df(engine, rows)


# =========================
# 9) 메인 루프
# =========================
def main():
    setup_logger()
    log("INFO", "=" * 90)
    log("INFO", "BOOT backend9 planned/nonop daemon starting (TRIGGER SNAPSHOT + snapshot_rows + FIX)")
    log("INFO", f"CFG sleep={SLEEP_SEC}s fetch_limit={FETCH_LIMIT} work_mem={PG_WORK_MEM}")
    log("INFO", f"CFG stmt_timeout={PG_STATEMENT_TIMEOUT_MS} lock_timeout={PG_LOCK_TIMEOUT_MS} idle_tx_timeout={PG_IDLE_IN_TX_TIMEOUT_MS}")
    log("INFO", f"CFG log_file={LOG_FILE}")
    log("INFO", f"CFG snapshot cooldown={SNAP_COOLDOWN_SEC}s fct4_min={SNAP_FCT4_MIN_SEC}s fct123_max={SNAP_FCT123_MAX_SEC}s drop_ratio={SNAP_DROP_RATIO}")
    log("INFO", f"CFG snapshot_rows edge=±{SNAP_EDGE_SEC}s rows_per_edge={SNAP_ROWS_PER_EDGE}")

    engine = connect_with_retry()

    try:
        _ensure_health_table(engine)
        _save_health_logs_df(engine, [_health_row("boot", "backend9 started (trigger snapshot + snapshot_rows + fix)")])
    except Exception as e:
        log("WARNING", f"[HEALTHLOG] init failed: {type(e).__name__}: {e}")

    ensure_schema(engine, SAVE_SCHEMA)

    tables_ready = False
    cur_window: Optional[WindowState] = None

    last_pk: Optional[Tuple[str, str, str]] = None
    seen_pk: Set[Tuple[str, str, str]] = set()
    nonop_rows_df: Optional[pd.DataFrame] = None

    last_heartbeat = time.time()
    loop_no = 0
    last_success_at: Optional[datetime] = None
    last_upsert_at: Optional[datetime] = None

    prev_total_vision: Optional[int] = None
    last_snapshot_ts: float = 0.0

    while True:
        loop_no += 1
        loop_started = datetime.now(KST)
        health_buffer: List[Dict[str, str]] = []

        try:
            now = datetime.now(KST).replace(microsecond=0)
            ws = window_state_now(now)

            if (cur_window is None) or (ws.prod_day != cur_window.prod_day) or (ws.shift_type != cur_window.shift_type):
                cur_window = ws
                last_pk = None
                seen_pk.clear()
                nonop_rows_df = None
                tables_ready = False
                prev_total_vision = None

                m = f"[WINDOW] changed => prod_day={ws.prod_day} shift={ws.shift_type} start={ws.start_ts.isoformat()} end(now)={ws.end_ts.isoformat()}"
                log("INFO", m)
                health_buffer.append(_health_row("window", m, now))

            planned_df = planned_fetch_for_window(engine, cur_window.prod_day)

            if last_pk is None or nonop_rows_df is None:
                m = "[BOOTSTRAP] start"
                log("INFO", m)
                health_buffer.append(_health_row("bootstrap", m, now))

                all_df = fct_nonop_bootstrap(engine, cur_window.prod_day)
                if not all_df.empty:
                    for r in all_df.itertuples(index=False):
                        seen_pk.add((str(r.end_day), str(r.station), str(r.from_time)))
                    last_row = all_df.iloc[-1]
                    last_pk = (str(last_row["end_day"]), str(last_row["station"]), str(last_row["from_time"]))
                else:
                    last_pk = ("00000000", "", "")

                nonop_rows_df = all_df[["end_day", "station", "from_time", "to_time"]].copy()

                m = f"[BOOTSTRAP] rows={len(nonop_rows_df)} last_pk={last_pk}"
                log("INFO", m)
                health_buffer.append(_health_row("bootstrap", m, now))

            inc_df = fct_nonop_incremental(engine, cur_window.prod_day, last_pk)
            raw_cnt = len(inc_df)

            new_station_counts: Dict[str, int] = {}
            new_rows = []
            if raw_cnt > 0:
                last_row_raw = inc_df.iloc[-1]
                last_pk = (str(last_row_raw["end_day"]), str(last_row_raw["station"]), str(last_row_raw["from_time"]))

                for r in inc_df.itertuples(index=False):
                    pk = (str(r.end_day), str(r.station), str(r.from_time))
                    if pk in seen_pk:
                        continue
                    seen_pk.add(pk)
                    st = str(r.station)
                    new_station_counts[st] = new_station_counts.get(st, 0) + 1
                    new_rows.append({
                        "end_day": str(r.end_day),
                        "station": st,
                        "from_time": str(r.from_time),
                        "to_time": str(r.to_time),
                    })

                if new_rows:
                    nonop_rows_df = pd.concat([nonop_rows_df, pd.DataFrame(new_rows)], ignore_index=True)

            log("INFO", f"[FETCH] raw={raw_cnt} new={len(new_rows)} cached={len(nonop_rows_df)} last_pk={last_pk}")

            df_planned = planned_rows_per_interval(
                planned_df, cur_window.prod_day, cur_window.shift_type, now, include_total_row=True
            )
            df_nonop, diag = nonop_summary_overlap_spec_from_rows(
                nonop_rows_df, planned_df, cur_window.prod_day, cur_window.shift_type, now
            )

            trig, reason = should_snapshot(diag, prev_total_vision, last_snapshot_ts)
            if trig:
                try:
                    snapshot_dump(
                        engine=engine,
                        now=now,
                        loop_no=loop_no,
                        cur_window=cur_window,
                        last_pk=last_pk,
                        planned_df=planned_df,
                        nonop_cache_df=nonop_rows_df,
                        inc_df=inc_df,
                        new_station_counts=new_station_counts,
                        diag=diag,
                        reason=reason,
                    )
                    last_snapshot_ts = time.time()
                except Exception as e:
                    log("ERROR", f"[SNAPSHOT] failed: {type(e).__name__}: {e}")
                    try:
                        _save_health_logs_df(engine, [_health_row("snapshot_error", f"{type(e).__name__}: {e}", now)])
                    except Exception:
                        pass

            try:
                vision = diag.get("vision", {}) or {}
                prev_total_vision = int(vision.get("total_after", 0))
            except Exception:
                prev_total_vision = None

            if not tables_ready:
                KEY_PLAN = ["prod_day", "shift_type", "from_time", "to_time"]
                KEY_NONOP = ["prod_day"]

                ensure_table(engine, SAVE_SCHEMA, T_PLAN_DAY,   list(df_planned.columns), KEY_PLAN)
                ensure_table(engine, SAVE_SCHEMA, T_PLAN_NIGHT, list(df_planned.columns), KEY_PLAN)
                ensure_table(engine, SAVE_SCHEMA, T_NONOP_DAY,   list(df_nonop.columns),  KEY_NONOP)
                ensure_table(engine, SAVE_SCHEMA, T_NONOP_NIGHT, list(df_nonop.columns),  KEY_NONOP)

                tables_ready = True
                log("INFO", "[DDL] tables ensured")
                health_buffer.append(_health_row("ddl", "tables ensured", now))

            KEY_PLAN = ["prod_day", "shift_type", "from_time", "to_time"]
            KEY_NONOP = ["prod_day"]

            if cur_window.shift_type == "day":
                upsert_df(engine, SAVE_SCHEMA, T_PLAN_DAY, df_planned, KEY_PLAN)
                upsert_df(engine, SAVE_SCHEMA, T_NONOP_DAY, df_nonop, KEY_NONOP)
            else:
                upsert_df(engine, SAVE_SCHEMA, T_PLAN_NIGHT, df_planned, KEY_PLAN)
                upsert_df(engine, SAVE_SCHEMA, T_NONOP_NIGHT, df_nonop, KEY_NONOP)

            last_success_at = datetime.now(KST)
            last_upsert_at = last_success_at

            elapsed = (datetime.now(KST) - loop_started).total_seconds()
            log("INFO", f"[LOOP] #{loop_no} done in {elapsed:.2f}s")
            health_buffer.append(_health_row("loop", f"loop={loop_no} ok {elapsed:.2f}s", now))

            try:
                _save_health_logs_df(engine, health_buffer)
            except Exception as e:
                log("WARNING", f"[HEALTHLOG] save failed: {type(e).__name__}: {e}")

        except (OperationalError, DBAPIError, SQLAlchemyError) as e:
            log_exception("DB error (will reconnect)", e)
            try:
                _save_health_logs_df(engine, [_health_row("error", f"db error reconnect: {type(e).__name__}: {e}")])
            except Exception:
                pass

            try:
                engine.dispose()
            except Exception:
                pass

            while True:
                try:
                    engine = connect_with_retry()
                    _ensure_health_table(engine)
                    _save_health_logs_df(engine, [_health_row("down", "db reconnected")])
                    break
                except Exception as re:
                    log("RETRY", f"Reconnect failed: {type(re).__name__}: {re} (sleep {SLEEP_SEC}s)")
                    time.sleep(SLEEP_SEC)

            tables_ready = False

        except Exception as e:
            log_exception("Unhandled error", e)
            try:
                _save_health_logs_df(engine, [_health_row("error", f"unhandled: {type(e).__name__}: {e}")])
            except Exception:
                pass

        now_ts = time.time()
        if now_ts - last_heartbeat >= HEARTBEAT_SEC:
            hb_now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
            hb_msg = (
                f"[HEARTBEAT] now={hb_now} loop={loop_no} "
                f"window={cur_window.prod_day if cur_window else '-'}:{cur_window.shift_type if cur_window else '-'} "
                f"last_pk={last_pk} cached_rows={len(nonop_rows_df) if nonop_rows_df is not None else 0} "
                f"last_success={last_success_at.strftime('%H:%M:%S') if last_success_at else '-'} "
                f"last_upsert={last_upsert_at.strftime('%H:%M:%S') if last_upsert_at else '-'}"
            )
            log("INFO", hb_msg)
            try:
                _save_health_logs_df(engine, [_health_row("heartbeat", hb_msg)])
            except Exception as e:
                log("WARNING", f"[HEALTHLOG] heartbeat save failed: {type(e).__name__}: {e}")
            last_heartbeat = now_ts

        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()