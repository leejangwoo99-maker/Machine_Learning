# -*- coding: utf-8 -*-
"""
backend9_planned_and_nonop_daemon.py

요구사항 반영 + 안정화 개선:
1) dataframe 콘솔 출력 제외 (로그만)
2) 날짜는 [WINDOW]기준 현재(KST) 날짜/시각으로 자동 전환 (window_end=now)
3) 멀티프로세스 1개
4) 무한루프 5초
5) DB 접속 실패 시 무한 재시도(블로킹)
6) 중간 끊김도 무한 재접속 후 계속
7) pool 최소화(상시 연결 1개)
8) PG_WORK_MEM 읽어서 연결마다 SET work_mem
9) 증분 조건 PK: (end_day, station, from_time) 문자열 비교
10) seen_pk set[(end_day, station, from_time)] 중복 방지
11) BOOT 로그 항상, DB 안 붙으면 RETRY 5초마다
12) 단계별 INFO 로그(last_pk / fetch / upsert)
    - fetch된 신규 row만 반영해서 집계 증분 업데이트 (in-memory)
    - last_pk는 메모리만 사용
13) 재실행 시 DELETE/TRUNCATE 금지
    - last_pk가 날아가므로 bootstrap(현재 window start~now 전체 재집계) 후 UPSERT

[변경]
- planned 결과에 from_time/to_time 구간별 1행 생성
- 데몬은 "제일 위 요약행(빈 from/to)" 포함 가능 => 포함하도록 구현
- planned 저장 테이블 UNIQUE KEY: (prod_day, shift_type, from_time, to_time)

[안정화]
- last_pk 전진 정체 버그 수정 (raw fetch만 있어도 last_pk는 전진)
- DB 무기한 대기 방지(statement_timeout/lock_timeout 등)
- 파일 로그(회전) + heartbeat 로그 추가
- 로그 경로 고정: C:\\AptivAgent\\_logs

[추가]
- 동작 로그를 DB에 저장 (schema: k_demon_heath_check, table: 9_log)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 순서: end_day, end_time, info, contents
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
from typing import Dict, List, Tuple, Optional, Set

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

# 로그 (요청: 무조건 고정 경로)
LOG_DIR = r"C:\AptivAgent\_logs"
LOG_FILE = os.path.join(LOG_DIR, "backend9_planned_and_nonop_daemon.log")
LOG_MAX_BYTES = int(os.getenv("BACKEND9_LOG_MAX_BYTES", str(20 * 1024 * 1024)))  # 20MB
LOG_BACKUP_COUNT = int(os.getenv("BACKEND9_LOG_BACKUP_COUNT", "10"))

DB_CONFIG = {
    "host": os.getenv("PG_HOST", "100.105.75.47"),
    "port": int(os.getenv("PG_PORT", "5432")),
    "dbname": os.getenv("PG_DBNAME", "postgres"),
    "user": os.getenv("PG_USER", "postgres"),
    "password": os.getenv("PG_PASSWORD", "leejangwoo1!"),
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


def _health_row(info: str, contents: str, now: Optional[datetime] = None) -> Dict[str, str]:
    ts = now or datetime.now(KST)
    return {
        "end_day": ts.strftime("%Y%m%d"),
        "end_time": ts.strftime("%H:%M:%S"),
        "info": _normalize_info(info),
        "contents": str(contents),
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
    """
    저장 순서 강제: end_day, end_time, info, contents
    """
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
    h, m, s = hms.strip().split(":")
    return Decimal(int(h)) * Decimal(3600) + Decimal(int(m)) * Decimal(60) + Decimal(s)


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


def record_interval_to_offsets(base_prod_day: str, end_day: str, from_time: str, to_time: str) -> Tuple[Decimal, Decimal]:
    base_d = parse_yyyymmdd(base_prod_day)
    d = parse_yyyymmdd(end_day)
    day_delta = (d - base_d).days

    a = Decimal(86400) * Decimal(day_delta) + parse_hms_decimal(from_time)
    b = Decimal(86400) * Decimal(day_delta) + parse_hms_decimal(to_time)

    if parse_hms_decimal(to_time) < parse_hms_decimal(from_time):
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
# 5) Fetch (bootstrap / incremental)
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


# =========================
# 6) 계산
# =========================
def build_planned_intervals_for_shift(
    planned_df: pd.DataFrame,
    prod_day: str,
    shift_type: str,
    now_ts: datetime
) -> List[Tuple[Decimal, Decimal]]:
    w0, w1 = window_offsets_now(prod_day, shift_type, now_ts)
    intervals = []
    for r in planned_df.itertuples(index=False):
        a, b = record_interval_to_offsets(prod_day, str(r.end_day), str(r.from_time), str(r.to_time))
        inter = intersect(a, b, w0, w1)
        if inter:
            intervals.append(inter)
    return merge_intervals_decimal(intervals)


def planned_rows_per_interval(
    planned_df: pd.DataFrame,
    prod_day: str,
    shift_type: str,
    now_ts: datetime,
    include_total_row: bool = True,
) -> pd.DataFrame:
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
        return pd.DataFrame(columns=["prod_day","shift_type","from_time","to_time","Total 계획 정지 시간","total_planned_time","updated_at"])

    return pd.DataFrame(rows)


def build_fct_station_raw_decimal_from_rows(
    rows_df: pd.DataFrame,
    prod_day: str,
    shift_type: str,
    now_ts: datetime,
) -> Dict[str, List[Tuple[Decimal, Decimal]]]:
    w0, w1 = window_offsets_now(prod_day, shift_type, now_ts)
    tmp: Dict[str, List[Tuple[Decimal, Decimal]]] = {st: [] for st in FCT_STATIONS}

    for r in rows_df.itertuples(index=False):
        st = str(r.station)
        if st not in tmp:
            continue
        a, b = record_interval_to_offsets(prod_day, str(r.end_day), str(r.from_time), str(r.to_time))
        inter = intersect(a, b, w0, w1)
        if inter:
            tmp[st].append(inter)

    return {st: merge_intervals_decimal(tmp[st]) for st in FCT_STATIONS}


def fct_display_seconds_after_planned(
    raw_station_dec: Dict[str, List[Tuple[Decimal, Decimal]]],
    planned_dec: List[Tuple[Decimal, Decimal]],
) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for st in FCT_STATIONS:
        dec_after = subtract_many_decimal(raw_station_dec[st], planned_dec)
        int_intervals = merge_intervals_int(to_int_intervals_half_up(dec_after))
        out[st] = sum_int_durations(int_intervals)
    return out


def vision_seconds_overlap_then_planned(
    raw_a: List[Tuple[Decimal, Decimal]],
    raw_b: List[Tuple[Decimal, Decimal]],
    planned_dec: List[Tuple[Decimal, Decimal]],
) -> int:
    overlap_dec = intersect_two_decimal_lists(raw_a, raw_b)
    after_planned_dec = subtract_many_decimal(overlap_dec, planned_dec)
    int_intervals = merge_intervals_int(to_int_intervals_half_up(after_planned_dec))
    return sum_int_durations(int_intervals)


def nonop_summary_overlap_spec_from_rows(
    rows_df: pd.DataFrame,
    planned_df: pd.DataFrame,
    prod_day: str,
    shift_type: str,
    now_ts: datetime,
) -> pd.DataFrame:
    raw_station_dec = build_fct_station_raw_decimal_from_rows(rows_df, prod_day, shift_type, now_ts)
    planned_dec = build_planned_intervals_for_shift(planned_df, prod_day, shift_type, now_ts)

    fct_sec = fct_display_seconds_after_planned(raw_station_dec, planned_dec)
    vision1_sec = vision_seconds_overlap_then_planned(raw_station_dec["FCT1"], raw_station_dec["FCT2"], planned_dec)
    vision2_sec = vision_seconds_overlap_then_planned(raw_station_dec["FCT3"], raw_station_dec["FCT4"], planned_dec)
    total_vision = vision1_sec + vision2_sec

    row = {
        "prod_day": prod_day,
        "shift_type": shift_type,
        "비가동 FCT1": sec_to_kor_str(fct_sec["FCT1"]),
        "비가동 FCT2": sec_to_kor_str(fct_sec["FCT2"]),
        "비가동 Vision1": sec_to_kor_str(vision1_sec),
        "비가동 FCT3": sec_to_kor_str(fct_sec["FCT3"]),
        "비가동 FCT4": sec_to_kor_str(fct_sec["FCT4"]),
        "비가동 Vision2": sec_to_kor_str(vision2_sec),
        "Total Vision 비가동 시간": sec_to_kor_str(total_vision),
        "vision1_non_time": int(vision1_sec),
        "vision2_non_time": int(vision2_sec),
        "total_vision_non_time": int(total_vision),
        "updated_at": now_ts,
    }
    return pd.DataFrame([row])


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
# 8) 메인 루프
# =========================
def main():
    setup_logger()
    log("INFO", "=" * 90)
    log("INFO", "BOOT backend9 planned/nonop daemon starting")
    log("INFO", f"CFG sleep={SLEEP_SEC}s fetch_limit={FETCH_LIMIT} work_mem={PG_WORK_MEM}")
    log("INFO", f"CFG stmt_timeout={PG_STATEMENT_TIMEOUT_MS} lock_timeout={PG_LOCK_TIMEOUT_MS} idle_tx_timeout={PG_IDLE_IN_TX_TIMEOUT_MS}")
    log("INFO", f"CFG log_file={LOG_FILE}")

    engine = connect_with_retry()

    # health log table 준비
    try:
        _ensure_health_table(engine)
        _save_health_logs_df(engine, [_health_row("boot", "backend9 started")])
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

                m = f"[WINDOW] changed => prod_day={ws.prod_day} shift={ws.shift_type} start={ws.start_ts.isoformat()} end(now)={ws.end_ts.isoformat()}"
                log("INFO", m)
                health_buffer.append(_health_row("window", m, now))

            planned_df = planned_fetch_for_window(engine, cur_window.prod_day)

            if last_pk is None or nonop_rows_df is None:
                m = "[BOOTSTRAP] start (load all nonop rows for prod_day+1 dates)"
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

            log("INFO", f"[LAST_PK] {last_pk}")
            inc_df = fct_nonop_incremental(engine, cur_window.prod_day, last_pk)

            new_cnt = 0
            raw_cnt = len(inc_df)

            if raw_cnt > 0:
                last_row_raw = inc_df.iloc[-1]
                last_pk = (str(last_row_raw["end_day"]), str(last_row_raw["station"]), str(last_row_raw["from_time"]))

                new_rows = []
                for r in inc_df.itertuples(index=False):
                    pk = (str(r.end_day), str(r.station), str(r.from_time))
                    if pk in seen_pk:
                        continue
                    seen_pk.add(pk)
                    new_rows.append({
                        "end_day": str(r.end_day),
                        "station": str(r.station),
                        "from_time": str(r.from_time),
                        "to_time": str(r.to_time),
                    })

                if new_rows:
                    nonop_rows_df = pd.concat([nonop_rows_df, pd.DataFrame(new_rows)], ignore_index=True)
                    new_cnt = len(new_rows)

            m = f"[FETCH] new_rows={new_cnt} (raw fetched={raw_cnt}) total_cached={len(nonop_rows_df)}"
            log("INFO", m)
            health_buffer.append(_health_row("fetch", m, now))

            df_planned = planned_rows_per_interval(
                planned_df,
                cur_window.prod_day,
                cur_window.shift_type,
                now,
                include_total_row=True
            )
            df_nonop = nonop_summary_overlap_spec_from_rows(
                nonop_rows_df,
                planned_df,
                cur_window.prod_day,
                cur_window.shift_type,
                now
            )

            if not tables_ready:
                KEY_PLAN = ["prod_day", "shift_type", "from_time", "to_time"]
                KEY_NONOP = ["prod_day"]

                ensure_table(engine, SAVE_SCHEMA, T_PLAN_DAY,   list(df_planned.columns), KEY_PLAN)
                ensure_table(engine, SAVE_SCHEMA, T_PLAN_NIGHT, list(df_planned.columns), KEY_PLAN)
                ensure_table(engine, SAVE_SCHEMA, T_NONOP_DAY,   list(df_nonop.columns),  KEY_NONOP)
                ensure_table(engine, SAVE_SCHEMA, T_NONOP_NIGHT, list(df_nonop.columns),  KEY_NONOP)

                tables_ready = True
                m = "[DDL] tables ensured"
                log("INFO", m)
                health_buffer.append(_health_row("ddl", m, now))

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
            m = f"[LOOP] #{loop_no} done in {elapsed:.2f}s"
            log("INFO", m)
            health_buffer.append(_health_row("upsert", f"loop={loop_no} upsert ok", now))
            health_buffer.append(_health_row("loop", m, now))

            # 정상 루프 health 로그 저장
            try:
                _save_health_logs_df(engine, health_buffer)
            except Exception as e:
                log("WARNING", f"[HEALTHLOG] save failed: {type(e).__name__}: {e}")

        except (OperationalError, DBAPIError, SQLAlchemyError) as e:
            log_exception("DB error (will reconnect)", e)
            err_msg = f"db error reconnect: {type(e).__name__}: {e}"
            try:
                _save_health_logs_df(engine, [_health_row("error", err_msg), _health_row("down", "db reconnect start")])
            except Exception:
                pass

            try:
                engine.dispose()
            except Exception:
                pass

            # reconnect loop에서 retry 로그도 health에 남김
            while True:
                try:
                    engine = connect_with_retry()
                    _ensure_health_table(engine)
                    _save_health_logs_df(engine, [_health_row("down", "db reconnected")])
                    break
                except Exception as re:
                    log("RETRY", f"Reconnect failed: {type(re).__name__}: {re} (sleep {SLEEP_SEC}s)")
                    try:
                        # engine 불확실하므로 skip 가능
                        pass
                    finally:
                        time.sleep(SLEEP_SEC)

            tables_ready = False

        except Exception as e:
            log_exception("Unhandled error", e)
            try:
                _save_health_logs_df(engine, [_health_row("error", f"unhandled: {type(e).__name__}: {e}")])
            except Exception:
                pass

        # heartbeat
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

        # sleep 로그
        try:
            _save_health_logs_df(engine, [_health_row("sleep", f"sleep {SLEEP_SEC}s")])
        except Exception:
            pass

        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
