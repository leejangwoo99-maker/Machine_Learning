# -*- coding: utf-8 -*-
"""
backend8_total_mes_loss_time_daemon.py

요구사항 반영:
1) DF 콘솔 출력 제외(로그만)
2) 날짜/윈도우는 현재시각(KST) 기준 자동
3) 멀티프로세스 1개
4) 무한루프 5초
5) DB 접속 실패 시 무한 재시도(블로킹)
6) 중간 끊김도 무한 재접속 후 계속
7) pool 최소화(1개 연결 유지)
8) PG_WORK_MEM 읽어서 매 연결에 SET work_mem
9) mes_fail_wasted_time 증분 PK: (end_day, station, from_time) 문자열 비교
10) seen_pk 캐시 추가(중복 방지)
11) BOOT 로그 항상, DB 안 붙으면 RETRY 5초마다
12) 단계별 INFO 로그(last_pk / fetch / upsert)
    - last_pk는 메모리만 사용
    - 신규 row만 증분 반영(메모리 누적합)
13) 재실행 시 DELETE/TRUNCATE 금지
    - last_pk가 날아가므로 현재 윈도우(start~now) bootstrap 후 UPSERT

[반영된 수정]
- fetch_increment_mes_fail(): (from,to]가 현재 윈도우(start~now)와 "겹치지 않으면" 완전 skip
  (중복방지 목적상 seen_pk에는 넣고, last_pk는 진행)

[추가 사양 반영: 데몬 헬스 로그 DB 저장]
- 스키마: k_demon_heath_check (없으면 생성)
- 테이블: "8_log" (없으면 생성)
- 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- 저장 시 DataFrame 컬럼 순서: end_day, end_time, info, contents
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, Tuple, Optional, List, Any

import pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

KST = ZoneInfo("Asia/Seoul")

# =========================
# 0) ENV / DB CONFIG
# =========================
DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),#비번은 보완 사항
}

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

MES_SCHEMA = "d1_machine_log"
T_MES_FAIL = "mes_fail_wasted_time"
T_MES_FAIL2 = "mes_fail2_wasted_time"

VISION_SCHEMA = "e2_vision_ct"
T_VISION_OP_CT = "vision_op_ct"

SAVE_SCHEMA = "i_daily_report"
T_DAY = "h_mes_wasted_time_day_daily"
T_NIGHT = "h_mes_wasted_time_night_daily"

# Health log target
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "8_log"  # 숫자 시작 테이블명 -> 반드시 quote 필요


# =========================
# 1) LOG (console + DB health table)
# =========================
def _now_kst() -> datetime:
    return datetime.now(KST)

def _fmt_day_time(dt: datetime) -> Tuple[str, str]:
    return dt.strftime("%Y%m%d"), dt.strftime("%H:%M:%S")

def _normalize_info(info: str) -> str:
    return (info or "info").strip().lower()

def _health_df(info: str, contents: str) -> pd.DataFrame:
    now = _now_kst()
    end_day, end_time = _fmt_day_time(now)
    # 반드시 이 순서
    return pd.DataFrame(
        [[end_day, end_time, _normalize_info(info), str(contents)]],
        columns=["end_day", "end_time", "info", "contents"],
    )

def ensure_health_table(engine: Engine) -> None:
    ddl_schema = f'CREATE SCHEMA IF NOT EXISTS "{HEALTH_SCHEMA}";'
    ddl_table = f"""
    CREATE TABLE IF NOT EXISTS "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (
        id bigserial PRIMARY KEY,
        end_day text NOT NULL,
        end_time text NOT NULL,
        info text NOT NULL,
        contents text,
        created_at timestamptz NOT NULL DEFAULT now()
    );
    """
    idx1 = f'CREATE INDEX IF NOT EXISTS "ix_{HEALTH_TABLE}_end_day_time" ON "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (end_day, end_time);'
    idx2 = f'CREATE INDEX IF NOT EXISTS "ix_{HEALTH_TABLE}_info" ON "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (info);'

    with engine.begin() as conn:
        conn.execute(text(ddl_schema))
        conn.execute(text(ddl_table))
        conn.execute(text(idx1))
        conn.execute(text(idx2))

def write_health_log(engine: Optional[Engine], info: str, contents: str) -> None:
    """
    DataFrame(end_day, end_time, info, contents) 형태로 생성 후 DB 저장.
    실패 시 콘솔로만 fallback (무한루프 방지 위해 여기서 재귀 로그 호출 금지).
    """
    if engine is None:
        return

    df = _health_df(info=info, contents=contents)

    sql = text(f"""
        INSERT INTO "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
    """)

    try:
        rec = df.iloc[0].to_dict()
        with engine.begin() as conn:
            conn.execute(sql, rec)
    except Exception as e:
        # DB 로그 저장 실패는 콘솔로만 남김(재귀 방지)
        now = _now_kst().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{now} [RETRY] health-log write failed: {type(e).__name__}: {e}", flush=True)

def _console(level: str, msg: str) -> None:
    print(f"{_now_kst():%Y-%m-%d %H:%M:%S} [{level}] {msg}", flush=True)

def log_boot(msg: str, engine: Optional[Engine] = None) -> None:
    _console("BOOT", msg)
    write_health_log(engine, "boot", msg)

def log_info(msg: str, engine: Optional[Engine] = None) -> None:
    _console("INFO", msg)
    write_health_log(engine, "info", msg)

def log_retry(msg: str, engine: Optional[Engine] = None) -> None:
    _console("RETRY", msg)
    write_health_log(engine, "down", msg)


# =========================
# 2) DB ENGINE + SESSION
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

def configure_session(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text("SET work_mem = :wm"), {"wm": WORK_MEM})

def connect_with_retry() -> Engine:
    while True:
        try:
            engine = make_engine()
            configure_session(engine)
            ensure_health_table(engine)  # health table 선생성
            write_health_log(engine, "info", f"db connected (work_mem={WORK_MEM})")
            return engine
        except Exception as e:
            _console("RETRY", f"DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 3) TIME / WINDOW
# =========================
@dataclass(frozen=True)
class Window:
    prod_day: str
    shift_type: str
    start: datetime
    end: datetime
    day_start: datetime
    day_end: datetime
    night_start: datetime
    night_end: datetime

def yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"

def current_window(now: datetime) -> Window:
    assert now.tzinfo is not None
    today = now.date()

    day_cut = now.replace(hour=8, minute=30, second=0, microsecond=0)
    night_cut = now.replace(hour=20, minute=30, second=0, microsecond=0)

    if day_cut <= now < night_cut:
        pd_day = today
        prod = yyyymmdd(pd_day)

        day_start = datetime(pd_day.year, pd_day.month, pd_day.day, 8, 30, 0, tzinfo=KST)
        day_end = datetime(pd_day.year, pd_day.month, pd_day.day, 20, 29, 59, tzinfo=KST)
        night_start = datetime(pd_day.year, pd_day.month, pd_day.day, 20, 30, 0, tzinfo=KST)
        pd2 = pd_day + timedelta(days=1)
        night_end = datetime(pd2.year, pd2.month, pd2.day, 8, 29, 59, tzinfo=KST)

        return Window(prod, "day", day_start, now, day_start, day_end, night_start, night_end)

    if now >= night_cut:
        pd_day = today
    else:
        pd_day = today - timedelta(days=1)

    prod = yyyymmdd(pd_day)

    day_start = datetime(pd_day.year, pd_day.month, pd_day.day, 8, 30, 0, tzinfo=KST)
    day_end = datetime(pd_day.year, pd_day.month, pd_day.day, 20, 29, 59, tzinfo=KST)
    night_start = datetime(pd_day.year, pd_day.month, pd_day.day, 20, 30, 0, tzinfo=KST)
    pd2 = pd_day + timedelta(days=1)
    night_end = datetime(pd2.year, pd2.month, pd2.day, 8, 29, 59, tzinfo=KST)

    return Window(prod, "night", night_start, now, day_start, day_end, night_start, night_end)

def prev_month_yyyymm_by_window_start(prod_day: str) -> str:
    d = datetime.strptime(prod_day, "%Y%m%d").date()
    first = date(d.year, d.month, 1)
    prev_last = first - timedelta(days=1)
    return f"{prev_last.year:04d}{prev_last.month:02d}"


# =========================
# 4) NUM / TIME PARSE
# =========================
def round_half_up_int(x: Any) -> int:
    d = Decimal(str(x))
    return int(d.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

def parse_time_text_halfup(t: str) -> Tuple[int, int, int]:
    t = (t or "").strip()
    if not t:
        return (0, 0, 0)

    hh_s, mm_s, ss_s = t.split(":")
    hh = int(hh_s)
    mm = int(mm_s)

    if "." in ss_s:
        ss = int(Decimal(ss_s).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
    else:
        ss = int(ss_s)

    if ss >= 60:
        mm += ss // 60
        ss = ss % 60
    if mm >= 60:
        hh += mm // 60
        mm = mm % 60

    return (hh, mm, ss)

def dt_from_end_day_and_time(end_day: str, hh: int, mm: int, ss: int) -> datetime:
    d = datetime.strptime(end_day, "%Y%m%d").date()
    add_days = hh // 24
    hh2 = hh % 24
    d2 = d + timedelta(days=add_days)
    return datetime(d2.year, d2.month, d2.day, hh2, mm, ss, tzinfo=KST)

def format_hms_korean(total_sec: int) -> str:
    total_sec = int(total_sec or 0)
    if total_sec <= 0:
        return "0초"
    h = total_sec // 3600
    m = (total_sec % 3600) // 60
    s = total_sec % 60
    parts = []
    if h: parts.append(f"{h}시간")
    if m: parts.append(f"{m}분")
    if s: parts.append(f"{s}초")
    return " ".join(parts)

def count_end_seconds_in_window(from_int: int, to_int: int, win_a: int, win_b: int) -> int:
    if to_int <= from_int:
        return 0
    lo = max(from_int + 1, win_a)
    hi = min(to_int, win_b)
    if hi < lo:
        return 0
    return hi - lo + 1


# =========================
# 5) mes_fail_wasted_time split
# =========================
def split_row_day_night(
    end_day: str,
    from_time_txt: str,
    to_time_txt: str,
    wasted_time_db: Any,
    w: Window,
) -> Tuple[int, int, int, int]:
    fh, fm, fs = parse_time_text_halfup(from_time_txt)
    th, tm, ts = parse_time_text_halfup(to_time_txt)

    from_dt = dt_from_end_day_and_time(end_day, fh, fm, fs)
    to_dt = dt_from_end_day_and_time(end_day, th, tm, ts)

    if to_dt <= from_dt:
        to_dt = to_dt + timedelta(days=1)

    from_int = int(from_dt.timestamp())
    to_int = int(to_dt.timestamp())
    total_s = max(0, to_int - from_int)

    day_a, day_b = int(w.day_start.timestamp()), int(w.day_end.timestamp())
    night_a, night_b = int(w.night_start.timestamp()), int(w.night_end.timestamp())

    day_s = count_end_seconds_in_window(from_int, to_int, day_a, day_b)
    night_s = count_end_seconds_in_window(from_int, to_int, night_a, night_b)

    total_w = round_half_up_int(wasted_time_db)

    if total_s <= 0:
        if day_a <= to_int <= day_b:
            return total_w, 0, 1, 0
        if night_a <= to_int <= night_b:
            return 0, total_w, 0, 1
        return 0, 0, 0, 0

    if day_s + night_s == 0:
        return 0, 0, 0, 0

    denom = day_s + night_s
    day_w = round_half_up_int(Decimal(total_w) * Decimal(day_s) / Decimal(denom))
    night_w = total_w - day_w

    crossed = (day_s > 0 and night_s > 0)

    if not crossed:
        if day_s > 0:
            return day_w, 0, 1, 0
        if night_s > 0:
            return 0, night_w, 0, 1
        return 0, 0, 0, 0

    if day_w >= night_w:
        return day_w, night_w, 1, 0
    else:
        return day_w, night_w, 0, 1

def mes_fail_days_for_window(w: Window) -> List[str]:
    d = datetime.strptime(w.prod_day, "%Y%m%d").date()
    if w.shift_type == "day":
        return [w.prod_day]
    return [w.prod_day, yyyymmdd(d + timedelta(days=1))]


# =========================
# 6) mes_fail2 helpers
# =========================
def parse_end_ts(end_day: str, end_time_txt: str) -> datetime:
    hh, mm, ss = map(int, end_time_txt.split(":"))
    d = datetime.strptime(end_day, "%Y%m%d").date()
    return datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=KST)

def mes_fail2_days_for_window(w: Window) -> List[str]:
    d = datetime.strptime(w.prod_day, "%Y%m%d").date()
    if w.shift_type == "day":
        return [w.prod_day]
    return [w.prod_day, yyyymmdd(d + timedelta(days=1))]


# =========================
# 7) SAVE TABLE (ensure + upsert)
# =========================
def ensure_schema(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SAVE_SCHEMA}";'))

def ensure_table(engine: Engine, table: str) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS "{SAVE_SCHEMA}"."{table}" (
        prod_day date NOT NULL,
        shift_type text,
        "MES 불량 손실시간(초)" integer,
        "MES 불량 제품 개수" integer,
        "재작업 시간(초)" integer,
        "FCT 재작업 시간(초)" integer,
        "Total MES 불량 손실 시간(초)" integer,
        "Total MES 불량 손실 시간" text,
        updated_at timestamptz,
        CONSTRAINT "{table}__uq_prod_day" UNIQUE (prod_day)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))

def upsert_daily(engine: Engine, table: str, row: dict) -> None:
    sql = text(f"""
    INSERT INTO "{SAVE_SCHEMA}"."{table}" (
        prod_day, shift_type,
        "MES 불량 손실시간(초)", "MES 불량 제품 개수",
        "재작업 시간(초)", "FCT 재작업 시간(초)",
        "Total MES 불량 손실 시간(초)", "Total MES 불량 손실 시간",
        updated_at
    ) VALUES (
        :prod_day, :shift_type,
        :mes_loss_sec, :mes_cnt,
        :rework_sec, :fct_rework_sec,
        :total_sec, :total_hms,
        :updated_at
    )
    ON CONFLICT (prod_day) DO UPDATE SET
        shift_type = COALESCE(EXCLUDED.shift_type, "{SAVE_SCHEMA}"."{table}".shift_type),
        "MES 불량 손실시간(초)" = COALESCE(EXCLUDED."MES 불량 손실시간(초)", "{SAVE_SCHEMA}"."{table}"."MES 불량 손실시간(초)"),
        "MES 불량 제품 개수" = COALESCE(EXCLUDED."MES 불량 제품 개수", "{SAVE_SCHEMA}"."{table}"."MES 불량 제품 개수"),
        "재작업 시간(초)" = COALESCE(EXCLUDED."재작업 시간(초)", "{SAVE_SCHEMA}"."{table}"."재작업 시간(초)"),
        "FCT 재작업 시간(초)" = COALESCE(EXCLUDED."FCT 재작업 시간(초)", "{SAVE_SCHEMA}"."{table}"."FCT 재작업 시간(초)"),
        "Total MES 불량 손실 시간(초)" = COALESCE(EXCLUDED."Total MES 불량 손실 시간(초)", "{SAVE_SCHEMA}"."{table}"."Total MES 불량 손실 시간(초)"),
        "Total MES 불량 손실 시간" = COALESCE(EXCLUDED."Total MES 불량 손실 시간", "{SAVE_SCHEMA}"."{table}"."Total MES 불량 손실 시간"),
        updated_at = COALESCE(EXCLUDED.updated_at, "{SAVE_SCHEMA}"."{table}".updated_at)
    ;
    """)

    params = {
        "prod_day": row["prod_day"],
        "shift_type": row["shift_type"],
        "mes_loss_sec": int(row.get("MES 불량 손실시간(초)", 0) or 0),
        "mes_cnt": int(row.get("MES 불량 제품 개수", 0) or 0),
        "rework_sec": int(row.get("재작업 시간(초)", 0) or 0),
        "fct_rework_sec": int(row.get("FCT 재작업 시간(초)", 0) or 0),
        "total_sec": int(row.get("Total MES 불량 손실 시간(초)", 0) or 0),
        "total_hms": row.get("Total MES 불량 손실 시간", None),
        "updated_at": row.get("updated_at", None),
    }

    with engine.begin() as conn:
        conn.execute(sql, params)


# =========================
# 8) vision_op_ct cache
# =========================
@dataclass
class VisionCache:
    cached_prev_month: Optional[str] = None
    v1: float = 0.0
    v2: float = 0.0

def refresh_vision_cache_if_needed(engine: Engine, w: Window, cache: VisionCache) -> None:
    prev_m = prev_month_yyyymm_by_window_start(w.prod_day)
    if cache.cached_prev_month == prev_m:
        return

    log_info(f"[VISION] refresh prev_month={prev_m}", engine)
    sql = text(f"""
        SELECT DISTINCT ON (station)
               station, del_out_op_ct_av, updated_at
        FROM {VISION_SCHEMA}.{T_VISION_OP_CT}
        WHERE month = :m
          AND station IN ('Vision1_only','Vision2_only')
          AND remark = 'PD'
        ORDER BY station, updated_at DESC NULLS LAST
    """)
    df = pd.read_sql(sql, engine, params={"m": prev_m})

    v1 = 0.0
    v2 = 0.0
    for r in df.itertuples(index=False):
        if str(r.station) == "Vision1_only":
            v1 = float(r.del_out_op_ct_av) if r.del_out_op_ct_av is not None else 0.0
        elif str(r.station) == "Vision2_only":
            v2 = float(r.del_out_op_ct_av) if r.del_out_op_ct_av is not None else 0.0

    cache.cached_prev_month = prev_m
    cache.v1 = v1
    cache.v2 = v2

def compute_rework_time_sec(cache: VisionCache, mes_fail_cnt: int) -> int:
    per = (cache.v1 + cache.v2) / 4.0
    return round_half_up_int(per * mes_fail_cnt)


# =========================
# 9) STATE
# =========================
@dataclass
class State:
    window_key: Optional[Tuple[str, str]] = None
    last_pk_fail: Optional[Tuple[str, str, str]] = None
    seen_pk_fail: set = None
    fail_loss_sec: int = 0
    fail_cnt: int = 0

    last_pk_fail2: Optional[Tuple[str, str, str, str]] = None
    seen_pk_fail2: set = None
    fct_rework_sec: int = 0

    def __post_init__(self):
        if self.seen_pk_fail is None:
            self.seen_pk_fail = set()
        if self.seen_pk_fail2 is None:
            self.seen_pk_fail2 = set()

def reset_for_new_window(state: State, w: Window) -> None:
    state.window_key = (w.prod_day, w.shift_type)
    state.last_pk_fail = None
    state.seen_pk_fail = set()
    state.fail_loss_sec = 0
    state.fail_cnt = 0

    state.last_pk_fail2 = None
    state.seen_pk_fail2 = set()
    state.fct_rework_sec = 0


# =========================
# 10) BOOTSTRAP + INCREMENT
# =========================
def bootstrap_mes_fail(engine: Engine, w: Window, state: State) -> None:
    days = mes_fail_days_for_window(w)
    log_info(f"[BOOTSTRAP] mes_fail_wasted_time days={days} range={w.start}~{w.end}", engine)

    sql = text(f"""
        SELECT end_day, station, from_time, to_time, wasted_time
        FROM {MES_SCHEMA}.{T_MES_FAIL}
        WHERE end_day = ANY(:days)
        ORDER BY end_day, station, from_time
    """)
    df = pd.read_sql(sql, engine, params={"days": days})

    max_pk = None
    loss = 0
    cnt = 0
    seen = set()

    for r in df.itertuples(index=False):
        end_day = str(r.end_day)
        station = str(r.station)
        from_time = str(r.from_time)
        pk = (end_day, station, from_time)
        seen.add(pk)

        day_w, night_w, day_c, night_c = split_row_day_night(
            end_day=end_day,
            from_time_txt=str(r.from_time),
            to_time_txt=str(r.to_time),
            wasted_time_db=r.wasted_time,
            w=w,
        )

        if w.shift_type == "day":
            loss += int(day_w)
            cnt += int(day_c)
        else:
            loss += int(night_w)
            cnt += int(night_c)

        if (max_pk is None) or (pk > max_pk):
            max_pk = pk

    state.fail_loss_sec = int(loss)
    state.fail_cnt = int(cnt)
    state.last_pk_fail = max_pk
    state.seen_pk_fail = seen

    log_info(f"[BOOTSTRAP] mes_fail done rows={len(df)} last_pk={state.last_pk_fail} loss={loss} cnt={cnt}", engine)

def fetch_increment_mes_fail(engine: Engine, w: Window, state: State) -> int:
    days = mes_fail_days_for_window(w)

    if state.last_pk_fail is None:
        return 0

    d0, s0, f0 = state.last_pk_fail

    sql = text(f"""
        SELECT end_day, station, from_time, to_time, wasted_time
        FROM {MES_SCHEMA}.{T_MES_FAIL}
        WHERE end_day = ANY(:days)
          AND (end_day, station, from_time) > (:d0, :s0, :f0)
        ORDER BY end_day, station, from_time
    """)
    df = pd.read_sql(sql, engine, params={"days": days, "d0": d0, "s0": s0, "f0": f0})

    if df.empty:
        return 0

    log_info(f"[FETCH] mes_fail new_rows={len(df)} from last_pk={state.last_pk_fail}", engine)

    max_pk = state.last_pk_fail
    applied = 0
    win_a = int(w.start.timestamp())
    win_b = int(w.end.timestamp())

    for r in df.itertuples(index=False):
        pk = (str(r.end_day), str(r.station), str(r.from_time))

        if pk in state.seen_pk_fail:
            if pk > max_pk:
                max_pk = pk
            continue

        fh, fm, fs = parse_time_text_halfup(str(r.from_time))
        th, tm, ts = parse_time_text_halfup(str(r.to_time))
        from_dt = dt_from_end_day_and_time(str(r.end_day), fh, fm, fs)
        to_dt = dt_from_end_day_and_time(str(r.end_day), th, tm, ts)
        if to_dt <= from_dt:
            to_dt = to_dt + timedelta(days=1)

        from_int = int(from_dt.timestamp())
        to_int = int(to_dt.timestamp())

        if to_int < win_a or (from_int + 1) > win_b:
            state.seen_pk_fail.add(pk)
            if pk > max_pk:
                max_pk = pk
            continue

        state.seen_pk_fail.add(pk)

        day_w, night_w, day_c, night_c = split_row_day_night(
            end_day=str(r.end_day),
            from_time_txt=str(r.from_time),
            to_time_txt=str(r.to_time),
            wasted_time_db=r.wasted_time,
            w=w,
        )

        if w.shift_type == "day":
            state.fail_loss_sec += int(day_w)
            state.fail_cnt += int(day_c)
        else:
            state.fail_loss_sec += int(night_w)
            state.fail_cnt += int(night_c)

        applied += 1
        if pk > max_pk:
            max_pk = pk

    state.last_pk_fail = max_pk
    log_info(f"[FETCH] mes_fail applied={applied} new_last_pk={state.last_pk_fail}", engine)
    return applied

def bootstrap_mes_fail2(engine: Engine, w: Window, state: State) -> None:
    days = mes_fail2_days_for_window(w)
    log_info(f"[BOOTSTRAP] mes_fail2_wasted_time days={days} range={w.start}~{w.end}", engine)

    sql = text(f"""
        SELECT barcode_information, end_day, end_time, station, final_ct
        FROM {MES_SCHEMA}.{T_MES_FAIL2}
        WHERE end_day = ANY(:days)
        ORDER BY barcode_information, end_day, end_time, station
    """)
    df = pd.read_sql(sql, engine, params={"days": days})

    max_pk = None
    seen = set()
    total = Decimal("0")

    for r in df.itertuples(index=False):
        end_ts = parse_end_ts(str(r.end_day), str(r.end_time))
        if not (w.start <= end_ts <= w.end):
            continue

        pk = (str(r.barcode_information), str(r.end_day), str(r.end_time), str(r.station))
        seen.add(pk)

        ct = r.final_ct if r.final_ct is not None else 0
        total += Decimal(str(ct))

        if (max_pk is None) or (pk > max_pk):
            max_pk = pk

    state.fct_rework_sec = round_half_up_int(total)
    state.last_pk_fail2 = max_pk
    state.seen_pk_fail2 = seen

    log_info(
        f"[BOOTSTRAP] mes_fail2 done scanned={len(df)} in_window={len(seen)} "
        f"last_pk={state.last_pk_fail2} fct_sum={state.fct_rework_sec}",
        engine
    )

def fetch_increment_mes_fail2(engine: Engine, w: Window, state: State) -> int:
    days = mes_fail2_days_for_window(w)

    if state.last_pk_fail2 is None:
        return 0

    b0, d0, t0, s0 = state.last_pk_fail2

    sql = text(f"""
        SELECT barcode_information, end_day, end_time, station, final_ct
        FROM {MES_SCHEMA}.{T_MES_FAIL2}
        WHERE end_day = ANY(:days)
          AND (barcode_information, end_day, end_time, station) > (:b0, :d0, :t0, :s0)
        ORDER BY barcode_information, end_day, end_time, station
    """)
    df = pd.read_sql(sql, engine, params={"days": days, "b0": b0, "d0": d0, "t0": t0, "s0": s0})

    if df.empty:
        return 0

    log_info(f"[FETCH] mes_fail2 new_rows={len(df)} from last_pk2={state.last_pk_fail2}", engine)

    max_pk = state.last_pk_fail2
    applied = 0
    add_total = Decimal("0")

    for r in df.itertuples(index=False):
        pk = (str(r.barcode_information), str(r.end_day), str(r.end_time), str(r.station))
        if pk in state.seen_pk_fail2:
            if pk > max_pk:
                max_pk = pk
            continue

        end_ts = parse_end_ts(str(r.end_day), str(r.end_time))
        if w.start <= end_ts <= w.end:
            state.seen_pk_fail2.add(pk)
            ct = r.final_ct if r.final_ct is not None else 0
            add_total += Decimal(str(ct))
            applied += 1

        if pk > max_pk:
            max_pk = pk

    if applied > 0:
        state.fct_rework_sec += round_half_up_int(add_total)

    state.last_pk_fail2 = max_pk
    log_info(
        f"[FETCH] mes_fail2 applied={applied} add={round_half_up_int(add_total)} new_last_pk2={state.last_pk_fail2}",
        engine
    )
    return applied


# =========================
# 11) BUILD ROW + UPSERT
# =========================
def build_row_for_upsert(w: Window, state: State, vcache: VisionCache) -> dict:
    prod_day_date = datetime.strptime(w.prod_day, "%Y%m%d").date()

    mes_loss = int(state.fail_loss_sec)
    mes_cnt = int(state.fail_cnt)
    rework_sec = int(compute_rework_time_sec(vcache, mes_cnt))
    fct_sec = int(state.fct_rework_sec)

    total_sec = round_half_up_int(Decimal(mes_loss + rework_sec + fct_sec))

    return {
        "prod_day": prod_day_date,
        "shift_type": w.shift_type,
        "MES 불량 손실시간(초)": mes_loss,
        "MES 불량 제품 개수": mes_cnt,
        "재작업 시간(초)": rework_sec,
        "FCT 재작업 시간(초)": fct_sec,
        "Total MES 불량 손실 시간(초)": int(total_sec),
        "Total MES 불량 손실 시간": format_hms_korean(int(total_sec)),
        "updated_at": datetime.now(KST),
    }

def target_table(shift_type: str) -> str:
    return T_DAY if shift_type == "day" else T_NIGHT


# =========================
# 12) MAIN LOOP
# =========================
def main():
    log_boot("backend8 total MES loss-time daemon starting", None)

    engine = connect_with_retry()
    log_info(f"DB connected (work_mem={WORK_MEM})", engine)

    ensure_schema(engine)
    ensure_table(engine, T_DAY)
    ensure_table(engine, T_NIGHT)
    ensure_health_table(engine)  # 안전하게 재확인

    state = State()
    vcache = VisionCache()

    while True:
        now = datetime.now(KST)

        try:
            w = current_window(now)
            key = (w.prod_day, w.shift_type)

            if state.window_key != key:
                log_info(f"[WINDOW] changed => prod_day={w.prod_day} shift={w.shift_type} start={w.start} end=now", engine)
                reset_for_new_window(state, w)

                refresh_vision_cache_if_needed(engine, w, vcache)

                bootstrap_mes_fail(engine, w, state)
                bootstrap_mes_fail2(engine, w, state)

                row = build_row_for_upsert(w, state, vcache)
                upsert_daily(engine, target_table(w.shift_type), row)
                log_info(
                    f"[UPSERT] {SAVE_SCHEMA}.{target_table(w.shift_type)} prod_day={row['prod_day']} "
                    f"shift={w.shift_type} total={row['Total MES 불량 손실 시간(초)']}",
                    engine
                )

                write_health_log(engine, "sleep", f"loop sleep {LOOP_INTERVAL_SEC}s")
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            refresh_vision_cache_if_needed(engine, w, vcache)

            log_info(f"[LAST_PK] fail={state.last_pk_fail} fail2={state.last_pk_fail2}", engine)
            n1 = fetch_increment_mes_fail(engine, w, state)
            n2 = fetch_increment_mes_fail2(engine, w, state)

            row = build_row_for_upsert(w, state, vcache)
            upsert_daily(engine, target_table(w.shift_type), row)
            log_info(
                f"[UPSERT] {SAVE_SCHEMA}.{target_table(w.shift_type)} prod_day={row['prod_day']} "
                f"shift={w.shift_type} new_fail={n1} new_fail2={n2} total={row['Total MES 불량 손실 시간(초)']}",
                engine
            )

            write_health_log(engine, "sleep", f"loop sleep {LOOP_INTERVAL_SEC}s")
            time_mod.sleep(LOOP_INTERVAL_SEC)

        except (OperationalError, DBAPIError) as e:
            log_retry(f"DB error: {type(e).__name__}: {e}", engine)
            write_health_log(engine, "down", f"db error -> reconnect start: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

            engine = connect_with_retry()
            log_info("DB reconnected", engine)

            try:
                configure_session(engine)
            except Exception as ee:
                log_retry(f"SET work_mem failed after reconnect: {type(ee).__name__}: {ee}", engine)

            try:
                now2 = datetime.now(KST)
                w2 = current_window(now2)
                log_info(f"[RECOVER] bootstrap after reconnect prod_day={w2.prod_day} shift={w2.shift_type}", engine)

                reset_for_new_window(state, w2)
                refresh_vision_cache_if_needed(engine, w2, vcache)
                bootstrap_mes_fail(engine, w2, state)
                bootstrap_mes_fail2(engine, w2, state)

                row = build_row_for_upsert(w2, state, vcache)
                upsert_daily(engine, target_table(w2.shift_type), row)
                log_info(
                    f"[UPSERT] recover {SAVE_SCHEMA}.{target_table(w2.shift_type)} prod_day={row['prod_day']} "
                    f"shift={w2.shift_type} total={row['Total MES 불량 손실 시간(초)']}",
                    engine
                )
            except Exception as ee:
                log_retry(f"recover failed: {type(ee).__name__}: {ee}", engine)
                write_health_log(engine, "error", f"recover failed: {type(ee).__name__}: {ee}")

        except Exception as e:
            log_retry(f"Unhandled error: {type(e).__name__}: {e}", engine)
            write_health_log(engine, "error", f"unhandled: {type(e).__name__}: {e}")
            time_mod.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    main()
