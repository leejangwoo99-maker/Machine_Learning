# -*- coding: utf-8 -*-
"""
7_total_afa_wasted_time.py
-------------------------------------------------
Backend-7: AFA wasted time (day/night) daemon

- Logs to BOTH console and rotating file (Nuitka exe friendly)
- Log path FIXED:
  C:\AptivAgent\_logs\backend7_afa.log

요구사항 반영(최종):
(사용자 원문 그대로 유지)

컬럼:
- prod_day (text)
- shift_type (text)
- Total 조립 불량 손실 시간 (text; "h시간 m분 s초", 0 단위 제외)
- updated_at (timestamptz, KST)

[추가 반영]
- DB 로그 모니터링 저장
  스키마: k_demon_heath_check (없으면 생성)
  테이블: "7_log" (없으면 생성)
  컬럼: end_day, end_time, info, contents
  - end_day: yyyymmdd
  - end_time: hh:mi:ss
  - info: 반드시 소문자
  - contents: 나머지 내용
- end_day, end_time, info, contents 순서로 dataframe화하여 저장
"""

from __future__ import annotations

import os
import re
import sys
import time as time_mod
import traceback
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime, date, timedelta
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple, Set, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")

# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    # ✅ exe 환경에서는 환경변수로 override 가능하게 해두는 걸 추천
    "password": os.getenv("PGPASSWORD", "leejangwoo1!"),
}

AFA_SCHEMA = "d1_machine_log"
AFA_TABLE = "afa_fail_wasted_time"

CT_SCHEMA = "e1_FCT_ct"
CT_TABLE = "fct_whole_op_ct"

SAVE_SCHEMA = "i_daily_report"
T_DAY = "g_afa_wasted_time_day_daily"
T_NIGHT = "g_afa_wasted_time_night_daily"

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
COL_TOTAL = "Total 조립 불량 손실 시간"

# ✅ 로그 경로 고정
FIXED_LOG_DIR = r"C:\AptivAgent\_logs"
LOG_FILENAME = "backend7_afa.log"

# ✅ DB 로그 저장 대상(요구사항)
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "7_log"  # 숫자로 시작하므로 항상 쿼트 사용

# =========================
# 1) 로깅 (콘솔 + 파일, rotating)
# =========================
_IDENT_RE = re.compile(r"^[A-Za-z0-9_]+$")


def _app_base_dir() -> str:
    try:
        if getattr(sys, "frozen", False):
            return os.path.dirname(sys.executable)
    except Exception:
        pass
    return os.path.dirname(os.path.abspath(__file__))


def _ensure_log_dir() -> str:
    """
    요구사항: 로그를 무조건 C:\\AptivAgent\\_logs 로 고정
    - 생성 실패 시에도 동일 경로를 반환하고, 파일 핸들러에서 예외를 잡아 콘솔만 유지.
    """
    try:
        os.makedirs(FIXED_LOG_DIR, exist_ok=True)
    except Exception:
        pass
    return FIXED_LOG_DIR


def _init_logger() -> logging.Logger:
    log_dir = _ensure_log_dir()
    log_path = os.path.join(log_dir, LOG_FILENAME)

    logger = logging.getLogger("backend7_afa")
    logger.setLevel(logging.INFO)
    logger.propagate = False

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # 파일 핸들러 (고정 경로)
    try:
        fh = RotatingFileHandler(
            log_path,
            maxBytes=5_000_000,
            backupCount=5,
            encoding="utf-8",
            delay=True,
        )
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        file_handler_ok = True
    except Exception:
        file_handler_ok = False

    # 콘솔 핸들러(항상)
    sh = logging.StreamHandler(stream=sys.stdout)
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    if file_handler_ok:
        logger.info(f"[BOOT] logger initialized | FIXED dir={log_dir} | file={log_path}")
    else:
        logger.error(
            f"[BOOT] logger file handler failed | FIXED dir={log_dir} | file={log_path} | console-only mode"
        )

    return logger


LOGGER = _init_logger()


def quote_ident(name: str) -> str:
    if not _IDENT_RE.match(name):
        raise ValueError(f"Unsafe identifier: {name!r}")
    return f'"{name}"'


# =========================
# 1-1) DB health log buffer / writer
# =========================
HEALTH_ENGINE: Optional[Engine] = None
HEALTH_TABLE_READY: bool = False
HEALTH_LOG_BUFFER: List[Dict[str, str]] = []


def _norm_info(level: str, msg: str) -> str:
    """
    info 컬럼은 반드시 소문자.
    예: error, down, sleep 등
    """
    lv = (level or "").lower()
    mm = (msg or "").lower()

    if lv in ("retry", "error"):
        return "error"
    if "sleep" in mm:
        return "sleep"
    if "down" in mm:
        return "down"
    if "boot" in lv:
        return "boot"
    return "info"


def _build_health_row(info: str, contents: str, now: Optional[datetime] = None) -> Dict[str, str]:
    dt = now or datetime.now(tz=KST)
    return {
        "end_day": dt.strftime("%Y%m%d"),
        "end_time": dt.strftime("%H:%M:%S"),
        "info": (info or "info").lower(),
        "contents": str(contents)[:4000],  # 과도한 길이 방지
    }


def _append_health_log(info: str, contents: str) -> None:
    # 요구사항: end_day, end_time, info, contents 순서
    HEALTH_LOG_BUFFER.append(_build_health_row(info=info, contents=contents))


def _ensure_health_table(engine: Engine) -> None:
    global HEALTH_TABLE_READY
    if HEALTH_TABLE_READY:
        return

    with engine.begin() as conn:
        conn.execute(text("SET work_mem = :wm"), {"wm": WORK_MEM})
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{HEALTH_SCHEMA}";'))
        conn.execute(
            text(
                f'''
                CREATE TABLE IF NOT EXISTS "{HEALTH_SCHEMA}"."{HEALTH_TABLE}" (
                    "end_day"  text,
                    "end_time" text,
                    "info"     text,
                    "contents" text
                );
                '''
            )
        )
    HEALTH_TABLE_READY = True


def _flush_health_logs() -> None:
    """
    버퍼를 dataframe으로 만들어 DB 저장.
    실패 시 버퍼 유지.
    """
    global HEALTH_LOG_BUFFER, HEALTH_ENGINE

    if not HEALTH_LOG_BUFFER:
        return
    if HEALTH_ENGINE is None:
        return

    try:
        _ensure_health_table(HEALTH_ENGINE)

        # 요구 순서대로 DataFrame 구성
        df = pd.DataFrame(HEALTH_LOG_BUFFER, columns=["end_day", "end_time", "info", "contents"])
        if df.empty:
            return

        rows = df.to_dict(orient="records")
        ins = text(
            f'''
            INSERT INTO "{HEALTH_SCHEMA}"."{HEALTH_TABLE}"
            ("end_day","end_time","info","contents")
            VALUES (:end_day,:end_time,:info,:contents)
            '''
        )

        with HEALTH_ENGINE.begin() as conn:
            conn.execute(text("SET work_mem = :wm"), {"wm": WORK_MEM})
            conn.execute(ins, rows)

        HEALTH_LOG_BUFFER = []
    except Exception:
        # DB 로그 저장 실패 시 버퍼 유지 (유실 방지)
        pass


def log(level: str, msg: str) -> None:
    lvl = (level or "INFO").upper()

    if lvl in ("BOOT", "INFO"):
        LOGGER.info(f"[{lvl}] {msg}")
    elif lvl in ("WARN", "WARNING"):
        LOGGER.warning(f"[{lvl}] {msg}")
    elif lvl in ("RETRY", "ERROR"):
        LOGGER.error(f"[{lvl}] {msg}")
    else:
        LOGGER.info(f"[{lvl}] {msg}")

    # DB health log buffer 적재 (info는 소문자)
    info = _norm_info(lvl, msg)
    _append_health_log(info=info, contents=f"[{lvl}] {msg}")

    # 가능한 경우 즉시 flush 시도 (실패해도 계속 진행)
    _flush_health_logs()


def log_exc(prefix: str, e: BaseException) -> None:
    tb = traceback.format_exc()
    log("RETRY", f"{prefix}: {type(e).__name__}: {e}\n{tb}")


# =========================
# 2) DB 엔진/재접속
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
            "connect_timeout": 5,  # ✅ exe에서 “그냥 멈춤”을 줄이기 위해
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        },
    )


def ensure_session(conn) -> None:
    conn.execute(text("SET work_mem = :wm"), {"wm": WORK_MEM})


def connect_with_retry() -> Engine:
    global HEALTH_ENGINE, HEALTH_TABLE_READY

    log("BOOT", "backend7 afa wasted time daemon starting")

    # ✅ exe 환경 진단 로그
    log("INFO", f"frozen={getattr(sys, 'frozen', False)}")
    log("INFO", f"executable={sys.executable}")
    log("INFO", f"cwd={os.getcwd()}")
    log("INFO", f"argv={sys.argv}")
    log("INFO", f"python={sys.version}")
    log("INFO", f"BASE_DIR={_app_base_dir()}")
    log("INFO", f"LOG_DIR_FIXED={FIXED_LOG_DIR}")
    log(
        "INFO",
        f"DB target={DB_CONFIG['host']}:{DB_CONFIG['port']} db={DB_CONFIG['dbname']} user={DB_CONFIG['user']} work_mem={WORK_MEM}",
    )

    while True:
        try:
            engine = make_engine()
            with engine.begin() as conn:
                conn.execute(text("SELECT 1"))
                ensure_session(conn)
            log("INFO", "DB connected OK")

            # health logger 대상으로 엔진 바인딩/테이블 준비
            HEALTH_ENGINE = engine
            HEALTH_TABLE_READY = False
            _ensure_health_table(engine)
            _flush_health_logs()

            return engine
        except Exception as e:
            # 아직 HEALTH_ENGINE이 없을 수 있으므로 buffer만 쌓임
            log_exc("DB connect failed", e)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            _append_health_log("sleep", f"db reconnect sleep {DB_RETRY_INTERVAL_SEC}s")


# =========================
# 3) 식별자 resolve
# =========================
def resolve_table_fqn(engine: Engine, preferred_schema: str, preferred_table: str) -> str:
    q = text(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE lower(table_name) = lower(:table_name)
          AND lower(table_schema) = lower(:schema_name)
        LIMIT 1
    """
    )
    with engine.begin() as conn:
        ensure_session(conn)
        row = conn.execute(
            q, {"schema_name": preferred_schema, "table_name": preferred_table}
        ).fetchone()
        if row is None:
            q2 = text(
                """
                SELECT table_schema, table_name
                FROM information_schema.tables
                WHERE lower(table_name) = lower(:table_name)
                ORDER BY table_schema, table_name
                LIMIT 1
            """
            )
            row = conn.execute(q2, {"table_name": preferred_table}).fetchone()

    if row is None:
        raise RuntimeError(f"Table not found: preferred={preferred_schema}.{preferred_table}")

    schema_actual, table_actual = row[0], row[1]
    return f"{quote_ident(schema_actual)}.{quote_ident(table_actual)}"


# =========================
# 4) 시간/윈도우
# =========================
def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def yyyymmdd(d: date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def current_window(now: datetime) -> Tuple[str, str, datetime, datetime]:
    assert now.tzinfo is not None
    today = now.date()

    day_start = datetime(today.year, today.month, today.day, 8, 30, 0, tzinfo=KST)
    day_end_fixed = datetime(today.year, today.month, today.day, 20, 29, 59, tzinfo=KST)
    night_start = datetime(today.year, today.month, today.day, 20, 30, 0, tzinfo=KST)

    if day_start <= now <= day_end_fixed:
        return yyyymmdd(today), "day", day_start, now

    if now >= night_start:
        return yyyymmdd(today), "night", night_start, now

    yday = today - timedelta(days=1)
    night_ws = datetime(yday.year, yday.month, yday.day, 20, 30, 0, tzinfo=KST)
    return yyyymmdd(yday), "night", night_ws, now


def fixed_shift_bounds(prod_day: str, shift: str) -> Tuple[datetime, datetime]:
    d = parse_yyyymmdd(prod_day)
    if shift == "day":
        ws = datetime(d.year, d.month, d.day, 8, 30, 0, tzinfo=KST)
        we = datetime(d.year, d.month, d.day, 20, 29, 59, tzinfo=KST)
        return ws, we
    if shift == "night":
        ws = datetime(d.year, d.month, d.day, 20, 30, 0, tzinfo=KST)
        d1 = d + timedelta(days=1)
        we = datetime(d1.year, d1.month, d1.day, 8, 29, 59, tzinfo=KST)
        return ws, we
    raise ValueError("shift must be day/night")


def prev_month_by_window_start(prod_day: str, shift: str) -> str:
    from dateutil.relativedelta import relativedelta

    ws, _ = fixed_shift_bounds(prod_day, shift)
    prev = ws.date().replace(day=1) - relativedelta(months=1)
    return f"{prev.year:04d}{prev.month:02d}"


def _time_to_decimal_seconds(t: str) -> Decimal:
    hh, mm, ss = t.split(":")
    return Decimal(hh) * 3600 + Decimal(mm) * 60 + Decimal(ss)


def normalize_time_to_dt(end_day: str, t: str) -> datetime:
    base = parse_yyyymmdd(end_day)
    sec = _time_to_decimal_seconds(t.split(".", 1)[0])  # 안전
    sec_i = int(sec.quantize(Decimal("1"), rounding=ROUND_HALF_UP))

    day_add = 0
    if sec_i >= 86400:
        day_add = sec_i // 86400
        sec_i = sec_i % 86400
    if sec_i < 0:
        day_add = -((-sec_i + 86399) // 86400)
        sec_i = sec_i % 86400

    hh = sec_i // 3600
    mm = (sec_i % 3600) // 60
    ss = sec_i % 60
    return datetime(base.year, base.month, base.day, hh, mm, ss, tzinfo=KST) + timedelta(days=day_add)


def overlap_seconds_tick(from_dt: datetime, to_dt: datetime, ws: datetime, we: datetime) -> int:
    if to_dt <= from_dt:
        return 0
    start_tick = from_dt + timedelta(seconds=1)  # open at from
    end_tick = to_dt  # closed at to
    if start_tick > end_tick:
        return 0
    lo = max(start_tick, ws)
    hi = min(end_tick, we)
    if lo > hi:
        return 0
    return int((hi - lo).total_seconds()) + 1


def format_kor_hms(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds))
    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60
    parts = []
    if h:
        parts.append(f"{h}시간")
    if m:
        parts.append(f"{m}분")
    if s or (not parts):
        parts.append(f"{s}초")
    return " ".join(parts)


# =========================
# 5) 저장 테이블 DDL/UPSERT
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        ensure_session(conn)
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))


def ensure_dest_table(engine: Engine, schema: str, table: str) -> None:
    idx_name = f"{table}__ux_prod_day"
    with engine.begin() as conn:
        ensure_session(conn)

        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}";'))
        conn.execute(
            text(
                f'''
            CREATE TABLE IF NOT EXISTS "{schema}"."{table}" (
              "prod_day" text
            );
        '''
            )
        )
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "shift_type" text;'))
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "{COL_TOTAL}" text;'))
        conn.execute(text(f'ALTER TABLE "{schema}"."{table}" ADD COLUMN IF NOT EXISTS "updated_at" timestamptz;'))
        conn.execute(
            text(
                f'''
            CREATE UNIQUE INDEX IF NOT EXISTS "{idx_name}"
            ON "{schema}"."{table}" ("prod_day");
        '''
            )
        )


def upsert_result(
    engine: Engine, schema: str, table: str, prod_day: str, shift_type: str, total_str: str, updated_at: datetime
) -> None:
    sql = text(
        f"""
        INSERT INTO "{schema}"."{table}" ("prod_day","shift_type","{COL_TOTAL}","updated_at")
        VALUES (:prod_day, :shift_type, :total, :updated_at)
        ON CONFLICT ("prod_day") DO UPDATE
        SET
          "shift_type" = EXCLUDED."shift_type",
          "{COL_TOTAL}" = EXCLUDED."{COL_TOTAL}",
          "updated_at" = EXCLUDED."updated_at";
    """
    )
    with engine.begin() as conn:
        ensure_session(conn)
        conn.execute(
            sql, {"prod_day": prod_day, "shift_type": shift_type, "total": total_str, "updated_at": updated_at}
        )


# =========================
# 6) 소스 fetch / 계산
# =========================
def end_days_for_window(prod_day: str, shift: str) -> list[str]:
    d = parse_yyyymmdd(prod_day)
    if shift == "day":
        return [yyyymmdd(d)]
    return [yyyymmdd(d), yyyymmdd(d + timedelta(days=1))]


def fetch_all_in_window(engine: Engine, afa_fqn: str, prod_day: str, shift: str) -> pd.DataFrame:
    end_days = end_days_for_window(prod_day, shift)
    sql = text(
        f"""
        SELECT end_day, station, from_time, to_time, wasted_time
        FROM {afa_fqn}
        WHERE end_day = ANY(:end_days)
        ORDER BY end_day, station, from_time, to_time
    """
    )
    with engine.begin() as conn:
        ensure_session(conn)
        return pd.read_sql(sql, conn, params={"end_days": end_days})


def fetch_incremental(
    engine: Engine, afa_fqn: str, prod_day: str, shift: str, last_pk3: Optional[Tuple[str, str, str]]
) -> pd.DataFrame:
    end_days = end_days_for_window(prod_day, shift)

    if last_pk3 is None:
        sql = text(
            f"""
            SELECT end_day, station, from_time, to_time, wasted_time
            FROM {afa_fqn}
            WHERE end_day = ANY(:end_days)
            ORDER BY end_day, station, from_time, to_time
        """
        )
        params = {"end_days": end_days}
    else:
        ld, ls, lf = last_pk3
        sql = text(
            f"""
            SELECT end_day, station, from_time, to_time, wasted_time
            FROM {afa_fqn}
            WHERE end_day = ANY(:end_days)
              AND (end_day, station, from_time) >= (:ld, :ls, :lf)
            ORDER BY end_day, station, from_time, to_time
        """
        )
        params = {"end_days": end_days, "ld": ld, "ls": ls, "lf": lf}

    with engine.begin() as conn:
        ensure_session(conn)
        return pd.read_sql(sql, conn, params=params)


def load_final_ct(engine: Engine, ct_fqn: str, prod_day: str, shift: str) -> float:
    prev_month = prev_month_by_window_start(prod_day, shift)
    sql = text(
        f"""
        SELECT final_ct
        FROM {ct_fqn}
        WHERE month = :month
          AND station = 'whole'
          AND remark = 'PD'
        LIMIT 1
    """
    )
    with engine.begin() as conn:
        ensure_session(conn)
        row = conn.execute(sql, {"month": prev_month}).fetchone()
    if row is None:
        raise RuntimeError(f"final_ct not found for month={prev_month}, station='whole', remark='PD'")
    return float(row[0])


def compute_increment(
    df: pd.DataFrame,
    prod_day: str,
    shift: str,
    window_end_now: datetime,
    seen_pk: Set[Tuple[str, str, str, str]],
) -> Tuple[int, int, Optional[Tuple[str, str, str]]]:
    day_ws, day_we_fixed = fixed_shift_bounds(prod_day, "day")
    nig_ws, nig_we_fixed = fixed_shift_bounds(prod_day, "night")

    day_we = min(day_we_fixed, window_end_now)
    nig_we = min(nig_we_fixed, window_end_now)

    cur_ws, cur_we_fixed = fixed_shift_bounds(prod_day, shift)
    cur_we = min(cur_we_fixed, window_end_now)

    if df.empty:
        return 0, 0, None

    df = df.copy()
    df["wasted_time"] = pd.to_numeric(df["wasted_time"], errors="coerce").fillna(0).astype(int)

    added_loss = 0
    added_cnt = 0
    last_pk3_max: Optional[Tuple[str, str, str]] = None

    for r in df.itertuples(index=False):
        end_day = str(r.end_day)
        station = str(r.station)
        from_t = str(r.from_time)
        to_t = str(r.to_time)
        wasted = int(r.wasted_time)

        pk4 = (end_day, station, from_t, to_t)
        if pk4 in seen_pk:
            continue

        fdt = normalize_time_to_dt(end_day, from_t)
        tdt = normalize_time_to_dt(end_day, to_t)
        if tdt < fdt:
            tdt = tdt + timedelta(days=1)

        if fdt > window_end_now:
            continue
        if tdt > window_end_now:
            tdt = window_end_now

        sec_day = overlap_seconds_tick(fdt, tdt, day_ws, day_we)
        sec_nig = overlap_seconds_tick(fdt, tdt, nig_ws, nig_we)
        sec_total = sec_day + sec_nig
        if sec_total <= 0:
            continue

        row_shift = "day" if sec_day >= sec_nig else "night"
        sec_cur = overlap_seconds_tick(fdt, tdt, cur_ws, cur_we)
        if sec_cur <= 0:
            continue

        if wasted <= 0:
            alloc_cur = 0
        else:
            alloc_cur = int(
                (Decimal(wasted) * Decimal(sec_cur) / Decimal(sec_total)).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
            )
            alloc_cur = max(0, min(wasted, alloc_cur))

        added_loss += alloc_cur
        if row_shift == shift:
            added_cnt += 1

        seen_pk.add(pk4)
        pk3 = (end_day, station, from_t)
        if last_pk3_max is None or pk3 > last_pk3_max:
            last_pk3_max = pk3

    return added_loss, added_cnt, last_pk3_max


# =========================
# 7) 메인
# =========================
def main() -> None:
    global HEALTH_ENGINE, HEALTH_TABLE_READY

    engine = connect_with_retry()

    # resolve FQN
    while True:
        try:
            afa_fqn = resolve_table_fqn(engine, AFA_SCHEMA, AFA_TABLE)
            ct_fqn = resolve_table_fqn(engine, CT_SCHEMA, CT_TABLE)
            log("INFO", f"AFA_FQN={afa_fqn}")
            log("INFO", f"CT_FQN={ct_fqn}")
            break
        except Exception as e:
            log_exc("Resolve table failed", e)
            try:
                engine.dispose()
            except Exception:
                pass
            engine = connect_with_retry()

    # ensure dest
    while True:
        try:
            ensure_schema(engine, SAVE_SCHEMA)
            ensure_dest_table(engine, SAVE_SCHEMA, T_DAY)
            ensure_dest_table(engine, SAVE_SCHEMA, T_NIGHT)
            log("INFO", f"Dest ensured: {SAVE_SCHEMA}.{T_DAY}, {SAVE_SCHEMA}.{T_NIGHT}")
            break
        except Exception as e:
            log_exc("Ensure dest failed", e)
            try:
                engine.dispose()
            except Exception:
                pass
            engine = connect_with_retry()

    cur_prod_day: Optional[str] = None
    cur_shift: Optional[str] = None

    seen_pk: Set[Tuple[str, str, str, str]] = set()
    last_pk3: Optional[Tuple[str, str, str]] = None

    loss_sec: int = 0
    cnt: int = 0
    final_ct: float = 0.0

    while True:
        try:
            now = datetime.now(tz=KST)
            prod_day, shift, ws, we = current_window(now)

            window_changed = (prod_day != cur_prod_day) or (shift != cur_shift)
            if window_changed:
                cur_prod_day, cur_shift = prod_day, shift
                seen_pk.clear()
                last_pk3 = None
                loss_sec = 0
                cnt = 0

                log("INFO", f"[WINDOW] changed => prod_day={prod_day} shift={shift} start={ws} end(now)={we}")
                log("INFO", "bootstrap start (scan window start~now)")

                final_ct = load_final_ct(engine, ct_fqn, prod_day, shift)
                log("INFO", f"final_ct loaded: {final_ct} (prev_month={prev_month_by_window_start(prod_day, shift)})")

                df_all = fetch_all_in_window(engine, afa_fqn, prod_day, shift)
                log("INFO", f"bootstrap fetched rows={len(df_all)}")

                add_loss, add_cnt, new_last_pk = compute_increment(df_all, prod_day, shift, we, seen_pk)
                loss_sec += add_loss
                cnt += add_cnt
                if new_last_pk is not None:
                    last_pk3 = new_last_pk

                rework_sec = int((Decimal(str(final_ct)) * Decimal(cnt)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
                total_sec = loss_sec + rework_sec
                total_str = format_kor_hms(total_sec)
                updated_at = datetime.now(tz=KST)

                table = T_DAY if shift == "day" else T_NIGHT
                upsert_result(engine, SAVE_SCHEMA, table, prod_day, shift, total_str, updated_at)

                log("INFO", f"bootstrap done | loss_sec={loss_sec} cnt={cnt} rework_sec={rework_sec} total={total_str}")
                log("INFO", f"upsert => {SAVE_SCHEMA}.{table} (prod_day={prod_day})")

                _append_health_log("sleep", f"loop sleep {LOOP_INTERVAL_SEC}s")
                _flush_health_logs()
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            log("INFO", f"[LAST_PK] {last_pk3}")
            df_new = fetch_incremental(engine, afa_fqn, prod_day, shift, last_pk3)
            log("INFO", f"[FETCH] candidates={len(df_new)}")

            if df_new.empty:
                _append_health_log("sleep", f"no new rows, sleep {LOOP_INTERVAL_SEC}s")
                _flush_health_logs()
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            add_loss, add_cnt, new_last_pk = compute_increment(df_new, prod_day, shift, we, seen_pk)
            if add_loss == 0 and add_cnt == 0 and new_last_pk is None:
                _append_health_log("sleep", f"no effective update, sleep {LOOP_INTERVAL_SEC}s")
                _flush_health_logs()
                time_mod.sleep(LOOP_INTERVAL_SEC)
                continue

            loss_sec += add_loss
            cnt += add_cnt
            if new_last_pk is not None and (last_pk3 is None or new_last_pk > last_pk3):
                last_pk3 = new_last_pk

            rework_sec = int((Decimal(str(final_ct)) * Decimal(cnt)).quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            total_sec = loss_sec + rework_sec
            total_str = format_kor_hms(total_sec)
            updated_at = datetime.now(tz=KST)

            table = T_DAY if shift == "day" else T_NIGHT
            upsert_result(engine, SAVE_SCHEMA, table, prod_day, shift, total_str, updated_at)

            log(
                "INFO",
                f"[UPDATE] add_loss={add_loss} add_cnt={add_cnt} | loss_sec={loss_sec} cnt={cnt} rework_sec={rework_sec} total={total_str}",
            )
            log("INFO", f"[UPSERT] {SAVE_SCHEMA}.{table} prod_day={prod_day}")

            _append_health_log("sleep", f"loop sleep {LOOP_INTERVAL_SEC}s")
            _flush_health_logs()
            time_mod.sleep(LOOP_INTERVAL_SEC)

        except (OperationalError, DBAPIError) as e:
            log_exc("DB error | reconnect", e)
            _append_health_log("down", "db connection down, reconnecting")
            _flush_health_logs()

            try:
                engine.dispose()
            except Exception:
                pass

            engine = connect_with_retry()
            HEALTH_ENGINE = engine
            HEALTH_TABLE_READY = False  # 재접속 시 재보장
            _ensure_health_table(engine)
            _flush_health_logs()

            cur_prod_day = None
            cur_shift = None

        except Exception as e:
            log_exc("Unhandled error", e)
            _append_health_log("sleep", f"exception loop sleep {LOOP_INTERVAL_SEC}s")
            _flush_health_logs()
            time_mod.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    main()
