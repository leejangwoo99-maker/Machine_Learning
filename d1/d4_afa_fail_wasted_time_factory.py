# -*- coding: utf-8 -*-
"""
d4_afa_fail_wasted_time_factory.py  (Warm-start Backfill 포함 + DB Health Log)

AFA FAIL wasted time (NG -> ON) 실시간 계산/저장
- MP=1, 5초 루프
- DB 접속 실패/끊김 시 무한 재시도(5초마다 [RETRY])
- 엔진 1개 고정(pool_size=1, max_overflow=0)
- work_mem 폭증 방지(세션마다 SET)
- NG는 오로지 contents ILIKE '%제품 감지 NG%' 만 인정
- WINDOW(KST) 기준 자동 전환:
  day   : [D] 08:30:00 ~ 20:29:59
  night : [D] 20:30:00 ~ [D+1] 08:29:59
- save PK/UNIQUE: (end_day, station, from_time, to_time)
- 증분 fetch는 cursor(end_ts) 기반
- Warm-start Backfill:
  - 프로그램 시작 직후, 그리고 shift/window가 바뀔 때마다
    현재 window(ws ~ now) 구간을 station별로 한번 "전체 스캔"하여 누락분을 채움
  - 이후 루프에서는 기존처럼 last_pk 기반 증분 처리

로그:
- 콘솔 + 파일 동시 기록
- 파일 경로: C:\\AptivAgent\\d4_afa_fail_wasted_time_factory.log
- 추가 DB 로그 저장:
  schema: k_demon_heath_check (없으면 생성)
  table : d4_log (없으면 생성)
  columns: end_day, end_time, info, contents
"""

from __future__ import annotations

import os
import re
import time
import unicodedata
import urllib.parse
from dataclasses import dataclass
from datetime import datetime, timedelta, time as dtime
from typing import Dict, Optional, Tuple, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError
from zoneinfo import ZoneInfo

# =========================
# 0) 환경/상수
# =========================
KST = ZoneInfo("Asia/Seoul")

os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

SRC_TABLES = [
    ("FCT1_machine_log", "FCT1"),
    ("FCT2_machine_log", "FCT2"),
    ("FCT3_machine_log", "FCT3"),
    ("FCT4_machine_log", "FCT4"),
]

SAVE_TABLE = "afa_fail_wasted_time"

# 데몬 헬스 로그 테이블
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "d4_log"

# 타겟 문구(소문자 기준 비교)
NG_PHRASE = "제품 감지 ng"
OFF_PHRASE = "제품 검사 투입요구 on"
MANUAL_PHRASE = "manual mode 전환"
AUTO_PHRASE = "auto mode 전환"

# DB 저장용 표기(원문 형식)
NG_PHRASE_RAW = "제품 감지 NG"
OFF_PHRASE_RAW = "제품 검사 투입요구 ON"

# Loop / Retry
LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

# work_mem cap
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# Cursor overlap(초) - 증분에서 누락 방지용
CURSOR_OVERLAP_SEC = int(os.getenv("AFA_CURSOR_OVERLAP_SEC", "3"))

# keepalive (선택)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

# 로그 파일
LOG_DIR = r"C:\AptivAgent"
LOG_FILE = os.path.join(LOG_DIR, "d4_afa_fail_wasted_time_factory.log")

_ENGINE = None


# =========================
# 1) 로깅 (콘솔 + 파일 + DB)
# =========================
def _ensure_log_dir() -> None:
    try:
        os.makedirs(LOG_DIR, exist_ok=True)
    except Exception:
        pass


def _to_health_row(level: str, msg: str, now_kst: datetime) -> Dict[str, str]:
    """
    level -> info(소문자) 매핑
    예: ERROR/RETRY/INFO/BOOT -> error/retry/info/boot
    """
    info = (level or "").strip().lower()
    if not info:
        info = "info"
    return {
        "end_day": now_kst.strftime("%Y%m%d"),      # yyyymmdd
        "end_time": now_kst.strftime("%H:%M:%S"),   # hh:mi:ss
        "info": info,                               # 반드시 소문자
        "contents": str(msg),
    }


def _db_log_try(rows: List[Dict[str, str]]) -> None:
    """
    DB 로그는 best-effort로만 저장.
    - 절대 블로킹 재시도하지 않음
    - DB 불가 시 조용히 스킵 (파일/콘솔 로그는 계속)
    """
    global _ENGINE
    if _ENGINE is None or not rows:
        return

    ins = text(f"""
        INSERT INTO {HEALTH_SCHEMA}.{HEALTH_TABLE}
        (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
    """)
    try:
        with _ENGINE.begin() as conn:
            conn.execute(ins, rows)
    except Exception:
        # health log insert 실패는 무시 (무한루프 방지)
        pass


def _write_log(level: str, msg: str) -> None:
    now_kst = datetime.now(tz=KST)
    ts = now_kst.strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [{level}] {msg}"

    # console
    print(line, flush=True)

    # file
    try:
        _ensure_log_dir()
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass

    # db (best-effort)
    row = _to_health_row(level, msg, now_kst)
    # 요구사항 6: end_day, end_time, info, contents 순서로 dataframe화 후 저장
    try:
        df = pd.DataFrame([row], columns=["end_day", "end_time", "info", "contents"])
        _db_log_try(df.to_dict("records"))
    except Exception:
        pass


def log_boot(msg: str) -> None:
    _write_log("BOOT", msg)


def log_info(msg: str) -> None:
    _write_log("INFO", msg)


def log_retry(msg: str) -> None:
    _write_log("RETRY", msg)


def log_error(msg: str) -> None:
    _write_log("ERROR", msg)


# =========================
# 2) 텍스트 정규화/매칭
# =========================
_ZWSP_RE = re.compile(r"[\u200b\u200c\u200d\ufeff]")


def norm_text(s: object) -> str:
    if s is None:
        return ""
    t = str(s)
    t = unicodedata.normalize("NFKC", t)
    t = _ZWSP_RE.sub("", t)
    t = t.replace("\r", " ").replace("\n", " ").replace("\t", " ")
    t = re.sub(r"\s+", " ", t).strip()
    return t.lower()


def is_ng(c: str) -> bool:
    return NG_PHRASE in c  # 오로지 이 문구 포함만 NG


def is_off(c: str) -> bool:
    return OFF_PHRASE in c


def is_manual(c: str) -> bool:
    return MANUAL_PHRASE in c


def is_auto(c: str) -> bool:
    return AUTO_PHRASE in c


# =========================
# 3) WINDOW 계산(KST 자동 전환)
# =========================
def calc_window(now_kst: datetime) -> Tuple[str, str, datetime, datetime]:
    """
    window_end는 항상 now(현재 시각).
    shift 정의:
      day   : 08:30:00 ~ 20:29:59
      night : 20:30:00 ~ D+1 08:29:59
    """
    t = now_kst.time()
    day_start = dtime(8, 30, 0)
    day_end = dtime(20, 29, 59)
    night_start = dtime(20, 30, 0)

    # day
    if day_start <= t <= day_end:
        ws = now_kst.replace(hour=8, minute=30, second=0, microsecond=0)
        return "day", ws.strftime("%Y%m%d"), ws, now_kst

    # night (20:30~23:59)
    if t >= night_start:
        ws = now_kst.replace(hour=20, minute=30, second=0, microsecond=0)
        return "night", ws.strftime("%Y%m%d"), ws, now_kst

    # night (00:00~08:29:59): 전날 20:30부터
    yday = (now_kst - timedelta(days=1)).date()
    ws = datetime(yday.year, yday.month, yday.day, 20, 30, 0, tzinfo=KST)
    return "night", ws.strftime("%Y%m%d"), ws, now_kst


# =========================
# 4) DB 연결/재접속(엔진 1개 고정)
# =========================
def _masked_db() -> str:
    c = DB_CONFIG
    return f"postgresql://{c['user']}:***@{c['host']}:{c['port']}/{c['dbname']}"


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, OperationalError):
        return True
    if isinstance(e, DBAPIError):
        if getattr(e, "connection_invalidated", False):
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
            "application_name": "afa_fail_wasted_time_realtime",
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
                log_retry(f"db ping failed -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                time.sleep(DB_RETRY_INTERVAL_SEC)

    while True:
        try:
            _ENGINE = _build_engine()
            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            log_info(f"db connected | {_masked_db()} | pool_size=1 max_overflow=0 work_mem={WORK_MEM}")
            return _ENGINE
        except Exception as e:
            log_retry(f"db connect failed | {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 5) 결과 테이블/UNIQUE 보장
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
            log_info("bootstrap ok: save table + unique index(end_day,station,from_time,to_time) ensured")
            return
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"init_save_table conn error -> rebuild | {type(e).__name__}: {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"init_save_table failed | {type(e).__name__}: {repr(e)}")
            time.sleep(DB_RETRY_INTERVAL_SEC)


def init_health_log_table_blocking(engine):
    """
    요구사항:
    - schema: k_demon_heath_check (없으면 생성)
    - table : d4_log (없으면 생성)
    - columns: end_day, end_time, info, contents
    """
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA};

    CREATE TABLE IF NOT EXISTS {HEALTH_SCHEMA}.{HEALTH_TABLE} (
        end_day   TEXT,
        end_time  TEXT,
        info      TEXT,
        contents  TEXT,
        created_at TIMESTAMP DEFAULT now()
    );
    """)
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(ddl)
            # 여기서는 _write_log를 부르지 않음(초기화 중 재귀 방지)
            print(f"[{datetime.now(tz=KST):%Y-%m-%d %H:%M:%S}] [INFO] health log table ensured: {HEALTH_SCHEMA}.{HEALTH_TABLE}", flush=True)
            return
        except Exception as e:
            if _is_connection_error(e):
                print(f"[{datetime.now(tz=KST):%Y-%m-%d %H:%M:%S}] [RETRY] init_health_log conn error -> rebuild | {type(e).__name__}: {repr(e)}", flush=True)
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                print(f"[{datetime.now(tz=KST):%Y-%m-%d %H:%M:%S}] [RETRY] init_health_log failed | {type(e).__name__}: {repr(e)}", flush=True)
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 6) station별 마지막 PK(to_ts) 읽기
# =========================
def read_last_pk_per_station(engine) -> Dict[str, Optional[datetime]]:
    out: Dict[str, Optional[datetime]] = {st: None for _, st in SRC_TABLES}

    sql = text(f"""
        SELECT station,
               to_timestamp(end_day || ' ' || split_part(to_time, '.', 1), 'YYYYMMDD HH24:MI:SS') AS to_ts
        FROM {SCHEMA}.{SAVE_TABLE}
        WHERE end_day IS NOT NULL AND station IS NOT NULL AND to_time IS NOT NULL
        ORDER BY to_ts DESC
    """)

    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                rows = conn.execute(sql).fetchall()

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
def fetch_src_logs(engine, table: str, station: str, ws: datetime, we: datetime, cursor_ts: datetime,
                   overlap_sec: int = CURSOR_OVERLAP_SEC) -> pd.DataFrame:
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

        log_info(f"[evt ] {r['station']} {ts.astimezone(KST):%Y-%m-%d %H:%M:%S} | {r['contents']}")

        if is_manual(c):
            state.in_manual = True
            log_info(f"[stat] {r['station']} -> in_manual=y")
            continue

        if is_auto(c):
            state.in_manual = False
            log_info(f"[stat] {r['station']} -> in_manual=n")
            continue

        if is_ng(c):
            if (not state.in_manual) and (state.pending_ng_ts is None):
                state.pending_ng_ts = ts
                log_info(f"[stat] {r['station']} pending_ng_ts={ts.astimezone(KST):%H:%M:%S}")
            continue

        if is_off(c):
            if state.pending_ng_ts is not None:
                from_ts = state.pending_ng_ts
                to_ts = ts
                end_day = from_ts.astimezone(KST).strftime("%Y%m%d")

                rows.append({
                    "end_day": end_day,
                    "station": r["station"],
                    "from_contents": NG_PHRASE_RAW,
                    "from_time": from_ts.astimezone(KST).strftime("%H:%M:%S.%f")[:-4],
                    "to_contents": OFF_PHRASE_RAW,
                    "to_time": to_ts.astimezone(KST).strftime("%H:%M:%S.%f")[:-4],
                    "wasted_time": round(abs((to_ts - from_ts).total_seconds()), 2),
                })

                log_info(
                    f"[pair] {r['station']} {from_ts.astimezone(KST):%H:%M:%S} -> "
                    f"{to_ts.astimezone(KST):%H:%M:%S} = {round(abs((to_ts - from_ts).total_seconds()), 2)}s"
                )
                state.pending_ng_ts = None

    out = pd.DataFrame(rows, columns=[
        "end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"
    ])
    return out, state


# =========================
# 9) INSERT
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
# 10) Warm-start Backfill
# =========================
def run_backfill_for_window(engine, ws: datetime, we: datetime, states: Dict[str, StationState]) -> None:
    log_info(f"[backfill] start window={ws:%Y-%m-%d %H:%M:%S}~{we:%Y-%m-%d %H:%M:%S}")

    total_fetched = 0
    total_pairs = 0
    total_ins = 0

    for table, station in SRC_TABLES:
        df = fetch_src_logs(engine, table, station, ws, we, cursor_ts=ws, overlap_sec=0)
        fetched = len(df)
        total_fetched += fetched
        log_info(f"[backfill][fetch] {station} cursor=ws -> rows={fetched}")

        if fetched == 0:
            continue

        pairs, states[station] = pair_events(df, states[station])
        pcount = len(pairs)
        total_pairs += pcount
        log_info(f"[backfill][pair ] {station} pairs={pcount} (pending_ng={'y' if states[station].pending_ng_ts else 'n'}, in_manual={'y' if states[station].in_manual else 'n'})")

        ins = insert_rows(engine, pairs) if pcount > 0 else 0
        total_ins += ins
        log_info(f"[backfill][ins  ] {station} attempted_insert={ins} (dedup by pk/unique)")

    log_info(f"[backfill] done | fetched={total_fetched} | pairs={total_pairs} | attempted_insert={total_ins}")


# =========================
# 11) main
# =========================
def main():
    _ensure_log_dir()

    # 부팅 초기 로그 (DB 없어도 남겨야 하므로 먼저 파일/콘솔)
    log_boot("backend afa_fail_wasted_time realtime starting (mp=1, loop=5s, warm-start backfill=on)")
    log_info(f"log_file = {LOG_FILE}")
    log_info(f"db = {_masked_db()}")
    log_info(f"work_mem cap = {WORK_MEM}")
    log_info(f"src = {SCHEMA}.FCT1~4_machine_log | pk(end_day,end_time,contents) assumed")
    log_info(f"save = {SCHEMA}.{SAVE_TABLE} | pk/unique(end_day,station,from_time,to_time)")
    log_info(f"cursor_overlap_sec = {CURSOR_OVERLAP_SEC}")

    engine = get_engine_blocking()

    # health log 테이블 먼저 보장 (이후부터 로그들이 db 적재됨)
    init_health_log_table_blocking(engine)
    log_info(f"health log target = {HEALTH_SCHEMA}.{HEALTH_TABLE} (end_day,end_time,info,contents)")

    # 결과 저장 테이블 보장
    init_save_table_blocking(engine)

    states: Dict[str, StationState] = {st: StationState() for _, st in SRC_TABLES}

    # warm-start 키: shift + ws(윈도우 시작) 단위로 한번만 backfill
    last_backfill_key: Optional[Tuple[str, datetime]] = None

    while True:
        loop_t0 = time.perf_counter()
        now_kst = datetime.now(tz=KST)
        shift, prod_day, ws, we = calc_window(now_kst)

        try:
            engine = get_engine_blocking()

            # 시작/shift 변경 시 ws~now 범위 전체 스캔
            backfill_key = (shift, ws)
            if last_backfill_key != backfill_key:
                states = {st: StationState() for _, st in SRC_TABLES}
                run_backfill_for_window(engine, ws, we, states)
                last_backfill_key = backfill_key

            # 1) 마지막 PK 읽기
            last_to_ts = read_last_pk_per_station(engine)
            last_pk_str = ", ".join(
                [f"{k}={(v.strftime('%Y-%m-%d %H:%M:%S') if v else 'None')}" for k, v in last_to_ts.items()]
            )
            log_info(
                f"[last_pk] shift={shift} prod_day={prod_day} "
                f"window={ws:%Y-%m-%d %H:%M:%S}~{we:%Y-%m-%d %H:%M:%S} | {last_pk_str}"
            )

            total_fetched = 0
            total_pairs = 0
            total_attempted = 0

            # 2) station별 신규 fetch/pair/insert (증분)
            for table, station in SRC_TABLES:
                cursor_base = last_to_ts.get(station) or ws
                if cursor_base < ws:
                    cursor_base = ws

                df = fetch_src_logs(engine, table, station, ws, we, cursor_base, overlap_sec=CURSOR_OVERLAP_SEC)
                fetched = len(df)
                total_fetched += fetched
                log_info(f"[fetch] {station} cursor={cursor_base:%Y-%m-%d %H:%M:%S} -> rows={fetched}")

                if fetched == 0:
                    continue

                pairs, states[station] = pair_events(df, states[station])
                pcount = len(pairs)
                total_pairs += pcount
                log_info(
                    f"[pair ] {station} pairs={pcount} "
                    f"(pending_ng={'y' if states[station].pending_ng_ts else 'n'}, "
                    f"in_manual={'y' if states[station].in_manual else 'n'})"
                )

                attempted = insert_rows(engine, pairs) if pcount > 0 else 0
                total_attempted += attempted
                log_info(f"[ins  ] {station} attempted_insert={attempted} (dedup by pk/unique)")

            log_info(
                f"[loop ] shift={shift} prod_day={prod_day} "
                f"| fetched={total_fetched} | pairs={total_pairs} | attempted_insert={total_attempted}"
            )

        except KeyboardInterrupt:
            log_info("keyboardinterrupt -> exit")
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
            log_info(f"sleep {sleep_sec:.3f}s")
            time.sleep(sleep_sec)


if __name__ == "__main__":
    main()
