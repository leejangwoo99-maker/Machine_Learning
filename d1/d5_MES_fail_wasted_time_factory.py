# -*- coding: utf-8 -*-
"""
d5_MES_fail_wasted_time_factory.py

✅ 이번 버전 핵심(요청 반영)
1) "지금 시간으로부터 10분 전" LOOKBACK 방식 제거
   - 루프 증분(fetch)은 OUT 마지막 PK 기준으로만 수행

2) Warm-start Backfill 추가 (재실행/재부팅 시 누락분 복구 목적)
   - 프로그램 시작 직후, station별로 "백필 윈도우"를 한 번 스캔해서
     혹시 누락된 MES FAIL(w/불량) 구간을 다시 계산/UPSERT
   - 백필 범위 기본값:
       last_pk가 있으면: max(오늘00:00, last_to - WARM_BACKFILL_MIN분)  ~ now
       last_pk가 없으면: 오늘00:00 ~ now
   - 백필은 "딱 1회"만 수행 (매 루프마다 백필 X)

3) FAIL 구간 조건
   - "MES" 포함 AND "불량" 포함
   - 연속 5회 이상일 때만 FAIL run으로 인정
   - 이후 최초 "MES 바코드 조회 완료"까지 wasted_time 계산

4) PK/인덱스
   - SRC(Vision1/2) best-effort UNIQUE: (end_day, end_time, contents)
     (중복 있으면 UNIQUE 생성을 건너뛰고 NON-UNIQUE INDEX만 보장)
   - OUT PK: (end_day, station, from_time, to_time) UNIQUE + ON CONFLICT UPSERT

5) 안정화/운영 요구
   - KST 고정
   - MP=1
   - 무한루프 5초
   - DB 접속 실패/중간끊김 무한 재시도(블로킹)
   - 엔진 1개 고정(pool_size=1, max_overflow=0, pool_pre_ping)
   - work_mem 세션마다 SET (환경변수 PG_WORK_MEM)

6) INFO 로그
   - last_pk read / fetch / candidates / upsert 단계마다 출력

7) [추가] 데몬 상태 로그 DB 저장
   - 스키마: k_demon_heath_check (없으면 생성)
   - 테이블: d5_log (없으면 생성)
   - 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(lower), contents
   - 로그는 DataFrame(end_day,end_time,info,contents) 형태로 insert
   - DB down 시 메모리 큐에 보관 후 복구 시 flush
"""

import os
import sys
import time as time_mod
import urllib.parse
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Tuple

import pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError, IntegrityError

# =========================
# TZ
# =========================
KST = ZoneInfo("Asia/Seoul")

# =========================
# 환경/안정화
# =========================
os.environ.setdefault("PGCLIENTENCODING", "UTF8")
os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
DB_RETRY_INTERVAL_SEC = 5
LOOP_INTERVAL_SEC = 5

MES_REPEAT_MIN = 5
TO_MATCH_TEXT = "MES 바코드 조회 완료"

# 증분 시작 시 last_to_time 이후 + buffer
START_BUFFER_SEC = 1

# Warm-start backfill 범위(분) - 재실행 시 1회만 사용
WARM_BACKFILL_MIN = int(os.getenv("WARM_BACKFILL_MIN", "60"))

# keepalive(환경변수로 조정 가능)
PG_KEEPALIVES = int(os.getenv("PG_KEEPALIVES", "1"))
PG_KEEPALIVES_IDLE = int(os.getenv("PG_KEEPALIVES_IDLE", "30"))
PG_KEEPALIVES_INTERVAL = int(os.getenv("PG_KEEPALIVES_INTERVAL", "10"))
PG_KEEPALIVES_COUNT = int(os.getenv("PG_KEEPALIVES_COUNT", "3"))

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

SCHEMA = "d1_machine_log"
SRC_TABLES = {
    "Vision1": f'{SCHEMA}."Vision1_machine_log"',
    "Vision2": f'{SCHEMA}."Vision2_machine_log"',
}
OUT_TABLE = f'{SCHEMA}."mes_fail_wasted_time"'

ID_SEQ_NAME = "mes_fail_wasted_time_id_seq"
ID_SEQ_REGCLASS = f"{SCHEMA}.{ID_SEQ_NAME}"

# 로그 저장용
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE_NAME = "d5_log"
LOG_TABLE = f'{LOG_SCHEMA}."{LOG_TABLE_NAME}"'

_ENGINE = None

# 로그 큐(DB down 시 적재)
_PENDING_LOG_ROWS: List[Dict[str, str]] = []
_LOG_READY = False  # 로그 테이블 생성 완료 여부


# =========================
# 공통 유틸
# =========================
def now_kst() -> datetime:
    return datetime.now(KST)


def yyyymmdd(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%Y%m%d")


def hhmmss(dt: datetime) -> str:
    return dt.astimezone(KST).strftime("%H:%M:%S")


def _masked_db_info() -> str:
    return f'postgresql://{DB_CONFIG["user"]}:***@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'


def _is_connection_error(e: Exception) -> bool:
    if isinstance(e, (OperationalError, DBAPIError)):
        return True
    msg = (str(e) or "").lower()
    return any(k in msg for k in [
        "server closed the connection",
        "connection not open",
        "terminating connection",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
    ])


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
    pw = urllib.parse.quote_plus(DB_CONFIG["password"])
    host = DB_CONFIG["host"]
    port = DB_CONFIG["port"]
    dbname = DB_CONFIG["dbname"]
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}?connect_timeout=5"

    return create_engine(
        url,
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
            "application_name": "d5_mes_fail_wasted_time_factory",
        },
    )


# =========================
# 로그 DB 저장
# =========================
def _build_log_row(info: str, contents: str) -> Dict[str, str]:
    dt = now_kst()
    return {
        "end_day": yyyymmdd(dt),      # yyyymmdd
        "end_time": hhmmss(dt),       # hh:mi:ss
        "info": (info or "").strip().lower(),   # 반드시 소문자
        "contents": str(contents or "").strip(),
    }


def _enqueue_log(info: str, contents: str):
    _PENDING_LOG_ROWS.append(_build_log_row(info, contents))


def ensure_log_table_blocking(engine):
    global _LOG_READY
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {LOG_SCHEMA};

    CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
        id BIGSERIAL PRIMARY KEY,
        end_day  TEXT NOT NULL,
        end_time TEXT NOT NULL,
        info     TEXT NOT NULL,
        contents TEXT,
        created_at TIMESTAMP DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE_NAME}_day_time
      ON {LOG_TABLE} (end_day, end_time);

    CREATE INDEX IF NOT EXISTS ix_{LOG_TABLE_NAME}_info
      ON {LOG_TABLE} (info);
    """
    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text(ddl))
            _LOG_READY = True
            return
        except Exception as e:
            _LOG_READY = False
            if _is_connection_error(e):
                print(f"[RETRY] ensure_log_table conn error -> rebuild ({type(e).__name__}): {repr(e)}", flush=True)
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                print(f"[RETRY] ensure_log_table failed ({type(e).__name__}): {repr(e)}", flush=True)
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def flush_pending_logs_once(engine) -> int:
    """큐에 쌓인 로그를 한 번에 INSERT. 실패 시 큐 유지."""
    if not _PENDING_LOG_ROWS or not _LOG_READY:
        return 0

    # 사양: end_day, end_time, info, contents 순서 DataFrame화
    df = pd.DataFrame(_PENDING_LOG_ROWS, columns=["end_day", "end_time", "info", "contents"])
    if df.empty:
        return 0

    sql = f"""
    INSERT INTO {LOG_TABLE} (end_day, end_time, info, contents)
    VALUES (:end_day, :end_time, :info, :contents)
    """

    payload = df.to_dict(orient="records")
    try:
        with engine.begin() as conn:
            conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
            conn.execute(text(sql), payload)
        n = len(payload)
        _PENDING_LOG_ROWS.clear()
        return n
    except Exception:
        # flush 실패는 조용히 유지(무한 재귀 로그 방지)
        return 0


def log_event(level: str, msg: str):
    """
    콘솔 + DB(큐) 동시 처리.
    level: boot/info/retry/warn/error/down/sleep 등 (DB에는 소문자 저장)
    """
    lv = (level or "info").strip().lower()
    prefix = lv.upper() if lv else "INFO"
    print(f"[{prefix}] {msg}", flush=True)

    _enqueue_log(lv, msg)

    # 가능하면 즉시 flush (실패해도 큐 유지)
    global _ENGINE
    if _ENGINE is not None and _LOG_READY:
        flush_pending_logs_once(_ENGINE)


# 기존 호출명 호환
def log_boot(msg: str): log_event("boot", msg)
def log_info(msg: str): log_event("info", msg)
def log_retry(msg: str): log_event("retry", msg)
def log_warn(msg: str): log_event("warn", msg)
def log_error(msg: str): log_event("error", msg)
def log_down(msg: str): log_event("down", msg)
def log_sleep(msg: str): log_event("sleep", msg)


def get_engine_blocking():
    global _ENGINE
    while True:
        try:
            if _ENGINE is None:
                # 초기 DB 정보 출력/기록
                log_event("info", f"DB = {_masked_db_info()}")
                _ENGINE = _build_engine()

            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))

            # 로그 테이블 준비 + 큐 flush
            if not _LOG_READY:
                ensure_log_table_blocking(_ENGINE)
            flush_pending_logs_once(_ENGINE)

            return _ENGINE

        except Exception as e:
            # DB down 상태 기록 (콘솔 + 큐)
            log_down(f"DB connect/ping failed ({type(e).__name__}): {repr(e)}")
            log_retry(f"reconnect in {DB_RETRY_INTERVAL_SEC}s")
            _dispose_engine()
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 시간 유틸
# =========================
def start_of_today_kst(dt: datetime) -> datetime:
    dt = dt.astimezone(KST)
    return dt.replace(hour=0, minute=0, second=0, microsecond=0)


def parse_end_dt(end_day: str, end_time_str: str) -> Optional[datetime]:
    if end_time_str is None:
        return None
    s = str(end_time_str).strip()
    if not s:
        return None

    try:
        d = datetime.strptime(end_day, "%Y%m%d").date()

        if "." in s:
            hhmmss_, frac = s.split(".", 1)
            t = datetime.strptime(hhmmss_, "%H:%M:%S").time()
            frac_digits = "".join(ch for ch in frac if ch.isdigit())
            frac_digits = (frac_digits + "000000")[:6] if frac_digits else "000000"
            us = int(frac_digits)
            return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, us, tzinfo=KST)

        t = datetime.strptime(s, "%H:%M:%S").time()
        return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, 0, tzinfo=KST)

    except Exception:
        return None


def _sql_norm_time(col_name: str) -> str:
    return f"""
    (
      CASE
        WHEN position('.' in {col_name}) > 0 THEN to_timestamp({col_name}, 'HH24:MI:SS.MS')::time
        ELSE to_timestamp({col_name}, 'HH24:MI:SS')::time
      END
    )
    """


# =========================
# 소스 PK/인덱스 보장 (best-effort)
# - UNIQUE: (end_day, end_time, contents)
# =========================
def ensure_src_pk_best_effort(engine):
    uniq_ddls = [
        f'CREATE UNIQUE INDEX IF NOT EXISTS ux_vision1_day_time_contents '
        f'ON {SRC_TABLES["Vision1"]} (end_day, end_time, contents);',

        f'CREATE UNIQUE INDEX IF NOT EXISTS ux_vision2_day_time_contents '
        f'ON {SRC_TABLES["Vision2"]} (end_day, end_time, contents);',
    ]

    idx_ddls = [
        f'CREATE INDEX IF NOT EXISTS ix_vision1_day_time ON {SRC_TABLES["Vision1"]} (end_day, end_time);',
        f'CREATE INDEX IF NOT EXISTS ix_vision2_day_time ON {SRC_TABLES["Vision2"]} (end_day, end_time);',
    ]

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                for ddl in uniq_ddls:
                    conn.execute(text(ddl))
            log_info("SRC UNIQUE(PK) ensured: Vision1/2(end_day,end_time,contents)")
            return

        except IntegrityError as e:
            log_warn("SRC UNIQUE(PK) 생성 실패: (end_day,end_time,contents) 중복 데이터 존재 -> UNIQUE는 건너뜀")
            log_warn(f"detail={repr(e)}")
            try:
                with engine.begin() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    for ddl in idx_ddls:
                        conn.execute(text(ddl))
                log_info("SRC fallback INDEX ensured (NON-UNIQUE): Vision1/2(end_day,end_time)")
            except Exception as e2:
                if _is_connection_error(e2):
                    log_retry(f"ensure_src_pk fallback conn error -> rebuild ({type(e2).__name__}): {repr(e2)}")
                    _dispose_engine()
                    engine = get_engine_blocking()
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)
                    continue
                log_warn(f"ensure_src_pk fallback index failed ({type(e2).__name__}): {repr(e2)}")
            return

        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"ensure_src_pk conn error -> rebuild ({type(e).__name__}): {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"ensure_src_pk failed ({type(e).__name__}): {repr(e)}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# OUT 테이블 보장
# =========================
def ensure_out_table_blocking(engine):
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    CREATE TABLE IF NOT EXISTS {OUT_TABLE} (
        id BIGINT,
        end_day TEXT NOT NULL,
        station TEXT NOT NULL,
        from_contents TEXT,
        from_time TEXT NOT NULL,
        to_contents TEXT,
        to_time TEXT NOT NULL,
        wasted_time NUMERIC(12,2),
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
    );

    CREATE UNIQUE INDEX IF NOT EXISTS ux_mes_fail_wasted_time_pk
      ON {OUT_TABLE} (end_day, station, from_time, to_time);

    CREATE INDEX IF NOT EXISTS ix_mes_fail_wasted_time_day_station
      ON {OUT_TABLE} (end_day, station);
    """

    seq_sql = f"""
    CREATE SEQUENCE IF NOT EXISTS {ID_SEQ_REGCLASS};

    ALTER TABLE {OUT_TABLE}
      ALTER COLUMN id SET DEFAULT nextval('{ID_SEQ_REGCLASS}'::regclass);

    ALTER SEQUENCE {ID_SEQ_REGCLASS}
      OWNED BY {OUT_TABLE}.id;

    SELECT setval(
        '{ID_SEQ_REGCLASS}'::regclass,
        COALESCE((SELECT MAX(id) FROM {OUT_TABLE}), 0) + 1,
        false
    );

    UPDATE {OUT_TABLE}
    SET id = nextval('{ID_SEQ_REGCLASS}'::regclass)
    WHERE id IS NULL;
    """

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text(ddl))
                conn.execute(text(seq_sql))
            log_info("OUT PK ensured: mes_fail_wasted_time(end_day,station,from_time,to_time)")
            return
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"ensure_out_table conn error -> rebuild ({type(e).__name__}): {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"ensure_out_table failed ({type(e).__name__}): {repr(e)}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# OUT 마지막 PK 읽기
# =========================
SQL_LAST_PK = f"""
SELECT end_day, station, from_time, to_time
FROM {OUT_TABLE}
WHERE station = :station
ORDER BY
  end_day DESC,
  {_sql_norm_time("to_time")} DESC,
  {_sql_norm_time("from_time")} DESC
LIMIT 1
"""


def read_last_pk_blocking(engine, station: str) -> Optional[Tuple[str, str, str, str]]:
    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                row = conn.execute(text(SQL_LAST_PK), {"station": station}).fetchone()
            if not row:
                return None
            return (str(row[0]), str(row[1]), str(row[2]), str(row[3]))
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"[{station}] last_pk conn error -> rebuild ({type(e).__name__}): {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"[{station}] last_pk read failed ({type(e).__name__}): {repr(e)}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


def last_to_dt_from_pk(pk: Optional[Tuple[str, str, str, str]]) -> Optional[datetime]:
    if not pk:
        return None
    end_day, _station, _from_time, to_time = pk
    return parse_end_dt(end_day, to_time)


# =========================
# 소스 로그 fetch (cross-day 지원)
# =========================
def _build_fetch_sql_sameday(table_full: str) -> str:
    return f"""
    SELECT end_day, station, end_time, contents
    FROM {table_full}
    WHERE end_day = :end_day
      AND station = :station
      AND {_sql_norm_time("end_time")} BETWEEN :t_start AND :t_end
    ORDER BY {_sql_norm_time("end_time")} ASC
    """


def _build_fetch_sql_crossday(table_full: str) -> str:
    return f"""
    SELECT end_day, station, end_time, contents
    FROM {table_full}
    WHERE station = :station
      AND (
            (end_day = :start_day AND {_sql_norm_time("end_time")} >= :t_start)
         OR (end_day = :end_day   AND {_sql_norm_time("end_time")} <= :t_end)
      )
    ORDER BY end_day ASC, {_sql_norm_time("end_time")} ASC
    """


def fetch_logs_blocking(engine, table_full: str, station: str, start_dt: datetime, end_dt: datetime) -> pd.DataFrame:
    start_dt = start_dt.astimezone(KST)
    end_dt = end_dt.astimezone(KST)

    start_day = yyyymmdd(start_dt)
    end_day = yyyymmdd(end_dt)
    t_start = start_dt.time().replace(microsecond=0)
    t_end = end_dt.time().replace(microsecond=0)

    if start_day == end_day:
        sql = _build_fetch_sql_sameday(table_full)
        params = {"end_day": end_day, "station": station, "t_start": t_start, "t_end": t_end}
    else:
        sql = _build_fetch_sql_crossday(table_full)
        params = {"start_day": start_day, "end_day": end_day, "station": station, "t_start": t_start, "t_end": t_end}

    while True:
        try:
            with engine.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                df = pd.read_sql(text(sql), conn, params=params)
            return df
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"[{station}] fetch conn error -> rebuild ({type(e).__name__}): {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"[{station}] fetch failed ({type(e).__name__}): {repr(e)}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# MES FAIL(w/불량) 구간 -> wasted rows
# =========================
def build_wasted_rows(df_log: pd.DataFrame, station: str) -> pd.DataFrame:
    cols_out = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]
    if df_log.empty:
        return pd.DataFrame(columns=cols_out)

    s = df_log["contents"].astype(str)
    is_fail = s.str.contains("MES", na=False) & s.str.contains("불량", na=False)
    is_done = s.str.contains(TO_MATCH_TEXT, na=False)

    n = len(df_log)
    i = 0
    rows: List[Dict] = []

    while i < n:
        if not is_fail.iloc[i]:
            i += 1
            continue

        # 연속 FAIL run 추출
        start = i
        j = i
        while j < n and is_fail.iloc[j]:
            j += 1

        if (j - start) < MES_REPEAT_MIN:
            i = j
            continue

        from_day = str(df_log.loc[start, "end_day"])
        from_time = str(df_log.loc[start, "end_time"])
        from_contents = str(df_log.loc[start, "contents"])

        # run 이후 최초 DONE 찾기
        k = j
        to_idx = None
        while k < n:
            if is_done.iloc[k]:
                to_idx = k
                break
            k += 1

        if to_idx is None:
            i = j
            continue

        to_day = str(df_log.loc[to_idx, "end_day"])
        to_time = str(df_log.loc[to_idx, "end_time"])
        to_contents = str(df_log.loc[to_idx, "contents"])

        dt_from = parse_end_dt(from_day, from_time)
        dt_to = parse_end_dt(to_day, to_time)

        wasted_time = None
        if dt_from and dt_to:
            wasted_time = round(float((dt_to - dt_from).total_seconds()), 2)

        rows.append({
            "end_day": from_day,
            "station": station,
            "from_contents": from_contents,
            "from_time": from_time,
            "to_contents": to_contents,
            "to_time": to_time,
            "wasted_time": wasted_time,
        })

        i = to_idx + 1

    return pd.DataFrame(rows, columns=cols_out)


# =========================
# OUT UPSERT
# =========================
def upsert_rows_blocking(engine, df_rows: pd.DataFrame) -> int:
    if df_rows.empty:
        return 0

    sql = f"""
    INSERT INTO {OUT_TABLE}
      (end_day, station, from_contents, from_time, to_contents, to_time, wasted_time, created_at, updated_at)
    VALUES
      (:end_day, :station, :from_contents, :from_time, :to_contents, :to_time, :wasted_time, now(), now())
    ON CONFLICT (end_day, station, from_time, to_time)
    DO UPDATE SET
      from_contents = EXCLUDED.from_contents,
      to_contents   = EXCLUDED.to_contents,
      wasted_time   = EXCLUDED.wasted_time,
      updated_at    = now();
    """

    payload = df_rows.to_dict(orient="records")

    while True:
        try:
            with engine.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text(sql), payload)
            return len(payload)
        except Exception as e:
            if _is_connection_error(e):
                log_retry(f"upsert conn error -> rebuild ({type(e).__name__}): {repr(e)}")
                _dispose_engine()
                engine = get_engine_blocking()
            else:
                log_retry(f"upsert failed ({type(e).__name__}): {repr(e)}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# Warm-start Backfill (1회)
# =========================
def warm_start_backfill_once(engine, station: str):
    """
    재실행 시 누락분 복구 목적:
    - last_pk가 있으면: max(오늘00:00, last_to - WARM_BACKFILL_MIN분) ~ now
    - last_pk가 없으면: 오늘00:00 ~ now
    """
    now_dt = now_kst()
    today0 = start_of_today_kst(now_dt)

    log_info(f"[{station}][WARM] last_pk read ...")
    last_pk = read_last_pk_blocking(engine, station)
    log_info(f"[{station}][WARM] last_pk = {last_pk if last_pk else 'None'}")

    last_to = last_to_dt_from_pk(last_pk)

    if last_to is None:
        warm_start = today0
    else:
        warm_start = max(today0, last_to - timedelta(minutes=WARM_BACKFILL_MIN))

    log_info(
        f"[{station}][WARM] backfill window = {warm_start:%Y-%m-%d %H:%M:%S} ~ {now_dt:%Y-%m-%d %H:%M:%S} "
        f"(KST, backfill_min={WARM_BACKFILL_MIN})"
    )

    df_log = fetch_logs_blocking(engine, SRC_TABLES[station], station, warm_start, now_dt)
    log_info(f"[{station}][WARM] fetched_rows = {len(df_log)}")

    df_rows = build_wasted_rows(df_log, station)
    log_info(f"[{station}][WARM] candidates = {len(df_rows)}")

    if not df_rows.empty:
        up = upsert_rows_blocking(engine, df_rows)
        log_info(f"[{station}][WARM] upserted = {up}")
    else:
        log_info(f"[{station}][WARM] upserted = 0")


# =========================
# MAIN LOOP (MP=1)
# =========================
def main_loop_single_process():
    engine = get_engine_blocking()

    # 로그 테이블 우선 보장
    ensure_log_table_blocking(engine)
    flush_pending_logs_once(engine)

    ensure_src_pk_best_effort(engine)
    ensure_out_table_blocking(engine)

    log_info("==============================================================================")
    log_info(f"START | loop={LOOP_INTERVAL_SEC}s | tz=KST | work_mem={WORK_MEM} | warm_backfill_min={WARM_BACKFILL_MIN}")
    log_info("==============================================================================")

    # ✅ Warm-start Backfill: 재실행 시 1회만 수행
    for station in ("Vision1", "Vision2"):
        warm_start_backfill_once(engine, station)

    # 이후부터는 "PK 이후 증분"만 수행
    while True:
        loop_t0 = time_mod.perf_counter()
        try:
            engine = get_engine_blocking()
            flush_pending_logs_once(engine)

            now_dt = now_kst()
            total_upserts = 0

            for station in ("Vision1", "Vision2"):
                table_full = SRC_TABLES[station]

                # 1) 마지막 PK 읽기
                log_info(f"[{station}] last_pk read ...")
                last_pk = read_last_pk_blocking(engine, station)
                log_info(f"[{station}] last_pk = {last_pk if last_pk else 'None'}")

                # 2) PK 기반 증분 시작점 결정 (LOOKBACK 없음)
                last_to = last_to_dt_from_pk(last_pk)
                if last_to is not None:
                    inc_start = last_to + timedelta(seconds=START_BUFFER_SEC)
                else:
                    # (예외) OUT에 아직 아무것도 없으면 오늘 00:00부터
                    inc_start = start_of_today_kst(now_dt)

                log_info(
                    f"[{station}] fetch window = {inc_start:%Y-%m-%d %H:%M:%S} ~ {now_dt:%Y-%m-%d %H:%M:%S} (KST)"
                )

                # 3) 신규 fetch
                df_log = fetch_logs_blocking(engine, table_full, station, inc_start, now_dt)
                log_info(f"[{station}] fetched_rows = {len(df_log)}")

                # 4) 구간 산출
                df_rows = build_wasted_rows(df_log, station)
                log_info(f"[{station}] candidates = {len(df_rows)}")

                # 5) insert/upsert
                if not df_rows.empty:
                    up = upsert_rows_blocking(engine, df_rows)
                    total_upserts += up
                    log_info(f"[{station}] upserted = {up}")
                else:
                    log_info(f"[{station}] upserted = 0")

            if total_upserts > 0:
                log_info(f"[TOTAL] upserted={total_upserts}")

        except KeyboardInterrupt:
            log_info("KeyboardInterrupt -> stop")
            break

        except Exception as e:
            if _is_connection_error(e):
                log_down(f"loop-level conn error ({type(e).__name__}): {repr(e)}")
                log_retry("engine rebuild")
                _dispose_engine()
                time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            else:
                log_error(f"loop-level error ({type(e).__name__}): {repr(e)}")

        # 5초 페이싱
        elapsed = time_mod.perf_counter() - loop_t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        log_sleep(f"loop sleep {sleep_sec:.2f}s")
        time_mod.sleep(sleep_sec)


def main():
    log_boot("backend5 MES fail(w/불량) wasted time daemon starting")
    main_loop_single_process()


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # 최상위 예외도 남김
        try:
            log_error(f"fatal: {type(e).__name__}: {repr(e)}")
            # stderr도 즉시 출력
            print(f"[FATAL] {type(e).__name__}: {repr(e)}", file=sys.stderr, flush=True)
        finally:
            raise
