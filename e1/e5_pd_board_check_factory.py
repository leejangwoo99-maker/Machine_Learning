# -*- coding: utf-8 -*-
"""
e5_pd_board_check.py  (SCHEDULED LOOP VERSION)

요구사항 반영
1) 하루 2회 실행(윈도우):
   - 08:27:00 ~ 08:29:59 (윈도우 내 50초 간격 반복 실행)
   - 20:27:00 ~ 20:29:59 (윈도우 내 50초 간격 반복 실행)
   - 윈도우 밖에서는 "대기"만 (1초 sleep)
   - 윈도우 진입 시 1초 루프로 반복 실행하되, 분석/저장은 50초 간격으로만 수행

2) end_day = 오늘 날짜(YYYYMMDD) 자동
   - 같은 날 2회 실행 시 PK(station,end_day) 기준 최신 결과로 update (UPSERT)

3) start_day = last_date(실제 데이터 최신일) - 7일
   - last_date는 소스 테이블에서 조건(stations, step_desc)에 맞는 최신 end_day를 SQL로 산출

4) 기존 목적/로직 유지:
   - 일자 평균 시계열 -> WINDOW(feature) -> score/cos/dist
   - robust threshold(MAD) + status + 연속경보(streak)
   - e4_predictive_maintenance.pd_board_check 에
     (station,end_day) PK로 UPSERT 저장

✅ 이번 요청 추가 반영(핵심)
- ✅ 실행 중 서버 연결이 끊겨도(네트워크/서버 재시작 등)
     - SELECT/READ/UPSERT/DDL 전 구간에서 "연결 복구까지 무한 재시도"
     - SQLAlchemy engine dispose/rebuild 후 재시도
- ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
- ✅ 백엔드별 상시 연결 1개로 고정(풀 최소화)
  * SQLAlchemy engine: pool_size=1, max_overflow=0
  * 엔진은 프로세스에서 1개만 생성/유지(죽으면 폐기 후 재생성)
- ✅ work_mem 폭증 방지: 세션마다 SET work_mem 적용
- ✅ 무한 루프 인터벌 5초: 반영하지 않음(기존 1초 루프 + 50초 실행 유지)
"""

import json
import time
import urllib.parse
from datetime import datetime, timezone, date, timedelta, time as dtime

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError, SQLAlchemyError


# =========================
# 0) DB 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"

TARGET_SCHEMA = "e4_predictive_maintenance"
TARGET_TABLE = "pd_board_check"

PATTERN_TABLE = "predictive_maintenance"
NORMAL_PATTERN_NAME = "pd_board_normal_ref"
ABN_PATTERN_NAME = "pd_board_degradation_ref"

# ✅ 연결/리소스 제한
DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = 5
WORK_MEM = "4MB"  # 필요 시 더 낮춰도 됨(예: 2MB)

# ✅ keepalive(선택): 끊김을 빠르게 감지하고 재연결 유도
PG_KEEPALIVES = 1
PG_KEEPALIVES_IDLE = 30
PG_KEEPALIVES_INTERVAL = 10
PG_KEEPALIVES_COUNT = 3


# =========================
# 1) 실행 파라미터
# =========================
STEP_DESC = "1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol"
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]

WINDOW = 5
COS_TH = 0.70

K_MAD = 4.0
MIN_SAMPLES_FOR_ROBUST = 8

# 스케줄 윈도우 (하루 2회)
WIN1_START = dtime(8, 27, 0)
WIN1_END   = dtime(8, 29, 59)

WIN2_START = dtime(20, 27, 0)
WIN2_END   = dtime(20, 29, 59)

# 윈도우 내 "실제 실행" 간격(초)
RUN_EVERY_SEC = 50

# 루프 대기(초)  ✅ 기존대로 유지(1초 루프)
SLEEP_SEC = 1


# =========================
# 2) 공용 함수
# =========================
def log(msg: str):
    print(msg, flush=True)


def yyyymmdd_today() -> str:
    return date.today().strftime("%Y%m%d")


def _is_conn_error(e: Exception) -> bool:
    """
    연결 끊김/네트워크/DB 재시작 계열 오류 감지(넓게).
    """
    if isinstance(e, (OperationalError, DBAPIError)):
        return True
    msg = (str(e) or "").lower()
    keys = [
        "server closed the connection",
        "terminating connection",
        "connection not open",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
        "admin shutdown",
        "the database system is starting up",
        "the database system is in recovery mode",
    ]
    return any(k in msg for k in keys)


# =========================
# 2-1) DB 연결: 상시 1개 + 무한 재시도 + work_mem 제한
# =========================
_ENGINE = None


def _build_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]

    # connect_timeout은 URL 파라미터로 전달
    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        f"?connect_timeout={CONNECT_TIMEOUT_SEC}"
    )

    # ✅ 풀 최소화: 상시 1개(overflow 0)
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
        connect_args={
            "connect_timeout": CONNECT_TIMEOUT_SEC,
            "keepalives": PG_KEEPALIVES,
            "keepalives_idle": PG_KEEPALIVES_IDLE,
            "keepalives_interval": PG_KEEPALIVES_INTERVAL,
            "keepalives_count": PG_KEEPALIVES_COUNT,
            "application_name": "e5_pd_board_check",
        },
    )


def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


def get_engine_blocking():
    """
    - DB 접속 성공할 때까지 무한 재시도(블로킹)
    - 성공 후에도 engine 1개를 계속 재사용
    - work_mem은 연결마다 보장(아래 helper로 세팅)
    """
    global _ENGINE
    while True:
        try:
            if _ENGINE is None:
                _ENGINE = _build_engine(DB_CONFIG)

            with _ENGINE.connect() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(text("SELECT 1"))
            return _ENGINE

        except Exception as e:
            log(f"[DB][RETRY] engine connect failed: {type(e).__name__}: {repr(e)}")
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


def exec_with_work_mem_blocking(engine, sql_text, params=None):
    """
    ✅ 모든 execute를 "연결 복구까지 무한 재시도"로 감싼 wrapper
    - work_mem 강제
    - connection error면 engine dispose 후 재획득
    """
    eng = engine
    while True:
        try:
            with eng.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                return conn.execute(sql_text, params or {})
        except Exception as e:
            log(f"[DB][RETRY] exec failed: {type(e).__name__}: {repr(e)}")
            if _is_conn_error(e):
                _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)
            eng = get_engine_blocking()


def read_sql_with_work_mem_blocking(engine, sql_text, params=None):
    """
    ✅ pd.read_sql도 "연결 복구까지 무한 재시도"
    """
    eng = engine
    while True:
        try:
            with eng.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                return pd.read_sql(sql_text, conn, params=params or {})
        except Exception as e:
            log(f"[DB][RETRY] read_sql failed: {type(e).__name__}: {repr(e)}")
            if _is_conn_error(e):
                _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)
            eng = get_engine_blocking()


# (기존 이름 유지가 필요하면 alias)
def exec_with_work_mem(engine, sql_text, params=None):
    return exec_with_work_mem_blocking(engine, sql_text, params=params)


def read_sql_with_work_mem(engine, sql_text, params=None):
    return read_sql_with_work_mem_blocking(engine, sql_text, params=params)


# =========================
# 3) 패턴 로드
# =========================
def load_pattern(engine, station, step_desc, pattern_name):
    q = text(f"""
        SELECT euclid_graph
        FROM {TARGET_SCHEMA}.{PATTERN_TABLE}
        WHERE station=:station
          AND step_description=:step_desc
          AND pattern_name=:pattern_name
    """)
    row = exec_with_work_mem(engine, q, {
        "station": station,
        "step_desc": step_desc,
        "pattern_name": pattern_name
    }).fetchone()

    if row is None:
        raise ValueError(f"[ERROR] Pattern not found: station={station}, pattern={pattern_name}")

    g = row[0]
    if isinstance(g, str):
        g = json.loads(g)
    return g


# =========================
# 4) last_date 산출 + start_day/end_day 자동 계산
# =========================
def get_last_date_from_source(engine, stations, step_desc) -> str:
    """
    소스 테이블에서 조건에 맞는 최신 end_day(YYYYMMDD)를 산출.
    end_day가 TEXT일 수 있으므로 숫자만 남긴 후 max로 구함(YYYYMMDD는 문자열 max == 날짜 max).
    """
    q = text(f"""
        SELECT
            MAX(substring(regexp_replace(trim(COALESCE(end_day,'')), '\\D', '', 'g') from 1 for 8)) AS last_day
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE trim(station) = ANY(:stations)
          AND step_description = :step_desc
          AND regexp_replace(trim(COALESCE(end_day,'')), '\\D', '', 'g') ~ '^[0-9]{{8}}'
    """)

    row = exec_with_work_mem(engine, q, {"stations": stations, "step_desc": step_desc}).fetchone()

    last_day = row[0] if row else None
    if last_day is None or str(last_day).strip() == "":
        raise ValueError("[ERROR] last_date not found in source (조건 불일치 또는 end_day 포맷 문제).")

    last_day = str(last_day).strip()[:8]
    if len(last_day) != 8:
        raise ValueError(f"[ERROR] invalid last_date={last_day}")

    return last_day


def calc_start_end_days(engine) -> tuple[str, str, str]:
    """
    start_day = last_date - 7일
    end_day   = 오늘 날짜
    last_date = 소스 최신일
    """
    end_day = yyyymmdd_today()
    last_date = get_last_date_from_source(engine, STATIONS, STEP_DESC)

    last_dt = datetime.strptime(last_date, "%Y%m%d").date()
    start_dt = last_dt - timedelta(days=7)
    start_day = start_dt.strftime("%Y%m%d")

    return start_day, last_date, end_day


# =========================
# 5) 원천 데이터 조회 → 일자 평균 (YYYYMMDD 기준으로 통일)
# =========================
def load_avg_df(engine, stations, start_day, end_day, step_desc):
    """
    반환: station, end_day_norm(YYYYMMDD), value_avg, sample_amount
    """
    sql = text(f"""
        SELECT
            trim(end_day)   AS end_day_raw,
            trim(station)   AS station,
            value
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE trim(station) = ANY(:stations)
          AND step_description = :step_desc
          AND regexp_replace(trim(COALESCE(end_day,'')), '\\D', '', 'g') >= :start_day
          AND regexp_replace(trim(COALESCE(end_day,'')), '\\D', '', 'g') <= :end_day
        ORDER BY station, end_day_raw
    """)

    raw = read_sql_with_work_mem(engine, sql, params={
        "stations": stations,
        "start_day": start_day,
        "end_day": end_day,
        "step_desc": step_desc
    })

    raw.columns = [str(c).strip() for c in raw.columns]
    if raw.empty:
        raise ValueError("[ERROR] raw is empty. (station/date/step_description 조건 불일치)")

    # value 숫자화
    raw["value_num"] = pd.to_numeric(raw["value"], errors="coerce")
    if raw["value_num"].notna().sum() == 0:
        raw["value_num"] = raw["value"].astype(str).str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
        raw["value_num"] = pd.to_numeric(raw["value_num"], errors="coerce")

    raw = raw.dropna(subset=["value_num"]).copy()
    if raw.empty:
        raise ValueError("[ERROR] value_num is empty after numeric parsing. (value 포맷 확인 필요)")

    # end_day 정규화(숫자 8자리)
    raw["end_day_norm"] = (
        raw["end_day_raw"]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str.zfill(8)
        .str.slice(0, 8)
    )
    raw = raw[raw["end_day_norm"].str.match(r"^\d{8}$", na=False)].copy()
    if raw.empty:
        raise ValueError("[ERROR] end_day_norm empty after normalization. (end_day 포맷 확인 필요)")

    avg_df = (
        raw.groupby(["station", "end_day_norm"], as_index=False)
           .agg(value_avg=("value_num", "mean"),
                sample_amount=("value_num", "count"))
           .sort_values(["station", "end_day_norm"])
           .reset_index(drop=True)
    )
    avg_df["value_avg"] = avg_df["value_avg"].round(4)
    return avg_df


# =========================
# 6) compare_df 생성
# =========================
def window_vector_from_values(values):
    """일자 평균 시계열 → 5차원 특성 벡터"""
    values = np.asarray(values, dtype=float)
    if len(values) < 2:
        return {"mean": np.nan, "std": np.nan, "amplitude": np.nan, "diff_mean": np.nan, "diff_std": np.nan}
    diffs = np.diff(values)
    return {
        "mean": float(np.mean(values)),
        "std": float(np.std(values, ddof=0)),
        "amplitude": float(np.max(values) - np.min(values)),
        "diff_mean": float(np.mean(diffs)),
        "diff_std": float(np.std(diffs, ddof=0)),
    }


def cosine_sim(a, b, eps=1e-12):
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na < eps or nb < eps:
        return np.nan
    return float(np.dot(a, b) / (na * nb))


def mad(x: pd.Series) -> float:
    x = x.dropna().astype(float).values
    if len(x) == 0:
        return np.nan
    med = np.median(x)
    return float(np.median(np.abs(x - med)))


def robust_threshold(x: pd.Series, k=K_MAD):
    x = x.dropna().astype(float)
    if len(x) < MIN_SAMPLES_FOR_ROBUST:
        if len(x) == 0:
            return np.nan
        p90 = float(np.percentile(x, 90))
        return p90 * 1.10 if np.isfinite(p90) else np.nan

    med = float(np.median(x))
    m = mad(x)
    if not np.isfinite(m) or m == 0:
        return float(np.percentile(x, 95))
    return med + k * m


def sanitize_for_json(obj):
    """JSONB 저장을 위해 NaN/Inf -> None"""
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]
    return obj


def make_ts_json(df_station: pd.DataFrame, x_col: str, y_col: str, th_value=None):
    """
    Streamlit에서 그대로 그릴 수 있도록 시계열(JSON)로 저장
    x는 YYYYMMDD 문자열, y는 값 배열
    """
    if df_station is None or df_station.empty:
        payload = {"type": "timeseries", "x": [], "y": []}
        if th_value is not None and np.isfinite(th_value):
            payload["th"] = float(th_value)
        return sanitize_for_json(payload)

    d = df_station.sort_values(x_col).copy()
    x = d[x_col].astype(str).tolist()
    y = [None if pd.isna(v) else float(v) for v in d[y_col].tolist()]
    payload = {"type": "timeseries", "x": x, "y": y}
    if th_value is not None and np.isfinite(th_value):
        payload["th"] = float(th_value)
    return sanitize_for_json(payload)


def build_compare_df(avg_df_all: pd.DataFrame,
                     V_normal: np.ndarray,
                     A_ref: np.ndarray,
                     FEATURES: list,
                     window: int) -> pd.DataFrame:
    """
    avg_df_all(station, end_day_norm, value_avg, sample_amount) 기반 WINDOW 슬라이딩
    """
    if avg_df_all is None or avg_df_all.empty:
        return pd.DataFrame(columns=["station", "window_end_day", "score_from_normal", "cos_sim_to_ref", "dist_to_ref"])

    need_cols = {"station", "end_day_norm", "value_avg"}
    missing = need_cols - set(avg_df_all.columns)
    if missing:
        raise KeyError(f"[ERROR] avg_df_all missing columns: {missing}")

    st_targets = sorted(avg_df_all["station"].dropna().astype(str).unique().tolist())
    rows = []

    for st in st_targets:
        df_st = avg_df_all[avg_df_all["station"] == st].copy()
        df_st = df_st.dropna(subset=["end_day_norm", "value_avg"]).copy()
        df_st["end_day_norm"] = df_st["end_day_norm"].astype(str).str.zfill(8)
        df_st = df_st.sort_values("end_day_norm").reset_index(drop=True)

        if len(df_st) < window:
            continue

        for i in range(len(df_st) - window + 1):
            chunk = df_st.iloc[i:i + window]
            end_day_key = chunk["end_day_norm"].iloc[-1]

            v = window_vector_from_values(chunk["value_avg"].values)
            V_t = np.array([v[k] for k in FEATURES], dtype=float)

            A_t = V_t - V_normal

            rows.append({
                "station": st,
                "window_end_day": str(end_day_key),
                "score_from_normal": float(np.linalg.norm(A_t)),
                "cos_sim_to_ref": cosine_sim(A_t, A_ref),
                "dist_to_ref": float(np.linalg.norm(A_t - A_ref)),
            })

    if not rows:
        return pd.DataFrame(columns=["station", "window_end_day", "score_from_normal", "cos_sim_to_ref", "dist_to_ref"])

    return (
        pd.DataFrame(rows)
          .sort_values(["station", "window_end_day"])
          .reset_index(drop=True)
    )


# =========================
# 7) dfi + summary 생성 (station 컬럼 보장)
# =========================
def build_dfi_and_summary(compare_df: pd.DataFrame, start_day: str, end_day: str):
    if compare_df is None or compare_df.empty:
        empty_dfi = pd.DataFrame(columns=[
            "station", "window_end_day",
            "score_from_normal", "cos_sim_to_ref", "dist_to_ref",
            "th_score", "status", "alert_streak"
        ])
        empty_summary = pd.DataFrame(columns=[
            "station", "start_day", "last_date", "end_day",
            "last_status", "last_score", "th_score", "last_cos",
            "max_score", "max_cos", "max_streak", "crit_days", "warn_days"
        ])
        return empty_dfi, empty_summary

    # threshold
    th_df = (
        compare_df.groupby("station")["score_from_normal"]
        .apply(lambda s: robust_threshold(s, k=K_MAD))
        .reset_index(name="th_score")
    )

    dfi = compare_df.merge(th_df, on="station", how="left").copy()

    dfi["is_cos_like"] = dfi["cos_sim_to_ref"] >= COS_TH
    dfi["is_score_high"] = dfi["score_from_normal"] >= dfi["th_score"]

    def classify(row):
        if row["is_cos_like"] and row["is_score_high"]:
            return "CRITICAL"
        if row["is_cos_like"] and (not row["is_score_high"]):
            return "WARNING"
        if (not row["is_cos_like"]) and row["is_score_high"]:
            return "WATCH"
        return "OK"

    dfi["status"] = dfi.apply(classify, axis=1)

    # streak (groupby.apply 금지: station 컬럼 소실 방지)
    ALERT_LEVELS = {"WARNING", "CRITICAL"}
    dfi = dfi.sort_values(["station", "window_end_day"]).reset_index(drop=True)

    streak_out = []
    prev_station = None
    consec = 0
    for _, row in dfi.iterrows():
        st = row["station"]
        if st != prev_station:
            prev_station = st
            consec = 0
        if row["status"] in ALERT_LEVELS:
            consec += 1
        else:
            consec = 0
        streak_out.append(consec)

    dfi["alert_streak"] = streak_out

    # summary
    summary = (
        dfi.sort_values(["station", "window_end_day"])
           .groupby("station", as_index=False)
           .agg(
               last_date=("window_end_day", "max"),
               last_status=("status", "last"),
               last_score=("score_from_normal", "last"),
               th_score=("th_score", "last"),
               last_cos=("cos_sim_to_ref", "last"),
               max_score=("score_from_normal", "max"),
               max_cos=("cos_sim_to_ref", "max"),
               max_streak=("alert_streak", "max"),
               crit_days=("status", lambda s: int((s == "CRITICAL").sum())),
               warn_days=("status", lambda s: int((s == "WARNING").sum())),
           )
    )

    summary.insert(1, "start_day", start_day)
    summary.insert(3, "end_day", end_day)

    return dfi, summary


# =========================
# 8) pd_board_check 테이블 생성 + UPSERT (연결 끊김에도 무한 재시도)
# =========================
def ensure_pd_board_check(engine):
    exec_with_work_mem(engine, text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

    exec_with_work_mem(engine, text(f"""
        CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
            station TEXT NOT NULL,
            start_day TEXT NOT NULL,
            last_date TEXT,
            end_day TEXT NOT NULL,

            last_status TEXT,
            last_score NUMERIC(12,4),
            th_score   NUMERIC(12,4),
            last_cos   NUMERIC(12,6),
            max_score  NUMERIC(12,4),
            max_cos    NUMERIC(12,6),
            max_streak INT,
            crit_days  INT,
            warn_days  INT,

            cosine_similarity JSONB,
            score_from_normal JSONB,

            run_start_ts TIMESTAMPTZ,
            run_end_ts   TIMESTAMPTZ,

            updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (station, end_day)
        );
    """))

    cols = exec_with_work_mem(engine, text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
    """), {"s": TARGET_SCHEMA, "t": TARGET_TABLE}).fetchall()
    existing = {c[0] for c in cols}

    def add_col_if_missing(col_name: str, ddl: str):
        if col_name not in existing:
            exec_with_work_mem(engine, text(f"ALTER TABLE {TARGET_SCHEMA}.{TARGET_TABLE} ADD COLUMN {ddl};"))

    add_col_if_missing("cosine_similarity", "cosine_similarity JSONB")
    add_col_if_missing("score_from_normal", "score_from_normal JSONB")
    add_col_if_missing("run_start_ts", "run_start_ts TIMESTAMPTZ")
    add_col_if_missing("run_end_ts", "run_end_ts TIMESTAMPTZ")


def _upsert_pd_board_check_blocking(engine, payloads: list[dict], end_day: str):
    upsert_sql = text(f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE} (
            station, start_day, last_date, end_day,
            last_status, last_score, th_score, last_cos,
            max_score, max_cos, max_streak, crit_days, warn_days,
            cosine_similarity, score_from_normal,
            run_start_ts, run_end_ts,
            updated_at
        ) VALUES (
            :station, :start_day, :last_date, :end_day,
            :last_status, :last_score, :th_score, :last_cos,
            :max_score, :max_cos, :max_streak, :crit_days, :warn_days,
            CAST(:cosine_similarity AS JSONB),
            CAST(:score_from_normal AS JSONB),
            :run_start_ts, :run_end_ts,
            NOW()
        )
        ON CONFLICT (station, end_day)
        DO UPDATE SET
            start_day = EXCLUDED.start_day,
            last_date = EXCLUDED.last_date,
            last_status = EXCLUDED.last_status,
            last_score = EXCLUDED.last_score,
            th_score = EXCLUDED.th_score,
            last_cos = EXCLUDED.last_cos,
            max_score = EXCLUDED.max_score,
            max_cos = EXCLUDED.max_cos,
            max_streak = EXCLUDED.max_streak,
            crit_days = EXCLUDED.crit_days,
            warn_days = EXCLUDED.warn_days,
            cosine_similarity = EXCLUDED.cosine_similarity,
            score_from_normal = EXCLUDED.score_from_normal,
            run_start_ts = EXCLUDED.run_start_ts,
            run_end_ts   = EXCLUDED.run_end_ts,
            updated_at = NOW();
    """)

    eng = engine
    while True:
        try:
            with eng.begin() as conn:
                conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                conn.execute(upsert_sql, payloads)
            log(f"[OK] pd_board_check UPSERT done | end_day={end_day} | rows={len(payloads)}")
            return
        except Exception as e:
            log(f"[DB][RETRY] upsert failed: {type(e).__name__}: {repr(e)}")
            if _is_conn_error(e):
                _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)
            eng = get_engine_blocking()


def upsert_pd_board_check(engine, dfi: pd.DataFrame, summary: pd.DataFrame,
                          *, start_day: str, end_day: str,
                          run_start_ts, run_end_ts):
    ensure_pd_board_check(engine)

    if summary is None or summary.empty:
        log("[WARN] summary empty -> skip upsert")
        return

    payloads = []
    for _, r in summary.iterrows():
        st = r["station"]
        dfi_st = dfi[dfi["station"] == st].copy()

        cos_json = make_ts_json(dfi_st, "window_end_day", "cos_sim_to_ref", th_value=COS_TH)
        score_json = make_ts_json(
            dfi_st,
            "window_end_day",
            "score_from_normal",
            th_value=float(r["th_score"]) if pd.notna(r["th_score"]) else None
        )

        payloads.append({
            "station": st,
            "start_day": start_day,
            "last_date": r.get("last_date"),
            "end_day": end_day,

            "last_status": r.get("last_status"),
            "last_score": None if pd.isna(r.get("last_score")) else float(r.get("last_score")),
            "th_score":   None if pd.isna(r.get("th_score")) else float(r.get("th_score")),
            "last_cos":   None if pd.isna(r.get("last_cos")) else float(r.get("last_cos")),
            "max_score":  None if pd.isna(r.get("max_score")) else float(r.get("max_score")),
            "max_cos":    None if pd.isna(r.get("max_cos")) else float(r.get("max_cos")),
            "max_streak": None if pd.isna(r.get("max_streak")) else int(r.get("max_streak")),
            "crit_days":  None if pd.isna(r.get("crit_days")) else int(r.get("crit_days")),
            "warn_days":  None if pd.isna(r.get("warn_days")) else int(r.get("warn_days")),

            "cosine_similarity": json.dumps(sanitize_for_json(cos_json), ensure_ascii=False, allow_nan=False),
            "score_from_normal": json.dumps(sanitize_for_json(score_json), ensure_ascii=False, allow_nan=False),

            "run_start_ts": run_start_ts,
            "run_end_ts": run_end_ts,
        })

    _upsert_pd_board_check_blocking(engine, payloads, end_day=end_day)


# =========================
# 9) 1회 실행 파이프라인 (윈도우 안에서 50초 간격 호출)
# =========================
def run_once(engine):
    run_start_ts = datetime.now(timezone.utc)

    # ✅ 날짜 자동 산출(이 과정도 연결 끊기면 무한 재시도)
    start_day, last_date, end_day = calc_start_end_days(engine)
    log(f"[INFO] start_day={start_day} | last_date={last_date} | end_day(today)={end_day}")

    # ✅ 패턴 로드(끊김 시 무한 재시도)
    g_normal = load_pattern(engine, "FCT2", STEP_DESC, NORMAL_PATTERN_NAME)
    g_abn = load_pattern(engine, "FCT2", STEP_DESC, ABN_PATTERN_NAME)

    FEATURES = g_normal["features"]
    V_normal = np.array([g_normal["reference_pattern"][k] for k in FEATURES], dtype=float)
    A_ref = np.array([g_abn["reference_pattern"][k] for k in FEATURES], dtype=float)

    # ✅ 원천 → 일자 평균(끊김 시 무한 재시도)
    avg_df_all = load_avg_df(engine, STATIONS, start_day, end_day, STEP_DESC)

    # compare_df
    compare_df = build_compare_df(avg_df_all, V_normal, A_ref, FEATURES, WINDOW)

    # dfi/summary
    dfi, summary = build_dfi_and_summary(compare_df, start_day, end_day)

    run_end_ts = datetime.now(timezone.utc)

    # ✅ 저장(UPSERT) (끊김 시 무한 재시도)
    upsert_pd_board_check(
        engine, dfi, summary,
        start_day=start_day,
        end_day=end_day,
        run_start_ts=run_start_ts,
        run_end_ts=run_end_ts,
    )


# =========================
# 10) 스케줄 루프 (기존 스펙 그대로)
# =========================
def in_any_window(now: datetime) -> bool:
    t = now.time()
    return (WIN1_START <= t <= WIN1_END) or (WIN2_START <= t <= WIN2_END)


def main():
    # ✅ DB 접속 성공할 때까지 블로킹 + 엔진 1개 상시 유지
    engine = get_engine_blocking()
    log("[OK] engine ready (blocking ensured)")
    log(f"[SCHEDULE] WIN1={WIN1_START}~{WIN1_END}, WIN2={WIN2_START}~{WIN2_END} | run_every={RUN_EVERY_SEC}s")
    log(f"[DB] pool_size=1 max_overflow=0 | work_mem={WORK_MEM}")

    last_exec_dt = None
    was_in_window = False

    while True:
        now = datetime.now()
        inside = in_any_window(now)

        if not inside:
            # 윈도우 밖: 대기만
            if was_in_window:
                last_exec_dt = None
                was_in_window = False
                log("[INFO] left run window -> reset interval state")
            time.sleep(SLEEP_SEC)
            continue

        # 윈도우 안
        if not was_in_window:
            was_in_window = True
            log("[INFO] entered run window")

        # 1초 루프는 유지하되, 실제 실행은 50초 간격
        do_run = False
        if last_exec_dt is None:
            do_run = True
        else:
            elapsed = (now - last_exec_dt).total_seconds()
            if elapsed >= RUN_EVERY_SEC:
                do_run = True

        if do_run:
            last_exec_dt = now

            # ✅ 실행 중간에 DB가 끊겨도 run_once 내부에서 무한 복구하지만,
            #    시작 전에 가볍게 ping을 넣어 더 빨리 감지하도록 함.
            try:
                with engine.connect() as conn:
                    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
                    conn.execute(text("SELECT 1"))

                log(f"[RUN] start at {now.strftime('%Y-%m-%d %H:%M:%S')}")
                run_once(engine)
                log(f"[RUN] end   at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

            except Exception as e:
                log(f"[ERROR] run_once failed: {type(e).__name__}: {repr(e)}")
                # ✅ 연결 오류라면 엔진을 폐기하고 "연결 성공까지" 다시 확보
                if _is_conn_error(e):
                    _dispose_engine()
                engine = get_engine_blocking()
                log("[DB] engine re-acquired after failure (blocking ensured)")

        time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    main()
