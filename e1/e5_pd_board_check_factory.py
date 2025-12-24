# -*- coding: utf-8 -*-
"""
pd_board_check Scheduler (2 runs/day) + UPSERT + JSON columns  (INTEGRATED)

Spec
1) SUMMARY 출력 생략 (다른 DF도 출력 없음)
2) 하루 2회 실행:
   - 08:27:00 ~ 08:29:59 (1초 간격 반복)
   - 20:27:00 ~ 20:29:59 (1초 간격 반복)
3) end_day = 오늘 날짜(YYYYMMDD) 자동
   - 같은 날 2회 실행 시 PK(station,end_day) 기준 최신 결과로 update
4) start_day = last_date(실제 데이터 최신일) - 7일
5) 저장 값 반올림:
   - 스칼라(점수/코사인/threshold 등) => 소수점 2자리
   - JSON(dict) 내부 값 => 소수점 2자리
6) JSON key 확장:
   - 기존 mmdd(예: "1218") -> yyyymmdd(예: "20251218")

Save
- schema.table: e4_predictive_maintenance.pd_board_check
- JSONB columns:
  * cosine_similarity: {yyyymmdd: cos_sim_to_ref, ...}
  * score_from_normal: {yyyymmdd: score_from_normal, ...}

Note
- run windows 밖에서는 "대기"만 하며, 윈도우 진입 시 1초 루프로 반복 실행.
"""

import json
import time
import urllib.parse
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# ============================================================
# 0) 환경/상수 설정
# ============================================================

@dataclass(frozen=True)
class DBConfig:
    host: str = "100.105.75.47"
    port: int = 5432
    dbname: str = "postgres"
    user: str = "postgres"
    password: str = "leejangwoo1!"


# Source
SCHEMA = "a2_fct_table"
TABLE = "fct_table"

# Reference Pattern Table
REF_SCHEMA = "e4_predictive_maintenance"
REF_TABLE = "predictive_maintenance"

# Save Target
SAVE_SCHEMA = "e4_predictive_maintenance"
SAVE_TABLE = "pd_board_check"

# 분석 대상
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
STEP_DESC = "1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol"

# Sliding window
WINDOW = 5

# cosine threshold
COS_TH = 0.70

# robust threshold params
K_MAD = 4.0
MIN_SAMPLES_FOR_ROBUST = 8

# 존재하는 생산일자 기준 최대 몇 개를 사용할지(최근 N개 일자)
MAX_EXIST_DAYS = 7

# 실행 윈도우(로컬시간 기준)
RUN_WINDOWS: List[Tuple[str, str]] = [
    ("08:27:00", "08:29:59"),
    ("20:27:00", "20:29:59"),
]

# 윈도우 내 반복 주기
TICK_SEC = 1.0

# 윈도우 밖 대기 주기(너무 바쁘게 돌지 않도록)
IDLE_SLEEP_SEC = 5.0

# 반올림 자릿수(저장 직전)
ROUND_N = 2


# ============================================================
# 1) DB 유틸
# ============================================================

def get_engine(cfg: DBConfig):
    password = urllib.parse.quote_plus(cfg.password)
    conn_str = f"postgresql+psycopg2://{cfg.user}:{password}@{cfg.host}:{cfg.port}/{cfg.dbname}"
    return create_engine(conn_str, pool_pre_ping=True)


def ensure_save_table(engine):
    """
    e4_predictive_maintenance.pd_board_check 없으면 생성
    PK: (station, end_day)
    """
    q_schema = text(f"CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA}")
    q_table = text(f"""
        CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{SAVE_TABLE} (
            station            text NOT NULL,
            start_day          text NOT NULL,
            last_date          date,
            end_day            text NOT NULL,

            last_status        text,
            last_score         double precision,
            th_score           double precision,
            last_cos           double precision,
            max_score          double precision,
            max_cos            double precision,
            max_streak         integer,
            crit_days          integer,
            warn_days          integer,

            cosine_similarity  jsonb,
            score_from_normal  jsonb,

            updated_at         timestamptz NOT NULL DEFAULT now(),

            PRIMARY KEY (station, end_day)
        )
    """)
    with engine.begin() as conn:
        conn.execute(q_schema)
        conn.execute(q_table)


def upsert_summary_rows(engine, df_summary: pd.DataFrame):
    """
    df_summary를 station,end_day PK 기준 UPSERT
    """
    q = text(f"""
        INSERT INTO {SAVE_SCHEMA}.{SAVE_TABLE} (
            station, start_day, last_date, end_day,
            last_status, last_score, th_score, last_cos,
            max_score, max_cos, max_streak,
            crit_days, warn_days,
            cosine_similarity, score_from_normal
        ) VALUES (
            :station, :start_day, :last_date, :end_day,
            :last_status, :last_score, :th_score, :last_cos,
            :max_score, :max_cos, :max_streak,
            :crit_days, :warn_days,
            CAST(:cosine_similarity AS jsonb),
            CAST(:score_from_normal AS jsonb)
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
            updated_at = now()
    """)

    rows = []
    for _, r in df_summary.iterrows():
        rows.append({
            "station": str(r["station"]),
            "start_day": str(r["start_day"]),
            "last_date": None if pd.isna(r["last_date"]) else pd.to_datetime(r["last_date"]).date(),
            "end_day": str(r["end_day"]),
            "last_status": None if pd.isna(r["last_status"]) else str(r["last_status"]),
            "last_score": None if pd.isna(r["last_score"]) else float(r["last_score"]),
            "th_score": None if pd.isna(r["th_score"]) else float(r["th_score"]),
            "last_cos": None if pd.isna(r["last_cos"]) else float(r["last_cos"]),
            "max_score": None if pd.isna(r["max_score"]) else float(r["max_score"]),
            "max_cos": None if pd.isna(r["max_cos"]) else float(r["max_cos"]),
            "max_streak": None if pd.isna(r["max_streak"]) else int(r["max_streak"]),
            "crit_days": None if pd.isna(r["crit_days"]) else int(r["crit_days"]),
            "warn_days": None if pd.isna(r["warn_days"]) else int(r["warn_days"]),
            "cosine_similarity": json.dumps(r["cosine_similarity"], ensure_ascii=False) if isinstance(r["cosine_similarity"], dict) else json.dumps({}, ensure_ascii=False),
            "score_from_normal": json.dumps(r["score_from_normal"], ensure_ascii=False) if isinstance(r["score_from_normal"], dict) else json.dumps({}, ensure_ascii=False),
        })

    with engine.begin() as conn:
        conn.execute(q, rows)


# ============================================================
# 2) 분석 유틸
# ============================================================

def round_float(v, n: int = ROUND_N):
    try:
        return round(float(v), n)
    except Exception:
        return v


def round_json_values(d: dict, n: int = ROUND_N) -> dict:
    """
    JSON(dict) 내부 float 값을 소수 n자리로 반올림
    """
    if not isinstance(d, dict):
        return d
    out = {}
    for k, v in d.items():
        out[k] = round_float(v, n)
    return out


def window_vector_from_values(values) -> Dict[str, float]:
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


def cosine_sim(a: np.ndarray, b: np.ndarray, eps: float = 1e-12) -> float:
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


def robust_threshold(x: pd.Series, k: float = K_MAD) -> float:
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


def classify_status(is_cos_like: bool, is_score_high: bool) -> str:
    if is_cos_like and is_score_high:
        return "CRITICAL"
    if is_cos_like and (not is_score_high):
        return "WARNING"
    if (not is_cos_like) and is_score_high:
        return "WATCH"
    return "OK"


def add_consecutive_alerts(df_station: pd.DataFrame, alert_levels: set) -> pd.DataFrame:
    df_station = df_station.sort_values("date").copy()
    consec = 0
    out = []
    for _, r in df_station.iterrows():
        if r["status"] in alert_levels:
            consec += 1
        else:
            consec = 0
        out.append(consec)
    df_station["alert_streak"] = out
    return df_station


def load_pattern(engine, station: str, step_desc: str, pattern_name: str) -> Dict[str, Any]:
    q = text(f"""
        SELECT euclid_graph
        FROM {REF_SCHEMA}.{REF_TABLE}
        WHERE station=:station AND step_description=:step_desc AND pattern_name=:pattern_name
    """)
    with engine.begin() as conn:
        row = conn.execute(q, {"station": station, "step_desc": step_desc, "pattern_name": pattern_name}).fetchone()
    if row is None:
        raise ValueError(f"[ERROR] Pattern not found: {station} / {pattern_name}")
    g = row[0]
    if isinstance(g, str):
        g = json.loads(g)
    return g


def resolve_available_days(engine, stations: List[str], step_desc: str, user_end_day: str, max_days: int = MAX_EXIST_DAYS) -> Dict[str, str]:
    """
    user_end_day(YYYYMMDD) 이하에서 존재하는 end_day 중 가장 가까운 최근 날짜를 effective_end로 선택
    effective_end 포함 최근 max_days개의 존재 일자를 사용 (start_effective)
    """
    q = text(f"""
        SELECT DISTINCT replace(CAST(end_day AS text), '-', '') AS yyyymmdd
        FROM {SCHEMA}.{TABLE}
        WHERE station = ANY(:stations)
          AND step_description = :step_desc
          AND replace(CAST(end_day AS text), '-', '') <= :user_end_day
        ORDER BY yyyymmdd
    """)
    with engine.begin() as conn:
        days_df = pd.read_sql(q, conn, params={"stations": stations, "step_desc": step_desc, "user_end_day": user_end_day})

    if days_df.empty:
        raise ValueError(f"[ERROR] No available end_day <= {user_end_day} for given stations/step_desc")

    effective_end = str(days_df["yyyymmdd"].iloc[-1])
    tail_days = days_df["yyyymmdd"].tail(max_days).tolist()
    effective_start = str(tail_days[0])
    base_year = effective_end[:4]

    return {"RAW_START_DAY": effective_start, "RAW_END_DAY": effective_end, "BASE_YEAR": base_year}


# ============================================================
# 3) 1회 실행: SUMMARY 생성 + JSON 컬럼 + DB 저장 (출력 없음)
# ============================================================

def run_once(engine, end_day_today: str):
    # effective 범위(존재일자 기준)
    day_cfg = resolve_available_days(engine, STATIONS, STEP_DESC, end_day_today, max_days=MAX_EXIST_DAYS)
    raw_start = day_cfg["RAW_START_DAY"]
    raw_end = day_cfg["RAW_END_DAY"]
    base_year = day_cfg["BASE_YEAR"]

    # raw 로드 + 일자 평균
    sql_q = text(f"""
        SELECT
          replace(CAST(end_day AS text), '-', '') AS end_day_yyyymmdd,
          station,
          value
        FROM {SCHEMA}.{TABLE}
        WHERE station = ANY(:stations)
          AND replace(CAST(end_day AS text), '-', '') BETWEEN :start_day AND :end_day
          AND step_description = :step_desc
        ORDER BY station, end_day_yyyymmdd
    """)
    with engine.begin() as conn:
        raw = pd.read_sql(sql_q, conn, params={
            "stations": STATIONS,
            "start_day": raw_start,
            "end_day": raw_end,
            "step_desc": STEP_DESC
        })

    raw["value_num"] = pd.to_numeric(raw["value"], errors="coerce")
    raw = raw.dropna(subset=["value_num"]).copy()
    raw["mmdd"] = raw["end_day_yyyymmdd"].str.slice(4, 8)

    avg_df_all = (
        raw.groupby(["station", "mmdd"], as_index=False)
           .agg(value_avg=("value_num", "mean"), sample_amount=("value_num", "count"))
           .sort_values(["station", "mmdd"])
    )
    # 표시/저장 목적이므로 여기 round(2) 유지
    avg_df_all["value_avg"] = avg_df_all["value_avg"].round(2)

    # base_year + mmdd -> date
    avg_df_all["date"] = pd.to_datetime(base_year + avg_df_all["mmdd"], format="%Y%m%d", errors="coerce")
    avg_df_all = avg_df_all.dropna(subset=["date"]).reset_index(drop=True)

    # 패턴 로드
    g_normal = load_pattern(engine, "FCT2", STEP_DESC, "pd_board_normal_ref")
    g_abn = load_pattern(engine, "FCT2", STEP_DESC, "pd_board_degradation_ref")

    features: List[str] = g_normal["features"]
    v_normal = np.array([g_normal["reference_pattern"][k] for k in features], dtype=float)
    a_ref = np.array([g_abn["reference_pattern"][k] for k in features], dtype=float)

    # compare_df 생성
    rows = []
    for st in STATIONS:
        df_st = avg_df_all[avg_df_all["station"] == st].copy()
        if len(df_st) < WINDOW:
            continue
        df_st = df_st.sort_values("date").reset_index(drop=True)

        for i in range(len(df_st) - WINDOW + 1):
            chunk = df_st.iloc[i:i + WINDOW]
            mmdd_end = str(chunk["mmdd"].iloc[-1])

            v = window_vector_from_values(chunk["value_avg"].values)
            v_t = np.array([v[k] for k in features], dtype=float)
            a_t = v_t - v_normal

            rows.append({
                "station": st,
                "mmdd": mmdd_end,
                "score_from_normal": float(np.linalg.norm(a_t)),
                "cos_sim_to_ref": cosine_sim(a_t, a_ref),
            })

    compare_df = pd.DataFrame(rows).sort_values(["station", "mmdd"]).reset_index(drop=True)
    if compare_df.empty:
        # 데이터 부족: 저장 스킵 (조용히)
        return

    # threshold + status + streak
    compare_df["score_from_normal"] = compare_df["score_from_normal"].astype(float)
    th_df = (
        compare_df.groupby("station")["score_from_normal"]
        .apply(lambda s: robust_threshold(s, k=K_MAD))
        .reset_index(name="th_score")
    )

    dfi = compare_df.merge(th_df, on="station", how="left").copy()
    dfi["date"] = pd.to_datetime(base_year + dfi["mmdd"], format="%Y%m%d", errors="coerce")
    dfi = dfi.dropna(subset=["date"]).copy()

    dfi["is_cos_like"] = dfi["cos_sim_to_ref"] >= COS_TH
    dfi["is_score_high"] = dfi["score_from_normal"] >= dfi["th_score"]
    dfi["status"] = dfi.apply(lambda r: classify_status(bool(r["is_cos_like"]), bool(r["is_score_high"])), axis=1)

    alert_levels = {"WARNING", "CRITICAL"}
    dfi = (
        dfi.groupby("station", as_index=True, group_keys=True)
           .apply(lambda g: add_consecutive_alerts(g, alert_levels), include_groups=False)
           .reset_index(level=0)
           .reset_index(drop=True)
    )

    # SUMMARY
    summary = (
        dfi.sort_values(["station", "date"])
           .groupby("station")
           .agg(
               last_date=("date", "max"),
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
           .reset_index()
    )

    # end_day는 "오늘" 고정(입력 없음)
    summary["end_day"] = end_day_today

    # start_day = last_date - 7일 (station별)
    summary["start_day"] = (pd.to_datetime(summary["last_date"]) - pd.Timedelta(days=7)).dt.strftime("%Y%m%d")

    # =========================
    # JSON 시계열(dict) 생성
    # - key: yyyymmdd (확장)
    # - value: 저장 직전 round(2)
    # =========================
    cos_dict_by_station: Dict[str, Dict[str, float]] = {}
    score_dict_by_station: Dict[str, Dict[str, float]] = {}

    for st in STATIONS:
        sdf = dfi[dfi["station"] == st].copy()  # ✅ date를 가지고 있으므로 yyyymmdd 생성 가능
        if sdf.empty:
            cos_dict_by_station[st] = {}
            score_dict_by_station[st] = {}
            continue

        sdf = sdf.sort_values("date")
        sdf["yyyymmdd"] = sdf["date"].dt.strftime("%Y%m%d")

        cos_map = dict(zip(sdf["yyyymmdd"].astype(str), sdf["cos_sim_to_ref"].astype(float)))
        score_map = dict(zip(sdf["yyyymmdd"].astype(str), sdf["score_from_normal"].astype(float)))

        cos_dict_by_station[st] = round_json_values(cos_map, ROUND_N)
        score_dict_by_station[st] = round_json_values(score_map, ROUND_N)

    summary["cosine_similarity"] = summary["station"].map(lambda st: cos_dict_by_station.get(st, {}))
    summary["score_from_normal"] = summary["station"].map(lambda st: score_dict_by_station.get(st, {}))

    # =========================
    # [SAVE-TIME] 스칼라 round(2)
    # =========================
    for c in ["last_score", "th_score", "last_cos", "max_score", "max_cos"]:
        if c in summary.columns:
            summary[c] = pd.to_numeric(summary[c], errors="coerce").round(ROUND_N)

    # JSON 방어적 round(2)
    summary["cosine_similarity"] = summary["cosine_similarity"].apply(lambda d: round_json_values(d, ROUND_N))
    summary["score_from_normal"] = summary["score_from_normal"].apply(lambda d: round_json_values(d, ROUND_N))

    # 컬럼 순서 고정
    summary = summary[[
        "station",
        "start_day",
        "last_date",
        "end_day",
        "last_status",
        "last_score",
        "th_score",
        "last_cos",
        "max_score",
        "max_cos",
        "max_streak",
        "crit_days",
        "warn_days",
        "cosine_similarity",
        "score_from_normal",
    ]]

    # 저장
    ensure_save_table(engine)
    upsert_summary_rows(engine, summary)


# ============================================================
# 4) 스케줄 실행
# ============================================================

def _parse_hms(hms: str) -> Tuple[int, int, int]:
    hh, mm, ss = hms.split(":")
    return int(hh), int(mm), int(ss)


def is_now_in_windows(now: datetime, windows: List[Tuple[str, str]]) -> bool:
    t = now.time()
    for s, e in windows:
        sh, sm, ss = _parse_hms(s)
        eh, em, es = _parse_hms(e)
        start_t = datetime(now.year, now.month, now.day, sh, sm, ss).time()
        end_t = datetime(now.year, now.month, now.day, eh, em, es).time()
        if start_t <= t <= end_t:
            return True
    return False


def main():
    engine = get_engine(DBConfig())
    ensure_save_table(engine)

    while True:
        try:
            now = datetime.now()

            if is_now_in_windows(now, RUN_WINDOWS):
                end_day_today = now.strftime("%Y%m%d")
                # 윈도우 안에서는 1초 반복
                try:
                    run_once(engine, end_day_today)
                except Exception:
                    # 출력 요구가 없으므로 조용히 무시(필요하면 로그로 교체)
                    pass
                time.sleep(TICK_SEC)
            else:
                # 윈도우 밖: 대기
                time.sleep(IDLE_SLEEP_SEC)

        except KeyboardInterrupt:
            break


if __name__ == "__main__":
    main()
