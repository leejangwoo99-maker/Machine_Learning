# -*- coding: utf-8 -*-
"""
Predictive Maintenance - Sliding Window Anomaly Compare
- Infinite loop: each iteration asks for end_day input (YYYYMMDD)
- SUMMARY only printed
- Save SUMMARY into e4_predictive_maintenance.pd_board_check (UPSERT)
- Add JSONB columns at the end:
  * cosine_similarity: {mmdd: cos_sim_to_ref, ...} per station
  * score_from_normal: {mmdd: score_from_normal, ...} per station
"""

import json
import time
import urllib.parse
from dataclasses import dataclass
from typing import Dict, Any, List

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# ============================================================
# 0) 설정
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

# Reference Pattern Table (정상/이상 패턴 로드용)
REF_SCHEMA = "e4_predictive_maintenance"
REF_TABLE = "predictive_maintenance"

# Save Target
SAVE_SCHEMA = "e4_predictive_maintenance"
SAVE_TABLE = "pd_board_check"

# 분석 대상
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
STEP_DESC = "1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol"

# 슬라이딩 윈도우
WINDOW = 5

# cosine 임계값
COS_TH = 0.70

# robust threshold 파라미터
K_MAD = 4.0
MIN_SAMPLES_FOR_ROBUST = 8

# 최근 "존재하는 end_day" 최대 몇 개를 사용할지 (최대 7일치)
MAX_DAYS = 7


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
            station, start_day, end_day,
            last_status, last_score, th_score, last_cos,
            max_score, max_cos, max_streak,
            crit_days, warn_days, last_date,
            cosine_similarity, score_from_normal
        ) VALUES (
            :station, :start_day, :end_day,
            :last_status, :last_score, :th_score, :last_cos,
            :max_score, :max_cos, :max_streak,
            :crit_days, :warn_days, :last_date,
            CAST(:cosine_similarity AS jsonb),
            CAST(:score_from_normal AS jsonb)
        )
        ON CONFLICT (station, end_day)
        DO UPDATE SET
            start_day = EXCLUDED.start_day,
            last_status = EXCLUDED.last_status,
            last_score = EXCLUDED.last_score,
            th_score = EXCLUDED.th_score,
            last_cos = EXCLUDED.last_cos,
            max_score = EXCLUDED.max_score,
            max_cos = EXCLUDED.max_cos,
            max_streak = EXCLUDED.max_streak,
            crit_days = EXCLUDED.crit_days,
            warn_days = EXCLUDED.warn_days,
            last_date = EXCLUDED.last_date,
            cosine_similarity = EXCLUDED.cosine_similarity,
            score_from_normal = EXCLUDED.score_from_normal,
            updated_at = now()
    """)

    # psycopg2는 dict/list를 바로 jsonb로 캐스팅하기 애매해서 문자열로 넣습니다.
    rows = []
    for _, r in df_summary.iterrows():
        rows.append({
            "station": str(r["station"]),
            "start_day": str(r["start_day"]),
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
            "last_date": None if pd.isna(r["last_date"]) else pd.to_datetime(r["last_date"]).date(),
            "cosine_similarity": json.dumps(r["cosine_similarity"], ensure_ascii=False) if isinstance(r["cosine_similarity"], dict) else json.dumps({}, ensure_ascii=False),
            "score_from_normal": json.dumps(r["score_from_normal"], ensure_ascii=False) if isinstance(r["score_from_normal"], dict) else json.dumps({}, ensure_ascii=False),
        })

    with engine.begin() as conn:
        conn.execute(q, rows)


# ============================================================
# 2) 분석 유틸
# ============================================================

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


def build_input_range(end_day_input: str) -> Dict[str, str]:
    end_dt = pd.to_datetime(end_day_input, format="%Y%m%d")
    start_dt = end_dt - pd.Timedelta(days=7)
    return {"start_day_input": start_dt.strftime("%Y%m%d"), "end_day_input": end_dt.strftime("%Y%m%d")}


def resolve_available_days(engine, stations: List[str], step_desc: str, user_end_day: str, max_days: int = MAX_DAYS) -> Dict[str, str]:
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
    end_effective = str(days_df["yyyymmdd"].iloc[-1])
    tail_days = days_df["yyyymmdd"].tail(max_days).tolist()
    start_effective = str(tail_days[0])
    base_year = end_effective[:4]
    return {"RAW_START_DAY": start_effective, "RAW_END_DAY": end_effective, "BASE_YEAR": base_year}


# ============================================================
# 3) 1회 실행: SUMMARY 생성 + JSON 컬럼 추가 + DB 저장 + 출력
# ============================================================

def run_once(engine, end_day_input: str) -> pd.DataFrame:
    # (1) 입력 기반 start/end (저장/표기용)
    input_range = build_input_range(end_day_input)
    start_day_input = input_range["start_day_input"]
    end_day_input = input_range["end_day_input"]  # 정규화

    # (2) 분석용 effective 범위(존재일자 기반)
    day_cfg = resolve_available_days(engine, STATIONS, STEP_DESC, end_day_input, max_days=MAX_DAYS)
    RAW_START_DAY = day_cfg["RAW_START_DAY"]
    RAW_END_DAY = day_cfg["RAW_END_DAY"]
    BASE_YEAR_FOR_MMDD = day_cfg["BASE_YEAR"]

    # (3) Raw 로드 + 일자 평균
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
            "start_day": RAW_START_DAY,
            "end_day": RAW_END_DAY,
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
    avg_df_all["value_avg"] = avg_df_all["value_avg"].round(2)
    avg_df_all["date"] = pd.to_datetime(BASE_YEAR_FOR_MMDD + avg_df_all["mmdd"], format="%Y%m%d", errors="coerce")
    avg_df_all = avg_df_all.dropna(subset=["date"]).reset_index(drop=True)

    # (4) 패턴 로드
    g_normal = load_pattern(engine, "FCT2", STEP_DESC, "pd_board_normal_ref")
    g_abn = load_pattern(engine, "FCT2", STEP_DESC, "pd_board_degradation_ref")

    features: List[str] = g_normal["features"]
    v_normal = np.array([g_normal["reference_pattern"][k] for k in features], dtype=float)
    a_ref = np.array([g_abn["reference_pattern"][k] for k in features], dtype=float)

    # (5) compare_df 생성
    rows = []
    for st in STATIONS:
        df_st = avg_df_all[avg_df_all["station"] == st].copy()
        if len(df_st) < WINDOW:
            continue
        df_st = df_st.sort_values("date").reset_index(drop=True)

        for i in range(len(df_st) - WINDOW + 1):
            chunk = df_st.iloc[i:i + WINDOW]
            mmdd_end = chunk["mmdd"].iloc[-1]

            v = window_vector_from_values(chunk["value_avg"].values)
            v_t = np.array([v[k] for k in features], dtype=float)
            a_t = v_t - v_normal

            rows.append({
                "station": st,
                "mmdd": mmdd_end,
                "score_from_normal": float(np.linalg.norm(a_t)),
                "cos_sim_to_ref": cosine_sim(a_t, a_ref),
                "dist_to_ref": float(np.linalg.norm(a_t - a_ref)),
            })

    compare_df = pd.DataFrame(rows).sort_values(["station", "mmdd"]).reset_index(drop=True)
    if compare_df.empty:
        # SUMMARY 테이블에는 최소한 “입력값 기반” 기록을 남길지 여부가 애매하므로, 여기서는 저장하지 않고 경고만 출력
        print(f"\n[SUMMARY]\n(empty)  (not enough data for WINDOW={WINDOW})\n")
        return compare_df

    compare_df["score_from_normal"] = compare_df["score_from_normal"].round(2)
    compare_df["cos_sim_to_ref"] = compare_df["cos_sim_to_ref"].round(3)
    compare_df["dist_to_ref"] = compare_df["dist_to_ref"].round(2)

    # (6) threshold + status + streak
    th_df = (
        compare_df.groupby("station")["score_from_normal"]
        .apply(lambda s: robust_threshold(s, k=K_MAD))
        .reset_index(name="th_score")
    )

    dfi = compare_df.merge(th_df, on="station", how="left").copy()
    dfi["date"] = pd.to_datetime(BASE_YEAR_FOR_MMDD + dfi["mmdd"], format="%Y%m%d")

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

    # (7) SUMMARY 생성
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

    # (8) start_day / end_day 컬럼 (last_date 기준)
    summary["end_day"] = end_day_input

    summary["start_day"] = (
            pd.to_datetime(summary["last_date"]) - pd.Timedelta(days=7)
    ).dt.strftime("%Y%m%d")

    # (9) JSON 컬럼 2개 추가: station별 시계열(dict)
    cos_dict_by_station = {}
    score_dict_by_station = {}

    for st in STATIONS:
        sdf = compare_df[compare_df["station"] == st].copy()
        if sdf.empty:
            cos_dict_by_station[st] = {}
            score_dict_by_station[st] = {}
            continue

        # mmdd를 key로 저장 (요청: json 형식 속성값)
        cos_map = dict(zip(sdf["mmdd"].astype(str), sdf["cos_sim_to_ref"].astype(float)))
        score_map = dict(zip(sdf["mmdd"].astype(str), sdf["score_from_normal"].astype(float)))

        cos_dict_by_station[st] = cos_map
        score_dict_by_station[st] = score_map

    summary["cosine_similarity"] = summary["station"].map(lambda st: cos_dict_by_station.get(st, {}))
    summary["score_from_normal"] = summary["station"].map(lambda st: score_dict_by_station.get(st, {}))

    # (10) 요청: SUMMARY 컬럼 순서 고정 + 마지막에 JSON 2컬럼
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

    # (11) DB 저장 (UPSERT)
    ensure_save_table(engine)
    upsert_summary_rows(engine, summary)

    # (12) SUMMARY만 출력
    print("\n[SUMMARY]")
    print(summary.to_string(index=False))

    return summary


# ============================================================
# 4) 무한 루프: 매번 end_day 입력
# ============================================================

def main():
    print("[OK] window_vector_from_values loaded")
    engine = get_engine(DBConfig())
    print("[OK] SQLAlchemy engine created")

    print("\n[INFO] Infinite loop mode: each iteration asks for end_day (Ctrl+C to stop)\n")

    while True:
        try:
            end_day_input = input("기준 날짜 입력 (YYYYMMDD, 예: 20251223): ").strip()
            if not (end_day_input.isdigit() and len(end_day_input) == 8):
                print(f"[WARN] Invalid format: {end_day_input}  (must be YYYYMMDD)")
                continue

            run_once(engine, end_day_input)

        except KeyboardInterrupt:
            print("\n[INFO] Stopped by user (Ctrl+C).")
            break
        except Exception as e:
            print(f"[ERROR] {type(e).__name__}: {e}")

        # 입력 기반 반복이므로, 너무 빠른 재질의 방지용(원치 않으면 0)
        time.sleep(0.1)


if __name__ == "__main__":
    main()
