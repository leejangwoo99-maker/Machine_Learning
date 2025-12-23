# -*- coding: utf-8 -*-
"""
e5_pd_board_check.py

요구사항(통합본)
1) summary(DataFrame) -> e4_predictive_maintenance.pd_board_check 저장
2) 그래프(plotly json) -> e4_predictive_maintenance.pd_board_check_graph 저장
   - 컬럼 last_date 포함
   - 컬럼 plotly_json (JSONB) 안에 plotly json 저장
   - ※ pandas.to_sql + psycopg2에서 dict 직접 저장 시 'can't adapt type dict' 발생
     -> json.dumps()로 문자열로 변환 후 저장(가장 안정적)

전제
- a2_fct_table.fct_table: end_day, station, step_description, value 존재
- e4_predictive_maintenance.predictive_maintenance: euclid_graph(JSONB)로 기준 패턴 저장됨
"""

import json
import urllib.parse
from datetime import datetime

import numpy as np
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SOURCE_SCHEMA = "a2_fct_table"
SOURCE_TABLE = "fct_table"

REF_SCHEMA = "e4_predictive_maintenance"
REF_TABLE = "predictive_maintenance"

OUT_SCHEMA = "e4_predictive_maintenance"
OUT_TABLE_CHECK = "pd_board_check"
OUT_TABLE_GRAPH = "pd_board_check_graph"

# 분석 대상
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
STEP_DESC = "1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol"

# raw 조회 범위(일자평균 산출용)
RAW_START_DAY = "20251117"
RAW_END_DAY = "20251218"

# 분석 기간(슬라이딩 윈도우 적용 구간)
START_DAY = "20251121"
END_DAY = "20251218"

BASE_YEAR = "2025"
WINDOW = 5
COS_TH = 0.70

# robust threshold
K_MAD = 4.0
MIN_SAMPLES_FOR_ROBUST = 8


# =========================
# 1) 공용 함수
# =========================
def get_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str, pool_pre_ping=True)


def window_vector_from_values(values):
    """일자 평균 시계열 → 5차원 특성 벡터"""
    values = np.asarray(values, dtype=float)

    if len(values) < 2:
        return {
            "mean": np.nan,
            "std": np.nan,
            "amplitude": np.nan,
            "diff_mean": np.nan,
            "diff_std": np.nan,
        }

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


def mmdd_to_date(mmdd: str, base_year: str = "2025") -> pd.Timestamp:
    mmdd = str(mmdd)
    if len(mmdd) == 8:
        return pd.to_datetime(mmdd, format="%Y%m%d")
    if len(mmdd) == 4:
        return pd.to_datetime(base_year + mmdd, format="%Y%m%d")
    raise ValueError(f"[ERROR] Invalid mmdd format: {mmdd}")


def sanitize_for_json(obj):
    """JSONB 저장용: NaN/Inf -> None"""
    if isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: sanitize_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [sanitize_for_json(v) for v in obj]
    return obj


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


def load_pattern(engine, station, step_desc, pattern_name):
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


# =========================
# 2) DB 테이블 생성
# =========================
def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA};"))

        # ❗ IF NOT EXISTS 제거 + 컬럼 순서 확정
        conn.execute(text(f"""
        CREATE TABLE {OUT_SCHEMA}.{OUT_TABLE_CHECK} (
            station          text        NOT NULL,
            step_description text        NOT NULL,
            start_day        text,
            end_day          text,
            last_date        date        NOT NULL,

            last_status      text,
            last_score       double precision,
            th_score         double precision,
            last_cos         double precision,

            max_score        double precision,
            max_cos          double precision,
            max_streak       integer,
            crit_days        integer,
            warn_days        integer,

            window_size      integer,
            updated_at       timestamp   NOT NULL DEFAULT now(),

            PRIMARY KEY (station, step_description, last_date)
        );
        """))

        conn.execute(text(f"""
        CREATE TABLE {OUT_SCHEMA}.{OUT_TABLE_GRAPH} (
            station          text        NOT NULL,
            step_description text        NOT NULL,
            start_day        text,
            end_day          text,
            last_date        date        NOT NULL,

            plotly_json      jsonb       NOT NULL,

            window_size      integer,
            updated_at       timestamp   NOT NULL DEFAULT now(),

            PRIMARY KEY (station, step_description, last_date)
        );
        """))

# =========================
# 3) 메인 로직
# =========================
def main():
    engine = get_engine()
    ensure_tables(engine)
    print("[OK] engine / tables ready")

    # -----------------------------
    # (A) 원본 조회 → 일자 평균
    # -----------------------------
    sql_src = text(f"""
        SELECT
          replace(CAST(end_day AS text), '-', '') AS end_day_yyyymmdd,
          station,
          value
        FROM {SOURCE_SCHEMA}.{SOURCE_TABLE}
        WHERE station = ANY(:stations)
          AND replace(CAST(end_day AS text), '-', '') BETWEEN :start_day AND :end_day
          AND step_description = :step_desc
        ORDER BY station, end_day_yyyymmdd
    """)

    with engine.begin() as conn:
        raw = pd.read_sql(
            sql_src,
            conn,
            params={"stations": STATIONS, "start_day": RAW_START_DAY, "end_day": RAW_END_DAY, "step_desc": STEP_DESC},
        )

    raw["value_num"] = pd.to_numeric(raw["value"], errors="coerce")
    raw = raw.dropna(subset=["value_num"]).copy()
    raw["mmdd"] = raw["end_day_yyyymmdd"].str.slice(4, 8)

    avg_df_all = (
        raw.groupby(["station", "mmdd"], as_index=False)
           .agg(value_avg=("value_num", "mean"), sample_amount=("value_num", "count"))
           .sort_values(["station", "mmdd"])
    )
    avg_df_all["value_avg"] = avg_df_all["value_avg"].round(2)
    print("[OK] avg_df_all built:", avg_df_all.shape)

    # -----------------------------
    # (B) 분석 기간 필터
    # -----------------------------
    START_DT = pd.to_datetime(START_DAY, format="%Y%m%d")
    END_DT = pd.to_datetime(END_DAY, format="%Y%m%d")

    avg_df_all = avg_df_all.copy()
    avg_df_all["date"] = avg_df_all["mmdd"].apply(lambda x: mmdd_to_date(x, base_year=BASE_YEAR))
    avg_df_all = avg_df_all[(avg_df_all["date"] >= START_DT) & (avg_df_all["date"] <= END_DT)].reset_index(drop=True)
    print(f"[OK] avg_df_all filtered rows = {len(avg_df_all):,}")

    # -----------------------------
    # (C) 기준 패턴 로드 (FCT2 기준)
    # -----------------------------
    g_normal = load_pattern(engine, "FCT2", STEP_DESC, "pd_board_normal_ref")
    g_abn = load_pattern(engine, "FCT2", STEP_DESC, "pd_board_degradation_ref")

    FEATURES = g_normal["features"]
    V_normal = np.array([g_normal["reference_pattern"][k] for k in FEATURES], dtype=float)
    A_ref = np.array([g_abn["reference_pattern"][k] for k in FEATURES], dtype=float)

    print("[OK] FEATURES =", FEATURES)

    # -----------------------------
    # (D) Sliding window → compare_df
    # -----------------------------
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
            V_t = np.array([v[k] for k in FEATURES], dtype=float)
            A_t = V_t - V_normal

            rows.append({
                "station": st,
                "mmdd": mmdd_end,
                "date": pd.to_datetime(chunk["date"].iloc[-1]).to_pydatetime(),
                "score_from_normal": float(np.linalg.norm(A_t)),
                "cos_sim_to_ref": cosine_sim(A_t, A_ref),
                "dist_to_ref": float(np.linalg.norm(A_t - A_ref)),
            })

    compare_df = pd.DataFrame(rows).sort_values(["station", "date"]).reset_index(drop=True)

    compare_df["score_from_normal"] = compare_df["score_from_normal"].round(2)
    compare_df["cos_sim_to_ref"] = compare_df["cos_sim_to_ref"].round(3)
    compare_df["dist_to_ref"] = compare_df["dist_to_ref"].round(2)

    print("[OK] compare_df built:", compare_df.shape)

    # -----------------------------
    # (E) Threshold → status → summary
    # -----------------------------
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

    ALERT_LEVELS = {"WARNING", "CRITICAL"}

    def add_consecutive_alerts(df_station: pd.DataFrame) -> pd.DataFrame:
        df_station = df_station.sort_values("date").copy()
        consec = 0
        out = []
        for _, r in df_station.iterrows():
            if r["status"] in ALERT_LEVELS:
                consec += 1
            else:
                consec = 0
            out.append(consec)
        df_station["alert_streak"] = out
        return df_station

    # include_groups=False 는 pandas 버전에 따라 없을 수 있어 안전 처리
    try:
        dfi = (
            dfi.groupby("station", as_index=True, group_keys=True)
               .apply(add_consecutive_alerts, include_groups=False)
               .reset_index(level=0)
               .reset_index(drop=True)
        )
    except TypeError:
        dfi = (
            dfi.groupby("station", as_index=True, group_keys=True)
               .apply(add_consecutive_alerts)
               .reset_index(level=0)
               .reset_index(drop=True)
        )

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

    summary["step_description"] = STEP_DESC
    summary["window_size"] = WINDOW
    summary["start_day"] = START_DAY
    summary["end_day"] = END_DAY
    summary["updated_at"] = datetime.now()

    # -----------------------------
    # (F) Plotly 그래프 → JSON 문자열 (to_sql 안전)
    # -----------------------------
    viz_df = compare_df.copy().sort_values(["station", "date"]).reset_index(drop=True)

    fig_cos = px.line(
        viz_df,
        x="date",
        y="cos_sim_to_ref",
        color="station",
        markers=True,
        title=f"Cosine Similarity to Abnormal Reference (COS_TH={COS_TH})",
    )
    fig_cos.add_hline(y=COS_TH, line_dash="dash", annotation_text="COS_TH", annotation_position="top left")
    fig_cos.update_xaxes(tickformat="%m%d", title_text="mmdd")

    fig_score = px.line(
        viz_df,
        x="date",
        y="score_from_normal",
        color="station",
        markers=True,
        title="Score from Normal Baseline (||A_t||)",
    )
    fig_score.update_xaxes(tickformat="%m%d", title_text="mmdd")

    # ✅ dict -> JSON 문자열
    plotly_json_obj = sanitize_for_json({
        "fig_cos": json.loads(fig_cos.to_json()),
        "fig_score": json.loads(fig_score.to_json()),
        "meta": {
            "step_description": STEP_DESC,
            "window_size": WINDOW,
            "start_day": START_DAY,
            "end_day": END_DAY,
            "cos_th": COS_TH,
        }
    })
    plotly_json_str = json.dumps(plotly_json_obj, ensure_ascii=False)

    # -----------------------------
    # (F-2) 그래프 저장용 DF
    # -----------------------------
    graph_rows = summary[["station", "last_date"]].copy()
    graph_rows["step_description"] = STEP_DESC
    graph_rows["plotly_json"] = [plotly_json_str] * len(graph_rows)  # ✅ 문자열
    graph_rows["window_size"] = WINDOW
    graph_rows["start_day"] = START_DAY
    graph_rows["end_day"] = END_DAY
    graph_rows["updated_at"] = datetime.now()

    # -----------------------------
    # (G) DB 저장 (PK(last_date) 단위 delete 후 insert)
    # -----------------------------
    last_date_global = pd.to_datetime(summary["last_date"].max()).date()

    del_check = text(f"""
        DELETE FROM {OUT_SCHEMA}.{OUT_TABLE_CHECK}
        WHERE step_description = :step_desc AND last_date = :last_date
    """)
    del_graph = text(f"""
        DELETE FROM {OUT_SCHEMA}.{OUT_TABLE_GRAPH}
        WHERE step_description = :step_desc AND last_date = :last_date
    """)

    with engine.begin() as conn:
        conn.execute(del_check, {"step_desc": STEP_DESC, "last_date": last_date_global})
        conn.execute(del_graph, {"step_desc": STEP_DESC, "last_date": last_date_global})

        summary_to_save = summary[[
            "station", "step_description", "start_day", "end_day", "last_date",
            "last_status", "last_score", "th_score", "last_cos",
            "max_score", "max_cos", "max_streak", "crit_days", "warn_days",
            "window_size", "updated_at"
        ]].copy()

        summary_to_save.to_sql(
            OUT_TABLE_CHECK,
            conn,
            schema=OUT_SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000,
        )

        graph_to_save = graph_rows[[
            "station", "step_description", "start_day", "end_day", "last_date",
            "plotly_json",
            "window_size", "updated_at"
        ]].copy()

        graph_to_save.to_sql(
            OUT_TABLE_GRAPH,
            conn,
            schema=OUT_SCHEMA,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=2000,
        )

    print(f"[OK] Saved summary -> {OUT_SCHEMA}.{OUT_TABLE_CHECK} (last_date={last_date_global})")
    print(f"[OK] Saved graphs  -> {OUT_SCHEMA}.{OUT_TABLE_GRAPH} (last_date={last_date_global})")


if __name__ == "__main__":
    main()