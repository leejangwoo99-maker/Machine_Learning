# -*- coding: utf-8 -*-
"""
e5_pd_board_check.py  (SCRIPT ONLY)

목표
- a2_fct_table.fct_table 에서 (station, end_day, value) 조회
- step_description 조건(고정 STEP_DESC)으로 필터
- 일자 평균 시계열 -> WINDOW(예:5) 슬라이딩 feature -> 정상/열화 패턴 대비 score/cos/dist 산출
- robust threshold(MAD) + status + 연속경보(streak)
- e4_predictive_maintenance.pd_board_check 에
  1) 요약(스칼라)
  2) 그래프 재생성용 시계열(JSONB)
  를 (station,end_day) PK로 UPSERT 저장

전제
- e4_predictive_maintenance.predictive_maintenance 테이블에
  station='FCT2', step_description=STEP_DESC, pattern_name = 'pd_board_normal_ref' / 'pd_board_degradation_ref'
  가 존재해야 함.
"""

import json
import urllib.parse
from datetime import datetime, timezone

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


# =========================
# 0) DB 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"

TARGET_SCHEMA = "e4_predictive_maintenance"
TARGET_TABLE = "pd_board_check"

PATTERN_TABLE = "predictive_maintenance"
NORMAL_PATTERN_NAME = "pd_board_normal_ref"
ABN_PATTERN_NAME = "pd_board_degradation_ref"


# =========================
# 1) 실행 파라미터
# =========================
STEP_DESC = "1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol"
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]

START_DAY = "20250103"  # YYYYMMDD
END_DAY = "20260110"    # YYYYMMDD

WINDOW = 5
COS_TH = 0.70

K_MAD = 4.0
MIN_SAMPLES_FOR_ROBUST = 8


# =========================
# 2) 공용 함수
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


def groupby_apply_safe(df: pd.DataFrame, group_col: str, fn):
    """
    pandas 버전에 따라 include_groups 파라미터 지원 여부가 달라서 안전하게 처리.
    - 최신 pandas: include_groups=False로 FutureWarning 회피
    - 구버전 pandas: include_groups 없이 실행
    """
    g = df.groupby(group_col, as_index=False, group_keys=False)
    try:
        # pandas>=2.1 계열에서 지원
        return g.apply(fn, include_groups=False).reset_index(drop=True)
    except TypeError:
        # 구버전 pandas
        return g.apply(fn).reset_index(drop=True)


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
    with engine.begin() as conn:
        row = conn.execute(q, {"station": station, "step_desc": step_desc, "pattern_name": pattern_name}).fetchone()
    if row is None:
        raise ValueError(f"[ERROR] Pattern not found: station={station}, pattern={pattern_name}")

    g = row[0]
    if isinstance(g, str):
        g = json.loads(g)
    return g


# =========================
# 4) 원천 데이터 조회 → 일자 평균
# =========================
def load_avg_df(engine, stations, start_day, end_day, step_desc):
    # fct_table 컬럼 타입이 text이므로 trim(end_day) BETWEEN이 동작하되,
    # 혹시 '-'가 섞여도 대비해서 숫자만 남기는 정규화 컬럼(end_day_norm)을 만든다.
    sql = text(f"""
        SELECT
            trim(end_day)   AS end_day_yyyymmdd,
            trim(station)   AS station,
            value
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE trim(station) = ANY(:stations)
          AND trim(end_day) BETWEEN :start_day AND :end_day
          AND step_description = :step_desc
        ORDER BY station, end_day_yyyymmdd
    """)

    with engine.begin() as conn:
        raw = pd.read_sql(sql, conn, params={
            "stations": stations,
            "start_day": start_day,
            "end_day": end_day,
            "step_desc": step_desc
        })

    raw.columns = [str(c).strip() for c in raw.columns]
    print("[DEBUG] load_avg_df raw.columns =", raw.columns.tolist())
    print("[DEBUG] load_avg_df raw.rows    =", len(raw))

    if raw.empty:
        raise ValueError("[ERROR] raw is empty. (station/date/step_description 조건 불일치)")

    if "station" not in raw.columns:
        raise KeyError(f"[ERROR] 'station' column missing from SQL result. columns={raw.columns.tolist()}")

    # value 숫자화(단위 섞임 대비)
    raw["value_num"] = pd.to_numeric(raw["value"], errors="coerce")
    if raw["value_num"].notna().sum() == 0:
        raw["value_num"] = raw["value"].astype(str).str.extract(r"(-?\d+(?:\.\d+)?)", expand=False)
        raw["value_num"] = pd.to_numeric(raw["value_num"], errors="coerce")

    raw = raw.dropna(subset=["value_num"]).copy()
    if raw.empty:
        raise ValueError("[ERROR] value_num is empty after numeric parsing. (value 포맷 확인 필요)")

    # end_day 정규화(숫자 8자리)
    raw["end_day_norm"] = (
        raw["end_day_yyyymmdd"]
        .astype(str)
        .str.replace(r"[^0-9]", "", regex=True)
        .str.zfill(8)
    )
    raw["mmdd"] = raw["end_day_norm"].str.slice(4, 8)

    avg_df = (
        raw.groupby(["station", "mmdd"], as_index=False)
           .agg(value_avg=("value_num", "mean"),
                sample_amount=("value_num", "count"))
           .sort_values(["station", "mmdd"])
           .reset_index(drop=True)
    )
    avg_df["value_avg"] = avg_df["value_avg"].round(4)

    print("[DEBUG] avg_df.columns =", avg_df.columns.tolist())
    print("[DEBUG] avg_df.rows    =", len(avg_df))
    print("[DEBUG] avg_df.stations=", sorted(avg_df["station"].unique().tolist()) if len(avg_df) else [])

    return avg_df


# =========================
# 5) compare_df 생성
# =========================
def build_compare_df(avg_df_all: pd.DataFrame,
                     V_normal: np.ndarray,
                     A_ref: np.ndarray,
                     FEATURES: list,
                     window: int) -> pd.DataFrame:
    """
    avg_df_all(station, mmdd, value_avg, sample_amount) 기반으로 WINDOW 슬라이딩하여
    score_from_normal / cos_sim_to_ref / dist_to_ref 계산.
    """
    if avg_df_all is None or avg_df_all.empty:
        return pd.DataFrame(columns=["station", "mmdd", "score_from_normal", "cos_sim_to_ref", "dist_to_ref"])

    need_cols = {"station", "mmdd", "value_avg"}
    missing = need_cols - set(avg_df_all.columns)
    if missing:
        raise KeyError(f"[ERROR] avg_df_all missing columns: {missing}")

    st_targets = sorted(avg_df_all["station"].dropna().astype(str).unique().tolist())
    rows = []

    for st in st_targets:
        df_st = avg_df_all[avg_df_all["station"] == st].copy()
        df_st = df_st.dropna(subset=["mmdd", "value_avg"]).copy()
        df_st["mmdd"] = df_st["mmdd"].astype(str).str.zfill(4)
        df_st = df_st.sort_values("mmdd").reset_index(drop=True)

        if len(df_st) < window:
            continue

        for i in range(len(df_st) - window + 1):
            chunk = df_st.iloc[i:i + window]
            mmdd_end = chunk["mmdd"].iloc[-1]

            v = window_vector_from_values(chunk["value_avg"].values)
            V_t = np.array([v[k] for k in FEATURES], dtype=float)

            # 정상 기준 대비 anomaly vector
            A_t = V_t - V_normal

            rows.append({
                "station": st,
                "mmdd": mmdd_end,
                "score_from_normal": float(np.linalg.norm(A_t)),
                "cos_sim_to_ref": cosine_sim(A_t, A_ref),
                "dist_to_ref": float(np.linalg.norm(A_t - A_ref)),
            })

    if not rows:
        return pd.DataFrame(columns=["station", "mmdd", "score_from_normal", "cos_sim_to_ref", "dist_to_ref"])

    return (
        pd.DataFrame(rows)
          .sort_values(["station", "mmdd"])
          .reset_index(drop=True)
    )


# =========================
# 6) dfi + summary 생성 (station 컬럼 보장)
# =========================
def build_dfi_and_summary(compare_df: pd.DataFrame, start_day: str, end_day: str):
    if compare_df is None or compare_df.empty:
        empty_dfi = pd.DataFrame(columns=[
            "station", "mmdd", "window_end_day",
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

    # window_end_day (연도는 start_day의 연도로 붙임)
    y = start_day[:4]
    dfi["window_end_day"] = y + dfi["mmdd"].astype(str).str.zfill(4)

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

    # -------------------------
    # streak 계산 (★ groupby.apply 사용 금지: pandas 버전 차이로 station 컬럼 소실 방지)
    # -------------------------
    ALERT_LEVELS = {"WARNING", "CRITICAL"}

    # 1) 정렬 (station, window_end_day)
    dfi = dfi.sort_values(["station", "window_end_day"]).reset_index(drop=True)

    # 2) station별로 streak 계산해서 리스트로 누적
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
# 7) pd_board_check 테이블 생성 + UPSERT
# =========================
def ensure_pd_board_check(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

        conn.execute(text(f"""
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

        # 컬럼 보강(기존 테이블이 중간 상태일 수 있으니)
        cols = conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t
        """), {"s": TARGET_SCHEMA, "t": TARGET_TABLE}).fetchall()
        existing = {c[0] for c in cols}

        def add_col_if_missing(col_name: str, ddl: str):
            if col_name not in existing:
                conn.execute(text(f"ALTER TABLE {TARGET_SCHEMA}.{TARGET_TABLE} ADD COLUMN {ddl};"))

        add_col_if_missing("cosine_similarity", "cosine_similarity JSONB")
        add_col_if_missing("score_from_normal", "score_from_normal JSONB")
        add_col_if_missing("run_start_ts", "run_start_ts TIMESTAMPTZ")
        add_col_if_missing("run_end_ts", "run_end_ts TIMESTAMPTZ")


def upsert_pd_board_check(engine, dfi: pd.DataFrame, summary: pd.DataFrame,
                          *, start_day: str, end_day: str,
                          run_start_ts, run_end_ts):
    ensure_pd_board_check(engine)

    if summary is None or summary.empty:
        print("[WARN] summary empty -> skip upsert")
        return

    payloads = []
    for _, r in summary.iterrows():
        st = r["station"]
        dfi_st = dfi[dfi["station"] == st].copy()

        # 그래프 재생성용 JSON
        cos_json = make_ts_json(dfi_st, "window_end_day", "cos_sim_to_ref", th_value=COS_TH)
        score_json = make_ts_json(dfi_st, "window_end_day", "score_from_normal",
                                  th_value=float(r["th_score"]) if pd.notna(r["th_score"]) else None)

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

    with engine.begin() as conn:
        conn.execute(upsert_sql, payloads)

    print(f"[OK] pd_board_check UPSERT done | end_day={end_day} | rows={len(payloads)}")


# =========================
# 8) main
# =========================
def main():
    engine = get_engine()
    print("[OK] engine ready")

    run_start_ts = datetime.now(timezone.utc)

    try:
        # 1) 패턴 로드 (FCT2 기준)
        g_normal = load_pattern(engine, "FCT2", STEP_DESC, NORMAL_PATTERN_NAME)
        g_abn = load_pattern(engine, "FCT2", STEP_DESC, ABN_PATTERN_NAME)

        FEATURES = g_normal["features"]
        V_normal = np.array([g_normal["reference_pattern"][k] for k in FEATURES], dtype=float)
        A_ref = np.array([g_abn["reference_pattern"][k] for k in FEATURES], dtype=float)

        # 2) 원천 데이터 → 일자 평균
        avg_df_all = load_avg_df(engine, STATIONS, START_DAY, END_DAY, STEP_DESC)

        # 3) compare_df 생성
        compare_df = build_compare_df(avg_df_all, V_normal, A_ref, FEATURES, WINDOW)
        print("[DEBUG] compare_df.rows =", len(compare_df))
        if not compare_df.empty:
            print("[DEBUG] compare_df.columns =", compare_df.columns.tolist())
            print("[DEBUG] compare_df.stations =", sorted(compare_df["station"].unique().tolist()))

        # 4) dfi/summary 생성
        dfi, summary = build_dfi_and_summary(compare_df, START_DAY, END_DAY)
        print("[DEBUG] dfi.rows =", len(dfi), "| summary.rows =", len(summary))
        if not summary.empty:
            print("[DEBUG] summary.stations =", summary["station"].astype(str).tolist())

    except Exception as e:
        # 여기서 죽는 원인을 정확히 고정 출력
        print("[ERROR] pipeline failed:", repr(e))
        return

    run_end_ts = datetime.now(timezone.utc)

    # 5) 저장
    upsert_pd_board_check(
        engine, dfi, summary,
        start_day=START_DAY,
        end_day=END_DAY,
        run_start_ts=run_start_ts,
        run_end_ts=run_end_ts,
    )


if __name__ == "__main__":
    main()
