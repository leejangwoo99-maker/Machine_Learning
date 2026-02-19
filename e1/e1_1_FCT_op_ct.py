# -*- coding: utf-8 -*-
"""
FCT OP-CT Boxplot Summary + UPH(병렬합산) 계산 + PostgreSQL 저장 스크립트

- Source: a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history
- Output (스키마: "e1_FCT_ct" - 대문자 유지)

  A) 최신 갱신(UPSERT)
    1) "e1_FCT_ct".fct_op_ct           (summary_df2: plotly_json 포함, html 제외)
       - 키: (station, remark, month)
    2) "e1_FCT_ct".fct_whole_op_ct     (final_df_86)
       - 키: (station, remark, month)

  B) 누적 적재(APPEND + 중복방지)
    3) "e1_FCT_ct".fct_op_ct_hist       (스냅샷 누적)
       - 키: (snapshot_day, station, remark, month)
    4) "e1_FCT_ct".fct_whole_op_ct_hist (스냅샷 누적)
       - 키: (snapshot_day, station, remark, month)

요구사항 유지:
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 실행 시작/종료 시각 및 총 소요 시간 출력
- boxplot html 저장 유지
- plotly_json 생성 유지
- "오늘(윈도우 현재 날짜) 데이터만" 처리

[운영 안정성]
- 기존 테이블이 이미 존재하는 경우에도:
  - created_at / updated_at 컬럼이 없으면 ADD COLUMN
  - ON CONFLICT용 UNIQUE INDEX가 없으면 생성
"""

import sys
import time
from datetime import datetime, date
from pathlib import Path
import urllib.parse

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text

import plotly.express as px
import psycopg2
from psycopg2 import sql
from pandas.api.types import CategoricalDtype


# =========================
# 0) 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE = "fct_vision_testlog_txt_processing_history"

# 스키마: 대문자 유지(따옴표 필요) -> psycopg2 sql.Identifier로 안전 처리
TARGET_SCHEMA = "e1_FCT_ct"

# 최신 갱신 테이블
TBL_OPCT_LATEST = "fct_op_ct"
TBL_WHOLE_LATEST = "fct_whole_op_ct"

# 누적 적재 테이블
TBL_OPCT_HIST = "fct_op_ct_hist"
TBL_WHOLE_HIST = "fct_whole_op_ct_hist"

# boxplot html 저장 폴더
OUT_DIR = Path("./fct_opct_boxplot_html")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# op_ct 필터
OPCT_MAX_SEC = 600


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)


def today_yyyymmdd() -> str:
    """윈도우(로컬) 오늘 날짜 YYYYMMDD"""
    return date.today().strftime("%Y%m%d")


def get_engine(config=DB_CONFIG):
    pw = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{pw}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )
    return create_engine(conn_str, pool_pre_ping=True)


def ensure_schema(conn, schema_name: str):
    """대문자 스키마 보존: Identifier 사용"""
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    conn.commit()


# =========================
# 2) 테이블 DDL / 마이그레이션 보정
# =========================
def ensure_target_tables():
    """
    - 스키마 생성
    - 테이블 생성(없으면)
    - 기존 테이블이 있어도 created_at/updated_at 없으면 ADD COLUMN
    - ON CONFLICT 위해 UNIQUE INDEX 보장
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_schema(conn, TARGET_SCHEMA)

        with conn.cursor() as cur:
            # -------------------------
            # LATEST: fct_op_ct
            # -------------------------
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    station TEXT NOT NULL,
                    remark  TEXT NOT NULL,
                    month   TEXT NOT NULL,
                    sample_amount INT,
                    op_ct_lower_outlier TEXT,
                    q1 NUMERIC(12,2),
                    median NUMERIC(12,2),
                    q3 NUMERIC(12,2),
                    op_ct_upper_outlier TEXT,
                    del_out_op_ct_av NUMERIC(12,2),
                    plotly_json TEXT
                );
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

            cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();""")
                        .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))
            cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();""")
                        .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

            cur.execute(sql.SQL("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_op_ct_key
                ON {}.{} (station, remark, month);
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST)))

            # -------------------------
            # LATEST: fct_whole_op_ct
            # -------------------------
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    station TEXT NOT NULL,
                    remark  TEXT NOT NULL,
                    month   TEXT NOT NULL,
                    ct_eq NUMERIC(12,2),
                    uph   NUMERIC(12,2),
                    final_ct NUMERIC(12,2)
                );
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

            cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();""")
                        .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))
            cur.execute(sql.SQL("""ALTER TABLE {}.{} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();""")
                        .format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

            cur.execute(sql.SQL("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_whole_op_ct_key
                ON {}.{} (station, remark, month);
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST)))

            # -------------------------
            # HIST: fct_op_ct_hist
            # -------------------------
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    snapshot_day TEXT NOT NULL,
                    snapshot_ts  TIMESTAMP DEFAULT now(),
                    station TEXT NOT NULL,
                    remark  TEXT NOT NULL,
                    month   TEXT NOT NULL,
                    sample_amount INT,
                    op_ct_lower_outlier TEXT,
                    q1 NUMERIC(12,2),
                    median NUMERIC(12,2),
                    q3 NUMERIC(12,2),
                    op_ct_upper_outlier TEXT,
                    del_out_op_ct_av NUMERIC(12,2),
                    plotly_json TEXT
                );
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST)))

            cur.execute(sql.SQL("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_op_ct_hist_key
                ON {}.{} (snapshot_day, station, remark, month);
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST)))

            # -------------------------
            # HIST: fct_whole_op_ct_hist
            # -------------------------
            cur.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {}.{} (
                    snapshot_day TEXT NOT NULL,
                    snapshot_ts  TIMESTAMP DEFAULT now(),
                    station TEXT NOT NULL,
                    remark  TEXT NOT NULL,
                    month   TEXT NOT NULL,
                    ct_eq NUMERIC(12,2),
                    uph   NUMERIC(12,2),
                    final_ct NUMERIC(12,2)
                );
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST)))

            cur.execute(sql.SQL("""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_fct_whole_op_ct_hist_key
                ON {}.{} (snapshot_day, station, remark, month);
            """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST)))

        conn.commit()

    log(f'[OK] target tables ensured/migrated in schema "{TARGET_SCHEMA}"')


# =========================
# 3) 저장 로직 (A=UPSERT, B=APPEND)
# =========================
def _cast_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    """DB 저장 전 dtype 안전화"""
    if df is None or len(df) == 0:
        return df
    out = df.copy()

    if "month" in out.columns:
        out["month"] = out["month"].astype(str)

    for c in out.columns:
        if isinstance(out[c].dtype, CategoricalDtype):
            out[c] = out[c].astype(str)

    return out


def upsert_latest_opct(df: pd.DataFrame):
    """A) 최신 갱신: fct_op_ct (UPSERT)"""
    if df is None or len(df) == 0:
        log("[SKIP] upsert_latest_opct: df empty")
        return

    df = _cast_df_for_db(df)

    cols = [
        "station", "remark", "month",
        "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
        "op_ct_upper_outlier", "del_out_op_ct_av", "plotly_json"
    ]
    df_load = df[cols].copy()

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            station, remark, month,
            sample_amount, op_ct_lower_outlier, q1, median, q3,
            op_ct_upper_outlier, del_out_op_ct_av, plotly_json,
            created_at, updated_at
        )
        VALUES (
            %(station)s, %(remark)s, %(month)s,
            %(sample_amount)s, %(op_ct_lower_outlier)s, %(q1)s, %(median)s, %(q3)s,
            %(op_ct_upper_outlier)s, %(del_out_op_ct_av)s, %(plotly_json)s,
            now(), now()
        )
        ON CONFLICT (station, remark, month)
        DO UPDATE SET
            sample_amount       = EXCLUDED.sample_amount,
            op_ct_lower_outlier = EXCLUDED.op_ct_lower_outlier,
            q1                  = EXCLUDED.q1,
            median              = EXCLUDED.median,
            q3                  = EXCLUDED.q3,
            op_ct_upper_outlier = EXCLUDED.op_ct_upper_outlier,
            del_out_op_ct_av    = EXCLUDED.del_out_op_ct_av,
            plotly_json         = EXCLUDED.plotly_json,
            updated_at          = now();
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_LATEST))

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()

    log(f'[OK] LATEST UPSERT -> "{TARGET_SCHEMA}".{TBL_OPCT_LATEST} (rows={len(df_load)})')


def upsert_latest_whole(df: pd.DataFrame):
    """A) 최신 갱신: fct_whole_op_ct (UPSERT)"""
    if df is None or len(df) == 0:
        log("[SKIP] upsert_latest_whole: df empty")
        return

    df = _cast_df_for_db(df)

    cols = ["station", "remark", "month", "ct_eq", "uph", "final_ct"]
    df_load = df[cols].copy()

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            station, remark, month,
            ct_eq, uph, final_ct,
            created_at, updated_at
        )
        VALUES (
            %(station)s, %(remark)s, %(month)s,
            %(ct_eq)s, %(uph)s, %(final_ct)s,
            now(), now()
        )
        ON CONFLICT (station, remark, month)
        DO UPDATE SET
            ct_eq      = EXCLUDED.ct_eq,
            uph        = EXCLUDED.uph,
            final_ct   = EXCLUDED.final_ct,
            updated_at = now();
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_LATEST))

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()

    log(f'[OK] LATEST UPSERT -> "{TARGET_SCHEMA}".{TBL_WHOLE_LATEST} (rows={len(df_load)})')


def append_hist_opct(df: pd.DataFrame, snapshot_day: str):
    """B) 누적 적재: fct_op_ct_hist (append + DO NOTHING)"""
    if df is None or len(df) == 0:
        log("[SKIP] append_hist_opct: df empty")
        return

    df = _cast_df_for_db(df)

    cols = [
        "station", "remark", "month",
        "sample_amount", "op_ct_lower_outlier", "q1", "median", "q3",
        "op_ct_upper_outlier", "del_out_op_ct_av", "plotly_json"
    ]
    df_load = df[cols].copy()
    df_load.insert(0, "snapshot_day", snapshot_day)

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            snapshot_day,
            station, remark, month,
            sample_amount, op_ct_lower_outlier, q1, median, q3,
            op_ct_upper_outlier, del_out_op_ct_av, plotly_json
        )
        VALUES (
            %(snapshot_day)s,
            %(station)s, %(remark)s, %(month)s,
            %(sample_amount)s, %(op_ct_lower_outlier)s, %(q1)s, %(median)s, %(q3)s,
            %(op_ct_upper_outlier)s, %(del_out_op_ct_av)s, %(plotly_json)s
        )
        ON CONFLICT (snapshot_day, station, remark, month)
        DO NOTHING;
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_OPCT_HIST))

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()

    log(f'[OK] HIST APPEND -> "{TARGET_SCHEMA}".{TBL_OPCT_HIST} (rows_try={len(df_load)})')


def append_hist_whole(df: pd.DataFrame, snapshot_day: str):
    """B) 누적 적재: fct_whole_op_ct_hist (append + DO NOTHING)"""
    if df is None or len(df) == 0:
        log("[SKIP] append_hist_whole: df empty")
        return

    df = _cast_df_for_db(df)

    cols = ["station", "remark", "month", "ct_eq", "uph", "final_ct"]
    df_load = df[cols].copy()
    df_load.insert(0, "snapshot_day", snapshot_day)

    stmt = sql.SQL("""
        INSERT INTO {}.{} (
            snapshot_day,
            station, remark, month,
            ct_eq, uph, final_ct
        )
        VALUES (
            %(snapshot_day)s,
            %(station)s, %(remark)s, %(month)s,
            %(ct_eq)s, %(uph)s, %(final_ct)s
        )
        ON CONFLICT (snapshot_day, station, remark, month)
        DO NOTHING;
    """).format(sql.Identifier(TARGET_SCHEMA), sql.Identifier(TBL_WHOLE_HIST))

    payload = df_load.to_dict(orient="records")
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt.as_string(conn), payload)
        conn.commit()

    log(f'[OK] HIST APPEND -> "{TARGET_SCHEMA}".{TBL_WHOLE_HIST} (rows_try={len(df_load)})')


# =========================
# 4) 소스 로딩 + op_ct 계산 (오늘만)
# =========================
def load_source_df(engine) -> pd.DataFrame:
    today_key = today_yyyymmdd()

    sql_query = f"""
    SELECT
        station,
        remark,
        end_day,
        end_time,
        result,
        goodorbad
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE
        station IN ('FCT1','FCT2','FCT3','FCT4')
        AND remark IN ('PD','Non-PD')
        AND result <> 'FAIL'
        AND goodorbad <> 'BadFile'
        AND (
            CASE
                WHEN end_day::text ~ '^\\d{{8}}$' THEN end_day::text
                WHEN end_day::text ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN replace(end_day::text, '-', '')
                ELSE replace(end_day::text, '-', '')
            END
        ) = :today_key
    ORDER BY end_day ASC, end_time ASC
    """

    log("[1/6] DB에서 원본 데이터 로딩 시작... (오늘 데이터만)")
    df = pd.read_sql(text(sql_query), engine, params={"today_key": today_key})
    log(f"[OK] 원본 로딩 완료 (rows={len(df)})")

    log("[2/6] 타입 정리 및 end_ts 생성...")
    df["end_day"] = pd.to_datetime(df["end_day"], errors="coerce").dt.date
    df["end_time_str"] = df["end_time"].astype(str)
    df["end_ts"] = pd.to_datetime(
        df["end_day"].astype(str) + " " + df["end_time_str"],
        errors="coerce"
    )

    df = df.sort_values(["station", "remark", "end_day", "end_ts"], ascending=True).reset_index(drop=True)
    df["op_ct"] = df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    df["month"] = pd.to_datetime(df["end_day"].astype(str)).dt.strftime("%Y%m")

    n_bad_ts = int(df["end_ts"].isna().sum())
    if n_bad_ts > 0:
        log(f"[WARN] end_ts 파싱 실패 행 {n_bad_ts}개 (end_time 형식 확인 필요)")

    log("[OK] op_ct / month 생성 완료")
    return df


# =========================
# 5) Boxplot 요약 + html 저장
# =========================
def _range_str(values: pd.Series):
    values = values.dropna()
    if len(values) == 0:
        return None
    vmin = float(values.min())
    vmax = float(values.max())
    return f"{vmin:.1f}~{vmax:.1f}"


def summarize_group(g: pd.DataFrame):
    s = g["op_ct"].dropna()
    s = s[s <= OPCT_MAX_SEC]

    sample_amount = len(s)
    if sample_amount == 0:
        return {
            "sample_amount": 0,
            "op_ct_lower_outlier": None,
            "q1": None,
            "median": None,
            "q3": None,
            "op_ct_upper_outlier": None,
            "del_out_op_ct_av": None,
            "html": None,
        }

    q1 = float(s.quantile(0.25))
    med = float(s.quantile(0.50))
    q3 = float(s.quantile(0.75))
    iqr = q3 - q1

    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    lower_outliers = s[s < lower_bound]
    upper_outliers = s[s > upper_bound]

    s_wo = s[(s >= lower_bound) & (s <= upper_bound)]
    avg_wo = float(s_wo.mean()) if len(s_wo) else None

    station = g["station"].iloc[0]
    remark = g["remark"].iloc[0]
    month = g["month"].iloc[0]

    fig = px.box(pd.DataFrame({"op_ct": s}), y="op_ct", points="outliers", title=None)

    html_name = f"boxplot_{station}_{remark}_{month}.html"
    html_path = OUT_DIR / html_name
    fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)

    return {
        "sample_amount": sample_amount,
        "op_ct_lower_outlier": _range_str(lower_outliers),
        "q1": round(q1, 2),
        "median": round(med, 2),
        "q3": round(q3, 2),
        "op_ct_upper_outlier": _range_str(upper_outliers),
        "del_out_op_ct_av": round(avg_wo, 2) if avg_wo is not None else None,
        "html": str(html_path),
    }


def build_summary_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    log("[3/6] (station, remark, month) 요약 생성 시작...")
    groups = list(df_raw.groupby(["station", "remark", "month"], sort=True))
    log(f"[INFO] 그룹 수 = {len(groups)}")

    summary_rows = []
    for idx, ((station, remark, month), g) in enumerate(groups, start=1):
        if idx % 20 == 0 or idx == 1 or idx == len(groups):
            log(f"[PROGRESS] summary {idx}/{len(groups)} ... ({station}, {remark}, {month})")

        stats = summarize_group(g)
        summary_rows.append({"station": station, "remark": remark, "month": month, **stats})

    summary_df = pd.DataFrame(summary_rows)

    # 표시용 id 유지(기존 기능)
    summary_df.insert(0, "id", range(1, len(summary_df) + 1))
    summary_df = summary_df.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    summary_df["id"] = range(1, len(summary_df) + 1)

    log(f"[OK] 요약 DF 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 6) plotly_json 컬럼 생성 (html 컬럼 제거)
# =========================
def make_boxplot_json(op_ct_series: pd.Series) -> str:
    fig = px.box(pd.DataFrame({"op_ct": op_ct_series}), y="op_ct", points="outliers", title=None)
    return fig.to_json()


def build_plotly_json_column(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    log("[4/6] plotly_json 생성 시작... (그룹별 boxplot figure JSON)")
    out = summary_df.copy()
    out["plotly_json"] = None

    total = len(out)
    for i, r in out.iterrows():
        if (i + 1) % 20 == 0 or i == 0 or (i + 1) == total:
            log(f"[PROGRESS] plotly_json {i+1}/{total} ...")

        station = r["station"]
        remark = r["remark"]
        month = r["month"]

        g = df_raw[
            (df_raw["station"] == station) &
            (df_raw["remark"] == remark) &
            (df_raw["month"] == month)
        ]["op_ct"].dropna()

        g = g[g <= OPCT_MAX_SEC]
        out.at[i, "plotly_json"] = make_boxplot_json(g) if len(g) else None

    if "html" in out.columns:
        out = out.drop(columns=["html"])

    log("[OK] plotly_json 생성 완료")
    return out


# =========================
# 7) UPH/CTeq 계산 (left/right/whole)
# =========================
def parallel_uph(ct_series: pd.Series) -> float:
    ct = ct_series.dropna()
    ct = ct[ct > 0]
    if len(ct) == 0:
        return np.nan
    return 3600.0 * (1.0 / ct).sum()


def build_final_df_86(summary_df2: pd.DataFrame) -> pd.DataFrame:
    log("[5/6] left/right/whole UPH 계산 시작...")

    need = {"station", "remark", "month", "del_out_op_ct_av"}
    missing = need - set(summary_df2.columns)
    if missing:
        raise KeyError(f"필요 컬럼 누락: {sorted(missing)}")

    b = summary_df2[summary_df2["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()
    b["del_out_op_ct_av"] = pd.to_numeric(b["del_out_op_ct_av"], errors="coerce")

    side_map = {"FCT1": "left", "FCT2": "left", "FCT3": "right", "FCT4": "right"}
    b["side"] = b["station"].map(side_map)

    grp_side = (
        b.groupby(["month", "remark", "side"], as_index=False)
         .agg(ct_list=("del_out_op_ct_av", lambda x: list(x)))
    )

    grp_side["uph"] = grp_side["ct_list"].apply(lambda lst: parallel_uph(pd.Series(lst)))
    grp_side["ct_eq"] = np.where(grp_side["uph"] > 0, 3600.0 / grp_side["uph"], np.nan)

    grp_side["uph"] = grp_side["uph"].round(2)
    grp_side["ct_eq"] = grp_side["ct_eq"].round(2)

    left_right_df = grp_side.rename(columns={"side": "station"})[
        ["station", "remark", "month", "ct_eq", "uph"]
    ].copy()
    left_right_df["final_ct"] = np.nan

    whole_df = left_right_df.groupby(["month", "remark"], as_index=False)["uph"].sum()
    whole_df["station"] = "whole"
    whole_df["ct_eq"] = np.nan
    whole_df["final_ct"] = np.where(
        whole_df["uph"] > 0,
        (3600.0 / whole_df["uph"]).round(2),
        np.nan
    )

    final_df_86 = pd.concat(
        [
            left_right_df[["station", "remark", "month", "ct_eq", "uph", "final_ct"]],
            whole_df[["station", "remark", "month", "ct_eq", "uph", "final_ct"]],
        ],
        ignore_index=True
    )

    station_order = pd.CategoricalDtype(["left", "right", "whole"], ordered=True)
    final_df_86["station"] = final_df_86["station"].astype(station_order)

    final_df_86 = final_df_86.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    final_df_86.insert(0, "id", range(1, len(final_df_86) + 1))

    log(f"[OK] final_df_86 생성 완료 (rows={len(final_df_86)})")
    return final_df_86


# =========================
# 8) main
# =========================
def main():
    start_dt = datetime.now()
    start_ts = time.perf_counter()
    snap_day = today_yyyymmdd()

    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== FCT OP-CT Pipeline START ===")

    try:
        engine = get_engine(DB_CONFIG)

        log("[0/6] target table ensure...")
        ensure_target_tables()

        df_raw = load_source_df(engine)
        summary_df = build_summary_df(df_raw)
        summary_df2 = build_plotly_json_column(df_raw, summary_df)
        final_df_86 = build_final_df_86(summary_df2)

        log("[6/6] DB 저장 단계 시작... (A=UPSERT latest, B=APPEND history)")
        upsert_latest_opct(summary_df2)
        upsert_latest_whole(final_df_86)
        append_hist_opct(summary_df2, snapshot_day=snap_day)
        append_hist_whole(final_df_86, snapshot_day=snap_day)

        log("=== FCT OP-CT Pipeline DONE ===")

    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)

    finally:
        elapsed = time.perf_counter() - start_ts
        end_dt = datetime.now()
        log(f"[END]   {end_dt:%Y-%m-%d %H:%M:%S}")
        log(f"[TIME]  total_elapsed = {elapsed:.2f} sec ({elapsed/60:.2f} min)")


if __name__ == "__main__":
    main()
