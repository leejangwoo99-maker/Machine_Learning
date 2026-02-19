# -*- coding: utf-8 -*-
"""
FCT OP-CT Boxplot Summary + UPH(병렬합산) 계산 + PostgreSQL 저장 스크립트 (MP 최대 적용)

- Source: a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history
- Output:
  1) e1_FCT_ct.fct_op_ct         (summary_df2: plotly_json 포함, html 제외)
  2) e1_FCT_ct.fct_whole_op_ct   (final_df_86: left/right/whole UPH/CTeq/final_ct)

요구사항:
- DataFrame 콘솔 출력 없음
- 진행상황만 표시
- 테이블 존재 시 PASS
- 멀티프로세스: 최대 (CPU 코어 기준 상한 적용)
"""

import sys
from pathlib import Path
import urllib.parse
from multiprocessing import cpu_count, freeze_support
from concurrent.futures import ProcessPoolExecutor, as_completed

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
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e1_FCT_ct"
TBL_OPCT      = "fct_op_ct"          # summary_df2 저장
TBL_WHOLE     = "fct_whole_op_ct"    # final_df_86 저장

# boxplot html 저장 폴더(현재 스크립트 기준)
OUT_DIR = Path("./fct_opct_boxplot_html")

# op_ct 필터
OPCT_MAX_SEC = 600

# =========================
# 멀티프로세스 최대 설정
# - 필요 시 MAX_WORKERS_CAP 숫자만 조정
# =========================
MAX_WORKERS_CAP = 12
MAX_WORKERS = max(1, min(cpu_count(), MAX_WORKERS_CAP))


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def get_engine(config=DB_CONFIG):
    pw = urllib.parse.quote_plus(config["password"])
    conn_str = (
        f"postgresql+psycopg2://{config['user']}:{pw}"
        f"@{config['host']}:{config['port']}/{config['dbname']}"
    )
    return create_engine(conn_str, pool_pre_ping=True)

def ensure_schema(conn, schema_name: str):
    with conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema_name)))
    conn.commit()

def table_exists(conn, schema_name: str, table_name: str) -> bool:
    q = """
    SELECT EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name   = %s
    )
    """
    with conn.cursor() as cur:
        cur.execute(q, (schema_name, table_name))
        return bool(cur.fetchone()[0])

def save_df_if_table_not_exists(df: pd.DataFrame, schema_name: str, table_name: str, engine):
    """
    테이블이 이미 존재하면 PASS
    없으면 테이블 생성 후 전체 저장
    """
    if df is None or len(df) == 0:
        log(f"[SKIP] {schema_name}.{table_name}: df가 비어있습니다.")
        return

    df_to_save = df.copy()

    # month는 문자열 고정
    if "month" in df_to_save.columns:
        df_to_save["month"] = df_to_save["month"].astype(str)

    # Categorical 안전 처리
    for c in df_to_save.columns:
        if isinstance(df_to_save[c].dtype, CategoricalDtype):
            df_to_save[c] = df_to_save[c].astype(str)

    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_schema(conn, schema_name)

        if table_exists(conn, schema_name, table_name):
            log(f"[PASS] {schema_name}.{table_name} 이미 존재 -> 저장 생략")
            return

    log(f"[SAVE] {schema_name}.{table_name} to_sql 시작 (rows={len(df_to_save)})")
    df_to_save.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists="fail",
        index=False,
        method="multi",
        chunksize=5000
    )
    log(f"[OK] {schema_name}.{table_name} 생성 및 저장 완료 (rows={len(df_to_save)})")


# =========================
# 2) 소스 로딩 + op_ct 계산
# =========================
def load_source_df(engine) -> pd.DataFrame:
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
    ORDER BY end_day ASC, end_time ASC
    """
    log("[1/6] DB에서 원본 데이터 로딩 시작...")
    df = pd.read_sql(text(sql_query), engine)
    log(f"[OK] 원본 로딩 완료 (rows={len(df)})")

    log("[2/6] 타입 정리 및 end_ts 생성...")
    df["end_day"] = pd.to_datetime(df["end_day"]).dt.date
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
# 3) Boxplot 요약 + html 저장 (멀티프로세스)
# =========================
def _range_str(values: pd.Series):
    values = values.dropna()
    if len(values) == 0:
        return None
    vmin = float(values.min())
    vmax = float(values.max())
    return f"{vmin:.1f}~{vmax:.1f}"

def _summarize_group_worker(args):
    """
    ProcessPoolExecutor용 워커 (pickle-safe)
    args = (station, remark, month, op_ct_values_list, out_dir_str)
    """
    station, remark, month, op_ct_list, out_dir_str = args
    out_dir = Path(out_dir_str)
    out_dir.mkdir(parents=True, exist_ok=True)

    s = pd.Series(op_ct_list).dropna()
    s = s[s <= OPCT_MAX_SEC]

    sample_amount = int(len(s))

    if sample_amount == 0:
        return {
            "station": station,
            "remark": remark,
            "month": str(month),
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

    # html boxplot 저장
    fig = px.box(
        pd.DataFrame({"op_ct": s}),
        y="op_ct",
        points="outliers",
        title=None
    )

    html_name = f"boxplot_{station}_{remark}_{month}.html"
    html_path = out_dir / html_name
    fig.write_html(str(html_path), include_plotlyjs="cdn", full_html=True)

    return {
        "station": station,
        "remark": remark,
        "month": str(month),
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
    log(f"[3/6] (station, remark, month) 요약 생성 시작... (MP 최대={MAX_WORKERS})")

    # 그룹별 op_ct 리스트만 뽑아서 워커로 전달(피클 안정)
    group_items = []
    for (station, remark, month), g in df_raw.groupby(["station", "remark", "month"], sort=True):
        op_ct_list = g["op_ct"].dropna().tolist()
        group_items.append((station, remark, month, op_ct_list, str(OUT_DIR)))

    total = len(group_items)
    log(f"[INFO] 그룹 수 = {total}")

    summary_rows = []
    done = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_summarize_group_worker, item) for item in group_items]
        for fut in as_completed(futures):
            res = fut.result()
            summary_rows.append(res)
            done += 1
            if done == 1 or done == total or done % 20 == 0:
                log(f"[PROGRESS] summary {done}/{total} ...")

    summary_df = pd.DataFrame(summary_rows)

    # id 컬럼 맨 앞
    summary_df = summary_df.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    summary_df.insert(0, "id", range(1, len(summary_df) + 1))

    log(f"[OK] 요약 DF 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 4) plotly_json 컬럼 생성 (html 컬럼 제거) (멀티프로세스)
# =========================
def _make_boxplot_json_worker(args):
    """
    args = (idx, op_ct_values_list)
    return (idx, json_or_none)
    """
    idx, op_ct_list = args
    s = pd.Series(op_ct_list).dropna()
    s = s[s <= OPCT_MAX_SEC]
    if len(s) == 0:
        return idx, None

    fig = px.box(
        pd.DataFrame({"op_ct": s}),
        y="op_ct",
        points="outliers",
        title=None
    )

    # ⭐ 핵심 수정
    return idx, fig.to_json(validate=False)


def build_plotly_json_column(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    log(f"[4/6] plotly_json 생성 시작... (MP 최대={MAX_WORKERS})")

    out = summary_df.copy()
    out["plotly_json"] = None

    # group key -> op_ct list (한 번만 만들어서 조회 비용 감소)
    group_map = {}
    for (station, remark, month), g in df_raw.groupby(["station", "remark", "month"], sort=False):
        s = g["op_ct"].dropna()
        s = s[s <= OPCT_MAX_SEC]
        group_map[(station, remark, str(month))] = s.tolist()

    tasks = []
    for i, r in out.iterrows():
        key = (r["station"], r["remark"], str(r["month"]))
        tasks.append((i, group_map.get(key, [])))

    total = len(tasks)
    done = 0

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(_make_boxplot_json_worker, t) for t in tasks]
        for fut in as_completed(futures):
            idx, js = fut.result()
            out.at[idx, "plotly_json"] = js
            done += 1
            if done == 1 or done == total or done % 20 == 0:
                log(f"[PROGRESS] plotly_json {done}/{total} ...")

    # html 필요 없으면 제거
    if "html" in out.columns:
        out = out.drop(columns=["html"])

    log("[OK] plotly_json 생성 완료")
    return out


# =========================
# 5) UPH/CTeq 계산 (left/right/whole)
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

    whole_df = (
        left_right_df.groupby(["month", "remark"], as_index=False)["uph"]
                     .sum()
    )
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
# 6) main
# =========================
def main():
    try:
        log("=== FCT OP-CT Pipeline START ===")
        log(f"[INFO] MP max_workers = {MAX_WORKERS} (cpu={cpu_count()}, cap={MAX_WORKERS_CAP})")

        # OUT_DIR는 메인에서 생성(워커에서도 mkdir하지만, 메인도 보장)
        OUT_DIR.mkdir(parents=True, exist_ok=True)

        engine = get_engine(DB_CONFIG)

        # 1) Load
        df_raw = load_source_df(engine)

        # 2) Summary + html (MP)
        summary_df = build_summary_df(df_raw)

        # 3) plotly_json + drop html (MP)
        summary_df2 = build_plotly_json_column(df_raw, summary_df)

        # 4) left/right/whole
        final_df_86 = build_final_df_86(summary_df2)

        # 5) Save to DB (PASS if exists)
        log("[6/6] DB 저장 단계 시작...")
        save_df_if_table_not_exists(summary_df2, TARGET_SCHEMA, TBL_OPCT, engine)
        save_df_if_table_not_exists(final_df_86, TARGET_SCHEMA, TBL_WHOLE, engine)

        log("=== FCT OP-CT Pipeline DONE ===")

    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    freeze_support()
    exit_code = 0

    try:
        main()

    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        exit_code = 1

    finally:
        # EXE(Nuitka/pyinstaller) 실행 시에만 콘솔 유지
        if getattr(sys, "frozen", False):
            print("\n[INFO] 프로그램이 종료되었습니다.")
            input("Press Enter to exit...")

    sys.exit(exit_code)


