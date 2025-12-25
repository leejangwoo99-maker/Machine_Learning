# -*- coding: utf-8 -*-
"""
non_pd_worst.py

추가 반영
- 실행 시작/종료 시간 기록
- 총 실행시간(초) 계산
- DB 저장 테이블에 run_start_ts, run_end_ts, run_seconds 컬럼 추가 및 UPSERT 저장
"""

import os
import time
import urllib.parse
from typing import Optional, Dict, Any, List
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text


# ============================
# 0) 설정
# ============================

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

TARGET_END_DAY = "20251219"
TARGET_REMARK  = "Non-PD"
TEST_CONTENTS_KEY = "1.32_dark_curr_check"

TARGET_SCHEMA = "e4_predictive_maintenance"
TARGET_TABLE  = "non_pd_worst"

SAVE_HTML_REPORT = True
REPORT_DIR = "./report_non_pd_worst"
MAX_PREVIEW_ROWS = 300


# ============================
# 1) DB 연결
# ============================

def get_engine(cfg: Dict[str, Any]):
    user = cfg["user"]
    pw = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    db = cfg["dbname"]
    return create_engine(
        f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}?connect_timeout=5",
        pool_pre_ping=True
    )


# ============================
# 2) 유틸
# ============================

def _reorder_run_time_after_end_time(df: pd.DataFrame,
                                    run_time_col: str = "run_time",
                                    after_col: str = "end_time") -> pd.DataFrame:
    if df is None or len(df) == 0:
        return df
    cols = list(df.columns)
    if run_time_col not in cols:
        return df
    if after_col not in cols:
        cols.remove(run_time_col)
        cols.insert(0, run_time_col)
        return df[cols]
    cols.remove(run_time_col)
    idx = cols.index(after_col) + 1
    cols.insert(idx, run_time_col)
    return df[cols]


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def df_to_html(df: pd.DataFrame, title: str, out_path: str, n: int = 300):
    view = df.head(n).copy()
    pd.set_option("display.max_columns", None)
    sty = view.style

    html_table = sty.to_html()
    html = f"""
    <html>
    <head>
      <meta charset="utf-8"/>
      <title>{title}</title>
    </head>
    <body>
      <h3>{title}</h3>
      <div style="border:1px solid #ddd; padding:8px;">
        <div style="overflow:auto; width:100%; max-height:720px;">
          {html_table}
        </div>
      </div>
    </body>
    </html>
    """
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)


def norm_end_day(x) -> str:
    s = str(x).strip()
    if s.isdigit() and len(s) == 8:
        return pd.to_datetime(s, format="%Y%m%d", errors="coerce").strftime("%Y-%m-%d")
    return pd.to_datetime(s, errors="coerce").strftime("%Y-%m-%d")


def norm_end_time(x) -> Optional[str]:
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s.replace(".", "", 1).isdigit():
        try:
            s = str(int(float(s)))
        except Exception:
            pass
    if s.isdigit() and len(s) < 6:
        s = s.zfill(6)
    return s


def parse_test_time_to_ts(end_day_str: str, t) -> pd.Timestamp:
    if t is None or (isinstance(t, float) and pd.isna(t)):
        return pd.NaT
    s = str(t).strip()
    if s == "" or s.lower() == "none":
        return pd.NaT
    return pd.to_datetime(f"{end_day_str} {s}", errors="coerce")


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ============================
# 3) 메인 로직
# ============================

def main():
    # -----------------------------
    # 실행 시작/종료/시간 측정
    # -----------------------------
    run_start_dt = datetime.now()
    run_start_perf = time.perf_counter()
    print(f"[RUN] start_ts={run_start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

    engine = get_engine(DB_CONFIG)

    if SAVE_HTML_REPORT:
        ensure_dir(REPORT_DIR)

    try:
        # --------------------------------
        # Cell A) boundary_run_time 로드
        # --------------------------------
        SQL_BOUNDARY = text("""
        SELECT end_day, upper_outlier
        FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
        WHERE test_contents = :tc
          AND upper_outlier IS NOT NULL
        ORDER BY end_day DESC
        LIMIT 1
        """)

        b = pd.read_sql(SQL_BOUNDARY, engine, params={"tc": TEST_CONTENTS_KEY})
        if b.empty:
            raise RuntimeError(f"[ERROR] non_pd_cal_test_ct_summary에서 {TEST_CONTENTS_KEY} upper_outlier를 찾지 못했습니다.")

        boundary_run_time = float(b.loc[0, "upper_outlier"])
        boundary_src_day  = str(b.loc[0, "end_day"])
        print(f"[OK] boundary_run_time={boundary_run_time} (source end_day={boundary_src_day})")

        # --------------------------------
        # Cell B) TOP 5% 산출
        # --------------------------------
        SQL_DATA = text("""
        SELECT
            barcode_information,
            remark,
            station,
            end_day,
            end_time,
            run_time
        FROM a2_fct_table.fct_table
        WHERE end_day = :end_day
          AND remark  = :remark
        """)

        df = pd.read_sql(SQL_DATA, engine, params={"end_day": TARGET_END_DAY, "remark": TARGET_REMARK})
        df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

        df["boundary_run_time"] = boundary_run_time
        df = df[df["run_time"] > df["boundary_run_time"]].copy()

        df = (
            df.sort_values(["barcode_information", "end_time"])
              .groupby("barcode_information", as_index=False)
              .agg({
                  "remark": "first",
                  "station": "first",
                  "end_day": "first",
                  "end_time": "max",
                  "boundary_run_time": "first",
                  "run_time": "first"
              })
        )

        cut = df["run_time"].quantile(0.95)
        df_top = df[df["run_time"] >= cut].copy()
        df_top = df_top.sort_values("run_time", ascending=False).reset_index(drop=True)

        df_top = df_top[[
            "barcode_information",
            "remark",
            "station",
            "end_day",
            "end_time",
            "boundary_run_time",
            "run_time"
        ]]

        print(f"[OK] boundary={boundary_run_time} / TOP5% cut={cut:.2f} / rows={len(df_top)}")

        if SAVE_HTML_REPORT:
            out = os.path.join(REPORT_DIR, "B_df_top_run_time_top5.html")
            df_to_html(_reorder_run_time_after_end_time(df_top), "Cell B: df_top (run_time TOP 5%)", out, n=MAX_PREVIEW_ROWS)

        # --------------------------------
        # Cell X1) df_top 바코드 → fct_detail 조회 + meta merge
        # --------------------------------
        if len(df_top) == 0:
            raise ValueError("[ERROR] df_top이 비어있습니다. (run_time TOP5% 결과 없음)")

        barcodes = df_top["barcode_information"].dropna().astype(str).drop_duplicates().tolist()
        print(f"[OK] Top barcodes = {len(barcodes)}")

        TARGET_END_DAY_TEXT = str(df_top["end_day"].iloc[0]).strip()  # YYYYMMDD
        TARGET_END_DAY_DATE = pd.to_datetime(TARGET_END_DAY_TEXT, format="%Y%m%d", errors="raise").strftime("%Y-%m-%d")
        TARGET_REMARK2 = str(df_top["remark"].iloc[0]).strip() if "remark" in df_top.columns else "PD"
        print(f"[OK] TARGET_END_DAY_TEXT={TARGET_END_DAY_TEXT} / TARGET_END_DAY_DATE={TARGET_END_DAY_DATE} / REMARK={TARGET_REMARK2}")

        SQL_FCT_DETAIL = text("""
        SELECT
            barcode_information,
            remark,
            end_day,
            end_time,
            contents,
            test_ct,
            test_time
        FROM c1_fct_detail.fct_detail
        WHERE end_day = CAST(:end_day AS date)
          AND remark = :remark
          AND barcode_information = ANY(CAST(:barcodes AS text[]))
        """)

        df_detail = pd.read_sql(
            SQL_FCT_DETAIL,
            engine,
            params={"end_day": TARGET_END_DAY_DATE, "remark": TARGET_REMARK2, "barcodes": barcodes}
        )

        df_meta = df_top[["barcode_information", "station", "run_time", "boundary_run_time"]].copy()
        df_meta["barcode_information"] = df_meta["barcode_information"].astype(str)
        df_meta["station"] = df_meta["station"].astype(str)
        df_meta["run_time"] = pd.to_numeric(df_meta["run_time"], errors="coerce")
        df_meta["boundary_run_time"] = pd.to_numeric(df_meta["boundary_run_time"], errors="coerce")
        df_meta = df_meta.drop_duplicates("barcode_information")

        df_detail["barcode_information"] = df_detail["barcode_information"].astype(str)
        df_detail = df_detail.merge(df_meta, on="barcode_information", how="left")

        df_detail = df_detail[[
            "barcode_information",
            "station",
            "remark",
            "end_day",
            "end_time",
            "run_time",
            "boundary_run_time",
            "contents",
            "test_ct",
            "test_time"
        ]].copy()

        print(f"[OK] df_detail rows={len(df_detail)}")

        if SAVE_HTML_REPORT:
            out = os.path.join(REPORT_DIR, "X1_df_detail.html")
            df_to_html(_reorder_run_time_after_end_time(df_detail), "Cell X1: df_detail", out, n=MAX_PREVIEW_ROWS)

        # --------------------------------
        # Cell X2) group 생성 + 제외 규칙 적용
        # --------------------------------
        df2 = df_detail.copy()
        df2["barcode_information"] = df2["barcode_information"].astype(str)
        df2["station"] = df2["station"].astype(str)
        df2["remark"] = df2["remark"].astype(str)
        df2["end_day"] = pd.to_datetime(df2["end_day"], errors="coerce").dt.strftime("%Y-%m-%d")
        df2["end_time"] = df2["end_time"].astype(str)

        df2["_test_ts"] = [parse_test_time_to_ts(d, t) for d, t in zip(df2["end_day"], df2["test_time"])]

        df2["group_key"] = df2["barcode_information"] + "|" + df2["end_day"] + "|" + df2["end_time"]
        df2["group"] = pd.factorize(df2["group_key"], sort=False)[0] + 1

        skip_mask = df2["contents"].astype(str).str.contains("START :: MES 이전공정 체크 SKIP", na=False)
        skip_groups = set(df2.loc[skip_mask, "group"].unique().tolist())

        df2_sorted_tmp = df2.sort_values(["group", "_test_ts"], ascending=[True, True]).copy()

        def first_3_row(sub: pd.DataFrame):
            m = sub["contents"].astype(str).str.startswith("3~", na=False)
            if not m.any():
                return None
            return sub.loc[m].iloc[0]

        valid_groups: List[int] = []
        for g, sub in df2_sorted_tmp.groupby("group", sort=False):
            if g in skip_groups:
                continue
            r = first_3_row(sub)
            if r is None:
                continue
            if pd.isna(r["test_ct"]):
                valid_groups.append(g)

        df2 = df2[df2["group"].isin(valid_groups)].copy()

        df2["_is_first_3_null"] = (
            df2["contents"].astype(str).str.startswith("3~", na=False) &
            df2["test_ct"].isna()
        ).astype(int)

        df2 = df2.sort_values(["group", "_is_first_3_null", "_test_ts"], ascending=[True, False, True]).reset_index(drop=True)
        df2.drop(columns=["group_key"], inplace=True, errors="ignore")

        print(f"[OK] df2 rows={len(df2)} / groups={df2['group'].nunique()}")

        if SAVE_HTML_REPORT:
            out = os.path.join(REPORT_DIR, "X2_df2.html")
            df_to_html(_reorder_run_time_after_end_time(df2), "Cell X2: df2", out, n=MAX_PREVIEW_ROWS)

        # --------------------------------
        # Cell X3) from_to_test_ct 생성 (OK/NG만)
        # --------------------------------
        df3 = df2.copy()
        df3["_base_ts"] = df3.groupby("group")["_test_ts"].transform("first")

        is_okng = df3["contents"].astype(str).isin(["테스트 결과 : OK", "테스트 결과 : NG"])
        df3["from_to_test_ct"] = pd.NA

        mask = is_okng & df3["_test_ts"].notna() & df3["_base_ts"].notna()
        df3.loc[mask, "from_to_test_ct"] = (df3.loc[mask, "_test_ts"] - df3.loc[mask, "_base_ts"]).dt.total_seconds()

        print(f"[OK] from_to_test_ct filled rows={df3['from_to_test_ct'].notna().sum()}")

        # --------------------------------
        # Cell X4) okng_seq + test_contents 매핑
        # --------------------------------
        df4 = df3.copy()

        MAP_1_67 = {
            1:"0.00_d_sig_val_090_set", 2:"0.00_load_a_cc_set", 3:"0.00_dmm_c_rng_set", 4:"0.00_load_c_cc_set",
            5:"0.00_dmm_c_rng_set", 6:"0.00_dmm_regi_set", 7:"0.00_dmm_regi_ac_0.6_set",
            8:"1.00_mini_b_short_check", 9:"1.01_usb_a_short_check", 10:"1.02_usb_c_short_check",
            11:"1.03_d_sig_val_000_set", 12:"1.03_dmm_regi_set", 13:"1.03_dmm_regi_ac_0.6_set",
            14:"1.03_pin12_short_check", 15:"1.04_pin23_short_check", 16:"1.05_pin34_short_check",
            17:"1.06_dmm_dc_v_set", 18:"1.06_dmm_ac_0.6_set", 19:"1.06_dmm_c_set",
            20:"1.06_load_a_sensing_on", 21:"1.06_load_c_sensing_on",
            22:"1.06_ps_16.5v_set", 23:"1.06_ps_on", 24:"1.06_dmm_ac_0.6_set", 25:"1.06_input_16.5v",
            26:"1.07_idle_c_check", 27:"1.08_fw_ver_check", 28:"1.09_chip_id_check",
            29:"1.10_dmm_3c_rng_set", 30:"1.10_load_a_5.5c_set", 31:"1.10_load_a_on", 32:"1.10_usb_a_v_check",
            33:"1.11_usb_a_c_check", 34:"1.12_usb_c_v_check", 35:"1.13_load_a_off", 36:"1.13_load_c_5.5c_set",
            37:"1.13_load_c_on", 38:"1.13_overcurr_usb_c_v", 39:"1.14_overcurr_usb_c_c", 40:"1.15_usb_a_v",
            41:"1.16_load_c_off", 42:"1.16_dut_reset", 43:"1.16_cc_aside_check", 44:"1.17_cc_bside_check",
            45:"1.18_load_a_2.4c_set", 46:"1.18_load_c_3c_set", 47:"1.18_load_a_on", 48:"1.18_load_c_on",
            49:"1.18_usb_a_v_check", 50:"1.19_usb_a_c_check", 51:"1.20_usb_c_v_check", 52:"1.21_usb_c_c_check",
            53:"1.22_load_a_off", 54:"1.22_load_c_off", 55:"1.22_usb_c_carplay", 56:"1.23_usb_a_carplay",
            57:"1.24_usb_c_pm1", 58:"1.25_usb_c_pm2", 59:"1.26_usb_c_pm3", 60:"1.27_usb_c_pm4",
            61:"1.28_usb_a_pm1", 62:"1.29_usb_a_pm2", 63:"1.30_usb_a_pm3", 64:"1.31_usb_a_pm4",
            65:"1.32_dmm_ac_0.6_set", 66:"1.32_dmm_c_rng_set", 67:"1.32_dark_curr_check"
        }

        is_okng2 = df4["contents"].astype(str).isin(["테스트 결과 : OK", "테스트 결과 : NG"])
        df4["okng_seq"] = pd.NA
        df4.loc[is_okng2, "okng_seq"] = (
            df4.loc[is_okng2]
               .groupby("group")
               .cumcount()
               .add(1)
               .astype(int)
        )

        df4["test_contents"] = pd.NA
        df4.loc[is_okng2, "test_contents"] = df4.loc[is_okng2, "okng_seq"].map(MAP_1_67)

        df_final = df4[[
            "group","barcode_information","station","remark","end_day","end_time","run_time","boundary_run_time",
            "test_time","contents","test_contents","test_ct","from_to_test_ct","okng_seq"
        ]].copy()

        # --------------------------------
        # Cell X5) df15
        # --------------------------------
        df15 = df_final.dropna(subset=["test_contents"]).copy()
        df15 = df15[[
            "group","barcode_information","station","remark","end_day","end_time","run_time","boundary_run_time",
            "test_time","test_contents","from_to_test_ct","okng_seq"
        ]].reset_index(drop=True)

        # --------------------------------
        # Cell X6) boundary_test_ct + problem1~4 JOIN
        # --------------------------------
        TARGET_END_DAY_X6 = str(df15["end_day"].dropna().iloc[0]).strip()

        SQL_PROBLEM_COLS = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'e4_predictive_maintenance'
          AND table_name   = 'non_pd_cal_test_ct_summary'
          AND column_name IN ('problem1','problem2','problem3','problem4')
        ORDER BY column_name
        """)
        present_problem_cols = pd.read_sql(SQL_PROBLEM_COLS, engine)["column_name"].tolist()
        present_problem_cols = [c.strip() for c in present_problem_cols if isinstance(c, str)]
        want_problem_cols = ["problem1","problem2","problem3","problem4"]
        use_problem_cols  = [c for c in want_problem_cols if c in present_problem_cols]

        SQL_HAS_ENDDAY_COL = text("""
        SELECT COUNT(*) AS n
        FROM information_schema.columns
        WHERE table_schema='e4_predictive_maintenance'
          AND table_name='non_pd_cal_test_ct_summary'
          AND column_name='end_day'
        """)
        has_endday = int(pd.read_sql(SQL_HAS_ENDDAY_COL, engine).iloc[0]["n"]) > 0

        select_cols = ["test_contents", "upper_outlier"] + use_problem_cols

        if has_endday:
            SQL_BOUNDARY2 = text(f"""
            SELECT {", ".join(select_cols)}
            FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
            WHERE end_day = :end_day
            """)
            df_boundary = pd.read_sql(SQL_BOUNDARY2, engine, params={"end_day": TARGET_END_DAY_X6})
            if len(df_boundary) == 0:
                SQL_LATEST_DAY = text("""
                SELECT MAX(end_day) AS max_day
                FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
                """)
                latest = pd.read_sql(SQL_LATEST_DAY, engine).iloc[0]["max_day"]
                if pd.isna(latest):
                    raise ValueError("[ERROR] non_pd_cal_test_ct_summary 테이블이 비어있습니다.")
                latest = str(latest).strip()
                df_boundary = pd.read_sql(SQL_BOUNDARY2, engine, params={"end_day": latest})
        else:
            SQL_BOUNDARY2 = text(f"""
            SELECT {", ".join(select_cols)}
            FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
            """)
            df_boundary = pd.read_sql(SQL_BOUNDARY2, engine)

        df_boundary["test_contents"] = df_boundary["test_contents"].astype(str).str.strip()
        df_boundary["upper_outlier"] = pd.to_numeric(df_boundary["upper_outlier"], errors="coerce").round(2)
        for c in use_problem_cols:
            df_boundary[c] = df_boundary[c].astype(str).str.strip()

        df15_out = df15.copy()
        df15_out["test_contents"] = df15_out["test_contents"].astype(str).str.strip()

        join_cols = ["test_contents", "upper_outlier"] + use_problem_cols
        df15_out = df15_out.merge(df_boundary[join_cols], on="test_contents", how="left")
        df15_out = df15_out.rename(columns={"upper_outlier": "boundary_test_ct"})

        for c in want_problem_cols:
            if c not in df15_out.columns:
                df15_out[c] = pd.NA

        for c in ["run_time","boundary_run_time","from_to_test_ct","boundary_test_ct"]:
            if c in df15_out.columns:
                df15_out[c] = pd.to_numeric(df15_out[c], errors="coerce").round(2)

        # --------------------------------
        # Cell X7) diff_ct
        # --------------------------------
        df15_out["boundary_test_ct"] = pd.to_numeric(df15_out["boundary_test_ct"], errors="coerce")
        df15_out["from_to_test_ct"]  = pd.to_numeric(df15_out["from_to_test_ct"], errors="coerce")
        df15_out["diff_ct"] = df15_out["from_to_test_ct"] - df15_out["boundary_test_ct"]

        df15_out2 = df15_out[[
            "group","barcode_information","station","remark","end_day","end_time","run_time","boundary_run_time",
            "okng_seq","test_contents","test_time","from_to_test_ct",
            "boundary_test_ct","diff_ct","problem1","problem2","problem3","problem4"
        ]].copy()

        # --------------------------------
        # Cell X8) 바코드 대표행 기준 diff_ct TOP5%
        # --------------------------------
        df_spike = df15_out2.copy()
        df_spike["diff_ct"]  = pd.to_numeric(df_spike["diff_ct"], errors="coerce")
        df_spike["run_time"] = pd.to_numeric(df_spike["run_time"], errors="coerce")
        df_spike["okng_seq"] = pd.to_numeric(df_spike["okng_seq"], errors="coerce")
        df_spike = df_spike.dropna(subset=["barcode_information", "okng_seq", "diff_ct"]).copy()

        df_spike["min_okng_seq"] = df_spike.groupby("barcode_information")["okng_seq"].transform("min")
        df_minseq = df_spike[df_spike["okng_seq"] == df_spike["min_okng_seq"]].copy()

        df_rep = (
            df_minseq.sort_values(
                ["barcode_information", "diff_ct", "run_time", "end_time"],
                ascending=[True, False, False, True]
            )
            .drop_duplicates(subset=["barcode_information"], keep="first")
            .copy()
        )

        cut95 = df_rep["diff_ct"].quantile(0.95)
        df_top2 = df_rep[df_rep["diff_ct"] >= cut95].copy()
        df_top2 = df_top2.sort_values("run_time", ascending=False).reset_index(drop=True)

        print(f"[OK] diff_ct TOP5% cut={cut95:.3f} / rows={len(df_top2)}")

        # --------------------------------
        # Cell X9) file_path 매칭
        # --------------------------------
        key_df = df_top2[["barcode_information", "end_day", "end_time"]].copy()
        key_df["barcode_information"] = key_df["barcode_information"].astype(str).str.strip()
        key_df["end_day"] = key_df["end_day"].apply(norm_end_day)
        key_df["end_time"] = key_df["end_time"].apply(norm_end_time)
        key_df = key_df.dropna(subset=["barcode_information", "end_day", "end_time"]).copy()

        barcodes2 = key_df["barcode_information"].drop_duplicates().tolist()
        days2     = key_df["end_day"].drop_duplicates().tolist()

        SQL_FILEPATH = text("""
        SELECT
            barcode_information::text AS barcode_information,
            to_char(end_day, 'YYYY-MM-DD') AS end_day,
            end_time::text AS end_time,
            file_path::text AS file_path
        FROM c1_fct_detail.fct_detail
        WHERE barcode_information = ANY(CAST(:barcodes AS text[]))
          AND end_day = ANY(CAST(:days AS date[]))
          AND file_path IS NOT NULL
        """)

        df_fp = pd.read_sql(SQL_FILEPATH, engine, params={"barcodes": barcodes2, "days": days2})
        df_fp["barcode_information"] = df_fp["barcode_information"].astype(str).str.strip()
        df_fp["end_day"] = pd.to_datetime(df_fp["end_day"], errors="coerce").dt.strftime("%Y-%m-%d")
        df_fp["end_time"] = df_fp["end_time"].apply(norm_end_time)
        df_fp["file_path"] = df_fp["file_path"].astype(str)

        df_fp = (
            df_fp.dropna(subset=["barcode_information", "end_day", "end_time"])
                 .sort_values(["barcode_information", "end_day", "end_time"])
                 .drop_duplicates(["barcode_information", "end_day", "end_time"], keep="first")
                 .reset_index(drop=True)
        )

        df_top_fp = df_top2.copy()
        df_top_fp["barcode_information"] = df_top_fp["barcode_information"].astype(str).str.strip()
        df_top_fp["end_day"] = df_top_fp["end_day"].apply(norm_end_day)
        df_top_fp["end_time"] = df_top_fp["end_time"].apply(norm_end_time)

        df_top_fp = df_top_fp.merge(
            df_fp[["barcode_information", "end_day", "end_time", "file_path"]],
            on=["barcode_information", "end_day", "end_time"],
            how="left"
        )

        # --------------------------------
        # Cell X10) 저장 준비 (CREATE는 실행하되, INSERT/UPSERT는 "DB에 존재하는 컬럼만"으로 동적 생성)
        #  - 테이블이 이미 존재하면 run_* 컬럼이 없을 수 있으므로, info_schema로 컬럼 확인 후 자동 제외
        # --------------------------------
        FULL_NAME = f"{TARGET_SCHEMA}.{TARGET_TABLE}"
        df_save = df_top_fp.copy()

        # group -> group_no
        if "group" in df_save.columns and "group_no" not in df_save.columns:
            df_save["group_no"] = pd.to_numeric(df_save["group"], errors="coerce").astype("Int64")
        elif "group_no" not in df_save.columns:
            df_save["group_no"] = pd.NA

        # 실행시간 컬럼(DF에는 만들어두되, DB에 컬럼이 없으면 저장 시 자동 제외됨)
        df_save["run_start_ts"] = run_start_dt
        df_save["run_end_ts"] = pd.NaT
        df_save["run_seconds"] = pd.NA

        pk_cols = ["barcode_information", "end_day", "end_time", "test_contents", "okng_seq"]
        missing_pk = [c for c in pk_cols if c not in df_save.columns]
        if missing_pk:
            raise KeyError(f"[ERROR] PK에 필요한 컬럼이 없습니다: {missing_pk}")

        # 타입 정규화
        df_save["barcode_information"] = df_save["barcode_information"].astype(str).str.strip()
        df_save["end_day"] = pd.to_datetime(df_save["end_day"], errors="coerce").dt.date
        df_save["end_time"] = df_save["end_time"].astype(str).str.strip()
        df_save["test_contents"] = df_save["test_contents"].astype(str).str.strip()
        df_save["okng_seq"] = pd.to_numeric(df_save["okng_seq"], errors="coerce").astype("Int64")

        for c in ["run_time","boundary_run_time","from_to_test_ct","boundary_test_ct","diff_ct","run_seconds"]:
            if c in df_save.columns:
                df_save[c] = pd.to_numeric(df_save[c], errors="coerce")

        for c in ["problem1","problem2","problem3","problem4"]:
            if c not in df_save.columns:
                df_save[c] = pd.NA
        if "file_path" not in df_save.columns:
            df_save["file_path"] = pd.NA

        # 테이블 없으면 생성(신규 생성 시에는 run_* 컬럼 포함)
        DDL = text(f"""
        CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

        CREATE TABLE IF NOT EXISTS {FULL_NAME} (
            group_no            integer,
            barcode_information text NOT NULL,
            station             text,
            remark              text,
            end_day             date NOT NULL,
            end_time            text NOT NULL,
            run_time            double precision,
            boundary_run_time   double precision,
            okng_seq            integer NOT NULL,
            test_contents       text NOT NULL,
            test_time           text,
            from_to_test_ct     double precision,
            boundary_test_ct    double precision,
            diff_ct             double precision,
            problem1            text,
            problem2            text,
            problem3            text,
            problem4            text,
            file_path           text,

            run_start_ts        timestamp without time zone,
            run_end_ts          timestamp without time zone,
            run_seconds         double precision,

            updated_at          timestamp without time zone DEFAULT now(),
            PRIMARY KEY (barcode_information, end_day, end_time, test_contents, okng_seq)
        );
        """)

        # ✅ 고정 UPSERT를 여기서 만들지 않고, finally에서 "DB 실제 컬럼" 확인 후 동적으로 생성
        payload_for_save = {
            "DDL": DDL,
            "pk_cols": pk_cols,
            "df_save": df_save,
            "full_name": FULL_NAME,
            "schema": TARGET_SCHEMA,
            "table": TARGET_TABLE,
        }


    except Exception:
        # 예외는 finally에서 종료시간 찍고 다시 raise
        raise

    finally:
        # -----------------------------
        # 실행 종료/총시간
        # -----------------------------
        run_end_dt = datetime.now()
        run_seconds = round(time.perf_counter() - run_start_perf, 3)

        print(f"[RUN] end_ts={run_end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[RUN] run_seconds={run_seconds:.3f}")

        # payload_for_save가 존재하는 경우에만 DB 저장 실행
        if "payload_for_save" in locals():
            df_save2 = payload_for_save["df_save"].copy()
            df_save2["run_end_ts"] = run_end_dt
            df_save2["run_seconds"] = float(run_seconds)

            SQL_EXISTING_COLS = text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name   = :table
            """)

            with engine.begin() as conn:
                # 테이블 없으면 생성 (있으면 아무것도 안 함)
                conn.execute(payload_for_save["DDL"])

                # ✅ DB에 실제 존재하는 컬럼만 가져오기
                existing_cols = set(
                    pd.read_sql(
                        SQL_EXISTING_COLS, conn,
                        params={"schema": payload_for_save["schema"], "table": payload_for_save["table"]}
                    )["column_name"].astype(str).tolist()
                )

                # 저장 후보 컬럼 (DF 기준) - 여기서 DB에 없는 컬럼은 자동 제외됨
                candidate_cols = [
                    "group_no",
                    "barcode_information", "station", "remark", "end_day", "end_time",
                    "run_time", "boundary_run_time",
                    "okng_seq", "test_contents", "test_time",
                    "from_to_test_ct", "boundary_test_ct", "diff_ct",
                    "problem1", "problem2", "problem3", "problem4",
                    "file_path",
                    "run_start_ts", "run_end_ts", "run_seconds"
                ]

                save_cols = [c for c in candidate_cols if (c in df_save2.columns and c in existing_cols)]

                # PK는 반드시 있어야 함(테이블 스키마 이상 여부 체크)
                pk_cols = payload_for_save["pk_cols"]
                for c in pk_cols:
                    if c not in existing_cols:
                        raise RuntimeError(f"[ERROR] DB 테이블에 PK 컬럼 '{c}' 가 없습니다. 테이블 스키마를 확인하세요.")
                    if c not in save_cols:
                        # save_cols에도 포함되어야 UPSERT가 성립
                        save_cols.append(c)

                # INSERT/UPSERT SQL 동적 구성
                insert_cols_sql = ", ".join(save_cols)
                values_sql = ", ".join([f":{c}" for c in save_cols])

                conflict_cols = "barcode_information, end_day, end_time, test_contents, okng_seq"
                set_cols = [c for c in save_cols if c not in pk_cols]
                set_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in set_cols] + ["updated_at = now()"])

                UPSERT_SQL = text(f"""
                INSERT INTO {payload_for_save["full_name"]} ({insert_cols_sql})
                VALUES ({values_sql})
                ON CONFLICT ({conflict_cols})
                DO UPDATE SET {set_sql};
                """)

                rows = df_save2[save_cols].to_dict(orient="records")
                for r in rows:
                    for k, v in list(r.items()):
                        if pd.isna(v):
                            r[k] = None

                conn.execute(UPSERT_SQL, rows)

            print(f"[OK] Saved to {payload_for_save['full_name']} (rows={len(df_save2)}) / used_cols={len(save_cols)}")

        if SAVE_HTML_REPORT:
            print(f"[OK] HTML report saved: {os.path.abspath(REPORT_DIR)}")


if __name__ == "__main__":
    main()
