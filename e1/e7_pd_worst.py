# -*- coding: utf-8 -*-
"""
pd_worst_service.py

- pd_cal_test_ct_summary에서 test_contents(예: 1.36_dark_curr_check) 최신 upper_outlier 1건을 boundary_run_time으로 사용
- a2_fct_table.fct_table에서 (end_day=YYYYMMDD text, remark='PD') 대상 중 run_time > boundary_run_time 필터
- barcode별 1행으로 축약 후 run_time TOP5% 산출 (df_top)
- df_top 바코드로 c1_fct_detail.fct_detail 조회 후 group 생성/필터/OKNG 시퀀스/매핑/summary join/diff_ct 계산
- diff_ct Worst TOP5% 산출 후 file_path 매칭
- e4_predictive_maintenance.pd_worst UPSERT 저장 (중복이면 UPDATE)
- 실행 시작/종료/총 실행시간(초) 콘솔 출력 + (가능하면) DB 컬럼(run_start_ts, run_end_ts, run_seconds) 저장
  * 단, 기존 테이블에 run_* 컬럼이 없으면 자동 제외(에러 방지)

주의:
- end_day 입력: TARGET_END_DAY_TEXT = '20251219' (a2_fct_table.fct_table의 TEXT end_day 기준)
- c1_fct_detail.fct_detail은 end_day가 date 타입이라고 가정
"""

import math
import time
import urllib.parse
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text


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

# 입력 조건
TARGET_END_DAY_TEXT = "20251219"   # a2_fct_table.fct_table의 end_day (TEXT)
TARGET_REMARK = "PD"
TEST_CONTENTS_KEY = "1.36_dark_curr_check"  # pd_cal_test_ct_summary에서 upper_outlier를 가져올 key

# 저장 테이블
TARGET_SCHEMA = "e4_predictive_maintenance"
TARGET_TABLE = "pd_worst"


# =========================
# 1) DB 연결
# =========================
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


# =========================
# 2) 유틸
# =========================
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


def build_dynamic_upsert_sql(full_name: str, pk_cols: List[str], save_cols: List[str]) -> text:
    """
    save_cols: INSERT/UPDATE 대상으로 사용할 컬럼 목록(이미 DB 컬럼 존재 확인된 것만 들어와야 안전)
    pk_cols: PK 컬럼 목록
    """
    insert_cols_sql = ", ".join(save_cols)
    values_sql = ", ".join([f":{c}" for c in save_cols])

    conflict_cols = "barcode_information, end_day, end_time, test_contents, okng_seq"
    set_cols = [c for c in save_cols if c not in pk_cols]
    set_sql = ", ".join([f"{c} = EXCLUDED.{c}" for c in set_cols] + ["updated_at = now()"])

    return text(f"""
    INSERT INTO {full_name} ({insert_cols_sql})
    VALUES ({values_sql})
    ON CONFLICT ({conflict_cols})
    DO UPDATE SET {set_sql};
    """)


# =========================
# 3) 메인
# =========================
def main():
    # 실행시간
    run_start_dt = datetime.now()
    run_start_perf = time.perf_counter()
    print(f"[RUN] start_ts={run_start_dt.strftime('%Y-%m-%d %H:%M:%S')}")

    engine = get_engine(DB_CONFIG)

    # -----------------------------
    # Cell A) boundary_run_time 로드
    # -----------------------------
    SQL_BOUNDARY = text("""
    SELECT end_day, upper_outlier
    FROM e4_predictive_maintenance.pd_cal_test_ct_summary
    WHERE test_contents = :tc
      AND upper_outlier IS NOT NULL
    ORDER BY end_day DESC
    LIMIT 1
    """)

    b = pd.read_sql(SQL_BOUNDARY, engine, params={"tc": TEST_CONTENTS_KEY})
    if b.empty:
        raise RuntimeError(f"[ERROR] pd_cal_test_ct_summary에서 {TEST_CONTENTS_KEY} upper_outlier를 찾지 못했습니다.")

    boundary_run_time = float(b.loc[0, "upper_outlier"])
    boundary_src_day = str(b.loc[0, "end_day"])
    print(f"[OK] boundary_run_time={boundary_run_time} (source end_day={boundary_src_day})")

    # -----------------------------
    # Cell B) run_time TOP 5% (df_top)
    # -----------------------------
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

    df = pd.read_sql(SQL_DATA, engine, params={"end_day": TARGET_END_DAY_TEXT, "remark": TARGET_REMARK})
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
        "barcode_information", "remark", "station", "end_day", "end_time", "boundary_run_time", "run_time"
    ]]

    print(f"[OK] boundary={boundary_run_time} / TOP5% cut={cut:.2f} / rows={len(df_top)}")

    if len(df_top) == 0:
        raise RuntimeError("[ERROR] df_top이 비어있습니다. (run_time TOP5% 결과 없음)")

    # -----------------------------
    # Cell X1) fct_detail 조회 + meta merge
    # -----------------------------
    barcodes = df_top["barcode_information"].dropna().astype(str).drop_duplicates().tolist()
    print(f"[OK] Top barcodes = {len(barcodes)}")

    target_end_day_date = pd.to_datetime(TARGET_END_DAY_TEXT, format="%Y%m%d", errors="raise").strftime("%Y-%m-%d")

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
        params={"end_day": target_end_day_date, "remark": TARGET_REMARK, "barcodes": barcodes}
    )

    df_meta = df_top[["barcode_information", "station", "run_time", "boundary_run_time"]].copy()
    df_meta["barcode_information"] = df_meta["barcode_information"].astype(str)
    df_meta = df_meta.drop_duplicates("barcode_information")

    df_detail["barcode_information"] = df_detail["barcode_information"].astype(str)
    df_detail = df_detail.merge(df_meta, on="barcode_information", how="left")

    df_detail = df_detail[[
        "barcode_information", "station", "remark", "end_day", "end_time",
        "run_time", "boundary_run_time", "contents", "test_ct", "test_time"
    ]].copy()

    print(f"[OK] df_detail rows={len(df_detail)}")

    # -----------------------------
    # Cell X2) group 생성 + 제외 규칙 적용
    # -----------------------------
    df2 = df_detail.copy()
    df2["barcode_information"] = df2["barcode_information"].astype(str)
    df2["station"] = df2["station"].astype(str)
    df2["remark"] = df2["remark"].astype(str)
    df2["end_day"] = pd.to_datetime(df2["end_day"], errors="coerce").dt.strftime("%Y-%m-%d")
    df2["end_time"] = df2["end_time"].astype(str)

    df2["_test_ts"] = [parse_test_time_to_ts(d, t) for d, t in zip(df2["end_day"], df2["test_time"])]

    df2["group_key"] = df2["barcode_information"] + "|" + df2["end_day"] + "|" + df2["end_time"]
    df2["group"] = pd.factorize(df2["group_key"], sort=False)[0] + 1

    # 제외 (6)
    skip_mask = df2["contents"].astype(str).str.contains("START :: MES 이전공정 체크 SKIP", na=False)
    skip_groups = set(df2.loc[skip_mask, "group"].unique().tolist())

    # (7)(8) - group별 첫 "3~" 행의 test_ct가 NaN인 group만 남김
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

    # -----------------------------
    # Cell X3) from_to_test_ct (OK/NG만)
    # -----------------------------
    df3 = df2.copy()
    df3["_base_ts"] = df3.groupby("group")["_test_ts"].transform("first")

    is_okng = df3["contents"].astype(str).isin(["테스트 결과 : OK", "테스트 결과 : NG"])
    df3["from_to_test_ct"] = pd.NA

    mask = is_okng & df3["_test_ts"].notna() & df3["_base_ts"].notna()
    df3.loc[mask, "from_to_test_ct"] = (df3.loc[mask, "_test_ts"] - df3.loc[mask, "_base_ts"]).dt.total_seconds()

    print(f"[OK] from_to_test_ct filled rows={df3['from_to_test_ct'].notna().sum()}")

    # -----------------------------
    # Cell X4) okng_seq + test_contents 매핑
    # -----------------------------
    df4 = df3.copy()

    MAP_1_78 = {
        1:"1.00_dmm_c_rng_set", 2:"1.00_d_sig_val_090_set", 3:"1.00_load_c_set_cc", 4:"1.00_load_c_cc_rng_set",
        5:"1.00_d_sig_val_000_set", 6:"1.00_dmm_dc_v_set", 7:"1.00_dmm_ac_0.6_set", 8:"1.00_ps_14.7_set",
        9:"1.00_dmm_dc_c_set", 10:"1.01_ps_14.7_on", 11:"1.00_dmm_ac_0.6_set", 12:"1.01_input_14.7v",
        13:"1.02_usb_c_pm1", 14:"1.03_usb_c_pm2", 15:"1.04_usb_c_pm3", 16:"1.05_usb_c_pm4",
        17:"1.06_usb_a_pm1", 18:"1.07_usb_a_pm2", 19:"1.08_usb_a_pm3", 20:"1.09_usb_a_pm4",
        21:"1.10_fw_ver_check", 22:"1.11_chip_id_check", 23:"1.12_usb_c_carplay", 24:"1.13_usb_a_carplay",
        25:"1.14_pd_profile_count", 26:"1.15_dmm_c_rng_set", 27:"1.15_load_a_cc_set", 28:"1.15_load_a_rng_set",
        29:"1.15_load_c_cc_set", 30:"1.15_load_c_rng_set", 31:"1.15_dmm_regi_set", 32:"1.15_dmm_regi_ac_0.6_set",
        33:"1.15_d_sig_val_000_set", 34:"1.15_pin12_short_check", 35:"1.16_pin23_short_check", 36:"1.17_pin34_short_check",
        37:"1.18_dmm_dc_v_set", 38:"1.18_dmm_ac_0.6_set", 39:"1.18_dmm_dc_c_set", 40:"1.18_load_a_sensing_on",
        41:"1.18_load_c_sensing_on", 42:"1.18_ps_18v_set", 43:"1.18_ps_18v_on", 44:"1.18_dmm_ac_0.6_set",
        45:"1.18_input_18v", 46:"1.19_idle_c_check", 47:"1.20_no_load_usb_c", 48:"1.21_no_load_usb_a",
        49:"1.22_dmm_3c_rng_set", 50:"1.22_load_a_5c_set", 51:"1.22_load_a_on", 52:"1.22_overcurr_usb_a_c",
        53:"1.23_overcurr_usb_a_v", 54:"1.24_usb_c_v", 55:"1.25_load_a_off", 56:"1.25_load_c_5c_set",
        57:"1.24_load_c_on", 58:"1.25_overcurr_usb_c_c", 59:"1.26_overcurr_usb_c_v", 60:"1.27_usb_a_v",
        61:"1.28_load_c_off", 62:"1.28_load_a_2.4c_set", 63:"1.28_load_c_3c_set", 64:"1.28_load_a_2.4c_on",
        65:"1.28_load_c_3c_on", 66:"1.28_usb_c_bside_3c_check", 67:"1.29_usb_c_bside_v_check", 68:"1.30_usb_a_2.4c_check",
        69:"1.31_usb_a_v_check", 70:"1.32_load_c_1.3c_set", 71:"1.32_pdo4_set", 72:"1.33_usb_c_1.35c_check",
        73:"1.34_usb_c_v_check", 74:"1.35_usb_c_aside_cc_check", 75:"1.36_load_c_off", 76:"1.36_dmm_ac_0.6_set",
        77:"1.36_dmm_c_rng_set", 78:"1.36_dark_curr_check",
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
    df4.loc[is_okng2, "test_contents"] = df4.loc[is_okng2, "okng_seq"].map(MAP_1_78)

    df_final = df4[[
        "group", "barcode_information", "station", "remark", "end_day", "end_time",
        "run_time", "boundary_run_time", "test_time", "contents",
        "test_contents", "test_ct", "from_to_test_ct", "okng_seq"
    ]].copy()

    print(f"[OK] df_final rows={len(df_final)} / groups={df_final['group'].nunique()}")

    # -----------------------------
    # Cell X5) df15 (test_contents NA 제외)
    # -----------------------------
    df15 = df_final.dropna(subset=["test_contents"]).copy()
    df15 = df15[[
        "group","barcode_information","station","remark","end_day","end_time",
        "run_time","boundary_run_time","test_time","test_contents","from_to_test_ct","okng_seq"
    ]].reset_index(drop=True)

    # -----------------------------
    # Cell X6) summary join (boundary_test_ct + problem1~4)
    # -----------------------------
    target_end_day_x6 = str(df15["end_day"].dropna().iloc[0]).strip()

    SQL_PROBLEM_COLS = text("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'e4_predictive_maintenance'
      AND table_name   = 'pd_cal_test_ct_summary'
      AND column_name IN ('problem1','problem2','problem3','problem4')
    ORDER BY column_name
    """)
    present_problem_cols = pd.read_sql(SQL_PROBLEM_COLS, engine)["column_name"].tolist()
    present_problem_cols = [c.strip() for c in present_problem_cols if isinstance(c, str)]
    want_problem_cols = ["problem1","problem2","problem3","problem4"]
    use_problem_cols = [c for c in want_problem_cols if c in present_problem_cols]

    SQL_HAS_DAY = text("""
    SELECT COUNT(*) AS n
    FROM e4_predictive_maintenance.pd_cal_test_ct_summary
    WHERE end_day = :end_day
    """)
    n = int(pd.read_sql(SQL_HAS_DAY, engine, params={"end_day": target_end_day_x6}).iloc[0]["n"])

    if n > 0:
        summary_day = target_end_day_x6
    else:
        SQL_LATEST_DAY = text("""
        SELECT MAX(end_day) AS max_day
        FROM e4_predictive_maintenance.pd_cal_test_ct_summary
        """)
        summary_day = pd.read_sql(SQL_LATEST_DAY, engine).iloc[0]["max_day"]
        if pd.isna(summary_day):
            raise RuntimeError("[ERROR] pd_cal_test_ct_summary 테이블이 비어있습니다.")
        summary_day = str(summary_day).strip()

    select_cols = ["test_contents", "upper_outlier"] + use_problem_cols
    SQL_BOUNDARY2 = text(f"""
    SELECT {", ".join(select_cols)}
    FROM e4_predictive_maintenance.pd_cal_test_ct_summary
    WHERE end_day = :end_day
    """)
    df_boundary = pd.read_sql(SQL_BOUNDARY2, engine, params={"end_day": summary_day})
    df_boundary["test_contents"] = df_boundary["test_contents"].astype(str).str.strip()
    df_boundary["upper_outlier"] = pd.to_numeric(df_boundary["upper_outlier"], errors="coerce")
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

    # -----------------------------
    # Cell X7) diff_ct
    # -----------------------------
    df15_out["boundary_test_ct"] = pd.to_numeric(df15_out["boundary_test_ct"], errors="coerce")
    df15_out["from_to_test_ct"] = pd.to_numeric(df15_out["from_to_test_ct"], errors="coerce")
    df15_out["diff_ct"] = df15_out["from_to_test_ct"] - df15_out["boundary_test_ct"]

    df15_out2 = df15_out[[
        "group","barcode_information","station","remark","end_day","end_time","run_time","boundary_run_time",
        "okng_seq","test_contents","test_time","from_to_test_ct",
        "boundary_test_ct","diff_ct","problem1","problem2","problem3","problem4"
    ]].copy()

    # -----------------------------
    # Cell X8) diff_ct Worst TOP 5% (원본에서 TOP5% 개수 먼저 컷 → 그 안에서 barcode 대표화)
    # -----------------------------
    dfw = df15_out2.copy()
    dfw["diff_ct"] = pd.to_numeric(dfw["diff_ct"], errors="coerce")
    dfw["run_time"] = pd.to_numeric(dfw["run_time"], errors="coerce")
    dfw["okng_seq"] = pd.to_numeric(dfw["okng_seq"], errors="coerce")
    dfw = dfw.dropna(subset=["barcode_information","diff_ct","okng_seq"]).copy()

    n_all = len(dfw)
    top_n = math.ceil(n_all * 0.05)
    df_top_raw = dfw.sort_values("diff_ct", ascending=False).head(top_n).copy()

    df_top_raw["min_okng"] = df_top_raw.groupby("barcode_information")["okng_seq"].transform("min")
    df_spike = (
        df_top_raw[df_top_raw["okng_seq"] == df_top_raw["min_okng"]]
        .sort_values(["barcode_information","diff_ct","run_time"], ascending=[True,False,False])
        .drop_duplicates("barcode_information")
        .sort_values("run_time", ascending=False)
        .reset_index(drop=True)
    )

    print(f"[OK] diff_ct worst: 원본={n_all} → TOP5% raw={len(df_top_raw)} → 최종 rows={len(df_spike)}")

    # -----------------------------
    # Cell X9) file_path 매칭
    # -----------------------------
    key_df = df_spike[["barcode_information","end_day","end_time"]].copy()
    key_df["barcode_information"] = key_df["barcode_information"].astype(str).str.strip()
    key_df["end_day"] = key_df["end_day"].apply(norm_end_day)
    key_df["end_time"] = key_df["end_time"].apply(norm_end_time)
    key_df = key_df.dropna(subset=["barcode_information","end_day","end_time"]).copy()

    barcodes2 = key_df["barcode_information"].drop_duplicates().tolist()
    days2 = key_df["end_day"].drop_duplicates().tolist()

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
        df_fp.dropna(subset=["barcode_information","end_day","end_time"])
             .sort_values(["barcode_information","end_day","end_time"])
             .drop_duplicates(["barcode_information","end_day","end_time"], keep="first")
             .reset_index(drop=True)
    )

    df_spike_fp = df_spike.copy()
    df_spike_fp["barcode_information"] = df_spike_fp["barcode_information"].astype(str).str.strip()
    df_spike_fp["end_day"] = df_spike_fp["end_day"].apply(norm_end_day)
    df_spike_fp["end_time"] = df_spike_fp["end_time"].apply(norm_end_time)

    df_spike_fp = df_spike_fp.merge(
        df_fp[["barcode_information","end_day","end_time","file_path"]],
        on=["barcode_information","end_day","end_time"],
        how="left"
    )
    print(f"[OK] file_path filled={df_spike_fp['file_path'].notna().sum()} / rows={len(df_spike_fp)}")

    # -----------------------------
    # Cell X10) DB 저장 (DDL + 동적 UPSERT + 실행시간 컬럼)
    # -----------------------------
    run_end_dt = datetime.now()
    run_seconds = round(time.perf_counter() - run_start_perf, 3)

    print(f"[RUN] end_ts={run_end_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[RUN] run_seconds={run_seconds:.3f}")

    df_save = df_spike_fp.copy()

    # group -> group_no
    if "group" in df_save.columns and "group_no" not in df_save.columns:
        df_save["group_no"] = pd.to_numeric(df_save["group"], errors="coerce").astype("Int64")
    elif "group_no" not in df_save.columns:
        df_save["group_no"] = pd.NA

    # 실행시간 컬럼(DF에 생성)
    df_save["run_start_ts"] = run_start_dt
    df_save["run_end_ts"] = run_end_dt
    df_save["run_seconds"] = float(run_seconds)

    # 타입 정리
    pk_cols = ["barcode_information","end_day","end_time","test_contents","okng_seq"]
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

    full_name = f"{TARGET_SCHEMA}.{TARGET_TABLE}"

    DDL = text(f"""
    CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};

    CREATE TABLE IF NOT EXISTS {full_name} (
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

    SQL_EXISTING_COLS = text("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema
      AND table_name   = :table
    """)

    candidate_cols = [
        "group_no",
        "barcode_information","station","remark","end_day","end_time",
        "run_time","boundary_run_time",
        "okng_seq","test_contents","test_time",
        "from_to_test_ct","boundary_test_ct","diff_ct",
        "problem1","problem2","problem3","problem4",
        "file_path",
        "run_start_ts","run_end_ts","run_seconds"
    ]

    with engine.begin() as conn:
        conn.execute(DDL)

        existing_cols = set(
            pd.read_sql(
                SQL_EXISTING_COLS,
                conn,
                params={"schema": TARGET_SCHEMA, "table": TARGET_TABLE}
            )["column_name"].astype(str).tolist()
        )

        save_cols = [c for c in candidate_cols if (c in df_save.columns and c in existing_cols)]

        # PK 필수 체크
        for c in pk_cols:
            if c not in existing_cols:
                raise RuntimeError(f"[ERROR] DB 테이블에 PK 컬럼 '{c}' 가 없습니다. 테이블 스키마를 확인하세요.")
            if c not in save_cols:
                save_cols.append(c)

        # run_* 컬럼이 테이블에 없으면 경고만 출력 (저장 자동 제외)
        for c in ["run_start_ts","run_end_ts","run_seconds"]:
            if c not in existing_cols:
                print(f"[WARN] DB column missing -> will skip save: {c}")

        UPSERT_SQL = build_dynamic_upsert_sql(full_name, pk_cols, save_cols)

        rows = df_save[save_cols].to_dict(orient="records")
        for r in rows:
            for k, v in list(r.items()):
                if pd.isna(v):
                    r[k] = None

        conn.execute(UPSERT_SQL, rows)

    print(f"[OK] Saved to {full_name} (rows={len(df_save)}) / used_cols={len(save_cols)}")


if __name__ == "__main__":
    main()
