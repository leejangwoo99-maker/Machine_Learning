# -*- coding: utf-8 -*-
# fct_database.py (최종 통합본 - cycle end_time 스냅 기반 value/min/max/result 매칭 보장)

import io
import time
import urllib.parse
from datetime import date

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# 0) 실행 옵션
# ============================================================
SHOW_PREVIEW = False
END_TIME_TOL_SECONDS = 2
COPY_CHUNK_ROWS = 200_000

# ============================================================
# 1) DB 접속
# ============================================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

def get_engine(cfg):
    pw = urllib.parse.quote_plus(cfg["password"])
    conn_str = (
        f"postgresql+psycopg2://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
        "?connect_timeout=5"
    )
    return create_engine(conn_str, pool_pre_ping=True)

engine = get_engine(DB_CONFIG)
print("[OK] engine ready")

# ============================================================
# 2) Source/Target
# ============================================================
SRC_SCHEMA = "c1_fct_detail"
SRC_TABLE  = "fct_detail"

DATE_FROM = date(2025, 11, 13)
DATE_TO   = date(2025, 11, 18)

FCT_SCHEMA = "a2_fct_table"
FCT_TABLE  = "fct_table"

OUT_SCHEMA = "f_database"
OUT_TABLE  = "fct_database"

# ============================================================
# 3) 테이블 부트스트랩 (컬럼 순서 영구 고정)
# ============================================================
def bootstrap_fct_database(engine_):
    with engine_.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA};"))
        conn.execute(text(f"DROP TABLE IF EXISTS {OUT_SCHEMA}.{OUT_TABLE};"))
        conn.execute(text(f"""
        CREATE TABLE {OUT_SCHEMA}.{OUT_TABLE} (
            "group" BIGINT,
            barcode_information TEXT,
            station TEXT,
            remark TEXT,
            end_day DATE,
            end_time TIME,
            run_time DOUBLE PRECISION,
            contents TEXT,
            step_description TEXT,
            set_up_or_test_ct DOUBLE PRECISION,
            value TEXT,
            min TEXT,
            max TEXT,
            result TEXT,
            test_ct DOUBLE PRECISION,
            test_time TEXT,
            file_path TEXT,
            updated_at TIMESTAMPTZ DEFAULT now(),
            PRIMARY KEY (end_day, barcode_information, end_time, test_time, contents)
        );
        """))
    print(f"[OK] bootstrap done: {OUT_SCHEMA}.{OUT_TABLE} recreated")

# ============================================================
# 4) PD/Non-PD step map (원본 유지)
# ============================================================
PD_STEP_MAP = [
    "pd_1.00_dmm_c_rng_set",
    "pd_1.00_d_sig_val_090_set",
    "pd_1.00_load_c_set_cc",
    "pd_1.00_load_c_cc_rng_set",
    "pd_1.00_d_sig_val_000_set",
    "pd_1.00_dmm_dc_v_set",
    "pd_1.00_dmm_ac_0.6_set",
    "pd_1.00_ps_14.7_set",
    "pd_1.00_dmm_dc_c_set",
    "pd_1.01_ps_14.7_on",
    "pd_1.00_dmm_ac_0.6_set",
    "pd_1.01 Test Input Voltage(V)",
    "pd_1.02_Test_USB2_error(Type-C_A_side)",
    "pd_1.03_Test USB2 benchmark.maxrd(Mbit/s)",
    "pd_1.04 Test USB2 benchmark.maxwr(Mbit/s)",
    "pd_1.05 Test USB2 benchmark.avgrw(Mbit/s)",
    "pd_1.06_Test_USB1_error(Type-A)",
    "pd_1.07 Test USB1 benchmark.maxrd(Mbit/s)",
    "pd_1.08 Test USB1 benchmark.maxwr(Mbit/s)",
    "pd_1.09 Test USB1 benchmark.avgrw(Mbit/s)",
    "pd_1.10 Test Boston Firmware Version",
    "pd_1.11 Test Boston ASIC Version",
    "pd_1.12_Test_Carplay_Type-C(B_side)",
    "pd_1.13_Test_Carplay_Type-A",
    "pd_1.14 Profile Count Check",
    "pd_1.15_dmm_c_rng_set",
    "pd_1.15_load_a_cc_set",
    "pd_1.15_load_a_rng_set",
    "pd_1.15_load_c_cc_set",
    "pd_1.15_load_c_rng_set",
    "pd_1.15_dmm_regi_set",
    "pd_1.15_dmm_regi_ac_0.6_set",
    "pd_1.15_d_sig_val_000_set",
    "pd_1.15 Test Power-NC_Line(ohm)Resistor",
    "pd_1.16 Test DIM-NC_Line(ohm)",
    "pd_1.17 Test DIM-GND(ohm)",
    "pd_1.18_dmm_dc_v_set",
    "pd_1.18_dmm_ac_0.6_set",
    "pd_1.18_dmm_dc_c_set",
    "pd_1.18_load_a_sensing_on",
    "pd_1.18_load_c_sensing_on",
    "pd_1.18_ps_18v_set",
    "pd_1.18_ps_18v_on",
    "pd_1.18_dmm_ac_0.6_set",
    "pd_1.18 Test Input Voltage(V)",
    "pd_1.19 Test Idle Current(mA)",
    "pd_1.20 Test VUSB_type-C(No-Load-A_side)",
    "pd_1.21 Test VUSB_Type-A(No-Load)",
    "pd_1.22_dmm_3c_rng_set",
    "pd_1.22_load_a_5c_set",
    "pd_1.22_load_a_on",
    "pd_1.22 Test VUSB_Type-A(ELoad1=5A)Current",
    "pd_1.23 Test VUSB_Type-A(ELoad1=5A)Volt",
    "pd_1.24 Test VUSB_Type-C(ELoad2=5A)",
    "pd_1.25_load_a_off",
    "pd_1.25_load_c_5c_set",
    "pd_1.24_load_c_on",
    "pd_1.25 Test VUSB_Type-C(ELoad2=5A)Current",
    "pd_1.26 Test VUSB_Type-C(ELoad2=5A)Volt",
    "pd_1.27 Test VUSB_Type-A(ELoad1=5A)",
    "pd_1.28_load_c_off",
    "pd_1.28_load_a_2.4c_set",
    "pd_1.28_load_c_3c_set",
    "pd_1.28_load_a_2.4c_on",
    "pd_1.28_load_c_3c_on",
    "pd_1.28_Test_IELoad2_Type-C(B_side)",
    "pd_1.29_Test_VUSB_type-C_B_side(ELoad2=3A)",
    "pd_1.30 Test IELoad1_Type-A",
    "pd_1.31_Test_Type-A(ELoad1=2.4A)",
    "pd_1.32_load_c_1.3c_set",
    "pd_1.32_PD Negotiation SET PDO4",
    "pd_1.33_Test_VUSB_Type-C_A(ELoad2=1.35A)cur",
    "pd_1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol",
    "pd_1.35 Test Check CC1 level(A side)",
    "pd_1.36_load_c_off",
    "pd_1.36_dmm_ac_0.6_set",
    "pd_1.36_dmm_c_rng_set",
    "pd_1.36 Test iqz(uA)",
]

NONPD_STEP_MAP = [
    "nonpd_0.00_d_sig_val_090_set",
    "nonpd_0.00_load_a_cc_set",
    "nonpd_0.00_dmm_c_rng_set",
    "nonpd_0.00_load_c_cc_set",
    "nonpd_0.00_dmm_c_rng_set",
    "nonpd_0.00_dmm_regi_set",
    "nonpd_0.00_dmm_regi_ac_0.6_set",
    "nonpd_1.00 Test RGUSB_MiniB(ohm)",
    "nonpd_1.01 Test RGUSB_usb(ohm)",
    "nonpd_1.02 Test RGUSB_type-C(ohm)",
    "nonpd_1.03_d_sig_val_000_set",
    "nonpd_1.03_dmm_regi_set",
    "nonpd_1.03_dmm_regi_ac_0.6_set",
    "nonpd_1.03 Test Power-NC_Line(ohm)",
    "nonpd_1.04 Test DIM-NC_Line(ohm)",
    "nonpd_1.05 Test DIM-GND(ohm)",
    "nonpd_1.06_dmm_dc_v_set",
    "nonpd_1.06_dmm_ac_0.6_set",
    "nonpd_1.06_dmm_c_set",
    "nonpd_1.06_load_a_sensing_on",
    "nonpd_1.06_load_c_sensing_on",
    "nonpd_1.06_ps_16.5v_set",
    "nonpd_1.06_ps_on",
    "nonpd_1.06_dmm_ac_0.6_set",
    "nonpd_1.06 Test Input Voltage(V)",
    "nonpd_1.07 Test Idle Current(mA)",
    "nonpd_1.08 Test Boston Firmware Version",
    "nonpd_1.09 Test Boston ASIC Version",
    "nonpd_1.10_dmm_3c_rng_set",
    "nonpd_1.10_load_a_5.5c_set",
    "nonpd_1.10_load_a_on",
    "nonpd_1.10 Test VUSB_usb(ELoad1=5A)Volt(V)",
    "nonpd_1.11 Test VUSB_usb(ELoad1=5A)Curr(A)",
    "nonpd_1.12 Test VUSB_type-C(ELoad2=5A)(V)",
    "nonpd_1.13_load_a_off",
    "nonpd_1.13_load_c_5.5c_set",
    "nonpd_1.13_load_c_on",
    "nonpd_1.13 Test VUSB(ELoad2=5A)Volt(V)",
    "nonpd_1.14 Test VUSB(ELoad2=5A)Curr(A)",
    "nonpd_1.15 Test VUSB_usb (ELoad1=5A)(V)",
    "nonpd_1.16_load_c_off",
    "nonpd_1.16_dut_reset",
    "nonpd_1.16 Test Check CC1 level(V)",
    "nonpd_1.17 Test Check CC2 level(B side)(V)",
    "nonpd_1.18_load_a_2.4c_set",
    "nonpd_1.18_load_c_3c_set",
    "nonpd_1.18_load_a_on",
    "nonpd_1.18_load_c_on",
    "nonpd_1.18 Test VUSB_usb(ELoad1=2.4A)(V)",
    "nonpd_1.19 Test IELoad1(A)",
    "nonpd_1.20 Test VUSB_type-C(ELoad2=3A)(V)",
    "nonpd_1.21 Test IELoad2(A)",
    "nonpd_1.22_load_a_off",
    "nonpd_1.22_load_c_off",
    "nonpd_1.22 Test Carplay type-C",
    "nonpd_1.23 Test Carplay usb",
    "nonpd_1.24 Test USB2 error",
    "nonpd_1.25 Test USB2 benchmark.maxrd(Mbit/s)",
    "nonpd_1.26 Test USB2 benchmark.maxwr(Mbit/s)",
    "nonpd_1.27 Test USB2 benchmark.avgrw(Mbit/s)",
    "nonpd_1.28 Test USB1 error",
    "nonpd_1.29 Test USB1 benchmark.maxrd(Mbit/s)",
    "nonpd_1.30 Test USB1 benchmark.maxwr(Mbit/s)",
    "nonpd_1.31 Test USB1 benchmark.avgrw(Mbit/s)",
    "nonpd_1.32_dmm_ac_0.6_set",
    "nonpd_1.32_dmm_c_rng_set",
    "nonpd_1.32 Test iqz(uA)",
]

# ============================================================
# 5) 유틸
# ============================================================
def normalize_barcode(s: pd.Series) -> pd.Series:
    return s.astype("string").fillna("").str.replace(r"\s+", "", regex=True).str.strip()

def to_day_text_from_date(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y%m%d").astype("string")

def to_time_text_from_time(s: pd.Series) -> pd.Series:
    """
    end_time을 hhmiss(6자리)로 '정확' 변환
    - "17:18:18 GMT+09:00" 같은 문자열 안전 처리
    - "15:03:10.26" -> 150310
    """
    ss = s.astype("string").fillna("").str.strip()
    out = pd.Series(pd.NA, index=ss.index, dtype="string")

    m = ss.str.extract(r"(\d{2}):(\d{2}):(\d{2})", expand=True)
    has_hms = m[0].notna()
    if has_hms.any():
        out.loc[has_hms] = (m.loc[has_hms, 0] + m.loc[has_hms, 1] + m.loc[has_hms, 2]).astype("string")

    m6 = out.isna() & ss.str.fullmatch(r"\d{6}")
    if m6.any():
        out.loc[m6] = ss.loc[m6]

    rest = out.isna()
    if rest.any():
        digits = ss.loc[rest].str.replace(r"\D", "", regex=True)
        cand_ok = []
        for d in digits.to_list():
            pick = None
            for i in range(0, max(0, len(d) - 5)):
                sub = d[i:i+6]
                if len(sub) < 6:
                    break
                try:
                    hh = int(sub[0:2]); mm = int(sub[2:4]); sec = int(sub[4:6])
                except Exception:
                    continue
                if 0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= sec <= 59:
                    pick = sub
                    break
            cand_ok.append(pick)
        out.loc[rest] = pd.Series(cand_ok, index=digits.index, dtype="string")

    return out.astype("string")

def hhmiss_to_seconds(hhmiss: pd.Series) -> pd.Series:
    x = pd.to_numeric(hhmiss, errors="coerce").fillna(-1).astype("int64")
    hh = (x // 10000).clip(lower=0, upper=23)
    mm = ((x // 100) % 100).clip(lower=0, upper=59)
    ss = (x % 100).clip(lower=0, upper=59)
    return (hh * 3600 + mm * 60 + ss).astype("int64")

def normalize_test_time_for_sort(s: pd.Series) -> pd.Series:
    ss = s.astype("string").fillna("").str.strip()
    out = pd.Series(pd.NaT, index=ss.index, dtype="datetime64[ns]")

    m = ss.str.fullmatch(r"\d{14}")
    if m.any():
        out.loc[m] = pd.to_datetime(ss[m], format="%Y%m%d%H%M%S", errors="coerce")

    m = out.isna() & ss.str.contains(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}", regex=True)
    if m.any():
        out.loc[m] = pd.to_datetime(ss[m], format="%Y-%m-%d %H:%M:%S.%f", errors="coerce")

    m = out.isna() & ss.str.fullmatch(r"\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}")
    if m.any():
        out.loc[m] = pd.to_datetime(ss[m], format="%Y-%m-%d %H:%M:%S", errors="coerce")

    return out

def strip_step_prefix_by_remark(step: pd.Series, remark: pd.Series) -> pd.Series:
    out = step.astype("string")

    is_pd = remark.astype("string") == "PD"
    is_np = remark.astype("string") == "Non-PD"

    out.loc[is_pd] = out.loc[is_pd].str.replace(r"^pd_", "", regex=True)
    out.loc[is_np] = out.loc[is_np].str.replace(r"^nonpd_", "", regex=True)

    return out.fillna(pd.NA).astype("string").str.strip()

def norm_step_key(s: pd.Series) -> pd.Series:
    # SQL과 동일한 step_key_norm:
    # - NBSP -> space
    # - [_\s]+ -> ' ' (언더스코어/공백을 공백 1개로 통합)
    # - \s+ -> ' ' (다중 공백 정리)
    # - strip + lower
    ss = s.astype("string").fillna("")
    ss = ss.str.replace("\u00a0", " ", regex=False)
    ss = ss.str.replace(r"[_\s]+", " ", regex=True)
    ss = ss.str.replace(r"\s+", " ", regex=True)
    return ss.str.strip().str.lower()

# ============================================================
# 6) Source 로드 + MES group 제외 + group 생성 + 정렬
# ============================================================
def load_source(engine_) -> pd.DataFrame:
    sql = f"""
    SELECT
        barcode_information,
        remark,
        end_day,
        end_time,
        contents,
        test_ct,
        test_time,
        file_path
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE end_day BETWEEN :d1 AND :d2
    """
    with engine_.connect() as conn:
        df = pd.read_sql(text(sql), conn, params={"d1": DATE_FROM, "d2": DATE_TO})

    if df.empty:
        raise ValueError("[STOP] source 0 rows")

    df["_key"] = (
        df["barcode_information"].astype(str) + "||" +
        df["end_day"].astype(str) + "||" +
        df["end_time"].astype(str)
    )

    mes_keys = df.loc[df["contents"].astype(str).str.contains("MES", case=False, na=False), "_key"].unique().tolist()
    if mes_keys:
        df = df[~df["_key"].isin(mes_keys)].copy()

    df = df.sort_values(["end_day", "end_time"], ascending=[True, True]).reset_index(drop=True)

    df["_gkey"] = (
        df["barcode_information"].astype(str) + "||" +
        df["end_day"].astype(str) + "||" +
        df["end_time"].astype(str)
    )
    df["group"] = pd.factorize(df["_gkey"])[0].astype(np.int64) + 1

    df["_seq"] = np.arange(len(df), dtype=np.int64)
    df["_tt"] = normalize_test_time_for_sort(df["test_time"])
    df = df.sort_values(["group", "_tt", "_seq"], ascending=[True, True, True], na_position="last").reset_index(drop=True)

    out_cols = ["group", "barcode_information", "remark", "end_day", "end_time", "contents", "test_ct", "test_time", "file_path"]
    df = df[out_cols + ["_key", "_gkey", "_seq", "_tt"]].copy()

    print("[OK] loaded:", len(df), "rows / groups:", df["group"].nunique(), "/ mes_filtered_keys:", len(mes_keys))
    return df

# ============================================================
# 7) OK/NG block 합 + step 강제 매핑 + step 삭제
# ============================================================
OK_TOKEN = "테스트 결과 : OK"
NG_TOKEN = "테스트 결과 : NG"

PD_OKNG_EXPECT = 78
NONPD_OKNG_EXPECT = 67

DROP_STEPS = {
    # PD 삭제
    "pd_1.10 Test Boston Firmware Version",
    "pd_1.11 Test Boston ASIC Version",
    "pd_1.02_Test_USB2_error(Type-C_A_side)",
    "pd_1.14 Profile Count Check",
    "pd_1.12_Test_Carplay_Type-C(B_side)",
    "pd_1.13_Test_Carplay_Type-A",
    "pd_1.03_Test USB2 benchmark.maxrd(Mbit/s)",
    "pd_1.04 Test USB2 benchmark.maxwr(Mbit/s)",
    "pd_1.05 Test USB2 benchmark.avgrw(Mbit/s)",
    "pd_1.06_Test_USB1_error(Type-A)",
    "pd_1.07 Test USB1 benchmark.maxrd(Mbit/s)",
    "pd_1.08 Test USB1 benchmark.maxwr(Mbit/s)",
    "pd_1.09 Test USB1 benchmark.avgrw(Mbit/s)",
    "pd_1.15 Test Power-NC_Line(ohm)Resistor",
    "pd_1.16 Test DIM-NC_Line(ohm)",
    "pd_1.17 Test DIM-GND(ohm)",

    # Non-PD 삭제
    "nonpd_1.00 Test RGUSB_MiniB(ohm)",
    "nonpd_1.01 Test RGUSB_usb(ohm)",
    "nonpd_1.02 Test RGUSB_type-C(ohm)",
    "nonpd_1.03 Test Power-NC_Line(ohm)",
    "nonpd_1.04 Test DIM-NC_Line(ohm)",
    "nonpd_1.05 Test DIM-GND(ohm)",
    "nonpd_1.08 Test Boston Firmware Version",
    "nonpd_1.09 Test Boston ASIC Version",
    "nonpd_1.22 Test Carplay type-C",
    "nonpd_1.23 Test Carplay usb",
    "nonpd_1.24 Test USB2 error",
    "nonpd_1.25 Test USB2 benchmark.maxrd(Mbit/s)",
    "nonpd_1.26 Test USB2 benchmark.maxwr(Mbit/s)",
    "nonpd_1.27 Test USB2 benchmark.avgrw(Mbit/s)",
    "nonpd_1.28 Test USB1 error",
    "nonpd_1.29 Test USB1 benchmark.maxrd(Mbit/s)",
    "nonpd_1.30 Test USB1 benchmark.maxwr(Mbit/s)",
    "nonpd_1.31 Test USB1 benchmark.avgrw(Mbit/s)",
}

def build_steps_and_setup_ct(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    work["step_description"] = pd.Series(pd.NA, index=work.index, dtype="string")
    work["set_up_or_test_ct"] = np.nan

    work["_test_ct_num"] = pd.to_numeric(work["test_ct"], errors="coerce").fillna(0.0).astype("float64")

    c = work["contents"].astype(str)
    work["_is_okng"] = c.str.contains(OK_TOKEN, na=False) | c.str.contains(NG_TOKEN, na=False)

    work["_row_no"] = work.groupby("group", sort=False).cumcount().astype("int64")
    work["_csum"] = work.groupby("group", sort=False)["_test_ct_num"].cumsum()

    work["_k"] = np.where(
        work["_is_okng"],
        work.groupby("group", sort=False)["_is_okng"].cumsum().astype("int64"),
        0
    )

    okng = work.loc[work["_is_okng"], ["group", "_row_no", "_csum", "_k", "remark"]].copy()
    okng["prev_okng_row_no"] = okng.groupby("group", sort=False)["_row_no"].shift(1)

    base = work[["group", "_row_no", "_csum"]].rename(columns={"_row_no": "prev_okng_row_no", "_csum": "prev_okng_csum"})
    okng = okng.merge(base, how="left", on=["group", "prev_okng_row_no"])
    okng["prev_okng_csum"] = okng["prev_okng_csum"].fillna(0.0)

    okng["set_up_or_test_ct"] = (okng["_csum"] - okng["prev_okng_csum"]).astype("float64")

    def map_step(remark_: str, k_: int):
        if remark_ == "PD":
            return PD_STEP_MAP[k_ - 1] if 1 <= k_ <= len(PD_STEP_MAP) else None
        if remark_ == "Non-PD":
            return NONPD_STEP_MAP[k_ - 1] if 1 <= k_ <= len(NONPD_STEP_MAP) else None
        return None

    okng["step_description"] = [map_step(r, int(k)) for r, k in zip(okng["remark"].astype(str), okng["_k"].astype(int))]

    work.loc[work["_is_okng"], "set_up_or_test_ct"] = okng["set_up_or_test_ct"].to_numpy()
    work.loc[work["_is_okng"], "step_description"] = okng["step_description"].astype("string").to_numpy()

    cnt = okng.groupby(["group", "remark"], sort=False).size().reset_index(name="okng_cnt")
    bad_pd = cnt[(cnt["remark"] == "PD") & (cnt["okng_cnt"] != PD_OKNG_EXPECT)]
    bad_np = cnt[(cnt["remark"] == "Non-PD") & (cnt["okng_cnt"] != NONPD_OKNG_EXPECT)]
    print(f"[OK] okng count check: bad_PD={len(bad_pd)} bad_NonPD={len(bad_np)}")

    before = len(work)
    work = work[~work["step_description"].isin(DROP_STEPS)].copy()
    after = len(work)
    if before != after:
        print(f"[OK] dropped steps rows: {before - after}")

    work = work.sort_values(["end_day", "end_time", "group", "_tt", "_seq"],
                            ascending=[True, True, True, True, True],
                            na_position="last").reset_index(drop=True)

    work = work.drop(columns=["_test_ct_num", "_is_okng", "_row_no", "_csum", "_k"], errors="ignore")
    return work

# ============================================================
# 8) fct_table 로드(필요 컬럼)
# ============================================================
def load_fct_table(engine_, days_yyyymmdd, patterns) -> pd.DataFrame:
    if not days_yyyymmdd or not patterns:
        return pd.DataFrame()

    sql = f"""
    SELECT barcode_information, remark, station, end_day, end_time, run_time,
           step_description, value, min, max, result
    FROM {FCT_SCHEMA}.{FCT_TABLE}
    WHERE end_day = ANY(:days)
      AND barcode_information ILIKE ANY(:patterns)
    """
    with engine_.connect() as conn:
        fct = pd.read_sql(text(sql), conn, params={"days": days_yyyymmdd, "patterns": patterns})

    if fct.empty:
        return fct

    fct["barcode_norm"] = normalize_barcode(fct["barcode_information"])
    fct["end_day_text"] = fct["end_day"].astype("string")
    fct["end_time_int"] = to_time_text_from_time(fct["end_time"])
    fct["end_sec"] = hhmiss_to_seconds(pd.to_numeric(fct["end_time_int"], errors="coerce"))

    # ✅ fct_table step도 pd_/nonpd_ prefix가 들어올 수 있으니 무조건 제거 후 정규화
    fct["_step_raw"] = (
        fct["step_description"].astype("string").fillna("")
        .str.replace(r"^(pd_|nonpd_)", "", regex=True)
    )
    fct["step_key_norm"] = norm_step_key(fct["_step_raw"])

    for c in ["value", "min", "max", "result"]:
        fct[c] = fct[c].astype("string")

    fct["run_time"] = pd.to_numeric(fct["run_time"], errors="coerce")
    return fct

# ============================================================
# 9) ✅ 그룹 cycle end_time 을 fct_table 기준으로 스냅 + station/run_time 확정
# ============================================================
def snap_group_cycle_to_fct(df: pd.DataFrame, fct: pd.DataFrame) -> pd.DataFrame:
    """
    - group 대표 1행을 잡아 (barcode_norm, end_day_text)로 후보를 만든 후,
      end_sec diff(±tol) 최소 1건을 선택하여 group 전체에 (station, run_time, fct_end_time_int, fct_end_sec)를 확정
    """
    out = df.copy()

    # ---- 결과 컬럼: 먼저 생성 ----
    if "fct_end_time_int" not in out.columns:
        out["fct_end_time_int"] = pd.Series(pd.NA, index=out.index, dtype="string")
    if "fct_end_sec" not in out.columns:
        out["fct_end_sec"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    if "station" not in out.columns:
        out["station"] = pd.Series(pd.NA, index=out.index, dtype="string")
    if "run_time" not in out.columns:
        out["run_time"] = pd.Series(np.nan, index=out.index, dtype="float64")

    if fct is None or fct.empty:
        print("[DBG] fct empty -> snap skip")
        return out

    # ---- out 키 보장 ----
    if "barcode_norm" not in out.columns:
        out["barcode_norm"] = normalize_barcode(out["barcode_information"])
    if "end_day_text" not in out.columns:
        out["end_day_text"] = to_day_text_from_date(out["end_day"])
    if "end_time_int" not in out.columns:
        out["end_time_int"] = to_time_text_from_time(out["end_time"])
    if "end_sec" not in out.columns:
        out["end_sec"] = hhmiss_to_seconds(pd.to_numeric(out["end_time_int"], errors="coerce"))

    # ---- fct 키 보장 ----
    f = fct.copy()
    if "barcode_norm" not in f.columns:
        f["barcode_norm"] = normalize_barcode(f["barcode_information"])
    if "end_day_text" not in f.columns:
        f["end_day_text"] = f["end_day"].astype("string")
    if "end_time_int" not in f.columns:
        f["end_time_int"] = to_time_text_from_time(f["end_time"])
    if "end_sec" not in f.columns:
        f["end_sec"] = hhmiss_to_seconds(pd.to_numeric(f["end_time_int"], errors="coerce"))

    # ---- group 대표 1행 ----
    reps = out.groupby("group", sort=False).head(1)[["group", "barcode_norm", "end_day_text", "end_sec"]].copy()
    reps = reps.dropna(subset=["barcode_norm", "end_day_text", "end_sec"]).reset_index(drop=True)
    if reps.empty:
        print("[DBG] reps empty -> snap skip")
        return out

    # ---- 후보 생성: (barcode_norm, end_day_text) ----
    j = reps.merge(
        f[["barcode_norm", "end_day_text", "end_time_int", "end_sec", "station", "run_time"]],
        how="left",
        on=["barcode_norm", "end_day_text"],
        suffixes=("", "_fct"),
    )
    if j.empty:
        print("[DBG] snap join empty")
        return out

    # fct쪽 컬럼명 확정(merge suffix 유무에 상관없이 안전)
    j["fct_end_sec"] = j["end_sec_fct"] if "end_sec_fct" in j.columns else j["end_sec"]
    j["fct_end_time_int"] = (j["end_time_int_fct"] if "end_time_int_fct" in j.columns else j["end_time_int"]).astype("string")

    # ---- end_sec diff(±tol) ----
    j["diff"] = (pd.to_numeric(j["fct_end_sec"], errors="coerce") - pd.to_numeric(j["end_sec"], errors="coerce")).abs()
    j = j[(j["diff"].notna()) & (j["diff"] <= END_TIME_TOL_SECONDS)].copy()
    if j.empty:
        print("[DBG] snap: no cand within tol")
        return out

    # ---- group별 diff 최소 1건 ----
    best_idx = j.groupby("group")["diff"].idxmin()
    best = j.loc[best_idx, ["group", "fct_end_time_int", "fct_end_sec", "station", "run_time"]].copy()

    # ---- group 전체 전파 ----
    out = out.merge(best, on="group", how="left", suffixes=("", "_m"))

    # dtype 안정화(Int64 nullable) + where 병합(FutureWarning 회피)
    if "fct_end_sec_m" in out.columns:
        out["fct_end_sec"] = out["fct_end_sec"].astype("Int64")
        out["fct_end_sec_m"] = out["fct_end_sec_m"].astype("Int64")

    out["fct_end_time_int"] = out["fct_end_time_int"].where(
        out["fct_end_time_int"].notna(), out.get("fct_end_time_int_m")
    )
    out["fct_end_sec"] = out["fct_end_sec"].where(
        out["fct_end_sec"].notna(), out.get("fct_end_sec_m")
    )
    out["station"] = out["station"].where(
        out["station"].notna(), out.get("station_m")
    )
    out["run_time"] = out["run_time"].where(
        out["run_time"].notna(), out.get("run_time_m")
    )

    out = out.drop(columns=["fct_end_time_int_m", "fct_end_sec_m", "station_m", "run_time_m"], errors="ignore")

    matched = int(out["fct_end_sec"].notna().sum())
    total_groups = int(out["group"].nunique())
    matched_groups = int(out.loc[out["fct_end_sec"].notna(), "group"].nunique())
    print(f"[OK] snap group cycle: matched_groups={matched_groups}/{total_groups} (rows with fct_end_sec={matched})")

    return out

# ============================================================
# 10) ✅ value/min/max/result 매칭
# ============================================================
def debug_join_coverage(df2: pd.DataFrame, fct: pd.DataFrame):
    print("\n" + "="*120)
    print("[DBG] JOIN COVERAGE CHECK (단계별)")
    print("="*120)

    if fct is None or fct.empty:
        print("[DBG] fct is empty -> STOP")
        return

    a = df2.copy()
    b = fct.copy()

    # --- df2 필수 키 보장 ---
    if "end_day_text" not in a.columns:
        a["end_day_text"] = to_day_text_from_date(a["end_day"])
    if "fct_end_time_int" not in a.columns:
        print("[DBG] df2 has no fct_end_time_int -> snap 실패 가능성")
        print("      columns:", a.columns.tolist())
        return
    if "station" not in a.columns:
        a["station"] = pd.NA
    if "remark" not in a.columns:
        a["remark"] = pd.NA

    # step_key_norm(df2)
    a["_step_stripped"] = strip_step_prefix_by_remark(a["step_description"].astype("string"), a["remark"].astype("string"))
    a["step_key_norm"] = norm_step_key(a["_step_stripped"])

    # --- fct 키 보장 ---
    if "end_day_text" not in b.columns:
        b["end_day_text"] = b["end_day"].astype("string")
    if "end_time_int" not in b.columns:
        b["end_time_int"] = to_time_text_from_time(b["end_time"])
    if "station" not in b.columns:
        b["station"] = pd.NA
    if "remark" not in b.columns:
        b["remark"] = pd.NA

    # step_key_norm(fct): pd_/nonpd_ 제거 후 norm
    b["_step_raw"] = b["step_description"].astype("string").fillna("").str.replace(r"^(pd_|nonpd_)", "", regex=True)
    b["step_key_norm"] = norm_step_key(b["_step_raw"])

    # --- need 필터(실제 매칭 대상) ---
    need = a[
        a["fct_end_time_int"].notna()
        & a["end_day_text"].notna()
        & a["step_description"].notna()
    ].copy()

    print(f"[DBG] df2 rows total={len(a)} / need rows={len(need)}")
    print(f"[DBG] snap filled fct_end_time_int={int(a['fct_end_time_int'].notna().sum())} / station={int(a['station'].notna().sum())}")

    if need.empty:
        print("[DBG] need empty -> 매칭 시도 자체가 0임(스냅/step_description/필터 문제)")
        return

    # --- 1) (end_day_text, cycle_end_time) 만 ---
    j1 = need.merge(
        b[["end_day_text", "end_time_int"]],
        how="left",
        left_on=["end_day_text", "fct_end_time_int"],
        right_on=["end_day_text", "end_time_int"],
        indicator=True,
    )
    hit1 = int((j1["_merge"] == "both").sum())
    print(f"[DBG-1] match by (end_day_text, cycle_end_time) = {hit1} / {len(need)}")

    # --- 2) + station ---
    j2 = need.merge(
        b[["end_day_text", "end_time_int", "station"]],
        how="left",
        left_on=["end_day_text", "fct_end_time_int", "station"],
        right_on=["end_day_text", "end_time_int", "station"],
        indicator=True,
    )
    hit2 = int((j2["_merge"] == "both").sum())
    print(f"[DBG-2] match + station = {hit2} / {len(need)}")

    # --- 3) + step_key_norm ---
    j3 = need.merge(
        b[["end_day_text", "end_time_int", "station", "step_key_norm"]],
        how="left",
        left_on=["end_day_text", "fct_end_time_int", "station", "step_key_norm"],
        right_on=["end_day_text", "end_time_int", "station", "step_key_norm"],
        indicator=True,
    )
    hit3 = int((j3["_merge"] == "both").sum())
    print(f"[DBG-3] match + station + step_key_norm = {hit3} / {len(need)}")

    # --- 4) + remark(있으면) ---
    j4 = need.merge(
        b[["end_day_text", "end_time_int", "station", "step_key_norm", "remark"]],
        how="left",
        left_on=["end_day_text", "fct_end_time_int", "station", "step_key_norm", "remark"],
        right_on=["end_day_text", "end_time_int", "station", "step_key_norm", "remark"],
        indicator=True,
    )
    hit4 = int((j4["_merge"] == "both").sum())
    print(f"[DBG-4] match + station + step_key_norm + remark = {hit4} / {len(need)}")

    bad = j3[j3["_merge"] != "both"].copy()
    if not bad.empty:
        print("\n[DBG] (DBG-3)에서 불일치 샘플 5개")
        cols = ["barcode_information", "remark", "station", "end_day_text", "fct_end_time_int", "step_description", "step_key_norm"]
        print(bad[cols].head(5).to_string(index=False))

    print("\n[DBG] fct end_time_int sample:", b["end_time_int"].dropna().astype(str).head(5).tolist())
    print("[DBG] df2 fct_end_time_int sample:", a["fct_end_time_int"].dropna().astype(str).head(5).tolist())
    print("="*120 + "\n")

def match_value_min_max_result_by_cycle(df: pd.DataFrame, fct: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in ["value", "min", "max", "result"]:
        out[c] = pd.Series(pd.NA, index=out.index, dtype="string")

    if fct is None or fct.empty:
        print("[CHK] value matched: 0 /", len(out), "(fct empty)")
        return out

    # --- out 키 보장(안전장치) ---
    if "end_day_text" not in out.columns:
        out["end_day_text"] = to_day_text_from_date(out["end_day"])
    if "fct_end_time_int" not in out.columns:
        out["fct_end_time_int"] = pd.Series(pd.NA, index=out.index, dtype="string")
    if "station" not in out.columns:
        out["station"] = pd.Series(pd.NA, index=out.index, dtype="string")
    if "remark" not in out.columns:
        out["remark"] = pd.Series(pd.NA, index=out.index, dtype="string")

    # row_id 고정
    out["_row_id"] = np.arange(len(out), dtype=np.int64)

    # ✅ station 포함(필수)
    need = out[
        out["fct_end_time_int"].notna()
        & out["end_day_text"].notna()
        & out["step_description"].notna()
        & out["station"].notna()
    ].copy()

    if need.empty:
        print("[CHK] value matched: 0 /", len(out), "(need empty)")
        return out.drop(columns=["_row_id"], errors="ignore")

    # step 정규화(need는 pd_/nonpd_ 제거 후)
    need["step_key"] = strip_step_prefix_by_remark(need["step_description"], need["remark"])
    need["step_key_norm"] = norm_step_key(need["step_key"])

    # fct 키/정규화 보장
    fct2 = fct.copy()
    if "end_day_text" not in fct2.columns:
        fct2["end_day_text"] = fct2["end_day"].astype("string")
    if "end_time_int" not in fct2.columns:
        fct2["end_time_int"] = to_time_text_from_time(fct2["end_time"])
    if "step_key_norm" not in fct2.columns:
        fct2["_step_raw"] = (
            fct2["step_description"].astype("string").fillna("")
            .str.replace(r"^(pd_|nonpd_)", "", regex=True)
        )
        fct2["step_key_norm"] = norm_step_key(fct2["_step_raw"])

    # ============================================================
    # [DBG-MATCH-KEY] 키별 매칭 원인 추적 (충돌 방지 rename 버전만 사용)
    #   - 여기 출력이 "어느 키에서 0이 되는지"를 정확히 보여줌
    # ============================================================
    print("[DBG-MATCH-KEY] NA check:",
          "end_day_text", int(need["end_day_text"].isna().sum()),
          "station", int(need["station"].isna().sum()),
          "fct_end_time_int", int(need["fct_end_time_int"].isna().sum()),
          "step_key_norm", int(need["step_key_norm"].isna().sum()))

    fctk1 = fct2[["end_day_text", "end_time_int"]].rename(columns={"end_time_int": "end_time_int_fct"})
    fctk2 = fct2[["end_day_text", "station", "end_time_int"]].rename(columns={"end_time_int": "end_time_int_fct"})
    fctk3 = fct2[["end_day_text", "station", "end_time_int", "step_key_norm"]].rename(
        columns={"end_time_int": "end_time_int_fct"}
    )

    j1 = need.merge(
        fctk1,
        how="left",
        left_on=["end_day_text", "fct_end_time_int"],
        right_on=["end_day_text", "end_time_int_fct"],
    )
    print("[DBG-MATCH-KEY-1] by day+cycle_end:", int(j1["end_time_int_fct"].notna().sum()), "/", len(need))

    j2 = need.merge(
        fctk2,
        how="left",
        left_on=["end_day_text", "station", "fct_end_time_int"],
        right_on=["end_day_text", "station", "end_time_int_fct"],
    )
    print("[DBG-MATCH-KEY-2] + station:", int(j2["end_time_int_fct"].notna().sum()), "/", len(need))

    j3 = need.merge(
        fctk3,
        how="left",
        left_on=["end_day_text", "station", "fct_end_time_int", "step_key_norm"],
        right_on=["end_day_text", "station", "end_time_int_fct", "step_key_norm"],
    )
    print("[DBG-MATCH-KEY-3] + step_key_norm:", int(j3["end_time_int_fct"].notna().sum()), "/", len(need))

    # ============================================================
    # ✅ 실제 조인 (충돌 방지: end_time_int -> end_time_int_fct 로 rename 후 join)
    # ============================================================
    # ============================================================
    # ✅ 실제 조인 (컬럼 충돌 방지: need에서 value/min/max/result 제거 후 merge)
    # ============================================================
    fct_full = fct2[
        ["end_day_text", "station", "end_time_int", "step_key_norm", "value", "min", "max", "result"]].rename(
        columns={"end_time_int": "end_time_int_fct"}
    )

    # need에는 이미 value/min/max/result가 있을 수 있음(out에서 만들어짐) → merge 충돌 방지 위해 제거
    need2 = need.drop(columns=["value", "min", "max", "result"], errors="ignore").copy()

    j = need2.merge(
        fct_full,
        how="left",
        left_on=["end_day_text", "station", "fct_end_time_int", "step_key_norm"],
        right_on=["end_day_text", "station", "end_time_int_fct", "step_key_norm"],
    )

    print("[DBG-MATCH] join rows:", len(j), "/ need rows:", len(need2))
    print("[DBG-MATCH] non-null value in join:", int(j["value"].notna().sum()))

    # row_id별 첫 후보(동일키 중복 방지)
    j = j.sort_values(["_row_id"]).drop_duplicates(["_row_id"], keep="first")

    got = j[["_row_id", "value", "min", "max", "result"]].copy()

    out = out.merge(got, on="_row_id", how="left", suffixes=("", "_m"))
    for c in ["value", "min", "max", "result"]:
        out[c] = out[c].combine_first(out[f"{c}_m"])
    out = out.drop(columns=[f"{c}_m" for c in ["value", "min", "max", "result"]], errors="ignore")
    out = out.drop(columns=["_row_id"], errors="ignore")

    print("[DBG-MATCH] out non-null value:", int(out["value"].notna().sum()))
    print("[CHK] value matched:", int(out["value"].notna().sum()), "/", len(out))
    return out

# ============================================================
# 11) COPY 기반 Bulk Upsert
# ============================================================
FINAL_COLS = [
    "group", "barcode_information", "station", "remark", "end_day", "end_time", "run_time",
    "contents", "step_description", "set_up_or_test_ct", "value", "min", "max", "result",
    "test_ct", "test_time", "file_path"
]

def bulk_upsert_copy(engine_, df: pd.DataFrame):
    if df.empty:
        print("[SKIP] df empty")
        return

    df2 = df[FINAL_COLS].copy()

    df2["group"] = pd.to_numeric(df2["group"], errors="coerce").astype("Int64")
    df2["run_time"] = pd.to_numeric(df2["run_time"], errors="coerce")
    df2["set_up_or_test_ct"] = pd.to_numeric(df2["set_up_or_test_ct"], errors="coerce")
    df2["test_ct"] = pd.to_numeric(df2["test_ct"], errors="coerce")

    for c in ["value", "min", "max", "result"]:
        df2[c] = df2[c].astype("string")

    df2["end_day"] = pd.to_datetime(df2["end_day"], errors="coerce").dt.date
    end_time_hhmiss = to_time_text_from_time(df2["end_time"])
    df2["end_time"] = pd.to_datetime(end_time_hhmiss, format="%H%M%S", errors="coerce").dt.time

    db_cols = [
        "group","barcode_information","station","remark","end_day","end_time","run_time",
        "contents","step_description","set_up_or_test_ct","value","min","max","result",
        "test_ct","test_time","file_path"
    ]
    df_db = df2[db_cols].copy()

    def to_copy_buffer(frame: pd.DataFrame) -> io.StringIO:
        buf = io.StringIO()
        frame.to_csv(buf, sep="\t", header=False, index=False, na_rep="\\N", lineterminator="\n")
        buf.seek(0)
        return buf

    total = len(df_db)
    t0 = time.time()

    raw = engine_.raw_connection()
    try:
        with raw.cursor() as cur:
            staging = f"{OUT_SCHEMA}._stg_{OUT_TABLE}"
            cur.execute(f"""
            CREATE UNLOGGED TABLE IF NOT EXISTS {staging}
            (LIKE {OUT_SCHEMA}.{OUT_TABLE} INCLUDING DEFAULTS);
            """)
            raw.commit()

            for start in range(0, total, COPY_CHUNK_ROWS):
                end = min(start + COPY_CHUNK_ROWS, total)
                chunk = df_db.iloc[start:end].copy()

                cur.execute(f"TRUNCATE TABLE {staging};")
                raw.commit()

                buf = to_copy_buffer(chunk)
                copy_sql = f"""
                COPY {staging} (
                    "group", barcode_information, station, remark, end_day, end_time, run_time,
                    contents, step_description, set_up_or_test_ct, value, min, max, result,
                    test_ct, test_time, file_path
                )
                FROM STDIN WITH (FORMAT csv, DELIMITER E'\\t', NULL '\\N');
                """
                cur.copy_expert(copy_sql, buf)
                raw.commit()

                cur.execute(f"""
                INSERT INTO {OUT_SCHEMA}.{OUT_TABLE} (
                    "group", barcode_information, station, remark, end_day, end_time, run_time,
                    contents, step_description, set_up_or_test_ct, value, min, max, result,
                    test_ct, test_time, file_path
                )
                SELECT
                    "group", barcode_information, station, remark, end_day, end_time, run_time,
                    contents, step_description, set_up_or_test_ct, value, min, max, result,
                    test_ct, test_time, file_path
                FROM {staging}
                ON CONFLICT (end_day, barcode_information, end_time, test_time, contents)
                DO UPDATE SET
                    "group" = EXCLUDED."group",
                    station = EXCLUDED.station,
                    remark = EXCLUDED.remark,
                    run_time = EXCLUDED.run_time,
                    step_description = EXCLUDED.step_description,
                    set_up_or_test_ct = EXCLUDED.set_up_or_test_ct,
                    value = EXCLUDED.value,
                    min = EXCLUDED.min,
                    max = EXCLUDED.max,
                    result = EXCLUDED.result,
                    test_ct = EXCLUDED.test_ct,
                    file_path = EXCLUDED.file_path,
                    updated_at = now();
                """)
                raw.commit()

                print(f"[OK] bulk upsert chunk: {start}~{end-1} ({end-start} rows)")

    finally:
        raw.close()

    print(f"[DONE] UPSERT 완료: {total} rows / sec={time.time()-t0:.2f}")

# ============================================================
# 12) MAIN
# ============================================================
def main():
    bootstrap_fct_database(engine)

    df0 = load_source(engine)
    df1 = build_steps_and_setup_ct(df0)

    # 매칭 키 준비 (fct_detail 쪽)
    df1["barcode_norm"] = normalize_barcode(df1["barcode_information"])
    df1["end_day_text"] = to_day_text_from_date(df1["end_day"])
    df1["end_time_int"] = to_time_text_from_time(df1["end_time"])
    df1["end_sec"] = hhmiss_to_seconds(pd.to_numeric(df1["end_time_int"], errors="coerce"))

    days = df1["end_day_text"].dropna().unique().tolist()
    patterns = [f"%{b}%" for b in df1["barcode_norm"].dropna().unique().tolist()]

    fct = load_fct_table(engine, days, patterns)

    # 1) group cycle end_time + station/run_time 스냅
    df2 = snap_group_cycle_to_fct(df1, fct)

    # 2) 스냅 결과 기준으로 단계별 커버리지 디버그
    debug_join_coverage(df2, fct)

    # 3) value/min/max/result 매칭 (✅ 한 번만 수행)
    df3 = match_value_min_max_result_by_cycle(df2, fct)

    df_final = df3[FINAL_COLS].copy()

    if SHOW_PREVIEW:
        print(df_final.head(200))

    bulk_upsert_copy(engine, df_final)
    print("[DONE] 전체 파이프라인 종료")

if __name__ == "__main__":
    main()
