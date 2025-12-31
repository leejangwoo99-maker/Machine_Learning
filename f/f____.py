# -*- coding: utf-8 -*-
# fct_database.py (실전 운영형 완전 통합본 - cycle end_time 스냅 기반 value/min/max/result 매칭 보장)
#
# [운영형 반영 핵심]
# 0) "누적 적재"가 기본: 매 실행마다 DROP/CREATE 금지
# 1) bootstrap = CREATE SCHEMA/TABLE IF NOT EXISTS (최초 1회/없으면 생성)
# 2) DATE_FROM/DATE_TO = "자동 증분 선택" 지원
#    - MODE = "AUTO_INCREMENT" : OUT_TABLE에 이미 적재된 max(end_day) 이후만 자동 처리
#    - MODE = "MANUAL_RANGE"   : DATE_FROM~DATE_TO 수동 범위 처리(기존과 동일)
# 3) 실행 시작/종료/총 시간(초) 콘솔 출력 유지
# 4) df 크기별 자동 분기 UPSERT 유지
#    - 소량: DIRECT UPSERT(execute_values)
#    - 대량: COPY+UNLOGGED staging+UPSERT
# 5) ✅ [추가] file_path 기준 "이미 처리된 파일"은 전체 파이프라인에서 제외(pass)
#    - OUT_TABLE에 존재하는 file_path와 동일하면, 소스(df0)에서 해당 file_path 전체 제외
#    - 안전장치: file_path NULL/빈값은 제외대상에서 뺌(처리 대상 유지)
#
# ⚠️ 기존 기능(스냅/매칭/스텝매핑/삭제/PK/업서트/타입변환/디버그 커버리지 등) 절대 누락 없이 유지
# ⚠️ [요청] 1초 실시간 감시모드는 제외(단발 실행)

import io
import time
import urllib.parse
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

# ============================================================
# 0) 실행 옵션
# ============================================================
SHOW_PREVIEW = False
END_TIME_TOL_SECONDS = 2
COPY_CHUNK_ROWS = 200_000
DEBUG_JOIN_COVERAGE = False   # ← 디버그 조인 커버리지 스위치

# 자동 분기 기준(행 수)
UPSERT_DIRECT_THRESHOLD_ROWS = 20_000
DIRECT_UPSERT_BATCH_ROWS = 5_000

# ============================================================
# [운영 모드] 날짜 처리 방식
# ============================================================
# - "AUTO_INCREMENT": OUT_TABLE에 이미 적재된 max(end_day) 이후만 자동 처리
# - "MANUAL_RANGE"  : DATE_FROM~DATE_TO 수동 처리
DATE_MODE = "MANUAL_RANGE"   # "AUTO_INCREMENT" or "MANUAL_RANGE"

# MANUAL_RANGE 모드에서만 사용
DATE_FROM = date(2025, 11, 15)
DATE_TO   = date(2025, 11, 18)

# AUTO_INCREMENT 모드 세부 옵션
# - SAFETY_LOOKBACK_DAYS: "실시간/지연 도착" 데이터 보정용 재처리 구간(최근 N일을 다시 UPSERT)
#   예) 2일로 두면: (max_end_day - 2일) ~ today 범위를 매번 재처리 -> 누락 방지(UPSERT라 중복 문제 없음)
SAFETY_LOOKBACK_DAYS = 2

# - AUTO_TO_TODAY: True면 DATE_TO를 오늘로 고정 (운영 기본)
AUTO_TO_TODAY = True

# - MAX_DAYS_PER_RUN: 하루치씩 처리하면 안정적, 너무 길면 한번에 크게 처리
MAX_DAYS_PER_RUN = 7   # 1~7 권장 (운영 안정 목적)

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

FCT_SCHEMA = "a2_fct_table"
FCT_TABLE  = "fct_table"

OUT_SCHEMA = "f_database"
OUT_TABLE  = "fct_database"

# ============================================================
# 3) 테이블 부트스트랩 (운영형: DROP 금지, IF NOT EXISTS)
# ============================================================
def bootstrap_fct_database(engine_):
    with engine_.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA};"))
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {OUT_SCHEMA}.{OUT_TABLE} (
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
        # 운영에서 흔히 필요한 보조 인덱스
        conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_end_day
        ON {OUT_SCHEMA}.{OUT_TABLE} (end_day);
        """))
        conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_barcode
        ON {OUT_SCHEMA}.{OUT_TABLE} (barcode_information);
        """))
        conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_station
        ON {OUT_SCHEMA}.{OUT_TABLE} (station);
        """))
        # ✅ file_path 스킵/조회 성능용(선택이지만 권장)
        conn.execute(text(f"""
        CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_file_path
        ON {OUT_SCHEMA}.{OUT_TABLE} (file_path);
        """))
    print(f"[OK] bootstrap done: {OUT_SCHEMA}.{OUT_TABLE} ensured (NO DROP)")

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
    ss = s.astype("string").fillna("")
    ss = ss.str.replace("\u00a0", " ", regex=False)
    ss = ss.str.replace(r"[_\s]+", " ", regex=True)
    ss = ss.str.replace(r"\s+", " ", regex=True)
    return ss.str.strip().str.lower()

# ============================================================
# 6) [운영] 처리 날짜 범위 결정
# ============================================================
def _safe_date(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return None
    if isinstance(v, str):
        try:
            return datetime.strptime(v, "%Y-%m-%d").date()
        except Exception:
            return None
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return None

def resolve_date_range(engine_) -> tuple[date, date]:
    """
    DATE_MODE에 따라 (DATE_FROM, DATE_TO) 결정
    - AUTO_INCREMENT: OUT_TABLE max(end_day) 기준 증분 처리 + safety lookback 적용
    - MANUAL_RANGE : 전역 DATE_FROM~DATE_TO 그대로
    """
    if DATE_MODE == "MANUAL_RANGE":
        if DATE_FROM is None or DATE_TO is None:
            raise ValueError("[STOP] MANUAL_RANGE requires DATE_FROM and DATE_TO")
        if DATE_TO < DATE_FROM:
            raise ValueError("[STOP] DATE_TO < DATE_FROM")
        return DATE_FROM, DATE_TO

    # AUTO_INCREMENT
    with engine_.connect() as conn:
        max_sql = text(f"SELECT MAX(end_day) AS max_end_day FROM {OUT_SCHEMA}.{OUT_TABLE};")
        row = conn.execute(max_sql).fetchone()
        max_end_day = _safe_date(row[0] if row else None)

        # 소스 최신일(참고)
        src_max_sql = text(f"SELECT MAX(end_day) AS src_max_end_day FROM {SRC_SCHEMA}.{SRC_TABLE};")
        row2 = conn.execute(src_max_sql).fetchone()
        src_max_end_day = _safe_date(row2[0] if row2 else None)

    today = datetime.now().date()
    to_day = today if AUTO_TO_TODAY else (src_max_end_day or today)

    if src_max_end_day is None:
        raise ValueError("[STOP] source has no end_day (src_max_end_day is NULL)")

    # 처리 상한: 소스 최신일/오늘 중 작은 값
    if to_day > src_max_end_day:
        to_day = src_max_end_day

    if max_end_day is None:
        # 최초 실행: 최신일 기준 MAX_DAYS_PER_RUN만큼만
        from_day = max(date(1900, 1, 1), to_day - timedelta(days=MAX_DAYS_PER_RUN - 1))
    else:
        from_day = max_end_day - timedelta(days=SAFETY_LOOKBACK_DAYS)
        if from_day > to_day:
            from_day = to_day
        if (to_day - from_day).days + 1 > MAX_DAYS_PER_RUN:
            from_day = to_day - timedelta(days=MAX_DAYS_PER_RUN - 1)

    if to_day < from_day:
        from_day = to_day

    return from_day, to_day

# ============================================================
# 6-A) ✅ [추가] file_path 기준 처리 제외(이미 처리된 파일 PASS)
# ============================================================
def _clean_file_path_series(s: pd.Series) -> pd.Series:
    # 공백/널/빈문자 안전 정리
    return s.astype("string").fillna("").str.strip()

def get_processed_file_paths(engine_) -> set:
    """
    OUT_TABLE에 이미 존재하는 file_path 집합을 읽어옴.
    - NULL/빈문자 제외
    - 대량에서도 문제없도록 DISTINCT만 가져옴(인덱스 권장)
    """
    with engine_.connect() as conn:
        rows = conn.execute(text(f"""
        SELECT DISTINCT file_path
        FROM {OUT_SCHEMA}.{OUT_TABLE}
        WHERE file_path IS NOT NULL
          AND btrim(file_path) <> ''
        """)).fetchall()
    return set(r[0] for r in rows if r and r[0] is not None and str(r[0]).strip() != "")

def filter_out_already_processed_by_file_path(df: pd.DataFrame, processed_paths: set) -> pd.DataFrame:
    """
    df0 (source)에서 file_path가 processed_paths에 존재하면 해당 file_path 전체를 제외
    """
    if df.empty:
        return df
    if not processed_paths:
        return df

    fp = _clean_file_path_series(df["file_path"] if "file_path" in df.columns else pd.Series([], dtype="string"))
    # file_path 비어있는 row는 "스킵 판단 불가" -> 처리 대상 유지
    has_fp = fp != ""
    mask_skip = has_fp & fp.isin(processed_paths)

    before = len(df)
    out = df.loc[~mask_skip].copy()
    skipped = before - len(out)

    if skipped > 0:
        # 몇 개의 file_path가 스킵됐는지 같이 출력(운영에 유용)
        try:
            skipped_files = int(fp.loc[mask_skip].nunique())
        except Exception:
            skipped_files = -1
        print(f"[SKIP] file_path already processed -> rows skipped: {skipped} (unique file_path skipped: {skipped_files})")

    return out

# ============================================================
# 7) Source 로드 + MES group 제외 + group 생성 + 정렬
# ============================================================
def load_source(engine_, d_from: date, d_to: date) -> pd.DataFrame:
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
        df = pd.read_sql(text(sql), conn, params={"d1": d_from, "d2": d_to})

    if df.empty:
        print(f"[SKIP] source 0 rows in range {d_from} ~ {d_to}")
        return df

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
# 8) OK/NG block 합 + step 강제 매핑 + step 삭제
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
# 9) fct_table 로드(필요 컬럼)
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
# 10) 그룹 cycle end_time 을 fct_table 기준으로 스냅 + station/run_time 확정
# ============================================================
def snap_group_cycle_to_fct(df: pd.DataFrame, fct: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

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

    if "barcode_norm" not in out.columns:
        out["barcode_norm"] = normalize_barcode(out["barcode_information"])
    if "end_day_text" not in out.columns:
        out["end_day_text"] = to_day_text_from_date(out["end_day"])
    if "end_time_int" not in out.columns:
        out["end_time_int"] = to_time_text_from_time(out["end_time"])
    if "end_sec" not in out.columns:
        out["end_sec"] = hhmiss_to_seconds(pd.to_numeric(out["end_time_int"], errors="coerce"))

    f = fct.copy()
    if "barcode_norm" not in f.columns:
        f["barcode_norm"] = normalize_barcode(f["barcode_information"])
    if "end_day_text" not in f.columns:
        f["end_day_text"] = f["end_day"].astype("string")
    if "end_time_int" not in f.columns:
        f["end_time_int"] = to_time_text_from_time(f["end_time"])
    if "end_sec" not in f.columns:
        f["end_sec"] = hhmiss_to_seconds(pd.to_numeric(f["end_time_int"], errors="coerce"))

    reps = out.groupby("group", sort=False).head(1)[["group", "barcode_norm", "end_day_text", "end_sec"]].copy()
    reps = reps.dropna(subset=["barcode_norm", "end_day_text", "end_sec"]).reset_index(drop=True)
    if reps.empty:
        print("[DBG] reps empty -> snap skip")
        return out

    j = reps.merge(
        f[["barcode_norm", "end_day_text", "end_time_int", "end_sec", "station", "run_time"]],
        how="left",
        on=["barcode_norm", "end_day_text"],
        suffixes=("", "_fct"),
    )
    if j.empty:
        print("[DBG] snap join empty")
        return out

    j["fct_end_sec"] = j["end_sec_fct"] if "end_sec_fct" in j.columns else j["end_sec"]
    j["fct_end_time_int"] = (j["end_time_int_fct"] if "end_time_int_fct" in j.columns else j["end_time_int"]).astype("string")

    j["diff"] = (pd.to_numeric(j["fct_end_sec"], errors="coerce") - pd.to_numeric(j["end_sec"], errors="coerce")).abs()
    j = j[(j["diff"].notna()) & (j["diff"] <= END_TIME_TOL_SECONDS)].copy()
    if j.empty:
        print("[DBG] snap: no cand within tol")
        return out

    best_idx = j.groupby("group")["diff"].idxmin()
    best = j.loc[best_idx, ["group", "fct_end_time_int", "fct_end_sec", "station", "run_time"]].copy()

    out = out.merge(best, on="group", how="left", suffixes=("", "_m"))

    if "fct_end_sec_m" in out.columns:
        out["fct_end_sec"] = out["fct_end_sec"].astype("Int64")
        out["fct_end_sec_m"] = out["fct_end_sec_m"].astype("Int64")

    out["fct_end_time_int"] = out["fct_end_time_int"].where(out["fct_end_time_int"].notna(), out.get("fct_end_time_int_m"))
    out["fct_end_sec"] = out["fct_end_sec"].where(out["fct_end_sec"].notna(), out.get("fct_end_sec_m"))
    out["station"] = out["station"].where(out["station"].notna(), out.get("station_m"))
    out["run_time"] = out["run_time"].where(out["run_time"].notna(), out.get("run_time_m"))

    out = out.drop(columns=["fct_end_time_int_m", "fct_end_sec_m", "station_m", "run_time_m"], errors="ignore")

    matched = int(out["fct_end_sec"].notna().sum())
    total_groups = int(out["group"].nunique())
    matched_groups = int(out.loc[out["fct_end_sec"].notna(), "group"].nunique())
    print(f"[OK] snap group cycle: matched_groups={matched_groups}/{total_groups} (rows with fct_end_sec={matched})")

    return out

# ============================================================
# 11) value/min/max/result 매칭
# ============================================================
def debug_join_coverage(df2: pd.DataFrame, fct: pd.DataFrame, sample_n: int = 200_000):
    print("\n" + "="*120)
    print("[DBG] JOIN COVERAGE CHECK (메모리 안전, 폭발 방지)")
    print("="*120)

    if fct is None or fct.empty:
        print("[DBG] fct is empty -> STOP")
        return

    a = df2.copy()
    b = fct.copy()

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

    a["_step_stripped"] = strip_step_prefix_by_remark(a["step_description"].astype("string"), a["remark"].astype("string"))
    a["step_key_norm"] = norm_step_key(a["_step_stripped"])

    if "end_day_text" not in b.columns:
        b["end_day_text"] = b["end_day"].astype("string")
    if "end_time_int" not in b.columns:
        b["end_time_int"] = to_time_text_from_time(b["end_time"])
    if "station" not in b.columns:
        b["station"] = pd.NA
    if "remark" not in b.columns:
        b["remark"] = pd.NA

    b["_step_raw"] = b["step_description"].astype("string").fillna("").str.replace(r"^(pd_|nonpd_)", "", regex=True)
    b["step_key_norm"] = norm_step_key(b["_step_raw"])

    need = a[
        a["fct_end_time_int"].notna()
        & a["end_day_text"].notna()
        & a["step_description"].notna()
        & a["station"].notna()
    ].copy()

    print(f"[DBG] df2 rows total={len(a)} / need rows={len(need)}")
    print(f"[DBG] snap filled fct_end_time_int={int(a['fct_end_time_int'].notna().sum())} / station={int(a['station'].notna().sum())}")

    if need.empty:
        print("[DBG] need empty -> 매칭 시도 자체가 0")
        return

    if len(need) > sample_n:
        need = need.sample(sample_n, random_state=42).copy()
        print(f"[DBG] need sampled to {len(need)} rows for safe debug")

    b1 = b[["end_day_text", "end_time_int"]].drop_duplicates()
    ref1 = (b1["end_day_text"].astype("string") + "|" + b1["end_time_int"].astype("string"))
    key1 = (need["end_day_text"].astype("string") + "|" + need["fct_end_time_int"].astype("string"))
    hit1 = int(key1.isin(pd.Index(ref1)).sum())
    print(f"[DBG-1] match by (end_day_text, cycle_end_time) = {hit1} / {len(need)}")

    b2 = b[["end_day_text", "end_time_int", "station"]].drop_duplicates()
    ref2 = (b2["end_day_text"].astype("string") + "|" + b2["end_time_int"].astype("string") + "|" + b2["station"].astype("string"))
    key2 = (need["end_day_text"].astype("string") + "|" + need["fct_end_time_int"].astype("string") + "|" + need["station"].astype("string"))
    hit2 = int(key2.isin(pd.Index(ref2)).sum())
    print(f"[DBG-2] match + station = {hit2} / {len(need)}")

    b3 = b[["end_day_text", "end_time_int", "station", "step_key_norm"]].drop_duplicates()
    ref3 = (b3["end_day_text"].astype("string") + "|" + b3["end_time_int"].astype("string") + "|" + b3["station"].astype("string") + "|" + b3["step_key_norm"].astype("string"))
    key3 = (need["end_day_text"].astype("string") + "|" + need["fct_end_time_int"].astype("string") + "|" + need["station"].astype("string") + "|" + need["step_key_norm"].astype("string"))
    hit3 = int(key3.isin(pd.Index(ref3)).sum())
    print(f"[DBG-3] match + station + step_key_norm = {hit3} / {len(need)}")

    b4 = b[["end_day_text", "end_time_int", "station", "step_key_norm", "remark"]].drop_duplicates()
    ref4 = (b4["end_day_text"].astype("string") + "|" + b4["end_time_int"].astype("string") + "|" + b4["station"].astype("string") + "|" + b4["step_key_norm"].astype("string") + "|" + b4["remark"].astype("string"))
    key4 = (need["end_day_text"].astype("string") + "|" + need["fct_end_time_int"].astype("string") + "|" + need["station"].astype("string") + "|" + need["step_key_norm"].astype("string") + "|" + need["remark"].astype("string"))
    hit4 = int(key4.isin(pd.Index(ref4)).sum())
    print(f"[DBG-4] match + station + step_key_norm + remark = {hit4} / {len(need)}")

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

    out["barcode_norm"] = normalize_barcode(out["barcode_information"])
    out["end_day_text"] = to_day_text_from_date(out["end_day"])
    out["fct_end_time_int"] = out["fct_end_time_int"].astype("string")
    out["station"] = out["station"].astype("string")
    out["remark"] = out["remark"].astype("string")

    out["_row_id"] = np.arange(len(out), dtype=np.int64)

    need = out[
        out["barcode_norm"].notna() &
        out["end_day_text"].notna() &
        out["fct_end_time_int"].notna() &
        out["station"].notna() &
        out["step_description"].notna()
    ].copy()

    if need.empty:
        print("[CHK] value matched: 0 /", len(out), "(need empty)")
        return out.drop(columns=["_row_id"], errors="ignore")

    need["_step_stripped"] = strip_step_prefix_by_remark(need["step_description"], need["remark"])
    need["step_key_norm"] = norm_step_key(need["_step_stripped"])

    fct2 = fct.copy()
    fct2["barcode_norm"] = normalize_barcode(fct2["barcode_information"])
    fct2["end_day_text"] = fct2["end_day"].astype("string")
    fct2["end_time_int"] = to_time_text_from_time(fct2["end_time"])
    fct2["_step_raw"] = fct2["step_description"].astype("string").fillna("").str.replace(r"^(pd_|nonpd_)", "", regex=True)
    fct2["step_key_norm"] = norm_step_key(fct2["_step_raw"])

    for c in ["value", "min", "max", "result"]:
        fct2[c] = fct2[c].astype("string")

    fct_key = fct2[
        ["barcode_norm","end_day_text","station","end_time_int","step_key_norm","value","min","max","result"]
    ].rename(columns={"end_time_int":"end_time_int_fct"})

    need2 = need.drop(columns=["value","min","max","result"], errors="ignore")

    j = need2.merge(
        fct_key,
        how="left",
        left_on=["barcode_norm","end_day_text","station","fct_end_time_int","step_key_norm"],
        right_on=["barcode_norm","end_day_text","station","end_time_int_fct","step_key_norm"]
    )

    print("[DBG-MATCH] join rows:", len(j), "/", len(need2))
    print("[DBG-MATCH] non-null value in join:", int(j["value"].notna().sum()))

    j = j.sort_values("_row_id").drop_duplicates("_row_id", keep="first")

    got = j[["_row_id","value","min","max","result"]]

    out = out.merge(got, on="_row_id", how="left", suffixes=("", "_m"))
    for c in ["value","min","max","result"]:
        out[c] = out[c].combine_first(out[f"{c}_m"])

    out = out.drop(columns=[f"{c}_m" for c in ["value","min","max","result"]], errors="ignore")
    out = out.drop(columns=["_row_id"], errors="ignore")

    print("[CHK] value matched:", int(out["value"].notna().sum()), "/", len(out))
    return out

# ============================================================
# 12) UPSERT (자동 분기: DIRECT vs COPY)
# ============================================================
FINAL_COLS = [
    "group", "barcode_information", "station", "remark", "end_day", "end_time", "run_time",
    "contents", "step_description", "set_up_or_test_ct", "value", "min", "max", "result",
    "test_ct", "test_time", "file_path"
]

def _prepare_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()

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

    return df2

def bulk_upsert_copy(engine_, df: pd.DataFrame):
    if df.empty:
        print("[SKIP] df empty")
        return

    df2 = _prepare_df_for_db(df)

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
                PK_COLS = ["end_day", "barcode_information", "end_time", "test_time", "contents"]

                before_rows = len(chunk)
                chunk = chunk.drop_duplicates(subset=PK_COLS, keep="last").copy()
                dropped = before_rows - len(chunk)
                if dropped > 0:
                    print(f"[WARN] staging chunk PK duplicates dropped: {dropped} (rows {before_rows} -> {len(chunk)})")

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

    print(f"[DONE] UPSERT(COPY) 완료: {total} rows / sec={time.time()-t0:.2f}")

def direct_upsert(engine_, df: pd.DataFrame):
    if df.empty:
        print("[SKIP] df empty")
        return

    df2 = _prepare_df_for_db(df)

    PK_COLS = ["end_day", "barcode_information", "end_time", "test_time", "contents"]
    before = len(df2)
    df2 = df2.drop_duplicates(subset=PK_COLS, keep="last").copy()
    dropped = before - len(df2)
    if dropped > 0:
        print(f"[WARN] direct upsert PK duplicates dropped: {dropped} (rows {before} -> {len(df2)})")

    cols = [
        "group","barcode_information","station","remark","end_day","end_time","run_time",
        "contents","step_description","set_up_or_test_ct","value","min","max","result",
        "test_ct","test_time","file_path"
    ]

    raw = engine_.raw_connection()
    t0 = time.time()
    try:
        from psycopg2.extras import execute_values
        with raw.cursor() as cur:
            sql = f"""
            INSERT INTO {OUT_SCHEMA}.{OUT_TABLE} (
                "group", barcode_information, station, remark, end_day, end_time, run_time,
                contents, step_description, set_up_or_test_ct, value, min, max, result,
                test_ct, test_time, file_path
            ) VALUES %s
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
            """

            total = len(df2)
            for start in range(0, total, DIRECT_UPSERT_BATCH_ROWS):
                end = min(start + DIRECT_UPSERT_BATCH_ROWS, total)
                chunk = df2.iloc[start:end][cols]

                records = []
                for row in chunk.itertuples(index=False, name=None):
                    rec = []
                    for v in row:
                        rec.append(None if pd.isna(v) else v)
                    records.append(tuple(rec))

                execute_values(cur, sql, records, page_size=min(1000, len(records)))
                raw.commit()
                print(f"[OK] direct upsert batch: {start}~{end-1} ({end-start} rows)")

    finally:
        raw.close()

    print(f"[DONE] UPSERT(DIRECT) 완료: {len(df2)} rows / sec={time.time()-t0:.2f}")

def upsert_auto(engine_, df: pd.DataFrame):
    n = int(len(df))
    if n <= 0:
        print("[SKIP] df empty")
        return

    if n <= UPSERT_DIRECT_THRESHOLD_ROWS:
        print(f"[MODE] DIRECT UPSERT (rows={n} <= threshold={UPSERT_DIRECT_THRESHOLD_ROWS})")
        direct_upsert(engine_, df)
    else:
        print(f"[MODE] COPY+STAGING UPSERT (rows={n} > threshold={UPSERT_DIRECT_THRESHOLD_ROWS})")
        bulk_upsert_copy(engine_, df)

# ============================================================
# 13) MAIN (운영형, 단발 실행)
# ============================================================
def main():
    run_start = datetime.now()
    print("=" * 120)
    print(f"[RUN] START : {run_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[RUN] MODE  : DATE_MODE={DATE_MODE}, SAFETY_LOOKBACK_DAYS={SAFETY_LOOKBACK_DAYS}, MAX_DAYS_PER_RUN={MAX_DAYS_PER_RUN}")
    print("=" * 120)

    # 운영형 부트스트랩 (NO DROP)
    bootstrap_fct_database(engine)

    # 처리 날짜 범위 결정
    d_from, d_to = resolve_date_range(engine)
    print(f"[RUN] DATE_RANGE : {d_from} ~ {d_to}")

    # 소스 로드 (범위 내 없으면 종료)
    df0 = load_source(engine, d_from, d_to)
    if df0.empty:
        run_end = datetime.now()
        print("=" * 120)
        print(f"[RUN] END   : {run_end.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[RUN] SEC   : {(run_end - run_start).total_seconds():.2f} sec")
        print("=" * 120)
        print("[DONE] no data -> exit")
        return

    # ✅ file_path 기준 "이미 처리된 파일" PASS
    processed_paths = get_processed_file_paths(engine)
    df0 = filter_out_already_processed_by_file_path(df0, processed_paths)

    if df0.empty:
        run_end = datetime.now()
        print("=" * 120)
        print(f"[RUN] END   : {run_end.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"[RUN] SEC   : {(run_end - run_start).total_seconds():.2f} sec")
        print("=" * 120)
        print("[DONE] all source rows skipped by file_path -> exit")
        return

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

    if DEBUG_JOIN_COVERAGE:
        debug_join_coverage(df2, fct)

    # 2) value/min/max/result 매칭
    df3 = match_value_min_max_result_by_cycle(df2, fct)

    df_final = df3[FINAL_COLS].copy()

    if SHOW_PREVIEW:
        print(df_final.head(200))

    # 3) 업서트 자동 분기 실행
    upsert_auto(engine, df_final)

    run_end = datetime.now()
    run_seconds = (run_end - run_start).total_seconds()
    print("=" * 120)
    print(f"[RUN] END   : {run_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"[RUN] SEC   : {run_seconds:.2f} sec")
    print("=" * 120)
    print("[DONE] 전체 파이프라인 종료")

if __name__ == "__main__":
    main()
