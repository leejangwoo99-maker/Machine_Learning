# -*- coding: utf-8 -*-
"""
pd_worst_service.py

요청 반영(핵심):
- ✅ 실행 중간에 DB 서버가 끊겨도, 모든 DB I/O 구간(SELECT/DDL/UPSERT/LOG INSERT 포함)에서
  "무한 재접속(블로킹) + 재시도" 하도록 통합 적용
- ✅ 엔진 풀 최소화(상시 1개): pool_size=1, max_overflow=0
- ✅ 세션마다 work_mem 설정
- ✅ 스케줄러: 하루 2번 윈도우에서 1초 간격 반복 실행

추가(로그 모니터링):
- ✅ 스키마: k_demon_heath_check (없으면 생성)
- ✅ 테이블: e7_log (없으면 생성)
- ✅ 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- ✅ end_day, end_time, info, contents 순서로 DataFrame화하여 저장

Nuitka 안정화:
- list/dict/set comprehension 신규 추가 금지(가능한 범위 내)
- f-string 내 복잡식/inline comp 제거(가능한 범위 내)
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
    "password": "leejangwoo1!",
}

# ✅ 오늘 날짜 자동 + 없으면 최신일 fallback
AUTO_PICK_END_DAY = True
TARGET_END_DAY_TEXT = None   # AUTO_PICK_END_DAY=False일 때만 직접 넣어서 사용

TARGET_REMARK = "PD"
TEST_CONTENTS_KEY = "1.36_dark_curr_check"  # pd_cal_test_ct_summary에서 upper_outlier를 가져올 key

# 저장 테이블
TARGET_SCHEMA = "e4_predictive_maintenance"
TARGET_TABLE = "pd_worst"

# 로그 저장 테이블
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "e7_log"

# ============================
# 스케줄 윈도우(매일)
# ============================
RUN_WINDOWS = [
    ((8, 27, 0), (8, 29, 59)),
    ((20, 27, 0), (20, 29, 59)),
]
SLEEP_SECONDS = 1.0  # ✅ 1초 폴링 유지

# ============================
# DB 안정화 옵션
# ============================
DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = 5
WORK_MEM = "4MB"
STATEMENT_TIMEOUT_MS = None  # 예: 60000


# =========================
# 1) DB 연결 (상시 1개 + 무한 재시도 + 풀 최소화)
# =========================
_ENGINE = None


class EngineBox:
    """engine 참조를 함수 내부에서 교체할 수 있도록 래핑(리턴/언패킹 난잡함 방지)"""
    def __init__(self, engine=None):
        self.engine = engine


def _build_engine(cfg: Dict[str, Any]):
    user = cfg["user"]
    pw = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    db = cfg["dbname"]
    conn_str = (
        "postgresql+psycopg2://{u}:{p}@{h}:{pt}/{d}?connect_timeout={cto}"
        .format(u=user, p=pw, h=host, pt=port, d=db, cto=int(CONNECT_TIMEOUT_SEC))
    )
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,      # ✅ 상시 연결 1개
        max_overflow=0,   # ✅ overflow 금지
        pool_timeout=30,
        pool_recycle=300,
        future=True,
    )


def _apply_session_limits(conn):
    """✅ 세션마다 리소스 제한"""
    conn.execute(text("SET work_mem TO :wm"), {"wm": WORK_MEM})
    if STATEMENT_TIMEOUT_MS is not None:
        conn.execute(text("SET statement_timeout TO :st"), {"st": int(STATEMENT_TIMEOUT_MS)})


def get_engine_blocking():
    """
    ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
    ✅ 엔진 1개를 전역으로 유지/재사용
    """
    global _ENGINE
    while True:
        try:
            if _ENGINE is None:
                _ENGINE = _build_engine(DB_CONFIG)

            with _ENGINE.connect() as conn:
                _apply_session_limits(conn)
                conn.execute(text("SELECT 1"))
            return _ENGINE

        except Exception as e:
            print("[DB][RETRY] connect failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            try:
                if _ENGINE is not None:
                    _ENGINE.dispose()
            except Exception:
                pass
            _ENGINE = None
            time.sleep(DB_RETRY_INTERVAL_SEC)


def safe_engine_recover(engine):
    """
    실행 중 DB 에러 발생 시:
    - 엔진 dispose
    - 재연결 성공까지 블로킹으로 재획득
    """
    global _ENGINE
    try:
        if engine is not None:
            engine.dispose()
    except Exception:
        pass
    _ENGINE = None
    return get_engine_blocking()


def ensure_engine_alive(box: EngineBox):
    """가벼운 ping + 실패 시 블로킹 복구"""
    while True:
        try:
            if box.engine is None:
                box.engine = get_engine_blocking()

            with box.engine.connect() as conn:
                _apply_session_limits(conn)
                conn.execute(text("SELECT 1"))
            return
        except Exception as e:
            print("[DB][RETRY] ping failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            box.engine = safe_engine_recover(box.engine)
            time.sleep(DB_RETRY_INTERVAL_SEC)


def read_sql_retry(box: EngineBox, sql, params=None) -> pd.DataFrame:
    """
    ✅ 모든 SELECT/read_sql 구간에서:
    - 실패하면 엔진 복구(블로킹) 후 무한 재시도
    """
    while True:
        try:
            ensure_engine_alive(box)
            with box.engine.begin() as conn:
                _apply_session_limits(conn)
                return pd.read_sql(sql, conn, params=params)
        except Exception as e:
            print("[DB][RETRY] read_sql failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            box.engine = safe_engine_recover(box.engine)
            time.sleep(DB_RETRY_INTERVAL_SEC)


def execute_retry(box: EngineBox, sql, params=None):
    """✅ DDL/execute 구간도 동일하게 무한 재시도"""
    while True:
        try:
            ensure_engine_alive(box)
            with box.engine.begin() as conn:
                _apply_session_limits(conn)
                return conn.execute(sql, params or {})
        except Exception as e:
            print("[DB][RETRY] execute failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            box.engine = safe_engine_recover(box.engine)
            time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 1-1) 로그 유틸
# =========================
def _now_day_time_text():
    now_dt = datetime.now()
    return now_dt.strftime("%Y%m%d"), now_dt.strftime("%H:%M:%S")


def ensure_log_table_retry(box: EngineBox):
    ddl = text("""
    CREATE SCHEMA IF NOT EXISTS k_demon_heath_check;

    CREATE TABLE IF NOT EXISTS k_demon_heath_check.e7_log (
        id          bigserial PRIMARY KEY,
        end_day     text NOT NULL,
        end_time    text NOT NULL,
        info        text NOT NULL,
        contents    text,
        created_at  timestamp without time zone DEFAULT now()
    );
    """)
    execute_retry(box, ddl, None)


def log_db_retry(box: EngineBox, info: str, contents: str):
    """
    - info는 반드시 소문자 저장
    - end_day/end_time/info/contents 순서로 DataFrame화 후 저장
    - DB 실패 시 무한 재시도
    """
    info_l = str(info).strip().lower()
    if info_l == "":
        info_l = "info"

    day_s, time_s = _now_day_time_text()

    data = {
        "end_day": [day_s],
        "end_time": [time_s],
        "info": [info_l],
        "contents": [str(contents)],
    }
    df_log = pd.DataFrame(data, columns=["end_day", "end_time", "info", "contents"])

    sql_ins = text("""
    INSERT INTO k_demon_heath_check.e7_log (end_day, end_time, info, contents)
    VALUES (:end_day, :end_time, :info, :contents)
    """)

    while True:
        try:
            ensure_engine_alive(box)
            ensure_log_table_retry(box)

            rows = df_log.to_dict(orient="records")
            with box.engine.begin() as conn:
                _apply_session_limits(conn)
                conn.execute(sql_ins, rows)
            return

        except Exception as e:
            # 로그 저장 실패 자체는 stdout으로 남기고 재시도
            print("[LOG][RETRY] log_db insert failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            box.engine = safe_engine_recover(box.engine)
            time.sleep(DB_RETRY_INTERVAL_SEC)


def write_event(box: EngineBox, info: str, contents: str):
    """콘솔 + DB 로그 동시 기록"""
    info_l = str(info).strip().lower()
    msg = "[{i}] {c}".format(i=info_l.upper(), c=contents)
    print(msg, flush=True)
    try:
        log_db_retry(box, info_l, contents)
    except Exception as _:
        # 여기까지 오면 내부적으로 이미 재시도 중일 가능성이 크므로 추가 예외 억제
        pass


# =========================
# 2) 유틸
# =========================
def norm_end_day(x) -> str:
    s = str(x).strip()
    if s.isdigit() and len(s) == 8:
        dt = pd.to_datetime(s, format="%Y%m%d", errors="coerce")
        if pd.isna(dt):
            return ""
        return dt.strftime("%Y-%m-%d")
    dt = pd.to_datetime(s, errors="coerce")
    if pd.isna(dt):
        return ""
    return dt.strftime("%Y-%m-%d")


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
    combo = "{} {}".format(end_day_str, s)
    return pd.to_datetime(combo, errors="coerce")


def build_dynamic_upsert_sql(full_name: str, pk_cols: List[str], save_cols: List[str]) -> text:
    insert_cols_sql = ", ".join(save_cols)

    value_parts: List[str] = []
    for c in save_cols:
        value_parts.append(":{}".format(c))
    values_sql = ", ".join(value_parts)

    conflict_cols = "barcode_information, end_day, end_time, test_contents, okng_seq"

    set_cols: List[str] = []
    for c in save_cols:
        if c not in pk_cols:
            set_cols.append(c)

    set_parts: List[str] = []
    for c in set_cols:
        set_parts.append("{} = EXCLUDED.{}".format(c, c))
    set_parts.append("updated_at = now()")
    set_sql = ", ".join(set_parts)

    sql_txt = """
    INSERT INTO {full} ({cols})
    VALUES ({vals})
    ON CONFLICT ({conflict})
    DO UPDATE SET {set_sql};
    """.format(full=full_name, cols=insert_cols_sql, vals=values_sql, conflict=conflict_cols, set_sql=set_sql)

    return text(sql_txt)


def _in_window(now_dt: datetime, start_hms, end_hms) -> bool:
    sh, sm, ss = start_hms
    eh, em, es = end_hms
    start_sec = sh * 3600 + sm * 60 + ss
    end_sec = eh * 3600 + em * 60 + es
    now_sec = now_dt.hour * 3600 + now_dt.minute * 60 + now_dt.second
    return start_sec <= now_sec <= end_sec


def is_run_time(now_dt: Optional[datetime] = None) -> bool:
    if now_dt is None:
        now_dt = datetime.now()
    for w in RUN_WINDOWS:
        if _in_window(now_dt, w[0], w[1]):
            return True
    return False


def fetch_existing_cols_retry(box: EngineBox, schema: str, table: str) -> List[str]:
    SQL_EXISTING_COLS = text("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema
      AND table_name   = :table
    """)
    df_cols = read_sql_retry(box, SQL_EXISTING_COLS, params={"schema": schema, "table": table})
    cols: List[str] = []
    if "column_name" in df_cols.columns:
        for v in df_cols["column_name"].tolist():
            if isinstance(v, str):
                cols.append(v)
    return cols


# =========================
# 3) 1회 실행 로직
# =========================
def run_once(box: EngineBox):
    run_start_dt = datetime.now()
    run_start_perf = time.perf_counter()
    write_event(box, "info", "run start_ts={}".format(run_start_dt.strftime("%Y-%m-%d %H:%M:%S")))

    payload_for_save = None

    # ✅ end_day 자동 선택(오늘 → 없으면 최신)
    if AUTO_PICK_END_DAY:
        today_text = datetime.now().strftime("%Y%m%d")

        SQL_CHECK_TODAY = text("""
        SELECT 1
        FROM a2_fct_table.fct_table
        WHERE end_day = :today
          AND remark  = :remark
        LIMIT 1
        """)
        chk = read_sql_retry(box, SQL_CHECK_TODAY, params={"today": today_text, "remark": TARGET_REMARK})

        if not chk.empty:
            use_end_day_text = today_text
            write_event(box, "info", "auto using today end_day={}".format(use_end_day_text))
        else:
            SQL_LATEST = text("""
            SELECT MAX(end_day) AS max_day
            FROM a2_fct_table.fct_table
            WHERE remark = :remark
            """)
            latest = read_sql_retry(box, SQL_LATEST, params={"remark": TARGET_REMARK})
            if latest.empty or pd.isna(latest.iloc[0]["max_day"]):
                raise RuntimeError("[ERROR] a2_fct_table.fct_table 에 유효한 end_day 가 없습니다.")
            use_end_day_text = str(latest.iloc[0]["max_day"]).strip()
            write_event(box, "info", "auto today not found -> fallback end_day={}".format(use_end_day_text))
    else:
        use_end_day_text = TARGET_END_DAY_TEXT
        write_event(box, "info", "manual end_day={}".format(use_end_day_text))

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
    b = read_sql_retry(box, SQL_BOUNDARY, params={"tc": TEST_CONTENTS_KEY})
    if b.empty:
        raise RuntimeError("[ERROR] pd_cal_test_ct_summary에서 {} upper_outlier를 찾지 못했습니다.".format(TEST_CONTENTS_KEY))

    boundary_run_time = float(b.loc[0, "upper_outlier"])
    boundary_src_day = str(b.loc[0, "end_day"])
    write_event(box, "info", "boundary_run_time={} source_end_day={}".format(boundary_run_time, boundary_src_day))

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
    df = read_sql_retry(box, SQL_DATA, params={"end_day": use_end_day_text, "remark": TARGET_REMARK})
    if df.empty:
        raise RuntimeError("[ERROR] a2_fct_table.fct_table 조회 결과가 비어있습니다. end_day={}, remark={}".format(use_end_day_text, TARGET_REMARK))

    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    df["boundary_run_time"] = boundary_run_time

    df = df[df["run_time"] > df["boundary_run_time"]].copy()
    if df.empty:
        raise RuntimeError("[ERROR] boundary_run_time 초과 데이터가 없습니다. boundary_run_time={}".format(boundary_run_time))

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

    write_event(box, "info", "top5 boundary={} cut={:.2f} rows={}".format(boundary_run_time, float(cut), len(df_top)))
    if len(df_top) == 0:
        raise RuntimeError("[ERROR] df_top이 비어있습니다. (run_time TOP5% 결과 없음)")

    # -----------------------------
    # Cell X1) fct_detail 조회 + meta merge
    # -----------------------------
    barcodes = df_top["barcode_information"].dropna().astype(str).drop_duplicates().tolist()
    write_event(box, "info", "top barcodes={}".format(len(barcodes)))

    target_end_day_date = pd.to_datetime(use_end_day_text, format="%Y%m%d", errors="raise").strftime("%Y-%m-%d")

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
    df_detail = read_sql_retry(box, SQL_FCT_DETAIL, params={"end_day": target_end_day_date, "remark": TARGET_REMARK, "barcodes": barcodes})
    if df_detail.empty:
        raise RuntimeError("[ERROR] c1_fct_detail.fct_detail 조회 결과가 비어있습니다. end_day={}, remark={}".format(target_end_day_date, TARGET_REMARK))

    df_meta = df_top[["barcode_information", "station", "run_time", "boundary_run_time"]].copy()
    df_meta["barcode_information"] = df_meta["barcode_information"].astype(str)
    df_meta = df_meta.drop_duplicates("barcode_information")

    df_detail["barcode_information"] = df_detail["barcode_information"].astype(str)
    df_detail = df_detail.merge(df_meta, on="barcode_information", how="left")

    if "station" not in df_detail.columns:
        df_detail["station"] = pd.NA
    if "run_time" not in df_detail.columns:
        df_detail["run_time"] = pd.NA
    if "boundary_run_time" not in df_detail.columns:
        df_detail["boundary_run_time"] = boundary_run_time

    df_detail = df_detail[[
        "barcode_information", "station", "remark", "end_day", "end_time",
        "run_time", "boundary_run_time", "contents", "test_ct", "test_time"
    ]].copy()

    write_event(box, "info", "df_detail rows={}".format(len(df_detail)))

    # -----------------------------
    # Cell X2) group 생성 + 제외 규칙 적용
    # -----------------------------
    df2 = df_detail.copy()
    df2["barcode_information"] = df2["barcode_information"].astype(str)
    df2["station"] = df2["station"].astype(str)
    df2["remark"] = df2["remark"].astype(str)
    df2["end_day"] = pd.to_datetime(df2["end_day"], errors="coerce").dt.strftime("%Y-%m-%d")
    df2["end_time"] = df2["end_time"].astype(str)

    def _mk_ts(row):
        return parse_test_time_to_ts(row["end_day"], row["test_time"])
    df2["_test_ts"] = df2.apply(_mk_ts, axis=1)

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

    write_event(box, "info", "df2 rows={} groups={}".format(len(df2), int(df2["group"].nunique())))

    # -----------------------------
    # Cell X3) from_to_test_ct (OK/NG만)
    # -----------------------------
    df3 = df2.copy()
    df3["_base_ts"] = df3.groupby("group")["_test_ts"].transform("first")

    is_okng = df3["contents"].astype(str).isin(["테스트 결과 : OK", "테스트 결과 : NG"])
    df3["from_to_test_ct"] = pd.NA

    mask = is_okng & df3["_test_ts"].notna() & df3["_base_ts"].notna()
    df3.loc[mask, "from_to_test_ct"] = (df3.loc[mask, "_test_ts"] - df3.loc[mask, "_base_ts"]).dt.total_seconds()

    write_event(box, "info", "from_to_test_ct filled={}".format(int(df3["from_to_test_ct"].notna().sum())))

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

    write_event(box, "info", "df_final rows={} groups={}".format(len(df_final), int(df_final["group"].nunique())))

    # -----------------------------
    # Cell X5) df15
    # -----------------------------
    df15 = df_final.dropna(subset=["test_contents"]).copy()
    df15 = df15[[
        "group", "barcode_information", "station", "remark", "end_day", "end_time",
        "run_time", "boundary_run_time", "test_time", "test_contents", "from_to_test_ct", "okng_seq"
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
    tmp_cols = read_sql_retry(box, SQL_PROBLEM_COLS, params=None)
    present_problem_cols: List[str] = []
    if "column_name" in tmp_cols.columns:
        for v in tmp_cols["column_name"].tolist():
            if isinstance(v, str):
                present_problem_cols.append(v.strip())

    want_problem_cols = ["problem1", "problem2", "problem3", "problem4"]
    use_problem_cols: List[str] = []
    for c in want_problem_cols:
        if c in present_problem_cols:
            use_problem_cols.append(c)

    SQL_HAS_DAY = text("""
    SELECT COUNT(*) AS n
    FROM e4_predictive_maintenance.pd_cal_test_ct_summary
    WHERE end_day = :end_day
    """)
    ndf = read_sql_retry(box, SQL_HAS_DAY, params={"end_day": target_end_day_x6})
    try:
        n = int(ndf.iloc[0]["n"])
    except Exception:
        n = 0

    if n > 0:
        summary_day = target_end_day_x6
    else:
        SQL_LATEST_DAY = text("""
        SELECT MAX(end_day) AS max_day
        FROM e4_predictive_maintenance.pd_cal_test_ct_summary
        """)
        latest_day_df = read_sql_retry(box, SQL_LATEST_DAY, params=None)
        summary_day = latest_day_df.iloc[0]["max_day"]
        if pd.isna(summary_day):
            raise RuntimeError("[ERROR] pd_cal_test_ct_summary 테이블이 비어있습니다.")
        summary_day = str(summary_day).strip()

    select_cols = ["test_contents", "upper_outlier"]
    for c in use_problem_cols:
        select_cols.append(c)

    SQL_BOUNDARY2 = text("""
    SELECT {cols}
    FROM e4_predictive_maintenance.pd_cal_test_ct_summary
    WHERE end_day = :end_day
    """.format(cols=", ".join(select_cols)))

    df_boundary = read_sql_retry(box, SQL_BOUNDARY2, params={"end_day": summary_day})
    df_boundary["test_contents"] = df_boundary["test_contents"].astype(str).str.strip()
    df_boundary["upper_outlier"] = pd.to_numeric(df_boundary["upper_outlier"], errors="coerce")
    for c in use_problem_cols:
        df_boundary[c] = df_boundary[c].astype(str).str.strip()

    df15_out = df15.copy()
    df15_out["test_contents"] = df15_out["test_contents"].astype(str).str.strip()

    join_cols = ["test_contents", "upper_outlier"]
    for c in use_problem_cols:
        join_cols.append(c)

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
        "group", "barcode_information", "station", "remark", "end_day", "end_time", "run_time", "boundary_run_time",
        "okng_seq", "test_contents", "test_time", "from_to_test_ct",
        "boundary_test_ct", "diff_ct", "problem1", "problem2", "problem3", "problem4"
    ]].copy()

    # -----------------------------
    # Cell X8) diff_ct Worst TOP 5%
    # -----------------------------
    dfw = df15_out2.copy()
    dfw["diff_ct"] = pd.to_numeric(dfw["diff_ct"], errors="coerce")
    dfw["run_time"] = pd.to_numeric(dfw["run_time"], errors="coerce")
    dfw["okng_seq"] = pd.to_numeric(dfw["okng_seq"], errors="coerce")
    dfw = dfw.dropna(subset=["barcode_information", "diff_ct", "okng_seq"]).copy()

    n_all = len(dfw)
    top_n = int(math.ceil(n_all * 0.05))
    if top_n < 1:
        top_n = 1

    df_top_raw = dfw.sort_values("diff_ct", ascending=False).head(top_n).copy()

    df_top_raw["min_okng"] = df_top_raw.groupby("barcode_information")["okng_seq"].transform("min")
    df_spike = (
        df_top_raw[df_top_raw["okng_seq"] == df_top_raw["min_okng"]]
        .sort_values(["barcode_information", "diff_ct", "run_time"], ascending=[True, False, False])
        .drop_duplicates("barcode_information")
        .sort_values("run_time", ascending=False)
        .reset_index(drop=True)
    )

    write_event(box, "info", "diff_ct worst all={} top_raw={} final={}".format(n_all, len(df_top_raw), len(df_spike)))

    # -----------------------------
    # Cell X9) file_path 매칭
    # -----------------------------
    key_df = df_spike[["barcode_information", "end_day", "end_time"]].copy()
    key_df["barcode_information"] = key_df["barcode_information"].astype(str).str.strip()
    key_df["end_day"] = key_df["end_day"].apply(norm_end_day)
    key_df["end_time"] = key_df["end_time"].apply(norm_end_time)
    key_df = key_df.dropna(subset=["barcode_information", "end_day", "end_time"]).copy()

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
    df_fp = read_sql_retry(box, SQL_FILEPATH, params={"barcodes": barcodes2, "days": days2})

    if not df_fp.empty:
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
    else:
        df_fp = pd.DataFrame(columns=["barcode_information", "end_day", "end_time", "file_path"])

    df_spike_fp = df_spike.copy()
    df_spike_fp["barcode_information"] = df_spike_fp["barcode_information"].astype(str).str.strip()
    df_spike_fp["end_day"] = df_spike_fp["end_day"].apply(norm_end_day)
    df_spike_fp["end_time"] = df_spike_fp["end_time"].apply(norm_end_time)

    df_spike_fp = df_spike_fp.merge(
        df_fp[["barcode_information", "end_day", "end_time", "file_path"]],
        on=["barcode_information", "end_day", "end_time"],
        how="left"
    )
    write_event(box, "info", "file_path filled={} rows={}".format(int(df_spike_fp["file_path"].notna().sum()), len(df_spike_fp)))

    # -----------------------------
    # Cell X10) DB 저장 (DDL + 동적 UPSERT + 실행시간 컬럼)
    # -----------------------------
    FULL_NAME = "{}.{}".format(TARGET_SCHEMA, TARGET_TABLE)
    df_save = df_spike_fp.copy()

    if "group" in df_save.columns and "group_no" not in df_save.columns:
        df_save["group_no"] = pd.to_numeric(df_save["group"], errors="coerce").astype("Int64")
    elif "group_no" not in df_save.columns:
        df_save["group_no"] = pd.NA

    df_save["run_start_ts"] = run_start_dt
    df_save["run_end_ts"] = pd.NaT
    df_save["run_seconds"] = pd.NA

    pk_cols = ["barcode_information", "end_day", "end_time", "test_contents", "okng_seq"]

    df_save["barcode_information"] = df_save["barcode_information"].astype(str).str.strip()
    df_save["end_day"] = pd.to_datetime(df_save["end_day"], errors="coerce").dt.date
    df_save["end_time"] = df_save["end_time"].astype(str).str.strip()
    df_save["test_contents"] = df_save["test_contents"].astype(str).str.strip()
    df_save["okng_seq"] = pd.to_numeric(df_save["okng_seq"], errors="coerce").astype("Int64")

    for c in ["run_time", "boundary_run_time", "from_to_test_ct", "boundary_test_ct", "diff_ct", "run_seconds"]:
        if c in df_save.columns:
            df_save[c] = pd.to_numeric(df_save[c], errors="coerce")

    for c in ["problem1", "problem2", "problem3", "problem4"]:
        if c not in df_save.columns:
            df_save[c] = pd.NA
    if "file_path" not in df_save.columns:
        df_save["file_path"] = pd.NA

    DDL = text("""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {full} (
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
    """.format(schema=TARGET_SCHEMA, full=FULL_NAME))

    payload_for_save = {
        "DDL": DDL,
        "pk_cols": pk_cols,
        "df_save": df_save,
        "full_name": FULL_NAME,
        "schema": TARGET_SCHEMA,
        "table": TARGET_TABLE,
    }

    try:
        return
    finally:
        run_end_dt = datetime.now()
        run_seconds = round(time.perf_counter() - run_start_perf, 3)

        write_event(box, "info", "run end_ts={}".format(run_end_dt.strftime("%Y-%m-%d %H:%M:%S")))
        write_event(box, "info", "run run_seconds={:.3f}".format(float(run_seconds)))

        if payload_for_save is None:
            write_event(box, "error", "payload_for_save is none; skip upsert")
            return

        df_save2 = payload_for_save["df_save"].copy()
        df_save2["run_end_ts"] = run_end_dt
        df_save2["run_seconds"] = float(run_seconds)

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

        # ✅ 저장 구간: DB 실패 시 복구/재시도(블로킹)
        while True:
            try:
                ensure_engine_alive(box)

                with box.engine.begin() as conn:
                    _apply_session_limits(conn)
                    conn.execute(payload_for_save["DDL"])

                    existing_cols_list = fetch_existing_cols_retry(box, payload_for_save["schema"], payload_for_save["table"])
                    existing_cols = set(existing_cols_list)

                    save_cols: List[str] = []
                    for c in candidate_cols:
                        if (c in df_save2.columns) and (c in existing_cols):
                            save_cols.append(c)

                    pk_cols2 = payload_for_save["pk_cols"]
                    for c in pk_cols2:
                        if c not in existing_cols:
                            raise RuntimeError("[ERROR] DB 테이블에 PK 컬럼 '{}' 가 없습니다. 테이블 스키마를 확인하세요.".format(c))
                        if c not in save_cols:
                            save_cols.append(c)

                    UPSERT_SQL = build_dynamic_upsert_sql(payload_for_save["full_name"], pk_cols2, save_cols)

                    rows = df_save2[save_cols].to_dict(orient="records")
                    for r in rows:
                        for k, v in list(r.items()):
                            if pd.isna(v):
                                r[k] = None

                    conn.execute(UPSERT_SQL, rows)

                write_event(box, "info", "saved {} rows={} used_cols={}".format(
                    payload_for_save["full_name"], len(df_save2), len(save_cols)
                ))
                break

            except Exception as e:
                write_event(box, "down", "upsert failed retry: {t}: {m}".format(
                    t=type(e).__name__, m=repr(e)
                ))
                box.engine = safe_engine_recover(box.engine)
                time.sleep(DB_RETRY_INTERVAL_SEC)


# =========================
# 4) 스케줄러: 무한 루프(1초 폴링) + 하루 2회 윈도우 실행
# =========================
def scheduler_loop():
    box = EngineBox()
    box.engine = get_engine_blocking()

    # 로그 테이블 사전 보장
    ensure_log_table_retry(box)

    write_event(
        box, "info",
        "engine ready blocking ensured pool_size=1 max_overflow=0 work_mem={}".format(WORK_MEM)
    )

    last_state = None  # "RUN" / "WAIT"

    while True:
        now_dt = datetime.now()

        if is_run_time(now_dt):
            if last_state != "RUN":
                write_event(box, "info", "sched enter run window @ {}".format(now_dt.strftime("%Y-%m-%d %H:%M:%S")))
                last_state = "RUN"

            try:
                ensure_engine_alive(box)
                run_once(box)

            except Exception as e:
                write_event(box, "error", "run_once failed: {}".format(repr(e)))
                box.engine = safe_engine_recover(box.engine)

            time.sleep(SLEEP_SECONDS)

        else:
            if last_state != "WAIT":
                write_event(box, "sleep", "sched wait outside windows @ {}".format(now_dt.strftime("%Y-%m-%d %H:%M:%S")))
                last_state = "WAIT"
            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    scheduler_loop()
