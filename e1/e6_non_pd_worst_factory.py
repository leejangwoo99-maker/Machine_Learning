# -*- coding: utf-8 -*-
"""
non_pd_worst.py

추가 반영
- 실행 시작/종료 시간 기록
- 총 실행시간(초) 계산
- DB 저장 테이블에 run_start_ts, run_end_ts, run_seconds 컬럼 추가 및 UPSERT 저장

추가 조건(스케줄러)
1) 무한 루프 기능 : 1초마다 재실행
2) 매일 하루 2번 실행
   - 08:27:00 시작 ~ 08:29:59 종료 (윈도우 안에서는 1초 간격 반복 실행)
   - 20:27:00 시작 ~ 20:29:59 종료 (윈도우 안에서는 1초 간격 반복 실행)

Nuitka 안정화(중요)
- list/dict/set comprehension 최대한 제거
- f-string 내 복잡식/inline comp 제거

[추가 통합 반영]
1) TARGET_END_DAY를 "현재 날짜(YYYYMMDD)" 기준으로 자동 선택
2) 만약 현재 날짜 데이터가 없으면, remark 기준 가장 최신 end_day로 fallback

[추가 통합 반영 2]
- updated_at은 "이 스크립트가 DB에 UPSERT를 실행한 시각(= DB 서버 now())" 로 저장
  * INSERT 시에도 updated_at=now()
  * UPDATE 시에도 updated_at=now()

✅ 이번 요청 추가 반영(핵심)
- ✅ 실행 중간에 DB 서버 연결이 끊겨도,
  - SELECT/READ/DDL/UPSERT 전 구간에서 "연결 복구까지 무한 재시도"
  - 엔진 dispose/rebuild 후 동일 작업을 반복 시도

- ✅ 멀티프로세스 = 1개 (유지)
- ✅ 무한 루프 인터벌 5초: 반영하지 않음(기존대로 1초 루프)
- ✅ DB 서버 접속 실패 시 무한 재시도(연결 성공할 때까지 블로킹)
- ✅ 백엔드별 상시 연결 1개 고정(풀 최소화)
  * SQLAlchemy engine: pool_size=1, max_overflow=0
  * 엔진을 전역 1개로 생성/유지(재사용), 실패 시 dispose 후 재획득(블로킹)
- ✅ work_mem 폭증 방지: 연결(세션)마다 SET work_mem 적용

주의:
- Nuitka 안정화 요구에 따라, 새로 추가하는 코드도 list/dict comp를 피했습니다.
"""

import os
import time
import urllib.parse
from typing import Optional, Dict, Any, List
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError, DBAPIError, SQLAlchemyError


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

# ✅ 오늘 날짜 자동 + 없으면 최신일 fallback
AUTO_PICK_END_DAY = True
TARGET_END_DAY = None  # AUTO_PICK_END_DAY=False일 때만 직접 넣어서 사용

TARGET_REMARK = "Non-PD"  # a2_fct_table.fct_table 의 remark
TEST_CONTENTS_KEY = "1.32_dark_curr_check"  # boundary source

TARGET_SCHEMA = "e4_predictive_maintenance"
TARGET_TABLE = "non_pd_worst"

SAVE_HTML_REPORT = True
REPORT_DIR = "./report_non_pd_worst"
MAX_PREVIEW_ROWS = 300

# ============================
# 스케줄 윈도우(매일)
# ============================
RUN_WINDOWS = [
    ((8, 27, 0), (8, 29, 59)),
    ((20, 27, 0), (20, 29, 59)),
]
SLEEP_SECONDS = 1.0  # 1초마다 폴링 (✅ 기존대로 유지)

# ============================
# DB 안정화 옵션
# ============================
DB_RETRY_INTERVAL_SEC = 5
CONNECT_TIMEOUT_SEC = 5
WORK_MEM = "4MB"      # 필요 시 2MB 등으로 더 낮출 수 있음
STATEMENT_TIMEOUT_MS = None  # 예: 60000 (1분). 미사용 시 None


# ============================
# 1) DB 연결 (상시 1개 + 무한 재시도 + 풀 최소화)
# ============================

_ENGINE = None


def _is_conn_error(e: Exception) -> bool:
    """
    연결 끊김/네트워크/DB 재시작 계열 오류를 넓게 감지.
    """
    if isinstance(e, (OperationalError, DBAPIError)):
        return True
    msg = (str(e) or "").lower()
    keys = [
        "server closed the connection",
        "terminating connection",
        "connection not open",
        "could not connect",
        "connection refused",
        "connection timed out",
        "timeout expired",
        "ssl connection has been closed",
        "broken pipe",
        "connection reset",
        "network is unreachable",
        "no route to host",
        "admin shutdown",
        "the database system is starting up",
        "the database system is in recovery mode",
    ]
    for k in keys:
        if k in msg:
            return True
    return False


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

    # ✅ 풀 최소화: 상시 1개(overflow 0)
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=1,
        max_overflow=0,
        pool_timeout=30,
        pool_recycle=300,
        future=True,
    )


def _dispose_engine():
    global _ENGINE
    try:
        if _ENGINE is not None:
            _ENGINE.dispose()
    except Exception:
        pass
    _ENGINE = None


def _apply_session_limits(conn):
    """
    ✅ work_mem 폭증 방지
    - 트랜잭션/세션 시작마다 work_mem 설정
    - (옵션) statement_timeout도 필요하면 적용
    """
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
            _dispose_engine()
            time.sleep(DB_RETRY_INTERVAL_SEC)


def safe_engine_recover(_engine_any=None):
    """
    실행 중 DB 에러 발생 시:
    - 전역 엔진 dispose
    - 재연결 성공까지 블로킹으로 재획득
    """
    _dispose_engine()
    return get_engine_blocking()


def read_sql_blocking(sql, params=None):
    """
    ✅ read_sql 전 구간 무한 재시도(연결 복구까지 블로킹)
    - 내부에서 전역 엔진을 사용(항상 최신 엔진)
    """
    while True:
        eng = get_engine_blocking()
        try:
            with eng.begin() as conn:
                _apply_session_limits(conn)
                return pd.read_sql(sql, conn, params=params)
        except Exception as e:
            print("[DB][RETRY] read_sql failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            if _is_conn_error(e):
                safe_engine_recover(eng)
            time.sleep(DB_RETRY_INTERVAL_SEC)


def execute_blocking(sql, params=None):
    """
    ✅ execute(DDL/UPSERT/SELECT execute) 전 구간 무한 재시도(연결 복구까지 블로킹)
    """
    while True:
        eng = get_engine_blocking()
        try:
            with eng.begin() as conn:
                _apply_session_limits(conn)
                return conn.execute(sql, params or {})
        except Exception as e:
            print("[DB][RETRY] execute failed: {t}: {m}".format(
                t=type(e).__name__, m=repr(e)
            ), flush=True)
            if _is_conn_error(e):
                safe_engine_recover(eng)
            time.sleep(DB_RETRY_INTERVAL_SEC)


# (기존 함수명 호환 유지: 외부 호출이 있으면 그대로 동작)
def read_sql_safe(engine, sql, params=None):
    _ = engine  # 엔진 인자는 호환용(실제로는 전역 엔진 사용)
    return read_sql_blocking(sql, params=params)


def execute_safe(engine, sql, params=None):
    _ = engine
    return execute_blocking(sql, params=params)


# ============================
# 2) 유틸
# ============================

def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


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


def df_to_html(df: pd.DataFrame, title: str, out_path: str, n: int = 300):
    view = df.head(n).copy()
    pd.set_option("display.max_columns", None)
    html_table = view.style.to_html()
    html = """
    <html>
    <head>
      <meta charset="utf-8"/>
      <title>{t}</title>
    </head>
    <body>
      <h3>{t}</h3>
      <div style="border:1px solid #ddd; padding:8px;">
        <div style="overflow:auto; width:100%; max-height:720px;">
          {tbl}
        </div>
      </div>
    </body>
    </html>
    """.format(t=title, tbl=html_table)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)


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
    if s == "" or s.lower() == "none":
        return None

    # 숫자/float 문자열 보정
    if s.replace(".", "", 1).isdigit():
        try:
            s = str(int(float(s)))
        except Exception:
            pass

    if s.isdigit() and len(s) < 6:
        s = s.zfill(6)
    return s


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


def build_test_ts_series(end_day_series: pd.Series, test_time_series: pd.Series) -> pd.Series:
    """
    Nuitka listcomp 크래시 회피:
    - list comprehension 대신 pandas 기반으로 _test_ts 생성
    - end_day: 'YYYY-MM-DD'
    - test_time: 'HHMMSS' 형태(혹은 숫자/float 문자열) -> norm_end_time로 정규화
    """
    if end_day_series is None or test_time_series is None:
        return pd.Series([], dtype="datetime64[ns]")

    ed = end_day_series.astype(str)
    tt = test_time_series.apply(norm_end_time)
    tt2 = tt.astype("string")
    combo = ed + " " + tt2
    ts = pd.to_datetime(combo, errors="coerce")
    return ts


def get_present_problem_cols(engine) -> List[str]:
    _ = engine
    SQL_PROBLEM_COLS = text("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'e4_predictive_maintenance'
      AND table_name   = 'non_pd_cal_test_ct_summary'
      AND column_name IN ('problem1','problem2','problem3','problem4')
    ORDER BY column_name
    """)
    tmp = read_sql_blocking(SQL_PROBLEM_COLS, params=None)
    cols: List[str] = []
    if "column_name" in tmp.columns:
        for v in tmp["column_name"].tolist():
            if isinstance(v, str):
                cols.append(v.strip())
    return cols


def table_has_column(engine, schema: str, table: str, column: str) -> bool:
    _ = engine
    SQL = text("""
    SELECT COUNT(*) AS n
    FROM information_schema.columns
    WHERE table_schema=:schema
      AND table_name=:table
      AND column_name=:col
    """)
    df = read_sql_blocking(SQL, params={"schema": schema, "table": table, "col": column})
    if df.empty:
        return False
    try:
        return int(df.iloc[0]["n"]) > 0
    except Exception:
        return False


def fetch_existing_cols(conn, schema: str, table: str) -> List[str]:
    SQL_EXISTING_COLS = text("""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = :schema
      AND table_name   = :table
    """)
    df_cols = pd.read_sql(SQL_EXISTING_COLS, conn, params={"schema": schema, "table": table})
    cols: List[str] = []
    if "column_name" in df_cols.columns:
        for v in df_cols["column_name"].tolist():
            if isinstance(v, str):
                cols.append(v)
    return cols


def filter_cols_excluding(cols: List[str], exclude: str) -> List[str]:
    """Nuitka listcomp 회피용: cols에서 exclude를 뺀 리스트 생성"""
    out: List[str] = []
    for c in cols:
        if c != exclude:
            out.append(c)
    return out


# ============================
# 3) 1회 실행 로직
# ============================

def run_once(engine):
    _ = engine  # 호환용(실제로는 전역 엔진 사용)
    run_start_dt = datetime.now()
    run_start_perf = time.perf_counter()
    print("[RUN] start_ts={}".format(run_start_dt.strftime("%Y-%m-%d %H:%M:%S")), flush=True)

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
        chk = read_sql_blocking(SQL_CHECK_TODAY, params={
            "today": today_text,
            "remark": TARGET_REMARK
        })

        if not chk.empty:
            use_end_day = today_text
            print("[AUTO] Using TODAY end_day = {}".format(use_end_day), flush=True)
        else:
            SQL_LATEST = text("""
            SELECT MAX(end_day) AS max_day
            FROM a2_fct_table.fct_table
            WHERE remark = :remark
            """)
            latest = read_sql_blocking(SQL_LATEST, params={"remark": TARGET_REMARK})
            if latest.empty or pd.isna(latest.iloc[0]["max_day"]):
                raise RuntimeError("[ERROR] a2_fct_table.fct_table 에 유효한 end_day 가 없습니다.")
            use_end_day = str(latest.iloc[0]["max_day"]).strip()
            print("[AUTO] TODAY not found → fallback to latest end_day = {}".format(use_end_day), flush=True)
    else:
        use_end_day = TARGET_END_DAY

    if SAVE_HTML_REPORT:
        ensure_dir(REPORT_DIR)

    payload_for_save = None

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
        b = read_sql_blocking(SQL_BOUNDARY, params={"tc": TEST_CONTENTS_KEY})
        if b.empty:
            raise RuntimeError("[ERROR] non_pd_cal_test_ct_summary에서 {} upper_outlier를 찾지 못했습니다.".format(TEST_CONTENTS_KEY))

        boundary_run_time = float(b.loc[0, "upper_outlier"])
        boundary_src_day = str(b.loc[0, "end_day"])
        print("[OK] boundary_run_time={} (source end_day={})".format(boundary_run_time, boundary_src_day), flush=True)

        # --------------------------------
        # Cell B) run_time TOP 5%
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
        df = read_sql_blocking(SQL_DATA, params={"end_day": use_end_day, "remark": TARGET_REMARK})
        if df.empty:
            raise RuntimeError("[ERROR] a2_fct_table.fct_table 조회 결과가 비어있습니다. end_day={}, remark={}".format(use_end_day, TARGET_REMARK))

        df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
        df["boundary_run_time"] = boundary_run_time
        df = df[df["run_time"] > df["boundary_run_time"]].copy()

        if df.empty:
            raise RuntimeError("[ERROR] boundary_run_time 초과 데이터가 없습니다. boundary_run_time={}".format(boundary_run_time))

        # barcode 1행 축약
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

        df_top = df_top[
            ["barcode_information", "remark", "station", "end_day", "end_time", "boundary_run_time", "run_time"]
        ].copy()

        print("[OK] boundary={} / TOP5% cut={:.2f} / rows={}".format(boundary_run_time, float(cut), len(df_top)), flush=True)

        if SAVE_HTML_REPORT:
            out = os.path.join(REPORT_DIR, "B_df_top_run_time_top5.html")
            df_to_html(_reorder_run_time_after_end_time(df_top), "Cell B: df_top (run_time TOP 5%)", out, n=MAX_PREVIEW_ROWS)

        if len(df_top) == 0:
            raise RuntimeError("[ERROR] df_top이 비어있습니다. (run_time TOP5% 결과 없음)")

        # --------------------------------
        # Cell X1) fct_detail 조회 + meta merge
        # --------------------------------
        # (Nuitka 안정화: listcomp 최소화는 유지하되, 새로는 만들지 않음)
        barcodes = df_top["barcode_information"].dropna().astype(str).drop_duplicates().tolist()
        print("[OK] Top barcodes = {}".format(len(barcodes)), flush=True)

        TARGET_END_DAY_TEXT = str(df_top["end_day"].iloc[0]).strip()  # YYYYMMDD
        TARGET_END_DAY_DATE = pd.to_datetime(TARGET_END_DAY_TEXT, format="%Y%m%d", errors="raise").strftime("%Y-%m-%d")
        TARGET_REMARK2 = str(df_top["remark"].iloc[0]).strip() if "remark" in df_top.columns else TARGET_REMARK
        print("[OK] TARGET_END_DAY_TEXT={} / TARGET_END_DAY_DATE={} / REMARK={}".format(
            TARGET_END_DAY_TEXT, TARGET_END_DAY_DATE, TARGET_REMARK2
        ), flush=True)

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
        df_detail = read_sql_blocking(
            SQL_FCT_DETAIL,
            params={"end_day": TARGET_END_DAY_DATE, "remark": TARGET_REMARK2, "barcodes": barcodes}
        )
        if df_detail.empty:
            raise RuntimeError("[ERROR] c1_fct_detail.fct_detail 조회 결과가 비어있습니다.")

        df_meta = df_top[["barcode_information", "station", "run_time", "boundary_run_time"]].copy()
        df_meta["barcode_information"] = df_meta["barcode_information"].astype(str)
        df_meta["station"] = df_meta["station"].astype(str)
        df_meta["run_time"] = pd.to_numeric(df_meta["run_time"], errors="coerce")
        df_meta["boundary_run_time"] = pd.to_numeric(df_meta["boundary_run_time"], errors="coerce")
        df_meta = df_meta.drop_duplicates("barcode_information")

        df_detail["barcode_information"] = df_detail["barcode_information"].astype(str)
        df_detail = df_detail.merge(df_meta, on="barcode_information", how="left")

        # 컬럼 방어
        if "station" not in df_detail.columns:
            df_detail["station"] = pd.NA
        if "run_time" not in df_detail.columns:
            df_detail["run_time"] = pd.NA
        if "boundary_run_time" not in df_detail.columns:
            df_detail["boundary_run_time"] = boundary_run_time

        df_detail = df_detail[
            ["barcode_information", "station", "remark", "end_day", "end_time", "run_time", "boundary_run_time",
             "contents", "test_ct", "test_time"]
        ].copy()

        print("[OK] df_detail rows={}".format(len(df_detail)), flush=True)

        if SAVE_HTML_REPORT:
            out = os.path.join(REPORT_DIR, "X1_df_detail.html")
            df_to_html(_reorder_run_time_after_end_time(df_detail), "Cell X1: df_detail", out, n=MAX_PREVIEW_ROWS)

        # --------------------------------
        # Cell X2) group 생성 + 제외 규칙
        # --------------------------------
        df2 = df_detail.copy()
        df2["barcode_information"] = df2["barcode_information"].astype(str)
        df2["station"] = df2["station"].astype(str)
        df2["remark"] = df2["remark"].astype(str)
        df2["end_day"] = pd.to_datetime(df2["end_day"], errors="coerce").dt.strftime("%Y-%m-%d")
        df2["end_time"] = df2["end_time"].astype(str)

        df2["_test_ts"] = build_test_ts_series(df2["end_day"], df2["test_time"])

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

        print("[OK] df2 rows={} / groups={}".format(len(df2), int(df2["group"].nunique())), flush=True)

        if SAVE_HTML_REPORT:
            out = os.path.join(REPORT_DIR, "X2_df2.html")
            df_to_html(_reorder_run_time_after_end_time(df2), "Cell X2: df2", out, n=MAX_PREVIEW_ROWS)

        # --------------------------------
        # Cell X3) from_to_test_ct (OK/NG만)
        # --------------------------------
        df3 = df2.copy()
        df3["_base_ts"] = df3.groupby("group")["_test_ts"].transform("first")

        is_okng = df3["contents"].astype(str).isin(["테스트 결과 : OK", "테스트 결과 : NG"])
        df3["from_to_test_ct"] = pd.NA

        mask = is_okng & df3["_test_ts"].notna() & df3["_base_ts"].notna()
        df3.loc[mask, "from_to_test_ct"] = (df3.loc[mask, "_test_ts"] - df3.loc[mask, "_base_ts"]).dt.total_seconds()

        print("[OK] from_to_test_ct filled rows={}".format(int(df3["from_to_test_ct"].notna().sum())), flush=True)

        # --------------------------------
        # Cell X4) okng_seq + test_contents 매핑
        # --------------------------------
        df4 = df3.copy()

        MAP_1_67 = {
            1: "0.00_d_sig_val_090_set", 2: "0.00_load_a_cc_set", 3: "0.00_dmm_c_rng_set", 4: "0.00_load_c_cc_set",
            5: "0.00_dmm_c_rng_set", 6: "0.00_dmm_regi_set", 7: "0.00_dmm_regi_ac_0.6_set",
            8: "1.00_mini_b_short_check", 9: "1.01_usb_a_short_check", 10: "1.02_usb_c_short_check",
            11: "1.03_d_sig_val_000_set", 12: "1.03_dmm_regi_set", 13: "1.03_dmm_regi_ac_0.6_set",
            14: "1.03_pin12_short_check", 15: "1.04_pin23_short_check", 16: "1.05_pin34_short_check",
            17: "1.06_dmm_dc_v_set", 18: "1.06_dmm_ac_0.6_set", 19: "1.06_dmm_c_set",
            20: "1.06_load_a_sensing_on", 21: "1.06_load_c_sensing_on",
            22: "1.06_ps_16.5v_set", 23: "1.06_ps_on", 24: "1.06_dmm_ac_0.6_set", 25: "1.06_input_16.5v",
            26: "1.07_idle_c_check", 27: "1.08_fw_ver_check", 28: "1.09_chip_id_check",
            29: "1.10_dmm_3c_rng_set", 30: "1.10_load_a_5.5c_set", 31: "1.10_load_a_on", 32: "1.10_usb_a_v_check",
            33: "1.11_usb_a_c_check", 34: "1.12_usb_c_v_check", 35: "1.13_load_a_off", 36: "1.13_load_c_5.5c_set",
            37: "1.13_load_c_on", 38: "1.13_overcurr_usb_c_v", 39: "1.14_overcurr_usb_c_c", 40: "1.15_usb_a_v",
            41: "1.16_load_c_off", 42: "1.16_dut_reset", 43: "1.16_cc_aside_check", 44: "1.17_cc_bside_check",
            45: "1.18_load_a_2.4c_set", 46: "1.18_load_c_3c_set", 47: "1.18_load_a_on", 48: "1.18_load_c_on",
            49: "1.18_usb_a_v_check", 50: "1.19_usb_a_c_check", 51: "1.20_usb_c_v_check", 52: "1.21_usb_c_c_check",
            53: "1.22_load_a_off", 54: "1.22_load_c_off", 55: "1.22_usb_c_carplay", 56: "1.23_usb_a_carplay",
            57: "1.24_usb_c_pm1", 58: "1.25_usb_c_pm2", 59: "1.26_usb_c_pm3", 60: "1.27_usb_c_pm4",
            61: "1.28_usb_a_pm1", 62: "1.29_usb_a_pm2", 63: "1.30_usb_a_pm3", 64: "1.31_usb_a_pm4",
            65: "1.32_dmm_ac_0.6_set", 66: "1.32_dmm_c_rng_set", 67: "1.32_dark_curr_check"
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

        df_final = df4[
            ["group", "barcode_information", "station", "remark", "end_day", "end_time", "run_time", "boundary_run_time",
             "test_time", "contents", "test_contents", "test_ct", "from_to_test_ct", "okng_seq"]
        ].copy()

        # --------------------------------
        # Cell X5) df15
        # --------------------------------
        df15 = df_final.dropna(subset=["test_contents"]).copy()
        df15 = df15[
            ["group", "barcode_information", "station", "remark", "end_day", "end_time", "run_time", "boundary_run_time",
             "test_time", "test_contents", "from_to_test_ct", "okng_seq"]
        ].reset_index(drop=True)

        # --------------------------------
        # Cell X6) boundary_test_ct + problem1~4 JOIN
        # --------------------------------
        TARGET_END_DAY_X6 = str(df15["end_day"].dropna().iloc[0]).strip()

        present_problem_cols = get_present_problem_cols(get_engine_blocking())
        want_problem_cols = ["problem1", "problem2", "problem3", "problem4"]
        use_problem_cols: List[str] = []
        for c in want_problem_cols:
            if c in present_problem_cols:
                use_problem_cols.append(c)

        has_endday = table_has_column(get_engine_blocking(), "e4_predictive_maintenance", "non_pd_cal_test_ct_summary", "end_day")

        select_cols = ["test_contents", "upper_outlier"]
        for c in use_problem_cols:
            select_cols.append(c)

        if has_endday:
            SQL_BOUNDARY2 = text("""
            SELECT {cols}
            FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
            WHERE end_day = :end_day
            """.format(cols=", ".join(select_cols)))
            df_boundary = read_sql_blocking(SQL_BOUNDARY2, params={"end_day": TARGET_END_DAY_X6})
            if len(df_boundary) == 0:
                SQL_LATEST_DAY = text("""
                SELECT MAX(end_day) AS max_day
                FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
                """)
                latest2_df = read_sql_blocking(SQL_LATEST_DAY, params=None)
                latest2 = None
                if not latest2_df.empty:
                    latest2 = latest2_df.iloc[0]["max_day"]
                if pd.isna(latest2):
                    raise ValueError("[ERROR] non_pd_cal_test_ct_summary 테이블이 비어있습니다.")
                latest2 = str(latest2).strip()
                df_boundary = read_sql_blocking(SQL_BOUNDARY2, params={"end_day": latest2})
        else:
            SQL_BOUNDARY2 = text("""
            SELECT {cols}
            FROM e4_predictive_maintenance.non_pd_cal_test_ct_summary
            """.format(cols=", ".join(select_cols)))
            df_boundary = read_sql_blocking(SQL_BOUNDARY2, params=None)

        df_boundary["test_contents"] = df_boundary["test_contents"].astype(str).str.strip()
        df_boundary["upper_outlier"] = pd.to_numeric(df_boundary["upper_outlier"], errors="coerce").round(2)
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

        for c in ["run_time", "boundary_run_time", "from_to_test_ct", "boundary_test_ct"]:
            if c in df15_out.columns:
                df15_out[c] = pd.to_numeric(df15_out[c], errors="coerce").round(2)

        # --------------------------------
        # Cell X7) diff_ct
        # --------------------------------
        df15_out["boundary_test_ct"] = pd.to_numeric(df15_out["boundary_test_ct"], errors="coerce")
        df15_out["from_to_test_ct"] = pd.to_numeric(df15_out["from_to_test_ct"], errors="coerce")
        df15_out["diff_ct"] = df15_out["from_to_test_ct"] - df15_out["boundary_test_ct"]

        df15_out2 = df15_out[
            ["group", "barcode_information", "station", "remark", "end_day", "end_time", "run_time", "boundary_run_time",
             "okng_seq", "test_contents", "test_time", "from_to_test_ct",
             "boundary_test_ct", "diff_ct", "problem1", "problem2", "problem3", "problem4"]
        ].copy()

        # --------------------------------
        # Cell X8) 바코드 대표행 기준 diff_ct TOP5%
        # --------------------------------
        df_spike = df15_out2.copy()
        df_spike["diff_ct"] = pd.to_numeric(df_spike["diff_ct"], errors="coerce")
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

        print("[OK] diff_ct TOP5% cut={:.3f} / rows={}".format(float(cut95), len(df_top2)), flush=True)

        if df_top2.empty:
            raise RuntimeError("[ERROR] diff_ct TOP5% 결과가 비어있습니다.")

        # --------------------------------
        # Cell X9) file_path 매칭
        # --------------------------------
        key_df = df_top2[["barcode_information", "end_day", "end_time"]].copy()
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
        df_fp = read_sql_blocking(SQL_FILEPATH, params={"barcodes": barcodes2, "days": days2})

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
        # Cell X10) 저장 준비 (DDL + UPSERT)
        # --------------------------------
        FULL_NAME = "{}.{}".format(TARGET_SCHEMA, TARGET_TABLE)
        df_save = df_top_fp.copy()

        if "group" in df_save.columns and "group_no" not in df_save.columns:
            df_save["group_no"] = pd.to_numeric(df_save["group"], errors="coerce").astype("Int64")
        elif "group_no" not in df_save.columns:
            df_save["group_no"] = pd.NA

        df_save["run_start_ts"] = run_start_dt
        df_save["run_end_ts"] = pd.NaT
        df_save["run_seconds"] = pd.NA
        # ✅ updated_at은 DB now()로 주입

        pk_cols = ["barcode_information", "end_day", "end_time", "test_contents", "okng_seq"]
        missing_pk = []
        for c in pk_cols:
            if c not in df_save.columns:
                missing_pk.append(c)
        if missing_pk:
            raise KeyError("[ERROR] PK에 필요한 컬럼이 없습니다: {}".format(missing_pk))

        # 타입 정규화(저장용)
        df_save["barcode_information"] = df_save["barcode_information"].astype(str).str.strip()
        df_save["end_day"] = pd.to_datetime(df_save["end_day"], errors="coerce").dt.date
        df_save["end_time"] = df_save["end_time"].astype(str).str.strip()
        df_save["test_contents"] = df_save["test_contents"].astype(str).str.strip()
        df_save["okng_seq"] = pd.to_numeric(df_save["okng_seq"], errors="coerce").astype("Int64")

        num_cols = ["run_time", "boundary_run_time", "from_to_test_ct", "boundary_test_ct", "diff_ct", "run_seconds"]
        for c in num_cols:
            if c in df_save.columns:
                df_save[c] = pd.to_numeric(df_save[c], errors="coerce")

        prob_cols = ["problem1", "problem2", "problem3", "problem4"]
        for c in prob_cols:
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

    finally:
        run_end_dt = datetime.now()
        run_seconds = round(time.perf_counter() - run_start_perf, 3)

        print("[RUN] end_ts={}".format(run_end_dt.strftime("%Y-%m-%d %H:%M:%S")), flush=True)
        print("[RUN] run_seconds={:.3f}".format(float(run_seconds)), flush=True)

        if payload_for_save is None:
            if SAVE_HTML_REPORT:
                print("[OK] HTML report saved: {}".format(os.path.abspath(REPORT_DIR)), flush=True)
            return

        df_save2 = payload_for_save["df_save"].copy()
        df_save2["run_end_ts"] = run_end_dt
        df_save2["run_seconds"] = float(run_seconds)

        # ✅ DB 저장 구간: DDL/UPSERT 모두 "연결 복구까지 무한 재시도"
        while True:
            try:
                eng = get_engine_blocking()
                with eng.begin() as conn:
                    _apply_session_limits(conn)

                    # 테이블/스키마 생성
                    conn.execute(payload_for_save["DDL"])

                    # DB에 실제 존재하는 컬럼만 사용
                    existing_cols_list = fetch_existing_cols(conn, payload_for_save["schema"], payload_for_save["table"])
                    existing_cols = set(existing_cols_list)

                    candidate_cols = [
                        "group_no",
                        "barcode_information", "station", "remark", "end_day", "end_time",
                        "run_time", "boundary_run_time",
                        "okng_seq", "test_contents", "test_time",
                        "from_to_test_ct", "boundary_test_ct", "diff_ct",
                        "problem1", "problem2", "problem3", "problem4",
                        "file_path",
                        "run_start_ts", "run_end_ts", "run_seconds",
                        "updated_at",
                    ]

                    save_cols: List[str] = []
                    for c in candidate_cols:
                        if c == "updated_at":
                            if c in existing_cols:
                                save_cols.append(c)
                        else:
                            if (c in df_save2.columns) and (c in existing_cols):
                                save_cols.append(c)

                    pk_cols = payload_for_save["pk_cols"]
                    for c in pk_cols:
                        if c not in existing_cols:
                            raise RuntimeError("[ERROR] DB 테이블에 PK 컬럼 '{}' 가 없습니다. 테이블 스키마를 확인하세요.".format(c))
                        if c not in save_cols:
                            save_cols.append(c)

                    insert_cols_sql = ", ".join(save_cols)

                    # ✅ values에서 updated_at만 DB now()로 강제
                    values_parts: List[str] = []
                    for c in save_cols:
                        if c == "updated_at":
                            values_parts.append("now()")
                        else:
                            values_parts.append(":{}".format(c))
                    values_sql = ", ".join(values_parts)

                    conflict_cols = "barcode_information, end_day, end_time, test_contents, okng_seq"

                    set_cols: List[str] = []
                    for c in save_cols:
                        if c not in pk_cols:
                            set_cols.append(c)

                    set_parts: List[str] = []
                    for c in set_cols:
                        if c == "updated_at":
                            set_parts.append("updated_at = now()")
                        else:
                            set_parts.append("{c} = EXCLUDED.{c}".format(c=c))

                    if "updated_at" not in save_cols:
                        set_parts.append("updated_at = now()")

                    set_sql = ", ".join(set_parts)

                    UPSERT_SQL = text("""
                    INSERT INTO {full} ({cols})
                    VALUES ({vals})
                    ON CONFLICT ({conflict})
                    DO UPDATE SET {set_sql};
                    """.format(
                        full=payload_for_save["full_name"],
                        cols=insert_cols_sql,
                        vals=values_sql,
                        conflict=conflict_cols,
                        set_sql=set_sql
                    ))

                    # ✅ listcomp 제거
                    cols_wo_updated = filter_cols_excluding(save_cols, "updated_at")
                    rows = df_save2[cols_wo_updated].to_dict(orient="records")

                    # NaN -> None
                    for r in rows:
                        for k, v in list(r.items()):
                            if pd.isna(v):
                                r[k] = None

                    conn.execute(UPSERT_SQL, rows)

                print("[OK] Saved to {} (rows={}) / used_cols={}".format(
                    payload_for_save["full_name"], len(df_save2), len(save_cols)
                ), flush=True)
                break

            except Exception as e:
                print("[DB][RETRY] UPSERT failed: {t}: {m}".format(
                    t=type(e).__name__, m=repr(e)
                ), flush=True)
                if _is_conn_error(e):
                    safe_engine_recover(None)
                time.sleep(DB_RETRY_INTERVAL_SEC)

        if SAVE_HTML_REPORT:
            print("[OK] HTML report saved: {}".format(os.path.abspath(REPORT_DIR)), flush=True)


# ============================
# 4) 스케줄러: 무한 루프(1초 폴링) + 하루 2회 윈도우 실행
# ============================

def scheduler_loop():
    # ✅ 시작 시 엔진 확보(연결 성공까지 블로킹)
    _ = get_engine_blocking()
    print("[DB] engine ready (blocking ensured) | pool_size=1 max_overflow=0 | work_mem={}".format(WORK_MEM), flush=True)

    last_state = None  # "RUN" / "WAIT"

    while True:
        now_dt = datetime.now()

        if is_run_time(now_dt):
            if last_state != "RUN":
                print("[SCHED] enter RUN window @ {}".format(now_dt.strftime("%Y-%m-%d %H:%M:%S")), flush=True)
                last_state = "RUN"

            try:
                # ✅ 매번 가볍게 연결 체크(끊겼으면 즉시 복구)
                try:
                    eng = get_engine_blocking()
                    with eng.connect() as conn:
                        _apply_session_limits(conn)
                        conn.execute(text("SELECT 1"))
                except Exception:
                    safe_engine_recover(None)

                run_once(get_engine_blocking())

            except Exception as e:
                print("[ERROR] run_once failed: {}".format(repr(e)), flush=True)
                # DB 오류 가능성까지 고려해 엔진 재확보(블로킹)
                safe_engine_recover(None)

            time.sleep(SLEEP_SECONDS)

        else:
            if last_state != "WAIT":
                print("[SCHED] WAIT (outside windows) @ {}".format(now_dt.strftime("%Y-%m-%d %H:%M:%S")), flush=True)
                last_state = "WAIT"

            time.sleep(SLEEP_SECONDS)


if __name__ == "__main__":
    scheduler_loop()
