# -*- coding: utf-8 -*-
"""
backend1_daily_final_amount_daemon_TEST_20260128_DAY.py

✅ 테스트 고정 버전:
- prod_day = 20260128 (고정)
- shift    = day (고정)
- window   = 2026-01-28 08:30:00 ~ 2026-01-28 20:29:59 (KST, 고정)

데몬 요구 유지:
- 무한 루프 5초
- DB 접속 실패/끊김 무한 재시도(5초)
- pool_size=1 유지
- work_mem 세팅
- 증분 조건 last_pk: (end_day, end_time, barcode_information)

✅ 추가/수정 사양(최종 반영):
- station 후보는 "Vision1/2에 FCT1~4를 포함" (replace 아님)
- barcode dedup(최신 1건)은
  * FCT1~4 군 내부에서만 적용
  * Vision1~2 군 내부에서만 적용
  (FCT군과 Vision군은 서로 dedup 범위 분리)
- 저장:
  * i_daily_report.a_day_daily_final_amount            : Vision군(최종) overall만
  * i_daily_report.a_station_day_daily_final_amount    : FCT군 + Vision군 station별
"""

from __future__ import annotations

import os
import time as time_mod
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Tuple, Optional, List, Any, Iterable

import pandas as pd
from zoneinfo import ZoneInfo

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / 기본 설정
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

FCT_STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
VISION_STATIONS = ["Vision1", "Vision2"]
STATIONS = FCT_STATIONS + VISION_STATIONS

GOODORBAD_VALUE = "GoodFile"

# =========================
# ✅ TEST FIXED WINDOW
# =========================
TEST_PROD_DAY = "20260128"
TEST_SHIFT_TYPE = "day"

TEST_START_DT = datetime(2026, 1, 28, 8, 30, 0, tzinfo=KST)
TEST_END_DT   = datetime(2026, 1, 28, 20, 29, 59, tzinfo=KST)  # 고정(inclusive)

# =========================
# DB 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

TESTLOG_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
TESTLOG_TABLE  = "fct_vision_testlog_txt_processing_history"

REMARK_SCHEMA = "g_production_film"
REMARK_TABLE  = "remark_info"

SAVE_SCHEMA = "i_daily_report"
T_DAY_OVERALL   = "a_day_daily_final_amount"
T_DAY_STATION   = "a_station_day_daily_final_amount"

# =========================
# 시간대(한 줄 라벨) - day만 사용
# =========================
DAY_BANDS = [
    ("A시간대(08:30:00 ~ 10:29:59)", 0,  2*3600 - 1),
    ("B시간대(10:30:00 ~ 12:29:59)", 2*3600,  4*3600 - 1),
    ("C시간대(12:30:00 ~ 14:29:59)", 4*3600,  6*3600 - 1),
    ("D시간대(14:30:00 ~ 16:29:59)", 6*3600,  8*3600 - 1),
    ("E시간대(16:30:00 ~ 18:29:59)", 8*3600, 10*3600 - 1),
    ("F시간대(18:30:00 ~ 20:29:59)",10*3600, 12*3600 - 1),
]

# =========================
# 로깅
# =========================
def log(level: str, msg: str):
    now = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{now} [{level}] {msg}", flush=True)

# =========================
# SQL 식별자 quoting
# =========================
def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

# =========================
# DB 엔진
# =========================
def make_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

def ensure_db_ready(engine: Engine):
    with engine.begin() as conn:
        conn.execute(text("SELECT 1;"))
        conn.execute(text("SET work_mem = :wm;"), {"wm": WORK_MEM})

def with_work_mem(conn):
    conn.execute(text("SET work_mem = :wm;"), {"wm": WORK_MEM})

# =========================
# SHIFT WINDOW (TEST 고정)
# =========================
@dataclass(frozen=True)
class ShiftWindow:
    prod_day: str
    shift_type: str
    start_dt: datetime
    end_dt: datetime

def fixed_test_window() -> ShiftWindow:
    return ShiftWindow(TEST_PROD_DAY, TEST_SHIFT_TYPE, TEST_START_DT, TEST_END_DT)

# =========================
# PASS/FAIL 정규화
# =========================
def normalize_passfail_series(s: pd.Series) -> pd.Series:
    x = s.astype(str).str.strip().str.upper()
    is_pass = x.isin(["PASS", "P", "OK", "TRUE", "1"]) | x.str.contains("PASS", na=False)
    is_fail = x.isin(["FAIL", "F", "NG", "FALSE", "0"]) | x.str.contains("FAIL", na=False)
    out = pd.Series(pd.NA, index=s.index, dtype="object")
    out.loc[is_pass] = "PASS"
    out.loc[is_fail] = "FAIL"
    return out

def passfail_cell(pass_cnt: int, fail_cnt: int) -> str:
    return f"PASS: {int(pass_cnt)}, FAIL: {int(fail_cnt)}"

# =========================
# time_band 부여 (day만)
# =========================
def assign_band(end_ts: pd.Series, window: ShiftWindow) -> pd.Series:
    delta_sec = (end_ts - window.start_dt).dt.total_seconds()
    out = pd.Series(pd.NA, index=end_ts.index, dtype="object")
    for label, lo, hi in DAY_BANDS:
        mask = (delta_sec >= lo) & (delta_sec <= hi)
        out.loc[mask] = label
    return out

# =========================
# remark_info 로드 (키->(pn,remark))
# =========================
def load_remark_map(engine: Engine) -> Dict[str, Tuple[str, str]]:
    q = text(f"""
        SELECT barcode_information, pn, remark
        FROM {REMARK_SCHEMA}.{REMARK_TABLE}
    """)
    with engine.begin() as conn:
        with_work_mem(conn)
        m = pd.read_sql(q, conn)

    m["barcode_information"] = m["barcode_information"].astype(str).str.strip()
    m["pn"] = m["pn"].astype(str).str.strip()
    m["remark"] = m["remark"].astype(str).str.strip()

    mp: Dict[str, Tuple[str, str]] = {}
    for _, r in m.iterrows():
        k = r["barcode_information"]
        if k and k != "nan":
            mp[k] = (r["pn"], r["remark"])
    return mp

def barcode_key18(s: str) -> Optional[str]:
    if not isinstance(s, str):
        return None
    s = s.strip()
    if len(s) < 18:
        return None
    return s[17]

# =========================
# 결과 컬럼 감지
# =========================
RESULT_CANDIDATES = ["result", "passfail", "pass_fail", "test_result", "pf", "judge", "status"]

def detect_result_col(engine: Engine) -> str:
    q = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :s AND table_name = :t
        ORDER BY ordinal_position
    """)
    with engine.begin() as conn:
        with_work_mem(conn)
        cols = [r[0] for r in conn.execute(q, {"s": TESTLOG_SCHEMA, "t": TESTLOG_TABLE}).fetchall()]

    lower = {c.lower(): c for c in cols}
    for cand in RESULT_CANDIDATES:
        if cand.lower() in lower:
            return lower[cand.lower()]

    raise RuntimeError(f"PASS/FAIL 컬럼을 찾지 못함. candidates={RESULT_CANDIDATES}, table_cols={cols}")

# =========================
# DB fetch (full / incremental)
# =========================
@dataclass
class LastPK:
    end_day: str
    end_time: str
    barcode: str

def fetch_rows(engine: Engine, window: ShiftWindow, result_col: str, last_pk: Optional[LastPK]) -> pd.DataFrame:
    end_days = sorted({window.start_dt.strftime("%Y%m%d"), window.end_dt.strftime("%Y%m%d")})
    rc = qident(result_col)

    sql = f"""
    WITH base AS (
        SELECT
            station,
            barcode_information,
            end_day::text AS end_day,
            regexp_replace(end_time::text, '\\\\..*$', '') AS end_time,
            {rc} AS result_raw
        FROM {TESTLOG_SCHEMA}.{TESTLOG_TABLE}
        WHERE station = ANY(:stations)
          AND goodorbad = :goodorbad
          AND end_day = ANY(:end_days)
          AND barcode_information IS NOT NULL
          AND barcode_information <> ''
    ),
    enriched AS (
        SELECT
            station,
            barcode_information,
            end_day,
            end_time,
            (to_timestamp(end_day || ' ' || end_time, 'YYYYMMDD HH24:MI:SS') AT TIME ZONE 'Asia/Seoul') AS end_ts,
            result_raw
        FROM base
        WHERE end_time IS NOT NULL AND end_time <> ''
    )
    SELECT station, barcode_information, end_day, end_time, end_ts, result_raw
    FROM enriched
    WHERE end_ts >= :win_start
      AND end_ts <= :win_end
      AND (
            :lp_end_day IS NULL
            OR (end_day, end_time, barcode_information) > (:lp_end_day, :lp_end_time, :lp_barcode)
          )
    ORDER BY end_day, end_time, barcode_information
    """

    params = {
        "stations": STATIONS,
        "goodorbad": GOODORBAD_VALUE,
        "end_days": end_days,
        "win_start": window.start_dt.replace(tzinfo=None),
        "win_end": window.end_dt.replace(tzinfo=None),
        "lp_end_day": None if last_pk is None else last_pk.end_day,
        "lp_end_time": None if last_pk is None else last_pk.end_time,
        "lp_barcode": None if last_pk is None else last_pk.barcode,
    }

    with engine.begin() as conn:
        with_work_mem(conn)
        df = pd.read_sql(text(sql), conn, params=params)

    if not df.empty:
        df["end_ts"] = pd.to_datetime(df["end_ts"], errors="coerce").dt.tz_localize(KST)
    return df

def compute_last_pk_from_df(df: pd.DataFrame) -> Optional[LastPK]:
    if df is None or df.empty:
        return None
    last = df.iloc[-1]
    return LastPK(str(last["end_day"]), str(last["end_time"]), str(last["barcode_information"]))

# =========================
# In-memory 최신 바코드 1건 유지 구조
# =========================
@dataclass
class LatestRow:
    station: str
    barcode: str
    end_day: str
    end_time: str
    end_ts: datetime
    passfail: str

def _is_fct_station(st: str) -> bool:
    return st in FCT_STATIONS

def _is_vision_station(st: str) -> bool:
    return st in VISION_STATIONS

def update_latest_maps(
    latest_fct: Dict[str, LatestRow],
    latest_vis: Dict[str, LatestRow],
    df_new: pd.DataFrame
) -> Tuple[int, int]:
    """
    ✅ dedup 범위 분리:
    - FCT군(FCT1~4) 안에서 barcode 최신 1건
    - Vision군(Vision1~2) 안에서 barcode 최신 1건
    """
    if df_new is None or df_new.empty:
        return 0, 0

    df_new = df_new.copy()
    df_new["passfail_norm"] = normalize_passfail_series(df_new["result_raw"])
    df_new = df_new.dropna(subset=["passfail_norm", "end_ts"])

    upd_fct = 0
    upd_vis = 0

    for r in df_new.itertuples(index=False):
        station = str(r.station)
        barcode = str(r.barcode_information).strip()
        if not barcode:
            continue

        row = LatestRow(
            station=station,
            barcode=barcode,
            end_day=str(r.end_day),
            end_time=str(r.end_time),
            end_ts=r.end_ts,
            passfail=str(r.passfail_norm),
        )

        if _is_fct_station(station):
            prev = latest_fct.get(barcode)
            if prev is None or row.end_ts > prev.end_ts:
                latest_fct[barcode] = row
                upd_fct += 1

        elif _is_vision_station(station):
            prev = latest_vis.get(barcode)
            if prev is None or row.end_ts > prev.end_ts:
                latest_vis[barcode] = row
                upd_vis += 1

        else:
            # station 값이 예상 범위를 벗어나면 무시 (안전)
            continue

    return upd_fct, upd_vis

def build_overall_df_from_latest(
    latest_rows: Iterable[LatestRow],
    window: ShiftWindow,
    remark_map: Dict[str, Tuple[str, str]],
    updated_at: datetime,
) -> pd.DataFrame:
    """
    overall(=station 없는 테이블) 생성.
    여기서는 '최종' 기준으로 Vision latest만 넣는 용도.
    """
    band_cols = [b[0] for b in DAY_BANDS]
    group_overall = ["prod_day", "shift_type", "pn", "remark"]

    rows = []
    for v in latest_rows:
        k = barcode_key18(v.barcode)
        mapped = remark_map.get(k) if k else None
        if not mapped:
            continue
        pn, remark = mapped
        rows.append({
            "barcode_information": v.barcode,
            "end_ts": v.end_ts,
            "passfail_norm": v.passfail,
            "pn": pn,
            "remark": remark,
            "prod_day": window.prod_day,
            "shift_type": window.shift_type,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        cols = ["prod_day", "shift_type", "pn", "remark"] + band_cols + ["합계", "updated_at"]
        return pd.DataFrame(columns=cols)

    df["time_band"] = assign_band(df["end_ts"], window)
    df = df.dropna(subset=["time_band"])

    g = df.groupby(group_overall + ["time_band", "passfail_norm"]).size().rename("cnt").reset_index()
    piv = g.pivot_table(index=group_overall + ["time_band"], columns="passfail_norm",
                        values="cnt", aggfunc="sum", fill_value=0).reset_index()
    if "PASS" not in piv.columns: piv["PASS"] = 0
    if "FAIL" not in piv.columns: piv["FAIL"] = 0
    piv["cell"] = piv.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)

    wide = piv.pivot_table(index=group_overall, columns="time_band", values="cell", aggfunc="first").reset_index()
    for c in band_cols:
        if c not in wide.columns:
            wide[c] = passfail_cell(0, 0)
    wide = wide[group_overall + band_cols]

    tot = (
        df.groupby(group_overall + ["passfail_norm"]).size().rename("cnt").reset_index()
          .pivot_table(index=group_overall, columns="passfail_norm", values="cnt", aggfunc="sum", fill_value=0)
          .reset_index()
    )
    if "PASS" not in tot.columns: tot["PASS"] = 0
    if "FAIL" not in tot.columns: tot["FAIL"] = 0
    tot["합계"] = tot.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)
    tot = tot[group_overall + ["합계"]]

    out = wide.merge(tot, on=group_overall, how="left")
    out["updated_at"] = updated_at
    out = out[["prod_day","shift_type","pn","remark"] + band_cols + ["합계","updated_at"]]
    out = out.sort_values(["prod_day","shift_type","pn","remark"], kind="mergesort").reset_index(drop=True)
    return out

def build_station_df_from_latest(
    latest_rows: Iterable[LatestRow],
    window: ShiftWindow,
    remark_map: Dict[str, Tuple[str, str]],
    updated_at: datetime,
) -> pd.DataFrame:
    """
    station 테이블 생성: FCT latest + Vision latest를 모두 넣는 용도.
    """
    band_cols = [b[0] for b in DAY_BANDS]
    group_station = ["prod_day", "shift_type", "station", "pn", "remark"]

    rows = []
    for v in latest_rows:
        k = barcode_key18(v.barcode)
        mapped = remark_map.get(k) if k else None
        if not mapped:
            continue
        pn, remark = mapped
        rows.append({
            "station": v.station,
            "barcode_information": v.barcode,
            "end_ts": v.end_ts,
            "passfail_norm": v.passfail,
            "pn": pn,
            "remark": remark,
            "prod_day": window.prod_day,
            "shift_type": window.shift_type,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        cols = ["prod_day", "shift_type", "station", "pn", "remark"] + band_cols + ["합계", "updated_at"]
        return pd.DataFrame(columns=cols)

    df["time_band"] = assign_band(df["end_ts"], window)
    df = df.dropna(subset=["time_band"])

    g2 = df.groupby(group_station + ["time_band", "passfail_norm"]).size().rename("cnt").reset_index()
    piv2 = g2.pivot_table(index=group_station + ["time_band"], columns="passfail_norm",
                          values="cnt", aggfunc="sum", fill_value=0).reset_index()
    if "PASS" not in piv2.columns: piv2["PASS"] = 0
    if "FAIL" not in piv2.columns: piv2["FAIL"] = 0
    piv2["cell"] = piv2.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)

    wide2 = piv2.pivot_table(index=group_station, columns="time_band", values="cell", aggfunc="first").reset_index()
    for c in band_cols:
        if c not in wide2.columns:
            wide2[c] = passfail_cell(0, 0)
    wide2 = wide2[group_station + band_cols]

    tot2 = (
        df.groupby(group_station + ["passfail_norm"]).size().rename("cnt").reset_index()
          .pivot_table(index=group_station, columns="passfail_norm", values="cnt", aggfunc="sum", fill_value=0)
          .reset_index()
    )
    if "PASS" not in tot2.columns: tot2["PASS"] = 0
    if "FAIL" not in tot2.columns: tot2["FAIL"] = 0
    tot2["합계"] = tot2.apply(lambda r: passfail_cell(r["PASS"], r["FAIL"]), axis=1)
    tot2 = tot2[group_station + ["합계"]]

    out = wide2.merge(tot2, on=group_station, how="left")
    out["updated_at"] = updated_at
    out = out[["prod_day","shift_type","station","pn","remark"] + band_cols + ["합계","updated_at"]]

    # 정렬: FCT1~4 -> Vision1~2
    station_order = FCT_STATIONS + VISION_STATIONS
    out["station"] = pd.Categorical(out["station"], categories=station_order, ordered=True)
    out = out.sort_values(["prod_day","shift_type","pn","station","remark"], kind="mergesort").reset_index(drop=True)
    return out

# =========================
# 저장(테이블 생성 + UPSERT/DELETE+INSERT)
# =========================
def ensure_schema(engine: Engine, schema: str):
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))

def ensure_table(engine: Engine, schema: str, table: str, columns: List[str], key_cols: List[str]):
    ddl_cols = []
    for c in columns:
        ddl_cols.append(f'{qident(c)} {"timestamptz" if c=="updated_at" else "text"}')
    ddl_cols_sql = ",\n  ".join(ddl_cols)
    key_sql = ", ".join([qident(c) for c in key_cols])

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      {ddl_cols_sql},
      UNIQUE ({key_sql})
    );
    """
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(create_sql))

def _to_py(v: Any):
    if v is None:
        return None
    try:
        if isinstance(v, float) and math.isnan(v):
            return None
    except Exception:
        pass
    if pd.isna(v):
        return None
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()
    return v

def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> int:
    if df is None or df.empty:
        return 0
    cols = df.columns.tolist()
    col_sql = ", ".join([qident(c) for c in cols])

    bind_names = [f"p{i}" for i in range(len(cols))]
    val_sql = ", ".join([f":{bn}" for bn in bind_names])

    key_sql = ", ".join([qident(c) for c in key_cols])
    non_keys = [c for c in cols if c not in key_cols]
    set_sql = ", ".join([f'{qident(c)} = EXCLUDED.{qident(c)}' for c in non_keys])

    sql = f"""
    INSERT INTO {schema}.{table} ({col_sql})
    VALUES ({val_sql})
    ON CONFLICT ({key_sql})
    DO UPDATE SET {set_sql};
    """

    records = []
    for row in df.itertuples(index=False, name=None):
        records.append({bn: _to_py(v) for bn, v in zip(bind_names, row)})

    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(sql), records)

    return len(records)

def delete_shift_rows(engine: Engine, schema: str, table: str, prod_day: str, shift_type: str):
    sql = text(f"DELETE FROM {schema}.{table} WHERE prod_day=:pd AND shift_type=:sh")
    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(sql, {"pd": prod_day, "sh": shift_type})

def insert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0
    cols = df.columns.tolist()
    col_sql = ", ".join([qident(c) for c in cols])

    bind_names = [f"p{i}" for i in range(len(cols))]
    val_sql = ", ".join([f":{bn}" for bn in bind_names])

    sql = f"INSERT INTO {schema}.{table} ({col_sql}) VALUES ({val_sql})"
    records = []
    for row in df.itertuples(index=False, name=None):
        records.append({bn: _to_py(v) for bn, v in zip(bind_names, row)})

    with engine.begin() as conn:
        with_work_mem(conn)
        conn.execute(text(sql), records)
    return len(records)

# =========================
# 메인 루프 (TEST)
# =========================
def main():
    log("BOOT", f"TEST daemon starting: prod_day={TEST_PROD_DAY}, shift=day, window={TEST_START_DT}~{TEST_END_DT}")
    log("INFO", f"stations={STATIONS} (FCT+Vision), dedup scope = FCT-group / Vision-group separated")

    engine: Optional[Engine] = None

    # ✅ dedup 맵 2개로 분리
    latest_fct: Dict[str, LatestRow] = {}
    latest_vis: Dict[str, LatestRow] = {}

    last_pk: Optional[LastPK] = None

    remark_map: Dict[str, Tuple[str, str]] = {}
    result_col: Optional[str] = None

    # DB connect (blocking)
    while True:
        try:
            engine = make_engine()
            ensure_db_ready(engine)
            log("INFO", f"DB connected (work_mem={WORK_MEM}, pool_size=1)")
            break
        except Exception as e:
            log("RETRY", f"DB connect failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    # bootstrap
    while True:
        try:
            assert engine is not None
            result_col = detect_result_col(engine)
            remark_map = load_remark_map(engine)
            ensure_schema(engine, SAVE_SCHEMA)
            log("INFO", f"bootstrap OK (result_col={result_col})")
            break
        except Exception as e:
            log("RETRY", f"bootstrap failed: {type(e).__name__}: {e}")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    window = fixed_test_window()

    # ✅ 처음 1회 full rebuild + delete+insert
    latest_fct = {}
    latest_vis = {}
    last_pk = None

    df_all = fetch_rows(engine, window, result_col, last_pk=None)
    log("INFO", f"[FETCH] full rows={len(df_all)}")

    upd_fct, upd_vis = update_latest_maps(latest_fct, latest_vis, df_all)
    last_pk = compute_last_pk_from_df(df_all)
    log("INFO", f"[BUILD] latest_fct={len(latest_fct)}(upd={upd_fct}) latest_vis={len(latest_vis)}(upd={upd_vis}) last_pk={None if last_pk is None else (last_pk.end_day, last_pk.end_time, last_pk.barcode)}")

    updated_at = datetime.now(KST)

    # ✅ overall = Vision만(기존 유지)
    overall_df = build_overall_df_from_latest(latest_vis.values(), window, remark_map, updated_at)

    # ✅ station = FCT + Vision 모두(추가 기능)
    station_latest_rows = list(latest_fct.values()) + list(latest_vis.values())
    station_df = build_station_df_from_latest(station_latest_rows, window, remark_map, updated_at)

    KEY_OVERALL = ["prod_day", "shift_type", "pn"]
    KEY_STATION = ["prod_day", "shift_type", "station", "pn"]

    ensure_table(engine, SAVE_SCHEMA, T_DAY_OVERALL, overall_df.columns.tolist(), KEY_OVERALL)
    ensure_table(engine, SAVE_SCHEMA, T_DAY_STATION, station_df.columns.tolist(), KEY_STATION)

    delete_shift_rows(engine, SAVE_SCHEMA, T_DAY_OVERALL, window.prod_day, window.shift_type)
    delete_shift_rows(engine, SAVE_SCHEMA, T_DAY_STATION, window.prod_day, window.shift_type)

    ins1 = insert_df(engine, SAVE_SCHEMA, T_DAY_OVERALL, overall_df)
    ins2 = insert_df(engine, SAVE_SCHEMA, T_DAY_STATION, station_df)
    log("INFO", f"[INSERT] inserted overall(Vision)={ins1}, station(FCT+Vision)={ins2}")

    # loop incremental
    while True:
        loop_t0 = time_mod.time()
        try:
            df_new = fetch_rows(engine, window, result_col, last_pk=last_pk)
            log("INFO", f"[FETCH] new rows={len(df_new)} (last_pk={None if last_pk is None else (last_pk.end_day,last_pk.end_time,last_pk.barcode)})")

            if not df_new.empty:
                upd_fct, upd_vis = update_latest_maps(latest_fct, latest_vis, df_new)

                last_pk_new = compute_last_pk_from_df(df_new)
                if last_pk_new is not None:
                    last_pk = last_pk_new

                log("INFO", f"[BUILD] latest_fct={len(latest_fct)}(upd={upd_fct}) latest_vis={len(latest_vis)}(upd={upd_vis}) last_pk={(last_pk.end_day,last_pk.end_time,last_pk.barcode) if last_pk else None}")

                updated_at = datetime.now(KST)

                # overall = Vision만
                overall_df = build_overall_df_from_latest(latest_vis.values(), window, remark_map, updated_at)

                # station = FCT + Vision 모두
                station_latest_rows = list(latest_fct.values()) + list(latest_vis.values())
                station_df = build_station_df_from_latest(station_latest_rows, window, remark_map, updated_at)

                up1 = upsert_df(engine, SAVE_SCHEMA, T_DAY_OVERALL, overall_df, KEY_OVERALL)
                up2 = upsert_df(engine, SAVE_SCHEMA, T_DAY_STATION, station_df, KEY_STATION)
                log("INFO", f"[UPSERT] upserted overall(Vision)={up1}, station(FCT+Vision)={up2}")

        except (OperationalError, DBAPIError) as e:
            log("RETRY", f"DB error: {type(e).__name__}: {e}")
            while True:
                try:
                    engine = make_engine()
                    ensure_db_ready(engine)
                    log("INFO", "DB reconnected")
                    result_col = detect_result_col(engine)
                    remark_map = load_remark_map(engine)
                    break
                except Exception as e2:
                    log("RETRY", f"DB reconnect failed: {type(e2).__name__}: {e2}")
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)

        except Exception as e:
            log("RETRY", f"Unhandled error: {type(e).__name__}: {e}")

        elapsed = time_mod.time() - loop_t0
        time_mod.sleep(max(0.0, LOOP_INTERVAL_SEC - elapsed))

if __name__ == "__main__":
    main()
