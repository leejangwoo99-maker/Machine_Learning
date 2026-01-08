# -*- coding: utf-8 -*-
"""
FCT OP-CT Boxplot Summary + UPH(병렬합산) + PostgreSQL 저장 (MP 적용, Realtime Window 실행)
- 증분 로딩: (station, remark)별 cursor(last_end_ts) 이후 신규 데이터만 로드
- 5초 주기 실행
- 윈도우 시간(08:27~08:29:59, 20:27~20:29:59)에서만 실행
- 현재달(YYYYMM)만 처리
- 안정화 버퍼: end_ts <= now - 2초
- 저장(APPEND): e1_FCT_ct.fct_op_ct / e1_FCT_ct.fct_whole_op_ct (inserted_at 포함)
- 커서 테이블: e1_FCT_ct.fct_opct_cursor (저장 성공 후 갱신)
- ✅ (추천) 커서 NULL이면 now()-10분만 읽고 시작(첫 실행 폭주 방지)

주의:
- e1 집계는 DB 기반이므로 파일 미완성 방지 대신 안정화 버퍼로 “방금 들어온 미확정 row” 회피
"""

import sys
from pathlib import Path
import urllib.parse
from datetime import datetime, time as dtime
import time as time_mod
from multiprocessing import freeze_support
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
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SRC_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
SRC_TABLE  = "fct_vision_testlog_txt_processing_history"

TARGET_SCHEMA = "e1_FCT_ct"
TBL_OPCT      = "fct_op_ct"
TBL_WHOLE     = "fct_whole_op_ct"

# Cursor table (증분 로딩)
CURSOR_SCHEMA = "e1_FCT_ct"
CURSOR_TABLE  = "fct_opct_cursor"

# ✅ (추천) 커서 NULL이면 최근 N분만 읽고 시작 (첫 실행 폭주 방지)
BOOTSTRAP_LOOKBACK_MINUTES = 10

# boxplot html 저장 폴더(현재 스크립트 기준)
OUT_DIR = Path("./fct_opct_boxplot_html")
ENABLE_HTML = False   # EXE 운용 안정성 위해 기본 OFF. 필요 시 True.

# op_ct 필터
OPCT_MAX_SEC = 600

# =========================
# Realtime Loop 사양
# =========================
MAX_WORKERS = 2
LOOP_INTERVAL_SEC = 5.0     # ✅ 5초 주기
STABLE_DATA_SEC = 2         # 안정화 버퍼

WINDOWS = [
    (dtime(8, 27, 0),  dtime(8, 29, 59)),
    (dtime(20, 27, 0), dtime(20, 29, 59)),
]

HEARTBEAT_EVERY_LOOPS = 12  # 5초*12=60초마다 idle 로그

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
REMARKS  = ["PD", "Non-PD"]


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def now_in_windows(now_dt: datetime) -> bool:
    t = now_dt.time()
    for s, e in WINDOWS:
        if s <= t <= e:
            return True
    return False

def current_yyyymm() -> str:
    return datetime.now().strftime("%Y%m")

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

def safe_cat_to_str(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for c in out.columns:
        if isinstance(out[c].dtype, CategoricalDtype):
            out[c] = out[c].astype(str)
    return out

def save_df_append(df: pd.DataFrame, schema_name: str, table_name: str, engine):
    """
    테이블 없으면 생성, 있으면 append 누적.
    중복 허용 (요구사항). 다만 소스 로딩은 cursor로 신규만 읽어 중복을 실질 차단.
    """
    if df is None or len(df) == 0:
        log(f"[SKIP] {schema_name}.{table_name}: df가 비어있습니다.")
        return

    df_to_save = df.copy()

    if "month" in df_to_save.columns:
        df_to_save["month"] = df_to_save["month"].astype(str)

    df_to_save = safe_cat_to_str(df_to_save)

    # schema 보장
    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_schema(conn, schema_name)

    log(f"[APPEND] {schema_name}.{table_name} to_sql append (rows={len(df_to_save)})")
    df_to_save.to_sql(
        name=table_name,
        con=engine,
        schema=schema_name,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=5000
    )
    log(f"[OK] append 완료: {schema_name}.{table_name} (+{len(df_to_save)})")


# =========================
# 1-1) Cursor 테이블
# =========================
def ensure_cursor_table():
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {CURSOR_SCHEMA};

    CREATE TABLE IF NOT EXISTS {CURSOR_SCHEMA}.{CURSOR_TABLE} (
        station     TEXT NOT NULL,
        remark      TEXT NOT NULL,
        last_end_ts TIMESTAMP NULL,
        updated_at  TIMESTAMP NOT NULL DEFAULT now(),
        PRIMARY KEY (station, remark)
    );
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

def get_cursors(engine) -> dict:
    """
    return: {(station, remark): last_end_ts or None}
    """
    ensure_cursor_table()
    q = text(f"SELECT station, remark, last_end_ts FROM {CURSOR_SCHEMA}.{CURSOR_TABLE}")
    df = pd.read_sql(q, engine)

    cur_map = {(st, rk): None for st in STATIONS for rk in REMARKS}
    for _, r in df.iterrows():
        cur_map[(r["station"], r["remark"])] = r["last_end_ts"]
    return cur_map

def upsert_cursor(engine, station: str, remark: str, last_end_ts: datetime):
    ensure_cursor_table()
    sql_up = text(f"""
        INSERT INTO {CURSOR_SCHEMA}.{CURSOR_TABLE} (station, remark, last_end_ts, updated_at)
        VALUES (:station, :remark, :last_end_ts, now())
        ON CONFLICT (station, remark)
        DO UPDATE SET last_end_ts = EXCLUDED.last_end_ts,
                      updated_at  = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql_up, {"station": station, "remark": remark, "last_end_ts": last_end_ts})


# =========================
# 2) 소스 로딩(증분) + op_ct 계산
#    - 현재달 + 안정화버퍼 + cursor 이후 신규만
#    ✅ 커서 NULL이면 now()-10분부터만 읽고 시작
# =========================
def load_source_df_incremental(engine) -> pd.DataFrame:
    yyyymm = current_yyyymm()
    cursors = get_cursors(engine)

    log("[1/6] DB에서 원본 데이터 증분 로딩 시작...(cursor 기반)")
    dfs = []
    total_rows = 0

    sql_query = f"""
    WITH base AS (
        SELECT
            station,
            remark,
            btrim(end_day::text)  AS end_day_text,
            btrim(end_time::text) AS end_time_text,
            result,
            goodorbad,

            CASE
                WHEN btrim(end_day::text) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$' THEN (btrim(end_day::text))::date
                ELSE NULL
            END AS end_day_date,

            CASE
                WHEN btrim(end_day::text) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'
                 AND btrim(end_time::text) ~ '^\\d{{2}}:\\d{{2}}:\\d{{2}}(\\.\\d+)?$'
                THEN to_timestamp(
                    btrim(end_day::text) || ' ' || split_part(btrim(end_time::text), '.', 1),
                    'YYYY-MM-DD HH24:MI:SS'
                )

                WHEN btrim(end_day::text) ~ '^\\d{{4}}-\\d{{2}}-\\d{{2}}$'
                 AND btrim(end_time::text) ~ '^\\d{{6}}(\\.\\d+)?$'
                THEN to_timestamp(
                    btrim(end_day::text) || ' ' ||
                    substring(btrim(end_time::text) from 1 for 2) || ':' ||
                    substring(btrim(end_time::text) from 3 for 2) || ':' ||
                    substring(btrim(end_time::text) from 5 for 2),
                    'YYYY-MM-DD HH24:MI:SS'
                )
                ELSE NULL
            END AS end_ts

        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE
            station = :station
            AND remark  = :remark
            AND result <> 'FAIL'
            AND goodorbad <> 'BadFile'
    )
    SELECT
        station,
        remark,
        end_day_text AS end_day,
        end_time_text AS end_time,
        result,
        goodorbad,
        end_ts
    FROM base
    WHERE
        end_day_date IS NOT NULL
        AND to_char(end_day_date, 'YYYYMM') = :yyyymm
        AND end_ts IS NOT NULL
        AND end_ts <= (now()::timestamp - INTERVAL '{STABLE_DATA_SEC} seconds')
        AND end_ts > COALESCE(
            :last_end_ts,
            (now()::timestamp - INTERVAL '{BOOTSTRAP_LOOKBACK_MINUTES} minutes')
        )
    ORDER BY end_ts ASC
    """

    for st in STATIONS:
        for rk in REMARKS:
            last_ts = cursors.get((st, rk))
            df_part = pd.read_sql(
                text(sql_query),
                engine,
                params={
                    "station": st,
                    "remark": rk,
                    "yyyymm": yyyymm,
                    "last_end_ts": last_ts,
                },
            )
            n = len(df_part)
            total_rows += n
            if n > 0:
                log(f"[INFO] +{st}/{rk}: rows={n} (cursor={last_ts})")
                dfs.append(df_part)
            else:
                log(f"[SKIP] {st}/{rk}: 신규 없음 (cursor={last_ts})")

    if total_rows == 0:
        log("[OK] 원본 로딩 완료 (rows=0)")
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    log(f"[OK] 원본 로딩 완료 (rows={len(df)})")

    log("[2/6] 타입 정리 및 op_ct 생성...")
    df["end_day"] = pd.to_datetime(df["end_day"], errors="coerce").dt.date
    df["end_ts"]  = pd.to_datetime(df["end_ts"], errors="coerce")

    df = df.sort_values(["station", "remark", "end_ts"], ascending=True).reset_index(drop=True)
    df["op_ct"] = df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    df["month"] = pd.to_datetime(df["end_day"].astype(str), errors="coerce").dt.strftime("%Y%m")

    log("[OK] op_ct / month 생성 완료")
    return df


# =========================
# 3) Boxplot 요약 + (옵션) HTML 저장 (MP=2)
# =========================
def _range_str(values: pd.Series):
    values = values.dropna()
    if len(values) == 0:
        return None
    vmin = float(values.min())
    vmax = float(values.max())
    return f"{vmin:.1f}~{vmax:.1f}"

def _summarize_group_worker(args):
    station, remark, month, op_ct_list, out_dir_str, enable_html = args
    out_dir = Path(out_dir_str)

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

    html_path = None
    if enable_html:
        out_dir.mkdir(parents=True, exist_ok=True)
        fig = px.box(pd.DataFrame({"op_ct": s}), y="op_ct", points="outliers", title=None)
        html_name = f"boxplot_{station}_{remark}_{month}.html"
        html_path = str(out_dir / html_name)
        fig.write_html(html_path, include_plotlyjs="cdn", full_html=True)

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
        "html": html_path,
    }

def build_summary_df(df_raw: pd.DataFrame) -> pd.DataFrame:
    log(f"[3/6] (station, remark, month) 요약 생성 시작... (MP={MAX_WORKERS})")

    if df_raw is None or len(df_raw) == 0:
        return pd.DataFrame(columns=[
            "id","station","remark","month","sample_amount","op_ct_lower_outlier",
            "q1","median","q3","op_ct_upper_outlier","del_out_op_ct_av","html"
        ])

    group_items = []
    for (station, remark, month), g in df_raw.groupby(["station", "remark", "month"], sort=True):
        op_ct_list = g["op_ct"].dropna().tolist()
        group_items.append((station, remark, month, op_ct_list, str(OUT_DIR), ENABLE_HTML))

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
    summary_df = summary_df.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    summary_df.insert(0, "id", range(1, len(summary_df) + 1))

    log(f"[OK] 요약 DF 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 4) plotly_json 컬럼 생성 (html 컬럼 제거) (MP=2)
# =========================
def _make_boxplot_json_worker(args):
    idx, op_ct_list = args
    s = pd.Series(op_ct_list).dropna()
    s = s[s <= OPCT_MAX_SEC]
    if len(s) == 0:
        return idx, None

    fig = px.box(pd.DataFrame({"op_ct": s}), y="op_ct", points="outliers", title=None)
    return idx, fig.to_json(validate=False)

def build_plotly_json_column(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    log(f"[4/6] plotly_json 생성 시작... (MP={MAX_WORKERS})")

    out = summary_df.copy()
    if len(out) == 0:
        out["plotly_json"] = []
        return out

    out["plotly_json"] = None

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
# 6) 1회 실행(main_once)
# =========================
def main_once():
    log("=== FCT OP-CT Pipeline RUN (ONE SHOT) ===")
    log(f"[INFO] MP workers = {MAX_WORKERS}")
    log(f"[INFO] month filter = {current_yyyymm()} | stable_buf={STABLE_DATA_SEC}s | loop=5s | bootstrap={BOOTSTRAP_LOOKBACK_MINUTES}m")

    engine = get_engine(DB_CONFIG)

    df_raw = load_source_df_incremental(engine)
    if df_raw is None or len(df_raw) == 0:
        log("[SKIP] 신규 데이터 없음 -> 저장 생략")
        return

    summary_df  = build_summary_df(df_raw)
    summary_df2 = build_plotly_json_column(df_raw, summary_df)
    final_df_86 = build_final_df_86(summary_df2)

    inserted_at = datetime.now()
    summary_df2["inserted_at"] = inserted_at
    final_df_86["inserted_at"] = inserted_at

    log("[6/6] DB 저장 단계 시작...(APPEND 누적)")
    save_df_append(summary_df2, TARGET_SCHEMA, TBL_OPCT, engine)
    save_df_append(final_df_86, TARGET_SCHEMA, TBL_WHOLE, engine)

    # ✅ 저장 성공 이후에만 cursor 갱신
    cur_update = df_raw.groupby(["station", "remark"])["end_ts"].max().reset_index()
    for _, r in cur_update.iterrows():
        upsert_cursor(engine, r["station"], r["remark"], r["end_ts"])
        log(f"[CURSOR] {r['station']}/{r['remark']} -> {r['end_ts']}")

    log("=== DONE (ONE SHOT) ===")


# =========================
# 7) Realtime loop (윈도우 시간에만)
# =========================
def realtime_loop():
    log("=== FCT OP-CT Realtime Loop START ===")
    log(f"[INFO] windows={[(s.strftime('%H:%M:%S'), e.strftime('%H:%M:%S')) for s,e in WINDOWS]}")
    log(f"[INFO] LOOP_INTERVAL_SEC={LOOP_INTERVAL_SEC} | workers={MAX_WORKERS} | ENABLE_HTML={ENABLE_HTML}")

    loop_count = 0
    while True:
        loop_count += 1
        loop_start = time_mod.perf_counter()
        now_dt = datetime.now()

        if not now_in_windows(now_dt):
            if (loop_count % HEARTBEAT_EVERY_LOOPS) == 0:
                log(f"[IDLE] {now_dt:%Y-%m-%d %H:%M:%S} (out of window)")
            elapsed = time_mod.perf_counter() - loop_start
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                time_mod.sleep(sleep_sec)
            continue

        try:
            log(f"[RUN] {now_dt:%Y-%m-%d %H:%M:%S} (in window)")
            main_once()
        except Exception as e:
            log(f"[ERROR] {type(e).__name__}: {e}")

        elapsed = time_mod.perf_counter() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            time_mod.sleep(sleep_sec)


# =========================
# 8) 엔트리포인트
# =========================
if __name__ == "__main__":
    freeze_support()

    exit_code = 0
    try:
        realtime_loop()
    except KeyboardInterrupt:
        log("\n[ABORT] 사용자 중단(CTRL+C)")
        exit_code = 130
    except Exception as e:
        log(f"\n[ERROR] Unhandled exception: {repr(e)}")
        exit_code = 1
    finally:
        if getattr(sys, "frozen", False):
            print("\n[INFO] 프로그램이 종료되었습니다.")
            input("Press Enter to exit...")

    sys.exit(exit_code)
