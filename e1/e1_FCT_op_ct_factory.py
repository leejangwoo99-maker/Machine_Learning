# -*- coding: utf-8 -*-
"""
FCT OP-CT Boxplot Summary + UPH(병렬합산) 계산 + PostgreSQL 저장 스크립트
- 실시간 무한루프(1초)
- 멀티프로세스 2개 고정
- end_day = 오늘 날짜 기준 같은 월(YYYYMM)만 처리
  예) 20251218 -> 2025-12-01 ~ 2026-01-01 미만
- end_ts 기준 최근 120초 + cutoff_ts 이후만 처리
- 저장: 테이블 없으면 생성, 있으면 UPSERT로 갱신
- (추가) 실행 타이밍 2회:
    1) 08:27:00 ~ 08:29:59 구간에서만 1초 루프 실행
    2) 20:27:00 ~ 20:29:59 구간에서만 1초 루프 실행
    (그 외 시간에는 대기)

Source: a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history
Output:
  1) e1_FCT_ct.fct_op_ct         (summary_df2: plotly_json 포함)
  2) e1_FCT_ct.fct_whole_op_ct   (final_df_86: left/right/whole UPH/CTeq/final_ct)
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, date, time as dtime
import urllib.parse
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd

from sqlalchemy import create_engine, text
import plotly.express as px

import psycopg2
from psycopg2.extras import execute_values
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

# boxplot html 저장 폴더
OUT_DIR = Path("./fct_opct_boxplot_html")

# op_ct 상한(초)
OPCT_MAX_SEC = 600

# ===== 실시간 조건 =====
LOOP_INTERVAL_SEC = 1               # 1초마다 재실행
REALTIME_WINDOW_SEC = 120           # 현재 시간 기준 120초 이내
CUTOFF_TS = 1765501841.4473598      # (요청값) epoch seconds

# ===== 멀티프로세스 고정 =====
MAX_WORKERS = 2

# ===== 실행 타이밍(추가) =====
RUN_WINDOWS = [
    (dtime(8, 27, 0),  dtime(8, 29, 59)),
    (dtime(20, 27, 0), dtime(20, 29, 59)),
]


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

def ensure_tables(conn):
    """
    실시간 루프에서 계속 갱신해야 하므로
    - 테이블이 없으면 생성
    - PK 걸고 UPSERT 가능하게 구성
    """
    ensure_schema(conn, TARGET_SCHEMA)

    with conn.cursor() as cur:
        # 1) fct_op_ct : (station, remark, month) PK
        cur.execute(sql.SQL(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TBL_OPCT} (
                station             TEXT NOT NULL,
                remark              TEXT NOT NULL,
                month               TEXT NOT NULL,
                sample_amount       INTEGER,
                op_ct_lower_outlier TEXT,
                q1                  DOUBLE PRECISION,
                median              DOUBLE PRECISION,
                q3                  DOUBLE PRECISION,
                op_ct_upper_outlier TEXT,
                del_out_op_ct_av    DOUBLE PRECISION,
                plotly_json         TEXT,
                PRIMARY KEY (station, remark, month)
            )
        """))

        # 2) fct_whole_op_ct : (station, remark, month) PK  (station=left/right/whole)
        cur.execute(sql.SQL(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TBL_WHOLE} (
                station   TEXT NOT NULL,
                remark    TEXT NOT NULL,
                month     TEXT NOT NULL,
                ct_eq     DOUBLE PRECISION,
                uph       DOUBLE PRECISION,
                final_ct  DOUBLE PRECISION,
                PRIMARY KEY (station, remark, month)
            )
        """))

    conn.commit()

def _sanitize_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()
    if "month" in df2.columns:
        df2["month"] = df2["month"].astype(str)

    for c in df2.columns:
        if isinstance(df2[c].dtype, CategoricalDtype):
            df2[c] = df2[c].astype(str)
    return df2

def upsert_df_psycopg2(df: pd.DataFrame, schema_name: str, table_name: str, key_cols, conn):
    """
    df 전체를 (key_cols) 기준으로 UPSERT
    """
    if df is None or len(df) == 0:
        log(f"[SKIP] {schema_name}.{table_name}: df가 비어있습니다.")
        return

    df2 = _sanitize_df_for_db(df)
    cols = list(df2.columns)

    values = [tuple(x) for x in df2.to_numpy()]
    col_ident = sql.SQL(",").join(map(sql.Identifier, cols))

    table_ident = sql.SQL("{}.{}").format(sql.Identifier(schema_name), sql.Identifier(table_name))
    key_ident = sql.SQL(",").join(map(sql.Identifier, key_cols))

    update_cols = [c for c in cols if c not in key_cols]
    if update_cols:
        set_clause = sql.SQL(",").join(
            sql.SQL("{}=EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c))
            for c in update_cols
        )
    else:
        set_clause = sql.SQL("")

    q = sql.SQL("""
        INSERT INTO {table} ({cols})
        VALUES %s
        ON CONFLICT ({keys}) DO UPDATE SET {set_clause}
    """).format(
        table=table_ident,
        cols=col_ident,
        keys=key_ident,
        set_clause=set_clause
    )

    with conn.cursor() as cur:
        execute_values(cur, q.as_string(conn), values, page_size=5000)
    conn.commit()

    log(f"[UPSERT] {schema_name}.{table_name} 갱신 완료 (rows={len(df2)})")

# ---- 실행 타이밍 헬퍼(추가) ----
def _now_time() -> dtime:
    return datetime.now().time().replace(microsecond=0)

def _is_in_run_window(t: dtime) -> bool:
    for start_t, end_t in RUN_WINDOWS:
        if start_t <= t <= end_t:
            return True
    return False

def _seconds_until_next_window(t: dtime) -> int:
    if _is_in_run_window(t):
        return 0

    now_sec = t.hour * 3600 + t.minute * 60 + t.second
    starts = []
    for start_t, _ in RUN_WINDOWS:
        s = start_t.hour * 3600 + start_t.minute * 60 + start_t.second
        starts.append(s)

    future = [s for s in starts if s > now_sec]
    if future:
        return min(future) - now_sec

    return (24 * 3600 - now_sec) + min(starts)


# =========================
# 2) 소스 로딩 + op_ct 계산 (이번달 + 최근 120초 + cutoff 이후)
# =========================
def load_source_df(engine) -> pd.DataFrame:
    """
    1) SQL에서 end_day = 오늘 기준 같은 월(이번달)만 가져옴(로드 최소화)
    2) end_ts 생성 후
       - end_ts >= (now-120s)
       - end_ts >= cutoff_ts
       둘 다 만족하는 것만 남김
    """
    log("[1/6] DB에서 원본 데이터(이번달) 로딩 시작...")

    # (추가) 이번달 범위: [월초, 다음달 월초)
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
        end_day >= date_trunc('month', CURRENT_DATE)::date
        AND end_day <  (date_trunc('month', CURRENT_DATE) + interval '1 month')::date
        AND station IN ('FCT1','FCT2','FCT3','FCT4')
        AND remark IN ('PD','Non-PD')
        AND result <> 'FAIL'
        AND goodorbad <> 'BadFile'
    ORDER BY end_day ASC, end_time ASC
    """
    df = pd.read_sql(text(sql_query), engine)
    log(f"[OK] 이번달 데이터 로딩 완료 (rows={len(df)})")

    if len(df) == 0:
        return df

    log("[2/6] 타입 정리 및 end_ts 생성...")

    df["end_day"] = pd.to_datetime(df["end_day"]).dt.date
    df["end_time_str"] = df["end_time"].astype(str)

    df["end_ts"] = pd.to_datetime(
        df["end_day"].astype(str) + " " + df["end_time_str"],
        errors="coerce"
    )

    n_bad_ts = int(df["end_ts"].isna().sum())
    if n_bad_ts > 0:
        log(f"[WARN] end_ts 파싱 실패 행 {n_bad_ts}개 (end_time 형식 확인 필요)")

    # ===== 실시간 필터 =====
    now_dt = datetime.now()
    window_start = now_dt - timedelta(seconds=REALTIME_WINDOW_SEC)
    cutoff_dt = datetime.fromtimestamp(float(CUTOFF_TS))

    threshold = max(window_start, cutoff_dt)
    before = len(df)
    df = df[df["end_ts"].notna() & (df["end_ts"] >= threshold)].copy()
    after = len(df)

    log(f"[INFO] realtime filter: end_ts >= {threshold.strftime('%Y-%m-%d %H:%M:%S')}  (kept {after}/{before})")

    if after == 0:
        return df

    df = df.sort_values(["station", "remark", "end_day", "end_ts"], ascending=True).reset_index(drop=True)
    df["op_ct"] = df.groupby(["station", "remark"])["end_ts"].diff().dt.total_seconds()
    df["month"] = pd.to_datetime(df["end_day"].astype(str)).dt.strftime("%Y%m")

    log("[OK] op_ct / month 생성 완료")
    return df


# =========================
# 3) Boxplot 요약 + html 저장 (MP=2)
# =========================
def _range_str(values: pd.Series):
    values = values.dropna()
    if len(values) == 0:
        return None
    vmin = float(values.min())
    vmax = float(values.max())
    return f"{vmin:.1f}~{vmax:.1f}"

def _summarize_group_worker(args):
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
    log(f"[3/6] (station, remark, month) 요약 생성 시작... (MP={MAX_WORKERS})")

    if df_raw is None or len(df_raw) == 0:
        return pd.DataFrame(columns=[
            "id","station","remark","month","sample_amount","op_ct_lower_outlier","q1","median","q3",
            "op_ct_upper_outlier","del_out_op_ct_av","html"
        ])

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
            summary_rows.append(fut.result())
            done += 1
            if done == 1 or done == total or done % 20 == 0:
                log(f"[PROGRESS] summary {done}/{total} ...")

    summary_df = pd.DataFrame(summary_rows)
    summary_df = summary_df.sort_values(["month", "remark", "station"]).reset_index(drop=True)
    summary_df.insert(0, "id", range(1, len(summary_df) + 1))

    log(f"[OK] 요약 DF 생성 완료 (rows={len(summary_df)})")
    return summary_df


# =========================
# 4) plotly_json 컬럼 생성 (html 제거) (MP=2)
# =========================
def _make_boxplot_json_worker(args):
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
    return idx, fig.to_json()

def build_plotly_json_column(df_raw: pd.DataFrame, summary_df: pd.DataFrame) -> pd.DataFrame:
    log(f"[4/6] plotly_json 생성 시작... (MP={MAX_WORKERS})")

    if summary_df is None or len(summary_df) == 0:
        out = summary_df.copy() if summary_df is not None else pd.DataFrame()
        if len(out) > 0:
            out["plotly_json"] = None
            if "html" in out.columns:
                out = out.drop(columns=["html"])
        return out

    out = summary_df.copy()
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

    if summary_df2 is None or len(summary_df2) == 0:
        return pd.DataFrame(columns=["id","station","remark","month","ct_eq","uph","final_ct"])

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
# 6) 1회 실행(루프 내부에서 호출)
# =========================
def run_once(engine):
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    df_raw = load_source_df(engine)
    if df_raw is None or len(df_raw) == 0:
        log("[INFO] 처리 대상 데이터 없음 (이번달/120초/cutoff 조건).")
        return

    summary_df = build_summary_df(df_raw)
    summary_df2 = build_plotly_json_column(df_raw, summary_df)
    final_df_86 = build_final_df_86(summary_df2)

    log("[6/6] DB 저장(UPSERT) 단계 시작...")

    with psycopg2.connect(**DB_CONFIG) as conn:
        ensure_tables(conn)

        if len(summary_df2) > 0:
            cols_need = [
                "station","remark","month","sample_amount","op_ct_lower_outlier","q1","median","q3",
                "op_ct_upper_outlier","del_out_op_ct_av","plotly_json"
            ]
            df_save1 = summary_df2[[c for c in cols_need if c in summary_df2.columns]].copy()
            upsert_df_psycopg2(df_save1, TARGET_SCHEMA, TBL_OPCT, key_cols=["station","remark","month"], conn=conn)

        if len(final_df_86) > 0:
            cols_need2 = ["station","remark","month","ct_eq","uph","final_ct"]
            df_save2 = final_df_86[[c for c in cols_need2 if c in final_df_86.columns]].copy()
            upsert_df_psycopg2(df_save2, TARGET_SCHEMA, TBL_WHOLE, key_cols=["station","remark","month"], conn=conn)

    log("=== 1-cycle DONE ===")


# =========================
# 7) main (타임윈도우 기반 1초 루프)
# =========================
def main():
    try:
        log("=== FCT OP-CT Realtime Loop START (Scheduled) ===")
        log(f"[INFO] MP workers = {MAX_WORKERS} (fixed)")
        log(f"[INFO] end_day = this month (based on today={date.today()})")
        log(f"[INFO] realtime window = {REALTIME_WINDOW_SEC}s, cutoff_ts = {CUTOFF_TS}")
        log(f"[INFO] run_windows = {RUN_WINDOWS}")

        engine = get_engine(DB_CONFIG)

        while True:
            now_t = _now_time()

            # 윈도우 밖이면 대기
            if not _is_in_run_window(now_t):
                wait_sec = _seconds_until_next_window(now_t)
                log(f"[WAIT] now={now_t} -> next window in {wait_sec}s")

                # 시작 시각 정밀하게 맞추기 위해 1초 단위 체크
                while wait_sec > 0:
                    time.sleep(1)
                    wait_sec -= 1
                    now_t = _now_time()
                    if _is_in_run_window(now_t):
                        break
                continue

            # 윈도우 안: 1초 주기 실행
            tick = time.time()
            run_once(engine)

            elapsed = time.time() - tick
            sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

    except KeyboardInterrupt:
        log("[STOP] KeyboardInterrupt")
        sys.exit(0)
    except Exception as e:
        log(f"[ERROR] {type(e).__name__}: {e}")
        sys.exit(1)


if __name__ == "__main__":
    # Windows 멀티프로세스 안전
    from multiprocessing import freeze_support
    freeze_support()
    main()
