# -*- coding: utf-8 -*-
"""
Vision RunTime CT (intensity8) 분석 파이프라인 - REALTIME(5s loop) + MP=2 고정 + LATEST/HIST 저장

[Source]
- a3_vision_table.vision_table

[Filter(기본)]
- barcode_information LIKE 'B%'
- station IN ('Vision1','Vision2')
- remark IN ('PD','Non-PD')
- step_description = 'intensity8'
- COALESCE(result,'') <> 'FAIL'
- end_day: "현재 달(YYYYMM)" 데이터만 대상 (월 롤오버 시 캐시 리셋)

[Summary (station, remark, month)]
- sample_amount
- IQR 기반 outlier 범위 문자열
- q1/median/q3
- outlier 제거 평균(del_out_run_time_av)
- plotly_json (boxplot, validate=False)

[Save 정책]
1) LATEST: e2_vision_ct.vision_run_time_ct
   - UNIQUE (station, remark, month)
   - ON CONFLICT DO UPDATE (최신값 갱신)
   - created_at/updated_at 포함

2) HIST(하루 1개 롤링): e2_vision_ct.vision_run_time_ct_hist
   - UNIQUE (snapshot_day, station, remark, month)
   - ON CONFLICT DO UPDATE (그날 마지막 값만 유지)
   - snapshot_ts 포함

[id NULL 해결]
- latest/hist 모두 id 자동채번 보정(컬럼/시퀀스/default/NULL 채움/setval)

[Runtime]
- 무한 루프(기본 5초)
- 신규 데이터가 들어올 때만 요약/UPSERT 수행
- 예외 발생 시 콘솔이 자동으로 닫히지 않도록 "hold_console_open" 적용
"""

import sys
import urllib.parse
from datetime import datetime, date
import time as time_mod
from multiprocessing import get_context

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

import plotly.graph_objects as go
import psycopg2
from psycopg2.extras import execute_values


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

SRC_SCHEMA = "a3_vision_table"
SRC_TABLE = "vision_table"

TARGET_SCHEMA = "e2_vision_ct"
TBL_LATEST = "vision_run_time_ct"
TBL_HIST = "vision_run_time_ct_hist"

STEP_DESC = "intensity8"

# 멀티프로세스 2개 고정
PROC_1_STATIONS = ["Vision1"]
PROC_2_STATIONS = ["Vision2"]

# 루프/조회 제한
LOOP_INTERVAL_SEC = 5
FETCH_LIMIT = 200000


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)


def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


def current_yyyymm(dt: datetime | None = None) -> str:
    """dt가 주어지면 dt 기준 YYYYMM, 없으면 현재시간 기준 YYYYMM"""
    base = dt if dt is not None else datetime.now()
    return base.strftime("%Y%m")


def get_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str, pool_pre_ping=True)


def get_conn_pg(cfg=DB_CONFIG):
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )


def _make_plotly_json(values: np.ndarray, name: str) -> str:
    """
    EXE(onefile)에서 plotly validators 오류 방지 -> validate=False
    """
    fig = go.Figure()
    fig.add_trace(go.Box(y=values.astype(float), name=name, boxpoints=False))
    return fig.to_json(validate=False)


def hold_console_open(exit_code: int = 1):
    """
    어떤 환경에서도(더블클릭/스케줄러/서비스) 에러 시 콘솔이 바로 닫히지 않게 보장.
    - stdin이 있으면 Enter 대기
    - stdin이 없으면 무한 sleep로 콘솔 유지
    """
    try:
        log("\n[HOLD] 프로그램이 종료되지 않도록 대기합니다.")
        log("[HOLD] Enter 입력 가능하면 Enter를 누르세요. 입력이 불가한 환경이면 계속 유지됩니다.")
        try:
            input()
            raise SystemExit(exit_code)
        except Exception:
            while True:
                time_mod.sleep(60)
    except SystemExit:
        raise
    except Exception:
        while True:
            time_mod.sleep(60)


# =========================
# 2) 테이블/인덱스/ID 보정
# =========================
def ensure_tables_and_indexes():
    """
    - 스키마 생성
    - LATEST/HIST 테이블 생성(없으면)
    - ✅ 이미 테이블이 있어도 컬럼 자동 동기화(ADD COLUMN IF NOT EXISTS)
    - UNIQUE 인덱스 보장
    - id 자동채번/NULL 보정
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = False
        with conn.cursor() as cur:
            cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{TARGET_SCHEMA}";')

            # --- LATEST: create minimal table if not exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_LATEST} (
                    id BIGINT,
                    station TEXT NOT NULL,
                    remark  TEXT NOT NULL,
                    month   TEXT NOT NULL
                );
            """)

            # --- LATEST: column sync (기존 테이블이어도 컬럼 추가)
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS sample_amount INTEGER;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS run_time_lower_outlier TEXT;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS q1 DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS median DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS q3 DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS run_time_upper_outlier TEXT;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS del_out_run_time_av DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS plotly_json JSONB;')

            # ✅ 문제의 created_at/updated_at 추가 + default
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;')
            cur.execute(f"""
                ALTER TABLE "{TARGET_SCHEMA}".{TBL_LATEST}
                ALTER COLUMN created_at SET DEFAULT now(),
                ALTER COLUMN updated_at SET DEFAULT now();
            """)
            # 기존 row NULL 보정
            cur.execute(f'UPDATE "{TARGET_SCHEMA}".{TBL_LATEST} SET created_at = COALESCE(created_at, now());')
            cur.execute(f'UPDATE "{TARGET_SCHEMA}".{TBL_LATEST} SET updated_at = COALESCE(updated_at, now());')

            # UNIQUE index
            cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_LATEST}_key
                ON "{TARGET_SCHEMA}".{TBL_LATEST} (station, remark, month);
            """)

            # --- HIST: create minimal table if not exists
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS "{TARGET_SCHEMA}".{TBL_HIST} (
                    id BIGINT,
                    snapshot_day TEXT NOT NULL,
                    station TEXT NOT NULL,
                    remark  TEXT NOT NULL,
                    month   TEXT NOT NULL
                );
            """)

            # --- HIST: column sync
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS snapshot_ts TIMESTAMPTZ;')
            cur.execute(f"""
                ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST}
                ALTER COLUMN snapshot_ts SET DEFAULT now();
            """)
            cur.execute(f'UPDATE "{TARGET_SCHEMA}".{TBL_HIST} SET snapshot_ts = COALESCE(snapshot_ts, now());')

            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS sample_amount INTEGER;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS run_time_lower_outlier TEXT;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS q1 DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS median DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS q3 DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS run_time_upper_outlier TEXT;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS del_out_run_time_av DOUBLE PRECISION;')
            cur.execute(f'ALTER TABLE "{TARGET_SCHEMA}".{TBL_HIST} ADD COLUMN IF NOT EXISTS plotly_json JSONB;')

            # UNIQUE index
            cur.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{TBL_HIST}_day_key
                ON "{TARGET_SCHEMA}".{TBL_HIST} (snapshot_day, station, remark, month);
            """)

        conn.commit()

    fix_id_sequence(TARGET_SCHEMA, TBL_LATEST, "vision_run_time_ct_id_seq")
    fix_id_sequence(TARGET_SCHEMA, TBL_HIST, "vision_run_time_ct_hist_id_seq")

    log(f'[OK] target tables ensured in schema "{TARGET_SCHEMA}"')


def fix_id_sequence(schema: str, table: str, seq_name: str):
    """
    - id 컬럼 없으면 추가
    - 시퀀스 없으면 생성
    - id default nextval 강제
    - id NULL인 기존 행은 nextval로 채움
    - 시퀀스 setval = max(id)+1 로 동기화
    """
    do_sql = f"""
    DO $$
    DECLARE
      v_schema TEXT := {schema!r};
      v_table  TEXT := {table!r};
      v_seq    TEXT := {seq_name!r};
      v_full_table TEXT := quote_ident(v_schema) || '.' || quote_ident(v_table);
      v_full_seq   TEXT := quote_ident(v_schema) || '.' || quote_ident(v_seq);
      v_max BIGINT;
    BEGIN
      IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = v_schema
          AND table_name = v_table
          AND column_name = 'id'
      ) THEN
        EXECUTE 'ALTER TABLE ' || v_full_table || ' ADD COLUMN id BIGINT';
      END IF;

      IF NOT EXISTS (
        SELECT 1
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'S'
          AND n.nspname = v_schema
          AND c.relname = v_seq
      ) THEN
        EXECUTE 'CREATE SEQUENCE ' || v_full_seq;
      END IF;

      EXECUTE 'ALTER TABLE ' || v_full_table ||
              ' ALTER COLUMN id SET DEFAULT nextval(''' || v_full_seq || ''')';

      EXECUTE 'ALTER SEQUENCE ' || v_full_seq ||
              ' OWNED BY ' || v_full_table || '.id';

      EXECUTE 'UPDATE ' || v_full_table || ' SET id = nextval(''' || v_full_seq || ''') WHERE id IS NULL';

      EXECUTE 'SELECT COALESCE(MAX(id), 0) FROM ' || v_full_table INTO v_max;
      EXECUTE 'SELECT setval(''' || v_full_seq || ''', ' || (v_max + 1) || ', false)';
    END $$;
    """
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(do_sql)
        conn.commit()


# =========================
# 3) Worker: station subset 월 전체 로드 + 요약
# =========================
def load_source_month(engine, stations: list[str], run_month: str) -> pd.DataFrame:
    q = text(f"""
    SELECT
        barcode_information,
        station,
        remark,
        step_description,
        result,
        end_day,
        end_time,
        run_time
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE barcode_information LIKE 'B%%'
      AND station = ANY(:stations)
      AND remark IN ('PD','Non-PD')
      AND step_description = :step_desc
      AND COALESCE(result,'') <> 'FAIL'
      AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
    ORDER BY end_day ASC, end_time ASC
    """)
    df = pd.read_sql(q, engine, params={"stations": stations, "run_month": run_month, "step_desc": STEP_DESC})
    if df.empty:
        return df

    df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
    df["end_time"] = df["end_time"].astype(str).str.strip()
    df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")
    df = df.dropna(subset=["run_time"]).copy()

    dt_str = df["end_day"] + " " + df["end_time"]
    df["end_dt"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")
    df = df.dropna(subset=["end_dt"]).copy()

    df = df.sort_values(["end_dt", "station", "remark"]).reset_index(drop=True)
    df["month"] = df["end_dt"].dt.strftime("%Y%m")
    return df


def _summary_from_group(station: str, remark: str, month: str, series: pd.Series) -> dict | None:
    vals = series.dropna().astype(float).to_numpy()
    if vals.size == 0:
        return None

    q1 = float(np.percentile(vals, 25))
    med = float(np.percentile(vals, 50))
    q3 = float(np.percentile(vals, 75))
    iqr = q3 - q1

    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    lower_out = vals[vals < lower_fence]
    upper_out = vals[vals > upper_fence]

    run_time_lower_outlier = f"{lower_out.min():.2f}~{lower_fence:.2f}" if lower_out.size > 0 else None
    run_time_upper_outlier = f"{upper_fence:.2f}~{upper_out.max():.2f}" if upper_out.size > 0 else None

    inliers = vals[(vals >= lower_fence) & (vals <= upper_fence)]
    del_out_mean = float(np.mean(inliers)) if inliers.size > 0 else np.nan

    plotly_json = _make_plotly_json(vals, name=f"{station}_{remark}_{month}")

    return {
        "station": station,
        "remark": remark,
        "month": str(month),
        "sample_amount": int(vals.size),
        "run_time_lower_outlier": run_time_lower_outlier,
        "q1": round(q1, 2),
        "median": round(med, 2),
        "q3": round(q3, 2),
        "run_time_upper_outlier": run_time_upper_outlier,
        "del_out_run_time_av": round(del_out_mean, 2) if not np.isnan(del_out_mean) else None,
        "plotly_json": plotly_json,
    }


def build_summary(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    rows = []
    groups = list(df.groupby(["station", "remark", "month"], sort=True))
    total = len(groups)
    log(f"[INFO] 그룹 수 = {total}")

    for i, ((st, rk, mo), g) in enumerate(groups, start=1):
        if i == 1 or i == total or i % 20 == 0:
            log(f"[PROGRESS] group {i}/{total} ... ({st},{rk},{mo})")
        r = _summary_from_group(st, rk, mo, g["run_time"])
        if r is not None:
            rows.append(r)

    out = pd.DataFrame(rows)
    if out.empty:
        return out
    return out.sort_values(["month", "station", "remark"]).reset_index(drop=True)


def worker(stations: list[str], run_month: str) -> pd.DataFrame:
    eng = get_engine(DB_CONFIG)
    try:
        df = load_source_month(eng, stations=stations, run_month=run_month)
        if df is None or df.empty:
            return pd.DataFrame()
        return build_summary(df)
    finally:
        try:
            eng.dispose()
        except Exception:
            pass


# =========================
# 4) 저장: LATEST/HIST UPSERT
# =========================
def upsert_latest(summary_df: pd.DataFrame):
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
        "plotly_json",
    ]
    df = df[cols]
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_LATEST} (
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json,
        created_at, updated_at
    )
    VALUES %s
    ON CONFLICT (station, remark, month)
    DO UPDATE SET
        sample_amount          = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1                     = EXCLUDED.q1,
        median                 = EXCLUDED.median,
        q3                     = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av    = EXCLUDED.del_out_run_time_av,
        plotly_json            = EXCLUDED.plotly_json,
        updated_at             = now();
    """
    template = "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb, now(), now())"

    conn = get_conn_pg(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=2000)
        conn.commit()
    finally:
        conn.close()


def upsert_hist_daily(summary_df: pd.DataFrame, snapshot_day: str):
    if summary_df is None or summary_df.empty:
        return

    df = summary_df.where(pd.notnull(summary_df), None).copy()
    cols = [
        "station", "remark", "month",
        "sample_amount",
        "run_time_lower_outlier",
        "q1", "median", "q3",
        "run_time_upper_outlier",
        "del_out_run_time_av",
        "plotly_json",
    ]
    df = df[cols]
    df.insert(0, "snapshot_day", snapshot_day)
    rows = [tuple(r) for r in df.itertuples(index=False, name=None)]

    insert_sql = f"""
    INSERT INTO "{TARGET_SCHEMA}".{TBL_HIST} (
        snapshot_day, snapshot_ts,
        station, remark, month,
        sample_amount,
        run_time_lower_outlier,
        q1, median, q3,
        run_time_upper_outlier,
        del_out_run_time_av,
        plotly_json
    )
    VALUES %s
    ON CONFLICT (snapshot_day, station, remark, month)
    DO UPDATE SET
        snapshot_ts            = now(),
        sample_amount          = EXCLUDED.sample_amount,
        run_time_lower_outlier = EXCLUDED.run_time_lower_outlier,
        q1                     = EXCLUDED.q1,
        median                 = EXCLUDED.median,
        q3                     = EXCLUDED.q3,
        run_time_upper_outlier = EXCLUDED.run_time_upper_outlier,
        del_out_run_time_av    = EXCLUDED.del_out_run_time_av,
        plotly_json            = EXCLUDED.plotly_json;
    """
    template = "(%s, now(), %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)"

    conn = get_conn_pg(DB_CONFIG)
    try:
        conn.autocommit = False
        with conn.cursor() as cur:
            if rows:
                execute_values(cur, insert_sql, rows, template=template, page_size=2000)
        conn.commit()
    finally:
        conn.close()


# =========================
# 5) main: REALTIME LOOP
# =========================
def main():
    start_dt = datetime.now()
    log(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=== REALTIME MODE: current-month incremental + periodic UPSERT ===")
    log(f"loop_interval={LOOP_INTERVAL_SEC}s | fetch_limit={FETCH_LIMIT}")

    ensure_tables_and_indexes()

    run_month = current_yyyymm()
    snapshot_day = today_yyyymmdd()

    # last_ts: (station, remark) -> pd.Timestamp
    last_ts: dict[tuple[str, str], pd.Timestamp] = {}

    def _fetch_new_rows(engine, stations: list[str], run_month_local: str) -> pd.DataFrame:
        min_last = min(last_ts.values()) if last_ts else None

        extra_where = ""
        params = {
            "stations": stations,
            "run_month": run_month_local,
            "step_desc": STEP_DESC,
            "limit": FETCH_LIMIT,
        }
        if min_last is not None:
            extra_where = """
              AND ( (regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') || ' ' || COALESCE(end_time::text,''))::timestamp > :min_last )
            """
            params["min_last"] = str(min_last)

        sql_query = f"""
        SELECT
            barcode_information,
            station,
            remark,
            end_day,
            end_time,
            run_time
        FROM {SRC_SCHEMA}.{SRC_TABLE}
        WHERE barcode_information LIKE 'B%%'
          AND station = ANY(:stations)
          AND remark IN ('PD','Non-PD')
          AND step_description = :step_desc
          AND COALESCE(result,'') <> 'FAIL'
          AND substring(regexp_replace(COALESCE(end_day::text,''), '\\D', '', 'g') from 1 for 6) = :run_month
          {extra_where}
        ORDER BY end_day ASC, end_time ASC
        LIMIT :limit
        """
        df = pd.read_sql(text(sql_query), engine, params=params)
        if df is None or df.empty:
            return df

        df["end_day"] = df["end_day"].astype(str).str.replace(r"\D", "", regex=True)
        df["end_time"] = df["end_time"].astype(str).str.strip()
        df["run_time"] = pd.to_numeric(df["run_time"], errors="coerce")

        dt_str = df["end_day"] + " " + df["end_time"]
        df["end_ts"] = pd.to_datetime(dt_str, errors="coerce", format="mixed")
        df = df.dropna(subset=["end_ts"]).copy()
        df = df.dropna(subset=["run_time"]).copy()

        keep_parts = []
        for (st, rm), g in df.groupby(["station", "remark"], sort=False):
            lt = last_ts.get((st, rm))
            if lt is None:
                keep_parts.append(g)
            else:
                keep_parts.append(g[g["end_ts"] > lt])

        if not keep_parts:
            return df.iloc[0:0].copy()
        return pd.concat(keep_parts, ignore_index=True)

    def _update_last_ts(df_new: pd.DataFrame):
        for (st, rm), g in df_new.groupby(["station", "remark"], sort=False):
            last_ts[(st, rm)] = g["end_ts"].max()

    ctx = get_context("spawn")
    stations_all = list(set(PROC_1_STATIONS + PROC_2_STATIONS))

    engine = get_engine(DB_CONFIG)

    try:
        while True:
            now = datetime.now()

            cur_month = current_yyyymm(now)
            if cur_month != run_month:
                log(f"[MONTH ROLLOVER] {run_month} -> {cur_month} (cache reset)")
                run_month = cur_month
                snapshot_day = today_yyyymmdd()
                last_ts.clear()

            df_new = _fetch_new_rows(engine, stations_all, run_month)

            if df_new is not None and not df_new.empty:
                _update_last_ts(df_new)

                log(f"[NEW] rows={len(df_new)} | month={run_month}")

                # MP=2로 요약 생성(월 전체를 station별로 나눠 계산)
                with ctx.Pool(processes=2) as pool:
                    tasks = [(PROC_1_STATIONS, run_month), (PROC_2_STATIONS, run_month)]
                    results = pool.starmap(worker, tasks)

                summary_all = pd.concat(results, ignore_index=True)
                if summary_all is not None and not summary_all.empty:
                    upsert_latest(summary_all)
                    upsert_hist_daily(summary_all, snapshot_day=snapshot_day)
                    log("[UPSERT] latest + hist OK")
                else:
                    log("[SKIP] summary 없음 -> 저장 생략")

            time_mod.sleep(LOOP_INTERVAL_SEC)

    finally:
        try:
            engine.dispose()
        except Exception:
            pass


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log("[INTERRUPT] 사용자 중단")
        hold_console_open(0)
    except Exception:
        log("\n[ERROR] 예외 발생")
        import traceback
        traceback.print_exc()
        hold_console_open(1)
