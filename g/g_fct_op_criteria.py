# -*- coding: utf-8 -*-
"""
backend_fct_op_criteria_daemon.py

FCT SWAP time monthly criteria daemon
- Current month only (KST)
- Incremental fetch with cursor (end_ts) and PK dedup (end_day, station, end_time)
- In-memory accumulate swap_sec per (month, table)
- UPSERT to g_production_film.fct_op_criteria ONLY when new data exists
- Persist state locally to resume from cursor after restart (no full rescan)

+ Added DB health log persistence
  - schema: k_demon_heath_check (auto create)
  - table : gc_log (auto create)
  - columns: end_day(yyyymmdd), end_time(hh:mi:ss), info(lowercase), contents
  - saved in DataFrame column order: end_day, end_time, info, contents
"""

from __future__ import annotations

import os
import time
import gzip
import pickle
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP
from typing import Dict, List, Optional, Tuple, Set

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# =========================
# 0) TIMEZONE / LOG
# =========================
KST = ZoneInfo("Asia/Seoul")


def now_kst() -> datetime:
    return datetime.now(tz=KST)


# -------------------------
# DB Log target
# -------------------------
LOG_SCHEMA = "k_demon_heath_check"
LOG_TABLE = "gc_log"


def _format_log_row(level: str, msg: str) -> dict:
    n = now_kst()
    info = str(level).strip().lower()  # 반드시 소문자
    return {
        "end_day": n.strftime("%Y%m%d"),
        "end_time": n.strftime("%H:%M:%S"),
        "info": info,
        "contents": str(msg),
    }


def print_log(level: str, msg: str) -> None:
    ts = now_kst().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{str(level).upper()}] {msg}", flush=True)


def ensure_log_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{LOG_SCHEMA}";'))
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{LOG_SCHEMA}"."{LOG_TABLE}" (
            id bigserial PRIMARY KEY,
            end_day text NOT NULL,
            end_time text NOT NULL,
            info text NOT NULL,
            contents text,
            created_at timestamptz NOT NULL DEFAULT now()
        );
        """))


def save_log_to_db(engine: Optional[Engine], level: str, msg: str) -> None:
    """
    요구사항:
    1) 로그 생성
    2) end_day: yyyymmdd
    3) end_time: hh:mi:ss
    4) info: 소문자
    5) contents: 나머지 내용
    6) end_day, end_time, info, contents 순서로 dataframe화 저장
    7) schema/table 생성 후 저장
    """
    if engine is None:
        return

    try:
        row = _format_log_row(level, msg)
        df = pd.DataFrame([row], columns=["end_day", "end_time", "info", "contents"])
        sql = text(f"""
        INSERT INTO "{LOG_SCHEMA}"."{LOG_TABLE}" (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
        """)
        with engine.begin() as conn:
            conn.execute(sql, df.to_dict(orient="records"))
    except Exception as e:
        # DB 로그 저장 실패는 콘솔만 남기고 메인 로직 영향 없게 처리
        print_log("retry", f"log db insert failed: {type(e).__name__}: {e}")


def log(level: str, msg: str, engine: Optional[Engine] = None) -> None:
    print_log(level, msg)
    save_log_to_db(engine, level, msg)


# =========================
# 1) CONFIG
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SRC_SCHEMA = "d1_machine_log"
TABLES_FCT = ["FCT1_machine_log", "FCT2_machine_log", "FCT3_machine_log", "FCT4_machine_log"]

RELEASE = "제품 지그 해제 완료"
ON = "제품 안착 완료 ON"

MAX_SEC = 500.0  # delta > 500 제외
SLEEP_SEC = 5

# work_mem: 환경변수 있으면 사용, 없으면 4MB 고정
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# 저장 대상
SAVE_SCHEMA = "g_production_film"
SAVE_TABLE = "fct_op_criteria"

# 상태파일 저장 경로
STATE_DIR = os.getenv("FCT_OP_STATE_DIR", ".")


# =========================
# 2) ENGINE (pool=1 + work_mem)
# =========================
def make_engine(cfg: dict) -> Engine:
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )
    engine = create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn, _connection_record):
        cur = dbapi_conn.cursor()
        try:
            cur.execute(f"SET work_mem = '{WORK_MEM}';")
        finally:
            cur.close()

    return engine


def connect_with_retry() -> Engine:
    print_log("boot", "backend fct_op_criteria daemon starting")
    while True:
        try:
            engine = make_engine(DB_CONFIG)
            with engine.begin() as conn:
                conn.execute(text("SELECT 1;"))
            # 로그 테이블 보장 후부터 DB 로그도 적재
            ensure_log_table(engine)
            log("info", f"DB connected (work_mem={WORK_MEM})", engine)
            return engine
        except Exception as e:
            print_log("retry", f"DB connect failed: {type(e).__name__}: {e} (retry in {SLEEP_SEC}s)")
            time.sleep(SLEEP_SEC)


# =========================
# 3) UTILS
# =========================
def round_half_up(x: float, ndigits: int = 2) -> float:
    q = Decimal("1").scaleb(-ndigits)
    return float(Decimal(str(x)).quantize(q, rounding=ROUND_HALF_UP))


def tukey_five_number(values: List[float]) -> Dict[str, Optional[float]]:
    """
    lower_outlier, q1, median, q3, upper_outlier (whisker)
    + upper_outlier_max + upper_outlier_range("whisker~max_outlier")
    """
    if not values:
        return {
            "lower_outlier": None, "q1": None, "median": None, "q3": None, "upper_outlier": None,
            "upper_outlier_max": None, "upper_outlier_range": None
        }

    arr = np.asarray(values, dtype=float)
    arr.sort()

    q1 = float(np.quantile(arr, 0.25, method="linear"))
    med = float(np.quantile(arr, 0.50, method="linear"))
    q3 = float(np.quantile(arr, 0.75, method="linear"))
    iqr = q3 - q1

    lower_fence = q1 - 1.5 * iqr
    upper_fence = q3 + 1.5 * iqr

    inlier = arr[(arr >= lower_fence) & (arr <= upper_fence)]
    if inlier.size == 0:
        lower_w = float(arr.min())
        upper_w = float(arr.max())
    else:
        lower_w = float(inlier.min())
        upper_w = float(inlier.max())

    upper_outliers = arr[arr > upper_w]
    upper_outlier_max = float(upper_outliers.max()) if upper_outliers.size > 0 else None

    lower_w_r = round_half_up(lower_w, 2)
    q1_r = round_half_up(q1, 2)
    med_r = round_half_up(med, 2)
    q3_r = round_half_up(q3, 2)
    upper_w_r = round_half_up(upper_w, 2)

    if upper_outlier_max is not None:
        upper_outlier_max_r = round_half_up(upper_outlier_max, 2)
        upper_range = f"{upper_w_r:.2f}~{upper_outlier_max_r:.2f}"
    else:
        upper_outlier_max_r = None
        upper_range = f"{upper_w_r:.2f}~{upper_w_r:.2f}"

    return {
        "lower_outlier": lower_w_r,
        "q1": q1_r,
        "median": med_r,
        "q3": q3_r,
        "upper_outlier": upper_w_r,
        "upper_outlier_max": upper_outlier_max_r,
        "upper_outlier_range": upper_range,
    }


# =========================
# 4) STATE (cursor + values) persist locally
# =========================
@dataclass
class State:
    month: str
    cursor_ts_iso_by_table: Dict[str, str]                   # table -> ISO timestamp (no tz)
    pending_release_iso_by_table: Dict[str, Optional[str]]   # table -> ISO or None
    per_table_values: Dict[str, List[float]]                 # table -> swap_sec list
    seen_pk: Set[Tuple[str, str, str]]                       # (end_day, station, end_time)


def state_path(month: str) -> str:
    return os.path.join(STATE_DIR, f"state_fct_op_criteria_{month}.pkl.gz")


def save_state(st: State) -> None:
    p = state_path(st.month)
    tmp = p + ".tmp"
    payload = {
        "month": st.month,
        "cursor": st.cursor_ts_iso_by_table,
        "pending": st.pending_release_iso_by_table,
        "values": st.per_table_values,
        "seen_pk": st.seen_pk,
    }
    with gzip.open(tmp, "wb") as f:
        pickle.dump(payload, f, protocol=pickle.HIGHEST_PROTOCOL)
    os.replace(tmp, p)


def load_state(month: str, engine: Optional[Engine] = None) -> Optional[State]:
    p = state_path(month)
    if not os.path.exists(p):
        return None
    try:
        with gzip.open(p, "rb") as f:
            payload = pickle.load(f)
        return State(
            month=payload["month"],
            cursor_ts_iso_by_table=payload.get("cursor", {}),
            pending_release_iso_by_table=payload.get("pending", {}),
            per_table_values=payload.get("values", {}),
            seen_pk=payload.get("seen_pk", set()),
        )
    except Exception as e:
        log("info", f"state load failed (ignore & rebuild): {type(e).__name__}: {e}", engine)
        return None


def month_start_iso(month: str) -> str:
    y = int(month[:4]); m = int(month[4:6])
    return f"{y:04d}-{m:02d}-01 00:00:00"


def iso_to_dt_naive(iso: str) -> datetime:
    return datetime.fromisoformat(iso)


def dt_naive_to_iso(dt: datetime) -> str:
    return dt.isoformat(sep=" ", timespec="seconds")


# =========================
# 5) DB: ensure output table + upsert summary
# =========================
def ensure_schema_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{SAVE_SCHEMA}";'))
        conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS "{SAVE_SCHEMA}"."{SAVE_TABLE}" (
            month text NOT NULL,
            "table" text NOT NULL,
            n bigint,
            lower_outlier double precision,
            q1 double precision,
            median double precision,
            q3 double precision,
            upper_outlier double precision,
            upper_outlier_max double precision,
            upper_outlier_range text,
            updated_at timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT "{SAVE_TABLE}__uq" UNIQUE (month, "table")
        );
        """))


def upsert_df_summary(engine: Engine, df_summary: pd.DataFrame) -> None:
    cols = [
        "month", "table", "n",
        "lower_outlier", "q1", "median", "q3", "upper_outlier",
        "upper_outlier_max", "upper_outlier_range"
    ]
    df = df_summary.copy()[cols]
    df = df.where(pd.notnull(df), None)

    sql = text(f"""
    INSERT INTO "{SAVE_SCHEMA}"."{SAVE_TABLE}" (
        month, "table", n,
        lower_outlier, q1, median, q3, upper_outlier,
        upper_outlier_max, upper_outlier_range,
        updated_at
    )
    VALUES (
        :month, :table, :n,
        :lower_outlier, :q1, :median, :q3, :upper_outlier,
        :upper_outlier_max, :upper_outlier_range,
        now()
    )
    ON CONFLICT (month, "table") DO UPDATE SET
        n = EXCLUDED.n,
        lower_outlier = EXCLUDED.lower_outlier,
        q1 = EXCLUDED.q1,
        median = EXCLUDED.median,
        q3 = EXCLUDED.q3,
        upper_outlier = EXCLUDED.upper_outlier,
        upper_outlier_max = EXCLUDED.upper_outlier_max,
        upper_outlier_range = EXCLUDED.upper_outlier_range,
        updated_at = now();
    """)

    rows = df.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(sql, rows)


# =========================
# 6) FETCH incremental (end_ts > last_ts)
# =========================
def fetch_new_events(
    engine: Engine,
    schema: str,
    table: str,
    month: str,
    last_ts_iso: str
) -> pd.DataFrame:
    fqn = f'"{schema}"."{table}"'
    like = f"{month}%"

    sql = text(f"""
    WITH src AS (
        SELECT
            end_day,
            station,
            end_time,
            contents,
            to_timestamp(
                end_day || ' ' ||
                (
                    CASE
                        WHEN position('.' in end_time) > 0 THEN
                            split_part(end_time,'.',1) || '.' || rpad(split_part(end_time,'.',2), 3, '0')
                        ELSE
                            end_time || '.000'
                    END
                ),
                'YYYYMMDD HH24:MI:SS.MS'
            ) AS end_ts
        FROM {fqn}
        WHERE end_day LIKE :end_day_like
          AND end_time IS NOT NULL
          AND contents IN (:release, :on)
    )
    SELECT end_day, station, end_time, contents, end_ts
    FROM src
    WHERE end_ts > :last_ts
    ORDER BY end_ts ASC
    """)

    with engine.begin() as conn:
        df = pd.read_sql(
            sql, conn,
            params={
                "end_day_like": like,
                "release": RELEASE,
                "on": ON,
                "last_ts": last_ts_iso,
            }
        )
    return df


# =========================
# 7) PROCESS
# =========================
def process_table_incremental(
    st: State,
    engine: Engine,
    table: str,
) -> Tuple[int, int, str]:
    last_ts_iso = st.cursor_ts_iso_by_table.get(table) or month_start_iso(st.month)
    log("info", f"[last_pk] month={st.month} table={table} last_ts={last_ts_iso}", engine)

    df = fetch_new_events(engine, SRC_SCHEMA, table, st.month, last_ts_iso)
    fetched = int(len(df))
    if fetched == 0:
        return 0, 0, last_ts_iso

    keep_rows = []
    for _, r in df.iterrows():
        pk = (str(r["end_day"]), str(r["station"]), str(r["end_time"]))
        if pk in st.seen_pk:
            continue
        st.seen_pk.add(pk)
        keep_rows.append(r)

    if not keep_rows:
        new_cursor = df["end_ts"].max()
        new_cursor_iso = dt_naive_to_iso(pd.to_datetime(new_cursor).to_pydatetime())
        return fetched, 0, new_cursor_iso

    df2 = pd.DataFrame(keep_rows)
    new_cursor = df2["end_ts"].max()
    new_cursor_iso = dt_naive_to_iso(pd.to_datetime(new_cursor).to_pydatetime())

    pending_iso = st.pending_release_iso_by_table.get(table)
    pending_dt = iso_to_dt_naive(pending_iso) if pending_iso else None

    added = 0
    values = st.per_table_values.setdefault(table, [])

    for _, r in df2.iterrows():
        c = str(r["contents"])
        end_ts_dt = pd.to_datetime(r["end_ts"]).to_pydatetime()

        if c == RELEASE:
            pending_dt = end_ts_dt
        elif c == ON:
            if pending_dt is None:
                continue

            delta = (end_ts_dt - pending_dt).total_seconds()
            pending_dt = None

            if delta <= 0:
                continue
            if delta > MAX_SEC:
                continue

            values.append(round_half_up(delta, 2))
            added += 1

    st.pending_release_iso_by_table[table] = dt_naive_to_iso(pending_dt) if pending_dt else None
    return fetched, added, new_cursor_iso


# =========================
# 8) BUILD SUMMARY DF
# =========================
def build_df_summary_for_month(st: State) -> pd.DataFrame:
    rows = []

    for t in TABLES_FCT:
        vals = st.per_table_values.get(t, [])
        s = tukey_five_number(vals)
        rows.append({"month": st.month, "table": t, "n": len(vals), **s})

    all_vals: List[float] = []
    for t in TABLES_FCT:
        all_vals.extend(st.per_table_values.get(t, []))
    s_all = tukey_five_number(all_vals)
    rows.append({"month": st.month, "table": "ALL_FCT", "n": len(all_vals), **s_all})

    df_summary = pd.DataFrame(rows)
    cols = [
        "month", "table", "n",
        "lower_outlier", "q1", "median", "q3", "upper_outlier",
        "upper_outlier_max", "upper_outlier_range"
    ]
    return df_summary[[c for c in cols if c in df_summary.columns]]


# =========================
# 9) MAIN LOOP
# =========================
def run() -> None:
    engine = connect_with_retry()
    ensure_schema_table(engine)
    ensure_log_table(engine)

    month = now_kst().strftime("%Y%m")
    st0 = load_state(month, engine)

    if st0 is None:
        st = State(
            month=month,
            cursor_ts_iso_by_table={},
            pending_release_iso_by_table={t: None for t in TABLES_FCT},
            per_table_values={t: [] for t in TABLES_FCT},
            seen_pk=set(),
        )
        save_state(st)
        log("info", f"[state] new state initialized for month={month}", engine)
    else:
        st = st0
        for t in TABLES_FCT:
            st.pending_release_iso_by_table.setdefault(t, None)
            st.per_table_values.setdefault(t, [])
        log("info", f"[state] loaded state for month={month} (seen_pk={len(st.seen_pk)})", engine)

    while True:
        try:
            cur_month = now_kst().strftime("%Y%m")
            if cur_month != st.month:
                log("info", f"[window] month changed {st.month} -> {cur_month} (reset state)", engine)
                st2 = load_state(cur_month, engine)
                if st2 is None:
                    st = State(
                        month=cur_month,
                        cursor_ts_iso_by_table={},
                        pending_release_iso_by_table={t: None for t in TABLES_FCT},
                        per_table_values={t: [] for t in TABLES_FCT},
                        seen_pk=set(),
                    )
                    save_state(st)
                    log("info", f"[state] new state initialized for month={cur_month}", engine)
                else:
                    st = st2
                    for t in TABLES_FCT:
                        st.pending_release_iso_by_table.setdefault(t, None)
                        st.per_table_values.setdefault(t, [])
                    log("info", f"[state] loaded state for month={cur_month} (seen_pk={len(st.seen_pk)})", engine)

            total_added_pairs = 0

            for t in TABLES_FCT:
                fetched, added, new_cursor_iso = process_table_incremental(st, engine, t)
                total_added_pairs += added
                st.cursor_ts_iso_by_table[t] = new_cursor_iso
                if fetched > 0:
                    log("info", f"[fetch] month={st.month} table={t} fetched={fetched} added_pairs={added} cursor={new_cursor_iso}", engine)

            if total_added_pairs > 0:
                log("info", f"[agg] month={st.month} new_pairs={total_added_pairs} -> build summary & upsert", engine)
                df_summary = build_df_summary_for_month(st)
                upsert_df_summary(engine, df_summary)
                save_state(st)
                log("info", f"[upsert] {SAVE_SCHEMA}.{SAVE_TABLE} updated (month={st.month})", engine)
            else:
                # 요청 예시에 sleep 로그 포함
                log("sleep", f"no new pairs. sleep {SLEEP_SEC}s", engine)

            time.sleep(SLEEP_SEC)

        except SQLAlchemyError as e:
            # DB 에러: 콘솔은 즉시 남기고 재접속 후 DB로그 재개
            print_log("error", f"DB error: {type(e).__name__}: {e}")
            print_log("down", f"reconnect in {SLEEP_SEC}s")
            time.sleep(SLEEP_SEC)
            try:
                engine.dispose()
            except Exception:
                pass
            engine = connect_with_retry()
            ensure_schema_table(engine)
            ensure_log_table(engine)
            log("info", "db reconnect complete", engine)

        except Exception as e:
            log("error", f"Unhandled error: {type(e).__name__}: {e}", engine)
            log("sleep", f"sleep {SLEEP_SEC}s after unhandled error", engine)
            time.sleep(SLEEP_SEC)


if __name__ == "__main__":
    run()
