# -*- coding: utf-8 -*-
"""
backend3_fail_step_repeat_daemon.py
-----------------------------------
FCT FAIL Step 반복(1회/2회 반복/3회 이상 반복) 집계 데몬

요구사항 반영:
1) dataframe 콘솔 출력 제외
2) 날짜는 [WINDOW] 기준 현재 날짜/현재시각으로 Default 자동 전환
3) 멀티프로세스 1개
4) 무한 루프 5초
5) DB 접속 실패 시 무한 재시도(블로킹)
6) 실행 중 DB 끊김 발생 시 무한 재접속 후 계속 진행
7) 상시 연결 1개 고정(풀 최소화: pool_size=1, max_overflow=0)
8) work_mem 폭증 방지(세션 SET work_mem)
9) 증분 조건: (end_day, end_time, barcode_information) 기준
10) [BOOT] 즉시 출력, DB 안붙으면 [RETRY] 5초마다 출력
11) 단계별 [INFO] 로그
    - last_pk 읽기 / 신규 fetch / insert(=upsert)
    - 신규 row만 반영해서 집계를 증분 업데이트(in-memory pair_count)
    - last_pk는 메모리만
12) 재실행 시 삭제/초기화 금지
    - 재실행 시 last_pk는 날아가므로 현재 윈도우(start~now) 전체 bootstrap 후 UPSERT
13) 저장 테이블:
    - day:   c_1time_step_decription_day_daily / c_2time_step_decription_day_daily / c_3time_over_step_decription_day_daily
    - night: c_1time_step_decription_night_daily / c_2time_step_decription_night_daily / c_3time_over_step_decription_night_daily

추가 반영(사용자 확정):
- ✅ 중복 방지 캐시 seen_pk(set) 추가
- ✅ DB 컬럼 타입은 TEXT 유지(updated_at만 timestamptz)

추가 반영(이번 요청):
- ✅ 데몬 동작 로그를 DB에 저장
- ✅ 스키마: k_demon_heath_check (없으면 생성)
- ✅ 테이블: "3_log" (없으면 생성)
- ✅ 컬럼: end_day(yyyymmdd), end_time(hh:mi:ss), info(소문자), contents
- ✅ 저장 순서: end_day, end_time, info, contents
"""

from __future__ import annotations

import os
import time as time_mod
from dataclasses import dataclass
from datetime import datetime, date, timedelta, time as dtime
from typing import Dict, Tuple, List, Optional, Set

import pandas as pd
from zoneinfo import ZoneInfo
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# TZ / Constants
# =========================
KST = ZoneInfo("Asia/Seoul")

LOOP_INTERVAL_SEC = 5
DB_RETRY_INTERVAL_SEC = 5

# work_mem 폭증 방지 (환경변수로 조정)
WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

# =========================
# DB Config (사용자 제공)
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

# =========================
# Source / Save Tables
# =========================
SRC_SCHEMA = "a2_fct_table"
SRC_TABLE = "fct_table"
REMARK_SCHEMA = "g_production_film"
REMARK_TABLE = "remark_info"

SAVE_SCHEMA = "i_daily_report"

DAY_T1 = "c_1time_step_decription_day_daily"
DAY_T2 = "c_2time_step_decription_day_daily"
DAY_T3 = "c_3time_over_step_decription_day_daily"

NIGHT_T1 = "c_1time_step_decription_night_daily"
NIGHT_T2 = "c_2time_step_decription_night_daily"
NIGHT_T3 = "c_3time_over_step_decription_night_daily"

# 컬럼명(요청대로)
COL_1 = "1회 FAIL_step_description"
COL_2 = "2회 반복_FAIL_step_description"
COL_3 = "3회 이상 반복_FAIL_step_description"

# =========================
# Health-Log Table (NEW)
# =========================
HEALTH_SCHEMA = "k_demon_heath_check"
HEALTH_TABLE = "3_log"  # 숫자 시작 테이블명 -> 반드시 쌍따옴표 사용

# 현재 연결된 엔진(로그 DB 저장용)
_ACTIVE_ENGINE: Optional[Engine] = None


# =========================
# Logging (console + DB)
# =========================
def _ts_kst() -> datetime:
    return datetime.now(KST)


def _fmt_now() -> str:
    return _ts_kst().strftime("%Y-%m-%d %H:%M:%S")


def ensure_health_log_table(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {HEALTH_SCHEMA};"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {HEALTH_SCHEMA}."{HEALTH_TABLE}" (
                    end_day  text,
                    end_time text,
                    info     text,
                    contents text
                );
                """
            )
        )


def _insert_health_log(engine: Engine, info: str, contents: str) -> None:
    """
    end_day, end_time, info, contents 순서로 DataFrame화하여 저장
    """
    now = _ts_kst()
    row = {
        "end_day": now.strftime("%Y%m%d"),
        "end_time": now.strftime("%H:%M:%S"),
        "info": (info or "").lower(),
        "contents": str(contents),
    }
    df = pd.DataFrame([row], columns=["end_day", "end_time", "info", "contents"])

    sql = text(
        f"""
        INSERT INTO {HEALTH_SCHEMA}."{HEALTH_TABLE}"
        (end_day, end_time, info, contents)
        VALUES (:end_day, :end_time, :info, :contents)
        """
    )

    with engine.begin() as conn:
        conn.execute(sql, df.to_dict(orient="records"))


def _emit(level_tag: str, info: str, msg: str, persist: bool = True) -> None:
    """
    level_tag: 콘솔 표기용 [BOOT]/[INFO]/[RETRY]/[WARN]
    info     : DB 저장용 소문자 코드 (error/down/sleep/info/warn/...).
    """
    print(f"{_fmt_now()} [{level_tag}] {msg}", flush=True)

    if not persist:
        return

    global _ACTIVE_ENGINE
    if _ACTIVE_ENGINE is None:
        return

    try:
        _insert_health_log(_ACTIVE_ENGINE, info=info.lower(), contents=msg)
    except Exception as e:
        # 로그 저장 실패로 본 데몬 로직 영향 주지 않음(재귀 방지)
        print(f"{_fmt_now()} [WARN] health-log insert failed: {type(e).__name__}: {e}", flush=True)


def log_boot(msg: str) -> None:
    _emit("BOOT", "boot", msg, persist=True)


def log_info(msg: str) -> None:
    _emit("INFO", "info", msg, persist=True)


def log_retry(msg: str) -> None:
    _emit("RETRY", "down", msg, persist=True)


def log_warn(msg: str) -> None:
    _emit("WARN", "warn", msg, persist=True)


def log_sleep(msg: str) -> None:
    _emit("INFO", "sleep", msg, persist=True)


def log_error(msg: str) -> None:
    _emit("WARN", "error", msg, persist=True)


# =========================
# Engine / Session
# =========================
def make_engine() -> Engine:
    os.environ.setdefault("PGCLIENTENCODING", "UTF8")
    os.environ.setdefault("PGOPTIONS", "-c client_encoding=UTF8")
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


def ensure_db_ready(engine: Engine) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))


# =========================
# Window 계산
# =========================
@dataclass(frozen=True)
class Window:
    prod_day: str      # YYYYMMDD (윈도우 시작 기준 날짜)
    shift: str         # 'day' or 'night'
    start_dt: datetime
    now_dt: datetime   # 현재 시각(윈도우 end = now)


def yyyymmdd(d: date) -> str:
    return d.strftime("%Y%m%d")


def parse_yyyymmdd(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def now_kst() -> datetime:
    return datetime.now(KST)


def get_window(now: datetime) -> Window:
    """
    [WINDOW] 기준 자동 전환:
    - day   : D 08:30:00 ~ 20:29:59
    - night : D 20:30:00 ~ D+1 08:29:59
    """
    t = now.timetz()
    tn = dtime(t.hour, t.minute, t.second)

    day_start = dtime(8, 30, 0)
    day_end = dtime(20, 29, 59)
    night_start = dtime(20, 30, 0)

    if day_start <= tn <= day_end:
        prod = yyyymmdd(now.date())
        start_dt = datetime(now.year, now.month, now.day, 8, 30, 0, tzinfo=KST)
        return Window(prod_day=prod, shift="day", start_dt=start_dt, now_dt=now)
    elif tn >= night_start:
        prod = yyyymmdd(now.date())
        start_dt = datetime(now.year, now.month, now.day, 20, 30, 0, tzinfo=KST)
        return Window(prod_day=prod, shift="night", start_dt=start_dt, now_dt=now)
    else:
        prod_date = now.date() - timedelta(days=1)
        prod = yyyymmdd(prod_date)
        start_dt = datetime(prod_date.year, prod_date.month, prod_date.day, 20, 30, 0, tzinfo=KST)
        return Window(prod_day=prod, shift="night", start_dt=start_dt, now_dt=now)


def time_norm(dt: datetime) -> str:
    return dt.strftime("%H%M%S")


def build_segments(w: Window) -> List[Tuple[str, str, str]]:
    """
    윈도우(start_dt ~ now_dt)를 end_day/end_time_norm 조건으로 분할.
    반환: [(end_day, start_time_norm, end_time_norm), ...]
    """
    prod_date = parse_yyyymmdd(w.prod_day)
    next_day = yyyymmdd(prod_date + timedelta(days=1))

    if w.shift == "day":
        seg_end = min(w.now_dt, datetime(prod_date.year, prod_date.month, prod_date.day, 20, 29, 59, tzinfo=KST))
        return [(w.prod_day, "083000", time_norm(seg_end))]
    else:
        now_day = yyyymmdd(w.now_dt.date())
        if now_day == w.prod_day:
            seg_end = min(w.now_dt, datetime(prod_date.year, prod_date.month, prod_date.day, 23, 59, 59, tzinfo=KST))
            return [(w.prod_day, "203000", time_norm(seg_end))]
        else:
            seg_end = min(w.now_dt, datetime(w.now_dt.year, w.now_dt.month, w.now_dt.day, 8, 29, 59, tzinfo=KST))
            return [
                (w.prod_day, "203000", "235959"),
                (next_day, "000000", time_norm(seg_end)),
            ]


# =========================
# SQL (FAIL row fetch)
# =========================
def build_where_segments(segments: List[Tuple[str, str, str]]) -> Tuple[str, Dict[str, str]]:
    parts = []
    params: Dict[str, str] = {}
    for i, (d, s, e) in enumerate(segments):
        parts.append(f"(ft.end_day = :d{i} AND LPAD(ft.end_time, 6, '0') BETWEEN :s{i} AND :e{i})")
        params[f"d{i}"] = d
        params[f"s{i}"] = s
        params[f"e{i}"] = e
    return " OR ".join(parts), params


def fetch_fail_rows(
    engine: Engine,
    w: Window,
    last_pk: Optional[Tuple[str, str, str]],
) -> pd.DataFrame:
    """
    last_pk: (end_day, end_time_norm, barcode_information)
    반환 DF columns:
      pn, barcode_information, step_description, end_day, end_time_norm
    """
    segments = build_segments(w)
    seg_sql, seg_params = build_where_segments(segments)

    # ✅ 증분 조건: (end_day, end_time, barcode_information)
    # 동일 triple 내 late-arrival 고려하여 >= fetch 후 seen_pk dedup
    inc_sql = ""
    inc_params: Dict[str, str] = {}
    if last_pk is not None:
        inc_sql = "AND (ft.end_day, LPAD(ft.end_time, 6, '0'), ft.barcode_information) >= (:lday, :ltime, :lbarcode)"
        inc_params = {"lday": last_pk[0], "ltime": last_pk[1], "lbarcode": last_pk[2]}

    sql = f"""
    SELECT
        COALESCE(ri.pn, 'UNKNOWN') AS pn,
        ft.barcode_information,
        ft.step_description,
        ft.end_day,
        LPAD(ft.end_time, 6, '0') AS end_time_norm
    FROM {SRC_SCHEMA}.{SRC_TABLE} ft
    LEFT JOIN {REMARK_SCHEMA}.{REMARK_TABLE} ri
        ON ri.barcode_information = SUBSTRING(ft.barcode_information FROM 18 FOR 1)
    WHERE
        ft.result = 'FAIL'
        AND ({seg_sql})
        {inc_sql}
    ORDER BY
        ft.end_day ASC,
        LPAD(ft.end_time, 6, '0') ASC,
        ft.barcode_information ASC,
        ft.step_description ASC
    ;
    """

    params = {}
    params.update(seg_params)
    params.update(inc_params)

    with engine.connect() as conn:
        conn.execute(text(f"SET work_mem = '{WORK_MEM}'"))
        df = pd.read_sql(text(sql), conn, params=params)

    return df


# =========================
# In-memory aggregation (+ seen_pk dedup)
# =========================
PairKey = Tuple[str, str, str]      # (pn, barcode_information, step_description)
SeenKey = Tuple[str, str, str, str] # (end_day, end_time_norm, barcode_information, step_description)


def apply_new_rows(pair_counts: Dict[PairKey, int], seen_pk: Set[SeenKey], df_new: pd.DataFrame) -> int:
    """
    신규 FAIL row를 pair_counts에 누적(+1)
    - ✅ seen_pk 중복 방지
    반환: 실제 반영된 row 수
    """
    applied = 0
    for r in df_new.itertuples(index=False):
        end_day = str(r.end_day)
        end_time_norm = str(r.end_time_norm)
        barcode = str(r.barcode_information)
        step = str(r.step_description)
        seen_key: SeenKey = (end_day, end_time_norm, barcode, step)

        if seen_key in seen_pk:
            continue

        seen_pk.add(seen_key)

        pn = str(r.pn)
        k: PairKey = (pn, barcode, step)
        pair_counts[k] = pair_counts.get(k, 0) + 1
        applied += 1

    return applied


def summarize_bucket_dfs(prod_day: str, shift: str, pair_counts: Dict[PairKey, int]) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    bucket별 DF 생성:
    - count = (barcode, step) 조합 개수
    - group by: (prod_day, shift_type, pn, step_description)
    """
    b1: Dict[Tuple[str, str], int] = {}  # (pn, step) -> count
    b2: Dict[Tuple[str, str], int] = {}
    b3: Dict[Tuple[str, str], int] = {}

    for (pn, _barcode, step), c in pair_counts.items():
        key = (pn, step)
        if c == 1:
            b1[key] = b1.get(key, 0) + 1
        elif c == 2:
            b2[key] = b2.get(key, 0) + 1
        else:
            b3[key] = b3.get(key, 0) + 1

    updated_at = datetime.now(KST)

    def to_df(bucket_map: Dict[Tuple[str, str], int], step_col: str) -> pd.DataFrame:
        if not bucket_map:
            return pd.DataFrame(columns=["prod_day", "shift_type", "pn", step_col, "count", "updated_at"])
        rows = []
        for (pn, step), cnt in bucket_map.items():
            rows.append({
                "prod_day": prod_day,
                "shift_type": shift,
                "pn": pn,
                step_col: step,
                "count": cnt,
                "updated_at": updated_at,
            })
        df = pd.DataFrame(rows)
        df = df.sort_values(["count", "pn"], ascending=[False, True]).reset_index(drop=True)
        return df

    return (to_df(b1, COL_1), to_df(b2, COL_2), to_df(b3, COL_3))


# =========================
# Save (schema/table/upsert)  [TEXT 유지]
# =========================
def ensure_schema(engine: Engine, schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema};"))


def ensure_table(engine: Engine, schema: str, table: str, columns: List[str], key_cols: List[str]) -> None:
    ddl_cols = []
    for c in columns:
        if c == "updated_at":
            ddl_cols.append(f'"{c}" timestamptz')
        else:
            ddl_cols.append(f'"{c}" text')
    ddl_cols_sql = ",\n  ".join(ddl_cols)
    key_sql = ", ".join([f'"{c}"' for c in key_cols])

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
      {ddl_cols_sql},
      CONSTRAINT {table}__uk UNIQUE ({key_sql})
    );
    """
    with engine.begin() as conn:
        conn.execute(text(create_sql))


def normalize_df_for_db(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    out = df.copy()
    for c in out.columns:
        if c == "updated_at":
            continue
        out[c] = out[c].astype("string")
    out = out.where(pd.notnull(out), None)
    return out


def key_cols_for(df: pd.DataFrame) -> List[str]:
    step_cols = [c for c in df.columns if str(c).endswith("step_description")]
    if not step_cols:
        raise RuntimeError(f"step_description 컬럼을 찾을 수 없음: {list(df.columns)}")
    step_col = step_cols[0]
    return ["prod_day", "shift_type", "pn", step_col]


def upsert_df(engine: Engine, schema: str, table: str, df: pd.DataFrame, key_cols: List[str]) -> None:
    if df is None or df.empty:
        log_info(f"[UPSERT] skip empty -> {schema}.{table}")
        return

    df = normalize_df_for_db(df)
    cols = list(df.columns)

    # bind param 안전 매핑 (한글/공백 컬럼명 대응)
    pkeys = [f"v{i}" for i in range(len(cols))]

    col_sql = ", ".join([f'"{c}"' for c in cols])
    val_sql = ", ".join([f":{k}" for k in pkeys])
    key_sql = ", ".join([f'"{c}"' for c in key_cols])

    set_sql = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols if c not in key_cols])

    sql = f"""
    INSERT INTO {schema}.{table} ({col_sql})
    VALUES ({val_sql})
    ON CONFLICT ({key_sql})
    DO UPDATE SET
      {set_sql}
    ;
    """

    recs_raw = df.to_dict(orient="records")
    records = [{k: r[c] for c, k in zip(cols, pkeys)} for r in recs_raw]

    with engine.begin() as conn:
        conn.execute(text(sql), records)

    log_info(f"[UPSERT] {len(df)} rows -> {schema}.{table}")


def save_bucket_tables(engine: Engine, w: Window, df1: pd.DataFrame, df2: pd.DataFrame, df3: pd.DataFrame) -> None:
    ensure_schema(engine, SAVE_SCHEMA)

    if w.shift == "day":
        targets = [(DAY_T1, df1), (DAY_T2, df2), (DAY_T3, df3)]
    else:
        targets = [(NIGHT_T1, df1), (NIGHT_T2, df2), (NIGHT_T3, df3)]

    for table, df in targets:
        if df is None or df.empty:
            log_info(f"[SAVE] {SAVE_SCHEMA}.{table} empty -> skip")
            continue
        keys = key_cols_for(df)
        ensure_table(engine, SAVE_SCHEMA, table, list(df.columns), keys)
        upsert_df(engine, SAVE_SCHEMA, table, df, keys)


# =========================
# Bootstrap / Loop
# =========================
def bootstrap(engine: Engine, w: Window) -> Tuple[Dict[PairKey, int], Set[SeenKey], Optional[Tuple[str, str, str]]]:
    """
    현재 윈도우(start~now) 전체 스캔해서 pair_counts + seen_pk 재구성 + last_pk 설정
    """
    log_info(f"[BOOTSTRAP] scan window: prod_day={w.prod_day} shift={w.shift} start={w.start_dt} now={w.now_dt}")

    df_all = fetch_fail_rows(engine, w, last_pk=None)
    log_info(f"[BOOTSTRAP] fetched_rows={len(df_all)}")

    pair_counts: Dict[PairKey, int] = {}
    seen_pk: Set[SeenKey] = set()

    applied = apply_new_rows(pair_counts, seen_pk, df_all)
    log_info(f"[BOOTSTRAP] applied_rows(after_dedup)={applied}")

    last_pk = None
    if len(df_all):
        tail = df_all.iloc[-1]
        last_pk = (str(tail["end_day"]), str(tail["end_time_norm"]), str(tail["barcode_information"]))
        log_info(f"[LAST_PK] bootstrap last_pk={last_pk}")

    df1, df2, df3 = summarize_bucket_dfs(w.prod_day, w.shift, pair_counts)
    log_info(f"[BOOTSTRAP] bucket_rows: 1회={len(df1)} 2회={len(df2)} 3회+={len(df3)}")
    save_bucket_tables(engine, w, df1, df2, df3)

    return pair_counts, seen_pk, last_pk


def main() -> None:
    global _ACTIVE_ENGINE

    log_boot("backend3 fail-step repeat daemon starting")

    engine = make_engine()

    # DB 연결 무한 재시도
    while True:
        try:
            ensure_db_ready(engine)

            # health-log 테이블 보장
            ensure_health_log_table(engine)

            # 연결 성공 후부터 DB 로그 저장 활성화
            _ACTIVE_ENGINE = engine

            log_info(f"DB connected (work_mem={WORK_MEM})")
            log_info(f'health log ready -> {HEALTH_SCHEMA}."{HEALTH_TABLE}"')
            break
        except Exception as e:
            # 연결 전이므로 콘솔은 반드시 출력, DB 저장은 불가
            _ACTIVE_ENGINE = None
            log_retry(f"DB connect failed: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    current_window: Optional[Window] = None
    pair_counts: Dict[PairKey, int] = {}
    seen_pk: Set[SeenKey] = set()
    last_pk: Optional[Tuple[str, str, str]] = None

    # 첫 bootstrap
    while True:
        try:
            current_window = get_window(now_kst())
            pair_counts, seen_pk, last_pk = bootstrap(engine, current_window)
            break
        except (OperationalError, DBAPIError) as e:
            _ACTIVE_ENGINE = None
            log_retry(f"bootstrap DB error: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)
            engine = make_engine()
        except Exception as e:
            log_error(f"bootstrap unhandled: {type(e).__name__}: {e}")
            log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s and retry bootstrap")
            time_mod.sleep(DB_RETRY_INTERVAL_SEC)

    # main loop
    while True:
        loop_start = time_mod.time()
        try:
            now = now_kst()
            w = get_window(now)

            # 윈도우 변경 → bootstrap + 캐시 초기화
            if (current_window is None) or (w.prod_day != current_window.prod_day) or (w.shift != current_window.shift):
                log_info(f"[WINDOW] changed => prod_day={w.prod_day} shift={w.shift}")
                current_window = w
                pair_counts, seen_pk, last_pk = bootstrap(engine, current_window)
            else:
                log_info(f"[FETCH] last_pk={last_pk}")
                df_new = fetch_fail_rows(engine, current_window, last_pk=last_pk)
                log_info(f"[FETCH] fetched_rows={len(df_new)}")

                if len(df_new) > 0:
                    applied = apply_new_rows(pair_counts, seen_pk, df_new)
                    log_info(f"[FETCH] applied_rows(after_dedup)={applied}")

                    # last_pk 갱신은 "가져온 DF 기준"
                    tail = df_new.iloc[-1]
                    last_pk = (str(tail["end_day"]), str(tail["end_time_norm"]), str(tail["barcode_information"]))
                    log_info(f"[LAST_PK] updated last_pk={last_pk}")

                    if applied > 0:
                        df1, df2, df3 = summarize_bucket_dfs(current_window.prod_day, current_window.shift, pair_counts)
                        log_info(f"[AGG] bucket_rows: 1회={len(df1)} 2회={len(df2)} 3회+={len(df3)}")
                        save_bucket_tables(engine, current_window, df1, df2, df3)
                    else:
                        log_info("[AGG] no new unique rows -> skip upsert")
                else:
                    log_info("[AGG] no new rows -> skip upsert")

        except (OperationalError, DBAPIError) as e:
            _ACTIVE_ENGINE = None
            log_retry(f"DB error: {type(e).__name__}: {e}")

            # 재연결 무한 재시도 + bootstrap으로 정상화
            while True:
                try:
                    log_sleep(f"sleep {DB_RETRY_INTERVAL_SEC}s before reconnect")
                    time_mod.sleep(DB_RETRY_INTERVAL_SEC)

                    engine = make_engine()
                    ensure_db_ready(engine)
                    ensure_health_log_table(engine)

                    _ACTIVE_ENGINE = engine
                    log_info("DB reconnected")

                    current_window = get_window(now_kst())
                    pair_counts, seen_pk, last_pk = bootstrap(engine, current_window)
                    break
                except Exception as e2:
                    _ACTIVE_ENGINE = None
                    log_retry(f"reconnect failed: {type(e2).__name__}: {e2}")
                    continue

        except Exception as e:
            log_error(f"Unhandled error: {type(e).__name__}: {e}")

        # loop pacing
        elapsed = time_mod.time() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec > 0:
            log_sleep(f"loop sleep {sleep_sec:.2f}s")
            time_mod.sleep(sleep_sec)


if __name__ == "__main__":
    main()
