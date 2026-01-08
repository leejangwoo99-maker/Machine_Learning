# -*- coding: utf-8 -*-
"""
Factory Realtime - Vision Non Operation Time Inspector (cursor incremental)

목적
- d1_machine_log.Vision1~2_machine_log 에서
  a) "검사 양품 신호 출력" 또는 "검사 불량 신호 출력" 이후
  b) "바코드 스캔 신호 수신" 까지의 시간차를 no_operation_time(초)로 계산
- g_production_film.op_ct_gap 의 del_out_av(Vision1/Vision2) 임계값과 비교하여
  no_operation_time > del_out_av 인 경우만 이벤트로 확정(real_no_operation_time=1)
- 이벤트를 g_production_film.vision_non_operation_time 로 UPSERT 저장
- (end_day, station)별 커서(last_end_ts) 테이블로 중복 처리 방지

운영 사양(요청 반영)
- DB: 192.168.108.162:5432/postgres
- 멀티프로세스: 2개 고정(Station 단위 병렬 계산)
- 무한 루프: 1초 주기
- 유효 날짜: end_day = 오늘(YYYYMMDD)만 처리
- 커서: station 별 last_end_ts 갱신
- 콘솔: 매 루프 시작/종료 시간 출력, EXE 실행 시 콘솔 자동 종료 방지
"""

import sys
import time as time_mod
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import urllib.parse

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text


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

SRC_SCHEMA = "d1_machine_log"
VISION_TABLES = {
    "Vision1": "Vision1_machine_log",
    "Vision2": "Vision2_machine_log",
}

SAVE_SCHEMA = "g_production_film"
SAVE_TABLE  = "vision_non_operation_time"

CURSOR_SCHEMA = "g_production_film"
CURSOR_TABLE  = "vision_non_operation_time_cursor"

MAX_WORKERS = 2
LOOP_INTERVAL_SEC = 1.0
STABLE_DATA_SEC = 2.0


# =========================
# 1) 유틸
# =========================
def log(msg: str):
    print(msg, flush=True)

def today_yyyymmdd() -> str:
    return datetime.now().strftime("%Y%m%d")

def now_ts() -> datetime:
    return datetime.now()

def _print_run_banner(tag: str, start_dt: datetime):
    log("=" * 110)
    log(f"[{tag}] {start_dt:%Y-%m-%d %H:%M:%S}")
    log("=" * 110)

def _print_end_banner(tag: str, start_dt: datetime, end_dt: datetime):
    log("-" * 110)
    log(f"[{tag}] {end_dt:%Y-%m-%d %H:%M:%S} | elapsed={end_dt - start_dt}")
    log("-" * 110)

def get_engine():
    pw = urllib.parse.quote_plus(DB_CONFIG["password"])
    conn_str = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{pw}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(conn_str, pool_pre_ping=True)

def parse_ts(end_day: str, end_time: str) -> pd.Timestamp:
    d = str(end_day).strip()
    t = str(end_time).strip()
    ts = pd.to_datetime(f"{d} {t}", format="%Y%m%d %H:%M:%S.%f", errors="coerce")
    if pd.isna(ts):
        ts = pd.to_datetime(f"{d} {t}", format="%Y%m%d %H:%M:%S", errors="coerce")
    return ts


# =========================
# 2) 커서 테이블
# =========================
def ensure_cursor_table(engine):
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {CURSOR_SCHEMA};
    CREATE TABLE IF NOT EXISTS {CURSOR_SCHEMA}.{CURSOR_TABLE} (
        end_day      TEXT NOT NULL,
        station      TEXT NOT NULL,
        last_end_ts  TIMESTAMP NULL,
        updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (end_day, station)
    );
    """)
    with engine.begin() as conn:
        conn.execute(ddl)

def load_cursors(engine, end_day: str) -> dict:
    ensure_cursor_table(engine)
    q = text(f"""
        SELECT station, last_end_ts
        FROM {CURSOR_SCHEMA}.{CURSOR_TABLE}
        WHERE end_day = :end_day
    """)
    df = pd.read_sql(q, engine, params={"end_day": end_day})
    cur = {st: None for st in VISION_TABLES.keys()}
    for _, r in df.iterrows():
        cur[str(r["station"])] = r["last_end_ts"]
    return cur

def upsert_cursor(engine, end_day: str, station: str, last_end_ts: datetime):
    ensure_cursor_table(engine)
    q = text(f"""
        INSERT INTO {CURSOR_SCHEMA}.{CURSOR_TABLE} (end_day, station, last_end_ts, updated_at)
        VALUES (:end_day, :station, :last_end_ts, now())
        ON CONFLICT (end_day, station)
        DO UPDATE SET last_end_ts = EXCLUDED.last_end_ts,
                      updated_at  = now()
    """)
    with engine.begin() as conn:
        conn.execute(q, {"end_day": end_day, "station": station, "last_end_ts": last_end_ts})


# =========================
# 3) 임계값 로드(op_ct_gap)
# =========================
def load_thresholds(engine):
    q = text("""
        SELECT station, del_out_av
        FROM g_production_film.op_ct_gap
        WHERE station IN ('Vision1', 'Vision2')
    """)
    df = pd.read_sql(q, engine)
    if df.empty:
        raise RuntimeError("[ERROR] g_production_film.op_ct_gap 에서 ('Vision1','Vision2') 데이터를 찾지 못했습니다.")
    df["del_out_av"] = pd.to_numeric(df["del_out_av"], errors="coerce")
    return dict(zip(df["station"].astype(str), df["del_out_av"].astype(float)))


# =========================
# 4) 소스 로딩(증분)
# =========================
A_PREFIXES = ("검사 양품 신호 출력", "검사 불량 신호 출력")
B_PREFIX = "바코드 스캔 신호 수신"
VALID_PREFIXES = A_PREFIXES + (B_PREFIX,)

def load_vision_incremental(engine, end_day: str, station: str, last_end_ts):
    tbl = VISION_TABLES[station]
    q = text(f"""
        SELECT end_day, :station AS station, contents, end_time
        FROM {SRC_SCHEMA}."{tbl}"
        WHERE end_day = :end_day
        ORDER BY end_time ASC
    """)
    df = pd.read_sql(q, engine, params={"end_day": end_day, "station": station})
    if df.empty:
        return df

    df["_ts"] = [parse_ts(d, t) for d, t in zip(df["end_day"], df["end_time"])]
    df = df[df["_ts"].notna()].copy()

    stable_cut = pd.Timestamp(now_ts() - pd.Timedelta(seconds=STABLE_DATA_SEC))
    df = df[df["_ts"] <= stable_cut].copy()

    if last_end_ts is not None and pd.notna(last_end_ts):
        df = df[df["_ts"] > pd.Timestamp(last_end_ts)].copy()

    return df.reset_index(drop=True)


# =========================
# 5) 이벤트 계산(Station 1개 처리) - 워커
# =========================
def compute_events_for_station(args):
    station, df_station, th_map = args

    if df_station is None or df_station.empty:
        return station, None, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df_station.copy()
    df["contents"] = df["contents"].astype(str)
    df = df[df["contents"].str.startswith(VALID_PREFIXES)].copy()
    if df.empty:
        mx = df_station["_ts"].max() if "_ts" in df_station.columns and not df_station.empty else None
        return station, mx, pd.DataFrame(columns=["end_day", "station", "from_time", "to_time", "no_operation_time"])

    df = df.sort_values(["_ts"], ascending=True).reset_index(drop=True)
    df["no_operation_time"] = np.nan

    last_a_idx = None
    for i in range(len(df)):
        c = df.at[i, "contents"]
        if c.startswith(A_PREFIXES):
            last_a_idx = i
            continue
        if c.startswith(B_PREFIX):
            if last_a_idx is not None:
                t_a = df.at[last_a_idx, "_ts"]
                t_b = df.at[i, "_ts"]
                if pd.notna(t_a) and pd.notna(t_b) and t_b >= t_a:
                    df.at[i, "no_operation_time"] = float(round((t_b - t_a).total_seconds(), 2))
            last_a_idx = None

    thr = float(th_map.get(station, np.nan))
    df["real_no_operation_time"] = np.where(
        df["no_operation_time"].notna() & (df["no_operation_time"] > thr),
        1, 0
    )

    df["end_time_str"] = df["end_time"].astype(str)
    df["to_time"] = np.where(df["real_no_operation_time"] == 1, df["end_time_str"], np.nan)
    df["from_time"] = np.where(
        df["real_no_operation_time"] == 1,
        df["end_time_str"].shift(1),
        np.nan
    )

    out = df.loc[df["real_no_operation_time"] == 1, ["end_day", "station", "from_time", "to_time", "no_operation_time"]].copy()
    out["end_day"] = out["end_day"].astype(str)
    out["station"] = out["station"].astype(str)
    out["from_time"] = out["from_time"].astype(str)
    out["to_time"] = out["to_time"].astype(str)

    max_ts = df_station["_ts"].max()
    return station, max_ts.to_pydatetime() if pd.notna(max_ts) else None, out.reset_index(drop=True)


# =========================
# 6) 저장(UPSERT)
# =========================
def ensure_target_table(engine):
    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA};
    CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{SAVE_TABLE} (
        end_day           TEXT NOT NULL,
        station           TEXT NOT NULL,
        from_time         TEXT NOT NULL,
        to_time           TEXT NOT NULL,
        no_operation_time NUMERIC(12,2),
        created_at        TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (end_day, station, from_time, to_time)
    );
    """)
    with engine.begin() as conn:
        conn.execute(ddl)

def upsert_events(engine, df_events: pd.DataFrame) -> int:
    if df_events is None or df_events.empty:
        return 0

    ensure_target_table(engine)

    upsert_sql = text(f"""
        INSERT INTO {SAVE_SCHEMA}.{SAVE_TABLE}
        (end_day, station, from_time, to_time, no_operation_time)
        VALUES (:end_day, :station, :from_time, :to_time, :no_operation_time)
        ON CONFLICT (end_day, station, from_time, to_time)
        DO UPDATE SET no_operation_time = EXCLUDED.no_operation_time;
    """)
    rows = df_events.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(upsert_sql, rows)
    return len(rows)


# =========================
# 7) 1회 실행
# =========================
def main_once(engine, th_map):
    run_start = now_ts()
    _print_run_banner("RUN", run_start)

    end_day = today_yyyymmdd()
    cursors = load_cursors(engine, end_day=end_day)

    station_dfs = {}
    total_loaded = 0
    for st, last_ts in cursors.items():
        df_st = load_vision_incremental(engine, end_day=end_day, station=st, last_end_ts=last_ts)
        station_dfs[st] = df_st
        total_loaded += len(df_st)
        if len(df_st) > 0:
            log(f"[LOAD] {end_day} {st}: rows={len(df_st)} (cursor={last_ts})")
        else:
            log(f"[SKIP] {end_day} {st}: 신규 없음 (cursor={last_ts})")

    if total_loaded == 0:
        run_end = now_ts()
        _print_end_banner("DONE", run_start, run_end)
        return

    tasks = [(st, station_dfs[st], th_map) for st in station_dfs.keys()]

    all_events = []
    cursor_updates = []

    with ProcessPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(compute_events_for_station, t) for t in tasks]
        for fut in as_completed(futures):
            st, max_ts, ev = fut.result()
            if ev is not None and not ev.empty:
                all_events.append(ev)
                log(f"[EVT] {st}: events={len(ev)}")
            else:
                log(f"[EVT] {st}: events=0")
            if max_ts is not None:
                cursor_updates.append((st, max_ts))

    inserted = 0
    if all_events:
        df_save = pd.concat(all_events, ignore_index=True)
        inserted = upsert_events(engine, df_save)
        log(f"[SAVE] {SAVE_SCHEMA}.{SAVE_TABLE}: upsert rows={inserted}")
    else:
        log(f"[SAVE] {SAVE_SCHEMA}.{SAVE_TABLE}: no events -> skip")

    for st, max_ts in cursor_updates:
        upsert_cursor(engine, end_day=end_day, station=st, last_end_ts=max_ts)
        log(f"[CURSOR] {end_day} {st} -> {max_ts}")

    run_end = now_ts()
    _print_end_banner("DONE", run_start, run_end)


# =========================
# 8) Realtime loop
# =========================
def realtime_loop():
    engine = get_engine()
    log("[OK] engine ready")

    th_map = load_thresholds(engine)
    log(f"[OK] thresholds loaded: {th_map}")

    while True:
        loop_start = time_mod.perf_counter()
        try:
            main_once(engine, th_map)
        except Exception as e:
            log(f"[ERROR] {type(e).__name__}: {e}")

        elapsed = time_mod.perf_counter() - loop_start
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        if sleep_sec:
            time_mod.sleep(sleep_sec)


# =========================
# 9) entry
# =========================
if __name__ == "__main__":
    start = datetime.now()
    _print_run_banner("START", start)
    exit_code = 0
    try:
        realtime_loop()
    except KeyboardInterrupt:
        log("[ABORT] 사용자 중단(CTRL+C)")
        exit_code = 130
    except Exception as e:
        log(f"[ERROR] Unhandled exception: {repr(e)}")
        exit_code = 1
    finally:
        end = datetime.now()
        _print_end_banner("END", start, end)

        if getattr(sys, "frozen", False):
            try:
                input("Press Enter to exit...")
            except Exception:
                pass

    sys.exit(exit_code)
