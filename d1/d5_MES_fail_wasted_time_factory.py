# -*- coding: utf-8 -*-
"""
MES 불량 소요 시간 계산 (Vision1/Vision2) - Realtime Loop Version

원본 로직:
- d1_machine_log.Vision1_machine_log / Vision2_machine_log 로딩
- 교대 경계(08:20~08:30, 20:20~20:30) 제외
- 'MES 바코드 공정 불량' 연속 5회 이상 구간의 첫 행을 from_row로 채택 (조회 완료 전까지 1회만)
- 이후 첫 'MES 바코드 조회 완료'를 to_row로 매칭
- wasted_time = |to - from| (초) , 10분(600초) 초과 제외
- 결과를 d1_machine_log.mes_fail_wasted_time 에 DROP→CREATE→INSERT

추가 요구사항(동일 5개):
1) 1초마다 재실행 무한루프
2) 멀티프로세스 2개 고정
3) end_day = 오늘 날짜만 처리
4) 중복 제거(SELECT DISTINCT 효과)
5) 실시간: 현재 시간 기준 120초 이내 데이터만 처리
"""

import time as pytime
import urllib.parse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from multiprocessing import Pool, freeze_support


# ============================================
# [1] DB 설정
# ============================================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

VISION1_TABLE = 'd1_machine_log."Vision1_machine_log"'
VISION2_TABLE = 'd1_machine_log."Vision2_machine_log"'

OUT_TABLE_SCHEMA = "d1_machine_log"
OUT_TABLE_NAME   = "mes_fail_wasted_time"

# (2) 멀티프로세스 2개 고정
WORKERS = 2

# (5) 실시간 윈도우(초)
REALTIME_WINDOW_SEC = 120

# (선택) epoch cutoff 강제 적용하고 싶으면 float epoch 넣기 (없으면 None)
FORCE_CUTOFF_TS = None


def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)


# ============================================
# [2] 유틸
# ============================================
def time_to_seconds(series: pd.Series) -> pd.Series:
    return pd.to_timedelta(series.astype(str)).dt.total_seconds()


def secs_to_hhmmss_ss(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec - h * 3600 - m * 60
    return f"{h:02d}:{m:02d}:{s:05.2f}"


def filter_shift_boundary(df: pd.DataFrame) -> pd.DataFrame:
    # 교대 경계(08:20~08:30, 20:20~20:30) 제거
    df = df.copy()
    df["time_secs"] = pd.to_timedelta(df["time"].astype(str)).dt.total_seconds()

    start_0820 = 8 * 3600 + 20 * 60
    end_0830   = 8 * 3600 + 30 * 60
    start_2020 = 20 * 3600 + 20 * 60
    end_2030   = 20 * 3600 + 30 * 60

    mask_shift = ~(
        ((df["time_secs"] >= start_0820) & (df["time_secs"] <= end_0830)) |
        ((df["time_secs"] >= start_2020) & (df["time_secs"] <= end_2030))
    )
    df = df[mask_shift].copy()
    df = df.drop(columns=["time_secs"], errors="ignore")
    return df


# ============================================
# [3] 그룹 처리(멀티프로세스 워커)
# ============================================
def process_one_group(args):
    """
    args = ((station, end_day), group_df)
    return: rows(list[dict]) - 결과 레코드들
    """
    (station, end_day), g = args
    g = g.sort_values("time").reset_index(drop=True).copy()

    g["is_mes_ng"] = g["contents"].astype(str).str.contains("MES 바코드 공정 불량", na=False)
    g["is_done"]   = g["contents"].astype(str).str.contains("MES 바코드 조회 완료", na=False)

    used_before_done = False
    from_indices = []

    i = 0
    n = len(g)
    while i < n:
        if bool(g["is_done"].iloc[i]):
            used_before_done = False
            i += 1
            continue

        if bool(g["is_mes_ng"].iloc[i]):
            run_start = i
            j = i + 1
            while j < n and bool(g["is_mes_ng"].iloc[j]):
                j += 1
            run_len = j - run_start

            if run_len >= 5 and not used_before_done:
                from_indices.append(run_start)
                used_before_done = True

            i = j
        else:
            i += 1

    if not from_indices:
        return []

    from_rows = g.loc[from_indices, ["end_day", "station", "dayornight", "time", "contents"]].copy()
    from_rows = from_rows.rename(columns={"time": "from_time", "contents": "from_contents"})
    from_rows["from_key"] = time_to_seconds(from_rows["from_time"])

    done_rows = g[g["is_done"]][["time", "contents"]].copy()
    if done_rows.empty:
        return []

    done_rows = done_rows.rename(columns={"time": "to_time", "contents": "to_contents"})
    done_rows["to_key"] = time_to_seconds(done_rows["to_time"])
    done_rows = done_rows.sort_values("to_key").reset_index(drop=True)

    keys_done = done_rows["to_key"].to_numpy()
    keys_from = from_rows["from_key"].to_numpy()

    idx = np.searchsorted(keys_done, keys_from, side="left")
    mask = idx < len(keys_done)
    if not mask.any():
        return []

    from_valid = from_rows.loc[mask].reset_index(drop=True)
    done_valid = done_rows.iloc[idx[mask]].reset_index(drop=True)

    merged = pd.concat([from_valid, done_valid[["to_time", "to_key", "to_contents"]]], axis=1)

    merged["wasted_time"] = (merged["to_key"] - merged["from_key"]).abs().round(2)
    merged = merged[merged["wasted_time"] <= 600].copy()
    if merged.empty:
        return []

    merged["from_time"] = merged["from_key"].map(secs_to_hhmmss_ss)
    merged["to_time"]   = merged["to_key"].map(secs_to_hhmmss_ss)

    out = merged[[
        "end_day",
        "station",
        "dayornight",
        "from_contents",
        "from_time",
        "to_contents",
        "to_time",
        "wasted_time",
    ]].copy()

    return out.to_dict("records")


# ============================================
# [4] DB 출력 테이블 DROP→CREATE
#     - 루프 중 매번 재생성하므로 원본 요구사항 유지
# ============================================
def recreate_out_table(engine):
    drop_sql = f"DROP TABLE IF EXISTS {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME};"
    create_sql = f"""
    CREATE TABLE {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (
        id            INTEGER,
        end_day       CHAR(8),
        station       TEXT,
        dayornight    TEXT,
        from_contents TEXT,
        from_time     TEXT,
        to_contents   TEXT,
        to_time       TEXT,
        wasted_time   NUMERIC(10,2)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(drop_sql))
        conn.execute(text(create_sql))


# ============================================
# [5] 단일 실행(루프에서 호출)
# ============================================
def run_once(engine):
    # (3) 오늘 날짜만 + (5) 최근 120초만 SQL에서 필터
    # end_day는 YYYYMMDD라 가정하고 to_date(end_day::text,'YYYYMMDD')로 CURRENT_DATE 비교
    if FORCE_CUTOFF_TS is None:
        ts_filter = f"(to_date(end_day::text,'YYYYMMDD') + \"time\"::time) >= (now() - interval '{int(REALTIME_WINDOW_SEC)} seconds')"
    else:
        ts_filter = f"(to_date(end_day::text,'YYYYMMDD') + \"time\"::time) >= to_timestamp({float(FORCE_CUTOFF_TS)})"

    q1 = text(f"""
        SELECT end_day, "time" as time, contents, dayornight, 'Vision1'::text AS station
        FROM {VISION1_TABLE}
        WHERE
            to_date(end_day::text,'YYYYMMDD') = CURRENT_DATE
            AND {ts_filter}
        ORDER BY end_day, "time";
    """)
    q2 = text(f"""
        SELECT end_day, "time" as time, contents, dayornight, 'Vision2'::text AS station
        FROM {VISION2_TABLE}
        WHERE
            to_date(end_day::text,'YYYYMMDD') = CURRENT_DATE
            AND {ts_filter}
        ORDER BY end_day, "time";
    """)

    df_v1 = pd.read_sql(q1, engine)
    df_v2 = pd.read_sql(q2, engine)
    df = pd.concat([df_v1, df_v2], ignore_index=True)

    if df.empty:
        # 결과 테이블은 요구사항대로 계속 유지 (빈 테이블)
        recreate_out_table(engine)
        print(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows=0)")
        return

    # 교대 경계 제외 + 정렬
    df = filter_shift_boundary(df)
    df = df.sort_values(["station", "end_day", "time"]).reset_index(drop=True)

    # (station, end_day) 그룹
    group_items = [((st, day), g.copy()) for (st, day), g in df.groupby(["station", "end_day"], sort=False)]

    # (2) 멀티프로세스 2개 고정
    with Pool(processes=WORKERS) as pool:
        results = pool.map(process_one_group, group_items)

    flat_rows = [r for sub in results for r in sub]

    if not flat_rows:
        result_df = pd.DataFrame(columns=[
            "id", "end_day", "station", "dayornight",
            "from_contents", "from_time", "to_contents", "to_time", "wasted_time"
        ])
        recreate_out_table(engine)
        print(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows=0)")
        return

    result_df = pd.DataFrame(flat_rows)

    # end_day CHAR(8) 정규화
    result_df["end_day"] = (
        result_df["end_day"].astype(str)
        .str.replace(",", "", regex=False)
        .str.zfill(8)
    )

    # (4) 중복 제거(SELECT DISTINCT 효과)
    distinct_cols = [
        "end_day", "station",
        "from_contents", "from_time",
        "to_contents", "to_time",
        "dayornight",
    ]
    result_df = result_df.drop_duplicates(subset=distinct_cols, keep="first").reset_index(drop=True)

    result_df.insert(0, "id", result_df.index + 1)

    # DROP → CREATE → INSERT
    recreate_out_table(engine)
    result_df.to_sql(
        OUT_TABLE_NAME,
        engine,
        schema=OUT_TABLE_SCHEMA,
        if_exists="append",
        index=False
    )

    print(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows={len(result_df)})")


# ============================================
# [6] main: (1) 1초마다 무한루프
# ============================================
def main_loop():
    engine = get_engine()
    print("[INFO] MES fail wasted time realtime loop start")
    print(f"[INFO] workers={WORKERS}, realtime_window={REALTIME_WINDOW_SEC}s, force_cutoff_ts={FORCE_CUTOFF_TS}")

    while True:
        try:
            run_once(engine)
        except Exception as e:
            print("[ERROR]", repr(e))
        pytime.sleep(1)


if __name__ == "__main__":
    freeze_support()
    main_loop()
