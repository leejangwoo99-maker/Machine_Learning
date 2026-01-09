# -*- coding: utf-8 -*-
"""
MES 불량 소요 시간 계산 (Vision1/Vision2)
- d1_machine_log.Vision1_machine_log / Vision2_machine_log 로딩
- 교대 경계(08:20~08:30, 20:20~20:30) 제외
- 'MES 바코드 공정 불량' 연속 5회 이상 구간의 첫 행을 from_row로 채택 (조회 완료 전까지 1회만)
- 이후 첫 'MES 바코드 조회 완료'를 to_row로 매칭
- wasted_time = |to - from| (초) , 10분(600초) 초과 제외
- 결과를 d1_machine_log.mes_fail_wasted_time 에 DROP→CREATE→INSERT

멀티프로세스:
- (station, end_day) 그룹 단위 병렬 처리

[변경사항(요청 반영)]
- dayornight 컬럼/속성값 완전 삭제
- time 컬럼 -> end_time 컬럼으로 변경
- 결과 테이블에서도 dayornight 제거
"""

import os
import urllib.parse
import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from multiprocessing import Pool, cpu_count, freeze_support


# ============================================
# [1] DB 설정
# ============================================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"
VISION1_TABLE = 'd1_machine_log."Vision1_machine_log"'
VISION2_TABLE = 'd1_machine_log."Vision2_machine_log"'

OUT_TABLE_SCHEMA = "d1_machine_log"
OUT_TABLE_NAME = "mes_fail_wasted_time"


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
    """
    교대 경계(08:20~08:30, 20:20~20:30) 제거
    (변경: time -> end_time 기준)
    """
    df = df.copy()
    df["time_secs"] = pd.to_timedelta(df["end_time"].astype(str)).dt.total_seconds()

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
    g = g.sort_values("end_time").reset_index(drop=True).copy()

    # flags
    g["is_mes_ng"] = g["contents"].astype(str).str.contains("MES 바코드 공정 불량", na=False)
    g["is_done"]   = g["contents"].astype(str).str.contains("MES 바코드 조회 완료", na=False)

    # from_row 선택
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

    # (변경) dayornight 제거, time -> end_time
    from_rows = g.loc[from_indices, ["end_day", "station", "end_time", "contents"]].copy()
    from_rows = from_rows.rename(columns={"end_time": "from_time", "contents": "from_contents"})
    from_rows["from_key"] = time_to_seconds(from_rows["from_time"])

    done_rows = g[g["is_done"]][["end_time", "contents"]].copy()
    if done_rows.empty:
        return []

    done_rows = done_rows.rename(columns={"end_time": "to_time", "contents": "to_contents"})
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
        "from_contents",
        "from_time",
        "to_contents",
        "to_time",
        "wasted_time",
    ]].copy()

    return out.to_dict("records")


# ============================================
# [4] 메인 로직
# ============================================
def main():
    engine = get_engine()

    # Vision1/2 로딩 (변경: time -> end_time, dayornight 제거)
    q1 = text(f"""
        SELECT end_day, end_time, contents, 'Vision1'::text AS station
        FROM {VISION1_TABLE}
        ORDER BY end_day, end_time;
    """)
    q2 = text(f"""
        SELECT end_day, end_time, contents, 'Vision2'::text AS station
        FROM {VISION2_TABLE}
        ORDER BY end_day, end_time;
    """)

    df_v1 = pd.read_sql(q1, engine)
    df_v2 = pd.read_sql(q2, engine)
    df = pd.concat([df_v1, df_v2], ignore_index=True)

    # 교대 경계 제외 + 정렬
    df = filter_shift_boundary(df)
    df = df.sort_values(["station", "end_day", "end_time"]).reset_index(drop=True)

    # (station, end_day) 그룹 생성
    group_items = [((st, day), g.copy()) for (st, day), g in df.groupby(["station", "end_day"], sort=False)]

    # 멀티프로세스 실행
    workers = max(1, min(cpu_count() - 1, 8))  # 과도한 프로세스 방지(원하면 숫자 조정)
    with Pool(processes=workers) as pool:
        results = pool.map(process_one_group, group_items)

    # 결과 합치기
    flat_rows = [r for sub in results for r in sub]
    if not flat_rows:
        result_df = pd.DataFrame(columns=[
            "id", "end_day", "station",
            "from_contents", "from_time", "to_contents", "to_time", "wasted_time"
        ])
    else:
        result_df = pd.DataFrame(flat_rows)

        # end_day CHAR(8) 맞추기
        result_df["end_day"] = (
            result_df["end_day"].astype(str)
            .str.replace(",", "", regex=False)
            .str.zfill(8)
        )

        result_df = result_df.reset_index(drop=True)
        result_df.insert(0, "id", result_df.index + 1)

    # DROP → CREATE (변경: dayornight 제거)
    drop_sql = f"DROP TABLE IF EXISTS {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME};"
    create_sql = f"""
    CREATE TABLE {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (
        id            INTEGER,
        end_day       CHAR(8),
        station       TEXT,
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

    # INSERT
    if not result_df.empty:
        result_df.to_sql(
            OUT_TABLE_NAME,
            engine,
            schema=OUT_TABLE_SCHEMA,
            if_exists="append",
            index=False
        )

    print(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows={len(result_df)})")


if __name__ == "__main__":
    freeze_support()  # Windows 멀티프로세스 안정화
    main()
