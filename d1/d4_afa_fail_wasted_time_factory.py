# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 스크립트 (REALTIME, 누적 적재 + 최근 10분 최적화)

- 대상: d1_machine_log.FCT1~4_machine_log
- 이벤트:
  1) NG_TEXT  = "제품 감지 NG"           (from)
  2) OFF_TEXT = "제품 검사 투입요구 ON"  (to)
- 규칙:
  - Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
  - (동일 station, 동일 end_day) 기준으로 NG -> OFF 한 쌍을 1건으로 기록
- 저장:
  - d1_machine_log.afa_fail_wasted_time 로 "누적 적재(append)"
  - UNIQUE 키로 중복을 영구 차단하고 ON CONFLICT DO NOTHING 적용
- 실행:
  - 멀티프로세스 2개 고정 (P1:FCT1~2 / P2:FCT3~4)
  - 오늘 날짜(end_day=YYYYMMDD)만 대상으로 함
  - DB 조회는 "최근 10분" 데이터만 읽도록 최적화
  - 1분(60초)마다 무한 루프 실행

[설명]
- end_time이 "HH:MM:SS" 또는 "HH:MM:SS.xx" 형태일 수 있어,
  SQL에서 split_part(end_time,'.',1)로 소수점 이하를 제거한 뒤 timestamp로 변환합니다.
"""

import time
from datetime import datetime
import urllib.parse
from multiprocessing import freeze_support
from concurrent.futures import ProcessPoolExecutor

import pandas as pd
from sqlalchemy import create_engine, text


# ============================================
# 0. DB / 상수 설정
# ============================================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_MACHINE = "d1_machine_log"

# 멀티프로세스 2개 고정 분할
GROUP_P1 = [("FCT1_machine_log", "FCT1"), ("FCT2_machine_log", "FCT2")]
GROUP_P2 = [("FCT3_machine_log", "FCT3"), ("FCT4_machine_log", "FCT4")]

# 이벤트 텍스트(원문 그대로 매칭)
NG_TEXT = "제품 감지 NG"
OFF_TEXT = "제품 검사 투입요구 ON"
MANUAL_TEXT = "Manual mode 전환"
AUTO_TEXT = "Auto mode 전환"

TABLE_SAVE_SCHEMA = "d1_machine_log"
TABLE_SAVE_NAME = "afa_fail_wasted_time"

# 1분마다 실행
LOOP_INTERVAL_SEC = 60

# 최근 N분만 조회(최적화)
LOOKBACK_MIN = 10


# ============================================
# 1. DB 유틸
# ============================================
def get_engine(config=DB_CONFIG):
    """
    SQLAlchemy 엔진 생성
    - password는 URL 인코딩 처리(특수문자 대비)
    """
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)


# ============================================
# 2. 저장 테이블 초기화 (누적 적재 + 중복 방지)
# ============================================
def init_save_table():
    """
    결과 테이블을 누적 적재 방식으로 운용하기 위한 준비
    - 테이블 없으면 생성
    - 중복 방지용 UNIQUE INDEX 생성
    """
    engine = get_engine(DB_CONFIG)

    ddl = text(f"""
    CREATE SCHEMA IF NOT EXISTS {TABLE_SAVE_SCHEMA};

    CREATE TABLE IF NOT EXISTS {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (
        id BIGSERIAL PRIMARY KEY,
        end_day TEXT,
        station TEXT,
        from_contents TEXT,
        from_time TEXT,
        to_contents TEXT,
        to_time TEXT,
        wasted_time NUMERIC(10,2),
        created_at TIMESTAMP DEFAULT now()
    );

    -- 같은 페어는 1번만 적재되도록 중복 방지 키 설정
    CREATE UNIQUE INDEX IF NOT EXISTS ux_afa_fail_wasted_time
    ON {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME}
        (end_day, station, from_time, to_time, from_contents, to_contents);
    """)

    with engine.begin() as conn:
        conn.execute(ddl)


# ============================================
# 3. FCT 로그 로드 (프로세스 단위 실행)
#    - 오늘 날짜 end_day만
#    - 최근 LOOKBACK_MIN 분만 조회(최적화)
#    - 멀티프로세스는 2개 고정(P1/P2)
# ============================================
def load_fct_group_mp(args):
    """
    하나의 프로세스가 그룹(FCT 2개)을 읽어 합친 DF 반환
    """
    group_tables, db_config, schema_name, today_yyyymmdd, lookback_min = args
    engine = get_engine(db_config)

    dfs = []
    for table_name, station_label in group_tables:
        # end_time에 소수점이 있을 수 있어 split_part로 "HH:MM:SS"만 사용
        # end_day(YYYYMMDD) + end_time(HH:MM:SS) => timestamp 변환 후 최근 N분 필터
        sql = text(f"""
            SELECT
                end_day,
                end_time,
                contents
            FROM {schema_name}."{table_name}"
            WHERE end_day = :today
              AND to_timestamp(end_day || ' ' || split_part(end_time, '.', 1), 'YYYYMMDD HH24:MI:SS')
                    >= now() - make_interval(mins => :lookback_min)
            ORDER BY end_time ASC
        """)

        df = pd.read_sql(
            sql,
            engine,
            params={"today": today_yyyymmdd, "lookback_min": int(lookback_min)},
        )

        if not df.empty:
            df["station"] = station_label
            dfs.append(df)

    if not dfs:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    return pd.concat(dfs, ignore_index=True)


def load_recent_fct_logs_mp2(today_yyyymmdd: str, lookback_min: int) -> pd.DataFrame:
    """
    멀티프로세스 2개 고정:
    - P1: FCT1,FCT2
    - P2: FCT3,FCT4
    최근 lookback_min 분만 조회
    """
    tasks = [
        (GROUP_P1, DB_CONFIG, SCHEMA_MACHINE, today_yyyymmdd, lookback_min),
        (GROUP_P2, DB_CONFIG, SCHEMA_MACHINE, today_yyyymmdd, lookback_min),
    ]

    dfs = []
    with ProcessPoolExecutor(max_workers=2) as ex:
        futs = [ex.submit(load_fct_group_mp, t) for t in tasks]
        for f in futs:
            dfs.append(f.result())

    if not dfs:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    return pd.concat(dfs, ignore_index=True)


# ============================================
# 4. 계산 로직
# ============================================
def compute_afa_fail_wasted(df_all: pd.DataFrame) -> pd.DataFrame:
    """
    station + end_day 단위로 이벤트 시퀀스를 훑으며
    - Manual~Auto 구간에서는 NG 무시
    - NG(첫 등장) -> OFF(첫 등장) 매칭되면 1건 기록
    """
    if df_all.empty:
        return pd.DataFrame(
            columns=[
                "end_day", "station",
                "from_contents", "from_time",
                "to_contents", "to_time",
                "wasted_time",
            ]
        )

    df_evt = df_all[df_all["contents"].isin([NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT])].copy()
    if df_evt.empty:
        return pd.DataFrame(
            columns=[
                "end_day", "station",
                "from_contents", "from_time",
                "to_contents", "to_time",
                "wasted_time",
            ]
        )

    # 정렬(문자열 end_time 기반) + 실제 계산은 ts로 수행
    df_evt = df_evt.sort_values(["end_day", "station", "end_time"]).reset_index(drop=True)

    result_rows = []

    for (end_day, station), grp in df_evt.groupby(["end_day", "station"], sort=False):
        pending_from_ts = None
        in_manual = False

        for _, row in grp.iterrows():
            contents = row["contents"]
            t = row["end_time"]

            ts = pd.to_datetime(f"{str(end_day)} {str(t)}", errors="coerce")
            if pd.isna(ts):
                continue

            if contents == MANUAL_TEXT:
                in_manual = True
                continue
            if contents == AUTO_TEXT:
                in_manual = False
                continue

            if contents == NG_TEXT:
                if (not in_manual) and (pending_from_ts is None):
                    pending_from_ts = ts
                continue

            if contents == OFF_TEXT and pending_from_ts is not None:
                from_ts = pending_from_ts
                to_ts = ts

                result_rows.append(
                    {
                        "end_day": str(end_day),
                        "station": station,
                        "from_contents": NG_TEXT,
                        "from_time": from_ts.strftime("%H:%M:%S.%f")[:-4],
                        "to_contents": OFF_TEXT,
                        "to_time": to_ts.strftime("%H:%M:%S.%f")[:-4],
                        "wasted_time": round(abs((to_ts - from_ts).total_seconds()), 2),
                    }
                )
                pending_from_ts = None

    df_wasted = pd.DataFrame(result_rows)
    if not df_wasted.empty:
        df_wasted = df_wasted.sort_values(["end_day", "station", "from_time"]).reset_index(drop=True)

    return df_wasted


# ============================================
# 5. DB 저장 (누적 적재 + 중복 무시)
# ============================================
def save_to_db_append_dedup(df_wasted: pd.DataFrame) -> int:
    """
    결과를 누적 적재하되, UNIQUE 키 충돌은 무시(중복 방지)
    - INSERT ... ON CONFLICT DO NOTHING
    """
    if df_wasted.empty:
        return 0

    engine = get_engine(DB_CONFIG)

    ins = text(f"""
        INSERT INTO {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME}
            (end_day, station, from_contents, from_time, to_contents, to_time, wasted_time)
        VALUES
            (:end_day, :station, :from_contents, :from_time, :to_contents, :to_time, :wasted_time)
        ON CONFLICT (end_day, station, from_time, to_time, from_contents, to_contents)
        DO NOTHING
    """)

    rows = df_wasted[
        ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]
    ].to_dict("records")

    with engine.begin() as conn:
        conn.execute(ins, rows)

    return len(rows)


# ============================================
# 6. main (실시간 무한 루프, 1분 주기, 최근 10분 조회)
# ============================================
def main():
    print("==============================================================================")
    print(f"[START] AFA FAIL wasted time REALTIME (MP=2, TodayOnly, LOOKBACK={LOOKBACK_MIN}m, APPEND+DEDUP) | {datetime.now():%Y-%m-%d %H:%M:%S}")
    print(f"[INFO] DB = {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    print(f"[INFO] SRC = {SCHEMA_MACHINE}.FCT1~4_machine_log (today only, last {LOOKBACK_MIN} minutes)")
    print(f"[INFO] SAVE TABLE = {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (append + dedup)")
    print(f"[INFO] LOOP_INTERVAL_SEC = {LOOP_INTERVAL_SEC} (1 minute)")
    print("==============================================================================")

    # 결과 테이블/인덱스 준비(최초 1회)
    try:
        init_save_table()
    except Exception as e:
        print(f"[ERROR] init_save_table failed: {type(e).__name__}: {e}")
        return

    while True:
        loop_start = time.perf_counter()
        today_yyyymmdd = datetime.now().strftime("%Y%m%d")

        try:
            # 1) 오늘자 + 최근 LOOKBACK_MIN분만 MP=2로 로드
            df_all = load_recent_fct_logs_mp2(today_yyyymmdd, LOOKBACK_MIN)

            # 2) 계산
            df_wasted = compute_afa_fail_wasted(df_all)

            # 3) 누적 적재(중복은 무시)
            attempted = save_to_db_append_dedup(df_wasted)

            print(
                f"[{datetime.now():%H:%M:%S}] today={today_yyyymmdd} | "
                f"lookback={LOOKBACK_MIN}m | src_rows={len(df_all)} | computed_pairs={len(df_wasted)} | attempted_insert={attempted} (dedup)"
            )

        except KeyboardInterrupt:
            print("\n[STOP] KeyboardInterrupt -> exit")
            break
        except Exception as e:
            print(f"[ERROR] {type(e).__name__}: {e}")

        # 1분 간격 페이싱
        elapsed = time.perf_counter() - loop_start
        sleep_sec = LOOP_INTERVAL_SEC - elapsed
        if sleep_sec > 0:
            time.sleep(sleep_sec)


if __name__ == "__main__":
    freeze_support()
    main()
