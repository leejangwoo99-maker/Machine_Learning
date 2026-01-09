# -*- coding: utf-8 -*-
"""
d5_MES_fail_wasted_time_factory_realtime.py

[공장 실시간 사양]
1) 멀티프로세스 2개 고정: Vision1 / Vision2 각각 담당
2) 1분 단위 무한 루프
3) 현재 시간 기준 "10분 전 ~ 현재" 범위의 end_time 데이터만 조회
4) 결과는 d1_machine_log.mes_fail_wasted_time 에 적재(기존 유지 + 신규만 추가)
   - 중복 방지는 UNIQUE (end_day, station, from_time, to_time) + ON CONFLICT UPSERT

[로직]
- 조회한 로그를 end_time 오름차순으로 정렬 후 스캔
- contents에 'MES'가 "연속 5회 이상"인 구간을 찾고
  - 그 구간의 첫 행을 from_time/from_contents
  - 그 구간 이후 처음 나오는 'MES 바코드 조회 완료'를 to_time/to_contents
  - wasted_time = (to_time - from_time) 초(소수점 2자리)

주의
- 최근 10분 데이터만 보므로, MES 연속 구간이 10분 경계 밖에서 시작하면 일부 케이스를 놓칠 수 있음
  (요구사항이 10분 조회 고정이므로 그대로 구현)
"""

import time as time_mod
from datetime import datetime, date, timedelta, time
from typing import Optional, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text
from multiprocessing import Process, Event


# =========================
# DB 설정 (요구사항)
# =========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

SRC_TABLES = {
    "Vision1": f'{SCHEMA}."Vision1_machine_log"',
    "Vision2": f'{SCHEMA}."Vision2_machine_log"',
}

OUT_TABLE = f'{SCHEMA}."mes_fail_wasted_time"'

# id 시퀀스 (이미 앞에서 정상 세팅하셨던 그대로 유지)
ID_SEQ_NAME = "mes_fail_wasted_time_id_seq"
ID_SEQ_REGCLASS = f"{SCHEMA}.{ID_SEQ_NAME}"


# =========================
# 실시간 루프 파라미터
# =========================
LOOP_INTERVAL_SEC = 60     # 1분 단위
LOOKBACK_MIN = 10          # 최근 10분 조회

MES_REPEAT_MIN = 5
TO_MATCH_TEXT = "MES 바코드 조회 완료"


# =========================
# 엔진 생성
# =========================
def make_engine():
    url = (
        f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}'
        f'@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'
    )
    return create_engine(url, pool_pre_ping=True)


# =========================
# 로컬 오늘 날짜 (윈도우 기준)
# =========================
def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


# =========================
# end_day + end_time(str) -> datetime
# =========================
def parse_end_dt(end_day: str, end_time_str: str) -> Optional[datetime]:
    """
    end_time: 'HH:MM:SS' 또는 'HH:MM:SS.xx' (xx 자리수 가변)
    """
    if end_time_str is None:
        return None
    s = str(end_time_str).strip()
    if not s:
        return None

    try:
        d = datetime.strptime(end_day, "%Y%m%d").date()

        if "." in s:
            hhmmss, frac = s.split(".", 1)
            t = datetime.strptime(hhmmss, "%H:%M:%S").time()

            frac_digits = "".join(ch for ch in frac if ch.isdigit())
            if not frac_digits:
                us = 0
            else:
                frac_digits = (frac_digits + "000000")[:6]
                us = int(frac_digits)

            return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, us)

        else:
            t = datetime.strptime(s, "%H:%M:%S").time()
            return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, 0)

    except Exception:
        return None


# =========================
# 최근 10분 데이터 조회 SQL
# - end_time이 TEXT여도 time 비교 가능하게 안전 캐스팅
# =========================
SQL_FETCH_LAST_10MIN = """
SELECT
  end_day,
  station,
  end_time,
  contents
FROM {table_full}
WHERE end_day = :end_day
  AND station = :station
  AND (
    CASE
      WHEN position('.' in end_time) > 0
        THEN to_timestamp(end_time, 'HH24:MI:SS.MS')::time
      ELSE
        to_timestamp(end_time, 'HH24:MI:SS')::time
    END
  ) BETWEEN :t_start AND :t_end
ORDER BY
  CASE
    WHEN position('.' in end_time) > 0
      THEN to_timestamp(end_time, 'HH24:MI:SS.MS')::time
    ELSE
      to_timestamp(end_time, 'HH24:MI:SS')::time
  END ASC
"""


def fetch_last_10min_log(engine, table_full: str, station: str, end_day: str,
                         t_start: time, t_end: time) -> pd.DataFrame:
    """
    오늘(end_day) 중에서, end_time이 [t_start ~ t_end] 범위인 데이터만 로딩
    """
    q = text(SQL_FETCH_LAST_10MIN.format(table_full=table_full))
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={
            "end_day": end_day,
            "station": station,
            "t_start": t_start,
            "t_end": t_end
        })

    if df.empty:
        return df

    # 혹시라도 동일 time에서 문자열 정렬 꼬임 방지용으로 python에서 한 번 더 정렬
    df["__dt"] = df["end_time"].apply(lambda x: parse_end_dt(end_day, x))
    df = df.sort_values(["__dt", "end_time"], ascending=True).reset_index(drop=True)
    df = df.drop(columns=["__dt"])
    return df


# =========================
# MES 구간 추출
# =========================
def build_wasted_rows(df_log: pd.DataFrame, station: str, end_day: str) -> pd.DataFrame:
    """
    return DF columns:
      [end_day, station, from_contents, from_time, to_contents, to_time, wasted_time]
    """
    cols_out = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]

    if df_log.empty:
        return pd.DataFrame(columns=cols_out)

    is_mes = df_log["contents"].astype(str).str.contains("MES", na=False)
    is_done = df_log["contents"].astype(str).str.contains(TO_MATCH_TEXT, na=False)

    n = len(df_log)
    i = 0
    rows: List[Dict] = []

    while i < n:
        if not is_mes.iloc[i]:
            i += 1
            continue

        # MES 연속 구간 [start, j)
        start = i
        j = i
        while j < n and is_mes.iloc[j]:
            j += 1

        run_len = j - start
        if run_len < MES_REPEAT_MIN:
            i = j
            continue

        from_time = str(df_log.loc[start, "end_time"])
        from_contents = str(df_log.loc[start, "contents"])

        # 구간 이후 첫 '조회 완료'
        k = j
        to_idx = None
        while k < n:
            if is_done.iloc[k]:
                to_idx = k
                break
            k += 1

        if to_idx is None:
            i = j
            continue

        to_time = str(df_log.loc[to_idx, "end_time"])
        to_contents = str(df_log.loc[to_idx, "contents"])

        dt_from = parse_end_dt(end_day, from_time)
        dt_to = parse_end_dt(end_day, to_time)

        wasted_time = None
        if dt_from is not None and dt_to is not None:
            wasted_time = round(float((dt_to - dt_from).total_seconds()), 2)

        rows.append({
            "end_day": end_day,
            "station": station,
            "from_contents": from_contents,
            "from_time": from_time,
            "to_contents": to_contents,
            "to_time": to_time,
            "wasted_time": wasted_time,
        })

        # 겹침 방지: to 다음부터
        i = to_idx + 1

    return pd.DataFrame(rows, columns=cols_out)


# =========================
# 출력 테이블 보장(UNIQUE + id 시퀀스/DEFAULT + timestamps)
# =========================
def ensure_out_table(engine):
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    CREATE TABLE IF NOT EXISTS {OUT_TABLE} (
        id BIGINT,
        end_day TEXT NOT NULL,
        station TEXT NOT NULL,
        from_contents TEXT,
        from_time TEXT NOT NULL,
        to_contents TEXT,
        to_time TEXT NOT NULL,
        wasted_time NUMERIC(12,2)
    );

    ALTER TABLE {OUT_TABLE} ADD COLUMN IF NOT EXISTS id BIGINT;
    ALTER TABLE {OUT_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();
    ALTER TABLE {OUT_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();

    CREATE UNIQUE INDEX IF NOT EXISTS ux_mes_fail_wasted_time_key
      ON {OUT_TABLE} (end_day, station, from_time, to_time);

    CREATE INDEX IF NOT EXISTS ix_mes_fail_wasted_time_day_station
      ON {OUT_TABLE} (end_day, station);
    """

    id_fix_sql = f"""
    CREATE SEQUENCE IF NOT EXISTS {ID_SEQ_REGCLASS};

    ALTER TABLE {OUT_TABLE}
      ALTER COLUMN id SET DEFAULT nextval('{ID_SEQ_REGCLASS}'::regclass);

    ALTER SEQUENCE {ID_SEQ_REGCLASS}
      OWNED BY {OUT_TABLE}.id;

    SELECT setval(
        '{ID_SEQ_REGCLASS}'::regclass,
        COALESCE((SELECT MAX(id) FROM {OUT_TABLE}), 0) + 1,
        false
    );
    """

    backfill_sql = f"""
    UPDATE {OUT_TABLE}
    SET id = nextval('{ID_SEQ_REGCLASS}'::regclass)
    WHERE id IS NULL;
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(id_fix_sql))
        conn.execute(text(backfill_sql))


# =========================
# UPSERT(적재 + 중복방지)
# =========================
def upsert_rows(engine, df_rows: pd.DataFrame):
    if df_rows.empty:
        return

    sql = f"""
    INSERT INTO {OUT_TABLE}
      (end_day, station, from_contents, from_time, to_contents, to_time, wasted_time, created_at, updated_at)
    VALUES
      (:end_day, :station, :from_contents, :from_time, :to_contents, :to_time, :wasted_time, now(), now())
    ON CONFLICT (end_day, station, from_time, to_time)
    DO UPDATE SET
      from_contents = EXCLUDED.from_contents,
      to_contents   = EXCLUDED.to_contents,
      wasted_time   = EXCLUDED.wasted_time,
      updated_at    = now();
    """

    payload = df_rows.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), payload)


# =========================
# Worker: station 1개 담당 (Vision1 또는 Vision2)
# =========================
def worker_loop(station: str, stop_event: Event):
    engine = make_engine()
    table_full = SRC_TABLES[station]

    print(f"[START] MES fail wasted time REALTIME | station={station} | loop={LOOP_INTERVAL_SEC}s | lookback={LOOKBACK_MIN}min")

    while not stop_event.is_set():
        t0 = time_mod.time()

        try:
            end_day = today_yyyymmdd()

            now_dt = datetime.now()
            start_dt = now_dt - timedelta(minutes=LOOKBACK_MIN)

            # 요구사항: "현재 시간 기준 10분 전 데이터(end_time)만 조회"
            # -> end_day는 오늘 고정, time 범위는 [start_time ~ now_time]
            # (자정 넘김 케이스는 요구사항에 없으므로 오늘 기준으로만 처리)
            t_start = start_dt.time().replace(microsecond=0)
            t_end = now_dt.time().replace(microsecond=0)

            df_log = fetch_last_10min_log(
                engine=engine,
                table_full=table_full,
                station=station,
                end_day=end_day,
                t_start=t_start,
                t_end=t_end
            )

            df_rows = build_wasted_rows(df_log, station=station, end_day=end_day)
            upsert_rows(engine, df_rows)

            if not df_rows.empty:
                print(f"[{station}] inserted/upserted rows={len(df_rows)} | window={t_start}~{t_end}")

        except Exception as e:
            # 어떤 상황에서도 프로세스가 멈추지 않도록 예외는 출력만 하고 continue
            print(f"[{station}][ERROR] {e}")

        # 1분 주기 페이싱(처리 시간 제외)
        elapsed = time_mod.time() - t0
        sleep_sec = max(0.0, LOOP_INTERVAL_SEC - elapsed)
        stop_event.wait(timeout=sleep_sec)

    print(f"[END] worker stopped | station={station}")


# =========================
# Main
# =========================
def main():
    # 출력 테이블/인덱스/시퀀스 보장(메인에서 1회)
    engine = make_engine()
    ensure_out_table(engine)

    stop_event = Event()

    p1 = Process(target=worker_loop, args=("Vision1", stop_event), daemon=True)
    p2 = Process(target=worker_loop, args=("Vision2", stop_event), daemon=True)

    p1.start()
    p2.start()

    try:
        while True:
            time_mod.sleep(1)
    except KeyboardInterrupt:
        print("[MAIN] KeyboardInterrupt -> stopping workers...")
        stop_event.set()
        p1.join(timeout=10)
        p2.join(timeout=10)
        print("[MAIN] all workers stopped.")


if __name__ == "__main__":
    main()
