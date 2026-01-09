# -*- coding: utf-8 -*-
"""
d5_MES_fail_wasted_time.py

Vision1/Vision2 머신로그에서
- 'MES' 포함 로그가 "연속 5회 이상" 발생한 구간을 찾고 (오름차순 end_time 기준)
- 그 구간의 첫 행을 from_time/from_contents
- 그 구간 이후 처음 등장하는 'MES 바코드 조회 완료' 행을 to_time/to_contents
- wasted_time = (to_time - from_time) 초(소수점 2자리)
- 결과를 d1_machine_log.mes_fail_wasted_time 로 UPSERT 저장

[핵심 보장 사항]
1) ON CONFLICT를 쓰려면 (end_day, station, from_time, to_time)에 UNIQUE/PK가 반드시 있어야 함
   -> UNIQUE INDEX를 IF NOT EXISTS로 강제 생성

2) id가 NULL로 들어가는 문제 해결
   -> 기존 테이블이 과거에 id DEFAULT(nextval)가 없는 상태로 생성되었을 가능성 대응
   -> init에서:
      - 시퀀스 생성(IF NOT EXISTS)
      - id DEFAULT nextval('schema.seq'::regclass) 강제
      - 시퀀스 OWNED BY 연결
      - setval(max(id)+1) 동기화
   -> 기존에 이미 쌓인 id IS NULL 행은 backfill로 채움(1회성)

요구 DF 컬럼:
[id, end_day, station, from_contents, from_time, to_contents, to_time, wasted_time]
"""

import os
from datetime import datetime, date
from typing import Optional, List, Dict

import pandas as pd
from sqlalchemy import create_engine, text


# =========================
# DB 설정 (요구사항)
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
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

# id 시퀀스 이름(고정)
ID_SEQ_NAME = "mes_fail_wasted_time_id_seq"
ID_SEQ_REGCLASS = f"{SCHEMA}.{ID_SEQ_NAME}"  # regclass로 캐스팅해서 사용


# =========================
# 로직 파라미터
# =========================
MES_REPEAT_MIN = 5
TO_MATCH_TEXT = "MES 바코드 조회 완료"

# (선택) CSV도 같이 저장하고 싶으면 True
SAVE_CSV = False
CSV_DIR = os.getcwd()
CSV_NAME_FMT = "mes_fail_wasted_time_{end_day}.csv"


# =========================
# DB 연결
# =========================
def make_engine():
    url = (
        f'postgresql+psycopg2://{DB_CONFIG["user"]}:{DB_CONFIG["password"]}'
        f'@{DB_CONFIG["host"]}:{DB_CONFIG["port"]}/{DB_CONFIG["dbname"]}'
    )
    return create_engine(url, pool_pre_ping=True)


# =========================
# 윈도우(로컬) 오늘 날짜
# =========================
def today_yyyymmdd() -> str:
    return date.today().strftime("%Y%m%d")


# =========================
# end_day + end_time -> datetime 변환
# =========================
def parse_end_dt(end_day: str, end_time_str: str) -> Optional[datetime]:
    """
    end_time이 'HH:MM:SS' 또는 'HH:MM:SS.xx' 형태라고 가정.
    'xx'는 소수초(자리수 가변)이며, microseconds로 환산해서 datetime 생성.
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
                # microsecond(6자리)로 패딩/절삭
                frac_digits = (frac_digits + "000000")[:6]
                us = int(frac_digits)

            return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, us)

        else:
            t = datetime.strptime(s, "%H:%M:%S").time()
            return datetime(d.year, d.month, d.day, t.hour, t.minute, t.second, 0)

    except Exception:
        return None


# =========================
# 소스 테이블 로딩 (오늘 데이터만)
# =========================
SQL_FETCH = """
SELECT
  end_day,
  station,
  end_time,
  contents
FROM {table_full}
WHERE end_day = :end_day
  AND station = :station
ORDER BY end_time ASC
"""

def fetch_today_log(engine, table_full: str, station: str, end_day: str) -> pd.DataFrame:
    q = text(SQL_FETCH.format(table_full=table_full))
    with engine.connect() as conn:
        df = pd.read_sql(q, conn, params={"end_day": end_day, "station": station})

    if df.empty:
        return df

    # 문자열 정렬만 믿지 않고, 실제 datetime 기준으로 한 번 더 정렬(안정성)
    df["__dt"] = df["end_time"].apply(lambda x: parse_end_dt(end_day, x))
    df = df.sort_values(["__dt", "end_time"], ascending=True).reset_index(drop=True)
    df = df.drop(columns=["__dt"])
    return df


# =========================
# 핵심 로직: MES 연속 5회 + 이후 조회완료 매칭
# =========================
def build_wasted_df(df_log: pd.DataFrame, station: str, end_day: str) -> pd.DataFrame:
    """
    df_log columns: [end_day, station, end_time, contents]
    return columns:
      [id, end_day, station, from_contents, from_time, to_contents, to_time, wasted_time]
    """
    cols_out = ["id", "end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]

    if df_log.empty:
        return pd.DataFrame(columns=cols_out)

    # 'MES' 포함 여부 (연속 구간 판단)
    is_mes = df_log["contents"].astype(str).str.contains("MES", na=False)

    # '조회 완료' 매칭 여부
    is_done = df_log["contents"].astype(str).str.contains(TO_MATCH_TEXT, na=False)

    n = len(df_log)
    i = 0
    rows: List[Dict] = []

    while i < n:
        if not is_mes.iloc[i]:
            i += 1
            continue

        # MES 연속 구간 찾기: [start, j)
        start = i
        j = i
        while j < n and is_mes.iloc[j]:
            j += 1

        run_len = j - start
        if run_len < MES_REPEAT_MIN:
            i = j
            continue

        # from = 연속 MES 구간의 첫 행
        from_time = str(df_log.loc[start, "end_time"])
        from_contents = str(df_log.loc[start, "contents"])

        # to = 연속 구간 이후에서 처음 등장하는 'MES 바코드 조회 완료'
        k = j
        to_idx = None
        while k < n:
            if is_done.iloc[k]:
                to_idx = k
                break
            k += 1

        if to_idx is None:
            # 조회 완료가 없으면 결과 생성 불가 -> 다음 탐색
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

        # 겹침/중복 방지: to 행 다음부터 다시 탐색
        i = to_idx + 1

    out = pd.DataFrame(rows)
    if out.empty:
        return pd.DataFrame(columns=cols_out)

    # 이 id는 "출력 DF 표시용" (DB의 id와 별개)
    out.insert(0, "id", range(1, len(out) + 1))
    out = out[cols_out]
    return out


# =========================
# 출력 테이블 초기화 + UNIQUE 보장 + id 자동채번 보장
# =========================
def ensure_out_table_and_unique(engine):
    """
    1) 테이블 없으면 생성
    2) created_at/updated_at 없으면 추가
    3) UNIQUE INDEX 보장
    4) id 시퀀스/DEFAULT 보장 + setval 동기화

    ※ 이번 버전은 DO 블록을 쓰지 않음(따옴표/문법 꼬임 방지)
    """
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA};

    -- 테이블 없으면 생성 (id 포함)
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

    -- id 컬럼이 없을 가능성까지 대비(혹시라도)
    ALTER TABLE {OUT_TABLE} ADD COLUMN IF NOT EXISTS id BIGINT;

    -- 타임스탬프 컬럼 보장
    ALTER TABLE {OUT_TABLE} ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now();
    ALTER TABLE {OUT_TABLE} ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT now();

    -- ON CONFLICT 타겟 UNIQUE 인덱스 보장
    CREATE UNIQUE INDEX IF NOT EXISTS ux_mes_fail_wasted_time_key
      ON {OUT_TABLE} (end_day, station, from_time, to_time);

    CREATE INDEX IF NOT EXISTS ix_mes_fail_wasted_time_day_station
      ON {OUT_TABLE} (end_day, station);
    """

    # id 자동채번 보장: 시퀀스 + DEFAULT(nextval) + OWNED BY + setval 동기화
    # 핵심: nextval('schema.seq'::regclass) 형태로 안전하게 사용
    id_fix_sql = f"""
    CREATE SEQUENCE IF NOT EXISTS {ID_SEQ_REGCLASS};

    ALTER TABLE {OUT_TABLE}
      ALTER COLUMN id SET DEFAULT nextval('{ID_SEQ_REGCLASS}'::regclass);

    ALTER SEQUENCE {ID_SEQ_REGCLASS}
      OWNED BY {OUT_TABLE}.id;

    -- 시퀀스 값을 현재 max(id)+1로 맞춤 (NULL만 있으면 1부터)
    SELECT setval(
        '{ID_SEQ_REGCLASS}'::regclass,
        COALESCE((SELECT MAX(id) FROM {OUT_TABLE}), 0) + 1,
        false
    );
    """

    with engine.begin() as conn:
        conn.execute(text(ddl))
        conn.execute(text(id_fix_sql))


def backfill_null_ids(engine):
    """
    기존 데이터 중 id가 NULL인 행을 시퀀스로 채움.
    (최초 1회성 목적이지만, 여러 번 실행해도 NULL만 채우므로 안전)
    """
    sql = f"""
    UPDATE {OUT_TABLE}
    SET id = nextval('{ID_SEQ_REGCLASS}'::regclass)
    WHERE id IS NULL;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))


# =========================
# UPSERT 저장
# =========================
def upsert_out(engine, df_out: pd.DataFrame):
    """
    (end_day, station, from_time, to_time) 기준 UPSERT
    - id는 DB에서 자동 채번되므로 INSERT에 포함하지 않음
    """
    if df_out.empty:
        return

    df_load = df_out.drop(columns=["id"], errors="ignore").copy()
    cols = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]
    df_load = df_load[cols]

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

    payload = df_load.to_dict(orient="records")
    with engine.begin() as conn:
        conn.execute(text(sql), payload)


# =========================
# 메인
# =========================
def main():
    end_day = today_yyyymmdd()
    engine = make_engine()

    # 1) 출력 테이블/인덱스/시퀀스/DEFAULT 보장
    ensure_out_table_and_unique(engine)

    # 2) 과거에 id NULL로 쌓인 행이 있으면 채움
    backfill_null_ids(engine)

    # 3) Vision1/2 각각 로딩 -> 결과 생성
    outs = []
    for station, table_full in SRC_TABLES.items():
        df_log = fetch_today_log(engine, table_full=table_full, station=station, end_day=end_day)
        df_out = build_wasted_df(df_log, station=station, end_day=end_day)
        outs.append(df_out)

    # 4) 합치고 id 재부여(보기용 DF)
    df_final = pd.concat(outs, ignore_index=True)
    if not df_final.empty:
        df_final["id"] = range(1, len(df_final) + 1)
        df_final = df_final[["id", "end_day", "station", "from_contents", "from_time",
                             "to_contents", "to_time", "wasted_time"]]

    # 5) 콘솔 출력
    print(df_final)

    # 6) CSV 저장(선택)
    if SAVE_CSV and not df_final.empty:
        os.makedirs(CSV_DIR, exist_ok=True)
        csv_path = os.path.join(CSV_DIR, CSV_NAME_FMT.format(end_day=end_day))
        df_final.to_csv(csv_path, index=False, encoding="utf-8-sig")
        print(f"[OK] CSV saved: {csv_path}")

    # 7) DB 저장(UPSERT)
    upsert_out(engine, df_final)
    print(f'[OK] UPSERT saved -> {OUT_TABLE}')

    return df_final


if __name__ == "__main__":
    main()
