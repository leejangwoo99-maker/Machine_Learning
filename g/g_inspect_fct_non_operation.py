# -*- coding: utf-8 -*-
"""Auto-converted from inspect_fct_non_operation.ipynb

- DataFrame 출력은 제거(요청 반영)
- 실행 시작/종료 시간 콘솔 출력
- EXE(Nuitka/PyInstaller) 실행 시 콘솔 자동 종료 방지

NOTE:
- 본 스크립트는 원본 노트북 로직을 최대한 유지하되, 노트북 전용 출력(display/df 출력)만 제거했습니다.
"""

import sys
from datetime import datetime

def _print_run_banner(start_dt: datetime):
    print("=" * 110, flush=True)
    print(f"[START] {start_dt:%Y-%m-%d %H:%M:%S}", flush=True)
    print("=" * 110, flush=True)

def _print_end_banner(start_dt: datetime, end_dt: datetime):
    print("=" * 110, flush=True)
    print(f"[END]   {end_dt:%Y-%m-%d %H:%M:%S}", flush=True)
    print(f"[ELAPSED] {end_dt - start_dt}", flush=True)
    print("=" * 110, flush=True)

def main():
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine, text

    DB_CONFIG = {
        "host": "100.105.75.47",
        "port": 5432,
        "dbname": "postgres",
        "user": "postgres",
        "password": "",
    }

    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    SCHEMA = "d1_machine_log"
    TARGET_END_DAY = "20260107"  # end_day가 VARCHAR라 문자열로 고정

    print("[OK] engine ready")


    def load_fct_table(table_name: str) -> pd.DataFrame:
        sql = text(f"""
            SELECT
                end_day,
                station,
                contents,
                end_time
            FROM {SCHEMA}."{table_name}"
            WHERE end_day = :end_day
            ORDER BY end_time ASC
        """)
        with engine.connect() as conn:
            df = pd.read_sql(sql, conn, params={"end_day": TARGET_END_DAY})

        # no_operation_time은 "새로 만들어서" 넣는 컬럼
        if not df.empty:
            df["no_operation_time"] = np.nan

        print(f"[LOAD] {table_name}: rows={len(df)}")
        return df


    fct_tables = [f"FCT{i}_machine_log" for i in range(1, 5)]
    df_fct = pd.concat([load_fct_table(t) for t in fct_tables], ignore_index=True)

    print("[OK] total rows:", len(df_fct))


    RESULT_PREFIXES = ("TEST RESULT :: OK", "TEST RESULT :: NG")
    AUTO_START_PREFIX = "TEST AUTO MODE START"

    def parse_ts(day_str: str, time_str: str) -> pd.Timestamp:
        """
        end_day(YYYYMMDD) + end_time(HH:MM:SS.xx) -> Timestamp
        end_time에 소수점이 없을 수도 있어 2단계 파싱
        """
        day_str = str(day_str)
        time_str = str(time_str)

        ts = pd.to_datetime(f"{day_str} {time_str}", format="%Y%m%d %H:%M:%S.%f", errors="coerce")
        if pd.isna(ts):
            ts = pd.to_datetime(f"{day_str} {time_str}", format="%Y%m%d %H:%M:%S", errors="coerce")
        return ts

    if df_fct.empty:
        print("[WARN] 데이터가 0건입니다. end_day=20260105가 실제로 존재하는지 먼저 확인하세요.")
    else:
        # 정렬 보강: station 단위로 end_time 오름차순
        df_fct = df_fct.sort_values(["station", "end_time"], ascending=[True, True]).reset_index(drop=True)

        # timestamp 생성
        df_fct["_ts"] = [parse_ts(d, t) for d, t in zip(df_fct["end_day"], df_fct["end_time"])]

        # station별 스캔
        for station, idxs in df_fct.groupby("station").groups.items():
            last_a_idx = None

            for i in idxs:
                c = str(df_fct.at[i, "contents"])

                # a) TEST RESULT
                if c.startswith(RESULT_PREFIXES):
                    last_a_idx = i
                    continue

                # b) TEST AUTO MODE START
                if c.startswith(AUTO_START_PREFIX):
                    if last_a_idx is not None:
                        t_a = df_fct.at[last_a_idx, "_ts"]
                        t_b = df_fct.at[i, "_ts"]

                        # a -> b 보장 + 파싱 성공 시에만 계산
                        if pd.notna(t_a) and pd.notna(t_b) and (t_b >= t_a):
                            diff_sec = (t_b - t_a).total_seconds()
                            df_fct.at[i, "no_operation_time"] = round(float(diff_sec), 2)

                    # 1페어 처리 후 리셋
                    last_a_idx = None

        # 보조 컬럼 제거
        df_fct.drop(columns=["_ts"], inplace=True)

        print("[OK] no_operation_time calculated")


    out_cols = ["end_day", "station", "contents", "end_time", "no_operation_time"]

    print("========== OUTPUT ==========")

    # 주피터 표로도 보고 싶으면:
    try:
        pass
    except Exception:
        pass


    # ============================================
    # FCT 필터 + a→b Gap 계산 + 최종 출력
    # ============================================

    VALID_PREFIX = (
        "TEST RESULT :: OK",
        "TEST RESULT :: NG",
        "TEST AUTO MODE START"
    )

    # 1) 필요한 로그만 필터링
    df = df_fct[df_fct["contents"].astype(str).str.startswith(VALID_PREFIX)].copy()

    # 2) 정렬 보강
    df = df.sort_values(["station", "end_time"], ascending=[True, True]).reset_index(drop=True)

    # 3) timestamp 생성
    def ts(day, t):
        ts = pd.to_datetime(f"{day} {t}", format="%Y%m%d %H:%M:%S.%f", errors="coerce")
        if pd.isna(ts):
            ts = pd.to_datetime(f"{day} {t}", format="%Y%m%d %H:%M:%S", errors="coerce")
        return ts

    df["_ts"] = [ts(d, t) for d, t in zip(df["end_day"], df["end_time"])]

    # 4) a → b gap 계산
    for station, idxs in df.groupby("station").groups.items():
        last_a = None
        for i in idxs:
            c = df.at[i, "contents"]

            if c.startswith("TEST RESULT"):
                last_a = i

            elif c.startswith("TEST AUTO MODE START") and last_a is not None:
                diff = (df.at[i, "_ts"] - df.at[last_a, "_ts"]).total_seconds()
                df.at[i, "no_operation_time"] = round(float(diff), 2)
                last_a = None

    df.drop(columns="_ts", inplace=True)

    # 5) 최종 출력
    out_cols = ["end_day","station","contents","end_time","no_operation_time"]

    print("========== FILTERED FCT OUTPUT ==========")


    # ============================================
    # Cell 추가) op_ct_gap(del_out_av) 기준 비교 -> real_no_operation_time 생성
    # ============================================

    # (1) op_ct_gap에서 FCT12/FCT34 del_out_av 로드
    sql_gap = text("""
        SELECT station, del_out_av
        FROM g_production_film.op_ct_gap
        WHERE station IN ('FCT12', 'FCT34')
    """)

    with engine.connect() as conn:
        df_gap = pd.read_sql(sql_gap, conn)

    if df_gap.empty:
        raise RuntimeError("[ERROR] g_production_film.op_ct_gap 에서 station IN ('FCT12','FCT34') 데이터를 찾지 못했습니다.")

    # 숫자형 변환 (DB가 NUMERIC이면 보통 자동이지만, 안전하게)
    df_gap["del_out_av"] = pd.to_numeric(df_gap["del_out_av"], errors="coerce")

    th_fct12 = df_gap.loc[df_gap["station"] == "FCT12", "del_out_av"].iloc[0]
    th_fct34 = df_gap.loc[df_gap["station"] == "FCT34", "del_out_av"].iloc[0]

    print("[OK] thresholds loaded:", {"FCT12": th_fct12, "FCT34": th_fct34})

    # (2) 현재 df(필터된 FCT 결과)에서 비교용 threshold 맵 생성
    # - FCT1/FCT2 => FCT12
    # - FCT3/FCT4 => FCT34
    def get_threshold(station: str):
        if station in ("FCT1", "FCT2"):
            return th_fct12
        if station in ("FCT3", "FCT4"):
            return th_fct34
        return np.nan

    df2 = df.copy()  # df는 직전 셀의 "필터+계산 결과" DataFrame
    df2["threshold_del_out_av"] = df2["station"].astype(str).map(get_threshold)

    # no_operation_time은 "30.00" 문자열일 수 있으니 numeric으로 변환
    df2["no_operation_time_num"] = pd.to_numeric(df2["no_operation_time"], errors="coerce")

    # (3) 비교: no_operation_time > del_out_av 이면 real_no_operation_time = 1
    # 조건상 TEST AUTO MODE START 행에만 no_operation_time이 들어가므로,
    # 숫자가 있는 행들만 비교되고 나머지는 자동 0 처리됩니다.
    df2["real_no_operation_time"] = np.where(
        (df2["no_operation_time_num"].notna()) &
        (df2["threshold_del_out_av"].notna()) &
        (df2["no_operation_time_num"] > df2["threshold_del_out_av"]),
        1, 0
    )

    # (4) 최종 출력 (요청 컬럼 + real_no_operation_time)
    out_cols2 = ["end_day", "station", "contents", "end_time", "no_operation_time", "real_no_operation_time"]

    print("\n========== OUTPUT + real_no_operation_time ==========")


    # ============================================
    # [FCT 전용] real_no_operation_time=1 -> from_time/to_time 생성 + 출력
    # 입력 DF: df2  (FCT용, real_no_operation_time 포함)
    # ============================================

    import numpy as np

    df_fct_evt = df2.copy()

    # 정렬 보강 (station, end_time 오름차순)
    df_fct_evt["end_time_str"] = df_fct_evt["end_time"].astype(str)
    df_fct_evt = df_fct_evt.sort_values(["station", "end_time_str"], ascending=[True, True]).reset_index(drop=True)

    # to_time: real_no_operation_time=1인 행의 end_time
    df_fct_evt["to_time"] = np.where(df_fct_evt["real_no_operation_time"] == 1, df_fct_evt["end_time_str"], np.nan)

    # from_time: real_no_operation_time=1인 행의 "바로 위 행" end_time (station별 shift)
    df_fct_evt["from_time"] = np.where(
        df_fct_evt["real_no_operation_time"] == 1,
        df_fct_evt.groupby("station")["end_time_str"].shift(1),
        np.nan
    )

    # 최종 출력 DF
    df_fct_out = df_fct_evt.loc[
        df_fct_evt["real_no_operation_time"] == 1,
        ["end_day", "station", "from_time", "to_time", "no_operation_time"]
    ].reset_index(drop=True)

    print("========== FCT OUTPUT (real_no_operation_time=1) ==========")


    import pandas as pd
    from sqlalchemy import text

    SAVE_SCHEMA = "g_production_film"
    SAVE_TABLE  = "fct_non_operation_time"

    # 0) 입력 DF 검증
    required_cols = ["end_day", "station", "from_time", "to_time", "no_operation_time"]
    missing = [c for c in required_cols if c not in df_fct_out.columns]
    if missing:
        raise ValueError(f"[ERROR] df_fct_out missing columns: {missing}")

    df_save = df_fct_out.copy()
    df_save["end_day"] = df_save["end_day"].astype(str)
    df_save["station"] = df_save["station"].astype(str)
    df_save["from_time"] = df_save["from_time"].astype(str)
    df_save["to_time"] = df_save["to_time"].astype(str)
    df_save["no_operation_time"] = pd.to_numeric(df_save["no_operation_time"], errors="coerce")

    # 1) 스키마/테이블 생성
    create_sql = text(f"""
    CREATE SCHEMA IF NOT EXISTS {SAVE_SCHEMA};

    CREATE TABLE IF NOT EXISTS {SAVE_SCHEMA}.{SAVE_TABLE} (
        end_day TEXT NOT NULL,
        station TEXT NOT NULL,
        from_time TEXT NOT NULL,
        to_time TEXT NOT NULL,
        no_operation_time NUMERIC(12,2),
        created_at TIMESTAMPTZ DEFAULT now(),
        PRIMARY KEY (end_day, station, from_time, to_time)
    );
    """)

    # 2) UPSERT
    upsert_sql = text(f"""
    INSERT INTO {SAVE_SCHEMA}.{SAVE_TABLE}
    (end_day, station, from_time, to_time, no_operation_time)
    VALUES (:end_day, :station, :from_time, :to_time, :no_operation_time)
    ON CONFLICT (end_day, station, from_time, to_time)
    DO UPDATE SET
        no_operation_time = EXCLUDED.no_operation_time;
    """)

    rows = df_save[required_cols].to_dict(orient="records")

    try:
        with engine.begin() as conn:
            conn.execute(create_sql)
            if rows:
                conn.execute(upsert_sql, rows)

            # 3) 검증: 테이블 존재 + 건수 + 샘플
            cnt = conn.execute(text(f"SELECT COUNT(*) FROM {SAVE_SCHEMA}.{SAVE_TABLE}")).scalar()
            sample = conn.execute(text(f"""
                SELECT end_day, station, from_time, to_time, no_operation_time
                FROM {SAVE_SCHEMA}.{SAVE_TABLE}
                ORDER BY created_at DESC
                LIMIT 20
            """)).fetchall()

        print(f"[OK] created/updated: {SAVE_SCHEMA}.{SAVE_TABLE}")
        print(f"[OK] inserted/upserted rows this run: {len(rows)}")
        print(f"[OK] total rows in table: {cnt}")
        print("[OK] latest 20 rows:")
        for r in sample:
            print(r)

    except Exception as e:
        print("[ERROR] FCT save failed:", repr(e))
        raise

if __name__ == "__main__":
    start = datetime.now()
    _print_run_banner(start)
    exit_code = 0
    try:
        main()
    except KeyboardInterrupt:
        print("[ABORT] 사용자 중단(CTRL+C)", flush=True)
        exit_code = 130
    except Exception as e:
        print(f"[ERROR] Unhandled exception: {repr(e)}", flush=True)
        exit_code = 1
    finally:
        end = datetime.now()
        _print_end_banner(start, end)
        # EXE로 실행 시 콘솔 강제 종료 방지
        if getattr(sys, "frozen", False):
            try:
                input("Press Enter to exit...")
            except Exception:
                pass
    sys.exit(exit_code)
