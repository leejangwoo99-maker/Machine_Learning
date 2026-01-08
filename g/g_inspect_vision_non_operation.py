# -*- coding: utf-8 -*-
"""Auto-converted from inspect_vision_non_operation.ipynb

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
        "password": "leejangwoo1!",
    }

    engine = create_engine(
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )

    SCHEMA = "d1_machine_log"
    TARGET_END_DAY = "20260107"  # end_day가 VARCHAR이므로 문자열로 고정

    print("[OK] engine ready")


    def load_vision_table(table_name: str) -> pd.DataFrame:
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

        if not df.empty:
            df["no_operation_time"] = np.nan

        print(f"[LOAD] {table_name}: rows={len(df)}")
        return df


    vision_tables = [f"Vision{i}_machine_log" for i in range(1, 3)]
    df_vision_raw = pd.concat([load_vision_table(t) for t in vision_tables], ignore_index=True)

    print("[OK] total rows:", len(df_vision_raw))


    # a / b 조건
    A_PREFIXES = ("검사 양품 신호 출력", "검사 불량 신호 출력")
    B_PREFIX = "바코드 스캔 신호 수신"

    # 출력에 남길 contents 3종만 필터 (startswith 기준)
    VALID_PREFIXES = A_PREFIXES + (B_PREFIX,)

    df = df_vision_raw[df_vision_raw["contents"].astype(str).str.startswith(VALID_PREFIXES)].copy()

    if df.empty:
        print("[WARN] 필터 후 데이터가 0건입니다. (end_day 또는 contents 문구를 확인)")
    else:
        # station + end_time 오름차순 정렬 보강
        df = df.sort_values(["station", "end_time"], ascending=[True, True]).reset_index(drop=True)

        # timestamp 파싱 함수 (소수점 유무 모두 대응)
        def parse_ts(day_str: str, time_str: str) -> pd.Timestamp:
            day_str = str(day_str)
            time_str = str(time_str)

            ts = pd.to_datetime(f"{day_str} {time_str}", format="%Y%m%d %H:%M:%S.%f", errors="coerce")
            if pd.isna(ts):
                ts = pd.to_datetime(f"{day_str} {time_str}", format="%Y%m%d %H:%M:%S", errors="coerce")
            return ts

        df["_ts"] = [parse_ts(d, t) for d, t in zip(df["end_day"], df["end_time"])]

        # station별 스캔하며 a->b 매칭
        for station, idxs in df.groupby("station").groups.items():
            last_a_idx = None

            for i in idxs:
                c = str(df.at[i, "contents"])

                # a) 검사 양품/불량 신호 출력
                if c.startswith(A_PREFIXES):
                    last_a_idx = i
                    continue

                # b) 바코드 스캔 신호 수신
                if c.startswith(B_PREFIX):
                    if last_a_idx is not None:
                        t_a = df.at[last_a_idx, "_ts"]
                        t_b = df.at[i, "_ts"]

                        # a -> b 순서 & 파싱 성공 시에만
                        if pd.notna(t_a) and pd.notna(t_b) and (t_b >= t_a):
                            diff_sec = (t_b - t_a).total_seconds()
                            df.at[i, "no_operation_time"] = round(float(diff_sec), 2)

                    # 1페어 처리 후 리셋
                    last_a_idx = None

        df.drop(columns=["_ts"], inplace=True)

    # 최종 출력
    out_cols = ["end_day", "station", "contents", "end_time", "no_operation_time"]

    print("\n========== VISION FILTERED OUTPUT ==========")
    print("rows:", len(df))


    # ============================================
    # Cell 추가) Vision op_ct_gap(del_out_av) 기준 비교 -> real_no_operation_time 생성
    # ============================================

    from sqlalchemy import text
    import numpy as np
    import pandas as pd

    # (1) op_ct_gap에서 Vision1/Vision2 del_out_av 로드
    sql_gap = text("""
        SELECT station, del_out_av
        FROM g_production_film.op_ct_gap
        WHERE station IN ('Vision1', 'Vision2')
    """)

    with engine.connect() as conn:
        df_gap_v = pd.read_sql(sql_gap, conn)

    if df_gap_v.empty:
        raise RuntimeError("[ERROR] g_production_film.op_ct_gap 에서 station IN ('Vision1','Vision2') 데이터를 찾지 못했습니다.")

    df_gap_v["del_out_av"] = pd.to_numeric(df_gap_v["del_out_av"], errors="coerce")

    # station별 threshold 딕셔너리
    th_map = dict(zip(df_gap_v["station"].astype(str), df_gap_v["del_out_av"]))
    print("[OK] thresholds:", th_map)

    # (2) 현재 df(비전 필터+gap 결과)에 threshold 매핑
    df2 = df.copy()  # df는 직전 셀 결과 (Vision filtered output)

    df2["threshold_del_out_av"] = df2["station"].astype(str).map(th_map)

    # no_operation_time은 문자열("30.00")일 수 있으므로 numeric 변환
    df2["no_operation_time_num"] = pd.to_numeric(df2["no_operation_time"], errors="coerce")

    # (3) 비교: no_operation_time > del_out_av 이면 real_no_operation_time = 1
    df2["real_no_operation_time"] = np.where(
        (df2["no_operation_time_num"].notna()) &
        (df2["threshold_del_out_av"].notna()) &
        (df2["no_operation_time_num"] > df2["threshold_del_out_av"]),
        1, 0
    )

    # (4) 최종 출력
    out_cols2 = ["end_day", "station", "contents", "end_time", "no_operation_time", "real_no_operation_time"]

    print("\n========== VISION OUTPUT + real_no_operation_time ==========")


    # ============================================
    # [Vision 전용] real_no_operation_time=1 -> from_time/to_time 생성 + 출력
    # 입력 DF: df2  (Vision용, real_no_operation_time 포함)
    # ============================================

    import numpy as np

    df_vis_evt = df2.copy()

    # 정렬 보강 (station, end_time 오름차순)
    df_vis_evt["end_time_str"] = df_vis_evt["end_time"].astype(str)
    df_vis_evt = df_vis_evt.sort_values(["station", "end_time_str"], ascending=[True, True]).reset_index(drop=True)

    # to_time: real_no_operation_time=1인 행의 end_time
    df_vis_evt["to_time"] = np.where(df_vis_evt["real_no_operation_time"] == 1, df_vis_evt["end_time_str"], np.nan)

    # from_time: real_no_operation_time=1인 행의 "바로 위 행" end_time (station별 shift)
    df_vis_evt["from_time"] = np.where(
        df_vis_evt["real_no_operation_time"] == 1,
        df_vis_evt.groupby("station")["end_time_str"].shift(1),
        np.nan
    )

    # 최종 출력 DF
    df_vis_out = df_vis_evt.loc[
        df_vis_evt["real_no_operation_time"] == 1,
        ["end_day", "station", "from_time", "to_time", "no_operation_time"]
    ].reset_index(drop=True)

    print("========== VISION OUTPUT (real_no_operation_time=1) ==========")


    import pandas as pd
    from sqlalchemy import text

    SAVE_SCHEMA = "g_production_film"
    SAVE_TABLE  = "vision_non_operation_time"

    # 0) 입력 DF 검증
    required_cols = ["end_day", "station", "from_time", "to_time", "no_operation_time"]
    missing = [c for c in required_cols if c not in df_vis_out.columns]
    if missing:
        raise ValueError(f"[ERROR] df_vis_out missing columns: {missing}")

    df_save = df_vis_out.copy()
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
        print("[ERROR] Vision save failed:", repr(e))
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
