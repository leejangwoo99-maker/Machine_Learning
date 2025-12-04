# -*- coding: utf-8 -*-
"""
VISION CT 분석 스크립트 (백엔드용)

기능:
- VIEW에서 PASS 데이터 로드 (이미 처리한 file_path는 e2_fct_vision_test_ct.processing 기준으로 제외)
- IQR 기반 이상치 계산
- FCT1~4 월별 CT 통계 / UPPER 초과 / FAIL 정보 계산
- 결과를 PostgreSQL에 UPSERT
- 처리한 file_path를 processing 테이블에 UPSERT
- 위 전체 과정을 1초마다 무한루프로 반복 수행
- 월별(FCT1~4 × PD/Non-PD) Boxplot 통계 계산을 멀티프로세싱(2 프로세스)으로 수행
"""

import time
import urllib.parse
import pandas as pd
from sqlalchemy import create_engine, text

import plotly.express as px

from multiprocessing import Pool

# ============================================
# DB 접속 (SQLAlchemy 엔진)
# ============================================
user = "postgres"
password_raw = "leejangwoo1!"
host = "100.105.75.47"
port = 5432
dbname = "postgres"

# 비밀번호 URL 인코딩
password = urllib.parse.quote_plus(password_raw)

conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conn_str)

VIEW_NAME = "a2_fct_vision_testlog_json_processing.vw_pass_fail_pd_nonpd_runtime"
SCHEMA_SAVE = "e2_fct_vision_test_ct"

print("=== [1] DB 엔진 생성 완료 ===")
print("Connection String:", conn_str)
print()


# ============================================
# Boxplot annotation 함수
# ============================================
def add_box_stats_annotations(fig, stats_df, x_col):
    """
    fig      : plotly figure
    stats_df : groupby 결과 (x_col, min, q1, median, mean, q3, max)
    x_col    : x축 컬럼 이름 ('station_remark' 등)
    """
    for _, r in stats_df.iterrows():
        x_val = r[x_col]
        vmin = r["min"]
        q1 = r["q1"]
        med = r["median"]
        mean = r["mean"]
        q3 = r["q3"]
        vmax = r["max"]

        span = max(vmax - vmin, 1.0)
        dy = span * 0.03

        fig.add_annotation(
            x=x_val, y=vmax + dy,
            text=f"max: {vmax:.2f}",
            showarrow=False, font=dict(size=9)
        )
        fig.add_annotation(
            x=x_val, y=q3,
            text=f"q3: {q3:.2f}",
            showarrow=False, font=dict(size=9)
        )
        fig.add_annotation(
            x=x_val, y=med,
            text=f"median: {med:.2f}",
            showarrow=False, font=dict(size=9)
        )
        fig.add_annotation(
            x=x_val, y=mean,
            text=f"mean: {mean:.2f}",
            showarrow=False, font=dict(size=9)
        )
        fig.add_annotation(
            x=x_val, y=q1,
            text=f"q1: {q1:.2f}",
            showarrow=False, font=dict(size=9)
        )
        fig.add_annotation(
            x=x_val, y=vmin - dy,
            text=f"min: {vmin:.2f}",
            showarrow=False, font=dict(size=9)
        )


# ============================================
# 멀티프로세싱: 월별 Boxplot 처리 함수
#  - args: (yyyymm, df_m)
# ============================================
def process_month_boxplot(args):
    ym, df_m = args

    if df_m.empty:
        return

    print(f"\n=== [6-1] {ym} 월 FCT1~4 × PD/Non-PD CT 분포 Boxplot (멀티프로세싱) ===")
    print(f"행 개수: {len(df_m)}")

    # x축 라벨: FCT1 / PD 형태
    df_m = df_m.copy()
    df_m["station_remark"] = df_m["station"].astype(str) + " / " + df_m["remark"].astype(str)

    # Boxplot 생성 (백엔드에서는 fig.show() 호출 안 함)
    fig_m = px.box(
        df_m,
        x="station_remark",
        y="run_time",
        points=False,
        title=f"[{ym}] FCT1~4 × PD/Non-PD CT 분포 (Boxplot)",
    )
    fig_m.update_layout(
        xaxis_title="Station / Remark",
        yaxis_title="CT",
    )

    # 통계값 계산
    stats_m = (
        df_m
        .groupby("station_remark")["run_time"]
        .agg(
            min="min",
            q1=lambda s: s.quantile(0.25),
            median="median",
            mean="mean",
            q3=lambda s: s.quantile(0.75),
            max="max",
        )
        .reset_index()
    )

    # 표시용 DataFrame
    stats_m_display = stats_m.copy()
    stats_m_display["yyyymm"] = ym
    stats_m_display[["station", "remark"]] = \
        stats_m_display["station_remark"].str.split(" / ", expand=True)

    stats_m_display[["min", "q1", "median", "mean", "q3", "max"]] = \
        stats_m_display[["min", "q1", "median", "mean", "q3", "max"]].round(2)

    stats_m_display = stats_m_display[
        ["yyyymm", "station", "remark", "min", "q1", "median", "mean", "q3", "max"]
    ]

    print(f"[{ym}] 통계값 (표시용)")
    print(stats_m_display.to_string(index=False))

    # y축 범위 설정
    gmin = stats_m["min"].min()
    gmax = stats_m["max"].max()
    span = max(gmax - gmin, 1.0)
    pad = span * 0.2
    fig_m.update_yaxes(range=[gmin - pad, gmax + pad])

    # Boxplot에 대표값 annotation 추가
    add_box_stats_annotations(fig_m, stats_m, x_col="station_remark")

    # 필요하면 여기서 fig_m.write_html(...) 등으로 파일로 저장 가능
    # 예: fig_m.write_html(f"boxplot_{ym}.html")


# ============================================
# 메인 1회 실행 함수 (이걸 1초마다 반복)
# ============================================
def process_once():
    print("\n\n================= [NEW LOOP RUN] =================")
    print("=== [2] VIEW에서 데이터 읽기 (FAIL 제외 + 이력 제외) ===")

    # processing 테이블 고려한 쿼리
    query_with_join = text(f"""
        SELECT
            v.end_day,
            v.end_time,
            v.remark,
            v.barcode_information,
            v.station,
            v.run_time,
            v.result,
            v.file_path
        FROM {VIEW_NAME} AS v
        LEFT JOIN {SCHEMA_SAVE}.processing AS p
            ON v.file_path = p.file_path
        WHERE v.result <> 'FAIL'
          AND p.file_path IS NULL
    """)

    # processing 테이블이 없을 때 fallback
    query_without_join = text(f"""
        SELECT
            end_day,
            end_time,
            remark,
            barcode_information,
            station,
            run_time,
            result,
            file_path
        FROM {VIEW_NAME}
        WHERE result <> 'FAIL'
    """)

    try:
        with engine.connect() as conn:
            df_raw = pd.read_sql(query_with_join, conn)
        print("processing 이력과 JOIN하여 신규 데이터만 로드했습니다.")
    except Exception as e:
        print("processing JOIN 중 오류 → 이력 무시하고 전체 로드:", e)
        with engine.connect() as conn:
            df_raw = pd.read_sql(query_without_join, conn)

    print("로드된 로우 수:", len(df_raw))

    # run_time 숫자형 변환
    df_raw["run_time"] = pd.to_numeric(df_raw["run_time"], errors="coerce")
    df_raw = df_raw.dropna(subset=["run_time"]).reset_index(drop=True)
    df_raw["file_path"] = df_raw["file_path"].astype(str)

    print("\nrun_time 숫자형 변환 + NaN 제거 후 로우 수:", len(df_raw))
    if not df_raw.empty:
        print(df_raw.head().to_string(index=False))
    else:
        print("데이터가 없습니다. 이후 단계는 스킵합니다.")
        return  # 더 이상 할 작업 없음

    # ============================================
    # 3. station + remark 별 평균 run_time
    # ============================================
    print("\n=== [3] station + remark 별 run_time 평균(run_average) 계산 ===")
    df_group = (
        df_raw
        .groupby(["station", "remark"], as_index=False)["run_time"]
        .mean()
        .rename(columns={"run_time": "run_average"})
        .sort_values(["station", "remark"])
        .reset_index(drop=True)
    )
    print("[station, remark 별 평균 run_time]")
    print(df_group.to_string(index=False))

    # ============================================
    # 4. IQR 기반 lower_extreme / upper 계산
    # ============================================
    print("\n=== [4] station + remark 별 IQR 기반 lower_extreme / upper 계산 ===")
    df = df_raw.copy()
    grouped = df.groupby(["station", "remark"])["run_time"]

    Q1 = grouped.transform(lambda x: x.quantile(0.25))
    Q3 = grouped.transform(lambda x: x.quantile(0.75))
    IQR = Q3 - Q1

    df["lower_extreme"] = Q1 - 3.0 * IQR
    df["upper"] = Q3 + 1.5 * IQR

    print("IQR 기반 경계값 컬럼 추가 후 데이터 예시:")
    print(df.head().to_string(index=False))

    # ============================================
    # 5. 이상치 플래그 추가
    # ============================================
    print("\n=== [5] 이상치 플래그(is_outlier) 추가 ===")
    df["is_outlier"] = (df["run_time"] < df["lower_extreme"]) | (df["run_time"] > df["upper"])
    outlier_count = df["is_outlier"].sum()
    print(f"이상치 개수: {outlier_count} / 전체 {len(df)}")
    print("이상치 여부 컬럼 포함 예시:")
    print(df.head().to_string(index=False))

    # ============================================
    # 6. 이상치만 추출 (df_outlier)
    # ============================================
    print("\n=== [6] 이상치만 추출 (df_outlier) ===")
    df_outlier = df[df["is_outlier"]].copy()
    df_outlier["run_time_outlier"] = df_outlier["run_time"]
    df_outlier = (
        df_outlier[
            [
                "end_day",
                "end_time",
                "remark",
                "barcode_information",
                "station",
                "run_time_outlier",
                "result",
                "file_path",
            ]
        ]
        .sort_values(["remark", "station", "end_day", "end_time"])
        .reset_index(drop=True)
    )
    print("이상치 데이터프레임(df_outlier) 행 수:", len(df_outlier))
    if not df_outlier.empty:
        print(df_outlier.head().to_string(index=False))

    # ============================================
    # 6-1. 월별(FCT1~4 × PD/Non-PD) Boxplot – 멀티프로세싱 사용
    # ============================================
    print("\n=== [6-1] 월별(FCT1~4 × PD/Non-PD) CT 분포 Boxplot (멀티프로세싱) ===")

    df_box = df[df["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()
    df_box["end_day_dt"] = pd.to_datetime(df_box["end_day"].astype(str), errors="coerce")
    df_box["end_ts"] = pd.to_datetime(
        df_box["end_day_dt"].dt.strftime("%Y-%m-%d") + " " + df_box["end_time"].astype(str),
        errors="coerce"
    )

    # 야간 기준일 처리 (08:30 이전 → 전날 야간)
    mask_prev = (
        (df_box["end_ts"].dt.hour < 8) |
        ((df_box["end_ts"].dt.hour == 8) & (df_box["end_ts"].dt.minute < 30))
    )
    df_box.loc[mask_prev, "prod_ts"] = df_box["end_ts"] - pd.Timedelta(days=1)
    df_box.loc[~mask_prev, "prod_ts"] = df_box["end_ts"]

    df_box["yyyymm"] = df_box["prod_ts"].dt.strftime("%Y%m")

    print("\n[Boxplot 베이스 데이터 예시 (FCT1~4만)]")
    print(df_box[["station", "remark", "end_day", "end_time", "prod_ts", "yyyymm", "run_time"]]
          .head(10).to_string(index=False))

    # 실제 존재하는 월
    available_months = (
        df_box["yyyymm"]
        .dropna()
        .sort_values()
        .unique()
    )
    print("\n[Boxplot을 그릴 대상 월 목록]", available_months)

    # 멀티프로세싱으로 각 월 처리
    tasks = []
    for ym in available_months:
        df_m = df_box[df_box["yyyymm"] == ym].copy()
        if not df_m.empty:
            tasks.append((ym, df_m))

    if tasks:
        with Pool(processes=2) as pool:
            pool.map(process_month_boxplot, tasks)
    else:
        print("Boxplot 대상 월 데이터가 없습니다.")

    # ============================================
    # 7. FCT1~4 기준 yyyymm CT 계산 + Boxplot (년월만 구분)
    # ============================================
    print("\n=== [7] 이상치 제거 후 FCT1~4 월별 CT(final_runtime) 계산 (yyyymm 기준) ===")

    df_fct = df[~df["is_outlier"]].copy()
    df_fct = df_fct[df_fct["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()
    df_fct = df_fct.reset_index(drop=True)

    print("이상치 제거 후 FCT1~4 데이터 행 수:", len(df_fct))
    print(df_fct.head().to_string(index=False))

    df_fct["end_day_dt"] = pd.to_datetime(df_fct["end_day"].astype(str), errors="coerce")
    df_fct["end_ts"] = pd.to_datetime(
        df_fct["end_day_dt"].dt.strftime("%Y-%m-%d") + " " + df_fct["end_time"].astype(str),
        errors="coerce"
    )

    mask_prev_day = (
        (df_fct["end_ts"].dt.hour < 8) |
        ((df_fct["end_ts"].dt.hour == 8) & (df_fct["end_ts"].dt.minute < 30))
    )
    df_fct.loc[mask_prev_day, "prod_ts"] = df_fct["end_ts"] - pd.Timedelta(days=1)
    df_fct.loc[~mask_prev_day, "prod_ts"] = df_fct["end_ts"]

    df_fct["yyyymm"] = df_fct["prod_ts"].dt.strftime("%Y%m")

    print("\n[기준일/yyyymm 매핑 예시 (FCT1~4만)]")
    print(df_fct[["station", "end_day", "end_time", "end_ts", "prod_ts", "yyyymm"]]
          .head(20).to_string(index=False))

    df_final = (
        df_fct
        .groupby(["yyyymm", "station", "remark"], as_index=False)["run_time"]
        .mean()
        .rename(columns={"run_time": "final_runtime"})
        .sort_values(["yyyymm", "station", "remark"])
        .reset_index(drop=True)
    )
    df_final["final_runtime"] = df_final["final_runtime"].round(2)
    df_final = df_final[["yyyymm", "station", "remark", "final_runtime"]]

    print("\n[yyyymm, station, remark 별 최종 final_runtime (FCT1~4)]")
    print(df_final.to_string(index=False))

    df_month_stats = (
        df_fct
        .groupby("yyyymm")["run_time"]
        .agg(
            count="count",
            min="min",
            q1=lambda s: s.quantile(0.25),
            median="median",
            mean="mean",
            q3=lambda s: s.quantile(0.75),
            max="max",
        )
        .reset_index()
    )
    df_month_stats[["min", "q1", "median", "mean", "q3", "max"]] = \
        df_month_stats[["min", "q1", "median", "mean", "q3", "max"]].round(2)

    print("\n[월별 CT 요약 통계 (FCT1~4 전체 합산)]")
    print(df_month_stats.to_string(index=False))

    # ============================================
    # 8. FCT1~4 상한초과(UPPER) 데이터
    # ============================================
    print("\n=== [8] FCT1~4 상한초과(UPPER) 데이터 ===")
    target_stations = ["FCT1", "FCT2", "FCT3", "FCT4"]
    df_upper = df[
        (df["run_time"] > df["upper"]) &
        (df["station"].isin(target_stations))
    ].copy()

    print(f"FCT1~4 upper 초과 로우 수: {len(df_upper)}")

    df_upper_view = df_upper[
        [
            "end_day",
            "end_time",
            "remark",
            "station",
            "run_time",
            "upper",
            "barcode_information",
            "file_path",
            "result"
        ]
    ].sort_values(["station", "end_day", "end_time"]).reset_index(drop=True)

    if not df_upper_view.empty:
        print(df_upper_view.head().to_string(index=False))

    # ============================================
    # 8-2. FAIL 바코드 + file_path 조회
    # ============================================
    print("\n=== [8-2] FAIL 바코드 + file_path 조회 ===")

    query_fail = text(f"""
        SELECT
            end_day,
            end_time,
            remark,
            barcode_information,
            station,
            run_time,
            result,
            file_path
        FROM {VIEW_NAME}
        WHERE result = 'FAIL'
    """)

    with engine.connect() as conn:
        df_fail = pd.read_sql(query_fail, conn)

    print("FAIL 로우 수:", len(df_fail))

    df_fail_view = df_fail[
        [
            "end_day",
            "end_time",
            "remark",
            "station",
            "run_time",
            "barcode_information",
            "file_path",
            "result",
        ]
    ].sort_values(["station", "end_day", "end_time"]).reset_index(drop=True)

    if not df_fail_view.empty:
        print(df_fail_view.head().to_string(index=False))

    # ============================================
    # 9. DataFrame들을 PostgreSQL에 저장 (UPSERT)
    # ============================================
    print("\n=== [9] DataFrame -> PostgreSQL 저장 (UPSERT) 시작 ===")

    with engine.begin() as conn:

        # 9-1) month_final_ct
        df_final.to_sql(
            name="month_final_ct_tmp",
            con=conn,
            schema=SCHEMA_SAVE,
            if_exists="replace",
            index=False,
        )

        merge_month_sql = text(f"""
            INSERT INTO {SCHEMA_SAVE}.month_final_ct AS dst
                (yyyymm, station, remark, final_runtime)
            SELECT
                yyyymm, station, remark, final_runtime
            FROM {SCHEMA_SAVE}.month_final_ct_tmp
            ON CONFLICT (yyyymm, station, remark)
            DO UPDATE SET
                final_runtime = EXCLUDED.final_runtime;
        """)
        conn.execute(merge_month_sql)
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA_SAVE}.month_final_ct_tmp;"))
        print(f"month_final_ct UPSERT 완료 (행 수: {len(df_final)})")

        # 9-2) fct_test_ct_upper_outlier
        df_upper_view.to_sql(
            name="fct_test_ct_upper_outlier_tmp",
            con=conn,
            schema=SCHEMA_SAVE,
            if_exists="replace",
            index=False,
        )

        merge_upper_sql = text(f"""
            INSERT INTO {SCHEMA_SAVE}.fct_test_ct_upper_outlier AS dst
                (end_day, end_time, remark, station,
                 run_time, upper, barcode_information,
                 file_path, result)
            SELECT
                end_day, end_time, remark, station,
                run_time, upper, barcode_information,
                file_path, result
            FROM {SCHEMA_SAVE}.fct_test_ct_upper_outlier_tmp
            ON CONFLICT (file_path, barcode_information, station, end_day, end_time)
            DO NOTHING;
        """)
        conn.execute(merge_upper_sql)
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA_SAVE}.fct_test_ct_upper_outlier_tmp;"))
        print(f"fct_test_ct_upper_outlier UPSERT(중복 무시) 완료 (행 수: {len(df_upper_view)})")

        # 9-3) fct_test_fail_info
        df_fail_view.to_sql(
            name="fct_test_fail_info_tmp",
            con=conn,
            schema=SCHEMA_SAVE,
            if_exists="replace",
            index=False,
        )

        merge_fail_sql = text(f"""
            INSERT INTO {SCHEMA_SAVE}.fct_test_fail_info AS dst
                (end_day, end_time, remark, barcode_information,
                 station, run_time, file_path, result)
            SELECT
                end_day, end_time, remark, barcode_information,
                station, run_time, file_path, result
            FROM {SCHEMA_SAVE}.fct_test_fail_info_tmp
            ON CONFLICT (file_path, barcode_information, station, end_day, end_time)
            DO NOTHING;
        """)
        conn.execute(merge_fail_sql)
        conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA_SAVE}.fct_test_fail_info_tmp;"))
        print(f"fct_test_fail_info UPSERT(중복 무시) 완료 (행 수: {len(df_fail_view)})")

    print("\n=== [9] PostgreSQL 저장 (UPSERT) 전체 완료 ===")

    # ============================================
    # 8-x. 처리 완료 file_path 이력 저장 (UPSERT)
    # ============================================
    print("\n=== [8-x] 처리 완료 file_path 이력 저장 (UPSERT) ===")

    file_paths = sorted(df_raw["file_path"].astype(str).unique())
    print(f"이번 실행에서 처리한 file_path 개수: {len(file_paths)}")

    if not file_paths:
        print("신규 file_path가 없어 이력 UPSERT는 건너뜁니다.")
    else:
        upsert_sql = text(f"""
            INSERT INTO {SCHEMA_SAVE}.processing (file_path)
            VALUES (:file_path)
            ON CONFLICT (file_path)
            DO NOTHING;
        """)

        with engine.begin() as conn:
            for fp in file_paths:
                conn.execute(upsert_sql, {"file_path": fp})

        print("processing 테이블에 file_path 이력이 UPSERT(INSERT/IGNORE) 되었습니다.")


# ============================================
#  메인: 1초마다 무한 루프 실행
# ============================================
if __name__ == "__main__":
    while True:
        loop_start = time.time()
        try:
            process_once()
        except Exception as e:
            print(f"[ERROR] 루프 실행 중 예외 발생: {repr(e)}")
        loop_end = time.time()
        print(f"[INFO] 이번 루프 소요 시간: {loop_end - loop_start:.2f} 초")

        # 1초마다 반복 (처리가 1초 이상 걸리면, 사실상 쉬는 시간은 줄어듦)
        time.sleep(1)
