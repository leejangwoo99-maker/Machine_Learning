# ============================================
# 0. 라이브러리 import
# ============================================
import pandas as pd
from sqlalchemy import create_engine, text
import urllib.parse
from datetime import datetime
import sys

import plotly.express as px

# Jupyter / .py 둘 다에서 display() 쓰기 위한 처리
try:
    from IPython.display import display  # 노트북이면 이거 사용
except ImportError:  # .py 단독 실행이면 print로 대체
    def display(obj):
        print(obj)


# ============================================
# 1. DB 접속 (SQLAlchemy 엔진 방식)
# ============================================
user = "postgres"
password_raw = "leejangwoo1!"
host = "100.105.75.47"
port = 5432
dbname = "postgres"

password = urllib.parse.quote_plus(password_raw)
conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(conn_str)

VIEW_NAME = "a2_fct_vision_testlog_json_processing.vw_pass_fail_pd_nonpd_runtime"

# 결과 저장용 스키마 / 테이블 이름
SCHEMA_RESULT   = "e2_fct_vision_test_ct"
TABLE_UPPER     = "fct_upper_outlier"
TABLE_CT        = "fct_vision_test_ct"
TABLE_FAIL      = "fct_fail"
TABLE_PROCESSED = "fct_processed_file"   # ✅ 처리 완료 file_path 관리용

# 이번 실행의 처리 시간 (모든 테이블 공통)
processed_time = datetime.now()

print("=== [1] DB 엔진 생성 완료 ===")
print("Connection String:", conn_str)
print("processed_time:", processed_time)
print()


# ============================================
# 2. VIEW에서 아직 처리 안 한 file_path만 읽기
#    (처리 완료 file_path는 패스)
# ============================================
print("=== [2] VIEW에서 '미처리 file_path' 데이터 읽기 ===")

# 2-0. 스키마 및 처리완료 테이블 생성
with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RESULT}"))
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_PROCESSED} (
            file_path      TEXT PRIMARY KEY,
            processed_time TIMESTAMP
        )
    """))

# 2-1. 처리 완료된 file_path는 제외하고 VIEW 조회
query_all = text(f"""
    SELECT
        v.end_time,
        v.end_day,
        v.remark,
        v.barcode_information,
        v.station,
        v.run_time,
        v.result,
        v.file_path
    FROM {VIEW_NAME} AS v
    LEFT JOIN {SCHEMA_RESULT}.{TABLE_PROCESSED} AS p
        ON v.file_path = p.file_path
    WHERE p.file_path IS NULL      -- ✅ 아직 처리한 적 없는 file_path만
""")

with engine.connect() as conn:
    df_all = pd.read_sql(query_all, conn)

if df_all.empty:
    print("👉 새로 처리할 데이터(미처리 file_path)가 없습니다. 스크립트를 종료합니다.")
    sys.exit(0)

# 컬럼 순서: end_day 다음에 end_time 오도록 정리
desired_cols = [
    "end_day",
    "end_time",
    "remark",
    "barcode_information",
    "station",
    "run_time",
    "result",
    "file_path",
]
df_all = df_all[desired_cols]

print("이번에 새로 처리할 로우 수:", len(df_all))
print("컬럼 목록:", df_all.columns.tolist())
display(df_all.head())

# run_time 숫자형 정리
df_all["run_time"] = pd.to_numeric(df_all["run_time"], errors="coerce")
df_all = df_all.dropna(subset=["run_time"]).reset_index(drop=True)

print("\nrun_time 숫자형 변환 + NaN 제거 후 로우 수:", len(df_all))
display(df_all.head())
print()

# PASS / FAIL 분리
df_pass_raw = df_all[df_all["result"] != "FAIL"].copy().reset_index(drop=True)
df_fail_raw = df_all[df_all["result"] == "FAIL"].copy().reset_index(drop=True)

print("PASS 로우 수:", len(df_pass_raw))
print("FAIL 로우 수:", len(df_fail_raw))
print()


# ============================================
# 3. station + remark 별 run_time 평균(run_average) 계산 (참고용)
# ============================================
print("=== [3] station + remark 별 run_average 계산 (참고용) ===")

df_group = (
    df_pass_raw
    .groupby(["station", "remark"], as_index=False)["run_time"]
    .mean()
    .rename(columns={"run_time": "run_average"})
    .sort_values(["station", "remark"])
    .reset_index(drop=True)
)

display(df_group)
print()


# ============================================
# 4. station + remark 별 IQR 기반 lower_extreme / upper 계산
#    - Extreme Lower = Q1 - 3*IQR
#    - Upper         = Q3 + 1.5*IQR
# ============================================
print("=== [4] IQR 기반 이상치 경계 계산 (PASS 데이터 기준) ===")

df_pass = df_pass_raw.copy()

grouped = df_pass.groupby(["station", "remark"])["run_time"]
Q1 = grouped.transform(lambda x: x.quantile(0.25))
Q3 = grouped.transform(lambda x: x.quantile(0.75))
IQR = Q3 - Q1

df_pass["lower_extreme"] = Q1 - 3.0 * IQR
df_pass["upper"]         = Q3 + 1.5 * IQR

display(df_pass.head())
print()


# ============================================
# 5. 이상치 플래그 추가
# ============================================
print("=== [5] 이상치 플래그 추가 ===")

df_pass["is_outlier"] = (df_pass["run_time"] < df_pass["lower_extreme"]) | (
    df_pass["run_time"] > df_pass["upper"]
)

outlier_count = df_pass["is_outlier"].sum()
print(f"PASS 데이터 중 이상치 개수: {outlier_count} / 전체 {len(df_pass)}")
display(df_pass.head())
print()


# ============================================
# 6. Plotly boxplot 시각화 (FCT1~FCT4 한 번만)
# ============================================
print("=== [6] Plotly boxplot 시각화 (FCT1~FCT4만) ===")

# FCT1~FCT4 데이터만 사용
df_fct = df_pass[df_pass["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()

if df_fct.empty:
    print("FCT1~FCT4 데이터가 없습니다. Boxplot 생성 생략.")
else:
    df_fct["station_remark"] = df_fct["station"] + " / " + df_fct["remark"]

    fig = px.box(
        df_fct,
        x="station_remark",
        y="run_time",
        points="outliers",
        title="[FCT1~FCT4] PD / Non-PD CT 분포 (Boxplot, 지표 표시)",
    )

    # 그룹별 통계량 계산
    stats_box = (
        df_fct
        .groupby("station_remark")["run_time"]
        .describe(percentiles=[0.25, 0.5, 0.75])
        .reset_index()
        .rename(columns={
            "min": "min_val",
            "25%": "q1_val",
            "50%": "median_val",
            "75%": "q3_val",
            "max": "max_val",
        })
    )

    for _, row in stats_box.iterrows():
        x = row["station_remark"]

        fig.add_annotation(
            x=x, y=row["min_val"],
            text=f"min: {row['min_val']:.2f}",
            showarrow=False, yshift=-20, font=dict(size=10),
        )
        fig.add_annotation(
            x=x, y=row["q1_val"],
            text=f"q1: {row['q1_val']:.2f}",
            showarrow=False, yshift=-5, font=dict(size=10),
        )
        fig.add_annotation(
            x=x, y=row["median_val"],
            text=f"median: {row['median_val']:.2f}",
            showarrow=False, yshift=10, font=dict(size=10),
        )
        fig.add_annotation(
            x=x, y=row["q3_val"],
            text=f"q3: {row['q3_val']:.2f}",
            showarrow=False, yshift=25, font=dict(size=10),
        )
        fig.add_annotation(
            x=x, y=row["max_val"],
            text=f"max: {row['max_val']:.2f}",
            showarrow=False, yshift=40, font=dict(size=10),
        )

    fig.update_layout(
        xaxis_title="Station / Remark",
        yaxis_title="CT",
    )
    fig.show()


# ============================================
# 7. 이상치 제거 후 yyyymm + station + remark 별 final_runtime 계산
#    - end_day + end_time → end_ts
#    - 00:00:00 ~ 08:29:59 는 전날(prod_date - 1일)로 간주
# ============================================
print("=== [7] 이상치 제거 후 월별(생산일 기준) CT 집계 ===")

df_no_outlier = df_pass[~df_pass["is_outlier"]].copy().reset_index(drop=True)
print("이상치 제거 후 PASS 데이터 행 수:", len(df_no_outlier))
display(df_no_outlier.head())

# 문자열 결합용
df_no_outlier["end_day"]  = df_no_outlier["end_day"].astype(str)
df_no_outlier["end_time"] = df_no_outlier["end_time"].astype(str)

# 실제 타임스탬프 (YYYYMMDD HH:MM:SS)
df_no_outlier["end_ts"] = pd.to_datetime(
    df_no_outlier["end_day"] + " " + df_no_outlier["end_time"],
    errors="coerce"
)

# NaT 제거
df_no_outlier = df_no_outlier.dropna(subset=["end_ts"]).reset_index(drop=True)

# 기본 생산일 = end_ts의 날짜
df_no_outlier["prod_date"] = df_no_outlier["end_ts"].dt.date

# 00:00:00 ~ 08:29:59 구간이면 전날로 이동
hour = df_no_outlier["end_ts"].dt.hour
minute = df_no_outlier["end_ts"].dt.minute

mask_night_morning = (hour < 8) | ((hour == 8) & (minute < 30))

df_no_outlier.loc[mask_night_morning, "prod_date"] = (
    df_no_outlier.loc[mask_night_morning, "prod_date"]
    - pd.to_timedelta(1, unit="D")
)

# 최종 집계용 yyyymm (생산일 기준)
df_no_outlier["yyyymm"] = pd.to_datetime(df_no_outlier["prod_date"]).dt.strftime("%Y%m")

# yyyymm + station + remark 기준 평균 CT
df_final_ct = (
    df_no_outlier
    .groupby(["yyyymm", "station", "remark"], as_index=False)["run_time"]
    .mean()
    .rename(columns={"run_time": "final_runtime"})
    .sort_values(["yyyymm", "station", "remark"])
    .reset_index(drop=True)
)

display(df_final_ct)
print()


# ============================================
# 8. DB 저장용 데이터프레임 준비
#    - fct_upper_outlier : FCT1~4 upper 이상치 바코드 + file_path
#    - fct_fail          : FAIL 바코드 + file_path
#    - fct_vision_test_ct: yyyymm + station + remark 별 final_runtime
#    ※ PASS(이상치 제거) 바코드 리스트(df_pass_unique)는 파싱/저장 안 함
# ============================================
print("=== [8] DB 저장용 데이터프레임 준비 ===")

target_stations = ["FCT1", "FCT2", "FCT3", "FCT4"]

# 8-1. FCT1~4 upper 이상치 (PASS 데이터 기준)
df_upper_fct = df_pass[
    (df_pass["run_time"] > df_pass["upper"]) &
    (df_pass["station"].isin(target_stations))
].copy()

df_upper_unique = (
    df_upper_fct[["barcode_information", "file_path"]]
    .drop_duplicates()
    .reset_index(drop=True)
)

print("FCT1~4 upper 이상치 바코드 개수(중복 제거):", len(df_upper_unique))
display(df_upper_unique.head())
print()

# 8-2. FAIL 바코드
df_fail_unique = (
    df_fail_raw[["barcode_information", "file_path"]]
    .drop_duplicates()
    .reset_index(drop=True)
)
print("FAIL 바코드 개수(중복 제거):", len(df_fail_unique))
display(df_fail_unique.head())
print()

# 8-3. CT 결과 (여긴 file_path 필요 없음, yyyymm+station+remark 단위)
df_final_ct_db = df_final_ct.copy()


def chunk_records(records, chunk_size=5000):
    """리스트(또는 list(dict))를 chunk_size 단위로 잘라서 yield."""
    for i in range(0, len(records), chunk_size):
        yield records[i:i + chunk_size]


# ============================================
# 9. DB 스키마 및 테이블 생성 + 데이터 저장
#    - fct_pass 관련 테이블/INSERT 없음 (요청대로 제외)
#    - file_path + barcode_information 으로 중복 체크
#    - 처리 완료 file_path 기록
# ============================================
print("=== [9] DB 스키마 및 테이블 생성 + 데이터 저장 ===")

with engine.begin() as conn:
    # 9-1. 스키마 생성 (이미 [2]에서 한 번 했지만 idempotent)
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA_RESULT}"))

    # 9-2. 테이블 생성
    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_UPPER} (
            file_path           TEXT NOT NULL,
            barcode_information TEXT NOT NULL,
            PRIMARY KEY (file_path, barcode_information)
        )
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_FAIL} (
            file_path           TEXT NOT NULL,
            barcode_information TEXT NOT NULL,
            PRIMARY KEY (file_path, barcode_information)
        )
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_CT} (
            yyyymm        VARCHAR(6)  NOT NULL,
            station       VARCHAR(50) NOT NULL,
            remark        VARCHAR(20) NOT NULL,
            final_runtime NUMERIC,
            PRIMARY KEY (yyyymm, station, remark)
        )
    """))

    conn.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA_RESULT}.{TABLE_PROCESSED} (
            file_path      TEXT PRIMARY KEY,
            processed_time TIMESTAMP
        )
    """))

    # ---------- 데이터 INSERT (중복 방지, chunk 단위) ----------

    # fct_upper_outlier : FCT1~4 upper 이상치
    if not df_upper_unique.empty:
        recs = df_upper_unique.to_dict(orient="records")
        for idx, chunk in enumerate(chunk_records(recs, chunk_size=5000), start=1):
            conn.execute(
                text(f"""
                    INSERT INTO {SCHEMA_RESULT}.{TABLE_UPPER}
                        (file_path, barcode_information)
                    VALUES
                        (:file_path, :barcode_information)
                    ON CONFLICT (file_path, barcode_information) DO NOTHING
                """),
                chunk
            )
            print(f"  - fct_upper_outlier chunk {idx} 완료 ({len(chunk)} rows)")
        print("→ fct_upper_outlier 저장 완료")

    # fct_fail : FAIL 바코드
    if not df_fail_unique.empty:
        recs = df_fail_unique.to_dict(orient="records")
        for idx, chunk in enumerate(chunk_records(recs, chunk_size=5000), start=1):
            conn.execute(
                text(f"""
                    INSERT INTO {SCHEMA_RESULT}.{TABLE_FAIL}
                        (file_path, barcode_information)
                    VALUES
                        (:file_path, :barcode_information)
                    ON CONFLICT (file_path, barcode_information) DO NOTHING
                """),
                chunk
            )
            print(f"  - fct_fail chunk {idx} 완료 ({len(chunk)} rows)")
        print("→ fct_fail 저장 완료")

    # fct_vision_test_ct : yyyymm + station + remark 별 CT (UPSERT)
    if not df_final_ct_db.empty:
        recs = df_final_ct_db.to_dict(orient="records")
        for idx, chunk in enumerate(chunk_records(recs, chunk_size=5000), start=1):
            conn.execute(
                text(f"""
                    INSERT INTO {SCHEMA_RESULT}.{TABLE_CT}
                        (yyyymm, station, remark, final_runtime)
                    VALUES
                        (:yyyymm, :station, :remark, :final_runtime)
                    ON CONFLICT (yyyymm, station, remark)
                    DO UPDATE SET
                        final_runtime = EXCLUDED.final_runtime
                """),
                chunk
            )
            print(f"  - fct_vision_test_ct chunk {idx} 완료 ({len(chunk)} rows)")
        print("→ fct_vision_test_ct 저장/업데이트 완료")

    # 처리 완료 file_path 기록
    df_new_files = (
        df_all[["file_path"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )

    if not df_new_files.empty:
        recs = [
            {"file_path": fp, "processed_time": processed_time}
            for fp in df_new_files["file_path"]
        ]
        for idx, chunk in enumerate(chunk_records(recs, chunk_size=5000), start=1):
            conn.execute(
                text(f"""
                    INSERT INTO {SCHEMA_RESULT}.{TABLE_PROCESSED}
                        (file_path, processed_time)
                    VALUES (:file_path, :processed_time)
                    ON CONFLICT (file_path) DO UPDATE
                        SET processed_time = EXCLUDED.processed_time
                """),
                chunk
            )
            print(f"  - fct_processed_file chunk {idx} 완료 ({len(chunk)} rows)")
        print("→ fct_processed_file 처리완료 기록 저장 완료")

print("\n=== 전체 파이프라인 완료 ===")
