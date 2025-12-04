from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from datetime import timedelta, time, datetime
import time as time_mod
from concurrent.futures import ProcessPoolExecutor
import os
import webbrowser

import plotly.express as px
import plotly.io as pio


# ===========================
# 전역 설정
# ===========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# # PostgreSQL 접속 정보
# DB_CONFIG = {
#     "host": "192.168.108.162",
#     "port": 5432,
#     "dbname": "postgres",
#     "user": "postgres",
#     "password": "leejangwoo1!",
# }

# 부트스트랩 반복 횟수
N_BOOTSTRAP = 10

# 신뢰구간 (%). 95.0 → 95% CI, 99.0 → 99% CI
CI_LEVEL = 99.0  # ★ 여기만 바꾸면 전체 CI 변경

# 부트스트랩 멀티프로세싱 워커 수
N_BOOTSTRAP_WORKERS = 2

# 그래프 HTML 저장 폴더 / 파일
PLOT_DIR = "./plots"
PLOT_FILE = "vision_dashboard.html"

# 루프 주기 (초)
LOOP_INTERVAL_SEC = 3.5

# 이전 결과 캐시: 전체 스냅샷 { (month, remark): ct }
LAST_RESULT = {}

# 브라우저 최초 오픈 여부
FIRST_OPEN = False

# ===========================
# 공통 유틸
# ===========================
def get_engine():
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)


def ensure_plot_dir():
    if not os.path.exists(PLOT_DIR):
        os.makedirs(PLOT_DIR, exist_ok=True)


# ===========================
# 1. VIEW에서 데이터 로드
# ===========================
def load_data(engine: create_engine) -> pd.DataFrame:
    query = """
        SELECT
            id,
            station,
            remark,
            barcode_information,
            end_day,
            end_time,
            ct,
            file_path
        FROM a2_fct_vision_testlog_json_processing.v_vision_pass_ct
        WHERE end_day = CURRENT_DATE          -- ★ 오늘 날짜(예: 2025-12-04)만
        ORDER BY station, end_day, end_time;
    """
    df = pd.read_sql_query(query, engine)
    print("\n[STEP 1] RAW DATA (앞 5행)")
    print(df.head().to_string(index=False))
    return df

# ===========================
# 1-1. 기존 clean 데이터 로드 (★ 이번 달 end_day만)
#      & 신규 clean 저장용 테이블 생성
# ===========================
def load_processed_clean(engine: create_engine) -> pd.DataFrame:
    today = datetime.now().date()
    start_month = today.replace(day=1)

    # 다음 달 1일 계산
    if start_month.month == 12:
        next_month_first = start_month.replace(year=start_month.year + 1, month=1, day=1)
    else:
        next_month_first = start_month.replace(month=start_month.month + 1, day=1)

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS e1_whole_ct;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS e1_whole_ct.processing (
                id                  bigint,
                station             varchar(50),
                remark              varchar(20),
                barcode_information text,
                ct                  numeric,
                end_day             date,
                end_time            time,
                shift               varchar(10),
                shift_date          date,
                file_path           text,
                PRIMARY KEY (station, remark, file_path)
            );
        """))

        df_old = pd.read_sql_query(
            """
            SELECT
                id, station, remark, barcode_information, ct,
                end_day, end_time, shift, shift_date, file_path
            FROM e1_whole_ct.processing
            WHERE end_day >= %(start_month)s
              AND end_day <  %(next_month_first)s
            """,
            conn,
            params={
                "start_month": start_month,
                "next_month_first": next_month_first,
            },
        )

    return df_old

def save_new_clean(engine, df: pd.DataFrame):
    print("[DEBUG] save_new_clean() 호출됨 — row 수:", len(df))

    if df.empty:
        print("[DEBUG] df 비어있음 — INSERT 스킵")
        return

    required_cols = [
        "station", "remark", "barcode_information", "ct",
        "end_day", "end_time", "shift", "shift_date", "file_path"
    ]

    missing = [c for c in required_cols if c not in df.columns]
    print("[DEBUG] 누락된 컬럼:", missing)

    if missing:
        print("[ERROR] INSERT 실패 — 컬럼이 부족함")
        return

    try:
        with engine.begin() as conn:
            df[required_cols].to_sql(
                "processing",
                con=conn,
                schema="e1_whole_ct",
                if_exists="append",
                index=False,
                method="multi",
            )
        print("[DEBUG] INSERT 성공!")
    except Exception as e:
        print("[ERROR] INSERT 중 오류:", e)


# ===========================
# 2. 주/야간 + shift_date 계산
# ===========================
def add_shift_info(df: pd.DataFrame) -> pd.DataFrame:
    df2 = df.copy()

    df2["end_day"] = df2["end_day"].astype(str).str.strip()
    df2["end_time"] = df2["end_time"].astype(str).str.strip()
    df2["end_dt"] = pd.to_datetime(
        df2["end_day"] + " " + df2["end_time"],
        format="%Y-%m-%d %H:%M:%S"
    )

    df2["shift"] = "Day"
    df2["shift_date"] = df2["end_dt"].dt.date

    t = df2["end_dt"].dt.time

    t_day_start = time(8, 30, 0)
    t_day_end = time(20, 29, 59)
    t_night_start = time(20, 30, 0)
    t_night_morning_end = time(8, 29, 59)

    # 주간
    cond_day = (t >= t_day_start) & (t <= t_day_end)
    df2.loc[cond_day, "shift"] = "Day"
    df2.loc[cond_day, "shift_date"] = df2.loc[cond_day, "end_dt"].dt.date

    # 야간 (저녁)
    cond_night_evening = (t >= t_night_start)
    df2.loc[cond_night_evening, "shift"] = "Night"
    df2.loc[cond_night_evening, "shift_date"] = df2.loc[cond_night_evening, "end_dt"].dt.date

    # 야간 (새벽) → 전날 야간
    cond_night_morning = (t <= t_night_morning_end)
    df2.loc[cond_night_morning, "shift"] = "Night"
    df2.loc[cond_night_morning, "shift_date"] = (
        df2.loc[cond_night_morning, "end_dt"] - timedelta(days=1)
    ).dt.date

    print("\n[STEP 2] SHIFT INFO (앞 10행)")
    print(df2[["id", "station", "remark", "end_day", "end_time", "shift", "shift_date"]]
          .head(10).to_string(index=False))
    return df2


# ===========================
# 3. IQR 이상치 제거
# ===========================
def detect_outliers_iqr(series: pd.Series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (series < lower) | (series > upper)
    return mask, lower, upper


def remove_outliers(df: pd.DataFrame):
    """
    IQR 기반 이상치 제거.
    (이미 처리된 file_path 필터링은 run_one_cycle에서 처리하므로
     여기서는 온전히 들어온 df에 대해서만 이상치 계산)
    """
    df_ct = df.dropna(subset=["ct"]).copy()

    # file_path 기준 중복 제거 (station, remark, file_path 조합별 마지막 행만)
    if "file_path" in df_ct.columns:
        df_ct = (
            df_ct
            .sort_values(["station", "remark", "end_day", "end_time"])
            .drop_duplicates(subset=["station", "remark", "file_path"], keep="last")
        )

    outlier_rows = []
    clean_rows = []

    for (station, remark), grp in df_ct.groupby(["station", "remark"]):
        mask, lower, upper = detect_outliers_iqr(grp["ct"])

        grp_out = grp[mask].copy()
        if not grp_out.empty:
            grp_out["Outliers"] = grp_out["ct"]
            outlier_rows.append(
                grp_out[[
                    "id", "station", "remark", "barcode_information", "ct",
                    "Outliers", "end_day", "end_time", "shift", "shift_date", "file_path"
                ]]
            )

        grp_clean = grp[~mask].copy()
        clean_rows.append(grp_clean)

    df_outliers = (
        pd.concat(outlier_rows, ignore_index=True)
        if outlier_rows
        else pd.DataFrame(columns=[
            "id", "station", "remark", "barcode_information", "ct",
            "Outliers", "end_day", "end_time", "shift", "shift_date", "file_path"
        ])
    )
    df_clean_new = (
        pd.concat(clean_rows, ignore_index=True)
        if clean_rows
        else pd.DataFrame(columns=[
            "id", "station", "remark", "barcode_information", "ct",
            "end_day", "end_time", "shift", "shift_date", "file_path"
        ])
    )

    print("\n[STEP 3] (신규 데이터) 이상치 (앞 10행)")
    print(df_outliers.head(10).to_string(index=False))

    print("\n[STEP 4] (신규 데이터) 이상치 제거 후 데이터 (앞 10행)")
    print(df_clean_new.head(10).to_string(index=False))

    return df_clean_new, df_outliers


# ===========================
# 4. Bootstrap (멀티프로세싱)
# ===========================
def bootstrap_ci_mean(series: pd.Series,
                      n_boot: int = N_BOOTSTRAP,
                      ci: float = CI_LEVEL):
    series = series.dropna()
    if len(series) == 0:
        return np.nan, np.nan, np.nan

    means = []
    n = len(series)
    for _ in range(n_boot):
        sample = series.sample(n, replace=True)
        means.append(sample.mean())

    means = np.array(means)
    alpha = 100.0 - ci
    lower = np.percentile(means, alpha / 2.0)
    upper = np.percentile(means, 100.0 - alpha / 2.0)
    return float(series.mean()), float(lower), float(upper)


def _bootstrap_one_group(args):
    station, remark, values = args
    s = pd.Series(values)
    mean_ct, ci_low, ci_high = bootstrap_ci_mean(s)
    return {
        "station": station,
        "remark": remark,
        "ct": round(mean_ct, 2),
        "ci_low": round(ci_low, 2),
        "ci_high": round(ci_high, 2),
    }


def compute_bootstrap_ci(df_clean: pd.DataFrame) -> pd.DataFrame:
    tasks = []
    for (station, remark), grp in df_clean.groupby(["station", "remark"]):
        tasks.append((station, remark, grp["ct"].values))

    if not tasks:
        return pd.DataFrame(columns=["station", "remark", "ct", "ci_low", "ci_high"])

    with ProcessPoolExecutor(max_workers=N_BOOTSTRAP_WORKERS) as ex:
        results = list(ex.map(_bootstrap_one_group, tasks))

    df_ci = pd.DataFrame(results)

    print(f"\n[STEP 4-1] station/remark별 CT 평균 + Bootstrap {N_BOOTSTRAP}회, {CI_LEVEL}% CI")
    print(df_ci.to_string(index=False))

    return df_ci


def add_box_stats_annotations(fig: 'go.Figure',
                              df: pd.DataFrame,
                              x_col: str,
                              y_col: str,
                              label_prefix: str = ""):
    """
    df 기준으로 x_col 카테고리별 boxplot 통계 (min, q1, median, q3, max)를 계산해서
    fig에 annotation으로 항상 표시.
    """
    if df.empty:
        return

    stats = (
        df.groupby(x_col)[y_col]
          .describe(percentiles=[0.25, 0.5, 0.75])
          .reset_index()
    )
    stats = stats.rename(columns={
        "min": "min",
        "25%": "q1",
        "50%": "median",
        "75%": "q3",
        "max": "max",
    })

    for _, row in stats.iterrows():
        x_val = row[x_col]
        entries = [
            ("min", row["min"]),
            ("q1", row["q1"]),
            ("median", row["median"]),
            ("q3", row["q3"]),
            ("max", row["max"]),
        ]

        y_shift_step = 10
        for i, (name, y_val) in enumerate(entries):
            text = f"{label_prefix}{name}: {y_val:.2f}"
            fig.add_annotation(
                x=x_val,
                y=y_val,
                text=text,
                showarrow=False,
                xanchor="left",
                yanchor="bottom",
                font=dict(size=10),
                yshift=i * y_shift_step,
            )


# ===========================
# 5. Plotly 대시보드 (한 HTML에 2개 그래프 좌우)
# ===========================
def save_dashboard_html(df_ci: pd.DataFrame, df_clean: pd.DataFrame, month_str: str) -> str:
    """
    부트스트랩 bar + 종합 Boxplot을 한 HTML에 좌우로 배치.
    """
    global FIRST_OPEN

    if df_clean.empty or df_ci.empty:
        print("[INFO] df_clean 또는 df_ci가 비어 있어서 대시보드 생성 스킵")
        return ""

    ensure_plot_dir()

    # (1) CI 막대 그래프
    df_ci_plot = df_ci.copy()
    df_ci_plot["err_plus"] = df_ci_plot["ci_high"] - df_ci_plot["ct"]
    df_ci_plot["err_minus"] = df_ci_plot["ct"] - df_ci_plot["ci_low"]

    df_ci_plot["label"] = df_ci_plot.apply(
        lambda r: f"ct={r['ct']:.2f} (+{r['err_plus']:.2f} / -{r['err_minus']:.2f})",
        axis=1
    )

    fig_bar = px.bar(
        df_ci_plot,
        x="station",
        y="ct",
        color="remark",
        barmode="group",
        error_y="err_plus",
        error_y_minus="err_minus",
        text="label",
        title=(
            f"[{month_str}] Vision1 / Vision2, PD/Non-PD별 CT 평균 "
            f"(Bootstrap {N_BOOTSTRAP}회, {CI_LEVEL}% CI)"
        )
    )
    fig_bar.update_traces(textposition="outside", cliponaxis=False)

    # (2) Boxplot
    df_box = df_clean.copy()
    df_box["station_remark"] = df_box["station"] + " / " + df_box["remark"]

    fig_combined = px.box(
        df_box,
        x="station_remark",
        y="ct",
        points="outliers",
        title=f"[{month_str}] Vision1 / Vision2 × PD / Non-PD CT 분포 (Boxplot)"
    )
    fig_combined.update_layout(xaxis_title="Station / Remark", yaxis_title="CT")

    add_box_stats_annotations(fig_combined, df_box, "station_remark", "ct")

    # (3) 두 그래프 HTML 변환
    figs = [fig_bar, fig_combined]
    fig_htmls = []
    for i, fig in enumerate(figs):
        html = pio.to_html(
            fig,
            include_plotlyjs="cdn" if i == 0 else False,
            full_html=False
        )
        fig_htmls.append(html)

    auto_refresh = """
    <script>
        setTimeout(function() {
            location.reload();
        }, 7000);
    </script>
    """

    full_html = f"""
<html>
<head>
<meta charset="utf-8">
{auto_refresh}
<style>
    body {{
        margin: 0;
        padding: 10px;
        font-family: Arial, sans-serif;
    }}
    .grid {{
        display: flex;
        flex-direction: row;
        width: 100vw;
        height: 100vh;
    }}
    .chart-half {{
        width: 50%;
        height: 100%;
        padding: 10px;
    }}
</style>
</head>
<body>
<div class="grid">
"""

    for html in fig_htmls:
        full_html += f'<div class="chart-half">{html}</div>'

    full_html += """
</div>
</body>
</html>
"""

    out_path = os.path.join(PLOT_DIR, PLOT_FILE)
    abs_path = os.path.abspath(out_path)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    print(f"[INFO] 그래프 HTML 생성 완료: {abs_path}")
    print(f"[DEBUG] FIRST_OPEN = {FIRST_OPEN}")

    # if FIRST_OPEN:
    #     try:
    #         FIRST_OPEN = False
    #         url = "file://" + abs_path
    #         print(f"[DEBUG] 브라우저 오픈 시도: {url}")
    #         webbrowser.open_new_tab(url)
    #         print("[INFO] 브라우저 오픈 성공")
    #     except Exception as e:
    #         print("[ERROR] webbrowser.open 실패:", repr(e))
    # else:
    #     print("[INFO] 기존 브라우저는 자동 새로고침만 동작")

    return full_html


# ===========================
# 6. remark별 최종 CT (/2)
# ===========================
def compute_final_ct(df_clean: pd.DataFrame) -> pd.DataFrame:
    df_remark_mean = (
        df_clean
        .groupby("remark")["ct"]
        .mean()
        .reset_index()
    )
    df_remark_mean["ct"] = (df_remark_mean["ct"] / 2).round(2)

    df_final = df_remark_mean.copy()
    df_final.insert(0, "id", range(1, len(df_final) + 1))

    print("\n[STEP 5] remark별 최종 CT (/2) 결과")
    print(df_final.to_string(index=False))

    return df_final


# ===========================
# 7. Month 계산 + upsert (전체 월 스냅샷 기준)
# ===========================
def add_month_and_save(engine: create_engine,
                       df_clean: pd.DataFrame,
                       df_final_dummy: pd.DataFrame):
    """
    df_clean 전체를 기준으로 month(YYYY-MM) × remark별 CT 평균(/2)을 계산해서
    e1_whole_ct.whole_ct에 모두 upsert.

    - 예: df_clean에 2025-10, 2025-12 섞여 있으면 두 달 모두 저장.
    - 현재는 VIEW와 processing 로딩이 '이번 달'로 제한되어 있어서
      실질적으로는 한 달만 나오지만, 구조는 멀티월에도 대응.
    """
    global LAST_RESULT

    if df_clean.empty:
        print("\n[WARN] df_clean이 비어있어 Month를 계산할 수 없습니다.")
        return None, False

    df_tmp = df_clean.copy()
    df_tmp["month"] = pd.to_datetime(df_tmp["shift_date"]).dt.to_period("M").astype(str)

    df_month = (
        df_tmp
        .groupby(["month", "remark"])["ct"]
        .mean()
        .reset_index()
    )
    df_month["ct"] = (df_month["ct"] / 2).round(2)

    print("\n[STEP 6] 월별 × remark별 최종 CT (/2) 결과")
    print(df_month.to_string(index=False))

    current_snapshot = {
        (row["month"], row["remark"]): float(row["ct"])
        for _, row in df_month.iterrows()
    }

    if LAST_RESULT and LAST_RESULT == current_snapshot:
        months = sorted(df_month["month"].unique())
        month_ym = months[-1]
        print(f"\n[INFO] 모든 월별 결과가 이전과 동일 → DB/그래프 스킵 (latest month={month_ym})")
        return month_ym, False

    LAST_RESULT = current_snapshot

    df_to_save = df_month.copy()

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS e1_whole_ct;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS e1_whole_ct.whole_ct (
                month   varchar(7),
                remark  varchar(20),
                ct      numeric
            );
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS whole_ct_month_remark_idx
            ON e1_whole_ct.whole_ct (month, remark);
        """))

        upsert_sql = text("""
            INSERT INTO e1_whole_ct.whole_ct (month, remark, ct)
            VALUES (:month, :remark, :ct)
            ON CONFLICT (month, remark)
            DO UPDATE SET ct = EXCLUDED.ct;
        """)

        for _, row in df_to_save.iterrows():
            conn.execute(upsert_sql, {
                "month": row["month"],
                "remark": row["remark"],
                "ct": float(row["ct"]),
            })

    print("\n[STEP 7] e1_whole_ct.whole_ct 테이블 upsert 완료")
    print(df_to_save.to_string(index=False))

    months = sorted(df_month["month"].unique())
    month_ym = months[-1]

    return month_ym, True


# ===========================
# ct_graph_html 저장
# ===========================
def save_ct_graph_html(engine: create_engine,
                       month_ym: str,
                       df_final: pd.DataFrame,
                       html: str):
    """
    e1_whole_ct.ct_graph_html 에 Plotly 대시보드 HTML 저장.
    - 키: (month, remark)
    - 같은 month+remark에 대해서는 html을 업데이트 (중복 없음)
    """
    if not html:
        print("\n[INFO] HTML 내용이 비어 있어 ct_graph_html 저장 스킵")
        return

    if df_final.empty:
        print("\n[INFO] df_final이 비어 있어 ct_graph_html 저장 스킵")
        return

    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS e1_whole_ct;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS e1_whole_ct.ct_graph_html (
                month   varchar(7),
                remark  varchar(20),
                html    text
            );
        """))
        conn.execute(text("""
            CREATE UNIQUE INDEX IF NOT EXISTS ct_graph_html_month_remark_idx
            ON e1_whole_ct.ct_graph_html (month, remark);
        """))

        upsert_sql = text("""
            INSERT INTO e1_whole_ct.ct_graph_html (month, remark, html)
            VALUES (:month, :remark, :html)
            ON CONFLICT (month, remark)
            DO UPDATE SET html = EXCLUDED.html;
        """)

        for _, row in df_final.iterrows():
            conn.execute(upsert_sql, {
                "month": month_ym,
                "remark": row["remark"],
                "html": html,
            })

    print(f"\n[STEP 8] e1_whole_ct.ct_graph_html 저장 완료 (month={month_ym})")


# ===========================
# 한 사이클 실행
#  - processing에서 이번 달 file_path 로드
#  - VIEW RAW와 비교해서 신규 file_path만 처리
# ===========================
def run_one_cycle(engine):
    print("\n==============================")
    print("[CYCLE START]", datetime.now())
    print("==============================")

    # 1) VIEW에서 RAW 로드 (이미 SQL에서 '이번 달'로 제한된 상태라고 가정)
    df_raw = load_data(engine)
    if df_raw.empty:
        print("[INFO] RAW DATA 없음")
        return

    df_raw["file_path"] = df_raw["file_path"].astype(str)

    # 2) 이미 처리된 clean 데이터 (이번 달 end_day 조건) 로드
    df_old_clean = load_processed_clean(engine)
    if df_old_clean.empty:
        processed_paths = set()
    else:
        df_old_clean["file_path"] = df_old_clean["file_path"].astype(str)
        processed_paths = set(df_old_clean["file_path"])

    print(f"[INFO] 이미 처리된 clean 데이터 수: {len(df_old_clean)}")
    print(f"[INFO] 이미 처리된 file_path 수: {len(processed_paths)}")

    # 3) RAW 중에서 아직 처리 안 된 file_path만 남기기
    df_new_raw = df_raw[~df_raw["file_path"].isin(processed_paths)].copy()
    print(f"[INFO] 이번 사이클 신규 RAW 데이터 수: {len(df_new_raw)}")

    if df_new_raw.empty:
        print("[INFO] 신규 RAW 데이터 없음 → 이번 사이클 스킵")
        return

    # 4) 신규 RAW에 주/야간 + shift_date 부여
    df_shift_new = add_shift_info(df_new_raw)

    # 5) 신규 RAW에 대해 IQR 이상치 제거
    df_new_clean, df_outliers = remove_outliers(df_shift_new)
    print(f"[INFO] 신규 clean 데이터 수: {len(df_new_clean)}")

    if df_new_clean.empty:
        print("[INFO] 신규 clean 데이터 없음 → 이번 사이클 스킵")
        return

    # 6) 기존 clean + 신규 clean 합치기 (이번 달 기준)
    if not df_old_clean.empty:
        df_clean = pd.concat([df_old_clean, df_new_clean], ignore_index=True)
    else:
        df_clean = df_new_clean

    print(f"[INFO] 합쳐진 clean 데이터 수: {len(df_clean)}")

    # 7) 월별 whole_ct 테이블 업데이트 (df_clean 전체 기준)
    df_final_all = compute_final_ct(df_clean)
    month_ym, updated = add_month_and_save(engine, df_clean, df_final_all)

    if (month_ym is None) or (not updated):
        # 그래도 신규 clean은 processing 에 기록
        save_new_clean(engine, df_new_clean)
        return

    # 8) 최신 month_ym에 해당하는 데이터만 골라 그래프용으로 사용
    df_clean["month"] = pd.to_datetime(df_clean["shift_date"]).dt.to_period("M").astype(str)
    df_month = df_clean[df_clean["month"] == month_ym].copy()

    if df_month.empty:
        print("[WARN] 최신 월(month_ym) 데이터 없음 → 그래프/HTML 스킵")
        save_new_clean(engine, df_new_clean)
        return

    df_ci_month = compute_bootstrap_ci(df_month)
    df_final_month = compute_final_ct(df_month)

    html = save_dashboard_html(df_ci_month, df_month, month_ym)
    save_ct_graph_html(engine, month_ym, df_final_month, html)

    # 9) 마지막으로 신규 clean만 processing 테이블에 적재
    save_new_clean(engine, df_new_clean)


# ===========================
# main: 1초마다 무한 루프
# ===========================
def main_loop():
    engine = get_engine()
    while True:
        try:
            run_one_cycle(engine)
        except Exception as e:
            print("\n[ERROR] 사이클 실행 중 예외 발생:", repr(e))
        time_mod.sleep(LOOP_INTERVAL_SEC)


if __name__ == "__main__":
    main_loop()
