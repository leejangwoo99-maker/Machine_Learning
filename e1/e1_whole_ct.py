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

# 부트스트랩 반복 횟수
N_BOOTSTRAP = 10

# 신뢰구간 (%). 95.0 → 95% CI, 99.0 → 99% CI
CI_LEVEL = 95.0  # ★ 여기만 바꾸면 전체 CI 변경

# 부트스트랩 멀티프로세싱 워커 수
N_BOOTSTRAP_WORKERS = 2

# 그래프 HTML 저장 폴더 / 파일
PLOT_DIR = "./plots"
PLOT_FILE = "vision_dashboard.html"

# 루프 주기 (초)
LOOP_INTERVAL_SEC = 1.0

# 이전 결과 캐시: {month: {remark: ct}}
LAST_RESULT = {}

# 브라우저 최초 오픈 여부
FIRST_OPEN = True


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
        ORDER BY station, end_day, end_time;
    """
    df = pd.read_sql_query(query, engine)
    print("\n[STEP 1] RAW DATA (앞 5행)")
    print(df.head().to_string(index=False))
    return df


# ===========================
# 1-1. 기존 clean 데이터 로드 / 신규 clean 저장 (processing 테이블)
# ===========================
def load_processed_clean(engine: create_engine) -> pd.DataFrame:
    """
    e1_whole_ct.processing 에 저장된 '이미 처리된 clean 데이터'를 읽어온다.
    없으면 빈 DataFrame 반환.
    """
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
            """,
            conn,
        )
    return df_old


def save_new_clean(engine, df):
    print("[DEBUG] save_new_clean() 호출됨 — row 수:", len(df))

    if df.empty:
        print("[DEBUG] df 비어있음 — INSERT 스킵")
        return

    # INSERT에 필요한 컬럼만 선택
    required_cols = ["station", "remark", "barcode_information", "ct",
                     "end_day", "end_time", "shift", "shift_date", "file_path"]

    missing = [c for c in required_cols if c not in df.columns]
    print("[DEBUG] 누락된 컬럼:", missing)

    if missing:
        print("[ERROR] INSERT 실패 — 컬럼이 부족함")
        return

    # 🔥 TEST INSERT (실제 commit 포함)
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
# 3. IQR 이상치 제거 (★ 기존 + processed_keys 적용 버전)
# ===========================
def detect_outliers_iqr(series: pd.Series):
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (series < lower) | (series > upper)
    return mask, lower, upper


def remove_outliers(df: pd.DataFrame, processed_keys=None):
    """
    processed_keys: {(station, remark, file_path), ...}
    에 포함된 조합은 이번 이상치 계산 대상에서 제외 (이미 처리된 데이터).
    """
    # 1) ct가 NULL인 행 제거
    df_ct = df.dropna(subset=["ct"]).copy()

    # 2) file_path 기준 중복 제거
    #    같은 file_path, station, remark 조합이 여러 번 있을 경우 → 마지막 행만 사용
    if "file_path" in df_ct.columns:
        df_ct = (
            df_ct
            .sort_values(["station", "remark", "end_day", "end_time"])
            .drop_duplicates(subset=["station", "remark", "file_path"], keep="last")
        )

    # === 이미 처리된 데이터 제거 ===
    if processed_keys:
        key_series = list(zip(df_ct["station"], df_ct["remark"], df_ct["file_path"]))
        mask_new = [k not in processed_keys for k in key_series]
        df_ct = df_ct[mask_new]

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
    fig에 annotation으로 항상 표시해 준다.
    label_prefix: 'Vision2 / PD' 같은 앞부분 문자열 넣을 때 사용 (선택)
    """
    if df.empty:
        return

    # describe로 기본 통계 계산
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
        # 표시할 지표들
        entries = [
            ("min", row["min"]),
            ("q1", row["q1"]),
            ("median", row["median"]),
            ("q3", row["q3"]),
            ("max", row["max"]),
        ]

        # 글자가 겹치지 않게 y 방향으로 살짝씩 밀어줌
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
    global FIRST_OPEN   # ← 반드시 함수 맨 위에서 선언해야 오류가 안남!

    if df_clean.empty or df_ci.empty:
        print("[INFO] df_clean 또는 df_ci가 비어 있어서 대시보드 생성 스킵")
        return

    ensure_plot_dir()

    # ---------- (1) CI 막대 그래프 + 텍스트 라벨 ----------
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

    # ---------- (2) Boxplot ----------
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

    # ---------- (3) 두 그래프 HTML 변환 ----------
    figs = [fig_bar, fig_combined]
    fig_htmls = []
    for i, fig in enumerate(figs):
        html = pio.to_html(
            fig,
            include_plotlyjs="cdn" if i == 0 else False,
            full_html=False
        )
        fig_htmls.append(html)

    # ★ 7초 자동 새로고침 스크립트
    auto_refresh = """
    <script>
        setTimeout(function() {
            location.reload();
        }, 7000);
    </script>
    """

    # ---------- (4) 최종 HTML ----------
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

    # ---------- (5) 파일 저장 ----------
    out_path = os.path.join(PLOT_DIR, PLOT_FILE)
    abs_path = os.path.abspath(out_path)
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(full_html)

    print(f"[INFO] 그래프 HTML 생성 완료: {abs_path}")
    print(f"[DEBUG] FIRST_OPEN = {FIRST_OPEN}")

    # ---------- (6) 최초 1회 브라우저 오픈 ----------
    if FIRST_OPEN:
        try:
            FIRST_OPEN = False
            url = "file://" + abs_path
            print(f"[DEBUG] 브라우저 오픈 시도: {url}")
            webbrowser.open_new_tab(url)
            print("[INFO] 브라우저 오픈 성공")
        except Exception as e:
            print("[ERROR] webbrowser.open 실패:", repr(e))
    else:
        print("[INFO] 기존 브라우저는 자동 새로고침만 동작")

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
# 7. Month 계산 + upsert (동일 데이터면 pass)
# ===========================
def add_month_and_save(engine: create_engine,
                       df_clean: pd.DataFrame,
                       df_final: pd.DataFrame):
    global LAST_RESULT

    if df_clean.empty:
        print("\n[WARN] df_clean이 비어있어 Month를 계산할 수 없습니다.")
        return None, False

    shift_dt = pd.to_datetime(df_clean["shift_date"])
    month_ym = shift_dt.max().strftime("%Y-%m")

    df_final_with_month = df_final.copy()
    df_final_with_month["Month"] = month_ym

    print("\n[STEP 6] Month 적용 최종 결과")
    print(df_final_with_month.to_string(index=False))

    # 동일 데이터 여부 체크
    current = {row["remark"]: float(row["ct"])
               for _, row in df_final_with_month.iterrows()}
    prev = LAST_RESULT.get(month_ym)

    if prev == current:
        print(f"\n[INFO] {month_ym} 결과가 이전과 동일 → DB/그래프 스킵")
        return month_ym, False

    # 캐시 갱신
    LAST_RESULT[month_ym] = current

    df_to_save = df_final_with_month[["Month", "remark", "ct"]].rename(columns={
        "Month": "month"
    })

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

    return month_ym, True


# ===========================
# 한 사이클 실행 (★ 기존 clean + 신규 clean 합치기)
# ===========================
def run_one_cycle(engine):
    print("\n==============================")
    print("[CYCLE START]", datetime.now())
    print("==============================")

    # 1) VIEW에서 RAW 로드
    df_raw = load_data(engine)
    if df_raw.empty:
        print("[INFO] RAW DATA 없음")
        return

    # 2) 이미 처리된 clean 데이터 로드
    df_old_clean = load_processed_clean(engine)
    print(f"[INFO] 이미 처리된 clean 데이터 수: {len(df_old_clean)}")

    # 3) RAW 전체에 shift 정보 부여
    df_shift = add_shift_info(df_raw)

    # 4) 이미 처리된 (station, remark, file_path) 조합 만들기
    if not df_old_clean.empty:
        processed_keys = set(
            zip(df_old_clean["station"], df_old_clean["remark"], df_old_clean["file_path"])
        )
    else:
        processed_keys = set()

    # 5) 신규 데이터만 IQR 이상치 계산
    df_new_clean, df_outliers = remove_outliers(df_shift, processed_keys=processed_keys)
    print(f"[DEBUG] 이번 사이클 신규 clean 데이터 수: {len(df_new_clean)}")

    if df_new_clean.empty and not df_old_clean.empty:
        print("[INFO] 신규 clean 데이터 없음 → 이번 사이클 계산 스킵 (기존 결과 유지)")
        return
    elif df_new_clean.empty and df_old_clean.empty:
        print("[INFO] 처리할 clean 데이터가 전혀 없음")
        return

    # 4) 이미 처리된 (station, remark, file_path) 조합 만들기
    if not df_old_clean.empty:
        processed_keys = set(
            zip(df_old_clean["station"], df_old_clean["remark"], df_old_clean["file_path"])
        )
    else:
        processed_keys = set()

    # 5) 신규 데이터만 IQR 이상치 계산
    df_new_clean, df_outliers = remove_outliers(df_shift, processed_keys=processed_keys)

    if df_new_clean.empty and not df_old_clean.empty:
        print("[INFO] 신규 clean 데이터 없음 → 이번 사이클 계산 스킵 (기존 결과 유지)")
        return
    elif df_new_clean.empty and df_old_clean.empty:
        print("[INFO] 처리할 clean 데이터가 전혀 없음")
        return

    # 6) 기존 + 신규 clean 데이터 합치기
    if not df_old_clean.empty:
        df_clean = pd.concat([df_old_clean, df_new_clean], ignore_index=True)
    else:
        df_clean = df_new_clean

    print(f"[INFO] 합쳐진 clean 데이터 수: {len(df_clean)}")

    # 7) 부트스트랩 CI, remark별 최종 CT 계산
    df_ci = compute_bootstrap_ci(df_clean)
    df_final = compute_final_ct(df_clean)
    month_ym, updated = add_month_and_save(engine, df_clean, df_final)

    # month 계산 실패 or 동일 데이터면 그래프/DB 스킵
    if (month_ym is None) or (not updated):
        # 그래도 신규 clean이 있다면 processing 에는 저장
        if not df_new_clean.empty:
            save_new_clean(engine, df_new_clean)
        return

    # 8) 데이터 변경이 있을 때만 대시보드 HTML 업데이트 + DB 저장
    html = save_dashboard_html(df_ci, df_clean, month_ym)
    save_ct_graph_html(engine, month_ym, df_final, html)

    # 9) 마지막으로 신규 clean 데이터만 processing 테이블에 적재
    if not df_new_clean.empty:
        save_new_clean(engine, df_new_clean)


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