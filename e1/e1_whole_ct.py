from sqlalchemy import create_engine, text
import pandas as pd
import numpy as np
from datetime import timedelta, time

import plotly.express as px


# ============================================
# DB 접속 정보
# ============================================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}


def get_engine():
    """SQLAlchemy 엔진 생성."""
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(url)


# ============================================
# 1. VIEW에서 데이터 로드
# ============================================
def load_data(engine: create_engine) -> pd.DataFrame:
    """
    v_vision_pass_ct VIEW에서 Vision1/2, PD/Non-PD CT 데이터 조회.
    VIEW 예시:
        SELECT id, station, remark, barcode_information, end_day, end_time, ct
        FROM a2_fct_vision_testlog_json_processing.v_vision_pass_ct
        ORDER BY station, end_day, end_time;
    """
    query = """
        SELECT id, station, remark, barcode_information, end_day, end_time, ct
        FROM a2_fct_vision_testlog_json_processing.v_vision_pass_ct
        ORDER BY station, end_day, end_time;
    """
    df = pd.read_sql_query(query, engine)
    print("\n[STEP 1] RAW DATA (앞 5행)")
    print(df.head().to_string(index=False))
    return df


# ============================================
# 2. 주/야간 및 shift_date 계산
# ============================================
def add_shift_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    end_day + end_time 기준으로 주/야간과 기준 날짜(shift_date)를 계산.
    - 주간: 08:30:00 ~ 20:29:59  → shift='Day',   shift_date=당일
    - 야간: 20:30:00 ~ 23:59:59  → shift='Night', shift_date=당일
    - 야간: 00:00:00 ~ 08:29:59  → shift='Night', shift_date=전날
    """
    df2 = df.copy()

    # end_day, end_time을 하나의 datetime으로 변환
    df2["end_day"] = df2["end_day"].astype(str).str.strip()
    df2["end_time"] = df2["end_time"].astype(str).str.strip()
    df2["end_dt"] = pd.to_datetime(
        df2["end_day"] + " " + df2["end_time"],
        format="%Y-%m-%d %H:%M:%S"
    )

    # 기본값
    df2["shift"] = "Day"
    df2["shift_date"] = df2["end_dt"].dt.date

    # 시간만 추출
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

    # 야간 (새벽) - 전날 야간으로 귀속
    cond_night_morning = (t <= t_night_morning_end)
    df2.loc[cond_night_morning, "shift"] = "Night"
    df2.loc[cond_night_morning, "shift_date"] = (
        df2.loc[cond_night_morning, "end_dt"] - timedelta(days=1)
    ).dt.date

    print("\n[STEP 2] SHIFT INFO (앞 10행)")
    print(df2[["id", "station", "remark", "end_day", "end_time", "shift", "shift_date"]]
          .head(10).to_string(index=False))

    return df2


# ============================================
# 3. IQR 이상치 탐지 및 제거
# ============================================
def detect_outliers_iqr(series: pd.Series):
    """
    IQR(Boxplot) 기준으로 이상치 여부 mask 반환.
    Q1 - 1.5*IQR 보다 작거나, Q3 + 1.5*IQR 보다 큰 값들을 이상치로 간주.
    """
    q1 = series.quantile(0.25)
    q3 = series.quantile(0.75)
    iqr = q3 - q1
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    mask = (series < lower) | (series > upper)
    return mask, lower, upper


def remove_outliers(df: pd.DataFrame):
    """
    station, remark별 IQR 이상치 제거.
    반환:
        df_clean   : 이상치 제거된 데이터
        df_outliers: 이상치만 모아둔 데이터
    """
    df_ct = df.dropna(subset=["ct"]).copy()

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
                    "Outliers", "end_day", "end_time", "shift", "shift_date"
                ]]
            )

        grp_clean = grp[~mask].copy()
        clean_rows.append(grp_clean)

    df_outliers = (
        pd.concat(outlier_rows, ignore_index=True)
        if outlier_rows
        else pd.DataFrame(columns=[
            "id", "station", "remark", "barcode_information", "ct",
            "Outliers", "end_day", "end_time", "shift", "shift_date"
        ])
    )
    df_clean = pd.concat(clean_rows, ignore_index=True)

    print("\n[STEP 3] 이상치 (앞 10행)")
    print(df_outliers.head(10).to_string(index=False))

    print("\n[STEP 4] 이상치 제거 후 데이터 (앞 10행)")
    print(df_clean.head(10).to_string(index=False))

    return df_clean, df_outliers


# ============================================
# 4. Bootstrap 설정 + 함수
# ============================================
# 부트스트랩 반복 횟수 (필요 시 여기 숫자만 변경)
# 예: 500, 1000, 2000 ...
N_BOOTSTRAP = 1000  # ★ 여기 값만 바꾸면 반복 횟수 변경됨


def bootstrap_ci_mean(series: pd.Series, n_boot: int = N_BOOTSTRAP, ci: float = 95.0):
    """
    복원추출 bootstrap으로 평균의 신뢰구간(CI)을 구함.
    - series : CT 값 시리즈
    - n_boot : 부트스트랩 반복 횟수 (기본값 N_BOOTSTRAP 사용)
    - ci     : 신뢰수준 (예: 95.0)
    """
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


def compute_bootstrap_ci(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    station / remark별로 CT 평균과 bootstrap 95% CI 계산.
    결과 DataFrame (station, remark, ct, ci_low, ci_high)을 반환하고 콘솔에 출력.
    """
    rows = []

    for (station, remark), grp in df_clean.groupby(["station", "remark"]):
        mean_ct, ci_low, ci_high = bootstrap_ci_mean(grp["ct"])
        rows.append({
            "station": station,
            "remark": remark,
            "ct": round(mean_ct, 2),
            "ci_low": round(ci_low, 2),
            "ci_high": round(ci_high, 2),
        })

    df_ci = pd.DataFrame(rows)

    print(f"\n[STEP 4-1] station/remark별 CT 평균 + Bootstrap {N_BOOTSTRAP}회, 95% CI")
    print(df_ci.to_string(index=False))

    return df_ci


# ============================================
# 5. Plotly 그래프들
# ============================================
def plot_ci_bar(df_ci: pd.DataFrame):
    """station/remark별 평균 CT + CI 막대 그래프."""
    df_ci_plot = df_ci.copy()
    df_ci_plot["err_plus"] = df_ci_plot["ci_high"] - df_ci_plot["ct"]
    df_ci_plot["err_minus"] = df_ci_plot["ct"] - df_ci_plot["ci_low"]

    fig = px.bar(
        df_ci_plot,
        x="station",
        y="ct",
        color="remark",
        barmode="group",
        error_y="err_plus",
        error_y_minus="err_minus",
        title=f"Vision1 / Vision2, PD/Non-PD별 CT 평균 (Bootstrap {N_BOOTSTRAP}회, 95% CI)"
    )
    fig.show()


def plot_boxplots(df_clean: pd.DataFrame):
    """PD / Non-PD Boxplot + station/remark 4조합 Boxplot."""
    # PD / Non-PD 분리
    df_pd = df_clean[df_clean["remark"] == "PD"].copy()
    df_nonpd = df_clean[df_clean["remark"] == "Non-PD"].copy()

    # PD Boxplot
    if not df_pd.empty:
        fig_pd = px.box(
            df_pd,
            x="station",
            y="ct",
            points="outliers",
            title="PD 제품 - Vision1 / Vision2 CT 분포 (Boxplot, 이상치 포함)"
        )
        fig_pd.show()

    # Non-PD Boxplot
    if not df_nonpd.empty:
        fig_nonpd = px.box(
            df_nonpd,
            x="station",
            y="ct",
            points="outliers",
            title="Non-PD 제품 - Vision1 / Vision2 CT 분포 (Boxplot, 이상치 포함)"
        )
        fig_nonpd.show()

    # station + remark 4조합 Boxplot
    df_box = df_clean.copy()
    df_box["station_remark"] = df_box["station"] + " / " + df_box["remark"]

    fig_combined = px.box(
        df_box,
        x="station_remark",
        y="ct",
        points="outliers",
        title="Vision1 / Vision2 × PD / Non-PD CT 분포 (Boxplot, 이상치 포함)"
    )
    fig_combined.update_layout(
        xaxis_title="Station / Remark",
        yaxis_title="CT"
    )
    fig_combined.show()


# ============================================
# 6. remark별 최종 CT 계산 (평균 CT / 2)
# ============================================
def compute_final_ct(df_clean: pd.DataFrame) -> pd.DataFrame:
    """
    remark(PD / Non-PD)별 평균 CT를 구한 뒤 2로 나누고,
    id, remark, ct 형태 DataFrame 반환.
    """
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


# ============================================
# 7. Month 계산 (shift_date 기준) + DB 저장
# ============================================
def add_month_and_save(engine: create_engine,
                       df_clean: pd.DataFrame,
                       df_final: pd.DataFrame):
    """
    shift_date 기준으로 Month(YYYY-MM)를 계산하고
    e1_whole_ct.whole_ct에 (month, remark, ct) 저장.
    """
    if df_clean.empty:
        print("\n[WARN] df_clean이 비어있어 Month를 계산할 수 없습니다.")
        return

    # shift_date → datetime으로 변환 후 Month 문자열 생성
    shift_dt = pd.to_datetime(df_clean["shift_date"])
    month_ym = shift_dt.max().strftime("%Y-%m")  # 예: '2025-11'

    df_final_with_month = df_final.copy()
    df_final_with_month["Month"] = month_ym

    print("\n[STEP 6] Month 적용 최종 결과")
    print(df_final_with_month.to_string(index=False))

    # 스키마/테이블 생성 후 INSERT
    with engine.begin() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS e1_whole_ct;"))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS e1_whole_ct.whole_ct (
                month   varchar(7),   -- 예: '2025-11'
                remark  varchar(20),  -- 'PD' / 'Non-PD'
                ct      numeric       -- 최종 CT (/2)
            );
        """))

    df_to_save = df_final_with_month[["Month", "remark", "ct"]].rename(columns={
        "Month": "month"
    })

    df_to_save.to_sql(
        name="whole_ct",
        schema="e1_whole_ct",
        con=engine,
        if_exists="append",
        index=False
    )

    print("\n[STEP 7] e1_whole_ct.whole_ct 테이블에 저장 완료")
    print(df_to_save.to_string(index=False))


# ============================================
# main
# ============================================
def main():
    engine = get_engine()

    # 1) VIEW 데이터 로드
    df_raw = load_data(engine)

    # 2) 주/야간 및 shift_date 계산
    df_shift = add_shift_info(df_raw)

    # 3) IQR 이상치 제거
    df_clean, df_outliers = remove_outliers(df_shift)

    # 4) station/remark별 bootstrap CI 계산
    df_ci = compute_bootstrap_ci(df_clean)

    # 5) Plotly 그래프들
    plot_ci_bar(df_ci)
    plot_boxplots(df_clean)

    # 6) remark별 최종 CT (/2) 계산
    df_final = compute_final_ct(df_clean)

    # 7) Month 계산 후 DB 저장
    add_month_and_save(engine, df_clean, df_final)


if __name__ == "__main__":
    main()
