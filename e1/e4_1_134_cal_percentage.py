# -*- coding: utf-8 -*-
import pandas as pd
from sqlalchemy import text, create_engine
import urllib.parse
from datetime import datetime
import time
import sys

# ================================
# 유틸: 로그/타이밍
# ================================
def now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def fmt_sec(sec: float) -> str:
    if sec < 60:
        return f"{sec:.2f} sec"
    return f"{sec:.2f} sec ({sec/60:.2f} min)"

class StepTimer:
    def __init__(self):
        self.t0 = time.perf_counter()

    def lap(self) -> float:
        t1 = time.perf_counter()
        dt = t1 - self.t0
        self.t0 = t1
        return dt

# ================================
# START TIME
# ================================
JOB_NAME = "e4_1_134_fail_percentage"
job_start_dt = datetime.now()
job_start_ts = time.perf_counter()

print(f"[START] {JOB_NAME} | {job_start_dt:%Y-%m-%d %H:%M:%S}")

# ================================
# DB 설정
# ================================
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

def get_engine(cfg=DB_CONFIG):
    user = cfg["user"]
    password = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_recycle=1800,
    )

engine = get_engine()

# ================================
# 파라미터
# ================================
SCHEMA = "a2_fct_table"
TABLE  = "fct_table"

START_DAY = "20251001"
END_DAY   = "20251124"

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4"]
TARGET_STEP = "1.34_Test_VUSB_Type-C_A(ELoad2=1.35A)vol"

TARGET_SCHEMA = "e4_predictive_maintenance"
TABLE_ALL     = "134_fail_percentage"
TABLE_ANOM    = "134_anomal_percentage"

try:
    step = StepTimer()

    # ================================
    # STEP 1) 데이터 조회
    # ================================
    print(f"[STEP 1] Query FAIL percentage data... ({now_str()})")

    sql = f"""
    SELECT
        station,
        end_day,
        COUNT(*) AS amount,
        ROUND(
            100.0 * SUM(CASE WHEN result='FAIL' THEN 1 ELSE 0 END)
            / NULLIF(COUNT(*), 0),
            2
        ) AS whole_fail_percentage,
        ROUND(
            100.0 * SUM(
                CASE
                    WHEN result='FAIL'
                     AND step_description = :target_step
                    THEN 1 ELSE 0
                END
            )
            / NULLIF(SUM(CASE WHEN result='FAIL' THEN 1 ELSE 0 END), 0),
            2
        ) AS "1.34_fail_percentage"
    FROM {SCHEMA}.{TABLE}
    WHERE station = ANY(:stations)
      AND end_day BETWEEN :start_day AND :end_day
    GROUP BY station, end_day
    ORDER BY station, end_day
    """

    q_ts = time.perf_counter()
    with engine.begin() as conn:
        df_all = pd.read_sql(
            text(sql),
            conn,
            params={
                "stations": STATIONS,
                "start_day": START_DAY,
                "end_day": END_DAY,
                "target_step": TARGET_STEP,
            }
        )
    q_dt = time.perf_counter() - q_ts

    print(f"[OK] Loaded rows = {len(df_all):,} | Query time = {fmt_sec(q_dt)}")
    print(f"[TIME] STEP 1 elapsed = {fmt_sec(step.lap())}")

    # ================================
    # STEP 2) 계산
    # ================================
    print(f"[STEP 2] Calculate delta & anomaly conditions... ({now_str()})")

    calc_ts = time.perf_counter()

    df_all["end_day"] = df_all["end_day"].astype(str)
    df_all["whole_fail_percentage"] = df_all["whole_fail_percentage"].fillna(0.0)
    df_all["1.34_fail_percentage"]  = df_all["1.34_fail_percentage"].fillna(0.0)

    df_all = df_all.sort_values(["station", "end_day"]).reset_index(drop=True)

    df_chg = df_all.copy()
    df_chg["delta_1.34"] = df_chg.groupby("station")["1.34_fail_percentage"].diff()
    df_chg["delta_flag"] = df_chg["delta_1.34"] >= 20

    prev_flag = (
        df_chg.groupby("station")["delta_flag"]
        .shift(1)
        .astype("boolean")
        .fillna(False)
    )

    df_chg["two_days_continuous"] = df_chg["delta_flag"] & prev_flag
    df_anom = df_chg[df_chg["two_days_continuous"]].copy()

    calc_dt = time.perf_counter() - calc_ts

    print(f"[OK] Anomaly rows = {len(df_anom):,} | Calc time = {fmt_sec(calc_dt)}")
    print(f"[TIME] STEP 2 elapsed = {fmt_sec(step.lap())}")

    # ================================
    # STEP 3) ALL 저장
    # ================================
    print(f"[STEP 3] Save ALL fail percentage (UPSERT)... ({now_str()})")

    up_ts = time.perf_counter()

    df_all = df_all.rename(columns={"1.34_fail_percentage": "134_fail_percentage"})

    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA};"))

        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}."{TABLE_ALL}" (
                station TEXT NOT NULL,
                end_day TEXT NOT NULL,
                amount INTEGER,
                whole_fail_percentage DOUBLE PRECISION,
                "134_fail_percentage" DOUBLE PRECISION,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (station, end_day)
            );
        """))

        conn.execute(text(f"""
            INSERT INTO {TARGET_SCHEMA}."{TABLE_ALL}" (
                station, end_day, amount,
                whole_fail_percentage, "134_fail_percentage", updated_at
            )
            VALUES (
                :station, :end_day, :amount,
                :whole_fail_percentage, :134_fail_percentage, NOW()
            )
            ON CONFLICT (station, end_day)
            DO UPDATE SET
                amount = EXCLUDED.amount,
                whole_fail_percentage = EXCLUDED.whole_fail_percentage,
                "134_fail_percentage" = EXCLUDED."134_fail_percentage",
                updated_at = NOW();
        """), df_all.to_dict("records"))

    up_dt = time.perf_counter() - up_ts

    print(f"[OK] Saved {len(df_all):,} rows -> {TARGET_SCHEMA}.\"{TABLE_ALL}\" | UPSERT time = {fmt_sec(up_dt)}")
    print(f"[TIME] STEP 3 elapsed = {fmt_sec(step.lap())}")

    # ================================
    # STEP 4) ANOMALY 저장
    # ================================
    print(f"[STEP 4] Save ANOMALY data (UPSERT)... ({now_str()})")

    up2_ts = time.perf_counter()

    saved_anom = 0
    if len(df_anom) > 0:
        df_anom = df_anom.rename(columns={
            "1.34_fail_percentage": "134_fail_percentage",
            "delta_1.34": "delta_1_34"
        })[
            ["station", "end_day", "amount",
             "whole_fail_percentage", "134_fail_percentage", "delta_1_34"]
        ]

        with engine.begin() as conn:
            conn.execute(text(f"""
                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}."{TABLE_ANOM}" (
                    station TEXT NOT NULL,
                    end_day TEXT NOT NULL,
                    amount INTEGER,
                    whole_fail_percentage DOUBLE PRECISION,
                    "134_fail_percentage" DOUBLE PRECISION,
                    delta_1_34 DOUBLE PRECISION,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    PRIMARY KEY (station, end_day)
                );
            """))

            conn.execute(text(f"""
                INSERT INTO {TARGET_SCHEMA}."{TABLE_ANOM}" (
                    station, end_day, amount,
                    whole_fail_percentage, "134_fail_percentage",
                    delta_1_34, updated_at
                )
                VALUES (
                    :station, :end_day, :amount,
                    :whole_fail_percentage, :134_fail_percentage,
                    :delta_1_34, NOW()
                )
                ON CONFLICT (station, end_day)
                DO UPDATE SET
                    amount = EXCLUDED.amount,
                    whole_fail_percentage = EXCLUDED.whole_fail_percentage,
                    "134_fail_percentage" = EXCLUDED."134_fail_percentage",
                    delta_1_34 = EXCLUDED.delta_1_34,
                    updated_at = NOW();
            """), df_anom.to_dict("records"))

        saved_anom = len(df_anom)

    up2_dt = time.perf_counter() - up2_ts

    print(f"[OK] Saved {saved_anom:,} rows -> {TARGET_SCHEMA}.\"{TABLE_ANOM}\" | UPSERT time = {fmt_sec(up2_dt)}")
    print(f"[TIME] STEP 4 elapsed = {fmt_sec(step.lap())}")

except Exception as e:
    print("[ERROR] Job failed")
    print(e)
    sys.exit(1)

# ================================
# END TIME
# ================================
job_end_dt = datetime.now()
job_elapsed = time.perf_counter() - job_start_ts

print(f"[END] {job_end_dt:%Y-%m-%d %H:%M:%S}")
print(f"[TOTAL] Elapsed time = {fmt_sec(job_elapsed)}")
