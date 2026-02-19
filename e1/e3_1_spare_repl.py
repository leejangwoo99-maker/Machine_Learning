# -*- coding: utf-8 -*-
"""
Sparepart Replacement Timing Pipeline (No DataFrame Print)

Flow:
1) Load b1_sparepart_usage.sparepart_usage (localhost)
2) Remove rows where all columns except (end_day, dayornight) are 0
3) Melt (wide -> long): extract station/sparepart (power -> probe)
4) Convert (end_day, dayornight) into from/to datetime window
5) For each window, find Manual -> Auto transition (>= 3 minutes) from d1_machine_log.FCT{1..4}_machine_log
   - keep only matched rows (drop NA)
6) Build ranges: current auto_dt -> next manual_dt per (station, sparepart)
7) Count amount in a1_fct_vision_testlog_txt_processing_history.fct_vision_testlog_txt_processing_history
   within each range (station=FCT1..FCT4)
   - keep amount >= 1000
8) Percentiles by sparepart: min, p10, p25, p50, p75, max (floored ints)
   - build plotly json (boxplot)
9) Save to localhost: e3_sparepart_replacement.sparepart_life_amount (UPSERT by sparepart)

Notes:
- No DataFrame printing.
- Minimal progress logs only.
"""

import sys
import time
from datetime import datetime

import numpy as np
import pandas as pd
import urllib.parse

from sqlalchemy import create_engine, text
import plotly.graph_objects as go


# =========================
# DB Config
# =========================
DB_LOCAL = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

DB_REMOTE = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}


def get_engine(cfg: dict):
    pw = urllib.parse.quote_plus(cfg["password"])
    conn_str = f"postgresql+psycopg2://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    return create_engine(conn_str, pool_pre_ping=True)


def floor_int(x):
    if pd.isna(x):
        return None
    return int(np.floor(x))


def parse_station_sparepart(col: str):
    """
    FCT1_mini_b -> (FCT1, mini_b)
    FCT1_power  -> (FCT1, probe)
    """
    station, part = col.split("_", 1)
    if part == "power":
        part = "probe"
    return station, part


def main():
    # ------------------------------------------------------------
    # 1) Load sparepart_usage (LOCAL)
    # ------------------------------------------------------------
    engine_local = get_engine(DB_LOCAL)

    SRC_SCHEMA = "b1_sparepart_usage"
    SRC_TABLE = "sparepart_usage"

    print("[INFO] Load:", f"{SRC_SCHEMA}.{SRC_TABLE}")
    df = pd.read_sql(f"SELECT * FROM {SRC_SCHEMA}.{SRC_TABLE}", engine_local)
    print("[INFO] Raw shape:", df.shape)

    # ------------------------------------------------------------
    # 2) Filter rows: non-base cols all 0 -> drop
    # ------------------------------------------------------------
    base_cols = ["end_day", "dayornight"]
    value_cols = [c for c in df.columns if c not in base_cols]
    df = df.loc[(df[value_cols] != 0).any(axis=1)].copy()
    print("[INFO] After zero-row filter:", df.shape)

    # ------------------------------------------------------------
    # 3) Melt -> extract station/sparepart
    # ------------------------------------------------------------
    melt_df = df.melt(
        id_vars=base_cols,
        value_vars=value_cols,
        var_name="raw_col",
        value_name="value",
    )
    melt_df = melt_df[melt_df["value"] != 0].copy()

    parsed = melt_df["raw_col"].apply(parse_station_sparepart)
    melt_df["station"] = parsed.apply(lambda x: x[0])
    melt_df["sparepart"] = parsed.apply(lambda x: x[1])

    # Result DF (no print)
    result_df = (
        melt_df[["end_day", "dayornight", "station", "sparepart"]]
        .sort_values(by="end_day", ascending=True)
        .reset_index(drop=True)
    )
    print("[INFO] result_df rows:", len(result_df))

    # ------------------------------------------------------------
    # 4) Build time windows from end_day + dayornight
    # ------------------------------------------------------------
    work_df = result_df.copy()
    work_df["end_day_dt"] = pd.to_datetime(
        work_df["end_day"].astype(str),
        format="%Y%m%d",
        errors="coerce"
    )

    DAY_FROM = pd.to_timedelta("08:30:00")
    DAY_TO = pd.to_timedelta("20:29:59")
    NIGHT_FROM = pd.to_timedelta("20:30:00")
    NIGHT_TO = pd.to_timedelta("08:29:59")  # next day

    is_day = work_df["dayornight"].astype(str).str.lower().eq("day")
    is_night = work_df["dayornight"].astype(str).str.lower().eq("night")

    work_df["from_dt"] = pd.NaT
    work_df["to_dt"] = pd.NaT

    work_df.loc[is_day, "from_dt"] = work_df.loc[is_day, "end_day_dt"] + DAY_FROM
    work_df.loc[is_day, "to_dt"] = work_df.loc[is_day, "end_day_dt"] + DAY_TO

    work_df.loc[is_night, "from_dt"] = work_df.loc[is_night, "end_day_dt"] + NIGHT_FROM
    work_df.loc[is_night, "to_dt"] = (work_df.loc[is_night, "end_day_dt"] + pd.Timedelta(days=1)) + NIGHT_TO

    # output window df
    out_df = (
        work_df.assign(
            from_day=work_df["from_dt"].dt.strftime("%Y%m%d"),
            from_time=work_df["from_dt"].dt.strftime("%H:%M:%S"),
            to_day=work_df["to_dt"].dt.strftime("%Y%m%d"),
            to_time=work_df["to_dt"].dt.strftime("%H:%M:%S"),
        )[["station", "sparepart", "from_day", "from_time", "to_day", "to_time"]]
        .sort_values(by=["station", "sparepart", "from_day", "from_time"])
        .reset_index(drop=True)
    )
    print("[INFO] out_df windows:", len(out_df))

    # ------------------------------------------------------------
    # 5) Find Manual->Auto (>=3 minutes) in d1_machine_log (LOCAL)
    # ------------------------------------------------------------
    SCHEMA_ML = "d1_machine_log"
    TABLE_BY_STATION = {
        "FCT1": "FCT1_machine_log",
        "FCT2": "FCT2_machine_log",
        "FCT3": "FCT3_machine_log",
        "FCT4": "FCT4_machine_log",
    }

    df_win = out_df.copy()
    df_win["from_dt"] = pd.to_datetime(
        df_win["from_day"] + " " + df_win["from_time"],
        format="%Y%m%d %H:%M:%S",
        errors="coerce"
    )
    df_win["to_dt"] = pd.to_datetime(
        df_win["to_day"] + " " + df_win["to_time"],
        format="%Y%m%d %H:%M:%S",
        errors="coerce"
    )
    df_win["station"] = df_win["station"].astype(str).str.strip()

    for c in ["manual_day", "manual_time", "auto_day", "auto_time"]:
        df_win[c] = pd.NA

    for st, g in df_win.groupby("station", sort=False):
        if st not in TABLE_BY_STATION:
            continue

        table = TABLE_BY_STATION[st]
        min_dt = g["from_dt"].min()
        max_dt = g["to_dt"].max()

        if pd.isna(min_dt) or pd.isna(max_dt):
            continue

        min_day = int(min_dt.strftime("%Y%m%d"))
        max_day = int((max_dt + pd.Timedelta(days=1)).strftime("%Y%m%d"))

        sql = text(f"""
            SELECT
                end_day::text  AS end_day,
                end_time::text AS end_time,
                contents::text AS contents
            FROM {SCHEMA_ML}."{table}"
            WHERE end_day::int BETWEEN :min_day AND :max_day
              AND (contents ILIKE '%Manual mode%' OR contents ILIKE '%Auto mode%')
            ORDER BY end_day, end_time
        """)

        ml = pd.read_sql(sql, engine_local, params={"min_day": min_day, "max_day": max_day})
        if ml.empty:
            continue

        ml["end_dt"] = pd.to_datetime(ml["end_day"] + " " + ml["end_time"], errors="coerce")
        ml = ml[(ml["end_dt"] >= min_dt) & (ml["end_dt"] <= max_dt)].copy()
        if ml.empty:
            continue

        ml["is_manual"] = ml["contents"].str.contains("Manual mode", case=False, na=False)
        ml["is_auto"] = ml["contents"].str.contains("Auto mode", case=False, na=False)

        manual_times = ml.loc[ml["is_manual"], "end_dt"].sort_values().reset_index(drop=True)
        auto_times = ml.loc[ml["is_auto"], "end_dt"].sort_values().reset_index(drop=True)

        if manual_times.empty or auto_times.empty:
            continue

        for idx, row in g.iterrows():
            w_from = row["from_dt"]
            w_to = row["to_dt"]

            m_in = manual_times[(manual_times >= w_from) & (manual_times <= w_to)]
            if m_in.empty:
                continue

            for m_t in m_in:
                a_in = auto_times[(auto_times >= m_t) & (auto_times <= w_to)]
                if a_in.empty:
                    continue

                a_t = a_in.iloc[0]
                if (a_t - m_t).total_seconds() >= 180:
                    df_win.loc[idx, "manual_day"] = m_t.strftime("%Y%m%d")
                    df_win.loc[idx, "manual_time"] = m_t.strftime("%H:%M:%S")
                    df_win.loc[idx, "auto_day"] = a_t.strftime("%Y%m%d")
                    df_win.loc[idx, "auto_time"] = a_t.strftime("%H:%M:%S")
                    break

    final_df = (
        df_win[
            [
                "station", "sparepart",
                "from_day", "from_time",
                "to_day", "to_time",
                "manual_day", "manual_time",
                "auto_day", "auto_time",
            ]
        ]
        .dropna()
        .sort_values(by=["station", "sparepart", "from_day", "from_time"])
        .reset_index(drop=True)
    )
    print("[INFO] Manual->Auto matched rows:", len(final_df))

    if final_df.empty:
        print("[WARN] final_df is empty. Stop.")
        return

    # ------------------------------------------------------------
    # 6) Build ranges: current auto_dt -> next manual_dt per (station, sparepart)
    # ------------------------------------------------------------
    df_in = final_df.copy()
    df_in["station"] = df_in["station"].astype(str).str.strip()
    df_in = df_in[df_in["station"].isin(["FCT1", "FCT2", "FCT3", "FCT4"])].copy()

    df_in["manual_dt"] = pd.to_datetime(
        df_in["manual_day"] + " " + df_in["manual_time"],
        format="%Y%m%d %H:%M:%S",
        errors="coerce"
    )
    df_in["auto_dt"] = pd.to_datetime(
        df_in["auto_day"] + " " + df_in["auto_time"],
        format="%Y%m%d %H:%M:%S",
        errors="coerce"
    )
    df_in = df_in.sort_values(by=["station", "sparepart", "manual_dt", "auto_dt"]).reset_index(drop=True)

    df_in["next_manual_dt"] = df_in.groupby(["station", "sparepart"])["manual_dt"].shift(-1)

    ranges_df = df_in.loc[df_in["auto_dt"].notna() & df_in["next_manual_dt"].notna()].copy()
    ranges_df["from_dt"] = ranges_df["auto_dt"]
    ranges_df["to_dt"] = ranges_df["next_manual_dt"]
    ranges_df = ranges_df[ranges_df["to_dt"] > ranges_df["from_dt"]].copy()

    ranges_df["from_day"] = ranges_df["from_dt"].dt.strftime("%Y%m%d")
    ranges_df["from_time"] = ranges_df["from_dt"].dt.strftime("%H:%M:%S")
    ranges_df["to_day"] = ranges_df["to_dt"].dt.strftime("%Y%m%d")
    ranges_df["to_time"] = ranges_df["to_dt"].dt.strftime("%H:%M:%S")
    print("[INFO] ranges_df rows:", len(ranges_df))

    if ranges_df.empty:
        print("[WARN] ranges_df is empty. Stop.")
        return

    # ------------------------------------------------------------
    # 7) Count amount in a1_fct_vision_testlog_txt_processing_history (REMOTE)
    # ------------------------------------------------------------
    engine_remote = get_engine(DB_REMOTE)

    H_SCHEMA = "a1_fct_vision_testlog_txt_processing_history"
    H_TABLE = "fct_vision_testlog_txt_processing_history"

    with engine_remote.begin() as conn:
        cols = pd.read_sql(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = :s AND table_name = :t
                ORDER BY ordinal_position
            """),
            conn,
            params={"s": H_SCHEMA, "t": H_TABLE},
        )["column_name"].tolist()

    cols_set = set(cols)
    if "station" not in cols_set:
        raise ValueError(f"[ERROR] history 테이블에 station 컬럼이 없습니다. columns={cols}")

    if {"end_day", "end_time"}.issubset(cols_set):
        day_col, time_col = "end_day", "end_time"
    elif {"day", "end_time"}.issubset(cols_set):
        day_col, time_col = "day", "end_time"
    elif {"end_day", "time"}.issubset(cols_set):
        day_col, time_col = "end_day", "time"
    else:
        raise ValueError(f"[ERROR] 시간 컬럼 조합을 찾지 못했습니다. columns={cols}")

    sql_count = text(f"""
        SELECT COUNT(*)::bigint AS cnt
        FROM {H_SCHEMA}."{H_TABLE}"
        WHERE station = :station
          AND ({day_col}::text || ' ' || {time_col}::text)::timestamp
                BETWEEN :from_dt AND :to_dt
          AND {day_col}::int BETWEEN :from_day AND :to_day
    """)

    amounts = []
    with engine_remote.begin() as conn:
        for r in ranges_df.itertuples(index=False):
            cnt = conn.execute(
                sql_count,
                {
                    "station": r.station,
                    "from_dt": r.from_dt.to_pydatetime(),
                    "to_dt": r.to_dt.to_pydatetime(),
                    "from_day": int(r.from_day),
                    "to_day": int(r.to_day),
                },
            ).scalar_one()
            amounts.append(int(cnt))

    ranges_df["amount"] = amounts

    out7_df = (
        ranges_df[["station", "sparepart", "from_day", "from_time", "to_day", "to_time", "amount"]]
        .query("amount >= 1000")
        .sort_values(by=["station", "sparepart", "from_day", "from_time"])
        .reset_index(drop=True)
    )
    print("[INFO] out7_df (amount>=1000) rows:", len(out7_df))

    if out7_df.empty:
        print("[WARN] out7_df is empty after amount>=1000 filter. Stop.")
        return

    # ------------------------------------------------------------
    # 8) Percentiles by sparepart + plotly json
    # ------------------------------------------------------------
    df_amt = out7_df.copy()
    df_amt["amount"] = pd.to_numeric(df_amt["amount"], errors="coerce")
    df_amt = df_amt.dropna(subset=["sparepart", "amount"]).copy()

    rows = []
    for sp, g in df_amt.groupby("sparepart", sort=True):
        vals = g["amount"].astype(float).values
        if len(vals) == 0:
            continue

        q_min = np.min(vals)
        q10 = np.percentile(vals, 10)
        q25 = np.percentile(vals, 25)
        q50 = np.percentile(vals, 50)
        q75 = np.percentile(vals, 75)
        q_max = np.max(vals)

        fig = go.Figure()
        fig.add_trace(go.Box(y=vals, name=str(sp), boxpoints="outliers"))
        fig.update_layout(
            title=f"amount distribution - {sp}",
            xaxis_title="sparepart",
            yaxis_title="amount",
            showlegend=False,
            height=260,
            margin=dict(l=30, r=30, t=40, b=30),
        )

        rows.append(
            {
                "sparepart": sp,
                "min": floor_int(q_min),
                "p10": floor_int(q10),
                "p25": floor_int(q25),
                "p50": floor_int(q50),
                "p75": floor_int(q75),
                "max": floor_int(q_max),
                "plotly_graph": fig.to_json(),
            }
        )

    summary8_df = pd.DataFrame(rows).sort_values(by="sparepart").reset_index(drop=True)
    print("[INFO] summary8_df rows:", len(summary8_df))

    if summary8_df.empty:
        print("[WARN] summary8_df empty. Stop.")
        return

    # ------------------------------------------------------------
    # 9) Save summary8_df to LOCAL (UPSERT)
    # ------------------------------------------------------------
    TARGET_SCHEMA = "e3_sparepart_replacement"
    TARGET_TABLE = "sparepart_life_amount"

    with engine_local.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {TARGET_SCHEMA}"))
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {TARGET_SCHEMA}.{TARGET_TABLE} (
                    sparepart      TEXT PRIMARY KEY,
                    min            INTEGER,
                    p10            INTEGER,
                    p25            INTEGER,
                    p50            INTEGER,
                    p75            INTEGER,
                    max            INTEGER,
                    plotly_graph   TEXT
                )
                """
            )
        )

    upsert_sql = text(
        f"""
        INSERT INTO {TARGET_SCHEMA}.{TARGET_TABLE}
        (sparepart, min, p10, p25, p50, p75, max, plotly_graph)
        VALUES
        (:sparepart, :min, :p10, :p25, :p50, :p75, :max, :plotly_graph)
        ON CONFLICT (sparepart)
        DO UPDATE SET
            min          = EXCLUDED.min,
            p10          = EXCLUDED.p10,
            p25          = EXCLUDED.p25,
            p50          = EXCLUDED.p50,
            p75          = EXCLUDED.p75,
            max          = EXCLUDED.max,
            plotly_graph = EXCLUDED.plotly_graph
        """
    )

    with engine_local.begin() as conn:
        for r in summary8_df.itertuples(index=False):
            conn.execute(
                upsert_sql,
                {
                    "sparepart": r.sparepart,
                    "min": r.min,
                    "p10": r.p10,
                    "p25": r.p25,
                    "p50": r.p50,
                    "p75": r.p75,
                    "max": r.max,
                    "plotly_graph": r.plotly_graph,
                },
            )

    print(f"[INFO] Saved: {TARGET_SCHEMA}.{TARGET_TABLE} (UPSERT) 완료")


if __name__ == "__main__":
    _start_ts = time.time()
    _start_dt = datetime.now()
    print(f"[INFO] Script START: {_start_dt:%Y-%m-%d %H:%M:%S}")

    try:
        main()
    except Exception as e:
        print("[ERROR]", repr(e))
        sys.exit(1)
    finally:
        _end_ts = time.time()
        _end_dt = datetime.now()
        _elapsed = _end_ts - _start_ts
        print(f"[INFO] Script END  : {_end_dt:%Y-%m-%d %H:%M:%S}")
        print(f"[INFO] Elapsed    : {_elapsed:,.2f} sec ({_elapsed/60:,.2f} min)")
