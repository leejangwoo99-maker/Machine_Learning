# -*- coding: utf-8 -*-
"""
fct_feature_to_db.py  (완전 통합본: Cell2~Cell7-B + COPY 기반 Bulk Upsert)

목표
- c1_fct_detail.fct_detail 에서 (end_day BETWEEN DATE_FROM~DATE_TO) 그룹 단위로 로드(JSON agg)
- group_logs 펼쳐 df_final 생성
- (Cell6) contents 로그에서 step_description / set_up_or_test_ct 생성
  * START :: <step>  ~ 다음 START 직전 구간에서 "테스트 결과 : OK/NG"만 잡아내고
  * 해당 OK/NG 행에 대해 step_description=<step>, set_up_or_test_ct="TEST_CT" 로 부여
- (7-A) a2_fct_table.fct_table 에서 group 단위로 station/run_time 매칭 (exact 우선, contains fallback)
- (7-B) step 있는 행만 value/min/max/result 매칭
  * df_out.step_description 의 선두 'pd_'/'nonpd_' 제거 후 step_key로 사용
  * fct_table.step_description==step_key 인 행의 value/min/max/result를 가져와 df_out에 채움
- 최종 df_feature 생성 후 f_database.fct_database에 COPY 기반 Bulk Upsert
  * 이번 "빅데이터 적재"는 대량 처리 최적화
  * 실무용(무한루프 2분 window, <=1000 rows)로도 재사용 가능하도록 함수화

주의
- COPY NULL 표기는 반드시 '\\N' (unicodeescape 에러 방지)
- psycopg2 execute_values 대신 COPY + staging + merge 사용 (대량 안정)

권장 DB 설정(대량 적재 시)
- staging은 UNLOGGED
- (옵션) session-level synchronous_commit=off
"""

import os
import io
import csv
import time
import urllib.parse
import numpy as np
import pandas as pd
from datetime import date, datetime
from sqlalchemy import create_engine, text

# ============================================================
# 설정
# ============================================================

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# 소스 1) 디테일 로그
TARGET_SCHEMA = "c1_fct_detail"
TARGET_TABLE  = "fct_detail"

# 날짜 (빅데이터 적재 시 범위 확장)
DATE_FROM = date(2025, 11, 30)
DATE_TO   = date(2025, 11, 30)

# 소스 2) FCT 테이블
SRC_SCHEMA = "a2_fct_table"
SRC_TABLE  = "fct_table"

# 목적지
OUT_SCHEMA = "f_database"
OUT_TABLE  = "fct_database"

# 성능/메모리
COPY_CHUNK_ROWS = 200_000     # staging 적재 chunk
PANDAS_FLOAT_DTYPE = "float64"

# ============================================================
# DB 연결
# ============================================================

def get_engine(cfg):
    user = cfg["user"]
    pw = urllib.parse.quote_plus(cfg["password"])
    host = cfg["host"]
    port = cfg["port"]
    dbname = cfg["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{dbname}?connect_timeout=5"
    return create_engine(conn_str, pool_pre_ping=True)

engine = get_engine(DB_CONFIG)
print("[OK] engine ready")


# ============================================================
# 유틸
# ============================================================

def now_ts():
    return datetime.now()

def fmt_hms(sec: float) -> str:
    sec = float(sec)
    mm = int(sec // 60)
    ss = int(sec % 60)
    hh = mm // 60
    mm = mm % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"

def ensure_cols(df, cols_with_default):
    for c, default in cols_with_default.items():
        if c not in df.columns:
            df[c] = default
    return df

def normalize_barcode(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\s+", "", regex=True)

def to_end_day_text(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, errors="coerce").dt.strftime("%Y%m%d")

def to_end_time_text(s: pd.Series) -> pd.Series:
    return s.astype(str).str.replace(r"\D", "", regex=True).str.zfill(6)

def strip_step_prefix(step_series: pd.Series) -> pd.Series:
    # step_description 선두 pd_/nonpd_ 제거 + trim
    return (
        step_series.astype("string")
        .str.replace(r"^(?:pd_|nonpd_)", "", regex=True)
        .str.strip()
    )


# ============================================================
# Cell2) group 단위 JSON 로드
# ============================================================

def load_groups(engine, date_from: date, date_to: date) -> pd.DataFrame:
    sql = f"""
    WITH base AS (
        SELECT
            barcode_information,
            remark,
            end_day,
            end_time,
            contents,
            test_ct,
            test_time,
            file_path
        FROM {TARGET_SCHEMA}.{TARGET_TABLE}
        WHERE end_day BETWEEN :date_from AND :date_to
    ),
    mes_groups AS (
        SELECT DISTINCT barcode_information, end_day, end_time
        FROM base
        WHERE contents ILIKE '%MES%'
    ),
    filtered AS (
        SELECT b.*
        FROM base b
        WHERE NOT EXISTS (
            SELECT 1
            FROM mes_groups m
            WHERE m.barcode_information = b.barcode_information
              AND m.end_day = b.end_day
              AND m.end_time = b.end_time
        )
    )
    SELECT
        barcode_information,
        remark,
        end_day,
        end_time,
        jsonb_agg(
            jsonb_build_object(
                'contents', contents,
                'test_ct', test_ct,
                'test_time', test_time,
                'file_path', file_path
            )
            ORDER BY test_time
        ) AS group_logs
    FROM filtered
    GROUP BY barcode_information, remark, end_day, end_time
    ORDER BY end_day ASC, end_time ASC
    """
    with engine.connect() as conn:
        df_groups = pd.read_sql(text(sql), conn, params={"date_from": date_from, "date_to": date_to})

    print("[OK] loaded groups:", len(df_groups))
    if df_groups.empty:
        raise ValueError("[STOP] df_groups가 0건입니다. DATE_FROM/DATE_TO 또는 원본 테이블 조건을 확인하세요.")

    # 로드 검증
    end_day_u = df_groups["end_day"].astype(str).unique().tolist()
    end_time_u = df_groups["end_time"].astype(str).unique().tolist()
    print("[CHECK] df_groups end_day unique(top20):", end_day_u[:20])
    print("[CHECK] df_groups end_time unique(top20):", end_time_u[:20])

    expected_from = str(date_from)
    expected_to = str(date_to)
    bad_days = [d for d in end_day_u if (d < expected_from or d > expected_to)]
    if bad_days:
        raise ValueError(
            "[STOP] Cell2 로드 결과에 DATE_FROM/DATE_TO 범위 밖 end_day가 섞였습니다.\n"
            f"- DATE_FROM={expected_from}, DATE_TO={expected_to}\n"
            f"- out_of_range end_day sample: {bad_days[:10]}"
        )

    return df_groups


# ============================================================
# Cell3~5) group 부여 + 펼치기 + 기본 df_final
# ============================================================

def explode_group_logs(df_groups: pd.DataFrame) -> pd.DataFrame:
    df = df_groups.copy().reset_index(drop=True)
    df["group"] = df.index + 1
    print("[OK] groups:", len(df))

    rows = []
    for _, r in df.iterrows():
        logs = r["group_logs"]
        for item in logs:
            rows.append({
                "group": r["group"],
                "barcode_information": r["barcode_information"],
                "remark": r["remark"],
                "end_day": r["end_day"],
                "end_time": r["end_time"],
                "contents": item.get("contents"),
                "test_ct": item.get("test_ct"),
                "test_time": item.get("test_time"),
                "file_path": item.get("file_path"),
            })

    df_final = pd.DataFrame(rows)
    print("[OK] df_final rows:", len(df_final))
    if df_final.empty:
        raise ValueError("[STOP] df_final이 0건입니다. group_logs가 비었거나 로직을 확인하세요.")

    # 컬럼 순서 고정
    final_cols = [
        "group","barcode_information","remark","end_day","end_time",
        "contents","test_ct","test_time","file_path"
    ]
    df_final = df_final[final_cols].copy()
    return df_final


# ============================================================
# Cell6) step_description / set_up_or_test_ct 생성
# ============================================================

def build_step_from_contents(df_final: pd.DataFrame) -> pd.DataFrame:
    """
    PD / Non-PD 누적 CT 규칙 최종 안전본

    핵심:
    - test_time을 datetime으로 파싱하지 않는다.
    - SQL에서 이미 ORDER BY test_time 된 순서를 그대로 신뢰한다.
    - group별 원본 순서대로 누적 CT 블럭을 만든다.
    """

    df = df_final.copy().reset_index(drop=True)

    # 원본 순서 보존용 시퀀스
    df["_seq"] = np.arange(len(df), dtype=np.int64)
    df = df.sort_values(["group", "_seq"], ascending=[True, True]).reset_index(drop=True)

    df["step_description"] = pd.Series(pd.NA, index=df.index, dtype="string")
    df["set_up_or_test_ct"] = np.nan

    OK_TOKEN = "테스트 결과 : OK"
    NG_TOKEN = "테스트 결과 : NG"
    START_PREFIX = "START ::"

    for g, idx in df.groupby("group", sort=False).groups.items():
        cur_step = None
        block_sum = 0.0

        for i in idx:
            ct = pd.to_numeric(df.at[i, "test_ct"], errors="coerce")
            if not pd.isna(ct):
                block_sum += ct

            c = str(df.at[i, "contents"])

            # START 단계 갱신
            if c.startswith(START_PREFIX):
                cur_step = c.replace(START_PREFIX, "", 1).strip()
                continue

            # OK / NG 시점 = 하나의 Step 완료 시점
            if (OK_TOKEN in c) or (NG_TOKEN in c):
                df.at[i, "step_description"] = cur_step
                df.at[i, "set_up_or_test_ct"] = block_sum
                block_sum = 0.0   # 다음 Step 블럭으로 리셋

    df = df.drop(columns=["_seq"])
    print("[OK] Cell6 누적 CT rows:", df["set_up_or_test_ct"].notna().sum())
    return df


# ============================================================
# fct_table 로드 (매칭용)
# ============================================================

def load_fct_table(engine, days_yyyymmdd: list[str], like_patterns: list[str]) -> pd.DataFrame:
    sql_fct = f"""
    SELECT
        barcode_information,
        remark,
        station,
        end_day,
        end_time,
        run_time,
        step_description,
        value,
        min,
        max,
        result
    FROM {SRC_SCHEMA}.{SRC_TABLE}
    WHERE end_day = ANY(:days)
      AND barcode_information ILIKE ANY(:patterns)
    """
    with engine.connect() as conn:
        df_fct = pd.read_sql(text(sql_fct), conn, params={"days": days_yyyymmdd, "patterns": like_patterns})

    print("[OK] df_fct loaded:", len(df_fct))
    if df_fct.empty:
        print("[WARN] df_fct is empty (조건이 너무 빡빡하거나 end_day/pattern 불일치).")
        return df_fct

    # 정규화
    df_fct["barcode_norm_db"] = normalize_barcode(df_fct["barcode_information"])
    df_fct["end_day_text"] = df_fct["end_day"].astype(str)
    df_fct["end_time_int"] = pd.to_numeric(
        df_fct["end_time"].astype(str).str.replace(r"\D", "", regex=True).str.zfill(6),
        errors="coerce"
    ).fillna(-1).astype("int64")

    df_fct["run_time"] = pd.to_numeric(df_fct["run_time"], errors="coerce")

    # step_key는 원문 step_description trim
    df_fct["step_key"] = df_fct["step_description"].astype(str).str.strip()

    for c in ["value","min","max"]:
        df_fct[c] = pd.to_numeric(df_fct[c], errors="coerce")

    # result는 문자로 보존
    if "result" in df_fct.columns:
        df_fct["result"] = df_fct["result"].astype("string")
    else:
        df_fct["result"] = pd.Series(pd.NA, index=df_fct.index, dtype="string")

    return df_fct


# ============================================================
# 7-A) station/run_time group 매칭
# ============================================================

def match_station_runtime(df_out: pd.DataFrame, df_fct: pd.DataFrame) -> pd.DataFrame:
    if df_fct.empty:
        df_out["station"] = pd.NA
        df_out["run_time"] = np.nan
        return df_out

    # group 단위 키
    df_keys = (
        df_out[["group","barcode_norm","end_day_text"]]
        .dropna(subset=["group","barcode_norm","end_day_text"])
        .drop_duplicates()
        .copy()
    )
    print("[OK] df_keys(groups):", len(df_keys))

    days = sorted(df_keys["end_day_text"].unique().tolist())

    # end_day별 정렬 + exact dict 생성
    fct_by_day = {}
    for d, g in df_fct.groupby("end_day_text", sort=False):
        gg = g.sort_values("end_time_int", ascending=False).copy()
        gg1 = gg.drop_duplicates(subset=["barcode_norm_db"], keep="first")
        fct_by_day[d] = gg1.set_index("barcode_norm_db")[["station","run_time"]]

    picked_rows = []
    miss = 0
    fallback_used = 0

    # fallback contains용 캐시
    day_chunk_cache = {
        d: df_fct[df_fct["end_day_text"] == d][["barcode_norm_db","station","run_time","end_time_int"]]
        for d in days
    }

    for r in df_keys.itertuples(index=False):
        g = r.group
        b = r.barcode_norm
        d = r.end_day_text

        day_map = fct_by_day.get(d)
        if day_map is None:
            miss += 1
            continue

        # 1) exact
        if b in day_map.index:
            row = day_map.loc[b]
            picked_rows.append({"group": g, "station": row["station"], "run_time": row["run_time"]})
            continue

        # 2) contains fallback
        chunk = day_chunk_cache.get(d)
        if chunk is None or chunk.empty:
            miss += 1
            continue

        m = chunk["barcode_norm_db"].str.contains(b, regex=False, na=False)
        sub = chunk.loc[m, ["station","run_time","end_time_int"]]
        if sub.empty:
            miss += 1
            continue

        best = sub.sort_values("end_time_int", ascending=False).iloc[0]
        picked_rows.append({"group": g, "station": best["station"], "run_time": best["run_time"]})
        fallback_used += 1

    print("[OK] miss groups:", miss)
    print("[OK] fallback_used:", fallback_used)

    df_group_map = pd.DataFrame(picked_rows).drop_duplicates(subset=["group"], keep="first")
    df_out = df_out.merge(df_group_map, how="left", on="group")
    print("[OK] station matched rows:", df_out["station"].notna().sum(), "/", len(df_out))
    return df_out


# ============================================================
# 7-B) value/min/max/result 매칭 (step 있는 행만)
# ============================================================

def match_value_min_max_result(df_out: pd.DataFrame, df_fct: pd.DataFrame) -> pd.DataFrame:
    # 결과 컬럼 확보
    for c in ["value","min","max","result"]:
        if c not in df_out.columns:
            df_out[c] = (pd.Series(pd.NA, index=df_out.index, dtype="string") if c == "result" else np.nan)

    if df_fct.empty:
        print("[WARN] df_fct empty -> skip 7-B")
        return df_out

    # df_out step_key: step_description에서 pd_/nonpd_ 제거 후 사용
    df_out["step_key"] = pd.Series(pd.NA, index=df_out.index, dtype="string")
    m_step = df_out["step_description"].notna()
    df_out.loc[m_step, "step_key"] = strip_step_prefix(df_out.loc[m_step, "step_description"])

    # step 있는 행만
    need_mask = df_out["step_key"].notna() & df_out["station"].notna() & df_out["end_day_text"].notna() & df_out["barcode_norm"].notna()
    df_need = df_out.loc[need_mask, ["group","barcode_norm","end_day_text","station","step_key"]].copy()
    print("[OK] df_need(step rows):", len(df_need), "/", len(df_out))

    if df_need.empty:
        print("[WARN] step rows=0 -> skip 7-B")
        return df_out

    # (end_day, station)별 dict: (barcode, step)->(value,min,max,result) 최신 end_time 우선
    fct_vs_map = {}
    base = df_fct.dropna(subset=["end_day_text","station","barcode_norm_db","step_key"])
    for (d, st), g in base.groupby(["end_day_text","station"], sort=False):
        gg = g.sort_values("end_time_int", ascending=False)
        gg = gg.drop_duplicates(subset=["barcode_norm_db","step_key"], keep="first")
        fct_vs_map[(d, st)] = gg.set_index(["barcode_norm_db","step_key"])[["value","min","max","result"]]

    # fallback contains용 캐시(필요 최소)
    fct_dayst_cache = {}
    for (d, st), g in base.groupby(["end_day_text","station"], sort=False):
        fct_dayst_cache[(d, st)] = g[["barcode_norm_db","step_key","value","min","max","result","end_time_int"]]

    vals = np.full((len(df_need), 3), np.nan, dtype=PANDAS_FLOAT_DTYPE)
    res = np.full((len(df_need),), None, dtype=object)

    keys_day = df_need["end_day_text"].to_numpy()
    keys_st  = df_need["station"].to_numpy()
    keys_bc  = df_need["barcode_norm"].to_numpy()
    keys_sp  = df_need["step_key"].astype(str).to_numpy()

    miss2 = 0
    fallback2 = 0

    for i in range(len(df_need)):
        d = keys_day[i]
        st = keys_st[i]
        bc = keys_bc[i]
        sp = keys_sp[i]

        mp = fct_vs_map.get((d, st))
        if mp is not None and (bc, sp) in mp.index:
            row = mp.loc[(bc, sp)]
            vals[i, 0] = row["value"]
            vals[i, 1] = row["min"]
            vals[i, 2] = row["max"]
            res[i] = None if pd.isna(row["result"]) else str(row["result"])
            continue

        # contains fallback
        chunk = fct_dayst_cache.get((d, st))
        if chunk is None or chunk.empty:
            miss2 += 1
            continue

        m = chunk["barcode_norm_db"].str.contains(bc, regex=False, na=False) & (chunk["step_key"] == sp)
        sub = chunk.loc[m, ["value","min","max","result","end_time_int"]]
        if sub.empty:
            miss2 += 1
            continue

        best = sub.sort_values("end_time_int", ascending=False).iloc[0]
        vals[i, 0] = best["value"]
        vals[i, 1] = best["min"]
        vals[i, 2] = best["max"]
        res[i] = None if pd.isna(best["result"]) else str(best["result"])
        fallback2 += 1

    print("[OK] miss(step) rows:", miss2, "/", len(df_need))
    print("[OK] fallback_used(step):", fallback2)

    # df_need에 결과
    df_need["value"] = vals[:, 0]
    df_need["min"]   = vals[:, 1]
    df_need["max"]   = vals[:, 2]
    df_need["result"] = pd.Series(res, dtype="string")

    # df_out에 merge
    df_out = df_out.merge(
        df_need[["group","barcode_norm","end_day_text","station","step_key","value","min","max","result"]],
        how="left",
        on=["group","barcode_norm","end_day_text","station","step_key"],
        suffixes=("", "_new"),
    )

    for c in ["value","min","max","result"]:
        if f"{c}_new" in df_out.columns:
            if c == "result":
                df_out[c] = df_out[c].fillna(df_out[f"{c}_new"])
            else:
                df_out[c] = df_out[c].fillna(df_out[f"{c}_new"])
            df_out = df_out.drop(columns=[f"{c}_new"])

    print("[OK] value notna:", df_out["value"].notna().sum(), "/", len(df_out))
    print("[OK] result notna:", df_out["result"].notna().sum(), "/", len(df_out))
    return df_out


# ============================================================
# 스키마/테이블/인덱스/옵션
# ============================================================

def ensure_schema_and_table(conn):
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {OUT_SCHEMA};"))

    # 메인 테이블 (빅데이터 적재 목적: UNLOGGED 권장)
    # PK 요구사항 반영:
    # - end_day(단일/최우선) + barcode_information + end_time 순으로 식별
    # - step 단위 중복 방지 위해 step_description + test_time + contents까지 포함
    #
    # 실제로 "머신러닝 학습용"이면 중복이 조금 있어도 큰 문제는 아니지만,
    # 향후 실무(1000줄) 무한루프에서는 중복 방지가 중요하므로 아래 PK 권장.
    conn.execute(text(f"""
    CREATE UNLOGGED TABLE IF NOT EXISTS {OUT_SCHEMA}.{OUT_TABLE} (
        end_day DATE NOT NULL,
        barcode_information TEXT NOT NULL,
        end_time TIME NOT NULL,
        test_time TEXT NOT NULL,
        contents TEXT NOT NULL,

        "group" BIGINT,
        station TEXT,
        remark TEXT,
        run_time DOUBLE PRECISION,
        step_description TEXT,
        set_up_or_test_ct DOUBLE PRECISION,
        value DOUBLE PRECISION,
        min DOUBLE PRECISION,
        max DOUBLE PRECISION,
        result TEXT,
        test_ct DOUBLE PRECISION,
        file_path TEXT,
        updated_at TIMESTAMPTZ DEFAULT now(),

        PRIMARY KEY (end_day, barcode_information, end_time, test_time, contents)
    );
    """))

    # 조회/학습용 보조 인덱스
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_day_bc_time ON {OUT_SCHEMA}.{OUT_TABLE} (end_day, barcode_information, end_time);"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_station_day ON {OUT_SCHEMA}.{OUT_TABLE} (station, end_day);"))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{OUT_TABLE}_step_day ON {OUT_SCHEMA}.{OUT_TABLE} (step_description, end_day);"))


def prepare_staging(conn, stg_table: str):
    conn.execute(text(f"DROP TABLE IF EXISTS {stg_table};"))
    conn.execute(text(f"""
    CREATE UNLOGGED TABLE {stg_table} (
        end_day DATE,
        barcode_information TEXT,
        end_time TIME,
        test_time TEXT,
        contents TEXT,

        "group" BIGINT,
        station TEXT,
        remark TEXT,
        run_time DOUBLE PRECISION,
        step_description TEXT,
        set_up_or_test_ct DOUBLE PRECISION,
        value DOUBLE PRECISION,
        min DOUBLE PRECISION,
        max DOUBLE PRECISION,
        result TEXT,
        test_ct DOUBLE PRECISION,
        file_path TEXT
    );
    """))
    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_stg_day_bc_time ON {stg_table} (end_day, barcode_information, end_time);"))


# ============================================================
# COPY 기반 Bulk Upsert
# ============================================================

def df_to_copy_buffer(df: pd.DataFrame, cols: list[str]) -> io.StringIO:
    # """
    # PostgreSQL COPY FROM STDIN (TEXT/TSV) 버퍼 생성
    # - NULL 토큰은 반드시 \N (백슬래시 1개)
    # - 탭/개행/백슬래시를 COPY 규칙에 맞게 이스케이프
    # """
    NULL = r"\N"  # 실제 텍스트는 \N (백슬래시 1개)
    buf = io.StringIO()

    arr = df[cols].to_numpy(dtype=object, copy=False)

    for row in arr:
        out_fields = []
        for v in row:
            # None/NaN -> NULL 토큰
            if v is None:
                out_fields.append(NULL)
                continue
            try:
                if pd.isna(v):
                    out_fields.append(NULL)
                    continue
            except Exception:
                pass

            # 숫자
            if isinstance(v, (np.integer,)):
                s = str(int(v))
            elif isinstance(v, (np.floating,)):
                s = str(float(v))
            else:
                s = str(v)

            # COPY TEXT 규칙: \, 탭, 개행 이스케이프
            s = s.replace("\\", "\\\\")   # \  -> \\
            s = s.replace("\t", "\\t")    # tab -> \t
            s = s.replace("\r", "\\r")    # CR  -> \r
            s = s.replace("\n", "\\n")    # LF  -> \n

            out_fields.append(s)

        buf.write("\t".join(out_fields) + "\n")

    buf.seek(0)
    return buf


def bulk_upsert_copy(engine, df_feature: pd.DataFrame):
    """
    1) staging UNLOGGED 생성
    2) COPY로 staging 적재(대량)
    3) MERGE(INSERT ... ON CONFLICT DO UPDATE)로 본 테이블 upsert
    4) staging drop
    """
    stg_table = f"{OUT_SCHEMA}._stg_{OUT_TABLE}_{int(time.time())}"

    # 타입 정리 (COPY 안정)
    dfw = df_feature.copy()

    # end_day: date
    dfw["end_day"] = pd.to_datetime(dfw["end_day"], errors="coerce").dt.date

    # end_time: TIME
    # - 'HH:MM:SS' 또는 datetime.time 모두 안전하게 처리
    dfw["end_time"] = pd.to_datetime(dfw["end_time"].astype(str), format="%H:%M:%S", errors="coerce").dt.time

    # 숫자
    for c in ["group","run_time","set_up_or_test_ct","value","min","max","test_ct"]:
        if c in dfw.columns:
            dfw[c] = pd.to_numeric(dfw[c], errors="coerce")

    # 문자열
    for c in ["barcode_information","station","remark","contents","step_description","result","test_time","file_path"]:
        if c in dfw.columns:
            dfw[c] = dfw[c].astype(object)

    # 최종 컬럼
    cols = [
        "end_day","barcode_information","end_time","test_time","contents",
        "group","station","remark","run_time","step_description","set_up_or_test_ct",
        "value","min","max","result","test_ct","file_path"
    ]
    dfw = ensure_cols(dfw, {c: None for c in cols})
    dfw = dfw[cols].copy()
    dfw = dfw.where(pd.notna(dfw), None)

    with engine.begin() as conn:
        # 대량 적재 시 세션 옵션 (원복은 커넥션 종료로 처리)
        conn.execute(text("SET LOCAL synchronous_commit TO off;"))
        conn.execute(text("SET LOCAL work_mem TO '256MB';"))
        conn.execute(text("SET LOCAL maintenance_work_mem TO '512MB';"))

        prepare_staging(conn, stg_table)

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            total = len(dfw)
            print("[COPY] total rows:", total)

            def pg_ident(col: str) -> str:
                # 필요한 경우에만 "..." 로 감싸기
                return '"group"' if col == "group" else col

            copy_cols_sql = ", ".join(pg_ident(c) for c in cols)
            copy_sql = f"COPY {stg_table} ({copy_cols_sql}) FROM STDIN WITH (FORMAT text, DELIMITER E'\\t', NULL '\\N');"

            for start in range(0, total, COPY_CHUNK_ROWS):
                part = dfw.iloc[start:start + COPY_CHUNK_ROWS]
                buf = df_to_copy_buffer(part, cols)
                cur.copy_expert(copy_sql, buf)
                raw.commit()
                done = min(start + COPY_CHUNK_ROWS, total)
                print(f"[COPY] loaded staging {done}/{total}")

        # MERGE/UPSERT
        with engine.begin() as conn:
            conn.execute(text("SET LOCAL synchronous_commit TO off;"))

            upsert_sql = f"""
            INSERT INTO {OUT_SCHEMA}.{OUT_TABLE} (
                end_day, barcode_information, end_time, test_time, contents,
                "group", station, remark, run_time, step_description, set_up_or_test_ct,
                value, min, max, result, test_ct, file_path
            )
            SELECT
                s.end_day, s.barcode_information, s.end_time, s.test_time, s.contents,
                s."group", s.station, s.remark, s.run_time, s.step_description, s.set_up_or_test_ct,
                s.value, s.min, s.max, s.result, s.test_ct, s.file_path
            FROM (
                SELECT DISTINCT ON (end_day, barcode_information, end_time, test_time, contents)
                    *
                FROM {stg_table}
                -- 중복이 있을 때 어떤 행을 살릴지 결정 규칙
                -- 1) station 있는 것 우선
                -- 2) step_description 있는 것 우선
                -- 3) value 있는 것 우선
                ORDER BY
                    end_day, barcode_information, end_time, test_time, contents,
                    (station IS NOT NULL) DESC,
                    (step_description IS NOT NULL) DESC,
                    (value IS NOT NULL) DESC
            ) s
            ON CONFLICT (end_day, barcode_information, end_time, test_time, contents)
            DO UPDATE SET
                "group" = EXCLUDED."group",
                station = EXCLUDED.station,
                remark = EXCLUDED.remark,
                run_time = EXCLUDED.run_time,
                step_description = EXCLUDED.step_description,
                set_up_or_test_ct = EXCLUDED.set_up_or_test_ct,
                value = EXCLUDED.value,
                min = EXCLUDED.min,
                max = EXCLUDED.max,
                result = EXCLUDED.result,
                test_ct = EXCLUDED.test_ct,
                file_path = EXCLUDED.file_path,
                updated_at = now()
            ;
            """
            conn.execute(text(upsert_sql))
            conn.execute(text(f"DROP TABLE IF EXISTS {stg_table};"))

    finally:
        raw.close()


# ============================================================
# 메인 실행
# ============================================================

def main():
    run_start = now_ts()
    print(f"[RUN] start: {run_start:%Y-%m-%d %H:%M:%S}")

    try:
        # 1) ensure table
        with engine.begin() as conn:
            ensure_schema_and_table(conn)
        print("[OK] schema/table ensured:", f"{OUT_SCHEMA}.{OUT_TABLE}")

        # 2) load groups
        df_groups = load_groups(engine, DATE_FROM, DATE_TO)

        # 3) explode
        df_final = explode_group_logs(df_groups)

        # 4) Cell6 step 생성
        df_step = build_step_from_contents(df_final)

        # 5) df_out 준비
        df_out = df_step.copy()
        df_out["barcode_norm"] = normalize_barcode(df_out["barcode_information"])
        df_out["end_day_text"] = to_end_day_text(df_out["end_day"])
        df_out["end_time_text"] = to_end_time_text(df_out["end_time"])

        # 6) fct_table 로드 (days/patterns)
        df_keys = (
            df_out[["group","barcode_norm","end_day_text"]]
            .dropna()
            .drop_duplicates()
            .copy()
        )
        days = sorted(df_keys["end_day_text"].unique().tolist())
        barcodes = df_keys["barcode_norm"].unique().tolist()
        like_patterns = [f"%{b}%" for b in barcodes if b and str(b).lower() != "nan"]

        df_fct = load_fct_table(engine, days, like_patterns)

        # 7-A station/run_time
        df_out = match_station_runtime(df_out, df_fct)

        # 7-B value/min/max/result
        df_out = match_value_min_max_result(df_out, df_fct)

        # 8) 최종 df_feature 컬럼 순서
        final_cols = [
            "group",
            "barcode_information",
            "station",
            "remark",
            "end_day",
            "end_time",
            "run_time",
            "contents",
            "step_description",
            "set_up_or_test_ct",
            "value",
            "min",
            "max",
            "result",
            "test_ct",
            "test_time",
            "file_path",
        ]
        df_out = ensure_cols(df_out, {c: (pd.Series(pd.NA, index=df_out.index, dtype="string") if c in ["station","remark","step_description","result"] else np.nan) for c in final_cols})
        df_feature = df_out[final_cols].copy()

        print("[OK] df_feature rows:", len(df_feature), "/ groups:", df_feature["group"].nunique())
        print("[OK] filled station:", df_feature["station"].notna().sum())
        print("[OK] filled value:", df_feature["value"].notna().sum())
        print("[OK] filled result:", df_feature["result"].notna().sum())
        print(df_feature.head(5).to_string(index=False))

        # 9) COPY 기반 bulk upsert
        bulk_upsert_copy(engine, df_feature)

        run_end = now_ts()
        elapsed = (run_end - run_start).total_seconds()
        print(f"[RUN] end:   {run_end:%Y-%m-%d %H:%M:%S}")
        print(f"[RUN] took:  {elapsed:.1f}s ({fmt_hms(elapsed)})")
        print("[DONE] saved to DB:", f"{OUT_SCHEMA}.{OUT_TABLE}")

    except Exception as e:
        run_end = now_ts()
        elapsed = (run_end - run_start).total_seconds()
        print(f"[RUN] end:   {run_end:%Y-%m-%d %H:%M:%S}")
        print(f"[RUN] took:  {elapsed:.1f}s ({fmt_hms(elapsed)})")
        print("[ERROR]", repr(e))
        raise


if __name__ == "__main__":
    main()
