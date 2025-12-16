# fct_detail_loader.py
# -*- coding: utf-8 -*-

import os
import re
import time
from pathlib import Path
from datetime import date, timedelta, datetime
from multiprocessing import Pool, cpu_count, freeze_support

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# =========================
# 고정 경로 (요구사항)
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

# 날짜 고정 (요구사항)
START_DATE = date(2025, 10, 1)
END_DATE   = date(2025, 12, 19)

# 매핑 CSV (C:\data 기준)
MAP_PD  = Path(r"C:\data\PD.csv")
MAP_NPD = Path(r"C:\data\Non-PD.csv")

# =========================
# DB 설정 (요구사항)
# =========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# 타겟 스키마/테이블 (요구사항: fct_detail로 넣기)
TGT_SCHEMA = "c1_fct_detail"
TGT_TABLE  = "fct_detail"

# 소스(매칭) 스키마/테이블
SRC_SCHEMA = "a2_fct_table"
SRC_TABLE  = "fct_table"

# 성능/인코딩
ANSI_LOG_ENCODING = "cp949"
NUM_WORKERS = 2                 # ✅ 멀티프로세스 2개 고정

# 결과 라인 패턴
OK_PAT = re.compile(r"테스트\s*결과\s*:\s*OK")
NG_PAT = re.compile(r"테스트\s*결과\s*:\s*NG")


# =========================
# DB 유틸
# =========================
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_table():
    """스키마/테이블 없으면 생성 + file_path 인덱스 생성"""
    create_schema_sql = sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(TGT_SCHEMA))

    create_table_sql = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {}.{} (
        id BIGSERIAL PRIMARY KEY,
        "group" INTEGER,
        barcode_information TEXT,
        station TEXT,
        contents TEXT,
        "time" TEXT,
        ct NUMERIC(10,2),
        result TEXT,
        test_item TEXT,
        problem1 TEXT,
        problem2 TEXT,
        problem3 TEXT,
        problem4 TEXT,
        end_day TEXT,
        end_time TEXT,
        remark TEXT,
        file_path TEXT,
        processed_at TIMESTAMP
    );
    """).format(sql.Identifier(TGT_SCHEMA), sql.Identifier(TGT_TABLE))

    idx_sql = sql.SQL("""
        CREATE INDEX IF NOT EXISTS {} ON {}.{} (file_path);
    """).format(
        sql.Identifier(f"idx_{TGT_TABLE}_file_path"),
        sql.Identifier(TGT_SCHEMA),
        sql.Identifier(TGT_TABLE),
    )

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(create_schema_sql)
            cur.execute(create_table_sql)
            cur.execute(idx_sql)
        conn.commit()


def fetch_existing_file_paths(file_paths):
    """
    중복 방지(요구사항):
    SELECT DISTINCT file_path 방식으로 신규 파일만 처리
    """
    if not file_paths:
        return set()

    q = sql.SQL("""
        SELECT DISTINCT file_path
        FROM {}.{}
        WHERE file_path = ANY(%s);
    """).format(sql.Identifier(TGT_SCHEMA), sql.Identifier(TGT_TABLE))

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(q, (list(file_paths),))
            return set(r[0] for r in cur.fetchall())


def fetch_fct_table_map(barcodes, chunk_size=5000):
    """barcode_information 기준으로 station/end_day/end_time 매칭"""
    if not barcodes:
        return {}

    q = sql.SQL("""
        SELECT barcode_information, station, end_day, end_time
        FROM {}.{}
        WHERE barcode_information = ANY(%s);
    """).format(sql.Identifier(SRC_SCHEMA), sql.Identifier(SRC_TABLE))

    out = {}
    with get_conn() as conn:
        with conn.cursor() as cur:
            for i in range(0, len(barcodes), chunk_size):
                chunk = barcodes[i:i+chunk_size]
                cur.execute(q, (chunk,))
                for bc, st, ed, et in cur.fetchall():
                    out[bc] = (st, ed, et)
    return out


def merge_station_fields(df: pd.DataFrame) -> pd.DataFrame:
    """df에 station/end_day/end_time 채우기 (end_day는 str로 강제)"""
    if df is None or df.empty:
        return df
    if "barcode_information" not in df.columns:
        raise KeyError("df에 'barcode_information' 컬럼이 없습니다.")

    barcodes = df["barcode_information"].dropna().unique().tolist()
    mp = fetch_fct_table_map(barcodes)

    st_list, ed_list, et_list = [], [], []
    for bc in df["barcode_information"].tolist():
        v = mp.get(bc)
        if v is None:
            st_list.append(None)
            ed_list.append(None)
            et_list.append(None)
        else:
            st_list.append(v[0])
            ed_list.append(str(v[1]) if v[1] is not None else None)  # ✅ 핵심
            et_list.append(v[2])

    df["station"]  = st_list
    df["end_day"]  = ed_list
    df["end_time"] = et_list
    return df

def insert_df(df: pd.DataFrame, batch_size=10000):
    if df is None or df.empty:
        print("[INFO] df is empty. skip insert.")
        return 0

    ensure_schema_table()

    cols = [
        "group", "barcode_information", "station",
        "contents", "time", "ct",
        "result", "test_item",
        "problem1", "problem2", "problem3", "problem4",
        "end_day", "end_time",
        "remark", "file_path", "processed_at"
    ]

    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise KeyError(f"[ERROR] df에 필요한 컬럼이 없습니다: {missing}")

    values = [tuple(x) for x in df[cols].to_numpy()]

    insert_sql = f"""
        INSERT INTO {TGT_SCHEMA}.{TGT_TABLE}
        ("group", barcode_information, station,
         contents, "time", ct,
         result, test_item,
         problem1, problem2, problem3, problem4,
         end_day, end_time,
         remark, file_path, processed_at)
        VALUES %s;
    """

    t0 = time.time()
    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=batch_size)
        conn.commit()
    elapsed = time.time() - t0

    print(f"[INFO] insert OK: {len(df):,} rows into {TGT_SCHEMA}.{TGT_TABLE} ({elapsed:.2f}s)")
    return len(df)


# =========================
# 매핑 CSV 로딩
# =========================
def read_csv_flex(path: Path) -> pd.DataFrame:
    try:
        return pd.read_csv(path, encoding="cp949")
    except Exception:
        return pd.read_csv(path, encoding="utf-8-sig")


def normalize_mapping(df: pd.DataFrame, remark_value: str) -> pd.DataFrame:
    df = df.copy()

    # seq 컬럼이 없으면 1..N
    if "seq" not in df.columns:
        df.insert(0, "seq", np.arange(1, len(df) + 1, dtype=int))

    need_cols = ["seq", "problem1", "problem2", "problem3", "problem4", "test_item"]
    for c in need_cols:
        if c not in df.columns:
            df[c] = None

    df["seq"] = pd.to_numeric(df["seq"], errors="coerce").astype("Int64")
    df["remark"] = remark_value
    return df[need_cols + ["remark"]]


def load_mapping_df() -> pd.DataFrame:
    map_pd  = normalize_mapping(read_csv_flex(MAP_PD),  "PD")
    map_npd = normalize_mapping(read_csv_flex(MAP_NPD), "Non-PD")
    md = pd.concat([map_pd, map_npd], ignore_index=True)
    print("[INFO] MAP_DF:", md.shape)
    return md


# =========================
# 파일 탐색/파싱
# =========================
def iter_dates(start, end):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def detect_remark_from_folder(folder_name: str):
    u = folder_name.upper()
    if "PD NONE" in u:
        return "Non-PD"
    if "PD" in u:
        return "PD"
    return None


def list_candidate_files_strict():
    """
    대상: BASE/YYYY/MM/DD/{target_folder}/{files}
    - DD 바로 아래 폴더(target_folder) 안의 파일만
    - 파일명에 '_' 포함 파일만
    """
    candidates = []  # (str_path, remark)
    for d in iter_dates(START_DATE, END_DATE):
        day_dir = BASE_DIR / f"{d.year:04d}" / f"{d.month:02d}" / f"{d.day:02d}"
        if not day_dir.exists():
            continue

        for target_folder in day_dir.iterdir():
            if not target_folder.is_dir():
                continue

            remark = detect_remark_from_folder(target_folder.name)

            for fp in target_folder.iterdir():  # 1-depth만
                if fp.is_file() and "_" in fp.name:
                    candidates.append((str(fp), remark))
    return candidates


def t_to_cs(tstr: str) -> int:
    # tstr: HH:MM:SS.xx (11 chars)
    hh = int(tstr[0:2]); mm = int(tstr[3:5]); ss = int(tstr[6:8]); cs = int(tstr[9:11])
    return ((hh*60 + mm)*60 + ss) * 100 + cs


def parse_one_file_fast(args):
    """멀티프로세스 워커 함수"""
    fp_str, remark = args
    fp = Path(fp_str)

    name = fp.name
    barcode_information = name.split("_", 1)[0] if "_" in name else name

    rows = []
    try:
        with open(fp, "r", encoding=ANSI_LOG_ENCODING, errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line.startswith("[") or len(line) < 13:
                    continue
                tstr = line[1:12]  # HH:MM:SS.xx
                if len(tstr) != 11 or tstr[2] != ":" or tstr[5] != ":" or tstr[8] != ".":
                    continue
                contents = line[line.find("]")+1:].strip()
                rows.append((t_to_cs(tstr), tstr, contents))
    except Exception:
        return None

    if not rows:
        return None

    rows.sort(key=lambda x: x[0])

    out = []
    prev_cs = None
    seq = 0

    for cs, tstr, contents in rows:
        ct = None
        if prev_cs is not None:
            ct = round((cs - prev_cs) / 100.0, 2)
        prev_cs = cs

        result = None
        is_ok = bool(OK_PAT.search(contents))
        is_ng = bool(NG_PAT.search(contents))

        if is_ok or is_ng:
            seq += 1
            result = "PASS" if is_ok else "FAIL"
            seq_val = seq
        else:
            seq_val = None

        out.append({
            "barcode_information": barcode_information,
            "remark": remark,
            "contents": contents,
            "time": tstr,
            "ct": ct,
            "result": result,
            "seq": seq_val,
            "file_path": str(fp),
        })

    return out


def build_df_from_files(new_cands, map_df: pd.DataFrame) -> pd.DataFrame:
    if not new_cands:
        return pd.DataFrame()

    parsed = []
    print(f"[INFO] parsing start (files={len(new_cands):,}, workers={NUM_WORKERS})")

    with Pool(processes=NUM_WORKERS) as pool:
        for r in pool.imap_unordered(parse_one_file_fast, new_cands, chunksize=20):
            if r:
                parsed.extend(r)

    df = pd.DataFrame(parsed)
    if df.empty:
        return df

    df["seq"] = pd.to_numeric(df["seq"], errors="coerce").astype("Int64")
    df = df.merge(map_df, how="left", on=["remark", "seq"])
    df.drop(columns=["seq"], inplace=True)

    # processed_at
    df["processed_at"] = datetime.now()

    # group: barcode_information별 1..N
    uniq = pd.unique(df["barcode_information"])
    gmap = {bc: i+1 for i, bc in enumerate(uniq)}
    df.insert(0, "group", pd.Series(df["barcode_information"].map(gmap), dtype="Int64"))

    return df


# =========================
# 메인 루프 (요구사항: 1초마다 무한 반복)
# =========================
def main_loop():
    ensure_schema_table()
    map_df = load_mapping_df()

    while True:
        try:
            # 1) 후보 파일 수집
            cands = list_candidate_files_strict()
            if not cands:
                print("[INFO] candidates: 0 (sleep 1s)")
                time.sleep(1)
                continue

            # 2) DB에 이미 적재된 file_path 확인 -> 신규만 선별
            all_paths = [fp for fp, _ in cands]
            existing = fetch_existing_file_paths(set(all_paths))
            new_cands = [(fp, remark) for (fp, remark) in cands if fp not in existing]

            print(f"[INFO] candidates={len(cands):,}, existing_files={len(existing):,}, new_files={len(new_cands):,}")

            if not new_cands:
                time.sleep(1)
                continue

            # 3) 멀티프로세스 파싱 -> DF
            df_parsed = build_df_from_files(new_cands, map_df)
            print(f"[INFO] parsed rows: {len(df_parsed):,}")

            if df_parsed.empty:
                time.sleep(1)
                continue

            # 4) station/end_day/end_time 매칭
            df_ready = merge_station_fields(df_parsed)

            # 5) INSERT
            inserted = insert_df(df_ready)
            print(f"[INFO] inserted_rows={inserted:,}")

        except Exception as e:
            print("[ERROR]", repr(e))

        time.sleep(1)


if __name__ == "__main__":
    freeze_support()
    print(f"[INFO] TARGET = {TGT_SCHEMA}.{TGT_TABLE}")
    print(f"[INFO] DATE RANGE = {START_DATE} ~ {END_DATE}")
    main_loop()
