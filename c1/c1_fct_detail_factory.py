# fct_detail_loader.py
# -*- coding: utf-8 -*-

import re
import time
from pathlib import Path
from datetime import date, datetime
from multiprocessing import Pool, freeze_support

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extras import execute_values

# =========================
# 고정 경로
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

# 매핑 CSV
MAP_PD  = Path(r"C:\data\PD.csv")
MAP_NPD = Path(r"C:\data\Non-PD.csv")

# =========================
# DB 설정
# =========================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

TGT_SCHEMA = "c1_fct_detail"
TGT_TABLE  = "fct_detail"

SRC_SCHEMA = "a2_fct_table"
SRC_TABLE  = "fct_table"

# =========================
# 성능 / 실시간 조건
# =========================
ANSI_LOG_ENCODING = "cp949"

NUM_WORKERS = 2                      # 멀티프로세스 고정
REALTIME_WINDOW_SEC = 120            # 120초 이내 파일만
REALTIME_MODE = True

OK_PAT = re.compile(r"테스트\s*결과\s*:\s*OK")
NG_PAT = re.compile(r"테스트\s*결과\s*:\s*NG")


# =========================
# DB 유틸
# =========================
def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def ensure_schema_table():
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}")
                    .format(sql.Identifier(TGT_SCHEMA)))

        cur.execute(sql.SQL("""
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
        """).format(sql.Identifier(TGT_SCHEMA), sql.Identifier(TGT_TABLE)))

        cur.execute(sql.SQL("""
            CREATE INDEX IF NOT EXISTS {} ON {}.{} (file_path);
        """).format(
            sql.Identifier(f"idx_{TGT_TABLE}_file_path"),
            sql.Identifier(TGT_SCHEMA),
            sql.Identifier(TGT_TABLE),
        ))
        conn.commit()


def fetch_existing_file_paths(file_paths):
    if not file_paths:
        return set()

    q = sql.SQL("""
        SELECT DISTINCT file_path
        FROM {}.{}
        WHERE file_path = ANY(%s);
    """).format(sql.Identifier(TGT_SCHEMA), sql.Identifier(TGT_TABLE))

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, (list(file_paths),))
        return set(r[0] for r in cur.fetchall())


def fetch_processed_file_paths(limit=30):
    q = sql.SQL("""
        SELECT file_path, remark
        FROM {}.{}
        ORDER BY processed_at DESC NULLS LAST
        LIMIT %s;
    """).format(sql.Identifier(TGT_SCHEMA), sql.Identifier(TGT_TABLE))

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, (limit,))
        return [(r[0], r[1]) for r in cur.fetchall()]


# =========================
# 매핑 / 병합
# =========================
def fetch_fct_table_map(barcodes):
    if not barcodes:
        return {}

    q = sql.SQL("""
        SELECT barcode_information, station, end_day, end_time
        FROM {}.{}
        WHERE barcode_information = ANY(%s);
    """).format(sql.Identifier(SRC_SCHEMA), sql.Identifier(SRC_TABLE))

    out = {}
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(q, (barcodes,))
        for bc, st, ed, et in cur.fetchall():
            out[bc] = (st, str(ed) if ed else None, et)
    return out


def merge_station_fields(df):
    barcodes = df["barcode_information"].dropna().unique().tolist()
    mp = fetch_fct_table_map(barcodes)

    df["station"]  = df["barcode_information"].map(lambda x: mp.get(x, (None,None,None))[0])
    df["end_day"]  = df["barcode_information"].map(lambda x: mp.get(x, (None,None,None))[1])
    df["end_time"] = df["barcode_information"].map(lambda x: mp.get(x, (None,None,None))[2])
    return df


# =========================
# 매핑 CSV
# =========================
def load_mapping_df():
    def _read(p):
        try:
            return pd.read_csv(p, encoding="cp949")
        except:
            return pd.read_csv(p, encoding="utf-8-sig")

    def _norm(df, remark):
        if "seq" not in df.columns:
            df.insert(0, "seq", np.arange(1, len(df)+1))
        df["remark"] = remark
        return df[["seq","problem1","problem2","problem3","problem4","test_item","remark"]]

    return pd.concat([
        _norm(_read(MAP_PD), "PD"),
        _norm(_read(MAP_NPD), "Non-PD")
    ], ignore_index=True)


# =========================
# 파일 스캔 (날짜 요구사항 제거)
# =========================
def detect_remark_from_folder(name):
    u = name.upper()
    if "PD NONE" in u:
        return "Non-PD"
    if "PD" in u:
        return "PD"
    return None


def list_candidate_files_realtime(cutoff_ts):
    """
    ✅ 오늘 날짜 폴더만 스캔
    ✅ mtime >= cutoff_ts 인 파일만
    """
    today = date.today()
    day_dir = BASE_DIR / f"{today.year:04d}" / f"{today.month:02d}" / f"{today.day:02d}"
    if not day_dir.exists():
        return []

    out = []
    for folder in day_dir.iterdir():
        if not folder.is_dir():
            continue
        remark = detect_remark_from_folder(folder.name)
        for fp in folder.iterdir():
            if fp.is_file() and "_" in fp.name:
                try:
                    if fp.stat().st_mtime >= cutoff_ts:
                        out.append((str(fp), remark))
                except:
                    pass
    return out


# =========================
# 파싱
# =========================
def t_to_cs(t):
    return ((int(t[:2])*60 + int(t[3:5]))*60 + int(t[6:8]))*100 + int(t[9:11])


def parse_one_file_fast(args):
    fp_str, remark = args
    fp = Path(fp_str)
    barcode = fp.name.split("_",1)[0]

    rows = []
    try:
        with open(fp, encoding=ANSI_LOG_ENCODING, errors="ignore") as f:
            for ln in f:
                ln = ln.strip()
                if ln.startswith("[") and len(ln) >= 13:
                    t = ln[1:12]
                    if t[2]==":" and t[5]==":" and t[8]==".":
                        rows.append((t_to_cs(t), t, ln.split("]",1)[1].strip()))
    except:
        return None

    rows.sort()
    out, prev, seq = [], None, 0
    for cs,t,c in rows:
        ct = None if prev is None else round((cs-prev)/100,2)
        prev = cs
        res = None
        if OK_PAT.search(c) or NG_PAT.search(c):
            seq += 1
            res = "PASS" if OK_PAT.search(c) else "FAIL"
        out.append({
            "barcode_information": barcode,
            "remark": remark,
            "contents": c,
            "time": t,
            "ct": ct,
            "result": res,
            "seq": seq if res else None,
            "file_path": str(fp)
        })
    return out


def build_df_from_files(cands, map_df):
    parsed = []
    with Pool(NUM_WORKERS) as p:
        for r in p.imap_unordered(parse_one_file_fast, cands):
            if r: parsed.extend(r)

    df = pd.DataFrame(parsed)
    if df.empty:
        return df

    df = df.merge(map_df, how="left", on=["remark","seq"])
    df.drop(columns=["seq"], inplace=True)
    df["processed_at"] = datetime.now()

    gmap = {b:i+1 for i,b in enumerate(df["barcode_information"].unique())}
    df.insert(0,"group",df["barcode_information"].map(gmap))
    return df


# =========================
# SHOW
# =========================
def show_already_processed_data(map_df):
    paths = fetch_processed_file_paths(30)
    if not paths:
        return
    cands = [(p,r) for p,r in paths]
    df = build_df_from_files(cands, map_df)
    print(df.head(20).to_string(index=False))


# =========================
# MAIN LOOP
# =========================
def main_loop():
    ensure_schema_table()
    map_df = load_mapping_df()

    show_already_processed_data(map_df)

    while True:
        cutoff_ts = time.time() - REALTIME_WINDOW_SEC
        print(f"[INFO] cutoff_ts={cutoff_ts}")

        cands = list_candidate_files_realtime(cutoff_ts)
        if not cands:
            time.sleep(1)
            continue

        existing = fetch_existing_file_paths([p for p,_ in cands])
        new_cands = [(p,r) for p,r in cands if p not in existing]

        if not new_cands:
            time.sleep(1)
            continue

        df = build_df_from_files(new_cands, map_df)
        if df.empty:
            time.sleep(1)
            continue

        df = merge_station_fields(df)

        # ✅ 오늘 end_day만 유효
        today = str(date.today())
        df = df[df["end_day"] == today]

        if not df.empty:
            insert_df(df)

        time.sleep(1)


if __name__ == "__main__":
    freeze_support()
    main_loop()
