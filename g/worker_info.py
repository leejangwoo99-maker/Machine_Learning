# -*- coding: utf-8 -*-
"""
ct.csv → g_production_film.ct_info
- 모든 컬럼 TEXT 고정
- CSV 원본 100% 보존 적재
"""

import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text

DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SCHEMA = "g_production_film"
TABLE  = "worker_info"
CSV_PATH = r"C:\Users\user\Desktop\worker_info.csv"

engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{urllib.parse.quote_plus(DB_CONFIG['password'])}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
)

print("[OK] engine ready")

# 1. CSV를 **모든 컬럼 문자열**로 강제 로딩
df = pd.read_csv(CSV_PATH, dtype=str, encoding="utf-8-sig")
df.columns = [c.strip() for c in df.columns]

print(f"[OK] csv loaded: rows={len(df)} cols={len(df.columns)}")
print("[DBG] columns:", df.columns.tolist())

# 2. 스키마 + 테이블 완전 재생성 (ALL TEXT)
with engine.begin() as conn:
    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA};"))
    conn.execute(text(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE} CASCADE;"))

# CREATE TABLE (ALL TEXT)
cols = ",\n".join([f'"{c}" TEXT' for c in df.columns])

with engine.begin() as conn:
    conn.execute(text(f"""
        CREATE TABLE {SCHEMA}.{TABLE} (
            {cols}
        );
    """))

print("[OK] table recreated (ALL TEXT)")

# 3. 데이터 적재
df.to_sql(
    TABLE,
    engine,
    schema=SCHEMA,
    if_exists="append",
    index=False,
    chunksize=2000,
    method="multi"
)

print(f"[DONE] ct master loaded safely: {len(df)} rows")
