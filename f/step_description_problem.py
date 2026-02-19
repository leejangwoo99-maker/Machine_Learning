# -*- coding: utf-8 -*-
"""
step_description_criteria.xlsx -> f_database.step_description_problem 적재 스크립트
- 파일 그대로 파싱(컬럼 그대로, 값 그대로)
- 테이블 없으면 생성
- 매 실행 시 TRUNCATE 후 전체 재적재
"""

import os
import sys
import pandas as pd
from sqlalchemy import create_engine, text

# =========================================================
# 1) 경로 / 파일명
# =========================================================
INPUT_XLSX = r"C:\Users\user\Desktop\step_description_criteria.xlsx"

# =========================================================
# 2) DB 설정
# =========================================================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",
}

SCHEMA = "f_database"
TABLE = "step_description_problem"

def build_engine(cfg: dict):
    # psycopg2 드라이버 기준
    url = (
        f"postgresql+psycopg2://{cfg['user']}:{cfg['password']}"
        f"@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"
    )
    return create_engine(url, future=True)

def quote_ident(name: str) -> str:
    # 안전한 식별자 quoting (컬럼명/테이블명)
    return '"' + name.replace('"', '""') + '"'

def main():
    # -----------------------------
    # 입력 파일 확인
    # -----------------------------
    if not os.path.exists(INPUT_XLSX):
        raise FileNotFoundError(f"[ERROR] 입력 파일이 없습니다: {INPUT_XLSX}")

    print(f"[OK] input: {INPUT_XLSX}")

    # -----------------------------
    # 엑셀 로드 (파일 그대로)
    # - dtype=str: 숫자/문자 섞여도 문자열로 유지
    # - keep_default_na=True: 빈칸을 NaN으로 읽되, 이후 NULL로 변환
    # -----------------------------
    df = pd.read_excel(INPUT_XLSX, dtype=str)

    # 컬럼명 그대로 사용 (공백/특수문자도 허용되지만, DB 컬럼으로는 권장X)
    # 필요한 경우 여기서 표준화 가능하나, "파일 그대로" 요구로 인해 그대로 둠.
    if df.empty:
        raise ValueError("[ERROR] 엑셀에 데이터가 없습니다(빈 파일).")

    print(f"[OK] excel loaded: rows={len(df)} cols={len(df.columns)}")
    print(f"[INFO] columns: {list(df.columns)}")

    # NaN -> None (PostgreSQL NULL)
    df = df.where(pd.notna(df), None)

    engine = build_engine(DB_CONFIG)

    # -----------------------------
    # 스키마/테이블 생성
    # -----------------------------
    # 엑셀 컬럼을 그대로 TEXT로 생성
    col_defs = []
    for c in df.columns:
        col_defs.append(f"{quote_ident(str(c))} TEXT")

    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {quote_ident(SCHEMA)};

    CREATE TABLE IF NOT EXISTS {quote_ident(SCHEMA)}.{quote_ident(TABLE)} (
        id BIGSERIAL PRIMARY KEY,
        {", ".join(col_defs)}
    );
    """

    # -----------------------------
    # 적재 (TRUNCATE -> INSERT)
    # -----------------------------
    with engine.begin() as conn:
        conn.execute(text(create_sql))
        print(f"[OK] ensured: {SCHEMA}.{TABLE}")

        conn.execute(text(f"TRUNCATE TABLE {quote_ident(SCHEMA)}.{quote_ident(TABLE)};"))
        print(f"[OK] truncated: {SCHEMA}.{TABLE}")

        # pandas to_sql은 컬럼을 그대로 매핑 (id는 자동 증가이므로 제외)
        df.to_sql(
            name=TABLE,
            con=conn,
            schema=SCHEMA,
            if_exists="append",
            index=False,
            method="multi",     # 다중 INSERT
            chunksize=2000
        )

    print(f"[DONE] inserted rows={len(df)} into {SCHEMA}.{TABLE}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
