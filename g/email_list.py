# -*- coding: utf-8 -*-
"""
Desktop의 email_list.xlsx → PostgreSQL 적재 스크립트 (Windows)
- 스키마/테이블 자동 생성
- email_list 컬럼에 엑셀 행들을 저장
- 중복은 PK + ON CONFLICT로 자동 무시
"""

import os
import pandas as pd
from sqlalchemy import create_engine, text

# =========================
# DB 설정
# =========================
DB_CONFIG = {
    "host": "100.105.75.47",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# =========================
# 엑셀 파일 경로 (바탕화면 자동 탐색)
# =========================
FILENAME = "email_list.xlsx"

home = os.path.expanduser("~")
candidates = [
    os.path.join(home, "Desktop", FILENAME),                 # 일반 바탕화면
    os.path.join(home, "OneDrive", "Desktop", FILENAME),     # OneDrive 바탕화면
    os.path.join(home, "OneDrive", "바탕 화면", FILENAME),   # OneDrive 한글 경로 케이스
    os.path.join(home, "바탕 화면", FILENAME),               # 한글 바탕화면 케이스(일부 환경)
]

EXCEL_PATH = next((p for p in candidates if os.path.exists(p)), None)
if EXCEL_PATH is None:
    raise FileNotFoundError(
        "[FATAL] 바탕화면에서 엑셀 파일을 찾을 수 없습니다.\n"
        f"- 찾는 파일명: {FILENAME}\n"
        "- 확인 경로:\n  " + "\n  ".join(candidates)
    )

print(f"[OK] Excel found: {EXCEL_PATH}")

# =========================
# DB 엔진
# =========================
engine = create_engine(
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
    f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}",
    pool_pre_ping=True
)

# =========================
# 테이블 생성
# =========================
with engine.begin() as conn:
    conn.execute(text("""
        CREATE SCHEMA IF NOT EXISTS g_production_film;
        CREATE TABLE IF NOT EXISTS g_production_film.email_list (
            email_list TEXT PRIMARY KEY
        );
    """))

# =========================
# 엑셀 로드
# =========================
df = pd.read_excel(EXCEL_PATH)

# 첫 번째 컬럼을 email_list로 사용
first_col = df.columns[0]
df = df[[first_col]].copy()
df.columns = ["email_list"]

# 정리: 공백 제거, 빈값 제거, 중복 제거
df["email_list"] = df["email_list"].astype(str).str.strip()
df = df[df["email_list"] != ""]
df = df.drop_duplicates()

print(f"[INFO] 적재 대상 이메일 수: {len(df)}")

# =========================
# DB 저장 (중복 무시)
# =========================
with engine.begin() as conn:
    conn.execute(text("SET search_path TO g_production_film;"))
    for email in df["email_list"]:
        conn.execute(
            text("""
                INSERT INTO g_production_film.email_list(email_list)
                VALUES (:email)
                ON CONFLICT (email_list) DO NOTHING
            """),
            {"email": email}
        )

print("[OK] g_production_film.email_list 적재 완료")
