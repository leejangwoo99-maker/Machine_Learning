import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

def build_db_url() -> str:
    host = os.getenv("DB_HOST")
    port = os.getenv("DB_PORT", "5432")
    name = os.getenv("DB_NAME", "postgres")
    user = os.getenv("DB_USER")
    pw = os.getenv("DB_PASSWORD")

    if not all([host, user, pw]):
        raise RuntimeError(
            "DB 환경변수(DB_HOST/DB_USER/DB_PASSWORD)가 설정되지 않았습니다. "
            "PowerShell에서 $env:DB_HOST=... 형태로 설정하세요."
        )

    # psycopg2-binary 사용
    return f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{name}"

DATABASE_URL = build_db_url()

engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
    pool_size=5,
    max_overflow=10,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
