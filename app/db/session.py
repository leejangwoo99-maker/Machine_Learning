# app/db/session.py
from __future__ import annotations

import os
from typing import Generator, Tuple

from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

# ------------------------------------------------------------
# ENV
# ------------------------------------------------------------
# app/.env 우선, 없으면 프로젝트 루트 .env
BASE_DIR = os.path.dirname(os.path.dirname(__file__))        # .../app
ROOT_DIR = os.path.dirname(BASE_DIR)                         # project root
ENV_APP = os.path.join(BASE_DIR, ".env")
ENV_ROOT = os.path.join(ROOT_DIR, ".env")

if os.path.exists(ENV_APP):
    load_dotenv(ENV_APP, override=False)
elif os.path.exists(ENV_ROOT):
    load_dotenv(ENV_ROOT, override=False)

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
PG_WORK_MEM = os.getenv("PG_WORK_MEM", "8MB").strip() or "8MB"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set (.env).")

# psycopg2 scheme normalize
if DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)

# ------------------------------------------------------------
# Engine (안정성 우선)
# ------------------------------------------------------------
# - pool_pre_ping: 죽은 커넥션 자동 제거
# - pool_recycle: 장시간 유휴 커넥션 재생성
# - pool_size/max_overflow: 과도한 동시성 시 튜닝
# - pool_timeout: checkout 대기
ENGINE = create_engine(
    DATABASE_URL,
    future=True,
    pool_pre_ping=True,
    pool_recycle=1800,
    pool_size=int(os.getenv("DB_POOL_SIZE", "10")),
    max_overflow=int(os.getenv("DB_MAX_OVERFLOW", "20")),
    pool_timeout=int(os.getenv("DB_POOL_TIMEOUT", "30")),
)

SessionLocal = sessionmaker(
    bind=ENGINE,
    autocommit=False,
    autoflush=False,
    expire_on_commit=False,
    future=True,
)

# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _set_session_work_mem(db: Session) -> None:
    # 파라미터 바인딩으로 SET 하는 방식은 환경에 따라 실패할 수 있어 literal 권장
    # 예: SET work_mem = '8MB'
    wm = PG_WORK_MEM.replace("'", "").strip()
    db.execute(text(f"SET work_mem = '{wm}'"))


def db_ping() -> Tuple[bool, str]:
    """health/db 용"""
    try:
        with ENGINE.connect() as conn:
            one = conn.execute(text("SELECT 1")).scalar_one()
            return (one == 1), "pong"
    except Exception as e:
        return False, str(e)


def db_whoami() -> dict:
    """health/db-whoami 용"""
    with ENGINE.connect() as conn:
        row = conn.execute(
            text(
                """
                SELECT
                    current_user::text AS current_user,
                    current_database()::text AS current_database,
                    inet_server_addr()::text AS server_addr,
                    inet_server_port()::int AS server_port,
                    version()::text AS version
                """
            )
        ).mappings().one()
        return dict(row)


def get_db() -> Generator[Session, None, None]:
    """FastAPI Depends"""
    db = SessionLocal()
    try:
        _set_session_work_mem(db)
        yield db
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()
