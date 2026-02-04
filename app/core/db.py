from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from app.core.config import settings


def make_engine() -> Engine:
    if not settings.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty. Set it in environment (.env).")

    return create_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        future=True,
    )
