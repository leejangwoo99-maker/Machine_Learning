from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Engine
from app.core.config import settings
from app.schemas.pd_board_check import PdBoardCheckOut


def fqn() -> str:
    return f'{settings.E4PM_SCHEMA}."{settings.PD_BOARD_CHECK_TABLE}"'


def list_by_end_day(engine: Engine, end_day: str) -> list[PdBoardCheckOut]:
    sql = text(f"""
        SELECT station, end_day, last_status, cosine_similarity
        FROM {fqn()}
        WHERE end_day = :end_day
        ORDER BY station ASC
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day}).mappings().all()
    return [PdBoardCheckOut(**dict(r)) for r in rows]
