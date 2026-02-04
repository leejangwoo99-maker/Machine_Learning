from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Engine
from app.core.config import settings
from app.schemas.nonop_time import NonOpRow


def _fqn(table: str) -> str:
    return f'{settings.GPF_SCHEMA}."{table}"'


def list_fct(engine: Engine, end_day: str) -> list[NonOpRow]:
    sql = text(f"""
        SELECT end_day, from_time, to_time, reason, sparepart
        FROM {_fqn(settings.FCT_NONOP_TABLE)}
        WHERE end_day = :end_day
        ORDER BY from_time, to_time
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day}).mappings().all()
    return [NonOpRow(**dict(r)) for r in rows]


def list_vision(engine: Engine, end_day: str) -> list[NonOpRow]:
    sql = text(f"""
        SELECT end_day, from_time, to_time, reason, sparepart
        FROM {_fqn(settings.VISION_NONOP_TABLE)}
        WHERE end_day = :end_day
        ORDER BY from_time, to_time
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day}).mappings().all()
    return [NonOpRow(**dict(r)) for r in rows]
