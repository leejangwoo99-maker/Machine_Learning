from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.planned_time import PlannedTimeIn, PlannedTimeOut


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.PLANNED_TIME_TABLE}"'


def list_by_end_day(engine: Engine, end_day: str) -> list[PlannedTimeOut]:
    sql = text(
        f"""
        SELECT end_day, from_time, to_time, reason
        FROM {fqn()}
        WHERE end_day = :end_day
        ORDER BY from_time, to_time
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day}).mappings().all()
    return [PlannedTimeOut(**dict(r)) for r in rows]


def upsert_today(engine: Engine, end_day: str, item: PlannedTimeIn) -> None:
    sql = text(
        f"""
        INSERT INTO {fqn()} (end_day, from_time, to_time, reason)
        VALUES (:end_day, :from_time, :to_time, :reason)
        ON CONFLICT (end_day, from_time, to_time)
        DO UPDATE SET
            reason = EXCLUDED.reason
        """
    )
    payload = {
        "end_day": end_day,
        "from_time": item.from_time,
        "to_time": item.to_time,
        "reason": item.reason,
    }
    with engine.begin() as conn:
        conn.execute(sql, payload)
