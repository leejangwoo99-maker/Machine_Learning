from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.alarm_record import AlarmRecordOut


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.ALARM_RECORD_TABLE}"'


def recent5(engine: Engine, end_day: str) -> list[AlarmRecordOut]:
    sql = text(
        f"""
        WITH last5 AS (
            SELECT
                end_day,
                end_time,
                station,
                sparepart,
                type_alarm
            FROM {fqn()}
            WHERE replace(end_day, '-', '') = :end_day
            ORDER BY (replace(end_day, '-', '') || ' ' || end_time) DESC
            LIMIT 5
        )
        SELECT
            end_day,
            end_time,
            station,
            sparepart,
            type_alarm
        FROM last5
        ORDER BY (replace(end_day, '-', '') || ' ' || end_time) ASC
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day}).mappings().all()
    return [AlarmRecordOut(**dict(r)) for r in rows]
