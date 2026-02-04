from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Engine
from app.core.config import settings
from app.schemas.worker_info import WorkerInfoIn, WorkerInfoOut


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.WORKER_INFO_TABLE}"'


def list_by_day_shift(engine: Engine, end_day: str, shift_type: str) -> list[WorkerInfoOut]:
    sql = text(f"""
        SELECT end_day, shift_type, worker_name, order_number
        FROM {fqn()}
        WHERE end_day = :end_day AND shift_type = :shift_type
        ORDER BY worker_name
    """)
    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day, "shift_type": shift_type}).mappings().all()
    return [WorkerInfoOut(**dict(r)) for r in rows]


def upsert(engine: Engine, item: WorkerInfoIn) -> None:
    sql = text(f"""
        INSERT INTO {fqn()} (end_day, shift_type, worker_name, order_number, created_at, updated_at)
        VALUES (:end_day, :shift_type, :worker_name, :order_number, now(), NULL)
        ON CONFLICT (end_day, shift_type, worker_name)
        DO UPDATE SET
            order_number = EXCLUDED.order_number,
            updated_at = now()
    """)
    with engine.begin() as conn:
        conn.execute(sql, item.model_dump())
