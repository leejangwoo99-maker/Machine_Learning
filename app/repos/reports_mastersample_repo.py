from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.reports_mastersample import MastersampleOut


def fqn_day() -> str:
    return f'{settings.IDR_SCHEMA}."e_mastersample_test_day_daily"'


def fqn_night() -> str:
    return f'{settings.IDR_SCHEMA}."e_mastersample_test_night_daily"'


def list_by_prod_day(engine: Engine, prod_day: str, shift_type: str) -> list[MastersampleOut]:
    table = fqn_day() if shift_type == "day" else fqn_night()

    # updated_at은 SELECT에서 제외
    sql = text(f"""
        SELECT
            prod_day,
            shift_type,
            "Mastersample",
            first_time
        FROM {table}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {"prod_day": prod_day, "shift_type": shift_type}).mappings().all()

    return [MastersampleOut(**dict(r)) for r in rows]
