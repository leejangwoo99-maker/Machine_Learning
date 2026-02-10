from __future__ import annotations

from sqlalchemy.engine import Engine

from app.repos.reports_mastersample_repo import list_by_prod_day


def get_mastersample(engine: Engine, prod_day: str, shift_type: str):
    # prod_day = YYYYMMDD, shift_type = day|night
    # ?곗씠???놁쑝硫?200 + []
    return list_by_prod_day(engine, prod_day, shift_type)
