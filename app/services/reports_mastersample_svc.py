from __future__ import annotations

from sqlalchemy.engine import Engine

from app.repos.reports_mastersample_repo import list_by_prod_day


def get_mastersample(engine: Engine, prod_day: str, shift_type: str):
    # prod_day = YYYYMMDD, shift_type = day|night
    # 데이터 없으면 200 + []
    return list_by_prod_day(engine, prod_day, shift_type)
