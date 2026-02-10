from __future__ import annotations
from sqlalchemy.engine import Engine
from app.core.timewindow import now_kst, yyyymmdd
from app.repos import planned_time_repo
from app.schemas.planned_time import PlannedTimeIn, PlannedTimeOut


def today_end_day() -> str:
    return yyyymmdd(now_kst())


def list_today(engine: Engine) -> list[PlannedTimeOut]:
    return planned_time_repo.list_by_end_day(engine, today_end_day())


def upsert_today(engine: Engine, item: PlannedTimeIn) -> None:
    planned_time_repo.upsert_today(engine, today_end_day(), item)
