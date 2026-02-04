from __future__ import annotations
from sqlalchemy.engine import Engine
from app.core.timewindow import validate_yyyymmdd, validate_shift_type, enforce_current_shift
from app.schemas.worker_info import WorkerInfoIn, WorkerInfoOut
from app.repos import worker_info_repo


def list_worker_info(engine: Engine, end_day: str, shift_type: str) -> list[WorkerInfoOut]:
    end_day = validate_yyyymmdd(end_day)
    shift_type = validate_shift_type(shift_type)
    return worker_info_repo.list_by_day_shift(engine, end_day, shift_type)


def upsert_worker_info(engine: Engine, item: WorkerInfoIn) -> None:
    item.end_day = validate_yyyymmdd(item.end_day)
    item.shift_type = validate_shift_type(item.shift_type)
    enforce_current_shift(item.end_day, item.shift_type)
    worker_info_repo.upsert(engine, item)
