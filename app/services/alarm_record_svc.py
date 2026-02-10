from __future__ import annotations
from sqlalchemy.engine import Engine
from app.core.timewindow import validate_yyyymmdd
from app.repos import alarm_record_repo
from app.schemas.alarm_record import AlarmRecordOut


def recent5(engine: Engine, end_day: str) -> list[AlarmRecordOut]:
    end_day = validate_yyyymmdd(end_day)
    return alarm_record_repo.recent5(engine, end_day)
