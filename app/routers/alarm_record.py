from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine

from app.core.db import make_engine
from app.schemas.alarm_record import AlarmRecordOut
from app.services import alarm_record_svc

router = APIRouter(tags=["9.alarm_record"])


def get_engine() -> Engine:
    return make_engine()


@router.get("/alarm_record/recent5", response_model=list[AlarmRecordOut])
def get_recent5(end_day: str, engine: Engine = Depends(get_engine)) -> list[AlarmRecordOut]:
    return alarm_record_svc.recent5(engine, end_day)
