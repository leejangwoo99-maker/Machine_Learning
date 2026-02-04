from __future__ import annotations
from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from app.core.db import make_engine
from app.schemas.nonop_time import NonOpResponse
from app.services import nonop_time_svc

router = APIRouter(prefix="/non_operation_time", tags=["7.non_operation_time"])


def get_engine() -> Engine:
    return make_engine()


@router.get("", response_model=NonOpResponse)
def get_nonop(end_day: str, shift_type: str | None = None, engine: Engine = Depends(get_engine)):
    # shift_type이 있으면 day/night window로 필터 (문자열 비교, overlap 기준)
    return nonop_time_svc.get_nonop(engine, end_day, shift_type)
