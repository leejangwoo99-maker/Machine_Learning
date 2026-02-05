from __future__ import annotations

from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine

from app.core.db import make_engine
from app.core.event_bus import event_bus
from app.schemas.nonop_time import NonOpListResponse, NonOpUpsertIn, NonOpUpsertOut
from app.services import nonop_time_svc

router = APIRouter(prefix="/non_operation_time", tags=["7.non_operation_time"])
KST = timezone(timedelta(hours=9))


def get_engine() -> Engine:
    return make_engine()


@router.get(
    "",
    response_model=NonOpListResponse,
    summary="Get Non-Operation Time",
    description=(
        "end_day + shift_type(day/night) 조건으로 fct/vision 비가동 데이터를 "
        "통합 조회한다. from_time 내림차순으로 반환한다."
    ),
)
def get_nonop(end_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    try:
        return nonop_time_svc.get_nonop(engine, end_day, shift_type)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post(
    "",
    response_model=NonOpUpsertOut,
    summary="Update Non-Operation reason/sparepart",
    description=(
        "키(end_day, station, from_time, to_time)로 row를 특정하여 "
        "reason, sparepart만 수정한다. (INSERT 없음)"
    ),
)
async def post_nonop(body: NonOpUpsertIn, engine: Engine = Depends(get_engine)):
    try:
        out, key = nonop_time_svc.upsert_nonop(engine, body)

        # 즉시 앱 내부 이벤트 발행 (트리거 대기 없음)
        await event_bus.publish(
            "non_operation_time_event",
            {
                "end_day": key.end_day,
                "station": key.station,
                "from_time": key.from_time,
                "to_time": key.to_time,
                "ts_kst": datetime.now(KST).isoformat(),
            },
        )
        return out
    except ValueError as e:
        msg = str(e)
        if msg == "target row not found":
            raise HTTPException(status_code=404, detail=msg)
        raise HTTPException(status_code=400, detail=msg)
