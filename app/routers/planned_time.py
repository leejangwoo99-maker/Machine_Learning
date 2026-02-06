from __future__ import annotations

from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine

from app.core.db import make_engine
from app.core.event_bus import event_bus
from app.schemas.planned_time import PlannedTimeIn, PlannedTimeOut
from app.services import planned_time_svc

router = APIRouter(prefix="/planned_time", tags=["6.planned_time"])
KST = timezone(timedelta(hours=9))


def get_engine() -> Engine:
    return make_engine()


@router.get("/today", response_model=list[PlannedTimeOut])
def get_today(engine: Engine = Depends(get_engine)):
    return planned_time_svc.list_today(engine)


@router.post("/today", response_model=dict)
async def post_today(body: PlannedTimeIn, engine: Engine = Depends(get_engine)):
    planned_time_svc.upsert_today(engine, body)

    # planned_time_event 발행
    await event_bus.publish(
        "planned_time_event",
        {
            "end_day": getattr(body, "end_day", None),
            "shift_type": getattr(body, "shift_type", None),
            "from_time": getattr(body, "from_time", None),
            "to_time": getattr(body, "to_time", None),
            "ts_kst": datetime.now(KST).isoformat(),
        },
    )
    return {"ok": True}
