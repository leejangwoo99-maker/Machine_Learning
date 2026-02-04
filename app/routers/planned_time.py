from __future__ import annotations
from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from app.core.db import make_engine
from app.schemas.planned_time import PlannedTimeIn, PlannedTimeOut
from app.services import planned_time_svc

router = APIRouter(prefix="/planned_time", tags=["6.planned_time"])


def get_engine() -> Engine:
    return make_engine()


@router.get("/today", response_model=list[PlannedTimeOut])
def get_today(engine: Engine = Depends(get_engine)):
    return planned_time_svc.list_today(engine)


@router.post("/today", response_model=dict)
def post_today(body: PlannedTimeIn, engine: Engine = Depends(get_engine)):
    planned_time_svc.upsert_today(engine, body)
    return {"ok": True}
