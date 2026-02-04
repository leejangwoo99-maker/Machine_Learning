from __future__ import annotations
from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from app.core.db import make_engine
from app.schemas.worker_info import WorkerInfoIn, WorkerInfoOut
from app.services import worker_info_svc

router = APIRouter(prefix="/worker_info", tags=["2.worker_info"])


def get_engine() -> Engine:
    return make_engine()


@router.get("", response_model=list[WorkerInfoOut])
def get_worker_info(end_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return worker_info_svc.list_worker_info(engine, end_day, shift_type)


@router.post("", response_model=dict)
def post_worker_info(body: WorkerInfoIn, engine: Engine = Depends(get_engine)):
    worker_info_svc.upsert_worker_info(engine, body)
    return {"ok": True}
