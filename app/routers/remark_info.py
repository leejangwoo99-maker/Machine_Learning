from __future__ import annotations
from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from app.core.db import make_engine
from app.core.security import require_admin_pass
from app.schemas.remark_info import RemarkInfoIn, RemarkInfoOut
from app.services import remark_info_svc

router = APIRouter(prefix="/remark_info", tags=["4.remark_info"])


def get_engine() -> Engine:
    return make_engine()


@router.get("", response_model=list[RemarkInfoOut])
def get_remarks(engine: Engine = Depends(get_engine)):
    return remark_info_svc.list_remarks(engine)


@router.post("", response_model=dict, dependencies=[Depends(require_admin_pass)])
def post_remark(body: RemarkInfoIn, engine: Engine = Depends(get_engine)):
    remark_info_svc.upsert_remark(engine, body)
    return {"ok": True}
