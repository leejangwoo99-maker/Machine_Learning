from __future__ import annotations
from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from app.core.db import make_engine
from app.core.security import require_admin_pass
from app.schemas.email_list import EmailListSyncIn
from app.schemas.common import SyncResult
from app.services import email_list_svc

router = APIRouter(prefix="/email_list", tags=["3.email_list"])


def get_engine() -> Engine:
    return make_engine()


@router.get("", response_model=list[str])
def get_email_list(engine: Engine = Depends(get_engine)):
    return email_list_svc.list_emails(engine)


@router.post("/sync", response_model=SyncResult, dependencies=[Depends(require_admin_pass)])
def post_email_sync(body: EmailListSyncIn, engine: Engine = Depends(get_engine)):
    ins, dele = email_list_svc.sync_emails(engine, body.emails)
    return SyncResult(inserted=ins, deleted=dele)
