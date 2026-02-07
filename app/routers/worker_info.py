# app/routers/worker_info.py
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import OperationalError, DBAPIError
from sqlalchemy.orm import Session

# 프로젝트 경로 자동 대응
try:
    from app.database import get_db  # type: ignore
except Exception:
    from app.db.session import get_db  # type: ignore

from app.schemas.worker_info import WorkerInfoIn, WorkerInfoSyncIn
from app.services import worker_info_svc

router = APIRouter(prefix="/worker_info", tags=["2.worker_info"])


@router.get("")
def get_worker_info(
    end_day: str,
    shift_type: str,
    db: Session = Depends(get_db),
):
    """
    end_day + shift_type 조건 조회
    """
    try:
        return worker_info_svc.list_worker_info(db, end_day=end_day, shift_type=shift_type)
    except (OperationalError, DBAPIError):
        raise HTTPException(status_code=503, detail="DB unavailable")


@router.post("")
def post_worker_info(
    body: WorkerInfoIn,
    db: Session = Depends(get_db),
):
    """
    단건 UPSERT (키: end_day, shift_type, worker_name)
    """
    try:
        worker_info_svc.upsert_worker_info(db, body)
        db.commit()
        return {"ok": True}
    except (OperationalError, DBAPIError):
        db.rollback()
        raise HTTPException(status_code=503, detail="DB unavailable")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"worker_info upsert failed: {e}")


@router.post("/sync")
def post_worker_info_sync(
    body: WorkerInfoSyncIn,
    db: Session = Depends(get_db),
):
    """
    전체 동기화(비밀번호 없음)
    - body.rows를 end_day+shift_type별로 그룹 처리
    - 키: (end_day, shift_type, worker_name)
      * target에만 있으면 INSERT
      * current에만 있으면 DELETE
      * 공통키면 order_number UPDATE
    """
    try:
        if not body.rows:
            raise HTTPException(status_code=422, detail="rows is required")

        out = worker_info_svc.sync_worker_info(db, body)
        db.commit()
        return out
    except HTTPException:
        db.rollback()
        raise
    except (OperationalError, DBAPIError):
        db.rollback()
        raise HTTPException(status_code=503, detail="DB unavailable")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"worker_info sync failed: {e}")
