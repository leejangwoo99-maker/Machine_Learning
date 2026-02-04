from __future__ import annotations
from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine
from app.core.db import make_engine
from app.schemas.pd_board_check import PdBoardCheckOut
from app.services import pd_board_check_svc

router = APIRouter(prefix="/predictive", tags=["10.pd_board_check"])


def get_engine() -> Engine:
    return make_engine()


@router.get("/pd-board-check/{end_day}", response_model=list[PdBoardCheckOut])
def get_pd_board_check(end_day: str, engine: Engine = Depends(get_engine)):
    return pd_board_check_svc.list_pd_board_check(engine, end_day)
