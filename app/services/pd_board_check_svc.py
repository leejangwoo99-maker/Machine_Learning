from __future__ import annotations
from sqlalchemy.engine import Engine
from app.core.timewindow import validate_yyyymmdd
from app.repos import pd_board_check_repo
from app.schemas.pd_board_check import PdBoardCheckOut


def list_pd_board_check(engine: Engine, end_day: str) -> list[PdBoardCheckOut]:
    end_day = validate_yyyymmdd(end_day)
    return pd_board_check_repo.list_by_end_day(engine, end_day)
