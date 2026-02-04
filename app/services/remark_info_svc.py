from __future__ import annotations
from sqlalchemy.engine import Engine
from app.repos import remark_info_repo
from app.schemas.remark_info import RemarkInfoIn, RemarkInfoOut


def list_remarks(engine: Engine) -> list[RemarkInfoOut]:
    return remark_info_repo.list_all(engine)


def upsert_remark(engine: Engine, item: RemarkInfoIn) -> None:
    remark_info_repo.upsert(engine, item)
