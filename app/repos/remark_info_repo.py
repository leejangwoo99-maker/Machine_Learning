from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.remark_info import RemarkInfoIn, RemarkInfoOut


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.REMARK_INFO_TABLE}"'


def list_all(engine: Engine) -> list[RemarkInfoOut]:
    sql = text(
        f"""
        SELECT barcode_information, pn, remark
        FROM {fqn()}
        ORDER BY barcode_information
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return [RemarkInfoOut(**dict(r)) for r in rows]


def upsert(engine: Engine, item: RemarkInfoIn) -> None:
    sql = text(
        f"""
        INSERT INTO {fqn()} (barcode_information, pn, remark)
        VALUES (:barcode_information, :pn, :remark)
        ON CONFLICT (barcode_information)
        DO UPDATE SET
            pn = EXCLUDED.pn,
            remark = EXCLUDED.remark
        """
    )
    with engine.begin() as conn:
        conn.execute(sql, item.model_dump())
