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


def delete_one(engine: Engine, barcode_information: str) -> int:
    sql = text(
        f"""
        DELETE FROM {fqn()}
        WHERE barcode_information = :barcode_information
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"barcode_information": barcode_information})
    return int(r.rowcount or 0)


def sync(engine: Engine, items: list[RemarkInfoIn]) -> tuple[int, int]:
    # target map (?숈씪 key 以?留덉?留?媛??곗꽑)
    target_map: dict[str, dict] = {}
    for it in items:
        d = it.model_dump()
        target_map[d["barcode_information"]] = d

    with engine.begin() as conn:
        cur_rows = conn.execute(
            text(f"SELECT barcode_information FROM {fqn()}")
        ).mappings().all()
        current = {r["barcode_information"] for r in cur_rows}
        target = set(target_map.keys())

        to_upsert = sorted(target)
        to_delete = sorted(current - target)

        upserted = 0
        deleted = 0

        upsert_sql = text(
            f"""
            INSERT INTO {fqn()} (barcode_information, pn, remark)
            VALUES (:barcode_information, :pn, :remark)
            ON CONFLICT (barcode_information)
            DO UPDATE SET
                pn = EXCLUDED.pn,
                remark = EXCLUDED.remark
            """
        )
        for k in to_upsert:
            conn.execute(upsert_sql, target_map[k])
            upserted += 1

        delete_sql = text(
            f"""
            DELETE FROM {fqn()}
            WHERE barcode_information = :barcode_information
            """
        )
        for k in to_delete:
            r = conn.execute(delete_sql, {"barcode_information": k})
            deleted += int(r.rowcount or 0)

    return upserted, deleted
