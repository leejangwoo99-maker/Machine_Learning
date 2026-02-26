from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.remark_info import RemarkInfoIn, RemarkInfoOut


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.REMARK_INFO_TABLE}"'


def _norm_barcode(x: str) -> str:
    # barcode도 공백/대소문자 문제로 delete가 안 되는 케이스가 많음
    return (x or "").strip().lower()


def list_all(engine: Engine) -> list[RemarkInfoOut]:
    # barcode_information은 정규화해서 반환(프론트도 이 기준으로 매칭되게)
    sql = text(
        f"""
        SELECT
            lower(btrim(barcode_information)) AS barcode_information,
            pn,
            remark
        FROM {fqn()}
        WHERE barcode_information IS NOT NULL AND btrim(barcode_information) <> ''
        ORDER BY lower(btrim(barcode_information))
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).mappings().all()
    return [RemarkInfoOut(**dict(r)) for r in rows]


def upsert(engine: Engine, item: RemarkInfoIn) -> None:
    d = item.model_dump()
    d["barcode_information"] = _norm_barcode(d.get("barcode_information", ""))

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
        conn.execute(sql, d)


def delete_one(engine: Engine, barcode_information: str) -> int:
    bc = _norm_barcode(barcode_information)
    if not bc:
        return 0

    # 정규화 기준으로 삭제 (원문이 공백/대소문자 달라도 삭제되게)
    sql = text(
        f"""
        DELETE FROM {fqn()}
        WHERE lower(btrim(barcode_information)) = :barcode_information
        """
    )
    with engine.begin() as conn:
        r = conn.execute(sql, {"barcode_information": bc})
    return int(r.rowcount or 0)


def sync(engine: Engine, items: list[RemarkInfoIn]) -> tuple[int, int]:
    """
    remark_info를 incoming items와 완전히 동기화.
    - key(barcode_information)는 lower(btrim()) 기준
    - incoming에 없는 기존 row는 삭제
    - incoming이 빈 리스트면 테이블의 모든 row 삭제
    """
    # 1) target_map 만들기 (정규화된 barcode 기준으로 마지막 값 승)
    target_map: dict[str, dict] = {}
    for it in items or []:
        d = it.model_dump()
        bc = _norm_barcode(d.get("barcode_information", ""))
        if not bc:
            continue
        d["barcode_information"] = bc
        target_map[bc] = d

    target_keys = set(target_map.keys())

    with engine.begin() as conn:
        # 2) 현재 DB key 목록 (정규화)
        current_keys = set(
            conn.execute(
                text(
                    f"""
                    SELECT lower(btrim(barcode_information)) AS barcode_information
                    FROM {fqn()}
                    WHERE barcode_information IS NOT NULL AND btrim(barcode_information) <> ''
                    """
                )
            ).scalars().all()
        )

        # 3) upsert 대상/삭제 대상
        to_upsert = sorted(target_keys)
        to_delete = sorted(current_keys - target_keys)

        upserted = 0
        deleted = 0

        # 4) upsert
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

        # 5) delete (정규화 기준)
        if to_delete:
            r = conn.execute(
                text(
                    f"""
                    DELETE FROM {fqn()}
                    WHERE lower(btrim(barcode_information)) = ANY(:arr)
                    """
                ),
                {"arr": to_delete},
            )
            deleted = int(r.rowcount or 0)

        # 6) 혹시 과거 찌꺼기 중복 정리(공백/대소문자만 다른 중복 row)
        conn.execute(
            text(
                f"""
                DELETE FROM {fqn()} a
                USING {fqn()} b
                WHERE a.ctid < b.ctid
                  AND lower(btrim(a.barcode_information)) = lower(btrim(b.barcode_information))
                """
            )
        )

    return upserted, deleted