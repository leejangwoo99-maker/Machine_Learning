from __future__ import annotations
from typing import Any
from sqlalchemy import text
from sqlalchemy.engine import Engine


def list_rows_excluding_updated_at(
    engine: Engine,
    schema: str,
    table: str,
    prod_day: str,
    prod_day_col: str = "prod_day",
) -> list[dict[str, Any]]:
    """
    - Postgres?먯꽌 updated_at??SELECT?먯꽌 ?쒖쇅?섎젮怨?而щ읆 紐⑸줉??information_schema濡??쎌뼱???숈쟻 SELECT.
    - ?뚯씠釉?而щ읆 ?몄감媛 ?덉뼱??robust.
    """
    cols_sql = text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
    """)

    with engine.connect() as conn:
        cols = conn.execute(cols_sql, {"schema": schema, "table": table}).scalars().all()

        # ?묐떟?먯꽌 updated_at key ?먯껜 ?쒓굅 => SELECT?먯꽌 ?쒖쇅
        cols = [c for c in cols if c != "updated_at"]
        if not cols:
            return []

        select_list = ", ".join([f'"{c}"' for c in cols])

        data_sql = text(f"""
            SELECT {select_list}
            FROM "{schema}"."{table}"
            WHERE "{prod_day_col}" = :prod_day
        """)

        rows = conn.execute(data_sql, {"prod_day": prod_day}).mappings().all()

    return [dict(r) for r in rows]
