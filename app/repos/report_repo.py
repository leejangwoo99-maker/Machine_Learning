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
    - Postgres에서 updated_at을 SELECT에서 제외하려고 컬럼 목록을 information_schema로 읽어서 동적 SELECT.
    - 테이블/컬럼 편차가 있어도 robust.
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

        # 응답에서 updated_at key 자체 제거 => SELECT에서 제외
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
