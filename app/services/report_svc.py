from __future__ import annotations

from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings


def _idr_schema() -> str:
    # i_daily_report ?ㅽ궎留덈챸? settings?먯꽌 媛?몄삤?? ?놁쑝硫?湲곕낯媛믪쑝濡?fallback
    return getattr(settings, "IDR_SCHEMA", "i_daily_report")


def _table_fqn(schema: str, table: str) -> str:
    # ?뚯씠釉붾챸? double-quote濡?媛먯떥???덉쟾?섍쾶
    return f'{schema}."{table}"'


def _normalize_shift(shift_type: str) -> str:
    s = (shift_type or "").strip().lower()
    if s not in ("day", "night"):
        raise ValueError("shift_type must be 'day' or 'night'")
    return s


def _get_columns(engine: Engine, schema: str, table: str) -> list[str]:
    sql = text(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name = :table
        ORDER BY ordinal_position
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql, {"schema": schema, "table": table}).mappings().all()
    return [r["column_name"] for r in rows]


def fetch_report_rows_by_shift(
    engine: Engine,
    prod_day: str,
    shift_type: str,
    *,
    day_table: str,
    night_table: str,
    prod_day_col: str = "prod_day",
    exclude_cols: list[str] | None = None,
    mask_cols: list[str] | None = None,   # ??媛믩쭔 留덉뒪??null)
) -> list[dict[str, Any]]:
    """
    - i_daily_report ?ㅽ궎留덉뿉??shift_type(day/night)??留욌뒗 *_day_daily / *_night_daily ?뚯씠釉?議고쉶
    - exclude_cols: ?대떦 而щ읆???묐떟?먯꽌 ?쒓굅
    - mask_cols: ?대떦 而щ읆? ?묐떟???④린??媛믩쭔 null 泥섎━
    """
    schema = _idr_schema()
    shift = _normalize_shift(shift_type)

    table = day_table if shift == "day" else night_table
    fqn = _table_fqn(schema, table)

    all_cols = _get_columns(engine, schema, table)
    if not all_cols:
        return []

    excl = {c.strip() for c in (exclude_cols or []) if c and c.strip()}
    mask = {c.strip() for c in (mask_cols or []) if c and c.strip()}

    # ?ㅼ젣 議댁옱?섎뒗 而щ읆留?諛섏쁺
    excl = {c for c in excl if c in all_cols}
    mask = {c for c in mask if c in all_cols and c not in excl}

    # prod_day 議곌굔 而щ읆 議댁옱 泥댄겕(?쒖쇅?섎뜑?쇰룄 WHERE???꾩슂)
    if prod_day_col not in all_cols:
        raise ValueError(f"{schema}.{table} has no column '{prod_day_col}'")

    select_cols = [c for c in all_cols if c not in excl]
    if not select_cols:
        return []

    # SELECT 由ъ뒪??援ъ꽦: 留덉뒪??而щ읆? NULL AS "col"
    select_exprs: list[str] = []
    for c in select_cols:
        if c in mask:
            select_exprs.append(f'NULL AS "{c}"')
        else:
            select_exprs.append(f'"{c}"')

    select_list = ", ".join(select_exprs)

    sql = text(
        f"""
        SELECT {select_list}
        FROM {fqn}
        WHERE "{prod_day_col}" = :prod_day
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"prod_day": prod_day}).mappings().all()

    return [dict(r) for r in rows]
