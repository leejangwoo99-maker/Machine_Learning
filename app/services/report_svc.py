from __future__ import annotations

from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings


def _idr_schema() -> str:
    # i_daily_report 스키마명은 settings에서 가져오되, 없으면 기본값으로 fallback
    return getattr(settings, "IDR_SCHEMA", "i_daily_report")


def _table_fqn(schema: str, table: str) -> str:
    # 테이블명은 double-quote로 감싸서 안전하게
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
) -> list[dict[str, Any]]:
    """
    - i_daily_report 스키마에서 shift_type(day/night)에 맞는 *_day_daily / *_night_daily 테이블 조회
    - 기본은 기존대로 "모든 컬럼" 반환
    - exclude_cols가 주어지면 해당 컬럼들을 SELECT에서 제외
    """
    schema = _idr_schema()
    shift = _normalize_shift(shift_type)

    table = day_table if shift == "day" else night_table
    fqn = _table_fqn(schema, table)

    cols = _get_columns(engine, schema, table)
    if not cols:
        return []

    if exclude_cols:
        excl = {c.strip() for c in exclude_cols if c and c.strip()}
        cols = [c for c in cols if c not in excl]

    if not cols:
        return []

    # prod_day 조건 컬럼이 실제로 존재하는지 방어
    if prod_day_col not in cols:
        # 조회 컬럼엔 없더라도 WHERE는 해야 하니, 테이블 전체 컬럼 목록에서 확인
        # (exclude_cols로 prod_day_col을 빼버린 경우도 있을 수 있어서)
        all_cols = _get_columns(engine, schema, table)
        if prod_day_col not in all_cols:
            raise ValueError(f"{schema}.{table} has no column '{prod_day_col}'")

    select_list = ", ".join([f'"{c}"' for c in cols])

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
