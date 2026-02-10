from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from app.db.session import get_db
from app.routers._common import norm_day, norm_shift

router = APIRouter(prefix="/pd_board_check", tags=["10.pd_board_check"])

SCHEMA = "e4_predictive_maintenance"
TABLE = "pd_board_check"

KST = ZoneInfo("Asia/Seoul")
DAY_START = "08:30:00"
DAY_END = "20:29:59"


def _resolve_kst_prod_day_shift() -> tuple[str, str]:
    now = datetime.now(KST)
    t = now.time()
    dstart = datetime.strptime(DAY_START, "%H:%M:%S").time()
    dend = datetime.strptime(DAY_END, "%H:%M:%S").time()
    if dstart <= t <= dend:
        return now.strftime("%Y%m%d"), "day"
    if t < dstart:
        prev = now.date().fromordinal(now.date().toordinal() - 1)
        return prev.strftime("%Y%m%d"), "night"
    return now.strftime("%Y%m%d"), "night"


def _load_columns(db: Session) -> list[str]:
    rows = db.execute(
        text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
        """),
        {"schema": SCHEMA, "table": TABLE},
    ).mappings().all()
    return [r["column_name"] for r in rows]


@router.get("")
def get_pd_board_check(
    end_day: Optional[str] = Query(None, description="YYYYMMDD (?놁쑝硫?KST ?먮룞)"),
    shift_type: Optional[str] = Query(None, description="day/night (?놁쑝硫?KST ?먮룞)"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        cols = _load_columns(db)
        if not cols:
            raise HTTPException(status_code=500, detail=f"{SCHEMA}.{TABLE} has no selectable columns")

        auto_day, auto_shift = _resolve_kst_prod_day_shift()
        d = norm_day(end_day) if end_day else auto_day
        s = norm_shift(shift_type) if shift_type else auto_shift

        day_col = "end_day" if "end_day" in cols else ("prod_day" if "prod_day" in cols else None)
        if day_col is None:
            raise HTTPException(status_code=500, detail=f"{SCHEMA}.{TABLE} requires end_day or prod_day column")

        where = [f"replace(cast(\"{day_col}\" as text), '-', '') = :d"]
        params = {"d": d}

        # shift_type 而щ읆 ?놁쑝硫?end_time 湲곗? 怨꾩궛
        if "shift_type" in cols:
            where.append('lower("shift_type") = :s')
            params["s"] = s
        elif "end_time" in cols:
            if s == "day":
                where.append('("end_time")::time BETWEEN TIME \'08:30:00\' AND TIME \'20:29:59\'')
            else:
                where.append('("end_time")::time < TIME \'08:30:00\' OR ("end_time")::time > TIME \'20:29:59\'')

        col_sql = ", ".join(f'"{c}"' for c in cols)
        sql = f"""
            SELECT {col_sql}
            FROM {SCHEMA}.{TABLE}
            WHERE {" AND ".join(where)}
            ORDER BY 1 DESC
        """
        rows = db.execute(text(sql), params).mappings().all()
        return Response(
            content=json.dumps([dict(r) for r in rows], ensure_ascii=False, default=str),
            media_type="application/json; charset=utf-8",
        )
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"pd_board_check db error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"pd_board_check get failed: {e}") from e
