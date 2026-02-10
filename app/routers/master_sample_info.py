# app/routers/master_sample_info.py
from __future__ import annotations

import json

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/mastersample_test_info", tags=["6.mastersample_test_info"])

SCHEMA = "i_daily_report"
DAY_TABLE = "e_mastersample_test_day_daily"
NIGHT_TABLE = "e_mastersample_test_night_daily"


def _norm_day(v: str) -> str:
    s = (v or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        s = s.replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError("end_day must be YYYYMMDD or YYYY-MM-DD")
    return s


def _norm_shift(v: str) -> str:
    s = (v or "").strip().lower()
    if s not in {"day", "night"}:
        raise ValueError("shift_type must be day/night")
    return s


def _pick_table(shift_type: str) -> str:
    return DAY_TABLE if shift_type == "day" else NIGHT_TABLE


@router.get("")
def get_mastersample_test_info(
    end_day: str,
    shift_type: str,
    db: Session = Depends(get_db),
) -> Response:
    try:
        d = _norm_day(end_day)
        s = _norm_shift(shift_type)
        table = _pick_table(s)

        rows = db.execute(
            text(
                f"""
                SELECT
                    "prod_day",
                    "shift_type",
                    "Mastersample",
                    "first_time"
                FROM {SCHEMA}.{table}
                WHERE "prod_day" = :prod_day
                  AND lower("shift_type") = :shift_type
                ORDER BY "Mastersample"
                """
            ),
            {"prod_day": d, "shift_type": s},
        ).mappings().all()

        body = json.dumps([dict(r) for r in rows], ensure_ascii=False, default=str)
        return Response(content=body, media_type="application/json; charset=utf-8")

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        # DB timeout/연결끊김 계열
        raise HTTPException(status_code=503, detail=f"mastersample_test_info db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"mastersample_test_info get failed: {e}") from e
