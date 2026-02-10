# app/routers/worker_info.py
from __future__ import annotations

from typing import List, Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(tags=["worker_info"])

SCHEMA = "g_production_film"
TABLE = "worker_info"


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


class WorkerInfoRow(BaseModel):
    end_day: str = Field(..., description="YYYYMMDD")
    shift_type: Literal["day", "night"]
    worker_name: Optional[str] = ""
    order_number: Optional[str] = ""


class WorkerInfoSyncBody(BaseModel):
    rows: List[WorkerInfoRow] = Field(default_factory=list)


@router.get("/worker_info")
def get_worker_info(
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: Literal["day", "night"] = Query(..., description="day|night"),
    limit: int = Query(200, ge=1, le=5000),
    db: Session = Depends(get_db),
):
    try:
        d = _norm_day(end_day)
        s = _norm_shift(shift_type)

        rows = db.execute(
            text(
                f"""
                SELECT
                    end_day::text     AS end_day,
                    shift_type::text  AS shift_type,
                    worker_name::text AS worker_name,
                    order_number::text AS order_number
                FROM {SCHEMA}.{TABLE}
                WHERE end_day = :end_day
                  AND shift_type = :shift_type
                ORDER BY worker_name NULLS LAST
                LIMIT :limit
                """
            ),
            {"end_day": d, "shift_type": s, "limit": limit},
        ).mappings().all()

        return [dict(r) for r in rows]

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"worker_info db error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"worker_info failed: {e}") from e


@router.post("/worker_info/sync")
def post_worker_info_sync(
    body: WorkerInfoSyncBody,
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: Literal["day", "night"] = Query(..., description="day|night"),
    db: Session = Depends(get_db),
):
    try:
        d = _norm_day(end_day)
        s = _norm_shift(shift_type)

        # 같은 날짜/주야간 기존 데이터 정리 후 재적재(기존 운영 방식 맞춤)
        db.execute(
            text(
                f"""
                DELETE FROM {SCHEMA}.{TABLE}
                WHERE end_day = :end_day
                  AND shift_type = :shift_type
                """
            ),
            {"end_day": d, "shift_type": s},
        )

        ins_sql = text(
            f"""
            INSERT INTO {SCHEMA}.{TABLE}
            (end_day, shift_type, worker_name, order_number)
            VALUES (:end_day, :shift_type, :worker_name, :order_number)
            """
        )

        count = 0
        for row in body.rows:
            wn = (row.worker_name or "").strip()
            on = (row.order_number or "").strip()
            if not wn and not on:
                continue

            db.execute(
                ins_sql,
                {
                    "end_day": d,
                    "shift_type": s,
                    "worker_name": wn,
                    "order_number": on,
                },
            )
            count += 1

        db.commit()
        return {"ok": True, "saved": count, "end_day": d, "shift_type": s}

    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"worker_info sync db error: {e}") from e
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"worker_info sync failed: {e}") from e
