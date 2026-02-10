# app/routers/alarm_record.py
from __future__ import annotations

from typing import Literal, List, Dict
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(tags=["8.alarm_record"])

SCHEMA = "g_production_film"
TABLE = "alarm_record"


# ---------------------------
# helpers
# ---------------------------
def _norm_day(v: str) -> str:
    """
    허용 입력:
      - YYYYMMDD
      - YYYY-MM-DD
    반환:
      - YYYYMMDD
    """
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


# ---------------------------
# routes
# ---------------------------
@router.get("/alarm_record/recent")
def get_alarm_record_recent(
    end_day: str = Query(..., description="YYYYMMDD or YYYY-MM-DD"),
    shift_type: Literal["day", "night"] = Query(...),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """
    알람 최근 목록 조회

    - end_day 입력 포맷: YYYYMMDD / YYYY-MM-DD 모두 허용
    - DB end_day 컬럼 타입(date/text) 혼재 대응:
        replace(end_day::text, '-', '') = :d8
    - shift window:
        day   = 08:30:00 <= end_time < 20:30:00
        night = end_time >= 20:30:00 OR end_time < 08:30:00
    - 반환: list[dict]  (프론트 기존 코드 호환)
    """
    try:
        d8 = _norm_day(end_day)
        sft = _norm_shift(shift_type)

        sql = text(
            f"""
            SELECT
                id,
                end_day::text                            AS end_day,
                end_time::text                           AS end_time,
                station::text                            AS station,
                COALESCE(sparepart::text, '')           AS sparepart,
                COALESCE(type_alarm::text, '')          AS type_alarm,
                COALESCE(amount::text, '')              AS amount,
                COALESCE(min_prob::text, '')            AS min_prob,
                created_at::text                         AS created_at
            FROM {SCHEMA}.{TABLE}
            WHERE replace(end_day::text, '-', '') = :d8
              AND (
                    (:sft = 'day'
                     AND end_time::time >= time '08:30:00'
                     AND end_time::time <  time '20:30:00')
                 OR (:sft = 'night'
                     AND (end_time::time >= time '20:30:00'
                          OR end_time::time <  time '08:30:00'))
              )
            ORDER BY
                replace(end_day::text, '-', '') DESC,
                end_time::time DESC,
                id DESC
            LIMIT :lim
            """
        )

        rows = db.execute(sql, {"d8": d8, "sft": sft, "lim": limit}).mappings().all()

        # 프론트에서 바로 리스트 처리 가능하게 list[dict] 반환
        items: List[Dict] = [dict(r) for r in rows]

        # message 키를 기대하는 프론트도 있어 기본 메시지 필드 보강
        # (이미 message/alarm이 있으면 덮어쓰지 않음)
        for it in items:
            if not it.get("message"):
                st = it.get("station", "")
                ta = it.get("type_alarm", "")
                sp = it.get("sparepart", "")
                am = it.get("amount", "")
                it["message"] = f"[{st}] {ta} / {sp} / {am}".strip()

        return items

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"alarm_record db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"alarm_record recent failed: {e}") from e
