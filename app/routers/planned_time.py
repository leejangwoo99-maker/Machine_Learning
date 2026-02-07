from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Optional, List

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from app.core.db import make_engine
from app.core.event_bus import event_bus

router = APIRouter(prefix="/planned_time", tags=["6.planned_time"])
KST = timezone(timedelta(hours=9))


def get_engine() -> Engine:
    return make_engine()


# -------------------------
# Pydantic models
# -------------------------
class PlannedTimeRowIn(BaseModel):
    from_time: str
    to_time: str
    reason: Optional[str] = None
    end_day: Optional[str] = None  # YYYYMMDD

    @field_validator("from_time", "to_time")
    @classmethod
    def validate_hms(cls, v: str) -> str:
        """
        입력 허용:
        - HH:MM:SS
        - HH:MM:SS.xx  (들어오면 잘라서 HH:MM:SS로 저장)
        """
        s = (v or "").strip()
        if not s:
            raise ValueError("time is required")

        # 소수점 제거 (HH:MM:SS.00 -> HH:MM:SS)
        if "." in s:
            s = s.split(".", 1)[0].strip()

        parts = s.split(":")
        if len(parts) != 3:
            raise ValueError("time must be HH:MM:SS")

        hh, mm, ss = parts
        if not (hh.isdigit() and mm.isdigit() and ss.isdigit()):
            raise ValueError("time must be numeric HH:MM:SS")

        h, m, sec = int(hh), int(mm), int(ss)
        if not (0 <= h <= 23 and 0 <= m <= 59 and 0 <= sec <= 59):
            raise ValueError("invalid time range")

        return f"{h:02d}:{m:02d}:{sec:02d}"

    @field_validator("end_day")
    @classmethod
    def validate_end_day(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = v.strip()
        if not s:
            return None
        if len(s) != 8 or not s.isdigit():
            raise ValueError("end_day must be YYYYMMDD")
        return s


class PlannedTimeSyncIn(BaseModel):
    rows: List[PlannedTimeRowIn]


def _today_yyyymmdd() -> str:
    return datetime.now(KST).strftime("%Y%m%d")


# -------------------------
# APIs
# -------------------------
@router.get("/today")
def get_today(engine: Engine = Depends(get_engine)):
    """
    오늘 planned_time 조회
    - 화면 요구사항: end_day는 반환하지 않음
    - from_time/to_time은 HH:MM:SS로 반환
    """
    today = _today_yyyymmdd()

    sql = text("""
        SELECT
            to_char(from_time::time, 'HH24:MI:SS') AS from_time,
            to_char(to_time::time,   'HH24:MI:SS') AS to_time,
            reason
        FROM g_production_film.planned_time
        WHERE end_day = :end_day
        ORDER BY from_time, to_time
    """)

    try:
        with engine.connect() as conn:
            rows = conn.execute(sql, {"end_day": today}).mappings().all()
        return [dict(r) for r in rows]
    except SQLAlchemyError as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e.__class__.__name__}")


@router.post("/today")
async def post_today(body: PlannedTimeRowIn, engine: Engine = Depends(get_engine)):
    """
    단건 upsert
    - key: (end_day, from_time, to_time)
    - 비밀번호 불필요
    """
    end_day = body.end_day or _today_yyyymmdd()

    upsert_sql = text("""
        INSERT INTO g_production_film.planned_time (end_day, from_time, to_time, reason)
        VALUES (:end_day, :from_time, :to_time, :reason)
        ON CONFLICT (end_day, from_time, to_time)
        DO UPDATE SET reason = EXCLUDED.reason
    """)

    try:
        with engine.begin() as conn:
            conn.execute(
                upsert_sql,
                {
                    "end_day": end_day,
                    "from_time": body.from_time,
                    "to_time": body.to_time,
                    "reason": body.reason,
                },
            )

        await event_bus.publish(
            "planned_time_event",
            {
                "end_day": end_day,
                "from_time": body.from_time,
                "to_time": body.to_time,
                "ts_kst": datetime.now(KST).isoformat(),
            },
        )
        return {"ok": True, "upserted": 1}
    except SQLAlchemyError as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e.__class__.__name__}")


@router.post("/sync")
async def post_sync(body: PlannedTimeSyncIn, engine: Engine = Depends(get_engine)):
    """
    전체 동기화
    - 비밀번호 불필요
    - rows 기준으로 해당 end_day 데이터 완전 동기화
    """
    # 빈 rows 허용: 오늘 데이터 전체 삭제 동작
    normalized_rows = body.rows or []

    # end_day별 그룹
    by_day: dict[str, list[PlannedTimeRowIn]] = {}
    for r in normalized_rows:
        d = r.end_day or _today_yyyymmdd()
        by_day.setdefault(d, []).append(r)

    # rows가 아예 비어있으면 today 대상으로 삭제
    if not by_day:
        by_day[_today_yyyymmdd()] = []

    try:
        with engine.begin() as conn:
            for end_day, rows in by_day.items():
                # 현재 DB
                cur = conn.execute(
                    text("""
                        SELECT
                            to_char(from_time::time, 'HH24:MI:SS') AS from_time,
                            to_char(to_time::time,   'HH24:MI:SS') AS to_time,
                            COALESCE(reason, '') AS reason
                        FROM g_production_film.planned_time
                        WHERE end_day = :end_day
                    """),
                    {"end_day": end_day},
                ).mappings().all()

                current_set = {
                    (str(x["from_time"]), str(x["to_time"]), str(x["reason"]))
                    for x in cur
                }
                target_set = {
                    (r.from_time, r.to_time, (r.reason or ""))
                    for r in rows
                }

                # delete: 시간키 기준으로 target에 없는 것 삭제
                current_time_keys = {(a, b) for a, b, _ in current_set}
                target_time_keys = {(a, b) for a, b, _ in target_set}
                to_delete_keys = current_time_keys - target_time_keys

                for ft, tt in to_delete_keys:
                    conn.execute(
                        text("""
                            DELETE FROM g_production_film.planned_time
                            WHERE end_day = :end_day
                              AND from_time = :from_time
                              AND to_time   = :to_time
                        """),
                        {"end_day": end_day, "from_time": ft, "to_time": tt},
                    )

                # upsert: target 반영
                for ft, tt, rs in target_set:
                    conn.execute(
                        text("""
                            INSERT INTO g_production_film.planned_time (end_day, from_time, to_time, reason)
                            VALUES (:end_day, :from_time, :to_time, :reason)
                            ON CONFLICT (end_day, from_time, to_time)
                            DO UPDATE SET reason = EXCLUDED.reason
                        """),
                        {
                            "end_day": end_day,
                            "from_time": ft,
                            "to_time": tt,
                            "reason": rs if rs != "" else None,
                        },
                    )

        await event_bus.publish(
            "planned_time_event",
            {"sync": True, "days": sorted(by_day.keys()), "ts_kst": datetime.now(KST).isoformat()},
        )
        return {"ok": True, "synced_days": sorted(by_day.keys())}

    except SQLAlchemyError as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e.__class__.__name__}")
