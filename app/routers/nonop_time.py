# app/routers/nonop_time.py
from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError, DBAPIError

# 프로젝트 경로 자동 대응
try:
    from app.database import get_db  # type: ignore
except Exception:
    from app.db.session import get_db  # type: ignore

router = APIRouter(prefix="/non_operation_time", tags=["7.non_operation_time"])

SCHEMA = "g_production_film"
TABLE = "fct_non_operation_time"

COL_END_DAY = "end_day"
COL_STATION = "station"
COL_FROM = "from_time"
COL_TO = "to_time"
COL_REASON = "reason"
COL_SPARE = "sparepart"


class NonOpSyncRow(BaseModel):
    end_day: str
    station: str
    from_time: str
    to_time: str
    reason: Optional[str] = None
    sparepart: Optional[str] = None

    @field_validator("end_day", "station", "from_time", "to_time")
    @classmethod
    def _required_strip(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            raise ValueError("required")
        return s

    @field_validator("reason", "sparepart")
    @classmethod
    def _optional_strip(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = v.strip()
        return s if s else None


class NonOpSyncIn(BaseModel):
    rows: List[NonOpSyncRow]


def _parse_yyyymmdd(s: str) -> datetime:
    s = (s or "").strip()
    try:
        return datetime.strptime(s, "%Y%m%d")
    except ValueError:
        raise HTTPException(status_code=422, detail="end_day must be YYYYMMDD")


def _window_by_shift(end_day: str, shift_type: str) -> tuple[datetime, datetime]:
    """
    KST 기준 생산 분류:
      - day   : [D] 08:30:00 ~ [D]   20:29:59
      - night : [D] 20:30:00 ~ [D+1] 08:29:59
    """
    base = _parse_yyyymmdd(end_day)
    st = (shift_type or "").strip().lower()

    if st == "day":
        start_dt = base.replace(hour=8, minute=30, second=0, microsecond=0)
        end_dt = base.replace(hour=20, minute=29, second=59, microsecond=0)
        return start_dt, end_dt

    if st == "night":
        start_dt = base.replace(hour=20, minute=30, second=0, microsecond=0)
        next_day = base + timedelta(days=1)
        end_dt = next_day.replace(hour=8, minute=29, second=59, microsecond=0)
        return start_dt, end_dt

    raise HTTPException(status_code=422, detail="shift_type must be 'day' or 'night'")


@router.get("")
def get_non_operation_time(
    end_day: str,
    shift_type: str,
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    """
    조회:
      - end_day(YYYYMMDD), shift_type(day/night) 기반
      - KST 주/야간 윈도우로 정확히 분류
      - 예) end_day=20260204, shift_type=night
            => 2026-02-04 20:30:00 ~ 2026-02-05 08:29:59
    """
    try:
        start_dt, end_dt = _window_by_shift(end_day, shift_type)

        # end_day + from_time을 합쳐 timestamp로 비교
        # from_time 문자열이 'HH:MM:SS.xx' 형태일 수 있어 split_part로 소수부 제거
        rows = db.execute(
            text(f"""
                WITH base AS (
                    SELECT
                        {COL_END_DAY}::text AS {COL_END_DAY},
                        {COL_STATION}::text AS {COL_STATION},
                        {COL_FROM}::text AS {COL_FROM},
                        {COL_TO}::text   AS {COL_TO},
                        {COL_REASON},
                        {COL_SPARE},
                        to_timestamp(
                            {COL_END_DAY}::text || ' ' || split_part({COL_FROM}::text, '.', 1),
                            'YYYYMMDD HH24:MI:SS'
                        ) AS from_ts
                    FROM {SCHEMA}.{TABLE}
                )
                SELECT
                    {COL_END_DAY},
                    {COL_STATION},
                    {COL_FROM},
                    {COL_TO},
                    {COL_REASON},
                    {COL_SPARE}
                FROM base
                WHERE from_ts >= :start_dt
                  AND from_ts <= :end_dt
                ORDER BY from_ts, {COL_STATION}, {COL_FROM}, {COL_TO}
            """),
            {
                "start_dt": start_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "end_dt": end_dt.strftime("%Y-%m-%d %H:%M:%S"),
            },
        ).mappings().all()

        # 응답 포맷 정리 (time은 HH:MM:SS까지만)
        out: List[Dict[str, Any]] = []
        for r in rows:
            d = dict(r)
            d[COL_FROM] = str(d.get(COL_FROM, "")).split(".", 1)[0]
            d[COL_TO] = str(d.get(COL_TO, "")).split(".", 1)[0]
            out.append(d)

        return out

    except HTTPException:
        raise
    except (OperationalError, DBAPIError):
        raise HTTPException(status_code=503, detail="DB unavailable")


@router.post("/sync")
def post_non_operation_time_sync(
    body: NonOpSyncIn,
    db: Session = Depends(get_db),
):
    """
    전체 동기화(비밀번호 없음):
      - 기준 집합: body.rows (end_day + station + from_time + to_time)
      - 같은 end_day의 DB 기존 데이터와 비교:
          * target에만 있는 키는 INSERT
          * current에만 있는 키는 DELETE
          * 공통 키는 reason/sparepart UPDATE
    """
    try:
        if not body.rows:
            raise HTTPException(status_code=422, detail="rows is required")

        # rows를 end_day별로 처리
        by_day: Dict[str, Dict[tuple, Dict[str, Any]]] = {}
        for r in body.rows:
            key = (r.station, r.from_time, r.to_time)
            day_map = by_day.setdefault(r.end_day, {})
            # 동일 key 중 마지막 값 우선
            day_map[key] = {
                "end_day": r.end_day,
                "station": r.station,
                "from_time": r.from_time,
                "to_time": r.to_time,
                "reason": r.reason,
                "sparepart": r.sparepart,
            }

        inserted = 0
        updated = 0
        deleted = 0

        for end_day, target_map in by_day.items():
            current_rows = db.execute(
                text(f"""
                    SELECT
                        {COL_END_DAY} AS end_day,
                        {COL_STATION} AS station,
                        {COL_FROM} AS from_time,
                        {COL_TO} AS to_time,
                        {COL_REASON} AS reason,
                        {COL_SPARE} AS sparepart
                    FROM {SCHEMA}.{TABLE}
                    WHERE {COL_END_DAY} = :end_day
                """),
                {"end_day": end_day},
            ).mappings().all()

            current_map: Dict[tuple, Dict[str, Any]] = {}
            for c in current_rows:
                k = (str(c["station"]), str(c["from_time"]), str(c["to_time"]))
                current_map[k] = dict(c)

            current_keys = set(current_map.keys())
            target_keys = set(target_map.keys())

            to_insert = target_keys - current_keys
            to_delete = current_keys - target_keys
            to_update = current_keys & target_keys

            # INSERT
            for k in sorted(to_insert):
                row = target_map[k]
                r = db.execute(
                    text(f"""
                        INSERT INTO {SCHEMA}.{TABLE}
                        ({COL_END_DAY}, {COL_STATION}, {COL_FROM}, {COL_TO}, {COL_REASON}, {COL_SPARE})
                        VALUES (:end_day, :station, :from_time, :to_time, :reason, :sparepart)
                    """),
                    row,
                )
                inserted += int(r.rowcount or 0)

            # UPDATE (reason/sparepart만)
            for k in sorted(to_update):
                cur = current_map[k]
                tgt = target_map[k]
                if (cur.get("reason") != tgt.get("reason")) or (cur.get("sparepart") != tgt.get("sparepart")):
                    r = db.execute(
                        text(f"""
                            UPDATE {SCHEMA}.{TABLE}
                            SET {COL_REASON} = :reason,
                                {COL_SPARE} = :sparepart
                            WHERE {COL_END_DAY} = :end_day
                              AND {COL_STATION} = :station
                              AND {COL_FROM} = :from_time
                              AND {COL_TO} = :to_time
                        """),
                        tgt,
                    )
                    updated += int(r.rowcount or 0)

            # DELETE
            for k in sorted(to_delete):
                station, from_time, to_time = k
                r = db.execute(
                    text(f"""
                        DELETE FROM {SCHEMA}.{TABLE}
                        WHERE {COL_END_DAY} = :end_day
                          AND {COL_STATION} = :station
                          AND {COL_FROM} = :from_time
                          AND {COL_TO} = :to_time
                    """),
                    {
                        "end_day": end_day,
                        "station": station,
                        "from_time": from_time,
                        "to_time": to_time,
                    },
                )
                deleted += int(r.rowcount or 0)

        db.commit()
        return {
            "ok": True,
            "inserted": inserted,
            "updated": updated,
            "deleted": deleted,
            "total_after": sum(len(v) for v in by_day.values()),
        }

    except HTTPException:
        db.rollback()
        raise
    except (OperationalError, DBAPIError):
        db.rollback()
        raise HTTPException(status_code=503, detail="DB unavailable")
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"sync failed: {e}")
