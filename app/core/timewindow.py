from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from fastapi import HTTPException
from app.core.config import settings


TZ = ZoneInfo(settings.TZ)

DAY_START = time(8, 30, 0)
DAY_END = time(20, 29, 59)
NIGHT_START = time(20, 30, 0)
NIGHT_END = time(8, 29, 59)


@dataclass(frozen=True)
class ShiftKey:
    end_day: str
    shift_type: str  # "day" | "night"


def now_kst() -> datetime:
    return datetime.now(tz=TZ)


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def validate_yyyymmdd(s: str) -> str:
    if len(s) != 8 or not s.isdigit():
        raise HTTPException(status_code=422, detail="date must be YYYYMMDD")
    return s


def validate_shift_type(shift_type: str) -> str:
    st = (shift_type or "").lower()
    if st not in ("day", "night"):
        raise HTTPException(status_code=422, detail="shift_type must be 'day' or 'night'")
    return st


def current_shift_by_now(now: datetime | None = None) -> ShiftKey:
    now = now or now_kst()
    t = now.timetz().replace(tzinfo=None)

    if DAY_START <= t <= DAY_END:
        return ShiftKey(end_day=yyyymmdd(now), shift_type="day")

    if t >= NIGHT_START:
        return ShiftKey(end_day=yyyymmdd(now), shift_type="night")

    prev = now - timedelta(days=1)
    return ShiftKey(end_day=yyyymmdd(prev), shift_type="night")


def enforce_current_shift(end_day: str, shift_type: str) -> None:
    cur = current_shift_by_now()
    if end_day != cur.end_day or shift_type != cur.shift_type:
        raise HTTPException(
            status_code=403,
            detail=f"Write forbidden: current shift is {cur.end_day} {cur.shift_type}",
        )
