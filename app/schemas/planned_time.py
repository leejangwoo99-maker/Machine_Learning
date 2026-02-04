from __future__ import annotations
from pydantic import BaseModel, Field


class PlannedTimeIn(BaseModel):
    from_time: str = Field(..., description="text time: HH:MM:SS or HH:MM:SS.xx")
    to_time: str = Field(..., description="text time: HH:MM:SS or HH:MM:SS.xx")
    reason: str


class PlannedTimeOut(BaseModel):
    end_day: str
    from_time: str
    to_time: str
    reason: str
