from __future__ import annotations
from typing import Optional, List
from pydantic import BaseModel, field_validator
from app.core.daykey import norm_day_key, norm_shift

class WorkerInfoIn(BaseModel):
    end_day: str
    shift_type: str
    worker_name: str
    order_number: Optional[str] = None

    @field_validator("end_day")
    @classmethod
    def _day(cls, v: str) -> str: return norm_day_key(v)

    @field_validator("shift_type")
    @classmethod
    def _shift(cls, v: str) -> str: return norm_shift(v)

    @field_validator("worker_name")
    @classmethod
    def _worker(cls, v: str) -> str:
        s=(v or "").strip()
        if not s: raise ValueError("required")
        return s

class WorkerInfoSyncIn(BaseModel):
    rows: List[WorkerInfoIn]
