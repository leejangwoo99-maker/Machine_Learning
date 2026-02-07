# app/schemas/worker_info.py
from __future__ import annotations

from typing import Optional, List
from pydantic import BaseModel, field_validator


class WorkerInfoIn(BaseModel):
    end_day: str
    shift_type: str
    worker_name: str
    order_number: Optional[str] = None

    @field_validator("end_day", "shift_type", "worker_name")
    @classmethod
    def _required_strip(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            raise ValueError("required")
        return s

    @field_validator("order_number")
    @classmethod
    def _optional_strip(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = v.strip()
        return s if s else None


class WorkerInfoSyncIn(BaseModel):
    rows: List[WorkerInfoIn]


class WorkerInfoOut(BaseModel):
    end_day: str
    shift_type: str
    worker_name: str
    order_number: Optional[str] = None
