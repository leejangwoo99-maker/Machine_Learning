from __future__ import annotations
from pydantic import BaseModel, Field


class WorkerInfoIn(BaseModel):
    end_day: str = Field(..., description="YYYYMMDD")
    shift_type: str = Field(..., description="day|night")
    worker_name: str
    order_number: str | None = None


class WorkerInfoOut(BaseModel):
    end_day: str
    shift_type: str
    worker_name: str
    order_number: str | None = None
