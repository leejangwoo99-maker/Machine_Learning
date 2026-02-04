from __future__ import annotations
from pydantic import BaseModel


class NonOpRow(BaseModel):
    end_day: str
    from_time: str
    to_time: str
    reason: str | None = None
    sparepart: str | None = None


class NonOpResponse(BaseModel):
    fct: list[NonOpRow]
    vision: list[NonOpRow]
