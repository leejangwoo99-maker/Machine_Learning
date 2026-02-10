from __future__ import annotations

from pydantic import BaseModel


class MastersampleOut(BaseModel):
    prod_day: str               # YYYYMMDD
    shift_type: str             # "day" | "night"
    Mastersample: str | None = None
    first_time: str | None = None
