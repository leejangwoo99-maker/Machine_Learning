from __future__ import annotations

from pydantic import BaseModel


class AlarmRecordOut(BaseModel):
    end_day: str
    end_time: str
    station: str | None = None
    sparepart: str | None = None
    type_alarm: str | None = None
