from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict


class NonOpRow(BaseModel):
    end_day: str = Field(..., description="YYYYMMDD")
    station: str = Field(..., description="FCT1/FCT2/FCT3/FCT4/Vision1/Vision2")
    from_time: str = Field(..., description="HH:MM:SS(.ff)")
    to_time: str = Field(..., description="HH:MM:SS(.ff)")
    reason: str | None = Field(default=None, description="비가동 사유")
    sparepart: str | None = Field(default=None, description="교체 부품명")


class NonOpListResponse(BaseModel):
    rows: list[NonOpRow]


class NonOpUpsertIn(BaseModel):
    """
    key 4개(end_day, station, from_time, to_time)로 대상 row를 특정하고,
    reason/sparepart만 수정한다. (INSERT 없음)
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "end_day": "20260205",
                "station": "FCT1",
                "from_time": "13:32:17.95",
                "to_time": "13:32:35.81",
                "reason": "휴식",
                "sparepart": "usb_a"
            }
        }
    )

    end_day: str = Field(..., description="YYYYMMDD (키)")
    station: str = Field(..., description="설비명 (키)")
    from_time: str = Field(..., description="시작시각 HH:MM:SS(.ff) (키)")
    to_time: str = Field(..., description="종료시각 HH:MM:SS(.ff) (키)")
    reason: str | None = Field(default=None, description="수정할 비가동 사유 (null 가능)")
    sparepart: str | None = Field(default=None, description="수정할 부품명 (null 가능)")


class NonOpUpsertOut(BaseModel):
    ok: bool = True
    event_name: str = "non_operation_time_event"


class NonOpKeyPayload(BaseModel):
    end_day: str
    station: str
    from_time: str
    to_time: str
