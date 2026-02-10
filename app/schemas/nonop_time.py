from __future__ import annotations

from pydantic import BaseModel, Field, ConfigDict


class NonOpRow(BaseModel):
    end_day: str = Field(..., description="YYYYMMDD")
    station: str = Field(..., description="FCT1/FCT2/FCT3/FCT4/Vision1/Vision2")
    from_time: str = Field(..., description="HH:MM:SS(.ff)")
    to_time: str = Field(..., description="HH:MM:SS(.ff)")
    reason: str | None = Field(default=None, description="鍮꾧????ъ쑀")
    sparepart: str | None = Field(default=None, description="援먯껜 遺?덈챸")


class NonOpListResponse(BaseModel):
    rows: list[NonOpRow]


class NonOpUpsertIn(BaseModel):
    """
    key 4媛?end_day, station, from_time, to_time)濡????row瑜??뱀젙?섍퀬,
    reason/sparepart留??섏젙?쒕떎. (INSERT ?놁쓬)
    """
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "end_day": "20260205",
                "station": "FCT1",
                "from_time": "13:32:17.95",
                "to_time": "13:32:35.81",
                "reason": "?댁떇",
                "sparepart": "usb_a"
            }
        }
    )

    end_day: str = Field(..., description="YYYYMMDD (??")
    station: str = Field(..., description="?ㅻ퉬紐?(??")
    from_time: str = Field(..., description="?쒖옉?쒓컖 HH:MM:SS(.ff) (??")
    to_time: str = Field(..., description="醫낅즺?쒓컖 HH:MM:SS(.ff) (??")
    reason: str | None = Field(default=None, description="?섏젙??鍮꾧????ъ쑀 (null 媛??")
    sparepart: str | None = Field(default=None, description="?섏젙??遺?덈챸 (null 媛??")


class NonOpUpsertOut(BaseModel):
    ok: bool = True
    event_name: str = "non_operation_time_event"


class NonOpKeyPayload(BaseModel):
    end_day: str
    station: str
    from_time: str
    to_time: str
