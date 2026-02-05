from __future__ import annotations

from sqlalchemy.engine import Engine

from app.repos import nonop_time_repo
from app.schemas.nonop_time import (
    NonOpListResponse,
    NonOpUpsertIn,
    NonOpUpsertOut,
    NonOpKeyPayload,
)


def _validate_yyyymmdd(s: str) -> str:
    t = (s or "").strip().replace("-", "")
    if len(t) != 8 or not t.isdigit():
        raise ValueError("end_day must be YYYYMMDD")
    return t


def _validate_shift(s: str) -> str:
    v = (s or "").strip().lower()
    if v not in ("day", "night"):
        raise ValueError("shift_type must be 'day' or 'night'")
    return v


def get_nonop(engine: Engine, end_day: str, shift_type: str) -> NonOpListResponse:
    end_day = _validate_yyyymmdd(end_day)
    shift_type = _validate_shift(shift_type)
    rows = nonop_time_repo.list_all_by_day_shift(engine, end_day, shift_type)
    return NonOpListResponse(rows=rows)


def upsert_nonop(engine: Engine, item: NonOpUpsertIn) -> tuple[NonOpUpsertOut, NonOpKeyPayload]:
    end_day = _validate_yyyymmdd(item.end_day)

    nonop_time_repo.update_reason_sparepart_by_key(
        engine=engine,
        end_day=end_day,
        station=item.station,
        from_time=item.from_time,
        to_time=item.to_time,
        reason=item.reason,
        sparepart=item.sparepart,
    )

    key_payload = NonOpKeyPayload(
        end_day=end_day,
        station=item.station,
        from_time=item.from_time,
        to_time=item.to_time,
    )
    return NonOpUpsertOut(ok=True), key_payload
