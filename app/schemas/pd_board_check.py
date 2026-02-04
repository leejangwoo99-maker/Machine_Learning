from __future__ import annotations

from pydantic import BaseModel
from typing import Any


class PdBoardCheckOut(BaseModel):
    station: str | None = None
    end_day: str
    last_status: str | None = None

    # DB에 dict(JSON)로 들어가 있으므로 그대로 내려보내기
    # 예: {"x":[...], "y":[...], "th":0.7, "type":"timeseries"}
    cosine_similarity: dict[str, Any] | None = None
