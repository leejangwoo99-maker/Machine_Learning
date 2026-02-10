from __future__ import annotations

from pydantic import BaseModel
from typing import Any


class PdBoardCheckOut(BaseModel):
    station: str | None = None
    end_day: str
    last_status: str | None = None

    # DB??dict(JSON)濡??ㅼ뼱媛 ?덉쑝誘濡?洹몃?濡??대젮蹂대궡湲?
    # ?? {"x":[...], "y":[...], "th":0.7, "type":"timeseries"}
    cosine_similarity: dict[str, Any] | None = None
