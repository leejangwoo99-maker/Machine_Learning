from __future__ import annotations
from pydantic import BaseModel
from typing import Any, Dict


class ReportRow(BaseModel):
    # i_daily_report???뚯씠釉붾쭏??而щ읆???ㅻⅤ?덇퉴 dict濡?諛쏅뒗??
    row: Dict[str, Any]
