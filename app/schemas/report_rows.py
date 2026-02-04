from __future__ import annotations
from pydantic import BaseModel
from typing import Any, Dict


class ReportRow(BaseModel):
    # i_daily_report는 테이블마다 컬럼이 다르니까 dict로 받는다
    row: Dict[str, Any]
