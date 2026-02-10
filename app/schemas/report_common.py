from __future__ import annotations

from typing import Any, Dict, List
from pydantic import BaseModel, Field


class RowsResponse(BaseModel):
    ok: bool = True
    count: int = 0
    items: List[Dict[str, Any]] = Field(default_factory=list)


class ReportResponse(BaseModel):
    ok: bool = True
    summary: Dict[str, Any] = Field(default_factory=dict)
    items: List[Dict[str, Any]] = Field(default_factory=list)
