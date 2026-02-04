from __future__ import annotations
from pydantic import BaseModel, Field


class EmailListSyncIn(BaseModel):
    emails: list[str] = Field(default_factory=list)


class EmailListOut(BaseModel):
    email_list: str
