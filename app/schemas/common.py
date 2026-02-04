from __future__ import annotations
from pydantic import BaseModel, Field


class Ok(BaseModel):
    ok: bool = True


class SyncResult(BaseModel):
    inserted: int = Field(0, ge=0)
    deleted: int = Field(0, ge=0)
