# app/schemas/demon_health.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from pydantic import BaseModel
from typing import Optional

class DemonHealthRow(BaseModel):
    end_day: str          # yyyymmdd
    end_time: str         # HH:MM:SS
    log: str
    status: str
    log_desc: Optional[str] = None
    apply_machine: Optional[str] = None
    updated_at: Optional[str] = None  # ISO string으로 내려도 되고, 그대로 둬도 됨