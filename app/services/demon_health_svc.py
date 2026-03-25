# app/services/demon_health_svc.py
# -*- coding: utf-8 -*-
from __future__ import annotations
from typing import List, Dict, Any
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from app.repos.demon_health_repo import fetch_rows_by_day_shift, fetch_rows_by_night_pair

KST = ZoneInfo("Asia/Seoul")

def _next_day_yyyymmdd(d: str) -> str:
    dt = datetime.strptime(str(d), "%Y%m%d")
    return (dt + timedelta(days=1)).strftime("%Y%m%d")

def get_demon_health_rows(prod_day: str, shift_type: str, limit: int = 500) -> List[Dict[str, Any]]:
    shift_type = (shift_type or "").strip().lower()
    if shift_type == "day":
        return fetch_rows_by_day_shift(prod_day=str(prod_day), shift_type="day", limit=limit)

    d0 = str(prod_day)
    d1 = _next_day_yyyymmdd(d0)
    return fetch_rows_by_night_pair(d0=d0, d1=d1, limit=limit)