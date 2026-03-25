# app/routers/demon_health.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Dict, Any

from fastapi import APIRouter, Depends, Query
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/demon_health", tags=["demon_health"])

SCHEMA = "k_demon_heath_check"
TABLE = "total_demon_report"

@router.get("/latest", response_model=List[Dict[str, Any]])
def fetch_latest(
    limit: int = Query(200, ge=1, le=5000),
    db: Session = Depends(get_db),
) -> List[Dict[str, Any]]:
    q = text(f"""
        SELECT *
        FROM {SCHEMA}.{TABLE}
        ORDER BY end_day DESC, end_time DESC
        LIMIT :lim
    """)
    rows = db.execute(q, {"lim": int(limit)}).mappings().all()
    return [dict(r) for r in rows]