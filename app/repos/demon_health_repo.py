# app/repos/demon_health_repo.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Dict, Any
from sqlalchemy import text

# ✅ 여기: get_session 말고 SessionLocal을 쓰는 패턴
from app.db.session import SessionLocal

SCHEMA = "k_demon_heath_check"
TABLE  = "total_demon_report"

def fetch_latest(limit: int = 200) -> List[Dict[str, Any]]:
    q = text(f"""
        SELECT end_day, end_time, log, status, log_desc, apply_machine, updated_at
        FROM {SCHEMA}.{TABLE}
        ORDER BY end_day DESC, end_time DESC
        LIMIT :lim
    """)

    db = SessionLocal()
    try:
        rows = db.execute(q, {"lim": int(limit)}).mappings().all()
        return [dict(r) for r in rows]
    finally:
        db.close()