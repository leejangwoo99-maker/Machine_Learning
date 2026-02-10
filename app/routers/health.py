from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy import text
from sqlalchemy.orm import Session

# 확정 경로
from app.db.session import get_db, db_ping, db_whoami

router = APIRouter(prefix="/health", tags=["health"])


@router.get("")
def health():
    return {"ok": True}


@router.get("/db")
def health_db():
    r = db_ping()

    # db_ping이 tuple이든 bool이든 모두 처리
    if isinstance(r, tuple):
        ok, detail = r[0], (r[1] if len(r) > 1 else "")
    else:
        ok, detail = bool(r), "pong" if r else "db ping failed"

    return {"ok": bool(ok), "detail": str(detail)}


@router.get("/db-whoami")
def health_db_whoami():
    return db_whoami()


@router.get("/db-tables")
def db_tables(db: Session = Depends(get_db)):
    out = {}
    targets = [
        ("g_production_film", "worker_info"),
        ("g_production_film", "email_list"),
        ("g_production_film", "remark_info"),
        ("g_production_film", "planned_time"),
        ("g_production_film", "fct_non_operation_time"),
        ("g_production_film", "alarm_record"),
        ("e4_predictive_maintenance", "pd_board_check"),
    ]

    for sch, tbl in targets:
        key = f"{sch}.{tbl}"
        try:
            exists = db.execute(text("""
                SELECT EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = :sch
                      AND table_name = :tbl
                ) AS ok
            """), {"sch": sch, "tbl": tbl}).scalar()
            out[key] = "ok" if exists else "missing"
        except Exception as e:
            out[key] = f"fail: {type(e).__name__}: {e}"

    return out
