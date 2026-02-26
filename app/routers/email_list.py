from __future__ import annotations

from typing import Dict, List, Literal, Optional, Set

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/email_list", tags=["2.email_list"])

SCHEMA = "g_production_film"
TABLE = "email_list"


def _norm_day(v: str) -> str:
    s = (v or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        s = s.replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError("end_day must be YYYYMMDD or YYYY-MM-DD")
    return s


def _norm_shift(v: str) -> str:
    s = (v or "").strip().lower()
    if s not in {"day", "night"}:
        raise ValueError("shift_type must be day/night")
    return s


def _admin_guard(x_admin_pass: Optional[str]) -> None:
    import os
    expected = (os.getenv("ADMIN_PASS") or "").strip()
    if not expected:
        return
    given = (x_admin_pass or "").strip()
    if given != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


def _norm_email(v: str) -> str:
    s = (v or "").strip().lower()
    if not s:
        raise ValueError("email required")
    return s


def _ensure_table_and_index(db: Session) -> None:
    db.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            email text PRIMARY KEY
        )
    """))
    db.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_email_list_email
        ON {SCHEMA}.{TABLE} (email)
    """))


def _fetch_all_set(db: Session) -> Set[str]:
    rows = db.execute(text(f"SELECT email FROM {SCHEMA}.{TABLE}")).mappings().all()
    out: Set[str] = set()
    for r in rows:
        e = (r.get("email") or "").strip().lower()
        if e:
            out.add(e)
    return out


class EmailListIn(BaseModel):
    email: str

    @field_validator("email")
    @classmethod
    def v_email(cls, v: str) -> str:
        return _norm_email(v)


class EmailListSyncIn(BaseModel):
    rows: List[EmailListIn]


@router.get("")
def get_email_list(
    end_day: str,
    shift_type: str,
    db: Session = Depends(get_db),
) -> Response:
    try:
        _ = _norm_day(end_day)
        _ = _norm_shift(shift_type)

        _ensure_table_and_index(db)

        rows = db.execute(text(f"""
            SELECT email
            FROM {SCHEMA}.{TABLE}
            ORDER BY email
        """)).mappings().all()

        import json
        body = json.dumps([dict(r) for r in rows], ensure_ascii=False)
        return Response(content=body, media_type="application/json; charset=utf-8")

    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"email_list db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"email_list get failed: {e}") from e


@router.post("/sync")
def post_email_list_sync(
    body: EmailListSyncIn,
    mode: Literal["add", "replace"] = Query("add"),
    dry_run: bool = Query(False),
    min_keep_ratio: float = Query(0.7, ge=0.0, le=1.0),
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
    db: Session = Depends(get_db),
):
    try:
        _admin_guard(x_admin_pass)
        _ensure_table_and_index(db)

        dedup: Dict[str, Dict[str, str]] = {}
        for r in body.rows:
            e = _norm_email(r.email)
            dedup[e] = {"email": e}

        target_keys: Set[str] = set(dedup.keys())
        current_keys: Set[str] = _fetch_all_set(db)

        delete_keys: Set[str] = set()
        if mode == "replace":
            base = len(current_keys) if len(current_keys) > 0 else 1
            keep_ratio = float(len(target_keys)) / float(base)
            if keep_ratio < float(min_keep_ratio):
                raise HTTPException(
                    status_code=400,
                    detail=f"guard blocked: keep_ratio={keep_ratio:.4f} < min_keep_ratio={min_keep_ratio:.4f}",
                )
            delete_keys = current_keys - target_keys

        if dry_run:
            total_after = len(target_keys) if mode == "replace" else len(current_keys | target_keys)
            return {
                "ok": True,
                "mode": mode,
                "dry_run": True,
                "upserted": len(target_keys),
                "deleted": len(delete_keys),
                "total_after": total_after,
            }

        # ✅ bulk upsert (executemany)
        if target_keys:
            params = [{"email": e} for e in sorted(target_keys)]
            db.execute(
                text(f"""
                    INSERT INTO {SCHEMA}.{TABLE} (email)
                    VALUES (:email)
                    ON CONFLICT (email) DO NOTHING
                """),
                params,
            )

        # ✅ bulk delete (replace)
        deleted = 0
        if mode == "replace" and delete_keys:
            res = db.execute(
                text(f"DELETE FROM {SCHEMA}.{TABLE} WHERE email = ANY(:emails)"),
                {"emails": list(delete_keys)},
            )
            deleted = int(res.rowcount or 0)

        db.commit()

        total_after = len(target_keys) if mode == "replace" else len(current_keys | target_keys)
        return {
            "ok": True,
            "mode": mode,
            "upserted": len(target_keys),
            "deleted": deleted,
            "total_after": total_after,
        }

    except HTTPException:
        db.rollback()
        raise
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"email_list db error: {e}") from e
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"email_list sync failed: {e}") from e