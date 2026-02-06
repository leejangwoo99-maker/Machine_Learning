# app/routers/email_list.py
from __future__ import annotations

from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError, DBAPIError

# 프로젝트마다 DB session 경로가 달라서 둘 다 대응
try:
    from app.database import get_db  # type: ignore
except Exception:
    from app.db.session import get_db  # type: ignore

router = APIRouter(prefix="/email_list", tags=["3.email_list"])

SCHEMA = "g_production_film"
TABLE = "email_list"
COL = "email_list"

ADMIN_PASSWORD = "leejangwoo1!"


class EmailListSyncIn(BaseModel):
    emails: List[str]

    @field_validator("emails")
    @classmethod
    def normalize_emails(cls, v: List[str]) -> List[str]:
        out: List[str] = []
        seen = set()
        for x in v or []:
            s = (x or "").strip().lower()
            if not s:
                continue
            if "@" not in s or "." not in s.split("@")[-1]:
                continue
            if s not in seen:
                seen.add(s)
                out.append(s)
        return out


def _check_admin_pass(x_admin_pass: Optional[str]) -> None:
    if (x_admin_pass or "").strip() != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid admin password")


@router.get("")
def get_email_list(db: Session = Depends(get_db)) -> List[Dict[str, Any]]:
    """
    전체 조회
    """
    try:
        rows = db.execute(
            text(f"""
                SELECT {COL}
                FROM {SCHEMA}.{TABLE}
                WHERE {COL} IS NOT NULL
                ORDER BY {COL}
            """)
        ).mappings().all()
        return [dict(r) for r in rows]
    except (OperationalError, DBAPIError):
        raise HTTPException(status_code=503, detail="DB unavailable")


@router.post("/sync")
def post_email_sync(
    body: EmailListSyncIn,
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
    db: Session = Depends(get_db),
):
    """
    화면 최종 목록 기준 동기화:
      - 없는 값은 INSERT
      - 빠진 값은 DELETE
    """
    _check_admin_pass(x_admin_pass)

    try:
        # 현재 DB 목록
        current_rows = db.execute(
            text(f"SELECT {COL} FROM {SCHEMA}.{TABLE} WHERE {COL} IS NOT NULL")
        ).mappings().all()
        current = {str(r[COL]).strip().lower() for r in current_rows if r.get(COL)}

        target = set(body.emails)

        to_insert = sorted(target - current)
        to_delete = sorted(current - target)

        inserted = 0
        deleted = 0

        for e in to_insert:
            r = db.execute(
                text(f"""
                    INSERT INTO {SCHEMA}.{TABLE}({COL})
                    VALUES (:email)
                    ON CONFLICT ({COL}) DO NOTHING
                """),
                {"email": e},
            )
            inserted += int(r.rowcount or 0)

        for e in to_delete:
            r = db.execute(
                text(f"DELETE FROM {SCHEMA}.{TABLE} WHERE {COL} = :email"),
                {"email": e},
            )
            deleted += int(r.rowcount or 0)

        db.commit()
        return {"inserted": inserted, "deleted": deleted, "total_after": len(target)}

    except (OperationalError, DBAPIError):
        db.rollback()
        raise HTTPException(status_code=503, detail="DB unavailable")
