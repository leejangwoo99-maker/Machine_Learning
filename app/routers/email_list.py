# app/routers/email_list.py
from __future__ import annotations

from typing import List, Optional, Dict, Any

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session

# 프로젝트마다 DB session 경로가 달라서 둘 다 대응
try:
    from app.database import get_db  # type: ignore
except Exception:
    from app.db.session import get_db  # type: ignore

router = APIRouter(prefix="/email_list", tags=["3.email_list"])

SCHEMA = "g_production_film"
TABLE = "email_list"
COL = "email_list"  # 실제 컬럼명 (중요)

# 필요 시 환경변수로 바꿔도 됨
ADMIN_PASSWORD = "leejangwoo1!"


# -------------------------
# Pydantic models
# -------------------------
class EmailOneIn(BaseModel):
    email: str

    @field_validator("email")
    @classmethod
    def normalize_email(cls, v: str) -> str:
        s = (v or "").strip().lower()
        if not s:
            raise ValueError("email is required")
        # 엄격 검증은 일부 환경에서 email_validator 모듈 필요하므로 단순 검증으로 처리
        if "@" not in s or "." not in s.split("@")[-1]:
            raise ValueError("invalid email format")
        return s


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


# -------------------------
# APIs
# -------------------------
@router.get("")
def get_email_list(db: Session = Depends(get_db)) -> List[Dict[str, Any]]:
    """
    전체 조회
    """
    rows = db.execute(
        text(f"""
            SELECT {COL}
            FROM {SCHEMA}.{TABLE}
            WHERE {COL} IS NOT NULL
            ORDER BY {COL}
        """)
    ).mappings().all()
    return [dict(r) for r in rows]


@router.post("")
def post_email_one(
    body: EmailOneIn,
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
    db: Session = Depends(get_db),
):
    """
    단건 추가 (이미 존재하면 변화 없음)
    """
    _check_admin_pass(x_admin_pass)

    result = db.execute(
        text(f"""
            INSERT INTO {SCHEMA}.{TABLE}({COL})
            VALUES (:email)
            ON CONFLICT ({COL}) DO NOTHING
        """),
        {"email": body.email},
    )
    db.commit()
    # rowcount: 1이면 insert, 0이면 이미 존재
    return {"inserted": int(result.rowcount or 0), "deleted": 0}


@router.delete("/{email}")
def delete_email_one(
    email: str,
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
    db: Session = Depends(get_db),
):
    """
    단건 삭제
    """
    _check_admin_pass(x_admin_pass)
    target = (email or "").strip().lower()
    if not target:
        raise HTTPException(status_code=400, detail="email is required")

    result = db.execute(
        text(f"""
            DELETE FROM {SCHEMA}.{TABLE}
            WHERE {COL} = :email
        """),
        {"email": target},
    )
    db.commit()
    return {"inserted": 0, "deleted": int(result.rowcount or 0)}


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
