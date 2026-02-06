# app/routers/remark_info.py
from __future__ import annotations

from typing import Optional, Any, Dict, List

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError, DBAPIError

# get_db 경로 자동 대응
try:
    from app.database import get_db  # type: ignore
except Exception:
    from app.db.session import get_db  # type: ignore

router = APIRouter(prefix="/remark_info", tags=["4.remark_info"])

ADMIN_PASSWORD = "leejangwoo1!"

SCHEMA = "g_production_film"
TABLE = "remark_info"
COL_BARCODE = "barcode_information"
COL_PN = "pn"
COL_REMARK = "remark"


def _check_admin_password(x_admin_pass: str | None) -> None:
    pw = (x_admin_pass or "").strip()
    if pw != ADMIN_PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid admin password")


class RemarkSyncRow(BaseModel):
    barcode_information: str = Field(..., min_length=1, max_length=128)
    pn: Optional[str] = None
    remark: Optional[str] = None

    @field_validator("barcode_information")
    @classmethod
    def validate_barcode(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            raise ValueError("barcode_information is required")
        return s

    @field_validator("pn", "remark")
    @classmethod
    def norm_nullable_text(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        s = v.strip()
        return s if s else None


class RemarkListSyncIn(BaseModel):
    rows: List[RemarkSyncRow]

    @field_validator("rows")
    @classmethod
    def dedup_rows(cls, v: List[RemarkSyncRow]) -> List[RemarkSyncRow]:
        # 같은 barcode_information 중 마지막 값 우선
        m: dict[str, RemarkSyncRow] = {}
        for row in v or []:
            m[row.barcode_information] = row
        # barcode_information 정렬
        return [m[k] for k in sorted(m.keys())]


@router.get("")
def get_remark_info(db: Session = Depends(get_db)) -> List[Dict[str, Any]]:
    try:
        rows = db.execute(
            text(f"""
                SELECT {COL_BARCODE}, {COL_PN}, {COL_REMARK}
                FROM {SCHEMA}.{TABLE}
                ORDER BY {COL_BARCODE}
            """)
        ).mappings().all()

        return [dict(r) for r in rows]

    except (OperationalError, DBAPIError):
        raise HTTPException(status_code=503, detail="DB unavailable")


@router.post("/sync")
def post_remark_sync(
    body: RemarkListSyncIn,
    db: Session = Depends(get_db),
    x_admin_pass: str | None = Header(default=None, alias="X-ADMIN-PASS"),
):
    """
    화면 최종 목록 기준 동기화:
      - target에 없는 barcode_information은 DELETE
      - target에 있는 건 UPSERT
    """
    _check_admin_password(x_admin_pass)

    try:
        # 현재 목록
        current_rows = db.execute(
            text(f"""
                SELECT {COL_BARCODE}
                FROM {SCHEMA}.{TABLE}
                WHERE {COL_BARCODE} IS NOT NULL
            """)
        ).mappings().all()
        current = {str(r[COL_BARCODE]).strip() for r in current_rows if r.get(COL_BARCODE)}

        target_map = {
            r.barcode_information: {"pn": r.pn, "remark": r.remark}
            for r in body.rows
        }
        target = set(target_map.keys())

        to_delete = sorted(current - target)

        deleted = 0
        upserted = 0

        # UPSERT
        for barcode, payload in target_map.items():
            db.execute(
                text(f"""
                    INSERT INTO {SCHEMA}.{TABLE} ({COL_BARCODE}, {COL_PN}, {COL_REMARK})
                    VALUES (:barcode_information, :pn, :remark)
                    ON CONFLICT ({COL_BARCODE}) DO UPDATE
                    SET
                        {COL_PN} = EXCLUDED.{COL_PN},
                        {COL_REMARK} = EXCLUDED.{COL_REMARK}
                """),
                {
                    "barcode_information": barcode,
                    "pn": payload["pn"],
                    "remark": payload["remark"],
                },
            )
            upserted += 1

        # DELETE
        for barcode in to_delete:
            r = db.execute(
                text(f"""
                    DELETE FROM {SCHEMA}.{TABLE}
                    WHERE {COL_BARCODE} = :barcode_information
                """),
                {"barcode_information": barcode},
            )
            deleted += int(r.rowcount or 0)

        db.commit()
        return {
            "upserted": upserted,
            "deleted": deleted,
            "total_after": len(target),
        }

    except (OperationalError, DBAPIError):
        db.rollback()
        raise HTTPException(status_code=503, detail="DB unavailable")
