from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Set

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/remark_info", tags=["4.remark_info"])

SCHEMA = "g_production_film"
TABLE = "remark_info"


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


def _to_opt_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    return s if s else None


def _ensure_table_exists(db: Session) -> None:
    db.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            barcode_information text PRIMARY KEY,
            pn                  text NULL,
            remark              text NULL
        )
    """))


def _ensure_unique_index(db: Session) -> None:
    db.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_remark_info_barcode
        ON {SCHEMA}.{TABLE} (barcode_information)
    """))


def _fetch_all_keys(db: Session) -> Set[str]:
    rows = db.execute(text(f"SELECT barcode_information FROM {SCHEMA}.{TABLE}")).mappings().all()
    out: Set[str] = set()
    for r in rows:
        k = (r.get("barcode_information") or "").strip()
        if k:
            out.add(k)
    return out


class RemarkInfoIn(BaseModel):
    barcode_information: str
    pn: Optional[str] = None
    remark: Optional[str] = None

    @field_validator("barcode_information")
    @classmethod
    def v_barcode(cls, v: str) -> str:
        s = (v or "").strip()
        if not s:
            raise ValueError("barcode_information required")
        return s

    @field_validator("pn")
    @classmethod
    def v_pn(cls, v: Optional[str]) -> Optional[str]:
        return _to_opt_str(v)

    @field_validator("remark")
    @classmethod
    def v_remark(cls, v: Optional[str]) -> Optional[str]:
        return _to_opt_str(v)


class RemarkInfoSyncIn(BaseModel):
    rows: List[RemarkInfoIn]


@router.get("")
def get_remark_info(
    end_day: str,
    shift_type: str,
    db: Session = Depends(get_db),
) -> Response:
    try:
        _ = _norm_day(end_day)
        _ = _norm_shift(shift_type)

        _ensure_table_exists(db)
        _ensure_unique_index(db)

        rows = db.execute(text(f"""
            SELECT
                barcode_information,
                pn,
                remark
            FROM {SCHEMA}.{TABLE}
            ORDER BY barcode_information
        """)).mappings().all()

        import json
        body = json.dumps([dict(r) for r in rows], ensure_ascii=False)
        return Response(content=body, media_type="application/json; charset=utf-8")

    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail="DB unavailable") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"remark_info get failed: {e}") from e


@router.post("/sync")
def post_remark_info_sync(
    body: RemarkInfoSyncIn,
    mode: Literal["add", "replace"] = Query("add"),
    dry_run: bool = Query(False),
    min_keep_ratio: float = Query(0.7, ge=0.0, le=1.0),
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
    db: Session = Depends(get_db),
):
    try:
        _admin_guard(x_admin_pass)
        _ensure_table_exists(db)
        _ensure_unique_index(db)

        dedup: Dict[str, Dict[str, Any]] = {}
        for r in body.rows:
            row = r.model_dump()
            k = row["barcode_information"].strip()
            dedup[k] = {
                "barcode_information": k,
                "pn": row.get("pn"),
                "remark": row.get("remark"),
            }

        target_keys: Set[str] = set(dedup.keys())
        current_keys: Set[str] = _fetch_all_keys(db)

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

        # ✅ bulk upsert
        if target_keys:
            params = [dedup[k] for k in sorted(target_keys)]
            db.execute(
                text(f"""
                    INSERT INTO {SCHEMA}.{TABLE}
                    (barcode_information, pn, remark)
                    VALUES (:barcode_information, :pn, :remark)
                    ON CONFLICT (barcode_information)
                    DO UPDATE SET
                        pn     = EXCLUDED.pn,
                        remark = EXCLUDED.remark
                """),
                params,
            )

        # ✅ bulk delete
        deleted = 0
        if mode == "replace" and delete_keys:
            res = db.execute(
                text(f"DELETE FROM {SCHEMA}.{TABLE} WHERE barcode_information = ANY(:keys)"),
                {"keys": list(delete_keys)},
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
        raise HTTPException(status_code=503, detail="DB unavailable") from e
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"remark_info sync failed: {e}") from e