from __future__ import annotations

from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/remark_info", tags=["4.remark_info"])

SCHEMA = "g_production_film"
TABLE = "remark_info"


# ------------------------------------------------------------
# Utils
# ------------------------------------------------------------
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
    # ADMIN_PASS 誘몄꽕????蹂댄샇 鍮꾪솢??媛쒕컻 ?몄쓽). ?댁쁺?먯꽌??諛섎뱶???ㅼ젙 沅뚯옣.
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
    # PK媛 ?대? ?덈뜑?쇰룄 ?댁쁺 以??덉쟾?섍쾶 ?쒕쾲 ??蹂닿컯 媛??
    db.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_remark_info_barcode
        ON {SCHEMA}.{TABLE} (barcode_information)
    """))


def _fetch_all_map(db: Session) -> Dict[str, Dict[str, Any]]:
    rows = db.execute(text(f"""
        SELECT
            barcode_information,
            pn,
            remark
        FROM {SCHEMA}.{TABLE}
    """)).mappings().all()

    out: Dict[str, Dict[str, Any]] = {}
    for r in rows:
        key = (r.get("barcode_information") or "").strip()
        if key:
            out[key] = {
                "barcode_information": key,
                "pn": r.get("pn"),
                "remark": r.get("remark"),
            }
    return out


# ------------------------------------------------------------
# Schemas
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# Read API
# ------------------------------------------------------------
@router.get("")
def get_remark_info(
    end_day: str,
    shift_type: str,
    db: Session = Depends(get_db),
) -> Response:
    """
    remark_info???꾩옱 ?ㅽ궎留???day/shift 而щ읆???놁쑝誘濡?
    荑쇰━ ?뚮씪誘명꽣???명꽣?섏씠???명솚???⑸룄濡쒕쭔 寃利앺븯怨?
    ?꾩껜 remark_info瑜?諛섑솚?쒕떎.
    """
    try:
        _ = _norm_day(end_day)
        _ = _norm_shift(shift_type)

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


# ------------------------------------------------------------
# Sync API
# ------------------------------------------------------------
@router.post("/sync")
def post_remark_info_sync(
    body: RemarkInfoSyncIn,
    mode: Literal["add", "replace"] = Query("add"),
    dry_run: bool = Query(False),
    min_keep_ratio: float = Query(0.7, ge=0.0, le=1.0),
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
    db: Session = Depends(get_db),
):
    """
    mode=add
      - ?낅젰 rows瑜?PK(barcode_information) 湲곗? upsert
      - 湲곗〈 ?곗씠????젣 ?놁쓬

    mode=replace
      - ?낅젰 rows瑜?理쒖쥌 ?ㅻ깄?룹쑝濡?媛꾩＜
      - DB???덇퀬 payload???녿뒗 barcode????젣 ???
      - dry_run=true硫??ㅼ젣 諛섏쁺 ?놁씠 移댁슫?몃쭔 諛섑솚
      - min_keep_ratio 蹂댄샇:
        keep_ratio = len(target_keys) / max(len(current_keys), 1)
        keep_ratio < min_keep_ratio ?대㈃ 400 李⑤떒
        (?? min_keep_ratio=0.0 ?대㈃ ?꾩쟾 援먯껜 ?덉슜)
    """
    try:
        _admin_guard(x_admin_pass)

        _ensure_table_exists(db)
        _ensure_unique_index(db)

        # payload dedup (last-write-wins)
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
        current_map = _fetch_all_map(db)
        current_keys: Set[str] = set(current_map.keys())

        # upsert ??? mode 愿怨꾩뾾??target 紐⑤몢 upsert
        upsert_count = len(target_keys)

        # delete ??? replace 紐⑤뱶?먯꽌留?怨꾩궛
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
                "upserted": upsert_count,
                "deleted": len(delete_keys),
                "total_after": total_after,
            }

        # 1) upsert
        for k in sorted(target_keys):
            row = dedup[k]
            db.execute(text(f"""
                INSERT INTO {SCHEMA}.{TABLE}
                (barcode_information, pn, remark)
                VALUES (:barcode_information, :pn, :remark)
                ON CONFLICT (barcode_information)
                DO UPDATE SET
                    pn     = EXCLUDED.pn,
                    remark = EXCLUDED.remark
            """), row)

        # 2) delete (replace)
        deleted = 0
        if mode == "replace" and delete_keys:
            for k in sorted(delete_keys):
                db.execute(text(f"""
                    DELETE FROM {SCHEMA}.{TABLE}
                    WHERE barcode_information = :barcode_information
                """), {"barcode_information": k})
                deleted += 1

        db.commit()

        total_after = len(target_keys) if mode == "replace" else len(current_keys | target_keys)
        return {
            "ok": True,
            "mode": mode,
            "upserted": upsert_count,
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
