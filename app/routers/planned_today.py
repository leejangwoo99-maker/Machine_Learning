from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Literal, Optional, Set, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, field_validator
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo

from app.db.session import get_db

router = APIRouter(prefix="/planned_time", tags=["4.planned_time"])

SCHEMA = "g_production_film"
TABLE = "planned_time"

KST = ZoneInfo("Asia/Seoul")
DAY_START = "08:30:00"
DAY_END = "20:29:59"


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
    # ?명꽣?섏씠???명솚??寃利??뚯씠釉붿뿏 shift_type 而щ읆 ?놁쓬)
    s = (v or "").strip().lower()
    if s not in {"day", "night"}:
        raise ValueError("shift_type must be day/night")
    return s


def _to_opt_str(v: Optional[str]) -> Optional[str]:
    if v is None:
        return None
    s = v.strip()
    return s if s else None


def _to_hhmmss(v: str) -> str:
    s = (v or "").strip()
    if not s:
        raise ValueError("time required")

    parts = s.split(":")
    if len(parts) not in (2, 3):
        raise ValueError("time must be HH:MM or HH:MM:SS")

    try:
        hh = int(parts[0])
        mm = int(parts[1])
        ss_raw = "0" if len(parts) == 2 else parts[2]
        if "." in ss_raw:
            ss_raw = ss_raw.split(".", 1)[0]
        ss = int(ss_raw)

        if not (0 <= hh <= 23 and 0 <= mm <= 59 and 0 <= ss <= 59):
            raise ValueError
    except Exception as e:
        raise ValueError("invalid time format") from e

    return f"{hh:02d}:{mm:02d}:{ss:02d}"


def _ensure_table_exists(db: Session) -> None:
    db.execute(text(f"""
        CREATE TABLE IF NOT EXISTS {SCHEMA}.{TABLE} (
            end_day   text NOT NULL,
            from_time text NOT NULL,
            to_time   text NOT NULL,
            reason    text NULL
        )
    """))


def _ensure_unique_index(db: Session) -> None:
    db.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_planned_time_day_range
        ON {SCHEMA}.{TABLE} (end_day, from_time, to_time)
    """))


def _fetch_keys_for_day(db: Session, end_day: str) -> Set[Tuple[str, str, str]]:
    rows = db.execute(text(f"""
        SELECT end_day, from_time, to_time
        FROM {SCHEMA}.{TABLE}
        WHERE replace(end_day, '-', '') = :end_day
    """), {"end_day": end_day}).mappings().all()

    out: Set[Tuple[str, str, str]] = set()
    for r in rows:
        d_raw = (r.get("end_day") or "").strip()
        d = _norm_day(d_raw) if d_raw else ""
        f = (r.get("from_time") or "").strip()
        t = (r.get("to_time") or "").strip()
        if d and f and t:
            out.add((d, _to_hhmmss(f), _to_hhmmss(t)))
    return out


def _resolve_kst_day_shift() -> Tuple[str, str]:
    """
    KST 湲곗? ?꾩옱 ?쒓컖???ъ뼇??留욎떠 (YYYYMMDD, shift_type)濡??섏궛
    - day   : D-day 08:30:00 ~ 20:29:59
    - night : D-day 20:30:00 ~ 23:59:59 + D+1 00:00:00 ~ 08:29:59
      => 00:00~08:29??'?꾩씪 night'
    """
    now = datetime.now(KST)
    t = now.time()

    if DAY_START <= t.strftime("%H:%M:%S") <= DAY_END:
        return now.strftime("%Y%m%d"), "day"

    if t.strftime("%H:%M:%S") < DAY_START:
        prev = now - timedelta(days=1)
        return prev.strftime("%Y%m%d"), "night"

    return now.strftime("%Y%m%d"), "night"


def _in_shift_window(from_time: str, shift_type: str) -> bool:
    ft = _to_hhmmss(from_time)
    if shift_type == "day":
        return DAY_START <= ft <= DAY_END
    # night
    return (ft >= "20:30:00") or (ft <= "08:29:59")


# ------------------------------------------------------------
# Schemas
# ------------------------------------------------------------
class PlannedTimeIn(BaseModel):
    end_day: str
    from_time: str
    to_time: str
    reason: Optional[str] = None

    @field_validator("end_day")
    @classmethod
    def v_end_day(cls, v: str) -> str:
        return _norm_day(v)

    @field_validator("from_time")
    @classmethod
    def v_from_time(cls, v: str) -> str:
        return _to_hhmmss(v)

    @field_validator("to_time")
    @classmethod
    def v_to_time(cls, v: str) -> str:
        return _to_hhmmss(v)

    @field_validator("reason")
    @classmethod
    def v_reason(cls, v: Optional[str]) -> Optional[str]:
        return _to_opt_str(v)


class PlannedTimeSyncIn(BaseModel):
    rows: List[PlannedTimeIn]


# ------------------------------------------------------------
# Read API (?ъ뼇 寃쎈줈)
# ------------------------------------------------------------
@router.get("")
def get_planned_time(
    end_day: Optional[str] = Query(None, description="YYYYMMDD or YYYY-MM-DD"),
    shift_type: Optional[str] = Query(None, description="day/night"),
    db: Session = Depends(get_db),
) -> Response:
    """
    planned_time ?뚯씠釉붿뿉 shift_type 而щ읆???놁뼱??
    from_time ?쒓컙? ?덈룄?곕줈 day/night ?꾪꽣留곹븳??

    - end_day/shift_type ?????놁쑝硫?KST ?꾩옱 湲곗? ?먮룞 怨꾩궛
    - ?섎굹留?二쇰㈃ ?섎㉧吏???먮룞 怨꾩궛媛??ъ슜
    """
    try:
        _ensure_table_exists(db)
        _ensure_unique_index(db)

        auto_day, auto_shift = _resolve_kst_day_shift()
        d = _norm_day(end_day) if end_day is not None else auto_day
        s = _norm_shift(shift_type) if shift_type is not None else auto_shift

        rows = db.execute(text(f"""
            SELECT
                end_day,
                from_time,
                to_time,
                reason
            FROM {SCHEMA}.{TABLE}
            WHERE replace(end_day, '-', '') = :end_day
            ORDER BY from_time, to_time
        """), {"end_day": d}).mappings().all()

        payload: List[Dict[str, Any]] = []
        for r in rows:
            from_t = _to_hhmmss((r.get("from_time") or "").strip())
            if not _in_shift_window(from_t, s):
                continue

            payload.append({
                "end_day": _norm_day((r.get("end_day") or "").strip()),
                "from_time": from_t,
                "to_time": _to_hhmmss((r.get("to_time") or "").strip()),
                "reason": r.get("reason"),
            })

        import json
        body = json.dumps(payload, ensure_ascii=False)
        return Response(content=body, media_type="application/json; charset=utf-8")

    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"planned_time db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"planned_time get failed: {e}") from e


# ------------------------------------------------------------
# Read API (湲곗〈 ?명솚 寃쎈줈 ?좎?)
# ------------------------------------------------------------
@router.get("/today")
def get_planned_time_today(
    end_day: Optional[str] = None,
    shift_type: Optional[str] = None,
    db: Session = Depends(get_db),
) -> Response:
    """
    湲곗〈 ?대씪?댁뼵???명솚??
    ?대??곸쑝濡?GET /planned_time 怨??숈씪?섍쾶 ?숈옉.
    """
    return get_planned_time(end_day=end_day, shift_type=shift_type, db=db)


# ------------------------------------------------------------
# Sync API
# ------------------------------------------------------------
@router.post("/sync")
def post_planned_time_sync(
    body: PlannedTimeSyncIn,
    mode: Literal["add", "replace"] = Query("add"),
    dry_run: bool = Query(False),
    min_keep_ratio: float = Query(0.7, ge=0.0, le=1.0),
    db: Session = Depends(get_db),
):
    """
    mode=add
      - (end_day, from_time, to_time) 湲곗? upsert
      - 湲곗〈 ?곗씠????젣 ?놁쓬

    mode=replace
      - payload瑜?end_day蹂?理쒖쥌 ?ㅻ깄?룹쑝濡?媛꾩＜
      - ?대떦 end_day?먯꽌 payload???녿뒗 range????젣 ???      - dry_run=true硫?諛섏쁺 ?놁씠 移댁슫?몃쭔 諛섑솚
      - min_keep_ratio 蹂댄샇 ?곸슜
    """
    try:
        _ensure_table_exists(db)
        _ensure_unique_index(db)

        # payload dedup: key=(end_day, from_time, to_time), last-write-wins
        dedup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        scope_days: Dict[str, Set[Tuple[str, str, str]]] = {}

        for r in body.rows:
            row = r.model_dump()
            d = _norm_day(row["end_day"])
            f = _to_hhmmss(row["from_time"])
            t = _to_hhmmss(row["to_time"])

            k = (d, f, t)
            dedup[k] = {
                "end_day": d,
                "from_time": f,
                "to_time": t,
                "reason": row.get("reason"),
            }

            if d not in scope_days:
                scope_days[d] = set()
            scope_days[d].add(k)

        target_keys: Set[Tuple[str, str, str]] = set(dedup.keys())
        upsert_count = len(target_keys)

        delete_keys: Set[Tuple[str, str, str]] = set()
        if mode == "replace":
            for d, target_in_day in scope_days.items():
                current_in_day = _fetch_keys_for_day(db, d)

                base = len(current_in_day) if len(current_in_day) > 0 else 1
                keep_ratio = float(len(target_in_day)) / float(base)
                if keep_ratio < float(min_keep_ratio):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"guard blocked on day({d}): "
                            f"keep_ratio={keep_ratio:.4f} < min_keep_ratio={min_keep_ratio:.4f}"
                        ),
                    )

                delete_keys |= (current_in_day - target_in_day)

        if dry_run:
            if mode == "replace":
                total_after = len(target_keys)
            else:
                current_all: Set[Tuple[str, str, str]] = set()
                for d in scope_days.keys():
                    current_all |= _fetch_keys_for_day(db, d)
                total_after = len(current_all | target_keys)

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
                (end_day, from_time, to_time, reason)
                VALUES (:end_day, :from_time, :to_time, :reason)
                ON CONFLICT (end_day, from_time, to_time)
                DO UPDATE SET
                    reason = EXCLUDED.reason
            """), row)

        # 2) delete (replace)
        deleted = 0
        if mode == "replace" and delete_keys:
            for d, f, t in sorted(delete_keys):
                db.execute(text(f"""
                    DELETE FROM {SCHEMA}.{TABLE}
                    WHERE replace(end_day, '-', '') = :end_day
                      AND from_time = :from_time
                      AND to_time = :to_time
                """), {"end_day": d, "from_time": f, "to_time": t})
                deleted += 1

        db.commit()

        total_after = 0
        for d in scope_days.keys():
            total_after += len(_fetch_keys_for_day(db, d))

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
        raise HTTPException(status_code=503, detail=f"planned_time db error: {e}") from e
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"planned_time sync failed: {e}") from e
