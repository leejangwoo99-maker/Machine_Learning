# app/api/routers/planned_today.py
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
NIGHT_START = "20:30:00"
NIGHT_END = "08:29:59"


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
        CREATE SCHEMA IF NOT EXISTS {SCHEMA};
    """))
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


def _resolve_kst_day_shift() -> Tuple[str, str]:
    """
    KST 현재 시각 기준 자동 (YYYYMMDD, shift_type)
    - day   : D 08:30:00 ~ 20:29:59
    - night : D 20:30:00 ~ 23:59:59 + D+1 00:00:00 ~ 08:29:59
      => 00:00~08:29 는 '전일 night'
    """
    now = datetime.now(KST)
    t = now.strftime("%H:%M:%S")

    if DAY_START <= t <= DAY_END:
        return now.strftime("%Y%m%d"), "day"

    if t < DAY_START:
        prev = now - timedelta(days=1)
        return prev.strftime("%Y%m%d"), "night"

    return now.strftime("%Y%m%d"), "night"


def _in_shift_window(from_time: str, shift_type: str) -> bool:
    ft = _to_hhmmss(from_time)
    if shift_type == "day":
        return DAY_START <= ft <= DAY_END
    # night
    return (ft >= NIGHT_START) or (ft <= NIGHT_END)


def _fetch_rows_for_day(db: Session, end_day: str) -> List[Dict[str, Any]]:
    rows = db.execute(text(f"""
        SELECT end_day, from_time, to_time, reason
        FROM {SCHEMA}.{TABLE}
        WHERE replace(end_day, '-', '') = :end_day
        ORDER BY from_time, to_time
    """), {"end_day": end_day}).mappings().all()
    return [dict(r) for r in rows]


def _fetch_keys_for_day_shift(db: Session, end_day: str, shift_type: str) -> Set[Tuple[str, str, str]]:
    """
    planned_time에는 shift_type 컬럼이 없으므로,
    'from_time'이 shift window에 해당하는 것만 키로 잡는다.
    """
    rows = _fetch_rows_for_day(db, end_day)
    out: Set[Tuple[str, str, str]] = set()

    for r in rows:
        d_raw = (r.get("end_day") or "").strip()
        d = _norm_day(d_raw) if d_raw else ""
        f = (r.get("from_time") or "").strip()
        t = (r.get("to_time") or "").strip()
        if not d or not f or not t:
            continue

        f2 = _to_hhmmss(f)
        t2 = _to_hhmmss(t)

        if _in_shift_window(f2, shift_type):
            out.add((d, f2, t2))

    return out


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
# Read API
# ------------------------------------------------------------
@router.get("")
def get_planned_time(
    end_day: Optional[str] = Query(None, description="YYYYMMDD or YYYY-MM-DD"),
    shift_type: Optional[str] = Query(None, description="day/night"),
    db: Session = Depends(get_db),
) -> Response:
    """
    planned_time 테이블에 shift_type 컬럼이 없으므로
    from_time 시간대를 기준으로 day/night 필터링.
    """
    try:
        _ensure_table_exists(db)
        _ensure_unique_index(db)

        auto_day, auto_shift = _resolve_kst_day_shift()
        d = _norm_day(end_day) if end_day is not None else auto_day
        s = _norm_shift(shift_type) if shift_type is not None else auto_shift

        rows = _fetch_rows_for_day(db, d)

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


@router.get("/today")
def get_planned_time_today(
    end_day: Optional[str] = None,
    shift_type: Optional[str] = None,
    db: Session = Depends(get_db),
) -> Response:
    return get_planned_time(end_day=end_day, shift_type=shift_type, db=db)


# ------------------------------------------------------------
# Sync API  ✅ 핵심 수정
# ------------------------------------------------------------
@router.post("/sync")
def post_planned_time_sync(
    body: PlannedTimeSyncIn,
    # ✅ api_client는 mode=replace 로 보냄
    mode: Literal["add", "replace"] = Query("add"),
    # ✅ streamlit client가 end_day/shift_type을 query로 보냄 (중요)
    end_day: Optional[str] = Query(None, description="YYYYMMDD or YYYY-MM-DD"),
    shift_type: Optional[str] = Query(None, description="day/night"),
    dry_run: bool = Query(False),
    min_keep_ratio: float = Query(0.7, ge=0.0, le=1.0),
    db: Session = Depends(get_db),
):
    """
    mode=add
      - (end_day, from_time, to_time) 기준 upsert
      - 삭제 없음

    mode=replace  ✅ FIXED
      - Query(end_day, shift_type) 스코프를 '반드시' 기준으로 replace 수행
      - payload가 비어도: 해당 end_day + shift window 데이터는 전부 삭제됨 (전체삭제 가능)
      - payload가 있으면: (현재 shift window에서) payload에 없는 range는 삭제됨
      - dry_run=true면 카운트만 반환
      - min_keep_ratio 가드:
          payload가 0이면 "전체 삭제 의도"로 간주하고 가드 미적용
          payload>0이면 기존 대비 payload 비율이 너무 낮을 때 차단
    """
    try:
        _ensure_table_exists(db)
        _ensure_unique_index(db)

        auto_day, auto_shift = _resolve_kst_day_shift()
        d = _norm_day(end_day) if end_day is not None else auto_day
        s = _norm_shift(shift_type) if shift_type is not None else auto_shift

        # payload dedup (end_day는 query d로 강제 고정)
        dedup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}

        for r in (body.rows or []):
            row = r.model_dump()
            f = _to_hhmmss(row["from_time"])
            t = _to_hhmmss(row["to_time"])

            # ✅ query shift window 밖은 무시 (UI/조회와 동일 규칙)
            if not _in_shift_window(f, s):
                continue

            k = (d, f, t)
            dedup[k] = {
                "end_day": d,
                "from_time": f,
                "to_time": t,
                "reason": row.get("reason"),
            }

        target_keys: Set[Tuple[str, str, str]] = set(dedup.keys())
        upsert_count = len(target_keys)

        # ✅ 현재 DB의 "해당 end_day + shift window" 키 집합
        current_keys = _fetch_keys_for_day_shift(db, d, s)

        delete_keys: Set[Tuple[str, str, str]] = set()

        if mode == "replace":
            # ✅ replace는 shift-window 범위 안에서만 replace 한다
            # payload=0이면 -> current_keys 전부 삭제(전체 삭제)
            delete_keys = current_keys - target_keys

            # ✅ 가드(선택)
            if len(target_keys) > 0:
                base = len(current_keys) if len(current_keys) > 0 else 1
                keep_ratio = float(len(target_keys)) / float(base)
                if keep_ratio < float(min_keep_ratio):
                    raise HTTPException(
                        status_code=400,
                        detail=(
                            f"guard blocked on day({d}) shift({s}): "
                            f"keep_ratio={keep_ratio:.4f} < min_keep_ratio={min_keep_ratio:.4f}"
                        ),
                    )
            # payload=0이면 keep_ratio=0이지만, 이건 '전체 삭제' 케이스라 가드 안 건다.

        if dry_run:
            total_after = 0
            if mode == "replace":
                total_after = len((current_keys - delete_keys) | target_keys)
            else:
                total_after = len(current_keys | target_keys)

            return {
                "ok": True,
                "mode": mode,
                "end_day": d,
                "shift_type": s,
                "dry_run": True,
                "upserted": upsert_count,
                "deleted": len(delete_keys),
                "total_after_in_shift": total_after,
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
            for dd, f, t in sorted(delete_keys):
                db.execute(text(f"""
                    DELETE FROM {SCHEMA}.{TABLE}
                    WHERE replace(end_day, '-', '') = :end_day
                      AND from_time = :from_time
                      AND to_time = :to_time
                """), {"end_day": dd, "from_time": f, "to_time": t})
                deleted += 1

        db.commit()

        # after-count (shift window 기준)
        total_after = len(_fetch_keys_for_day_shift(db, d, s))

        return {
            "ok": True,
            "mode": mode,
            "end_day": d,
            "shift_type": s,
            "upserted": upsert_count,
            "deleted": deleted,
            "total_after_in_shift": total_after,
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