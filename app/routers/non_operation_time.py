# app/routers/non_operation_time.py
from __future__ import annotations

import json
from datetime import datetime, timedelta, time
from typing import Literal, Tuple, List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(tags=["7.non_operation_time"])

SCHEMA = "g_production_film"
FCT_NONOP_TABLE = "fct_non_operation_time"
VISION_NONOP_TABLE = "vision_non_operation_time"

FCT_STATIONS = {"FCT1", "FCT2", "FCT3", "FCT4"}
VISION_STATIONS = {"Vision1", "Vision2"}


# -----------------------------
# Models
# -----------------------------
class NonOpRow(BaseModel):
    # end_day/shift_type는 row에 없어도 되게 Optional
    end_day: Optional[str] = Field(None, description="YYYYMMDD or YYYY-MM-DD")
    shift_type: Optional[Literal["day", "night"]] = None

    station: str
    from_time: str = Field(..., description="HH:MM or HH:MM:SS or HH:MM:SS.ss")
    to_time: str = Field(..., description="HH:MM or HH:MM:SS or HH:MM:SS.ss")

    reason: Optional[str] = ""
    sparepart: Optional[str] = ""


class NonOpSyncReq(BaseModel):
    # body 레벨 end_day/shift_type도 Optional 처리(쿼리로 받을 수 있게)
    end_day: Optional[str] = Field(None, description="YYYYMMDD or YYYY-MM-DD")
    shift_type: Optional[Literal["day", "night"]] = None
    rows: List[NonOpRow]


# -----------------------------
# utils
# -----------------------------
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


def _norm_time_input(v: str) -> str:
    """
    입력 시간 정규화:
    - HH:MM
    - HH:MM:SS
    - HH:MM:SS.ss
    를 모두 허용해서 DB CAST(:t AS time)에 넣기 좋은 문자열로 통일
    """
    s = (v or "").strip()
    if not s:
        raise ValueError("time is empty")

    # HH:MM -> HH:MM:00
    if s.count(":") == 1:
        hh, mm = s.split(":")
        if len(hh) == 2 and len(mm) == 2 and hh.isdigit() and mm.isdigit():
            s = f"{hh}:{mm}:00"
        else:
            raise ValueError(f"invalid time format: {v}")

    # HH:MM:SS(.ss) 검증
    try:
        if "." in s:
            base, frac = s.split(".", 1)
            datetime.strptime(base, "%H:%M:%S")
            if frac and (not frac.isdigit()):
                raise ValueError
        else:
            datetime.strptime(s, "%H:%M:%S")
    except Exception as e:
        raise ValueError(f"invalid time format: {v}") from e

    return s


def _parse_hms(v: str) -> time:
    """
    GET 계산용 파서:
    - HH:MM
    - HH:MM:SS
    - HH:MM:SS.ss(소수초)
    둘 다 허용, 계산 시 초 단위로 사용
    """
    s = _norm_time_input(v)
    if "." in s:
        s = s.split(".", 1)[0]
    return datetime.strptime(s, "%H:%M:%S").time()


def _shift_window(shift_type: str) -> Tuple[time, time]:
    if shift_type == "day":
        return time(8, 30, 0), time(20, 30, 0)
    return time(20, 30, 0), time(8, 30, 0)


def _to_dt(day_yyyymmdd: str, t: time) -> datetime:
    return datetime.strptime(day_yyyymmdd + t.strftime("%H%M%S"), "%Y%m%d%H%M%S")


def _resolve_interval(day: str, start_t: time, end_t: time, shift_type: str) -> Tuple[datetime, datetime]:
    st = _to_dt(day, start_t)
    ed = _to_dt(day, end_t)
    if shift_type == "night" and ed <= st:
        ed = ed + timedelta(days=1)
    if shift_type == "day" and ed <= st:
        ed = ed + timedelta(days=1)
    return st, ed


def _intersect(a_st: datetime, a_ed: datetime, b_st: datetime, b_ed: datetime) -> bool:
    return not (a_ed <= b_st or b_ed <= a_st)


def _sec_for_sort(v: str) -> int:
    try:
        s = str(v or "").strip()
        if not s:
            return -1
        if s.count(":") == 1:
            s = f"{s}:00"
        if "." in s:
            s = s.split(".", 1)[0]
        hh, mm, ss = s.split(":")
        return int(hh) * 3600 + int(mm) * 60 + int(ss)
    except Exception:
        return -1


def _normalize_reason_sparepart(reason: Optional[str], sparepart: Optional[str]) -> Tuple[str, str]:
    r = (reason or "").strip()
    sp = (sparepart or "").strip()
    # reason이 sparepart 교체가 아니면 sparepart는 공백
    if r != "sparepart 교체":
        sp = ""
    return r, sp


# -----------------------------
# GET
# -----------------------------
@router.get("/non_operation_time")
def get_non_operation_time(
    end_day: str = Query(..., description="YYYYMMDD or YYYY-MM-DD"),
    shift_type: Literal["day", "night"] = Query(...),
    db: Session = Depends(get_db),
) -> Response:
    """
    비가동 상세 조회
    - FCT: g_production_film.fct_non_operation_time
    - Vision: g_production_film.vision_non_operation_time
    - 지정 shift window 에 겹치는 구간만 반환
    - from_time 최신순 정렬(desc)
    """
    try:
        d = _norm_day(end_day)
        s = _norm_shift(shift_type)

        win_st_t, win_ed_t = _shift_window(s)
        win_st, win_ed = _resolve_interval(d, win_st_t, win_ed_t, s)

        fct_rows = db.execute(
            text(
                f"""
                SELECT
                    end_day::text AS end_day,
                    station::text AS station,
                    from_time::text AS from_time,
                    to_time::text AS to_time,
                    COALESCE(reason::text, '') AS reason,
                    COALESCE(sparepart::text, '') AS sparepart
                FROM {SCHEMA}.{FCT_NONOP_TABLE}
                WHERE end_day::text = :end_day
                  AND station IN ('FCT1','FCT2','FCT3','FCT4')
                """
            ),
            {"end_day": d},
        ).mappings().all()

        vision_rows = db.execute(
            text(
                f"""
                SELECT
                    end_day::text AS end_day,
                    station::text AS station,
                    from_time::text AS from_time,
                    to_time::text AS to_time,
                    COALESCE(reason::text, '') AS reason,
                    COALESCE(sparepart::text, '') AS sparepart
                FROM {SCHEMA}.{VISION_NONOP_TABLE}
                WHERE end_day::text = :end_day
                  AND station IN ('Vision1','Vision2')
                """
            ),
            {"end_day": d},
        ).mappings().all()

        items: List[Dict[str, Any]] = []
        for src, rows in (("fct", fct_rows), ("vision", vision_rows)):
            for r in rows:
                st_, ed_ = _resolve_interval(d, _parse_hms(r["from_time"]), _parse_hms(r["to_time"]), s)
                if _intersect(st_, ed_, win_st, win_ed):
                    items.append(
                        {
                            "source": src,
                            "end_day": r["end_day"],
                            "shift_type": s,
                            "station": r["station"],
                            # DB 원본 문자열 유지 (HH:MM:SS.ss 포함 가능)
                            "from_time": r["from_time"],
                            "to_time": r["to_time"],
                            "reason": r.get("reason", "") or "",
                            "sparepart": r.get("sparepart", "") or "",
                        }
                    )

        items.sort(
            key=lambda x: (str(x.get("end_day", "")), _sec_for_sort(str(x.get("from_time", "")))),
            reverse=True,
        )

        payload = {
            "end_day": d,
            "shift_type": s,
            "window_start": win_st.strftime("%Y-%m-%d %H:%M:%S"),
            "window_end": win_ed.strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(items),
            "rows": items,
        }
        return Response(
            content=json.dumps(payload, ensure_ascii=False),
            media_type="application/json; charset=utf-8",
        )

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"non_operation_time failed: {e}") from e


# -----------------------------
# Internal save logic (UPDATE ONLY)
# -----------------------------
def _save_non_operation_update_only(
    req: NonOpSyncReq,
    query_end_day: Optional[str],
    query_shift_type: Optional[str],
    db: Session,
) -> Dict[str, Any]:
    # 우선순위: query > body
    base_day_raw = (query_end_day or req.end_day or "").strip()
    base_shift_raw = (query_shift_type or req.shift_type or "").strip()

    if not base_day_raw:
        raise ValueError("end_day is required (query or body)")
    if not base_shift_raw:
        raise ValueError("shift_type is required (query or body)")

    base_day = _norm_day(base_day_raw)
    base_shift = _norm_shift(base_shift_raw)

    updated_fct = 0
    updated_vision = 0
    no_change_skipped = 0
    not_found = 0
    skipped: List[Dict[str, str]] = []

    for row in req.rows:
        stn = (row.station or "").strip()
        if not stn:
            skipped.append({"station": "", "reason": "empty station"})
            continue

        target_day = _norm_day(row.end_day) if (row.end_day and row.end_day.strip()) else base_day

        # from/to는 PK 매칭용
        ft_raw = _norm_time_input(row.from_time or "")
        tt_raw = _norm_time_input(row.to_time or "")

        new_reason, new_sparepart = _normalize_reason_sparepart(row.reason, row.sparepart)

        if stn in FCT_STATIONS:
            table = FCT_NONOP_TABLE
            is_fct = True
        elif stn in VISION_STATIONS:
            table = VISION_NONOP_TABLE
            is_fct = False
        else:
            skipped.append({"station": stn, "reason": "unknown station"})
            continue

        # 1) 기존값 조회(변경 없으면 UPDATE 생략)
        sel_sql = text(
            f"""
            SELECT
                COALESCE(reason::text, '') AS reason,
                COALESCE(sparepart::text, '') AS sparepart
            FROM {SCHEMA}.{table}
            WHERE end_day::text = :end_day
              AND station::text = :station
              AND from_time::time = CAST(:from_time AS time)
              AND to_time::time   = CAST(:to_time AS time)
            LIMIT 1
            """
        )

        cur = db.execute(
            sel_sql,
            {
                "end_day": target_day,
                "station": stn,
                "from_time": ft_raw,
                "to_time": tt_raw,
            },
        ).mappings().first()

        if cur is None:
            # 네 정책: PK는 존재해야 함. 없으면 skip로 기록(삽입 안 함)
            not_found += 1
            skipped.append(
                {
                    "station": stn,
                    "reason": "target row not found",
                }
            )
            continue

        old_reason = (cur.get("reason") or "").strip()
        old_sparepart = (cur.get("sparepart") or "").strip()

        if old_reason == new_reason and old_sparepart == new_sparepart:
            no_change_skipped += 1
            continue

        # 2) 변경 있을 때만 UPDATE
        upd_sql = text(
            f"""
            UPDATE {SCHEMA}.{table}
               SET reason = :reason,
                   sparepart = :sparepart
             WHERE end_day::text = :end_day
               AND station::text = :station
               AND from_time::time = CAST(:from_time AS time)
               AND to_time::time   = CAST(:to_time AS time)
            """
        )
        res = db.execute(
            upd_sql,
            {
                "reason": new_reason,
                "sparepart": new_sparepart,
                "end_day": target_day,
                "station": stn,
                "from_time": ft_raw,
                "to_time": tt_raw,
            },
        )
        cnt = int(res.rowcount or 0)
        if is_fct:
            updated_fct += cnt
        else:
            updated_vision += cnt

    db.commit()

    return {
        "ok": True,
        "mode": "update_only_reason_sparepart",
        "end_day": base_day,
        "shift_type": base_shift,
        "updated": updated_fct + updated_vision,
        "updated_fct": updated_fct,
        "updated_vision": updated_vision,
        "no_change_skipped": no_change_skipped,   # 변경없음 저장 생략
        "not_found": not_found,                   # PK 미존재
        "skipped": skipped,
    }


# -----------------------------
# POST endpoints
# -----------------------------
@router.post("/non_operation_time")
def post_non_operation_time(
    req: NonOpSyncReq,
    end_day: Optional[str] = Query(None),
    shift_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> Response:
    """
    UPDATE ONLY:
    - from_time/to_time는 매칭키로만 사용
    - reason/sparepart만 업데이트
    - 변경사항 없으면 업데이트 생략
    """
    try:
        result = _save_non_operation_update_only(req, end_day, shift_type, db)
        return Response(
            content=json.dumps(result, ensure_ascii=False),
            media_type="application/json; charset=utf-8",
        )
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"non_operation_time update failed: {e}") from e


@router.post("/non_operation_time/sync")
def post_non_operation_time_sync(
    req: NonOpSyncReq,
    end_day: Optional[str] = Query(None),
    shift_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> Response:
    """
    UPDATE ONLY alias endpoint
    """
    try:
        result = _save_non_operation_update_only(req, end_day, shift_type, db)
        return Response(
            content=json.dumps(result, ensure_ascii=False),
            media_type="application/json; charset=utf-8",
        )
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"non_operation_time update failed: {e}") from e
