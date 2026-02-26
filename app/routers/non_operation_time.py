# app/routers/non_operation_time.py
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, time
from typing import Literal, Tuple, List, Dict, Any, Optional
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from pydantic import BaseModel, Field
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(tags=["7.non_operation_time"])

KST = ZoneInfo("Asia/Seoul")

# =========================================================
# legacy sources (기존 유지)
# =========================================================
SCHEMA = "g_production_film"
FCT_NONOP_TABLE = "fct_non_operation_time"
VISION_NONOP_TABLE = "vision_non_operation_time"

FCT_STATIONS = {"FCT1", "FCT2", "FCT3", "FCT4"}
VISION_STATIONS = {"Vision1", "Vision2"}

NONOP_PAGE_LIMIT = 50
NONOP_PAGE_LIMIT_MAX = 2000

# =========================================================
# NEW consolidated table (운영)
# =========================================================
NEW_SCHEMA = "i_daily_report"
NEW_TABLE = "total_non_operation_time"
NEW_STATIONS = {"FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"}

# -----------------------------
# Models (legacy)
# -----------------------------
class NonOpRow(BaseModel):
    end_day: Optional[str] = Field(None, description="YYYYMMDD or YYYY-MM-DD")
    shift_type: Optional[Literal["day", "night"]] = None

    station: str
    from_time: str = Field(..., description="HH:MI:SS or ISO datetime; server will normalize")
    to_time: str = Field(..., description="HH:MI:SS or ISO datetime; server will normalize")

    reason: Optional[str] = ""
    sparepart: Optional[str] = ""


class NonOpSyncReq(BaseModel):
    end_day: Optional[str] = Field(None, description="YYYYMMDD or YYYY-MM-DD")
    shift_type: Optional[Literal["day", "night"]] = None
    rows: List[NonOpRow]

# -----------------------------
# Models (NEW)
# -----------------------------
class NonopUpdateRow(BaseModel):
    id: Optional[int] = None
    prod_day: str
    shift_type: Literal["day", "night"]
    station: str
    from_ts: str  # iso
    to_ts: str    # iso
    reason: Optional[str] = None
    sparepart: Optional[str] = None


class NonopUpdateReq(BaseModel):
    rows: List[NonopUpdateRow] = Field(default_factory=list)

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


def _shift_window(shift_type: str) -> Tuple[time, time]:
    if shift_type == "day":
        return time(8, 30, 0), time(20, 30, 0)
    return time(20, 30, 0), time(8, 30, 0)


def _to_dt(day_yyyymmdd: str, t: time) -> datetime:
    return datetime.strptime(day_yyyymmdd + t.strftime("%H%M%S"), "%Y%m%d%H%M%S")


def _resolve_interval(day: str, start_t: time, end_t: time) -> Tuple[datetime, datetime]:
    st = _to_dt(day, start_t)
    ed = _to_dt(day, end_t)
    if ed <= st:
        ed = ed + timedelta(days=1)
    return st, ed


def _normalize_reason_sparepart(reason: Optional[str], sparepart: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    r = (reason or "").strip()
    sp = (sparepart or "").strip()

    r = r if r else None
    sp = sp if sp else None

    if sp and not r:
        r = "sparepart 교체"

    if r and r != "sparepart 교체":
        sp = None

    return r, sp


def _clamp_limit(limit: int) -> int:
    if limit <= 0:
        return 0
    return min(int(limit), NONOP_PAGE_LIMIT_MAX)


def _json_safe_row(d: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(d)
    for k in ("created_at", "updated_at", "from_ts", "to_ts"):
        v = out.get(k)
        if isinstance(v, datetime):
            out[k] = v.isoformat()
    return out


def _parse_since_updated_at(v: Optional[str]) -> datetime:
    if not v:
        return datetime(1970, 1, 1, tzinfo=KST)
    s = str(v).strip()
    if not s:
        return datetime(1970, 1, 1, tzinfo=KST)
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        try:
            dt = datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
        except Exception:
            raise ValueError("since_updated_at must be ISO8601 datetime string") from None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    return dt


_TIME_HHMMSS_RE = re.compile(r"(\d{2}:\d{2}:\d{2})")
_TIME_HHMM_RE = re.compile(r"(\d{2}:\d{2})(?!:)")


def _norm_time_str(v: str) -> str:
    s = (v or "").strip()
    if not s:
        raise ValueError("from_time/to_time empty")

    if len(s) >= 8 and s[2] == ":" and s[5] == ":":
        return s[:8]

    if len(s) == 5 and s[2] == ":":
        return s + ":00"

    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        return dt.strftime("%H:%M:%S")
    except Exception:
        pass

    m = _TIME_HHMMSS_RE.search(s)
    if m:
        return m.group(1)

    m2 = _TIME_HHMM_RE.search(s)
    if m2:
        return m2.group(1) + ":00"

    raise ValueError(f"invalid time format: {v}")


def _norm_iso_ts(v: str) -> str:
    s = (v or "").strip()
    if not s:
        raise ValueError("from_ts/to_ts empty")
    s2 = s.replace("Z", "+00:00")
    if " " in s2 and "T" not in s2 and len(s2) >= 19 and s2[4] == "-" and s2[7] == "-":
        s2 = s2.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(s2)
    except Exception:
        raise ValueError(f"invalid ISO datetime: {v}") from None
    if dt.tzinfo is None:
        raise ValueError(f"from_ts/to_ts must include timezone offset: {v}")
    return dt.isoformat()


ROW_KEY_EXPR = """
md5(
  coalesce(station,'') || '|' ||
  coalesce(from_time::text,'') || '|' ||
  coalesce(to_time::text,'') || '|' ||
  coalesce(reason,'') || '|' ||
  coalesce(sparepart,'')
)
"""

# =========================================================
# LEGACY GET: /non_operation_time (기존 유지)
# =========================================================
@router.get("/non_operation_time")
def get_non_operation_time(
    end_day: str = Query(..., description="YYYYMMDD or YYYY-MM-DD"),
    shift_type: Literal["day", "night"] = Query(...),
    limit: int = Query(0, ge=0, le=2000, description="0이면 legacy(전체) / >0이면 paged"),
    offset: int = Query(0, ge=0, description="paged offset"),
    db: Session = Depends(get_db),
) -> Response:
    try:
        d = _norm_day(end_day)
        s = _norm_shift(shift_type)

        win_st_t, win_ed_t = _shift_window(s)
        win_st, win_ed = _resolve_interval(d, win_st_t, win_ed_t)

        lim = _clamp_limit(limit)
        off = int(offset) if lim > 0 else 0

        sql_base = f"""
        WITH params AS (
            SELECT
                CAST(:win_st AS timestamp) AS win_st,
                CAST(:win_ed AS timestamp) AS win_ed
        ),
        all_rows AS (
            SELECT
                'fct'::text AS source,
                t.end_day::text AS end_day,
                t.station::text AS station,
                t.from_time::text AS from_time,
                t.to_time::text AS to_time,
                COALESCE(t.reason::text, '') AS reason,
                COALESCE(t.sparepart::text, '') AS sparepart,
                t.created_at AS created_at,
                t.updated_at AS updated_at,
                {ROW_KEY_EXPR} AS row_key
            FROM {SCHEMA}.{FCT_NONOP_TABLE} t
            WHERE t.end_day::text = :end_day
              AND t.station IN ('FCT1','FCT2','FCT3','FCT4')

            UNION ALL

            SELECT
                'vision'::text AS source,
                t.end_day::text AS end_day,
                t.station::text AS station,
                t.from_time::text AS from_time,
                t.to_time::text AS to_time,
                COALESCE(t.reason::text, '') AS reason,
                COALESCE(t.sparepart::text, '') AS sparepart,
                t.created_at AS created_at,
                t.updated_at AS updated_at,
                {ROW_KEY_EXPR} AS row_key
            FROM {SCHEMA}.{VISION_NONOP_TABLE} t
            WHERE t.end_day::text = :end_day
              AND t.station IN ('Vision1','Vision2')
        ),
        filtered AS (
            SELECT a.*
            FROM all_rows a, params p
            WHERE a.created_at >= p.win_st
              AND a.created_at <  p.win_ed
        )
        """

        args: Dict[str, Any] = {"end_day": d, "win_st": win_st, "win_ed": win_ed}

        if lim > 0:
            rows_sql = text(
                sql_base
                + """
                SELECT
                    source, end_day, station, from_time, to_time, reason, sparepart,
                    created_at, updated_at, row_key
                FROM filtered
                ORDER BY created_at DESC, row_key DESC
                LIMIT :limit OFFSET :offset;
                """
            )
            args2 = {**args, "limit": lim, "offset": off}
        else:
            rows_sql = text(
                sql_base
                + """
                SELECT
                    source, end_day, station, from_time, to_time, reason, sparepart,
                    created_at, updated_at, row_key
                FROM filtered
                ORDER BY created_at DESC, row_key DESC;
                """
            )
            args2 = args

        rows = db.execute(rows_sql, args2).mappings().all()
        items = [_json_safe_row(dict(r)) for r in rows]

        next_offset = None
        if lim > 0:
            next_offset = (off + len(items)) if len(items) == lim else None

        payload = {
            "end_day": d,
            "shift_type": s,
            "window_start": win_st.strftime("%Y-%m-%d %H:%M:%S"),
            "window_end": win_ed.strftime("%Y-%m-%d %H:%M:%S"),
            "count": len(items),
            "limit": lim if lim > 0 else None,
            "offset": off if lim > 0 else None,
            "next_offset": next_offset,
            "rows": items,
        }

        return Response(content=json.dumps(payload, ensure_ascii=False), media_type="application/json; charset=utf-8")

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"non_operation_time failed: {e}") from e


# =========================================================
# LEGACY HEAD: /non_operation_time/head (기존 유지)
# =========================================================
@router.get("/non_operation_time/head")
def get_non_operation_time_head(
    end_day: str = Query(..., description="YYYYMMDD or YYYY-MM-DD"),
    limit: int = Query(NONOP_PAGE_LIMIT, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> Response:
    try:
        d = _norm_day(end_day)
        lim = int(limit)

        sql = text(f"""
            WITH all_rows AS (
                SELECT
                    'fct'::text AS source,
                    t.end_day::text AS end_day,
                    t.station::text AS station,
                    t.from_time::text AS from_time,
                    t.to_time::text AS to_time,
                    COALESCE(t.reason::text, '') AS reason,
                    COALESCE(t.sparepart::text, '') AS sparepart,
                    t.created_at AS created_at,
                    t.updated_at AS updated_at,
                    {ROW_KEY_EXPR} AS row_key
                FROM {SCHEMA}.{FCT_NONOP_TABLE} t
                WHERE t.end_day::text = :end_day
                  AND t.station IN ('FCT1','FCT2','FCT3','FCT4')

                UNION ALL

                SELECT
                    'vision'::text AS source,
                    t.end_day::text AS end_day,
                    t.station::text AS station,
                    t.from_time::text AS from_time,
                    t.to_time::text AS to_time,
                    COALESCE(t.reason::text, '') AS reason,
                    COALESCE(t.sparepart::text, '') AS sparepart,
                    t.created_at AS created_at,
                    t.updated_at AS updated_at,
                    {ROW_KEY_EXPR} AS row_key
                FROM {SCHEMA}.{VISION_NONOP_TABLE} t
                WHERE t.end_day::text = :end_day
                  AND t.station IN ('Vision1','Vision2')
            )
            SELECT
                source, end_day, station, from_time, to_time, reason, sparepart,
                created_at, updated_at, row_key
            FROM all_rows
            ORDER BY created_at DESC, row_key DESC
            LIMIT :limit;
        """)

        rows = db.execute(sql, {"end_day": d, "limit": lim}).mappings().all()
        items = [_json_safe_row(dict(r)) for r in rows]

        next_cursor = None
        if items:
            last = items[-1]
            next_cursor = {"cursor_created_at": last["created_at"], "cursor_row_key": last["row_key"]}

        payload = {"end_day": d, "limit": lim, "items": items, "next_cursor": next_cursor}
        return Response(content=json.dumps(payload, ensure_ascii=False), media_type="application/json; charset=utf-8")

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"non_operation_time head failed: {e}") from e


# =========================================================
# LEGACY POST: update-only (기존 유지)
# =========================================================
def _save_non_operation_update_only(
    req: NonOpSyncReq,
    query_end_day: Optional[str],
    query_shift_type: Optional[str],
    db: Session,
) -> Dict[str, Any]:
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
    skipped: List[Dict[str, str]] = []

    for row in req.rows:
        stn = (row.station or "").strip()
        if not stn:
            skipped.append({"station": "", "reason": "empty station"})
            continue

        target_day = _norm_day(row.end_day) if (row.end_day and row.end_day.strip()) else base_day

        try:
            ft_raw = _norm_time_str(row.from_time)
            tt_raw = _norm_time_str(row.to_time)
        except ValueError as e:
            skipped.append({"station": stn, "reason": str(e)})
            continue

        reason, sparepart = _normalize_reason_sparepart(row.reason, row.sparepart)

        if stn in FCT_STATIONS:
            table = FCT_NONOP_TABLE
        elif stn in VISION_STATIONS:
            table = VISION_NONOP_TABLE
        else:
            skipped.append({"station": stn, "reason": "unknown station"})
            continue

        upd_sql = text(
            f"""
            UPDATE {SCHEMA}.{table}
               SET reason = COALESCE(NULLIF(:reason, ''), reason),
                   sparepart = CASE
                                WHEN COALESCE(NULLIF(:reason, ''), reason) = 'sparepart 교체'
                                  THEN COALESCE(NULLIF(:sparepart, ''), sparepart)
                                ELSE NULL
                              END,
                   updated_at = now()
             WHERE end_day::text = :end_day
               AND station::text = :station
               AND from_time::time = CAST(:from_time AS time)
               AND to_time::time   = CAST(:to_time AS time)
            """
        )

        res = db.execute(
            upd_sql,
            {
                "reason": (reason or ""),
                "sparepart": (sparepart or ""),
                "end_day": target_day,
                "station": stn,
                "from_time": ft_raw,
                "to_time": tt_raw,
            },
        )
        cnt = int(res.rowcount or 0)
        if stn in FCT_STATIONS:
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
        "skipped": skipped,
    }


@router.post("/non_operation_time")
def post_non_operation_time(
    req: NonOpSyncReq,
    end_day: Optional[str] = Query(None),
    shift_type: Optional[str] = Query(None),
    db: Session = Depends(get_db),
) -> Response:
    try:
        result = _save_non_operation_update_only(req, end_day, shift_type, db)
        return Response(content=json.dumps(result, ensure_ascii=False), media_type="application/json; charset=utf-8")
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
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
    try:
        result = _save_non_operation_update_only(req, end_day, shift_type, db)
        return Response(content=json.dumps(result, ensure_ascii=False), media_type="application/json; charset=utf-8")
    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"non_operation_time db error: {e}") from e
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"non_operation_time update failed: {e}") from e


# =========================================================
# NEW (FAST): consolidated nonop endpoints
# =========================================================
def _norm_prod_day(v: str) -> str:
    s = "".join(ch for ch in (v or "").strip() if ch.isdigit())[:8]
    if len(s) != 8:
        raise ValueError("prod_day must be YYYYMMDD")
    return s


def _window_bounds(prod_day: str, shift_type: str) -> Tuple[datetime, datetime]:
    d = _norm_prod_day(prod_day)
    s = _norm_shift(shift_type)
    base = datetime.strptime(d, "%Y%m%d").replace(tzinfo=KST)

    if s == "day":
        st = base.replace(hour=8, minute=30, second=0, microsecond=0)
        ed = base.replace(hour=20, minute=30, second=0, microsecond=0)
        return st, ed

    st = base.replace(hour=20, minute=30, second=0, microsecond=0)
    ed = (base + timedelta(days=1)).replace(hour=8, minute=30, second=0, microsecond=0)
    return st, ed


def _baseline_last_id(db: Session, prod_day: str, shift_type: str, win_start: datetime) -> int:
    sql = text(f"""
        SELECT COALESCE(MAX(id), 0) AS last_before_start
        FROM {NEW_SCHEMA}.{NEW_TABLE}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
          AND to_ts <= :win_start
    """)
    r = db.execute(sql, {"prod_day": prod_day, "shift_type": shift_type, "win_start": win_start}).scalar()
    try:
        return int(r or 0)
    except Exception:
        return 0


def _window_latest_cursor(db: Session, prod_day: str, shift_type: str, win_start: datetime, win_end: datetime) -> Tuple[int, str]:
    """
    ✅ seed limit에 걸려도 cursor가 '실제 최신'을 따라가도록
    - 최신 id 1건
    - 최신 updated_at 1건
    인덱스 타기 쉬운 ORDER BY ... LIMIT 1 방식(집계 MAX 회피)
    """
    q_latest_id = text(f"""
        SELECT id
        FROM {NEW_SCHEMA}.{NEW_TABLE}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
          AND from_ts < :win_end
          AND to_ts   > :win_start
        ORDER BY id DESC
        LIMIT 1
    """)
    q_latest_upd = text(f"""
        SELECT updated_at
        FROM {NEW_SCHEMA}.{NEW_TABLE}
        WHERE prod_day = :prod_day
          AND shift_type = :shift_type
          AND from_ts < :win_end
          AND to_ts   > :win_start
        ORDER BY updated_at DESC, id DESC
        LIMIT 1
    """)

    args = {"prod_day": prod_day, "shift_type": shift_type, "win_start": win_start, "win_end": win_end}

    rid = db.execute(q_latest_id, args).scalar()
    rupd = db.execute(q_latest_upd, args).scalar()

    max_id = int(rid or 0)
    if isinstance(rupd, datetime):
        max_updated_at = rupd.isoformat()
    else:
        max_updated_at = str(rupd or "")  # 빈문자 가능

    return max_id, max_updated_at


@router.get("/nonop/window")
def get_nonop_window(
    prod_day: str = Query(..., description="YYYYMMDD"),
    shift_type: Literal["day", "night"] = Query(...),
    limit: int = Query(2000, ge=1, le=2000),
    db: Session = Depends(get_db),
) -> Response:
    try:
        pd = _norm_prod_day(prod_day)
        sh = _norm_shift(shift_type)
        win_st, win_ed = _window_bounds(pd, sh)

        baseline_id = _baseline_last_id(db, pd, sh, win_st)

        sql_rows = text(f"""
            SELECT
              id, prod_day, shift_type, station,
              from_ts, to_ts, reason, sparepart,
              created_at, updated_at
            FROM {NEW_SCHEMA}.{NEW_TABLE}
            WHERE prod_day = :prod_day
              AND shift_type = :shift_type
              AND from_ts < :win_end
              AND to_ts   > :win_start
            ORDER BY from_ts ASC
            LIMIT :limit
        """)

        rows = db.execute(
            sql_rows,
            {"prod_day": pd, "shift_type": sh, "win_start": win_st, "win_end": win_ed, "limit": int(limit)},
        ).mappings().all()
        items = [_json_safe_row(dict(r)) for r in rows]

        # ✅ seed limit에 걸려도 항상 실제 최신을 cursor로 제공
        max_id, max_updated_at = _window_latest_cursor(db, pd, sh, win_st, win_ed)

        payload = {
            "prod_day": pd,
            "shift_type": sh,
            "window_start": win_st.isoformat(),
            "window_end": win_ed.isoformat(),
            "count": len(items),
            "limit": int(limit),

            "baseline_last_id": baseline_id,
            "max_id": max_id,
            "max_updated_at": max_updated_at,

            "items": items,
        }
        return Response(content=json.dumps(payload, ensure_ascii=False), media_type="application/json; charset=utf-8")

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"nonop window db error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"nonop window failed: {e}") from e


@router.get("/nonop/changes")
def get_nonop_changes(
    prod_day: str = Query(..., description="YYYYMMDD"),
    shift_type: Literal["day", "night"] = Query(...),
    since_id: int = Query(0, ge=0),
    since_updated_at: Optional[str] = Query(None, description="ISO8601. e.g. 2026-02-22T16:10:00+09:00"),
    limit: int = Query(5000, ge=1, le=20000),
    db: Session = Depends(get_db),
) -> Response:
    try:
        pd = _norm_prod_day(prod_day)
        sh = _norm_shift(shift_type)
        win_st, win_ed = _window_bounds(pd, sh)
        since_u = _parse_since_updated_at(since_updated_at)

        lim = int(limit)
        upd_lim = max(50, min(300, lim // 5))

        sql = text(f"""
            WITH new_rows AS (
                SELECT
                  id, prod_day, shift_type, station,
                  from_ts, to_ts, reason, sparepart,
                  created_at, updated_at
                FROM {NEW_SCHEMA}.{NEW_TABLE}
                WHERE prod_day = :prod_day
                  AND shift_type = :shift_type
                  AND from_ts < :win_end
                  AND to_ts   > :win_start
                  AND id > :since_id
                ORDER BY id ASC
                LIMIT :new_limit
            ),
            upd_rows AS (
                SELECT
                  id, prod_day, shift_type, station,
                  from_ts, to_ts, reason, sparepart,
                  created_at, updated_at
                FROM {NEW_SCHEMA}.{NEW_TABLE}
                WHERE prod_day = :prod_day
                  AND shift_type = :shift_type
                  AND from_ts < :win_end
                  AND to_ts   > :win_start
                  AND updated_at > :since_updated_at
                ORDER BY updated_at ASC, id ASC
                LIMIT :upd_limit
            ),
            merged AS (
                SELECT * FROM new_rows
                UNION ALL
                SELECT * FROM upd_rows
            )
            SELECT DISTINCT ON (id)
              id, prod_day, shift_type, station,
              from_ts, to_ts, reason, sparepart,
              created_at, updated_at
            FROM merged
            -- ✅ 최신 updated_at을 선택해야 한다
            ORDER BY id ASC, updated_at DESC
            LIMIT :limit
        """)

        rows = db.execute(
            sql,
            {
                "prod_day": pd,
                "shift_type": sh,
                "win_start": win_st,
                "win_end": win_ed,
                "since_id": int(since_id),
                "since_updated_at": since_u,
                "new_limit": lim,
                "upd_limit": upd_lim,
                "limit": lim,
            },
        ).mappings().all()

        items = [_json_safe_row(dict(r)) for r in rows]

        max_id = int(since_id)
        max_updated = since_u
        for it in items:
            try:
                max_id = max(max_id, int(it.get("id") or 0))
            except Exception:
                pass

            u = it.get("updated_at")
            try:
                udt = u if isinstance(u, datetime) else datetime.fromisoformat(str(u))
                if udt.tzinfo is None:
                    udt = udt.replace(tzinfo=KST)
                if udt > max_updated:
                    max_updated = udt
            except Exception:
                pass

        payload = {
            "prod_day": pd,
            "shift_type": sh,
            "window_start": win_st.isoformat(),
            "window_end": win_ed.isoformat(),

            "since_id": int(since_id),
            "since_updated_at": since_u.isoformat(),

            "count": len(items),
            "max_id": max_id,
            "max_updated_at": max_updated.isoformat(),

            "items": items,
        }
        return Response(content=json.dumps(payload, ensure_ascii=False), media_type="application/json; charset=utf-8")

    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"nonop changes db error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"nonop changes failed: {e}") from e


@router.post("/nonop/update")
def post_nonop_update(
    req: NonopUpdateReq,
    db: Session = Depends(get_db),
) -> Response:
    try:
        rows = req.rows or []
        if not rows:
            return Response(
                content=json.dumps({"ok": True, "updated": 0}, ensure_ascii=False),
                media_type="application/json; charset=utf-8",
            )

        upd_by_id = text(f"""
            UPDATE {NEW_SCHEMA}.{NEW_TABLE}
               SET
                 reason = COALESCE(NULLIF(:reason, ''), reason),
                 sparepart = CASE
                   WHEN COALESCE(NULLIF(:reason, ''), reason) = 'sparepart 교체'
                     THEN COALESCE(NULLIF(:sparepart, ''), sparepart)
                   ELSE NULL
                 END,
                 updated_at = now()
             WHERE id = :id
        """)

        upd_by_key = text(f"""
            UPDATE {NEW_SCHEMA}.{NEW_TABLE}
               SET
                 reason = COALESCE(NULLIF(:reason, ''), reason),
                 sparepart = CASE
                   WHEN COALESCE(NULLIF(:reason, ''), reason) = 'sparepart 교체'
                     THEN COALESCE(NULLIF(:sparepart, ''), sparepart)
                   ELSE NULL
                 END,
                 updated_at = now()
             WHERE prod_day = :prod_day
               AND shift_type = :shift_type
               AND station = :station
               AND from_ts = CAST(:from_ts AS timestamptz)
               AND to_ts   = CAST(:to_ts   AS timestamptz)
        """)

        updated = 0
        for r in rows:
            stn = (r.station or "").strip()
            if stn not in NEW_STATIONS:
                continue

            norm_reason, norm_sp = _normalize_reason_sparepart(r.reason, r.sparepart)
            reason_s = norm_reason or ""
            spare_s = norm_sp or ""

            if r.id is not None and int(r.id) > 0:
                res = db.execute(
                    upd_by_id,
                    {"id": int(r.id), "reason": reason_s, "sparepart": spare_s},
                )
                updated += int(res.rowcount or 0)
            else:
                pd = _norm_prod_day(r.prod_day)
                sh = _norm_shift(r.shift_type)
                from_ts = _norm_iso_ts(r.from_ts)
                to_ts = _norm_iso_ts(r.to_ts)

                res = db.execute(
                    upd_by_key,
                    {
                        "prod_day": pd,
                        "shift_type": sh,
                        "station": stn,
                        "from_ts": from_ts,
                        "to_ts": to_ts,
                        "reason": reason_s,
                        "sparepart": spare_s,
                    },
                )
                updated += int(res.rowcount or 0)

        db.commit()
        return Response(
            content=json.dumps({"ok": True, "updated": updated}, ensure_ascii=False),
            media_type="application/json; charset=utf-8",
        )

    except ValueError as e:
        db.rollback()
        raise HTTPException(status_code=422, detail=str(e)) from e
    except (OperationalError, DBAPIError) as e:
        db.rollback()
        raise HTTPException(status_code=503, detail=f"nonop update db error: {e}") from e
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"nonop update failed: {e}") from e