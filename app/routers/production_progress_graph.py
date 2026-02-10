# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta, time
from typing import Dict, List, Literal, Optional, Tuple

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/production_progress_graph", tags=["production_progress_graph"])

SCHEMA = "g_production_film"
PLANNED_TABLE = "planned_time"
FCT_NONOP_TABLE = "fct_non_operation_time"
VISION_NONOP_TABLE = "vision_non_operation_time"

STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"]


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


def _parse_hms(v: str) -> time:
    s = (v or "").strip()
    if not s:
        raise ValueError("time value is empty")
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


def _intersect(a_start: datetime, a_end: datetime, b_start: datetime, b_end: datetime) -> Optional[Tuple[datetime, datetime]]:
    s = max(a_start, b_start)
    e = min(a_end, b_end)
    if e <= s:
        return None
    return s, e


def _subtract_intervals(base: Tuple[datetime, datetime], cuts: List[Tuple[datetime, datetime]]) -> List[Tuple[datetime, datetime]]:
    pieces = [base]
    for cst, ced in cuts:
        new_pieces: List[Tuple[datetime, datetime]] = []
        for pst, ped in pieces:
            inter = _intersect(pst, ped, cst, ced)
            if not inter:
                new_pieces.append((pst, ped))
                continue
            ist, ied = inter
            if pst < ist:
                new_pieces.append((pst, ist))
            if ied < ped:
                new_pieces.append((ied, ped))
        pieces = new_pieces
        if not pieces:
            break
    return pieces


def _make_slots(window_start: datetime, window_end: datetime, slot_minutes: int) -> List[Tuple[datetime, datetime]]:
    out: List[Tuple[datetime, datetime]] = []
    cur = window_start
    delta = timedelta(minutes=slot_minutes)
    while cur < window_end:
        nxt = min(cur + delta, window_end)
        out.append((cur, nxt))
        cur = nxt
    return out


def _fmt_slot_label(st: datetime, ed: datetime) -> str:
    return f"{st.strftime('%H:%M')}~{ed.strftime('%H:%M')}"


def _fetch_planned(db: Session, end_day: str) -> List[Tuple[str, str]]:
    rows = db.execute(
        text(f"""
        SELECT from_time::text AS from_time, to_time::text AS to_time
        FROM {SCHEMA}.{PLANNED_TABLE}
        WHERE end_day = :end_day
        ORDER BY from_time, to_time
        """),
        {"end_day": end_day},
    ).mappings().all()
    return [(r["from_time"], r["to_time"]) for r in rows]


def _fetch_nonop_fct(db: Session, end_day: str) -> List[Tuple[str, str, str]]:
    rows = db.execute(
        text(f"""
        SELECT station, from_time::text AS from_time, to_time::text AS to_time
        FROM {SCHEMA}.{FCT_NONOP_TABLE}
        WHERE end_day = :end_day
          AND station IN ('FCT1','FCT2','FCT3','FCT4')
        ORDER BY station, from_time, to_time
        """),
        {"end_day": end_day},
    ).mappings().all()
    return [(r["station"], r["from_time"], r["to_time"]) for r in rows]


def _fetch_nonop_vision(db: Session, end_day: str) -> List[Tuple[str, str, str]]:
    rows = db.execute(
        text(f"""
        SELECT station, from_time::text AS from_time, to_time::text AS to_time
        FROM {SCHEMA}.{VISION_NONOP_TABLE}
        WHERE end_day = :end_day
          AND station IN ('Vision1','Vision2')
        ORDER BY station, from_time, to_time
        """),
        {"end_day": end_day},
    ).mappings().all()
    return [(r["station"], r["from_time"], r["to_time"]) for r in rows]


@router.get("")
def get_production_progress_graph(
    end_day: str = Query(...),
    shift_type: Literal["day", "night"] = Query(...),
    slot_minutes: int = Query(60, ge=5, le=120),
    db: Session = Depends(get_db),
) -> Response:
    try:
        d = _norm_day(end_day)
        s = _norm_shift(shift_type)

        win_st_t, win_ed_t = _shift_window(s)
        win_st, win_ed = _resolve_interval(d, win_st_t, win_ed_t, s)

        planned_raw = _fetch_planned(db, d)
        fct_raw = _fetch_nonop_fct(db, d)
        vision_raw = _fetch_nonop_vision(db, d)

        planned_intervals: List[Tuple[datetime, datetime]] = []
        for ft, tt in planned_raw:
            st, ed = _resolve_interval(d, _parse_hms(ft), _parse_hms(tt), s)
            inter = _intersect(st, ed, win_st, win_ed)
            if inter:
                planned_intervals.append(inter)

        nonop_map: Dict[str, List[Tuple[datetime, datetime]]] = {k: [] for k in STATIONS}

        for station, ft, tt in (fct_raw + vision_raw):
            st, ed = _resolve_interval(d, _parse_hms(ft), _parse_hms(tt), s)
            inter = _intersect(st, ed, win_st, win_ed)
            if inter and station in nonop_map:
                nonop_map[station].append(inter)

        for station in STATIONS:
            cleaned: List[Tuple[datetime, datetime]] = []
            for seg in nonop_map[station]:
                pieces = _subtract_intervals(seg, planned_intervals)
                cleaned.extend(pieces)
            nonop_map[station] = cleaned

        slots = _make_slots(win_st, win_ed, slot_minutes)
        slot_labels = [_fmt_slot_label(a, b) for a, b in slots]

        def _has_overlap(seg_list: List[Tuple[datetime, datetime]], slot_st: datetime, slot_ed: datetime) -> bool:
            for s1, e1 in seg_list:
                if _intersect(s1, e1, slot_st, slot_ed):
                    return True
            return False

        station_rows = []
        for stn in STATIONS:
            states: List[str] = []
            for sst, sed in slots:
                if _has_overlap(planned_intervals, sst, sed):
                    states.append("planned")
                elif _has_overlap(nonop_map[stn], sst, sed):
                    states.append("nonop")
                else:
                    states.append("run")
            station_rows.append({"station": stn, "states": states})

        payload = {
            "end_day": d,
            "shift_type": s,
            "slot_minutes": slot_minutes,
            "legend": {
                "run": "RUN",
                "planned": "PLANNED",
                "nonop": "NONOP",
                "idle": "IDLE",
            },
            "slots": slot_labels,
            "stations": station_rows,
            "meta": {
                "window_start": win_st.strftime("%Y-%m-%d %H:%M:%S"),
                "window_end": win_ed.strftime("%Y-%m-%d %H:%M:%S"),
                "planned_count": len(planned_intervals),
                "fct_nonop_count": len(fct_raw),
                "vision_nonop_count": len(vision_raw),
            },
        }

        import json
        return Response(
            content=json.dumps(payload, ensure_ascii=False),
            media_type="application/json; charset=utf-8",
        )

    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"production_progress_graph db error: {e}") from e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"production_progress_graph failed: {e}") from e
