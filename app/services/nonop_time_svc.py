from __future__ import annotations
from typing import Iterable
from sqlalchemy.engine import Engine
from app.core.timewindow import validate_yyyymmdd, validate_shift_type
from app.repos import nonop_time_repo
from app.schemas.nonop_time import NonOpResponse, NonOpRow


# window boundaries as strings (string compare works when HH:MM:SS padded)
DAY_START = "08:30:00"
DAY_END = "20:29:59"
N1_START = "20:30:00"  # night part1
N1_END = "23:59:59"
N2_START = "00:00:00"  # night part2
N2_END = "08:29:59"


def _normalize_time_str(s: str) -> str:
    """
    Accepts:
      - "HH:MM:SS"
      - "HH:MM:SS.xx"
    For string compare, we keep as-is but ensure it's at least HH:MM:SS length.
    """
    if not s:
        return ""
    s = s.strip()
    # already HH:MM:SS...
    if len(s) >= 8:
        return s
    return s


def _overlaps_interval(a_from: str, a_to: str, w_from: str, w_to: str) -> bool:
    """
    Overlap test on same-day intervals using string compare.
    We assume a_from <= a_to and w_from <= w_to in same 00:00~23:59 domain.
    """
    a_from = _normalize_time_str(a_from)
    a_to = _normalize_time_str(a_to)
    if not a_from or not a_to:
        return False
    return not (a_to < w_from or a_from > w_to)


def _overlaps_window(from_time: str, to_time: str, shift_type: str) -> bool:
    ft = _normalize_time_str(from_time)
    tt = _normalize_time_str(to_time)
    if not ft or not tt:
        return False

    st = validate_shift_type(shift_type)

    # day: 08:30~20:29:59
    if st == "day":
        return _overlaps_interval(ft, tt, DAY_START, DAY_END)

    # night: (20:30~23:59:59) OR (00:00~08:29:59)
    return (
        _overlaps_interval(ft, tt, N1_START, N1_END)
        or _overlaps_interval(ft, tt, N2_START, N2_END)
    )


def _filter_by_shift(rows: Iterable[NonOpRow], shift_type: str) -> list[NonOpRow]:
    st = validate_shift_type(shift_type)
    return [r for r in rows if _overlaps_window(r.from_time, r.to_time, st)]


def get_nonop(engine: Engine, end_day: str, shift_type: str | None) -> NonOpResponse:
    end_day = validate_yyyymmdd(end_day)

    fct = nonop_time_repo.list_fct(engine, end_day)
    vision = nonop_time_repo.list_vision(engine, end_day)

    if shift_type:
        fct = _filter_by_shift(fct, shift_type)
        vision = _filter_by_shift(vision, shift_type)

    return NonOpResponse(fct=fct, vision=vision)
