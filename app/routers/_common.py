# app/routers/_common.py
from __future__ import annotations

import re


def norm_day(value: str) -> str:
    """
    YYYYMMDD / YYYY-MM-DD 紐⑤몢 ?덉슜 -> YYYYMMDD 諛섑솚
    """
    if value is None:
        raise ValueError("end_day is required")

    s = str(value).strip()
    s = s.replace("-", "")

    if not re.fullmatch(r"\d{8}", s):
        raise ValueError("end_day must be YYYYMMDD or YYYY-MM-DD")
    return s


def norm_shift(value: str) -> str:
    """
    day/night ?뺢퇋??
    """
    if value is None:
        raise ValueError("shift_type is required")

    s = str(value).strip().lower()
    if s not in ("day", "night"):
        raise ValueError("shift_type must be 'day' or 'night'")
    return s
