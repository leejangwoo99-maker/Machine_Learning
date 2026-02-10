from __future__ import annotations

from datetime import datetime, timedelta
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")


def normalize_prod_day(value: str) -> str:
    s = str(value).strip().replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise ValueError("prod_day must be YYYYMMDD")
    return s


def normalize_shift_type(value: str) -> str:
    s = str(value).strip().lower()
    if s not in ("day", "night"):
        raise ValueError("shift_type must be day|night")
    return s


def default_window_now() -> Tuple[str, str]:
    """
    KST 湲곗? window 湲곕낯媛?
    - day:   08:30:00 ~ 20:29:59
    - night: ?섎㉧吏
      (00:00:00 ~ 08:29:59 ???꾩씪 night)
    """
    now = datetime.now(KST)
    t = now.time()

    if (t.hour, t.minute, t.second) >= (8, 30, 0) and (t.hour, t.minute, t.second) <= (20, 29, 59):
        return now.strftime("%Y%m%d"), "day"

    if (t.hour, t.minute, t.second) < (8, 30, 0):
        return (now - timedelta(days=1)).strftime("%Y%m%d"), "night"

    return now.strftime("%Y%m%d"), "night"


def resolve_window(prod_day: Optional[str], shift_type: Optional[str]) -> Tuple[str, str]:
    """
    ?????낅젰?섎㈃ ?뺢퇋?뷀빐??諛섑솚.
    ?섎굹?쇰룄 ?놁쑝硫?KST ?꾩옱?쒓컖 湲곗? 湲곕낯 window瑜?梨꾩슫??
    """
    d_def, s_def = default_window_now()

    d = normalize_prod_day(prod_day) if prod_day else d_def
    s = normalize_shift_type(shift_type) if shift_type else s_def
    return d, s
