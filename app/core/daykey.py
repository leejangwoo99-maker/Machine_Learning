from __future__ import annotations

def norm_day_key(v: str) -> str:
    s=(v or "").strip()
    if len(s)==10 and s[4]=="-" and s[7]=="-":
        s=s.replace("-","")
    if len(s)!=8 or not s.isdigit():
        raise ValueError("day key must be YYYYMMDD or YYYY-MM-DD")
    return s

def norm_shift(v: str) -> str:
    s=(v or "").strip().lower()
    if s not in {"day","night"}:
        raise ValueError("shift_type must be day or night")
    return s
