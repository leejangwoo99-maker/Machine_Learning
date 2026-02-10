from __future__ import annotations

def repair_mojibake(v):
    if not isinstance(v, str):
        return v
    # LATIN1濡??쏀엺 源⑥쭊 臾몄옄?댁쓣 CP949濡?蹂듦뎄 ?쒕룄
    try:
        return v.encode("latin1").decode("cp949")
    except Exception:
        return v
