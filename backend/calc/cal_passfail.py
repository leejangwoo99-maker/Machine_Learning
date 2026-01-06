from collections import defaultdict
from .cal_shift import classify_shift, classify_band

def build_passfail_table(prod_day: str, rows: list[dict]) -> dict:
    """
    결과 형태:
    {
      "DAY": { pn: { band: {"PASS":x,"FAIL":y} } },
      "NIGHT": { ... }
    }
    """
    agg = {
        "DAY": defaultdict(lambda: defaultdict(lambda: {"PASS": 0, "FAIL": 0})),
        "NIGHT": defaultdict(lambda: defaultdict(lambda: {"PASS": 0, "FAIL": 0})),
    }

    for r in rows:
        end_day = str(r.get("end_day"))
        end_time = str(r.get("end_time"))
        result = (r.get("result") or "").upper()
        pn = r.get("pn") or "UNKNOWN"

        shift = classify_shift(prod_day, end_day, end_time)
        if shift not in ("DAY", "NIGHT"):
            continue

        band = classify_band(shift, end_time)
        if band == "OUT":
            continue

        if result not in ("PASS", "FAIL"):
            continue

        agg[shift][pn][band][result] += 1

    # dict로 변환
    def to_dict(d):
        return {k: dict(v) for k, v in d.items()}

    return {
        "DAY": {pn: dict(bands) for pn, bands in agg["DAY"].items()},
        "NIGHT": {pn: dict(bands) for pn, bands in agg["NIGHT"].items()},
    }
