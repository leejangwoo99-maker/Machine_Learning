from collections import defaultdict
from .cal_shift import classify_shift

def calc_yield(prod_day: str, rows: list[dict]) -> dict:
    """
    station별 PASS%를 주간/야간으로 산출.
    Total은 Vision1 + Vision2의 합계에서 PASS% 계산. :contentReference[oaicite:5]{index=5}
    """
    # shift -> station -> {pass, total}
    stat = {
        "DAY": defaultdict(lambda: {"pass": 0, "total": 0}),
        "NIGHT": defaultdict(lambda: {"pass": 0, "total": 0}),
    }

    for r in rows:
        end_day = str(r.get("end_day"))
        end_time = str(r.get("end_time"))
        station = r.get("station") or "UNKNOWN"
        result = (r.get("result") or "").upper()

        shift = classify_shift(prod_day, end_day, end_time)
        if shift not in ("DAY", "NIGHT"):
            continue

        stat[shift][station]["total"] += 1
        if result == "PASS":
            stat[shift][station]["pass"] += 1

    def pct(p, t):
        return round((p / t * 100.0), 2) if t else None

    out = {"DAY": {}, "NIGHT": {}}
    for shift in ("DAY", "NIGHT"):
        # station별
        for st, v in stat[shift].items():
            out[shift][st] = {
                "pass": v["pass"],
                "total": v["total"],
                "pass_pct": pct(v["pass"], v["total"]),
            }

        # Vision Total
        v_pass = stat[shift]["Vision1"]["pass"] + stat[shift]["Vision2"]["pass"]
        v_tot  = stat[shift]["Vision1"]["total"] + stat[shift]["Vision2"]["total"]
        out[shift]["VisionTotal"] = {
            "pass": v_pass,
            "total": v_tot,
            "pass_pct": pct(v_pass, v_tot),
        }

    return out
