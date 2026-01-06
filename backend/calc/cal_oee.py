from collections import defaultdict
from .cal_shift import classify_shift

def calc_standard_time_and_oee(prod_day: str, rows: list[dict], planned_stop_sec: int, downtime_sec: int) -> dict:
    """
    표준 생산 시간(hr) = PASS수량 * ct / 3600 :contentReference[oaicite:6]{index=6}
    OEE = 표준 생산 시간 합계 / 실 작업시간 * 100
    실 작업시간 = 12시간 - (계획정지 + 비가동)  (Front 입력)
    """
    total_work_sec = 12 * 3600
    real_work_sec = max(0, total_work_sec - int(planned_stop_sec) - int(downtime_sec))
    real_work_hr = real_work_sec / 3600.0 if real_work_sec else 0.0

    # shift -> pn -> {pass_qty, ct, std_hr}
    stat = {"DAY": defaultdict(lambda: {"pass_qty": 0, "ct": None}),
            "NIGHT": defaultdict(lambda: {"pass_qty": 0, "ct": None})}

    for r in rows:
        end_day = str(r.get("end_day"))
        end_time = str(r.get("end_time"))
        shift = classify_shift(prod_day, end_day, end_time)
        if shift not in ("DAY", "NIGHT"):
            continue

        pn = r.get("pn") or "UNKNOWN"
        ct = r.get("ct")
        result = (r.get("result") or "").upper()

        if stat[shift][pn]["ct"] is None and ct is not None:
            stat[shift][pn]["ct"] = ct

        if result == "PASS":
            stat[shift][pn]["pass_qty"] += 1

    out = {"DAY": {}, "NIGHT": {}}
    for shift in ("DAY", "NIGHT"):
        total_std_hr = 0.0
        pn_rows = []
        for pn, v in stat[shift].items():
            ct = v["ct"]
            pass_qty = v["pass_qty"]
            std_hr = (pass_qty * float(ct) / 3600.0) if ct is not None else 0.0
            total_std_hr += std_hr
            pn_rows.append({"pn": pn, "ct": ct, "pass_qty": pass_qty, "std_hr": round(std_hr, 4)})

        oee = round((total_std_hr / real_work_hr * 100.0), 2) if real_work_hr > 0 else None
        out[shift] = {
            "per_pn": sorted(pn_rows, key=lambda x: x["pn"]),
            "std_total_hr": round(total_std_hr, 4),
            "real_work_hr": round(real_work_hr, 4),
            "oee_pct": oee,
            "planned_stop_sec": int(planned_stop_sec),
            "downtime_sec": int(downtime_sec),
        }

    return out
