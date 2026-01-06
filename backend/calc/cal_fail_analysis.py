from collections import defaultdict
from .cal_shift import classify_shift

def analyze_first_second_fail(prod_day: str, fail_rows: list[dict]) -> dict:
    """
    FAIL row들만 들어온다고 가정.
    barcode_information별로 시간순 정렬 후:
    - 1개면 1st_FAIL_step_description
    - 2개 이상이면 2nd_FAIL_step_description까지 채움
    shift별로 (pn, step_description, station) count 집계 형태로 반환
    """
    # shift -> barcode -> list of fails
    per_bc = {"DAY": defaultdict(list), "NIGHT": defaultdict(list)}

    for r in fail_rows:
        end_day = str(r.get("end_day"))
        end_time = str(r.get("end_time"))
        bc = r.get("barcode_information") or ""
        shift = classify_shift(prod_day, end_day, end_time)
        if shift not in ("DAY", "NIGHT"):
            continue
        per_bc[shift][bc].append(r)

    def sort_key(x):
        return (str(x.get("end_day")), str(x.get("end_time")))

    # 결과: shift -> { "first": [...], "second": [...] }
    out = {"DAY": {"first": [], "second": []}, "NIGHT": {"first": [], "second": []}}

    for shift in ("DAY", "NIGHT"):
        # 집계: (pn, step, station) -> count
        first_agg = defaultdict(int)
        second_agg = defaultdict(int)

        for bc, items in per_bc[shift].items():
            items_sorted = sorted(items, key=sort_key)
            if not items_sorted:
                continue

            # 1st
            r1 = items_sorted[0]
            pn1 = r1.get("pn") or "UNKNOWN"
            step1 = r1.get("step_description") or "UNKNOWN"
            st1 = r1.get("station") or "UNKNOWN"
            first_agg[(pn1, step1, st1)] += 1

            # 2nd
            if len(items_sorted) >= 2:
                r2 = items_sorted[1]
                pn2 = r2.get("pn") or "UNKNOWN"
                step2 = r2.get("step_description") or "UNKNOWN"
                st2 = r2.get("station") or "UNKNOWN"
                second_agg[(pn2, step2, st2)] += 1

        out[shift]["first"] = [
            {"pn": pn, "1st_FAIL_step_description": step, "station": st, "count": cnt}
            for (pn, step, st), cnt in sorted(first_agg.items(), key=lambda x: (-x[1], x[0]))
        ]
        out[shift]["second"] = [
            {"pn": pn, "2nd_FAIL_step_description": step, "station": st, "count": cnt}
            for (pn, step, st), cnt in sorted(second_agg.items(), key=lambda x: (-x[1], x[0]))
        ]

    return out
