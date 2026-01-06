from dataclasses import dataclass
from datetime import datetime, timedelta

DAY_START = "08:30:00"
DAY_END   = "20:29:59"
NIGHT_START = "20:30:00"
NIGHT_END_NEXT = "08:29:59"

@dataclass(frozen=True)
class Band:
    code: str
    start: str
    end: str

DAY_BANDS = [
    Band("A", "08:30:00", "10:29:59"),
    Band("B", "10:30:00", "12:29:59"),
    Band("C", "12:30:00", "14:29:59"),
    Band("D", "14:30:00", "16:29:59"),
    Band("E", "16:30:00", "18:29:59"),
    Band("F", "18:30:00", "20:29:59"),
]

NIGHT_BANDS = [
    Band("A'", "20:30:00", "22:29:59"),
    Band("B'", "22:30:00", "00:29:59"),
    Band("C'", "00:30:00", "02:29:59"),
    Band("D'", "02:30:00", "04:29:59"),
    Band("E'", "04:30:00", "06:29:59"),
    Band("F'", "06:30:00", "08:29:59"),
]

def next_day_yyyymmdd(yyyymmdd: str) -> str:
    d = datetime.strptime(yyyymmdd, "%Y%m%d")
    return (d + timedelta(days=1)).strftime("%Y%m%d")

def classify_shift(prod_day: str, end_day: str, end_time: str) -> str:
    """
    prod_day 기준:
    - 주간: end_day=prod_day AND 08:30~20:29
    - 야간: (end_day=prod_day AND 20:30~23:59) OR (end_day=prod_day+1 AND 00:00~08:29)
    """
    nd = next_day_yyyymmdd(prod_day)

    if end_day == prod_day and DAY_START <= end_time <= DAY_END:
        return "DAY"
    if (end_day == prod_day and end_time >= NIGHT_START) or (end_day == nd and end_time < "08:30:00"):
        return "NIGHT"
    return "OUT"

def classify_band(shift: str, end_time: str) -> str:
    if shift == "DAY":
        for b in DAY_BANDS:
            if b.start <= end_time <= b.end:
                return b.code
    elif shift == "NIGHT":
        # 야간 band는 자정 넘어가는 구간이 있으므로 조건을 분리
        for b in NIGHT_BANDS:
            if b.code in ("B'", "C'", "D'", "E'", "F'"):
                # 00:00~08:29 구간
                if b.start <= end_time <= b.end:
                    return b.code
            else:
                # A' (20:30~22:29)
                if b.start <= end_time <= b.end:
                    return b.code
    return "OUT"
