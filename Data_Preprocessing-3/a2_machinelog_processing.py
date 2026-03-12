from pathlib import Path
from datetime import datetime, date, time, timedelta
import pandas as pd
import re

# =========================
# 1. 기본 경로 설정
# =========================
FCT_DIR = Path(r"C:\Users\user\Desktop\machinlog\FCT\10")
MAIN_DIR = Path(r"C:\Users\user\Desktop\machinlog\Main\10")
VISION_DIR = Path(r"C:\Users\user\Desktop\machinlog\Vision\10")

OUTPUT_DIR = Path(r"C:\Users\user\Desktop\machinlog\output")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# =========================
# 2. 유틸 / 정규식
# =========================

FILENAME_PATTERN = re.compile(
    r"(?P<date>\d{8})_(?P<kind>FCT[1-4]|Main|Vision[1-2])_Machine_Log\.txt",
    re.IGNORECASE
)

LINE_PATTERN = re.compile(
    r"^\[(?P<time>\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(?P<msg>.*)$"
)

# 엑셀 불가 제어문자(0x00~0x1F, 개행계열 제외)는 '_'로 치환
ILLEGAL_CHARS_RE = re.compile(r"[\x00-\x08\x0b-\x0c\x0e-\x1f]")


def sanitize_for_excel(text: str) -> str:
    """
    엑셀에서 허용 안 되는 제어문자를 '_' 로 교체
    """
    if text is None:
        return ""
    return ILLEGAL_CHARS_RE.sub("_", text)


def convert_to_excel_text(raw_text: str) -> str:
    """
    엑셀에 넣을 텍스트로 변환
    - [HH:MI:SS.ss]는 그대로 유지
    - 그 뒤 내용 부분 공백은 '_' 로 변환
    - 제어문자는 '_' 로 변환
    """
    if ']' not in raw_text:
        cleaned = raw_text.replace(" ", "_")
        return sanitize_for_excel(cleaned)

    prefix, rest = raw_text.split("]", 1)  # prefix: "[HH:MI:SS.ss"
    rest_replaced = rest.replace(" ", "_")
    result = prefix + "]" + rest_replaced
    return sanitize_for_excel(result)


def read_file_lines(path: Path):
    """
    인코딩이 섞여 있어도 최대한 깨지지 않게 읽기
    - 우선 utf-8-sig, 안 되면 cp949, euc-kr 시도
    - 그래도 안 되면 utf-8 + replace
    """
    for enc in ("utf-8-sig", "cp949", "euc-kr"):
        try:
            with path.open(encoding=enc) as f:
                return f.readlines()
        except UnicodeDecodeError:
            continue
    with path.open(encoding="utf-8", errors="replace") as f:
        return f.readlines()


def parse_filename(path: Path):
    m = FILENAME_PATTERN.match(path.name)
    if not m:
        return None
    date_str = m.group("date")
    kind = m.group("kind")
    file_date = datetime.strptime(date_str, "%Y%m%d").date()
    kind_upper = kind.upper()
    if kind_upper == "MAIN":
        kind_upper = "MAIN"
    return file_date, kind_upper


def parse_log_line(line: str):
    line = line.rstrip("\r\n")
    if not line:
        return None
    m = LINE_PATTERN.match(line)
    if not m:
        return None
    time_str = m.group("time")
    msg = m.group("msg")
    return time_str, msg


def to_datetime(file_date: date, time_str: str) -> datetime:
    try:
        t = datetime.strptime(time_str, "%H:%M:%S.%f").time()
    except ValueError:
        t = datetime.strptime(time_str, "%H:%M:%S").time()
    return datetime.combine(file_date, t)


def make_shift_ranges(d: date):
    """
    D-day 기준 주간/야간 범위
    - day : D 08:30:00 ~ D 20:30:00 (미만)
    - night : D 20:30:00 ~ (D+1) 08:30:00 (미만)
    """
    day_start = datetime.combine(d, time(8, 30, 0))
    day_end = datetime.combine(d, time(20, 30, 0))

    night_start = datetime.combine(d, time(20, 30, 0))
    night_end = datetime.combine(d + timedelta(days=1), time(8, 30, 0))

    return {
        "day": (day_start, day_end),
        "night": (night_start, night_end),
    }


def collect_entries():
    """
    group:
      - left / right : 기본 L/R 로그 (sheet2 존재)
      - ld / rd / c  : 추가 로그 (sheet2 없음)
    """
    entries = []

    # ---------- 1) FCT ----------
    for path in FCT_DIR.glob("*.txt"):
        parsed = parse_filename(path)
        if not parsed:
            continue
        file_date, kind = parsed  # FCT1~4
        if not kind.startswith("FCT"):
            continue

        lines = read_file_lines(path)
        for line in lines:
            parsed_line = parse_log_line(line)
            if not parsed_line:
                continue
            time_str, msg = parsed_line
            dt = to_datetime(file_date, time_str)

            source = kind  # FCT1~4
            raw_text = f"[{time_str}]{source} {msg}"

            groups = []
            if source in ("FCT1", "FCT2"):
                groups.append("left")
            if source in ("FCT3", "FCT4"):
                groups.append("right")

            for g in groups:
                entries.append({
                    "datetime": dt,
                    "date": file_date,
                    "group": g,
                    "raw_text": raw_text
                })

    # ---------- 2) VISION ----------
    for path in VISION_DIR.glob("*.txt"):
        parsed = parse_filename(path)
        if not parsed:
            continue
        file_date, kind = parsed  # VISION1/2
        if not kind.startswith("VISION"):
            continue

        lines = read_file_lines(path)
        for line in lines:
            parsed_line = parse_log_line(line)
            if not parsed_line:
                continue
            time_str, msg = parsed_line
            dt = to_datetime(file_date, time_str)

            source = kind  # VISION1/2
            raw_text = f"[{time_str}]{source} {msg}"

            groups = []
            if source == "VISION1":
                groups.append("left")
            if source == "VISION2":
                groups.append("right")

            for g in groups:
                entries.append({
                    "datetime": dt,
                    "date": file_date,
                    "group": g,
                    "raw_text": raw_text
                })

    # ---------- 3) MAIN ----------
    for path in MAIN_DIR.glob("*.txt"):
        parsed = parse_filename(path)
        if not parsed:
            continue
        file_date, kind = parsed
        if kind != "MAIN":
            continue

        lines = read_file_lines(path)
        for line in lines:
            parsed_line = parse_log_line(line)
            if not parsed_line:
                continue
            time_str, msg = parsed_line  # msg: "PLC : ..."
            dt = to_datetime(file_date, time_str)

            u = msg.upper()
            groups = []

            # (1) 기본 left/right (STF, TE, UP, DOWN, ETF 등은 제외)
            # left: FCT1/2, VISION1, FCT FAIL CV1, VISION FAIL CV1
            if ("FCT1" in u or "FCT2" in u or
                "VISION1" in u or
                "FCT FAIL CV1" in u or
                "VISION FAIL CV1" in u):
                groups.append("left")

            # right: FCT3/4, VISION2, FCT FAIL CV2, VISION FAIL CV2
            if ("FCT3" in u or "FCT4" in u or
                "VISION2" in u or
                "FCT FAIL CV2" in u or
                "VISION FAIL CV2" in u):
                groups.append("right")

            # (2) 추가 그룹 ld / rd / c
            # ld: UP1~2, UP-BUFFER1, UP2-1, DOWN3~4
            if ("UP1" in u or "UP2" in u or
                "UP-BUFFER1" in u or "UP2-1" in u or
                "DOWN3" in u or "DOWN4" in u):
                groups.append("ld")

            # rd: DOWN1~2, UP3~5, UP-BUFFER2
            if ("DOWN1" in u or "DOWN2" in u or
                "UP3" in u or "UP4" in u or "UP5" in u or
                "UP-BUFFER2" in u):
                groups.append("rd")

            # c: STF, TE1~3, ETF
            if ("STF" in u or
                "TE1" in u or "TE2" in u or "TE3" in u or
                "ETF" in u):
                groups.append("c")

            if not groups:
                continue

            raw_text = f"[{time_str}] {msg}"

            for g in set(groups):
                entries.append({
                    "datetime": dt,
                    "date": file_date,
                    "group": g,
                    "raw_text": raw_text
                })

    return entries


def build_summary_df(raw_texts, direction: str) -> pd.DataFrame:
    """
    sheet2 요약:
      - 기준: Vision 로그에서 'TEST RESULT :: OK'
      - left  : VISION1 TEST RESULT :: OK 카운트
      - right : VISION2 TEST RESULT :: OK 카운트
    """
    u_texts = [t.upper() for t in raw_texts]

    if direction == "left":
        v1_ok = sum(
            ("VISION1" in u) and
            ("TEST RESULT" in u) and (":: OK" in u)
            for u in u_texts
        )
        return pd.DataFrame({
            "item": ["VISION1 TEST RESULT :: OK", "TOTAL"],
            "count": [v1_ok, v1_ok]
        })

    if direction == "right":
        v2_ok = sum(
            ("VISION2" in u) and
            ("TEST RESULT" in u) and (":: OK" in u)
            for u in u_texts
        )
        return pd.DataFrame({
            "item": ["VISION2 TEST RESULT :: OK", "TOTAL"],
            "count": [v2_ok, v2_ok]
        })

    # 혹시 다른 group이면 비워서 리턴
    return pd.DataFrame({
        "item": [],
        "count": []
    })


def save_main_excel(date_str: str, direction: str, shift: str, raw_texts):
    """
    left/right용 엑셀 저장 (sheet2 포함)
    파일명: YYYYMMDD_day_left_machinelog.xlsx
    """
    excel_texts = [convert_to_excel_text(t) for t in raw_texts]
    df_log = pd.DataFrame({"log": excel_texts})
    df_summary = build_summary_df(raw_texts, direction)

    filename = f"{date_str}_{shift}_{direction}_machinelog.xlsx"
    out_path = OUTPUT_DIR / filename

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_log.to_excel(writer, index=False, sheet_name="log")
        if not df_summary.empty:
            df_summary.to_excel(writer, index=False, sheet_name="summary")

    print(f"[MAIN] 저장 완료: {out_path}")


def save_additional_excel(date_str: str, group: str, shift: str, raw_texts):
    """
    ld / rd / c 추가 엑셀 (sheet2 없음)
    파일명: YYYYMMDD_day_ld_machinelog.xlsx 등
    """
    excel_texts = [convert_to_excel_text(t) for t in raw_texts]
    df_log = pd.DataFrame({"log": excel_texts})

    filename = f"{date_str}_{shift}_{group}_machinelog.xlsx"
    out_path = OUTPUT_DIR / filename

    with pd.ExcelWriter(out_path, engine="openpyxl") as writer:
        df_log.to_excel(writer, index=False, sheet_name="log")

    print(f"[ADD-{group}] 저장 완료: {out_path}")


def main():
    entries = collect_entries()
    if not entries:
        print("읽어올 로그가 없습니다.")
        return

    all_dates = sorted({e["date"] for e in entries})

    for d in all_dates:
        date_str = d.strftime("%Y%m%d")
        shift_ranges = make_shift_ranges(d)

        for shift, (start_dt, end_dt) in shift_ranges.items():
            # 기본 left / right
            for direction in ("left", "right"):
                subset = [
                    e for e in entries
                    if e["group"] == direction
                    and start_dt <= e["datetime"] < end_dt
                ]
                if not subset:
                    continue
                subset.sort(key=lambda x: x["datetime"])
                raw_texts = [e["raw_text"] for e in subset]
                save_main_excel(date_str, direction, shift, raw_texts)

            # 추가 ld / rd / c
            for group in ("ld", "rd", "c"):
                subset = [
                    e for e in entries
                    if e["group"] == group
                    and start_dt <= e["datetime"] < end_dt
                ]
                if not subset:
                    continue
                subset.sort(key=lambda x: x["datetime"])
                raw_texts = [e["raw_text"] for e in subset]
                save_additional_excel(date_str, group, shift, raw_texts)


if __name__ == "__main__":
    main()
