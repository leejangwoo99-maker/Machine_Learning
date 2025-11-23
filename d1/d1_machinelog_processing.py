from pathlib import Path
import re
from collections import defaultdict

import pandas as pd


# ================== 설정 ==================
# 상위 경로
ROOT_DIR = Path(r"C:\Users\user\Desktop\machinlog")

# 엑셀 저장 경로 (바탕화면\confirm)
OUTPUT_DIR = Path(r"C:\Users\user\Desktop\confirm")

# 인코딩 설정
ENC_FCT_PDI = "cp949"   # FCT / PDI / Main : ANSI 가정 (cp949)
ENC_MAIN = "cp949"
ENC_VISION = "utf-8"

# 파일명 패턴 (yyyymmdd_FCT1~4 / PDI1~4 / Main / Vision1~2_Machine_Log.txt)
LOG_FILENAME_PATTERN = re.compile(
    r"(?P<date>\d{8})_(?P<tag>FCT[1-4]|PDI[1-4]|Main|Vision[1-2])_Machine_Log\.txt$",
    re.IGNORECASE,
)

# 특수문자 치환 패턴 (한글/영문/숫자/공백/일부 기호만 허용, 나머지는 '_' 처리)
SPECIAL_CHAR_PATTERN = re.compile(r"[^0-9A-Za-z가-힣\s\[\]\.:/\-_]")


# ================== 유틸 함수 ==================
def sanitize_message(msg: str) -> str:
    """특수문자를 '_'로 치환 (한글/영문/숫자/공백/일부 기호만 허용)"""
    return SPECIAL_CHAR_PATTERN.sub("_", msg)


def read_log_file(file_path: Path, encoding: str, source: str, date_str: str) -> pd.DataFrame:
    """
    로그 파일 한 개를 읽어 DataFrame 으로 반환.
    - [hh:mi:ss.ss] 없는 행은 직전 시간 따라감
    - 특수문자 '_' 로 치환
    """
    records = []
    current_time = None

    with file_path.open("r", encoding=encoding, errors="replace") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n").rstrip("\r")

            # [hh:mi:ss.ss] 패턴 파싱
            m = re.match(r"\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)", line)
            if m:
                current_time = m.group(1)
                message = m.group(2)
            else:
                # 시간 정보가 없으면 이전 시간 유지
                message = line

            if current_time is None:
                # 파일 맨 앞에 시간 없는 이상 케이스는 그냥 스킵
                continue

            message_clean = sanitize_message(message)

            records.append(
                {
                    "date": date_str,
                    "time": current_time,
                    "source": source,
                    "message": message_clean,
                    "file_path": str(file_path),
                }
            )

    if not records:
        return pd.DataFrame(columns=["date", "time", "source", "message", "file_path"])

    df = pd.DataFrame(records)
    # 시간 정렬용 컬럼
    df["time_sort"] = pd.to_timedelta(df["time"])
    return df


def collect_all_files(root_dir: Path):
    # """
    # ROOT_DIR 아래 FCT / Main / Vision 폴더의 yyyy\mm\ 파일들을 스캔하여
    # 날짜별 / 종류별 경로 인덱스를 만든다.
    # """
    index = defaultdict(lambda: {"FCT": {}, "PDI": {}, "Main": None, "Vision": {}})

    total_files = 0
    total_fct = 0
    total_pdi = 0
    total_main = 0
    total_vision = 0

    for mid in ["FCT", "Main", "Vision"]:
        mid_path = root_dir / mid
        if not mid_path.exists():
            print(f"[SKIP] 중간 폴더 없음: {mid_path}")
            continue

        # yyyy\mm 아래 모든 파일 검색
        for year_dir in mid_path.iterdir():
            if not year_dir.is_dir():
                continue
            for month_dir in year_dir.iterdir():
                if not month_dir.is_dir():
                    continue
                for file_path in month_dir.iterdir():
                    if not file_path.is_file():
                        continue

                    m = LOG_FILENAME_PATTERN.search(file_path.name)
                    if not m:
                        continue

                    date_str = m.group("date")
                    tag = m.group("tag")

                    total_files += 1
                    t_lower = tag.lower()

                    if t_lower.startswith("fct"):
                        idx = int(tag[-1])
                        index[date_str]["FCT"][idx] = file_path
                        total_fct += 1
                    elif t_lower.startswith("pdi"):
                        idx = int(tag[-1])
                        index[date_str]["PDI"][idx] = file_path
                        total_pdi += 1
                    elif t_lower == "main":
                        index[date_str]["Main"] = file_path
                        total_main += 1
                    elif t_lower.startswith("vision"):
                        idx = int(tag[-1])
                        index[date_str]["Vision"][idx] = file_path
                        total_vision += 1

    print("\n====== 파일 스캔 결과 ======")
    print(f"총 파일 수          : {total_files}")
    print(f"  FCT 파일 수       : {total_fct}")
    print(f"  PDI 파일 수       : {total_pdi}")
    print(f"  Main 파일 수      : {total_main}")
    print(f"  Vision 파일 수    : {total_vision}")
    print("==========================\n")

    return index


# ---------- Error 컬럼 생성 ----------
def add_error_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Error 컬럼 추가
    규칙:
      1) '[BARCODE: ]' → '결측치'
      2) 메시지에 '_숫자' → 'Tray_숫자로 수정'
      3) '바코드 ON' 포함 → 'Barcode 삭제'
      4) [BARCODE: 내용] 이 들어간 메시지가 동일 내용으로 여러 번 나오면 → 'Barcode 반복'
      5) 여러 에러가 겹쳐도 중복 없이 '에러1; 에러2' 형식으로 표기
    """
    if df.empty:
        df["Error"] = ""
        return df

    df = df.copy()

    messages = df["message"].fillna("")
    n = len(df)
    errors_per_row = [[] for _ in range(n)]

    barcode_empty_pattern = re.compile(r"\[BARCODE:\s*\]")
    tray_pattern = re.compile(r"_(\d+)")
    barcode_with_value_pattern = re.compile(r"\[BARCODE:\s*([^\]]+)\]")

    # 1, 2, 3번 규칙
    for i, msg in enumerate(messages):
        # 1) BARCODE 결측치
        if barcode_empty_pattern.search(msg):
            errors_per_row[i].append("결측치")

        # 2) _숫자 → Tray_x로 수정
        m_tray = tray_pattern.search(msg)
        if m_tray:
            tray_no = m_tray.group(1)
            errors_per_row[i].append(f"Tray_{tray_no}로 수정")

        # 3) '바코드 ON' 포함
        if "바코드 ON" in msg:
            errors_per_row[i].append("Barcode 삭제")

    # 4) 동일 내용 반복(Barcode 포함 메시지만)
    mask_barcode_val = messages.str.contains(r"\[BARCODE:\s*[^\]]+\]", regex=True)
    # BARCODE 값이 있는 메시지만 대상으로 value_counts
    barcode_msgs = messages[mask_barcode_val]
    dup_counts = barcode_msgs.value_counts()
    duplicated_msgs = set(dup_counts[dup_counts > 1].index)

    if duplicated_msgs:
        for i, msg in enumerate(messages):
            if msg in duplicated_msgs:
                errors_per_row[i].append("Barcode 반복")

    # 중복 제거 후 문자열로 합치기
    error_strings = []
    for err_list in errors_per_row:
        if not err_list:
            error_strings.append("")
        else:
            seen = set()
            unique_ordered = []
            for e in err_list:
                if e not in seen:
                    seen.add(e)
                    unique_ordered.append(e)
            error_strings.append("; ".join(unique_ordered))

    df["Error"] = error_strings
    return df


def build_daily_datasets(date_str: str, info: dict):
    """
    한 날짜(date_str)에 대해
    - FCT1~4 + PDI1~4 병합 (PDI 소스는 FCT로 표시)
    - Main / Vision1 / Vision2 내용 분리
    - 두 개의 DataFrame 반환
      A: Main_FCT1,2_Vision1_FVI1
      B: Main_FCT3,4_Vision2_FVI2
    """
    # ===== Main =====
    main_path = info.get("Main")
    main_df = pd.DataFrame(columns=["date", "time", "source", "message", "file_path"])
    if main_path is not None:
        main_df = read_log_file(main_path, ENC_MAIN, "Main", date_str)

    # A용 Main 키워드 / B용 Main 키워드
    keywords_a_main = [
        "UP1",
        "UP-BUFFER1",
        "UP2",
        "UP2-1",
        "FCT1",
        "FCT2",
        "VISION1",
        "FVI1",
        "FCT FAIL CV 1",
        "VISION FAIL CV1",
    ]

    keywords_b_main = [
        "UP3",
        "UP4",
        "UP-BUFFER2",
        "UP5",
        "FCT3",
        "FCT4",
        "VISION2",
        "FVI2",
        "FCT FAIL CV 2",
        "VISION FAIL CV2",
    ]

    def filter_by_keywords(df, keywords):
        if df.empty:
            return df
        pattern = "|".join(re.escape(k) for k in keywords)
        return df[df["message"].str.contains(pattern, na=False)]

    main_a = filter_by_keywords(main_df, keywords_a_main)
    main_b = filter_by_keywords(main_df, keywords_b_main)

    # ===== FCT + PDI 병합 =====
    fct_merged = {}  # idx: DataFrame
    for idx in range(1, 5):
        df_list = []

        fct_path = info["FCT"].get(idx)
        if fct_path is not None:
            df_list.append(
                read_log_file(fct_path, ENC_FCT_PDI, f"FCT{idx}", date_str)
            )

        pdi_path = info["PDI"].get(idx)
        if pdi_path is not None:
            # ★ PDI도 source를 FCT로 맞춰서 표시 ★
            df_list.append(
                read_log_file(pdi_path, ENC_FCT_PDI, f"FCT{idx}", date_str)
            )

        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            df.sort_values("time_sort", inplace=True)
            fct_merged[idx] = df
        else:
            fct_merged[idx] = pd.DataFrame(
                columns=["date", "time", "source", "message", "file_path", "time_sort"]
            )

    # A: FCT1,2 / B: FCT3,4
    fct_a_list = [fct_merged[idx] for idx in (1, 2) if not fct_merged[idx].empty]
    fct_b_list = [fct_merged[idx] for idx in (3, 4) if not fct_merged[idx].empty]

    fct_a = (
        pd.concat(fct_a_list, ignore_index=True)
        if fct_a_list
        else pd.DataFrame(columns=main_df.columns)
    )
    fct_b = (
        pd.concat(fct_b_list, ignore_index=True)
        if fct_b_list
        else pd.DataFrame(columns=main_df.columns)
    )

    # ===== Vision =====
    vision1_path = info["Vision"].get(1)
    vision2_path = info["Vision"].get(2)

    vision1_df = pd.DataFrame(
        columns=["date", "time", "source", "message", "file_path", "time_sort"]
    )
    vision2_df = pd.DataFrame(
        columns=["date", "time", "source", "message", "file_path", "time_sort"]
    )

    if vision1_path is not None:
        vision1_df = read_log_file(vision1_path, ENC_VISION, "Vision1", date_str)
    if vision2_path is not None:
        vision2_df = read_log_file(vision2_path, ENC_VISION, "Vision2", date_str)

    # ===== A/B 데이터셋 구성 =====
    df_a_list = [main_a, fct_a, vision1_df]
    df_b_list = [main_b, fct_b, vision2_df]

    df_a_list = [d for d in df_a_list if not d.empty]
    df_b_list = [d for d in df_b_list if not d.empty]

    if df_a_list:
        df_a = pd.concat(df_a_list, ignore_index=True)
        df_a.sort_values("time_sort", inplace=True)
    else:
        df_a = pd.DataFrame(
            columns=["date", "time", "source", "message", "file_path", "time_sort"]
        )

    if df_b_list:
        df_b = pd.concat(df_b_list, ignore_index=True)
        df_b.sort_values("time_sort", inplace=True)
    else:
        df_b = pd.DataFrame(
            columns=["date", "time", "source", "message", "file_path", "time_sort"]
        )

    # time_sort 제거
    if "time_sort" in df_a.columns:
        df_a = df_a.drop(columns=["time_sort"])
    if "time_sort" in df_b.columns:
        df_b = df_b.drop(columns=["time_sort"])

    # Error 컬럼 생성
    df_a = add_error_column(df_a)
    df_b = add_error_column(df_b)

    # file_path 제거
    if "file_path" in df_a.columns:
        df_a = df_a.drop(columns=["file_path"])
    if "file_path" in df_b.columns:
        df_b = df_b.drop(columns=["file_path"])

    # 🔥 여기서 컬럼명 변경
    df_a = df_a.rename(columns={"Error": "Error or Requirement"})
    df_b = df_b.rename(columns={"Error": "Error or Requirement"})

    return df_a, df_b

def save_daily_excels(date_str: str, df_a: pd.DataFrame, df_b: pd.DataFrame):
    """
    날짜별로 A/B DataFrame을 엑셀로 저장.
    파일명:
      - {yyyymmdd}_Main_FCT1,2_Vision1_FVI1_machinelog.xlsx
      - {yyyymmdd}_Main_FCT3,4_Vision2_FVI2_machinelog.xlsx
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if df_a is not None and not df_a.empty:
        filename_a = f"{date_str}_Main_FCT1,2_Vision1_FVI1_machinelog.xlsx"
        out_a = OUTPUT_DIR / filename_a
        df_a.to_excel(out_a, index=False)
        print(f"[저장] A (Main_FCT1,2_Vision1_FVI1): {out_a}")

    if df_b is not None and not df_b.empty:
        filename_b = f"{date_str}_Main_FCT3,4_Vision2_FVI2_machinelog.xlsx"
        out_b = OUTPUT_DIR / filename_b
        df_b.to_excel(out_b, index=False)
        print(f"[저장] B (Main_FCT3,4_Vision2_FVI2): {out_b}")


def main():
    # 1) 파일 스캔 및 인덱스 생성
    file_index = collect_all_files(ROOT_DIR)

    if not file_index:
        print("처리할 파일이 없습니다.")
        return

    # 2) 날짜별 처리
    for date_str in sorted(file_index.keys()):
        info = file_index[date_str]
        print(f"\n===== 날짜 {date_str} 처리 중... =====")

        df_a, df_b = build_daily_datasets(date_str, info)
        save_daily_excels(date_str, df_a, df_b)

    print("\n모든 날짜 처리 완료!")


if __name__ == "__main__":
    main()
