from pathlib import Path
import pandas as pd

# ======================================================
# ① 경로 설정 (여기만 나중에 바꿔서 사용)
# ======================================================
BASE_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")  # 원본 데이터 폴더
SAVE_DIR = Path(r"C:\Users\user\Desktop")          # 엑셀 파일 저장 경로 (바탕화면)
# ======================================================

# Vision3 포함
TC_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision3"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]


##########################################################
# 0) 파일명에서 시간(HHMISS) 추출 함수
##########################################################
def extract_time_from_filename(stem: str):
    """
    파일명 길이에 따라
    51/53자리  → 32~45자리 = YYYYMMDDHHMISS
    52/54자리  → 33~46자리 = YYYYMMDDHHMISS
    에서 HHMISS(마지막 6자리)만 뽑아서 반환
    """
    length = len(stem)

    if length in (51, 53):
        if len(stem) < 45:
            return None
        dt14 = stem[31:45]  # 0-based index (32~45, 총 14글자)
    elif length in (52, 54):
        if len(stem) < 46:
            return None
        dt14 = stem[32:46]  # 33~46
    else:
        return None

    if len(dt14) != 14 or not dt14.isdigit():
        return None

    # 뒤 6자리 HHMISS
    return dt14[8:14]


##########################################################
# 1) TXT 파일 목록 수집 단계
##########################################################
def collect_raw_files(base_dir):
    raw_files = []

    for tc in TC_FOLDERS:
        tc_path = base_dir / tc
        if not tc_path.exists():
            print(f"[SKIP] {tc_path} 존재하지 않음")
            continue

        # YYYYMMDD 폴더 찾기
        for date_dir in tc_path.iterdir():
            if not (date_dir.is_dir() and date_dir.name.isdigit() and len(date_dir.name) == 8):
                continue

            date_folder = date_dir.name  # '20250804'

            for folder_type in TARGET_FOLDERS:
                type_path = date_dir / folder_type
                if not type_path.exists():
                    continue

                # txt 파일 수집
                for txt in type_path.glob("*.txt"):
                    raw_files.append({
                        "tc": tc,
                        "date_folder": date_folder,
                        "path": txt
                    })

    return pd.DataFrame(raw_files)


##########################################################
# 2) 파일명 유효성 검사 함수
##########################################################
def validate_filename(file_path, date_folder):
    name = file_path.stem
    length = len(name)

    valid = True
    reasons = []

    # 1) 길이 체크
    if not (51 <= length <= 54):
        return False, [f"파일명 길이 오류({length})"]

    # 2) 51/53 → 32~39 자리 = YYYYMMDD
    if length in (51, 53):
        ymd = name[31:39]  # index 31~38
    else:
        # 52/54 → 33~40 자리
        ymd = name[32:40]  # index 32~39

    # 형식 체크
    if not (len(ymd) == 8 and ymd.isdigit()):
        valid = False
        reasons.append(f"파일명 날짜 형식 오류({ymd})")
    else:
        # 폴더명과 비교
        if ymd != date_folder:
            valid = False
            reasons.append(f"파일명 날짜({ymd})와 폴더명({date_folder}) 불일치")

    return valid, reasons


##########################################################
# 3) Vision3 전용 내용 검사 (6번째 행 Test Program LED1/LED2)
##########################################################
def validate_vision3_content(file_path):
    """
    Vision3 파일에 한해,
    6번째 행(0-based index 5)이
    'Test Program            : LED1' 또는
    'Test Program            : LED2'
    가 아니면 FAIL 처리

    ★ 성능: 6줄까지만 readline()으로 읽음
    """
    allowed_stripped = {
        "Test Program            : LED1".strip(),
        "Test Program            : LED2".strip(),
    }

    try:
        try:
            f = open(file_path, "r", encoding="cp949", errors="ignore")
        except UnicodeDecodeError:
            f = open(file_path, "r", encoding="utf-8", errors="ignore")

        with f:
            line6 = None
            for i in range(6):
                line = f.readline()
                if not line:
                    return False, "Vision3: 파일 줄 수 부족(6번째 행 없음)"
                if i == 5:
                    line6 = line

        line6_stripped = line6.strip()
        if line6_stripped not in allowed_stripped:
            return False, f"Vision3: 6번째 행 Test Program LED1/LED2 아님 (실제='{line6_stripped}')"

        return True, ""
    except Exception as e:
        return False, f"Vision3: 내용 검사 중 오류({e})"


##########################################################
# 4) 전체 파일에 적용
##########################################################
def run_validation(df_raw):
    results = []

    total = len(df_raw)
    print(f"   → 검증 대상 파일 수: {total}개")

    for idx, row in enumerate(df_raw.itertuples(index=False), start=1):
        file_path = row.path
        date_folder = row.date_folder
        tc = row.tc

        valid, reasons = validate_filename(file_path, date_folder)

        # Vision3 전용 추가 검사
        if tc == "Vision3":
            v3_ok, v3_reason = validate_vision3_content(file_path)
            if not v3_ok:
                valid = False
                if v3_reason:
                    reasons.append(v3_reason)

        # 파일명에서 시간(HHMISS) 추출
        time_str = extract_time_from_filename(file_path.stem)

        results.append({
            "tc": tc,
            "date_folder": date_folder,
            "path": str(file_path),
            "filename": file_path.name,
            "valid": valid,
            "time_str": time_str,  # HHMISS (예: '083000')
            "reason": ", ".join(reasons) if len(reasons) > 0 else ""
        })

        if idx % 10000 == 0:
            print(f"   → 현재 {idx}/{total} 파일 검사 완료")

    return pd.DataFrame(results)


##########################################################
# 5) 엑셀 저장 (불량 + 정상파일 날짜별 주/야간 집계)
##########################################################
def save_results_excel(bad_df, good_df, save_dir):
    """
    Sheet1: 불량 파일 리스트
    Sheet2: 정상 파일 중 날짜(YYYYMMDD)별 주간/야간 개수 집계
    """
    # ---- 정상 파일 시간 파싱 ----
    good_df = good_df.copy()
    good_df["time_int"] = pd.to_numeric(good_df["time_str"], errors="coerce")

    # time_int 있는 것만 집계 대상으로 사용
    valid_time_df = good_df[good_df["time_int"].notna()].copy()

    # 주간: 08:30:00.00 ~ 20:29:59.99  → 083000 ~ 202959
    day_mask = (valid_time_df["time_int"] >= 83000) & (valid_time_df["time_int"] <= 202959)
    night_mask = ~day_mask  # 나머지(20:30~08:29)

    # shift 컬럼 생성
    valid_time_df.loc[day_mask, "shift"] = "주간"
    valid_time_df.loc[night_mask, "shift"] = "야간"

    # 날짜별 + 주/야간 별 개수 집계
    grouped = (
        valid_time_df
        .groupby(["date_folder", "shift"])
        .size()
        .reset_index(name="정상_파일_개수")
    )

    # 피벗: 날짜 행, 주간/야간 컬럼
    summary_pivot = grouped.pivot(index="date_folder", columns="shift", values="정상_파일_개수").fillna(0)

    # 주간/야간 컬럼 없을 경우 대비
    for col in ["주간", "야간"]:
        if col not in summary_pivot.columns:
            summary_pivot[col] = 0

    summary_pivot = summary_pivot[["주간", "야간"]].astype(int)

    # 보기 좋게 컬럼명 변경
    summary_pivot = summary_pivot.reset_index()
    summary_pivot = summary_pivot.rename(columns={
        "date_folder": "날짜(YYYYMMDD)",
        "주간": "주간_정상_파일_개수",
        "야간": "야간_정상_파일_개수",
    })

    save_path = save_dir / "불량파일_리스트.xlsx"

    with pd.ExcelWriter(save_path, engine="openpyxl") as writer:
        # Sheet1: 불량 파일 리스트
        bad_df.to_excel(writer, sheet_name="불량파일", index=False)
        # Sheet2: 날짜별 정상 파일 주/야간 집계
        summary_pivot.to_excel(writer, sheet_name="정상파일_시간대집계", index=False)

    print(f"\n✔ 엑셀 저장 완료 → {save_path}\n")


##########################################################
# ★ 실행 파트 (Jupyter / Python 모두 가능)
##########################################################
if __name__ == "__main__":

    print("1) TXT 파일 스캔 중...\n")
    raw_df = collect_raw_files(BASE_DIR)

    print(f"   → 총 파일 수집: {len(raw_df)}개")

    print("\n2) 파일명 + Vision3 내용 검증 수행...")
    df = run_validation(raw_df)

    bad_df = df[df["valid"] == False]
    good_df = df[df["valid"] == True]

    print("\n=== 결과 요약 ===")
    print("전체 파일 개수 :", len(df))
    print("정상 파일 개수 :", len(good_df))
    print("불량 파일 개수:", len(bad_df))

    print("\n=== 불량 사유 집계 ===")
    if len(bad_df) > 0:
        print(bad_df["reason"].value_counts())
    else:
        print("불량 파일 없음")

    print("\n3) 엑셀 저장 (불량 리스트 + 날짜별 정상파일 주/야간 개수)...")
    save_results_excel(bad_df, good_df, SAVE_DIR)

    print("완료!")
