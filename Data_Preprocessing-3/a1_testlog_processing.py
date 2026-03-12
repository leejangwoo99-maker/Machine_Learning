from pathlib import Path
import pandas as pd

# ======================================================
# ① 경로 설정 (여기만 나중에 바꿔서 사용)
# ======================================================
BASE_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")  # 원본 데이터 폴더
SAVE_DIR = Path(r"C:\Users\user\Desktop")  # 엑셀 파일 저장 경로 (바탕화면)
# ======================================================


TC_FOLDERS = ["TC6", "TC7", "TC8", "TC9"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

##########################################################
# 1) TXT 파일 목록 수집 단계 (조각 검증 가능)
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
        # 4) 폴더명과 비교
        if ymd != date_folder:
            valid = False
            reasons.append(f"파일명 날짜({ymd})와 폴더명({date_folder}) 불일치")

    return valid, reasons


##########################################################
# 3) 전체 파일에 적용
##########################################################
def run_validation(df_raw):
    results = []

    for idx, row in df_raw.iterrows():
        file_path = row["path"]
        date_folder = row["date_folder"]

        valid, reasons = validate_filename(file_path, date_folder)

        results.append({
            "tc": row["tc"],
            "date_folder": date_folder,
            "path": str(file_path),
            "filename": file_path.name,
            "valid": valid,
            "reason": ", ".join(reasons) if len(reasons) > 0 else ""
        })

    return pd.DataFrame(results)


##########################################################
# 4) 엑셀 저장
##########################################################
def save_bad_files_excel(bad_df, save_dir):
    save_path = save_dir / "불량파일_리스트.xlsx"
    bad_df.to_excel(save_path, index=False)
    print(f"\n✔ 엑셀 저장 완료 → {save_path}\n")


##########################################################
# ★ 실행 파트 (Jupyter / Python 모두 가능)
##########################################################
if __name__ == "__main__":

    print("1) TXT 파일 스캔 중...\n")
    raw_df = collect_raw_files(BASE_DIR)

    print(f"   → 총 파일 수집: {len(raw_df)}개")

    print("\n2) 파일명 검증 수행...")
    df = run_validation(raw_df)

    bad_df = df[df["valid"] == False]
    good_df = df[df["valid"] == True]

    print("\n=== 결과 요약 ===")
    print("전체 파일 개수 :", len(df))
    print("정상 파일 개수 :", len(good_df))
    print("불량 파일 개수:", len(bad_df))

    print("\n=== 불량 사유 집계 ===")
    print(bad_df["reason"].value_counts())

    print("\n3) 불량 파일 엑셀 저장 중...")
    save_bad_files_excel(bad_df, SAVE_DIR)

    print("완료!")
