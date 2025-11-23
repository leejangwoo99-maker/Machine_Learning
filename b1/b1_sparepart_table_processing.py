# """
# sparepart 텍스트 → RDBMS 업로드용 DataFrame 변환 (멀티프로세싱 + 이력 + CSV 저장)
# ---------------------------------------------------------------------------
# 기능:
#   1) C:\Users\user\Desktop\sparepart\ 아래의
#      'YY.MM.DD_주간.txt' / 'YY.MM.DD_야간.txt' 파일들을 병렬로 처리해서
#
#      컬럼:
#        date (YYYY-MM-DD)
#        shift ("주간"/"야간")
#        fct1_mini_b ~ fct4_power
#
#      형태의 pandas DataFrame 생성
#
#   2) 이미 처리한 파일 경로를 이력 CSV로 관리해서, 다음 실행 시 중복 처리 방지
#      - 이력 파일: C:\Users\user\Desktop\b1_sparepart_history.csv
#
#   3) 최종 DataFrame을 CSV로 저장
#      - 결과 파일: C:\Users\user\Desktop\b1_sparepart_usage.csv
# """

from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import List, Dict, Optional, Set
import pandas as pd
import csv

# ====================================================
# 1. 기본 설정 (필요 시 여기만 수정)
# ====================================================

# sparepart 텍스트 파일들이 있는 상위 폴더
BASE_DIR = Path(r"C:\Users\user\Desktop\sparepart")

# 멀티프로세싱 워커 수 (코어 수에 맞게 조절, 0 또는 None이면 자동 결정)
MAX_WORKERS = 4

# 🔹 이력 CSV 파일 경로 (이미 처리한 파일 경로 기록)
HISTORY_CSV_PATH = Path(r"C:\Users\user\Desktop\b1_sparepart_history.csv")

# 🔹 결과 DataFrame CSV 저장 경로
OUTPUT_CSV_PATH = Path(r"C:\Users\user\Desktop\b1_sparepart_usage.csv")

# FCT별 부품 컬럼(순서 고정) – 11~26번째 줄에 해당
SPAREPART_KEYS = [
    "fct1_mini_b",
    "fct1_usb_c",
    "fct1_usb_a",
    "fct1_power",
    "fct2_mini_b",
    "fct2_usb_c",
    "fct2_usb_a",
    "fct2_power",
    "fct3_mini_b",
    "fct3_usb_c",
    "fct3_usb_a",
    "fct3_power",
    "fct4_mini_b",
    "fct4_usb_c",
    "fct4_usb_a",
    "fct4_power",
]

# 사람이 보기 쉬운 라벨(파일 내용의 앞부분과 매칭용, 선택 사항)
SPAREPART_LABELS = [
    "FCT1 Mini B",
    "FCT1 USB-C",
    "FCT1 USB-A",
    "FCT1 Power",
    "FCT2 Mini B",
    "FCT2 USB-C",
    "FCT2 USB-A",
    "FCT2 Power",
    "FCT3 Mini B",
    "FCT3 USB-C",
    "FCT3 USB-A",
    "FCT3 Power",
    "FCT4 Mini B",
    "FCT4 USB-C",
    "FCT4 USB-A",
    "FCT4 Power",
]


# ====================================================
# 2. 이력 로드 / 저장 함수
# ====================================================
def load_history(history_path: Path) -> Set[str]:
    """이력 CSV에서 file_path 컬럼을 읽어와 set으로 반환."""
    if not history_path.exists():
        return set()

    try:
        df_hist = pd.read_csv(history_path, dtype=str)
        if "file_path" not in df_hist.columns:
            return set()
        return set(df_hist["file_path"].dropna().astype(str).tolist())
    except Exception as e:
        print(f"[WARN] 이력 파일 로드 중 오류, 이력 무시: {e}")
        return set()


def append_history(history_path: Path, new_paths: List[str]) -> None:
    """새로 처리한 파일 경로들을 이력 CSV에 append."""
    if not new_paths:
        return

    file_exists = history_path.exists()
    history_path.parent.mkdir(parents=True, exist_ok=True)

    with open(history_path, "a", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["file_path"])
        for p in new_paths:
            writer.writerow([p])


# ====================================================
# 3. 파일명에서 날짜/주야간 추출 함수
#    파일명 예) 25.11.20_주간.txt / 25.11.20_야간.txt
# ====================================================
def parse_filename(path: Path):
    """
    파일명에서 날짜(YYYY-MM-DD)와 주/야간(주간/야간)을 추출.

    반환:
        date_str: "YYYY-MM-DD" 형식 문자열
        shift: "주간" 또는 "야간"
    """
    stem = path.stem  # 예: "25.11.20_주간"
    try:
        date_part, shift = stem.split("_", 1)
    except ValueError:
        raise ValueError(f"[파일명 오류] 'YY.MM.DD_주간/야간.txt' 형식이 아님: {path.name}")

    # YY.MM.DD 파싱
    try:
        yy, mm, dd = date_part.split(".")
        year = 2000 + int(yy)  # 25 → 2025 로 가정
        month = int(mm)
        day = int(dd)
    except Exception as e:
        raise ValueError(f"[날짜 파싱 오류] {path.name}: {e}")

    date_str = f"{year:04d}-{month:02d}-{day:02d}"  # 예: 2025-11-20
    return date_str, shift


# ====================================================
# 4. 한 개 sparepart 텍스트 파일 파싱
# ====================================================
def parse_sparepart_file(path_str: str) -> Optional[Dict]:
    """
    텍스트 파일(YY.MM.DD_주간/야간.txt) 하나를 읽어서
    딕셔너리 한 행(row)으로 변환.

    컬럼:
      - date  : "YYYY-MM-DD"
      - shift : "주간" 또는 "야간"
      - fct1_mini_b ~ fct4_power : int (소모량)

    에러 발생 시 None 반환.
    """
    path = Path(path_str)

    try:
        # 파일명에서 날짜와 주/야간 추출
        date_str, shift = parse_filename(path)

        # 파일 읽기 (cp949 → utf-8-sig 순으로 시도)
        try:
            text = path.read_text(encoding="cp949")
        except UnicodeDecodeError:
            text = path.read_text(encoding="utf-8-sig")

        lines = text.splitlines()

        # 최소 26줄 이상인지 확인 (11~26 번째 줄에 데이터)
        if len(lines) < 26:
            print(f"[라인 부족] {path.name}: 총 {len(lines)}줄")
            return None

        # 11번째~26번째 줄 → 0-base index 10~25
        data_lines = [line.strip() for line in lines[10:26]]

        # 결과 딕셔너리
        row = {
            "date": date_str,
            "shift": shift,
        }

        # 각 라인에서 숫자 부분 파싱
        for i, key in enumerate(SPAREPART_KEYS):
            line = data_lines[i]
            # "FCT1 Mini B: 011" → ":" 기준으로 split
            if ":" in line:
                label_part, value_part = line.split(":", 1)
                label_part = label_part.strip()
                value_str = value_part.strip()
            else:
                # 콜론이 없으면 전체를 값으로 보고 처리
                label_part = ""
                value_str = line.strip()

            # (옵션) 라벨 체크 – 순서가 깨진 경우 경고 출력만 하고 진행
            expected_label = SPAREPART_LABELS[i]
            if expected_label not in label_part:
                print(
                    f"[라벨 경고] {path.name}의 {i+1}번째 sparepart 줄 "
                    f"라벨 불일치: 기대='{expected_label}', 실제='{label_part}'"
                )

            # 숫자 변환 (앞에 0이 붙어 있어도 int로 변환)
            try:
                value = int(value_str)
            except ValueError:
                print(
                    f"[값 경고] {path.name}의 {expected_label} 값이 정수가 아님: "
                    f"'{value_str}', 0으로 처리"
                )
                value = 0

            row[key] = value

        return row

    except Exception as e:
        print(f"[ERROR] {path.name} 처리 중 오류: {e}")
        return None


# ====================================================
# 5. 전체 폴더 스캔 후 DataFrame 생성 (멀티프로세싱 + 이력)
# ====================================================
def build_sparepart_df_parallel(base_dir: Path,
                                history_path: Path,
                                max_workers: Optional[int] = None) -> pd.DataFrame:
    """
    base_dir 아래의 'YY.MM.DD_주간.txt', 'YY.MM.DD_야간.txt' 파일을 모두 찾아
    (이미 처리한 파일은 이력으로 스킵) 멀티프로세싱으로 병렬 처리 후
    pandas DataFrame으로 반환.
    """
    # txt 파일 전체 스캔 (하위 폴더까지 포함하고 싶으면 rglob("*.txt") 사용)
    txt_files: List[Path] = sorted(base_dir.glob("*.txt"))

    if not txt_files:
        print(f"[INFO] '{base_dir}' 아래에 .txt 파일이 없습니다.")
        return pd.DataFrame()

    # 이력 로드 (이미 처리한 파일 경로 set)
    history_set = load_history(history_path)
    if history_set:
        print(f"[INFO] 이력에 기록된 파일 수: {len(history_set)}개")
    else:
        print("[INFO] 이력 파일이 없거나 비어 있음 → 모든 파일 신규 처리")

    # 이력에 없는 파일만 대상
    targets: List[Path] = [
        p for p in txt_files
        if str(p.resolve()) not in history_set
    ]

    if not targets:
        print("[INFO] 새로 처리할 파일이 없습니다. (모두 이력에 존재)")
        return pd.DataFrame()

    print(f"[INFO] 발견된 텍스트 파일 수: {len(txt_files)}개")
    print(f"[INFO] 이력 제외 후 처리 대상 파일 수: {len(targets)}개")

    rows: List[Dict] = []
    newly_processed_paths: List[str] = []

    # 프로세스 풀 생성
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 파일 경로 문자열을 넘겨야 pickling 안전
        future_to_path = {
            executor.submit(parse_sparepart_file, str(path)): path
            for path in targets
        }

        total = len(future_to_path)
        for i, future in enumerate(as_completed(future_to_path), start=1):
            path = future_to_path[future]
            try:
                result = future.result()
                if result is not None:
                    rows.append(result)
                    newly_processed_paths.append(str(path.resolve()))
                    print(f"  - ({i}/{total}) 처리 완료: {path.name}")
                else:
                    print(f"  - ({i}/{total}) 처리 실패/무시: {path.name}")
            except Exception as e:
                print(f"[ERROR] {path.name} 처리 중 예외 발생: {e}")

    if not rows:
        print("[WARN] 유효한 신규 데이터가 없습니다.")
        return pd.DataFrame()

    # 신규 처리 파일 이력을 CSV에 append
    append_history(history_path, newly_processed_paths)
    print(f"[INFO] 이력 파일 업데이트 완료: {history_path}")

    df = pd.DataFrame(rows)

    # 컬럼 순서 정리: date, shift, FCT1~4 Mini B~Power
    ordered_cols = ["date", "shift"] + SPAREPART_KEYS
    df = df[ordered_cols]

    return df


# ====================================================
# 6. 메인 실행부
# ====================================================
def main():
    df = build_sparepart_df_parallel(
        BASE_DIR,
        HISTORY_CSV_PATH,
        max_workers=MAX_WORKERS
    )

    print("\n[DataFrame 미리보기 (상위 5행)]")
    if df.empty:
        print(" → DataFrame 이 비어 있습니다.")
        return

    print(df.head())
    print("\n[행/열 수]")
    print(df.shape)

    # 결과 DataFrame을 CSV로 저장
    try:
        OUTPUT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(OUTPUT_CSV_PATH, index=False, encoding="utf-8-sig")
        print(f"\n[완료] DataFrame을 CSV로 저장했습니다.")
        print(f"       → {OUTPUT_CSV_PATH}")
    except Exception as e:
        print(f"[ERROR] CSV 저장 중 오류: {e}")

    # 여기서 바로 RDBMS로 업로드도 가능 (예: PostgreSQL)
    # from sqlalchemy import create_engine
    # engine = create_engine("postgresql+psycopg2://user:password@host:port/dbname")
    # df.to_sql("b1_sparepart_usage", engine, if_exists="append", index=False)


if __name__ == "__main__":
    # 윈도우 환경에서 멀티프로세싱 사용 시 필수
    main()
