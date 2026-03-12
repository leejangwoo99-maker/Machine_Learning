import os
import json
import re
import multiprocessing as mp

# ===== 1. 기본 경로 설정 =====
INPUT_ROOT = r"C:\Users\user\Desktop\FORD A+C_FCT LOG"
OUTPUT_ROOT = r"C:\Users\user\Desktop\1"
TC_LIST = ["TC6", "TC7", "TC8", "TC9"]


# ===== 2. 파일 읽기 =====
def read_file_text(path):
    """파일을 최대한 안정적으로 텍스트로 읽기."""
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            return f.read()
    except:
        with open(path, "r", encoding="cp949", errors="ignore") as f:
            return f.read()


# ===== 3. 헤더/추가 정보 파서 =====
HEADER_PATTERNS = {
    "station": re.compile(r"Station\s*[:=]\s*(\S+)", re.IGNORECASE),
    "start_time": re.compile(r"Start\s*Time\s*[:=]\s*([0-9/\-:\s]+)", re.IGNORECASE),
    "end_time": re.compile(r"End\s*Time\s*[:=]\s*([0-9/\-:\s]+)", re.IGNORECASE),
    "result": re.compile(r"Result\s*[:=]\s*(PASS|FAIL)", re.IGNORECASE),
}

BARCODE_PATTERN = re.compile(r"Barcode\s+information\s*[:=]\s*(.+)", re.IGNORECASE)
RUNTIME_PATTERN = re.compile(r"Run\s*Time\s*[:=]\s*(.+)", re.IGNORECASE)


# ===== 4. 스텝 라인 패턴 =====
STEP_LINE_PATTERN = re.compile(
    r"""
    ^\s*
    (?P<step_no>\d+(?:\.\d+)?)          # 1.00
    \s+
    (?P<desc>.+?)                       # 설명 (콤마 전까지)
    \s*,\s*
    (?P<value>[^,]+)                    # 첫 값
    ,\s*
    (?P<min>[^,]+)                      # MIN
    ,\s*
    (?P<max>[^,]+)                      # MAX
    ,\s*
    \[(?P<status>[A-Za-z]+)\]           # [PASS], [FAIL]
    \s*$
    """,
    re.VERBOSE | re.MULTILINE,
)


def parse_steps_from_text(text: str):
    """
    STEP_LINE_PATTERN 형식의 스텝 라인을 steps 리스트로 파싱.

    - step_no: "1.00" 처럼 문자열 그대로
    - desc, value, min, max, status: 모두 문자열 그대로
    """
    steps = []

    for m in STEP_LINE_PATTERN.finditer(text):
        step_no_raw = m.group("step_no")
        desc = m.group("desc").rstrip()

        value_raw = m.group("value").strip()
        min_raw = m.group("min").strip()
        max_raw = m.group("max").strip()
        status_raw = m.group("status")

        step = {
            "step_no": step_no_raw,   # "1.00", "1.10" 그대로
            "desc": desc,
            "value": value_raw,
            "min": min_raw,
            "max": max_raw,
            "status": status_raw.upper(),
        }
        steps.append(step)

    return steps


def parse_log_content(text: str):
    """
    로그 전체에서 station, start_time, end_time, result,
    Barcode information, Run Time, steps를 추출.
    """
    header = {}

    # 기본 헤더 (station, start_time, end_time, result)
    for key, pattern in HEADER_PATTERNS.items():
        m = pattern.search(text)
        header[key] = m.group(1).strip() if m else None

    if header.get("result"):
        header["result"] = header["result"].upper()

    # Barcode information
    m_bar = BARCODE_PATTERN.search(text)
    barcode_info = m_bar.group(1).strip() if m_bar else None

    # Run Time
    m_rt = RUNTIME_PATTERN.search(text)
    run_time = m_rt.group(1).strip() if m_rt else None

    # Steps
    steps = parse_steps_from_text(text)

    return {
        "station": header.get("station"),
        "barcode_information": barcode_info,
        "start_time": header.get("start_time"),
        "end_time": header.get("end_time"),
        "run_time": run_time,
        "result": header.get("result"),
        "steps": steps,
    }


# ===== 5. 개별 파일 변환 함수 =====
def convert_one_file(args):
    """
    단일 파일 → 트리형 JSON 변환 후 저장.
    """
    input_path, rel_path = args

    # 출력 JSON 경로
    rel_dir, rel_file = os.path.split(rel_path)
    file_stem, _ = os.path.splitext(rel_file)
    output_path = os.path.join(OUTPUT_ROOT, rel_dir, file_stem + ".json")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    text = read_file_text(input_path)
    parsed = parse_log_content(text)

    # key 순서 맞춰서 dict 구성 (station 밑에 Barcode, end_time 밑에 Run Time)
    data = {
        "source_path": input_path,
        "relative_path": rel_path,
        "file_name": os.path.basename(input_path),
        "station": parsed["station"],
        "Barcode  information": parsed["barcode_information"],  # ← 추가
        "start_time": parsed["start_time"],
        "end_time": parsed["end_time"],
        "Run Time": parsed["run_time"],                         # ← 추가
        "result": parsed["result"],
        "steps": parsed["steps"],
    }

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    return input_path


# ===== 6. 메인: 수평전개 + 멀티프로세싱 =====
def main():
    print("파일 스캔 중(수평전개)...")

    convert_tasks = []

    # TC6 ~ TC9 하위 전체 파일 수집
    for tc in TC_LIST:
        tc_dir = os.path.join(INPUT_ROOT, tc)
        if not os.path.isdir(tc_dir):
            print(f"[경고] 없음 → {tc_dir}")
            continue

        for root, dirs, files in os.walk(tc_dir):
            for file in files:
                input_path = os.path.join(root, file)
                rel_path = os.path.relpath(input_path, INPUT_ROOT)
                convert_tasks.append((input_path, rel_path))

    total_files = len(convert_tasks)
    print(f"총 {total_files}개 파일 발견. 병렬 트리형 JSON 변환 시작...")

    if total_files == 0:
        print("변환할 파일이 없습니다.")
        return

    cpu_count = max(2, mp.cpu_count() - 1)
    print(f"병렬 프로세스: {cpu_count}개")

    with mp.Pool(cpu_count) as pool:
        for i, result in enumerate(pool.imap_unordered(convert_one_file, convert_tasks), 1):
            print(f"[{i}/{total_files}] 변환 완료: {result}")

    print("\n=== 모든 파일 트리형 JSON 변환 완료 ===")


if __name__ == "__main__":
    main()
