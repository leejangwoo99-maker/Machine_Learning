from pathlib import Path
from datetime import datetime, timedelta
import time
import multiprocessing as mp

import psycopg2
from psycopg2.extras import execute_batch

# ============================================
# 0. 기본 설정
#    >> 경로 / DB 정보는 여기만 수정하면 됨
# ============================================
# 로그 위치 (현재는 로컬 RAW_LOG)
# NAS 사용할 땐 아래 주석 해제해서 쓰면 됨:
# BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")

# 중간 폴더 & 타겟 폴더
MIDDLE_FOLDERS = ["TC6", "TC7", "TC8", "TC9", "Vision03"]
TARGET_FOLDERS = ["GoodFile", "BadFile"]

# TC6~9 → FCT1~4 매핑
FCT_MAP = {
    "TC6": "FCT1",
    "TC7": "FCT2",
    "TC8": "FCT3",
    "TC9": "FCT4",
}

# PostgreSQL 접속 정보
DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# 스키마 이름
SCHEMA_HISTORY = "a1_fct_vision_testlog_txt_processing_history"
SCHEMA_RESULT = "a1_fct_vision_testlog_txt_processing_result"
SCHEMA_DETAIL = "a1_fct_vision_testlog_txt_processing_result_detail"


# ============================================
# 1. DB 유틸
# ============================================
def table_name_from_schema(schema: str) -> str:
    """
    스키마명에서 'a1_'만 제거하여 테이블명 생성
    예) a1_fct_vision_testlog_txt_processing_history
        -> fct_vision_testlog_txt_processing_history
    """
    return schema[3:] if schema.startswith("a1_") else schema


def get_connection():
    return psycopg2.connect(**DB_CONFIG)


def init_db(conn):
    """
    스키마 / 테이블 자동 생성
    - history  : CSV 형식 (full_path, equipment, date_folder, good_bad, filename, processed_at)
    - result   : 설비별 요약 (run_started_at, run_finished_at, equipment, file_count)
    - detail   : 파일명 검증 상세 (run_started_at, run_finished_at, path_label, filename, reason)
    """
    cur = conn.cursor()

    # ---------- history ----------
    sch = SCHEMA_HISTORY
    tbl = table_name_from_schema(sch)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sch};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
            id           BIGSERIAL PRIMARY KEY,
            full_path    TEXT NOT NULL,
            equipment    TEXT,
            date_folder  TEXT,
            good_bad     TEXT,
            filename     TEXT NOT NULL,
            processed_at TIMESTAMPTZ NOT NULL
        );
        """
    )
    # full_path / filename 인덱스 (중복 검사용)
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{tbl}_full_path ON {sch}.{tbl}(full_path);"
    )
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{tbl}_filename ON {sch}.{tbl}(filename);"
    )

    # ---------- result ----------
    sch = SCHEMA_RESULT
    tbl = table_name_from_schema(sch)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sch};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
            id              BIGSERIAL PRIMARY KEY,
            run_started_at  TIMESTAMPTZ NOT NULL,
            run_finished_at TIMESTAMPTZ NOT NULL,
            equipment       TEXT NOT NULL,
            file_count      INTEGER NOT NULL
        );
        """
    )

    # ---------- detail ----------
    sch = SCHEMA_DETAIL
    tbl = table_name_from_schema(sch)
    cur.execute(f"CREATE SCHEMA IF NOT EXISTS {sch};")
    cur.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {sch}.{tbl} (
            id              BIGSERIAL PRIMARY KEY,
            run_started_at  TIMESTAMPTZ NOT NULL,
            run_finished_at TIMESTAMPTZ NOT NULL,
            path_label      TEXT NOT NULL,
            filename        TEXT NOT NULL,
            reason          TEXT NOT NULL
        );
        """
    )

    conn.commit()
    cur.close()


def load_processed_keys(conn):
    """
    이미 PostgreSQL history 테이블에 올라간
    full_path와 filename을 전부 읽어와서 set으로 반환.
    -> 둘 중 하나라도 겹치면 '중복'으로 판단해서 스킵.
    """
    sch = SCHEMA_HISTORY
    tbl = table_name_from_schema(sch)
    cur = conn.cursor()
    cur.execute(f"SELECT full_path, filename FROM {sch}.{tbl};")
    rows = cur.fetchall()
    cur.close()

    processed_full_paths = set()
    processed_filenames = set()
    for fp, fn in rows:
        if fp:
            processed_full_paths.add(fp)
        if fn:
            processed_filenames.add(fn)
    return processed_full_paths, processed_filenames


def insert_history_rows(conn, rows):
    """history용 데이터 INSERT"""
    if not rows:
        return 0
    sch = SCHEMA_HISTORY
    tbl = table_name_from_schema(sch)
    cur = conn.cursor()
    execute_batch(
        cur,
        f"""
        INSERT INTO {sch}.{tbl}
            (full_path, equipment, date_folder, good_bad, filename, processed_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [
            (
                r["full_path"],
                r["equipment"],
                r["date_folder"],
                r["good_bad"],
                r["filename"],
                r["processed_at"],
            )
            for r in rows
        ],
        page_size=1000,
    )
    conn.commit()
    cur.close()
    return len(rows)


def insert_result_rows(conn, rows):
    """result용 데이터 INSERT"""
    if not rows:
        return 0
    sch = SCHEMA_RESULT
    tbl = table_name_from_schema(sch)
    cur = conn.cursor()
    execute_batch(
        cur,
        f"""
        INSERT INTO {sch}.{tbl}
            (run_started_at, run_finished_at, equipment, file_count)
        VALUES (%s, %s, %s, %s)
        """,
        [
            (
                r["run_started_at"],
                r["run_finished_at"],
                r["equipment"],
                r["file_count"],
            )
            for r in rows
        ],
        page_size=100,
    )
    conn.commit()
    cur.close()
    return len(rows)


def insert_detail_rows(conn, rows):
    """detail용 데이터 INSERT"""
    if not rows:
        return 0
    sch = SCHEMA_DETAIL
    tbl = table_name_from_schema(sch)
    cur = conn.cursor()
    execute_batch(
        cur,
        f"""
        INSERT INTO {sch}.{tbl}
            (run_started_at, run_finished_at, path_label, filename, reason)
        VALUES (%s, %s, %s, %s, %s)
        """,
        [
            (
                r["run_started_at"],
                r["run_finished_at"],
                r["path_label"],
                r["filename"],
                r["reason"],
            )
            for r in rows
        ],
        page_size=1000,
    )
    conn.commit()
    cur.close()
    return len(rows)


# ============================================
# 2. Vision3 설비 분류 (각 프로세스에서 사용)
# ============================================
def classify_vision_equipment(file_path: Path):
    """
    파일 6번째 줄의 Test Program으로 Vision1/Vision2 결정
    """
    equipment = "Vision?"
    test_program = None
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            lines = f.readlines()
        if len(lines) >= 6:
            line6 = lines[5]
            if "Test Program" in line6:
                if "LED1" in line6:
                    equipment, test_program = "Vision1", "LED1"
                elif "LED2" in line6:
                    equipment, test_program = "Vision2", "LED2"
                else:
                    equipment = "Vision3"
            else:
                equipment = "Vision3"
    except Exception:
        equipment = "Vision3"
        test_program = None
    return equipment, test_program


# ============================================
# 3. 한 파일 처리 (멀티프로세스에서 호출)
# ============================================
def process_one_file(args):
    """
    args: (full_path_str, mid, folder_date, gb)
    return: dict(
        history_row = {...},
        equipment   = "FCT1" / "Vision1" ...
        detail_rows = [ {...}, ... ]
    )
    """
    full_path_str, mid, folder_date, gb = args
    p = Path(full_path_str)
    stem = p.stem
    length = len(stem)
    char18 = stem[17] if length >= 18 else ""

    # 설비 분류
    if mid in FCT_MAP:
        equipment, tp = FCT_MAP[mid], None
    else:
        equipment, tp = classify_vision_equipment(p)

    # --------------------------
    # c) 파일명 길이 검증
    # --------------------------
    length_reason = None
    if length < 18:
        length_reason = "파일명 길이<18 → 잘못된 파일명"
    else:
        if char18 in ("C", "1"):
            if length != 51:
                length_reason = f"18번째={char18} → 길이 51 아님(현재 {length})"
        elif char18 == "J":
            if length not in (51, 53):
                length_reason = f"18번째=J → 길이 51/53 아님({length})"
            else:
                if length == 53 and (len(stem) < 47 or stem[46] != "R"):
                    length_reason = "길이 53인데 47번째 글자 R 아님"
        elif char18 in ("P", "N"):
            if length != 52:
                length_reason = f"18번째={char18} → 길이 52 아님({length})"
        elif char18 == "S":
            if length not in (52, 54):
                length_reason = f"18번째=S → 길이 52/54 아님({length})"
            else:
                if length == 54 and (len(stem) < 48 or stem[47] != "R"):
                    length_reason = "길이 54인데 48번째 글자 R 아님"
        else:
            length_reason = f"18번째 글자 규칙 외({char18})"

    # --------------------------
    # d) 날짜 비교
    #   - 길이 51/53 → 32~39
    #   - 길이 52/54 → 33~40
    #   - OK: |file_date - folder_date| <= 1일
    #         (폴더 기준 -1일, 0일, +1일까지 OK)
    # --------------------------
    date_reason = None
    name_date = ""
    try:
        if length in (51, 53):
            name_date = stem[31:39]
        elif length in (52, 54):
            name_date = stem[32:40]
        else:
            date_reason = "[날짜] 길이 규칙 벗어나 날짜 추출불가"

        if not date_reason:
            file_date = datetime.strptime(name_date, "%Y%m%d").date()
            folder_date_dt = datetime.strptime(folder_date, "%Y%m%d").date()

            day_diff = (file_date - folder_date_dt).days
            # 폴더 yyyymmdd, yyyymmdd(d+1), yyyymmdd(d-1) 모두 허용
            if day_diff not in (-1, 0, 1):
                date_reason = (
                    f"[날짜] 파일={file_date} / 폴더={folder_date_dt} "
                    f"(차이 {day_diff}일)"
                )
    except Exception:
        if not date_reason:
            date_reason = f"[날짜] 날짜 파싱 오류({name_date})"

    # 경로 라벨
    path_label = f"{equipment}\\{folder_date}\\{gb}"

    # history용 한 행
    history_row = {
        "full_path": full_path_str,
        "equipment": equipment,
        "date_folder": folder_date,
        "good_bad": gb,
        "filename": p.name,
        "processed_at": datetime.now(),  # timestamptz
    }

    # detail용 행 목록
    detail_rows = []
    if length_reason:
        detail_rows.append(
            {
                "path_label": path_label,
                "filename": p.name,
                "reason": "[길이] " + length_reason,
            }
        )
    if date_reason:
        detail_rows.append(
            {
                "path_label": path_label,
                "filename": p.name,
                "reason": date_reason,
            }
        )

    return {
        "history_row": history_row,
        "equipment": equipment,
        "detail_rows": detail_rows,
    }


# ============================================
# 4. 한 번 실행(run_once): 파일 스캔 → 멀티프로세스 → DB 저장
# ============================================
def run_once():
    started_at = datetime.now()
    print("\n==================== run_once 시작 ====================")
    print(f"시각: {started_at}")

    conn = get_connection()
    try:
        # 스키마 / 테이블 생성
        init_db(conn)

        # 이미 처리된 full_path / filename 로드
        processed_full_paths, processed_filenames = load_processed_keys(conn)
        print(f"[이력] 이미 처리된 full_path 수 : {len(processed_full_paths)}")
        print(f"[이력] 이미 처리된 filename 수 : {len(processed_filenames)}")

        # ---------- 파일 스캔 ----------
        file_infos = []
        total_scanned = 0
        seen_full_paths_this_run = set()
        seen_filenames_this_run = set()

        for mid in MIDDLE_FOLDERS:
            mid_path = BASE_LOG_DIR / mid
            if not mid_path.exists():
                print(f"[SKIP] {mid_path} 없음")
                continue

            for date_folder in sorted(mid_path.iterdir()):
                if not date_folder.is_dir():
                    continue
                folder_date = date_folder.name

                for gb in TARGET_FOLDERS:
                    gb_path = date_folder / gb
                    if not gb_path.exists():
                        continue

                    for f in gb_path.iterdir():
                        if not f.is_file():
                            continue

                        total_scanned += 1
                        full_path_str = str(f)
                        filename = f.name

                        # 이미 PostgreSQL에 올라간 파일이면 패스
                        if (
                            full_path_str in processed_full_paths
                            or filename in processed_filenames
                        ):
                            continue

                        # 이번 실행 내에서 이미 본 파일이면 패스
                        if (
                            full_path_str in seen_full_paths_this_run
                            or filename in seen_filenames_this_run
                        ):
                            continue

                        seen_full_paths_this_run.add(full_path_str)
                        seen_filenames_this_run.add(filename)
                        file_infos.append((full_path_str, mid, folder_date, gb))

        print(f"[스캔] 전체 스캔 파일 수: {total_scanned}")
        print(f"[스캔] 이번 실행에서 새로 처리할 파일 수: {len(file_infos)}")

        if not file_infos:
            print("[정보] 새로 처리할 파일이 없습니다.")
            return

        # ---------- 멀티프로세스로 파일 처리 ----------
        cpu_cnt = max(1, mp.cpu_count() - 1)
        print(f"[멀티프로세스] 사용 프로세스 수: {cpu_cnt}")

        with mp.Pool(processes=cpu_cnt) as pool:
            results = pool.map(process_one_file, file_infos)

        # ---------- 결과 정리 ----------
        history_rows = []
        detail_rows_raw = []
        equip_counts = {}

        for item in results:
            history_rows.append(item["history_row"])
            for d in item["detail_rows"]:
                detail_rows_raw.append(d)
            eq = item["equipment"]
            equip_counts[eq] = equip_counts.get(eq, 0) + 1

        finished_at = datetime.now()

        # result(요약) 행들 (설비별 1행씩)
        result_rows = [
            {
                "run_started_at": started_at,
                "run_finished_at": finished_at,
                "equipment": eq,
                "file_count": cnt,
            }
            for eq, cnt in equip_counts.items()
        ]

        # detail 행들에 run time 정보 추가
        detail_rows = [
            {
                "run_started_at": started_at,
                "run_finished_at": finished_at,
                "path_label": d["path_label"],
                "filename": d["filename"],
                "reason": d["reason"],
            }
            for d in detail_rows_raw
        ]

        # ---------- DB 저장 ----------
        n_hist = insert_history_rows(conn, history_rows)
        n_res = insert_result_rows(conn, result_rows)
        n_det = insert_detail_rows(conn, detail_rows)

        print(f"[DB] history 저장 건수 : {n_hist}")
        print(f"[DB] result  저장 건수 : {n_res}")
        print(f"[DB] detail  저장 건수 : {n_det}")

    finally:
        conn.close()
        print("==================== run_once 종료 ====================")


# ============================================
# 5. 메인 루프: 1초마다 무한 반복
# ============================================
if __name__ == "__main__":
    try:
        while True:
            run_once()
            time.sleep(1)  # 1초 대기 후 반복
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다. 프로그램을 종료합니다.")
