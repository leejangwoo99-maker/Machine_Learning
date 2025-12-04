from pathlib import Path
from datetime import datetime, date
import time
import multiprocessing as mp
import calendar

import psycopg2
from psycopg2.extras import execute_batch

# ============================================
# 0. 기본 설정
# ============================================
# 로그 위치 (NAS HistoryLog)
BASE_LOG_DIR = Path(r"\\192.168.108.101\HistoryLog")
# BASE_LOG_DIR = Path(r"C:\Users\user\Desktop\RAW_LOG")  # 로컬 테스트용

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
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

# 스키마 이름
SCHEMA_HISTORY = "a1_fct_vision_testlog_txt_processing_history"
SCHEMA_RESULT = "a1_fct_vision_testlog_txt_processing_result"
SCHEMA_DETAIL = "a1_fct_vision_testlog_txt_processing_result_detail"

# 고정 최소 시작일 (예: 2025-10-01부터만 본다)
FIXED_START_DATE = date(2025, 10, 1)

# 한 번에 멀티프로세스로 처리할 최대 파일 개수 (메모리 절약용)
BATCH_SIZE = 10000


# ============================================
# 날짜 유틸: 오늘 기준 6개월 전 계산
# ============================================
def one_month_ago(d: date) -> date:
    year = d.year
    month = d.month - 1
    if month <= 0:
        year -= 1
        month += 12

    last_day = calendar.monthrange(year, month)[1]
    day = min(d.day, last_day)
    return date(year, month, day)

def get_window_dates():
    """
    오늘 날짜 기준으로 '이번 달 1일 ~ 오늘' 범위를 반환.
    예)
      - 오늘 = 2025-12-04 → 2025-12-01 ~ 2025-12-04
      - 오늘 = 2025-12-31 → 2025-12-01 ~ 2025-12-31
      - 오늘 = 2026-01-01 → 2026-01-01 ~ 2026-01-01
    """
    today = date.today()

    # 이번 달 1일
    # 이번 달 1일
    month_start = today.replace(day=1)

    # FIXED_START_DATE 이후부터만 보겠다는 정책 유지
    window_start_date = max(month_start, FIXED_START_DATE)

    # 윈도우 끝은 '오늘'
    window_end_date = today

    return window_start_date, window_end_date

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
    cur.execute(
        f"CREATE INDEX IF NOT EXISTS idx_{tbl}_full_path ON {sch}.{tbl}(full_path);"
    )

    # 🔥 여기 추가
    cur.execute(
        f"CREATE UNIQUE INDEX IF NOT EXISTS uq_{tbl}_full_path ON {sch}.{tbl}(full_path);"
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


def cleanup_old_data(conn, window_start_date: date):
    """
    RDBMS에서 '현재 날짜 기준 6개월 이상 된 데이터' 완전 삭제 (DELETE 사용).

    - history : date_folder < window_start_date (yyyymmdd 비교)
    - result  : run_started_at < window_start_date 00:00:00
    - detail  : run_started_at < window_start_date 00:00:00
    """
    cutoff_str = window_start_date.strftime("%Y%m%d")
    cutoff_dt = datetime.combine(window_start_date, datetime.min.time())

    cur = conn.cursor()

    # history
    sch = SCHEMA_HISTORY
    tbl = table_name_from_schema(sch)
    cur.execute(
        f"""
        DELETE FROM {sch}.{tbl}
        WHERE date_folder < %s
        """,
        (cutoff_str,),
    )
    deleted_hist = cur.rowcount

    # result
    sch = SCHEMA_RESULT
    tbl = table_name_from_schema(sch)
    cur.execute(
        f"""
        DELETE FROM {sch}.{tbl}
        WHERE run_started_at < %s
        """,
        (cutoff_dt,),
    )
    deleted_res = cur.rowcount

    # detail
    sch = SCHEMA_DETAIL
    tbl = table_name_from_schema(sch)
    cur.execute(
        f"""
        DELETE FROM {sch}.{tbl}
        WHERE run_started_at < %s
        """,
        (cutoff_dt,),
    )
    deleted_det = cur.rowcount

    conn.commit()
    cur.close()

    print(
        f"[정리] 6개월 이전 데이터 삭제 완료 "
        f"(history={deleted_hist}, result={deleted_res}, detail={deleted_det})"
    )


def load_processed_paths(conn, window_start_date: date, window_end_date: date):
    """
    이미 PostgreSQL history 테이블에 올라간 full_path를 읽어서 set으로 반환.
    >> date_folder를 window_start_date ~ window_end_date 범위로 제한해서 메모리 절약.
    """
    sch = SCHEMA_HISTORY
    tbl = table_name_from_schema(sch)
    cur = conn.cursor()

    start_str = window_start_date.strftime("%Y%m%d")
    end_str = window_end_date.strftime("%Y%m%d")

    cur.execute(
        f"""
        SELECT full_path
        FROM {sch}.{tbl}
        WHERE date_folder BETWEEN %s AND %s
        """,
        (start_str, end_str),
    )
    rows = cur.fetchall()
    cur.close()

    processed_full_paths = {fp for (fp,) in rows if fp}
    return processed_full_paths


def insert_history_rows(conn, rows):
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
        ON CONFLICT (full_path) DO NOTHING
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
    inserted = cur.rowcount  # 실제 들어간 행 수
    cur.close()
    return inserted

def insert_result_rows(conn, rows):
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
# 2. Vision03 설비 분류
# ============================================
def classify_vision_equipment(file_path: Path):
    """
    파일 6번째 줄의 Test Program으로 Vision1/Vision2 결정
    >> 메모리 절약: 6줄까지만 순차 읽기
    """
    equipment = "Vision?"
    test_program = None
    try:
        with open(file_path, "r", encoding="cp949", errors="ignore") as f:
            for i, line in enumerate(f, start=1):
                if i == 6:  # 6번째 줄
                    if "Test Program" in line:
                        if "LED1" in line:
                            equipment, test_program = "Vision1", "LED1"
                        elif "LED2" in line:
                            equipment, test_program = "Vision2", "LED2"
                        else:
                            equipment = "Vision3"
                    else:
                        equipment = "Vision3"
                    break
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
    """
    full_path_str, mid, folder_date, gb = args
    p = Path(full_path_str)
    stem = p.stem
    length = len(stem)
    char18 = stem[17] if length >= 18 else ""

    # 설비 분류
    if mid in FCT_MAP:
        equipment, tp = FCT_MAP[mid], None
    elif mid == "Vision03":
        equipment, tp = classify_vision_equipment(p)
    else:
        equipment, tp = mid, None

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
        "processed_at": datetime.now(),
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
# 4. 배치 처리
# ============================================
def process_batch(pool, file_infos, conn, equip_counts, run_started_at):
    """
    file_infos: [(full_path_str, mid, folder_date_str, gb), ...]
    """
    if not file_infos:
        return 0, 0

    batch_finished_at = datetime.now()
    results = pool.map(process_one_file, file_infos)

    history_rows = []
    detail_rows = []

    for item in results:
        h = item["history_row"]
        history_rows.append(h)

        eq = item["equipment"]
        equip_counts[eq] = equip_counts.get(eq, 0) + 1

        for d in item["detail_rows"]:
            detail_rows.append(
                {
                    "run_started_at": run_started_at,
                    "run_finished_at": batch_finished_at,
                    "path_label": d["path_label"],
                    "filename": d["filename"],
                    "reason": d["reason"],
                }
            )

    n_hist = insert_history_rows(conn, history_rows)
    n_det = insert_detail_rows(conn, detail_rows)

    return n_hist, n_det


# ============================================
# 5. 한 번 실행(run_once)
# ============================================
def run_once():
    run_started_at = datetime.now()
    print("\n==================== run_once 시작 ====================")
    print(f"시각: {run_started_at}")

    # 현재 기준 6개월 윈도우 계산
    window_start_date, window_end_date = get_window_dates()
    print(f"[윈도우] 스캔/보관 기간: {window_start_date} ~ {window_end_date}")

    conn = get_connection()
    try:
        # 스키마 / 테이블 생성
        init_db(conn)

        total_scanned = 0
        total_new = 0
        total_hist_inserted = 0
        total_det_inserted = 0
        equip_counts = {}

        # 🔥 CPU 코어 기반이 아니라 "고정 2개"로 강제
        cpu_cnt = 2
        print(f"[멀티프로세스] 사용 프로세스 수: {cpu_cnt}")

        window_start_str = window_start_date.strftime("%Y%m%d")
        window_end_str = window_end_date.strftime("%Y%m%d")

        # 🔥 multiprocessing Pool = 2개
        with mp.Pool(processes=cpu_cnt) as pool:
            batch = []

            for mid in MIDDLE_FOLDERS:
                mid_path = BASE_LOG_DIR / mid
                if not mid_path.exists():
                    print(f"[SKIP] {mid_path} 없음")
                    continue

                for date_folder in sorted(mid_path.iterdir()):
                    if not date_folder.is_dir():
                        continue

                    folder_date_str = date_folder.name

                    # yyyymmdd 형식 체크
                    if len(folder_date_str) != 8 or not folder_date_str.isdigit():
                        continue

                    # 폴더 날짜가 윈도우 범위 안에 있는지 체크
                    if not (window_start_str <= folder_date_str <= window_end_str):
                        continue

                    for gb in TARGET_FOLDERS:
                        gb_path = date_folder / gb
                        if not gb_path.exists():
                            continue

                        for f in gb_path.iterdir():
                            if not f.is_file():
                                continue

                            total_scanned += 1
                            full_path_str = str(f)

                            batch.append((full_path_str, mid, folder_date_str, gb))
                            total_new += 1

                            if len(batch) >= BATCH_SIZE:
                                print(f"[배치 처리] {len(batch)}개 파일 처리 시작...")
                                n_hist, n_det = process_batch(
                                    pool,
                                    batch,
                                    conn,
                                    equip_counts,
                                    run_started_at,
                                )

                                total_hist_inserted += n_hist
                                total_det_inserted += n_det
                                print(
                                    f"[배치 처리] history {n_hist}건, "
                                    f"detail {n_det}건 저장 완료."
                                )
                                batch = []

            # 남은 배치 처리
            if batch:
                print(f"[배치 처리] 마지막 {len(batch)}개 파일 처리 시작...")
                n_hist, n_det = process_batch(
                    pool,
                    batch,
                    conn,
                    processed_full_paths,
                    equip_counts,
                    run_started_at,
                )
                total_hist_inserted += n_hist
                total_det_inserted += n_det
                print(
                    f"[배치 처리] history {n_hist}건, "
                    f"detail {n_det}건 저장 완료."
                )

        run_finished_at = datetime.now()

        print(f"[스캔] 전체 스캔 파일 수: {total_scanned}")
        print(f"[스캔] 이번 실행에서 새로 처리한 파일 수: {total_new}")
        print(f"[DB] 누적 history 저장 건수 : {total_hist_inserted}")
        print(f"[DB] 누적 detail  저장 건수 : {total_det_inserted}")

        # result(요약) 행들 (설비별 1행씩)
        result_rows = [
            {
                "run_started_at": run_started_at,
                "run_finished_at": run_finished_at,
                "equipment": eq,
                "file_count": cnt,
            }
            for eq, cnt in equip_counts.items()
        ]
        n_res = insert_result_rows(conn, result_rows)
        print(f"[DB] result  저장 건수 : {n_res}")

    finally:
        conn.close()
        print("==================== run_once 종료 ====================")

# ============================================
# 6. 메인 루프
# ============================================
if __name__ == "__main__":
    try:
        while True:
            try:
                run_once()
            except Exception as e:
                print("[ERROR] run_once 중 예외 발생:", e)

            time.sleep(1)  # 1초 대기 후 재실행
    except KeyboardInterrupt:
        print("\n사용자에 의해 중단되었습니다. 프로그램을 종료합니다.")
