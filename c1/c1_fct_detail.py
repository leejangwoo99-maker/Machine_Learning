# c1_fct_detail_loader.py
# ============================================
# FCT Detail TXT Parser -> PostgreSQL 적재
# (요구사항 통합 + 추가 2가지 반영)
#
# 추가 반영 사항:
# A) remark는 반드시 'PD' 또는 'Non-PD'만 허용
#    - 폴더명에 'PD NONE' 포함 -> 'Non-PD'
#    - 그 외 폴더명에 'PD' 포함 -> 'PD'
#    - 둘 다 아니면 해당 파일은 "파일 전체 제외"
#
# B) test_ct 음수 보정 (자정 넘김 케이스)
#    - 현재 test_time - 이전 test_time < 0 이면 +86400(초) 보정
#
# Scan:
#   C:\Users\user\Desktop\machinlog\FCT\yyyy\mm\dd\**\*.txt (반드시 yyyy/mm/dd 형식만)
# filename:
#   (barcode)_yyyymmdd_(start time).txt
# line:
#   [hh:mm:ss.ss] (내용)  (형식 외 라인은 제외)
# end_time:
#   마지막 행 시간 반올림 -> hh:mm:ss
# end_day:
#   파일명 yyyymmdd (단, end_time < start_time이면 end_day +1)
# 중복방지:
#   file_path 이미 존재하면 파일 통째 PASS
# ============================================

import re
from pathlib import Path
from datetime import datetime, timedelta, time as dt_time
from multiprocessing import Pool, cpu_count, freeze_support

import urllib.parse
import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 설정
# =========================
BASE_DIR = Path(r"C:\Users\user\Desktop\machinlog\FCT")

DB_CONFIG = {
    "host": "localhost",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "",#비번은 보완 사항
}

SCHEMA_NAME = "c1_fct_detail"
TABLE_NAME = "fct_detail"

# 라인 패턴: [hh:mm:ss.ss] 내용
LINE_RE = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{1,3})\]\s(.+)$")

# yyyy/mm/dd 디렉토리명 검증
YEAR_RE = re.compile(r"^\d{4}$")
MONTH_RE = re.compile(r"^\d{2}$")
DAY_RE = re.compile(r"^\d{2}$")

# 파일명: (barcode)_yyyymmdd_(start time).txt
FNAME_RE = re.compile(r"^(.+?)_(\d{8})_(.+?)\.txt$", re.IGNORECASE)


# =========================
# 1) 유틸
# =========================
def _conn_str(cfg: dict) -> str:
    pw = urllib.parse.quote_plus(cfg["password"])
    return f"postgresql+psycopg2://{cfg['user']}:{pw}@{cfg['host']}:{cfg['port']}/{cfg['dbname']}"

def _psycopg2_conn(cfg: dict):
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg["port"],
        dbname=cfg["dbname"],
        user=cfg["user"],
        password=cfg["password"],
    )

def _ensure_schema_and_table():
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};

    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
        barcode_information TEXT,
        remark              TEXT,
        end_day             DATE,
        end_time            TIME,
        contents            VARCHAR(80),
        test_ct             DOUBLE PRECISION,
        test_time           VARCHAR(12),
        file_path           TEXT
    );

    CREATE INDEX IF NOT EXISTS ix_{TABLE_NAME}_file_path
        ON {SCHEMA_NAME}.{TABLE_NAME} (file_path);
    """
    with _psycopg2_conn(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(ddl)
        conn.commit()

def _load_existing_file_paths() -> set:
    sql = f"SELECT DISTINCT file_path FROM {SCHEMA_NAME}.{TABLE_NAME};"
    with _psycopg2_conn(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            rows = cur.fetchall()
    return {r[0] for r in rows if r and r[0]}

def _parse_time_to_seconds(t_str: str) -> float:
    # t_str: "hh:mm:ss.ss"
    hh = int(t_str[0:2])
    mm = int(t_str[3:5])
    ss = float(t_str[6:])  # includes decimals
    return hh * 3600.0 + mm * 60.0 + ss

def _round_to_hms(t_str: str) -> dt_time:
    # "hh:mm:ss.ss" -> nearest second
    sec = _parse_time_to_seconds(t_str)
    sec_rounded = int(sec + 0.5)
    sec_rounded %= 24 * 3600
    hh = sec_rounded // 3600
    mm = (sec_rounded % 3600) // 60
    ss = sec_rounded % 60
    return dt_time(hour=hh, minute=mm, second=ss)

def _infer_remark_strict(file_path: Path) -> str | None:
    """
    remark는 반드시 PD/Non-PD만 허용.
    - 'PD NONE' (우선) -> Non-PD
    - 그 외 'PD' -> PD
    - 둘 다 아니면 None
    """
    parts_upper = [p.upper() for p in file_path.parts]
    if any("PD NONE" in p for p in parts_upper):
        return "Non-PD"
    if any("PD" in p for p in parts_upper):
        return "PD"
    return None

def _safe_read_lines(path: Path) -> list[str]:
    # ANSI(윈도우) 계열 우선
    encodings = ["cp949", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read().splitlines()
        except Exception:
            pass
    # 최후: replace
    with open(path, "r", encoding="cp949", errors="replace") as f:
        return f.read().splitlines()

def _try_parse_start_time_to_seconds(s: str) -> float | None:
    """
    파일명 start time 문자열 파싱:
    1) hh:mm:ss
    2) hh:mm:ss.ss
    3) hhmmss
    """
    s = s.strip()
    m1 = re.match(r"^(\d{2}:\d{2}:\d{2})(\.\d{1,3})?$", s)
    if m1:
        base = m1.group(1)
        frac = m1.group(2) or ".00"
        return _parse_time_to_seconds(base + frac)
    m2 = re.match(r"^(\d{2})(\d{2})(\d{2})$", s)
    if m2:
        return float(int(m2.group(1)) * 3600 + int(m2.group(2)) * 60 + int(m2.group(3)))
    return None


# =========================
# 2) 파일 파싱 (멀티프로세스 worker)
# =========================
def _parse_one_file(file_path_str: str):
    """
    return:
      (file_path_str, rows, status)
      status: "OK" | "SKIP_REMARK" | "SKIP_EMPTY" | "SKIP_BADNAME"
    """
    p = Path(file_path_str)

    # remark 엄격 매칭 (실패 시 파일 제외)
    remark = _infer_remark_strict(p)
    if remark is None:
        return file_path_str, [], "SKIP_REMARK"

    m = FNAME_RE.match(p.name)
    if not m:
        return file_path_str, [], "SKIP_BADNAME"

    barcode = m.group(1).strip()
    yyyymmdd = m.group(2).strip()
    start_time_raw = m.group(3).strip()

    try:
        base_day = datetime.strptime(yyyymmdd, "%Y%m%d").date()
    except ValueError:
        return file_path_str, [], "SKIP_BADNAME"

    lines = _safe_read_lines(p)

    parsed_times = []
    parsed_contents = []

    for line in lines:
        mm2 = LINE_RE.match(line)
        if not mm2:
            continue
        t_str = mm2.group(1).strip()  # hh:mm:ss.ss
        content = mm2.group(2).strip()
        if not content:
            continue
        parsed_times.append(t_str[:12])         # VARCHAR(12)
        parsed_contents.append(content[:80])    # contents <= 80

    if not parsed_times:
        return file_path_str, [], "SKIP_EMPTY"

    # end_time: 마지막 라인 시간 반올림 -> hh:mm:ss
    end_time_obj = _round_to_hms(parsed_times[-1])

    # end_day: 파일명 날짜, 단 start_time 대비 마지막 시간이 작으면 +1일
    start_sec = _try_parse_start_time_to_seconds(start_time_raw)
    end_sec_last = _parse_time_to_seconds(parsed_times[-1])
    end_day = base_day
    if start_sec is not None and end_sec_last < start_sec:
        end_day = base_day + timedelta(days=1)

    # test_ct 계산(초) + 음수 보정(+86400)
    rows = []
    prev_sec = None
    for t_str, content in zip(parsed_times, parsed_contents):
        cur_sec = _parse_time_to_seconds(t_str)
        test_ct = None
        if prev_sec is not None:
            diff = cur_sec - prev_sec
            if diff < 0:
                diff += 86400.0  # 자정 넘김 보정
            test_ct = diff
        prev_sec = cur_sec

        rows.append((
            barcode,          # barcode_information
            remark,           # remark (PD/Non-PD only)
            end_day,          # end_day
            end_time_obj,     # end_time
            content,          # contents
            test_ct,          # test_ct
            t_str,            # test_time
            str(p),           # file_path
        ))

    return file_path_str, rows, "OK"


# =========================
# 3) 스캔: yyyy/mm/dd 형식만
# =========================
def _iter_valid_month_folders(base: Path):
    for y in sorted([p for p in base.iterdir() if p.is_dir() and YEAR_RE.match(p.name)]):
        for m in sorted([p for p in y.iterdir() if p.is_dir() and MONTH_RE.match(p.name)]):
            yield m

def _collect_target_files(base: Path) -> list[str]:
    targets = []
    for month_dir in _iter_valid_month_folders(base):
        print(f"[INFO] Scan folder: {month_dir}")

        day_dirs = [d for d in month_dir.iterdir() if d.is_dir() and DAY_RE.match(d.name)]
        for dd in sorted(day_dirs):
            for fp in dd.rglob("*.txt"):
                if fp.is_file():
                    targets.append(str(fp))
    return targets


# =========================
# 4) DB Insert
# =========================
def _insert_rows(rows: list[tuple]) -> int:
    if not rows:
        return 0
    sql = f"""
    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME}
    (barcode_information, remark, end_day, end_time, contents, test_ct, test_time, file_path)
    VALUES %s
    """
    with _psycopg2_conn(DB_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows, page_size=5000)
        conn.commit()
    return len(rows)


# =========================
# 5) main
# =========================
def main():
    print(f"[INFO] Connection String: {_conn_str(DB_CONFIG)}")

    _ensure_schema_and_table()
    print(f"[INFO] Table ensured: {SCHEMA_NAME}.{TABLE_NAME}")

    all_files = _collect_target_files(BASE_DIR)
    print(f"[INFO] Target files: {len(all_files):,}")

    existing = _load_existing_file_paths()
    new_files = [f for f in all_files if f not in existing]

    procs = max(1, cpu_count() - 1)
    chunksize = 10
    print(f"[INFO] Multiprocessing enabled: processes={procs}, chunksize={chunksize}")
    print(f"[INFO] existing_files={len(existing):,}, new_files={len(new_files):,}")

    if not new_files:
        print("[INFO] No new files. Done.")
        return

    parsed_rows_all = []
    processed = 0

    skip_remark = 0
    skip_empty = 0
    skip_badname = 0

    with Pool(processes=procs) as pool:
        for _, rows, status in pool.imap_unordered(_parse_one_file, new_files, chunksize=chunksize):
            processed += 1
            if status == "OK":
                parsed_rows_all.extend(rows)
            elif status == "SKIP_REMARK":
                skip_remark += 1
            elif status == "SKIP_EMPTY":
                skip_empty += 1
            elif status == "SKIP_BADNAME":
                skip_badname += 1

            if processed % 200 == 0:
                print(f"[INFO] parsing... files_done={processed:,}/{len(new_files):,}, rows_accum={len(parsed_rows_all):,}")

    inserted = _insert_rows(parsed_rows_all)

    print(f"[INFO] Parsed files: {processed:,}")
    print(f"[INFO] Skipped files: remark_mismatch={skip_remark:,}, empty={skip_empty:,}, badname={skip_badname:,}")
    print(f"[INFO] Inserted rows: {inserted:,}")
    print("[INFO] Done.")


if __name__ == "__main__":
    freeze_support()
    main()
