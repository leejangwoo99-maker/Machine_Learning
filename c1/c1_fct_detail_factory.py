# c1_fct_detail_loader_realtime.py
# ============================================
# FCT Detail TXT Parser -> PostgreSQL 적재 (Realtime Loop)
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
#
# end_time:
#   마지막 행 시간 반올림 -> hh:mm:ss
#
# end_day:
#   파일명 yyyymmdd (단, end_time < start_time이면 end_day +1)
#
# 중복방지:
#   file_path 이미 존재하면 파일 통째 PASS (in-memory set + insert 후 set 갱신)
#
# 추가 요구 사양:
# - [멀티프로세스] 2개 고정
# - [무한 루프] 1초마다 재실행
# - [유효 날짜] end_day == 오늘(date.today()) 인 row만 적재
# - [실시간] 현재시간 기준 60초 이내로 새로 생성/수정된 파일만 파싱 (mtime)
# ============================================

import re
import time as time_mod
from pathlib import Path
from datetime import datetime, timedelta, date, time as dt_time
from multiprocessing import Pool, freeze_support

import urllib.parse
import psycopg2
from psycopg2.extras import execute_values


# =========================
# 0) 설정
# =========================
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")  # 루트 (YYYY/MM 구조)

DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA_NAME = "c1_fct_detail"
TABLE_NAME = "fct_detail"

# 실시간 mtime 컷오프(초)
MTIME_WINDOW_SEC = 60

# 무한루프 주기(초)
LOOP_SLEEP_SEC = 1

# 멀티프로세스 고정
MP_PROCESSES = 2

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
    hh = int(t_str[0:2])
    mm = int(t_str[3:5])
    ss = float(t_str[6:])
    return hh * 3600.0 + mm * 60.0 + ss

def _round_to_hms(t_str: str) -> dt_time:
    sec = _parse_time_to_seconds(t_str)
    sec_rounded = int(sec + 0.5)
    sec_rounded %= 24 * 3600
    hh = sec_rounded // 3600
    mm = (sec_rounded % 3600) // 60
    ss = sec_rounded % 60
    return dt_time(hour=hh, minute=mm, second=ss)

def _infer_remark_strict(file_path: Path):
    parts_upper = [p.upper() for p in file_path.parts]
    if any("PD NONE" in p for p in parts_upper):
        return "Non-PD"
    if any("PD" in p for p in parts_upper):
        return "PD"
    return None

def _safe_read_lines(path: Path) -> list[str]:
    encodings = ["cp949", "cp1252", "utf-8"]
    for enc in encodings:
        try:
            with open(path, "r", encoding=enc, errors="strict") as f:
                return f.read().splitlines()
        except Exception:
            pass
    with open(path, "r", encoding="cp949", errors="replace") as f:
        return f.read().splitlines()

def _try_parse_start_time_to_seconds(s: str):
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
def _parse_one_file_only_today(file_path_str: str):
    """
    end_day == 오늘(date.today()) 인 row만 리턴.
    return:
      (file_path_str, rows, status)
      status: "OK" | "SKIP_REMARK" | "SKIP_EMPTY" | "SKIP_BADNAME" | "SKIP_NOT_TODAY"
    """
    p = Path(file_path_str)

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
        t_str = mm2.group(1).strip()
        content = mm2.group(2).strip()
        if not content:
            continue
        parsed_times.append(t_str[:12])
        parsed_contents.append(content[:80])

    if not parsed_times:
        return file_path_str, [], "SKIP_EMPTY"

    end_time_obj = _round_to_hms(parsed_times[-1])

    start_sec = _try_parse_start_time_to_seconds(start_time_raw)
    end_sec_last = _parse_time_to_seconds(parsed_times[-1])
    end_day = base_day
    if start_sec is not None and end_sec_last < start_sec:
        end_day = base_day + timedelta(days=1)

    # [유효 날짜] end_day == 오늘만
    today = date.today()
    if end_day != today:
        return file_path_str, [], "SKIP_NOT_TODAY"

    rows = []
    prev_sec = None
    for t_str, content in zip(parsed_times, parsed_contents):
        cur_sec = _parse_time_to_seconds(t_str)
        test_ct = None
        if prev_sec is not None:
            diff = cur_sec - prev_sec
            if diff < 0:
                diff += 86400.0
            test_ct = diff
        prev_sec = cur_sec

        rows.append((
            barcode,
            remark,
            end_day,
            end_time_obj,
            content,
            test_ct,
            t_str,
            str(p),
        ))

    return file_path_str, rows, "OK"


# =========================
# 3) 스캔: yyyy/mm/dd 형식만 + 실시간(mtime 60초)
# =========================
def _iter_valid_month_folders(base: Path):
    for y in sorted([p for p in base.iterdir() if p.is_dir() and YEAR_RE.match(p.name)]):
        for m in sorted([p for p in y.iterdir() if p.is_dir() and MONTH_RE.match(p.name)]):
            yield m

def _collect_realtime_files(base: Path, cutoff_ts: float) -> list[str]:
    """
    cutoff_ts 이후 mtime 변경이 있는 txt만 수집
    """
    targets = []
    for month_dir in _iter_valid_month_folders(base):
        day_dirs = [d for d in month_dir.iterdir() if d.is_dir() and DAY_RE.match(d.name)]
        for dd in day_dirs:
            for fp in dd.rglob("*.txt"):
                if not fp.is_file():
                    continue
                try:
                    if fp.stat().st_mtime >= cutoff_ts:
                        targets.append(str(fp))
                except Exception:
                    # 접근 중 삭제/권한 등은 스킵
                    continue
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
# 5) main loop
# =========================
def main():
    print(f"[INFO] Connection String: {_conn_str(DB_CONFIG)}")
    _ensure_schema_and_table()
    print(f"[INFO] Table ensured: {SCHEMA_NAME}.{TABLE_NAME}")

    # 기동 시 1회만 로딩 (이후 insert될 때 set 갱신)
    existing = _load_existing_file_paths()
    print(f"[INFO] existing_files_loaded={len(existing):,}")

    chunksize = 10
    print(f"[INFO] Multiprocessing fixed: processes={MP_PROCESSES}, chunksize={chunksize}")
    print(f"[INFO] Realtime window: mtime within {MTIME_WINDOW_SEC}s, loop every {LOOP_SLEEP_SEC}s")
    print(f"[INFO] Only insert rows where end_day == today")

    # 누적 통계(옵션)
    total_inserted = 0

    while True:
        try:
            now_ts = time_mod.time()
            cutoff_ts = now_ts - MTIME_WINDOW_SEC  # 실시간 컷오프(요구사항)

            candidate_files = _collect_realtime_files(BASE_DIR, cutoff_ts)

            # 중복(이미 적재된 file_path) 제외
            new_files = [f for f in candidate_files if f not in existing]

            if not new_files:
                # 너무 시끄럽지 않게 주기적으로만 출력
                time_mod.sleep(LOOP_SLEEP_SEC)
                continue

            parsed_rows_all = []
            processed = 0

            skip_remark = 0
            skip_empty = 0
            skip_badname = 0
            skip_not_today = 0

            with Pool(processes=MP_PROCESSES) as pool:
                for file_path_str, rows, status in pool.imap_unordered(_parse_one_file_only_today, new_files, chunksize=chunksize):
                    processed += 1
                    if status == "OK":
                        parsed_rows_all.extend(rows)
                        # 파일 단위 중복방지 set에 즉시 반영 (동일 loop 내 재처리 방지)
                        existing.add(file_path_str)
                    else:
                        if status == "SKIP_REMARK":
                            skip_remark += 1
                        elif status == "SKIP_EMPTY":
                            skip_empty += 1
                        elif status == "SKIP_BADNAME":
                            skip_badname += 1
                        elif status == "SKIP_NOT_TODAY":
                            skip_not_today += 1

                        # 파싱 실패/스킵 파일은 다음 loop에서 재시도될 수 있으므로 set에 넣지 않음

            inserted = _insert_rows(parsed_rows_all)
            total_inserted += inserted

            print(
                f"[INFO] loop_done | candidates={len(candidate_files):,} new_files={len(new_files):,} "
                f"processed={processed:,} inserted_rows={inserted:,} total_inserted={total_inserted:,} "
                f"| skipped: remark={skip_remark:,}, empty={skip_empty:,}, badname={skip_badname:,}, not_today={skip_not_today:,}"
            )

        except KeyboardInterrupt:
            print("[INFO] KeyboardInterrupt. Stop.")
            break
        except Exception as e:
            # 실시간 루프이므로 죽지 않게
            print(f"[WARN] loop_error: {e}")

        time_mod.sleep(LOOP_SLEEP_SEC)


if __name__ == "__main__":
    freeze_support()
    main()
