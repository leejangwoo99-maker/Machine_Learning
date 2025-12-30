# -*- coding: utf-8 -*-
"""
AFA FAIL (NG -> OFF) wasted time 계산 및 DB 저장 스크립트 (Realtime) - HARDENED (Nuitka/EXE Safe)
✅ Tail-Follow + State Machine 버전 (기존 기능/결과 동일 유지)

[기존 기능 동일 유지]
- 이벤트: NG_TEXT(제품 감지 NG) -> OFF_TEXT(제품 검사 투입요구 ON)
- Manual mode 전환 ~ Auto mode 전환 구간에서는 NG 무시
- 1초마다 무한루프
- 유효 날짜: end_day = 오늘(YYYYMMDD)만
- 실시간 저장 조건: from/to 모두 "현재시간-60초" 이상인 페어만 저장
- 저장: d1_machine_log.afa_fail_wasted_time (append + UNIQUE INDEX + ON CONFLICT DO NOTHING)
- EXE/Nuitka 환경에서도 깨지지 않게 방어

[구조 변경(성능 개선)]
- ❌ DB에서 120초 lookback 로드 + pandas groupby
- ✅ 머신로그 파일을 "tail-follow"로 따라가며 새 줄만 처리
- ✅ station별 메모리 state machine으로 즉시 페어 계산 후 DB 저장

주의:
- 아래 BASE_DIR 및 파일명 패턴은 기존 d1_machine_log 수집기와 동일한 FCT 머신 로그 기준입니다.
"""

import re
import sys
import time
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timedelta
from multiprocessing import freeze_support
from concurrent.futures import ThreadPoolExecutor, as_completed

import psycopg2
from psycopg2.extras import execute_values
from sqlalchemy import create_engine, text as sa_text


# ============================================
# 0) DB / 상수
# ============================================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

TABLE_SAVE_SCHEMA = "d1_machine_log"
TABLE_SAVE_NAME = "afa_fail_wasted_time"

# 이벤트 텍스트 (기존 동일)
NG_TEXT = "제품 감지 NG"
OFF_TEXT = "제품 검사 투입요구 ON"
MANUAL_TEXT = "Manual mode 전환"
AUTO_TEXT = "Auto mode 전환"

VALID_CONTENTS = (NG_TEXT, OFF_TEXT, MANUAL_TEXT, AUTO_TEXT)

# 실시간 조건 (기존 동일)
THREAD_WORKERS = 2
LOOP_INTERVAL_SEC = 1
TS_WINDOW_SEC = 60

# tail-follow가 “최근 구간만” 처리하도록 안전 버퍼(기존의 QUERY_LOOKBACK_SEC 역할 대체)
# - 파일을 tail-follow 하므로 DB lookback은 불필요하지만,
#   EXE 재시작/파일이 갑자기 커진 경우 등을 대비해
#   "지금-120초 이전 이벤트"는 계산/저장 대상에서 자연스럽게 탈락하게끔 유지
LOOKBACK_SEC_SOFT = 120

# 로그 과다 방지
LOG_EVERY_LOOP = 10

# ============================================
# 1) 로그 파일 경로/패턴 (FCT 머신 로그 기준)
# ============================================
# 기존 FCT 머신 로그 수집기에서 사용하던 경로와 동일 계열로 맞춤
# 파일이 실제로 있는 위치에 맞춰 BASE_DIR만 정확히 유지하면 됩니다.
BASE_DIR = Path(r"\\192.168.108.155\FCT LogFile\Machine Log\FCT")

# 파일명 패턴 (기존 d1 수집기와 동일)
# 예: 20251230_FCT1_Machine_Log.txt / 20251230_PDI1_Machine_Log.txt (PDI도 FCT로 취급 가능)
FILENAME_PATTERN = re.compile(r"(\d{8})_(FCT|PDI)([1-4])_Machine_Log", re.IGNORECASE)

# 라인 패턴:
# - [hh:mm:ss] 또는 [hh:mm:ss.xx] 또는 [hh:mm:ss.xxxxxx] 허용
LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2})(?:\.(\d{1,6}))?\]\s*(.*)$")

# end_time 검증(완화): HH:MM:SS(.1~6)
END_TIME_REGEX = re.compile(r"^[0-2]\d:[0-5]\d:[0-5]\d(\.\d{1,6})?$")


# ============================================
# 공용: 로그/콘솔 유지
# ============================================
def log(msg: str):
    print(msg, flush=True)


def pause_console():
    try:
        input("\n[END] 작업이 종료되었습니다. 콘솔을 닫으려면 Enter를 누르세요...")
    except Exception:
        pass


# ============================================
# 2) DB 유틸 (기존과 동일)
# ============================================
def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]

    conn_str = (
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
        "?connect_timeout=5"
    )

    return create_engine(
        conn_str,
        pool_pre_ping=True,
        pool_size=3,
        max_overflow=3,
        pool_recycle=300,
    )


def get_psycopg2_conn(config=DB_CONFIG):
    return psycopg2.connect(
        host=config["host"],
        port=config["port"],
        dbname=config["dbname"],
        user=config["user"],
        password=config["password"],
        connect_timeout=5,
    )


# ============================================
# 3) 저장 테이블 준비 (기존과 동일)
# ============================================
def ensure_save_table():
    engine = get_engine(DB_CONFIG)
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (
        id            BIGSERIAL PRIMARY KEY,
        end_day       TEXT NOT NULL,
        station       TEXT NOT NULL,
        from_contents TEXT NOT NULL,
        from_time     TEXT NOT NULL,
        to_contents   TEXT NOT NULL,
        to_time       TEXT NOT NULL,
        wasted_time   NUMERIC(10,2) NOT NULL,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
    );
    """
    uniq_sql = f"""
    CREATE UNIQUE INDEX IF NOT EXISTS ux_{TABLE_SAVE_NAME}_dedup
    ON {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (end_day, station, from_time, to_time, from_contents, to_contents);
    """
    with engine.begin() as conn:
        conn.execute(sa_text(create_sql))
        conn.execute(sa_text(uniq_sql))


# ============================================
# 4) tail-follow 상태 구조
# ============================================
@dataclass
class StationState:
    in_manual: bool = False
    pending_ng_ts: datetime | None = None


# station별 상태
STATES: dict[str, StationState] = {
    "FCT1": StationState(),
    "FCT2": StationState(),
    "FCT3": StationState(),
    "FCT4": StationState(),
}

# tail-follow 오프셋: file_path -> last_byte_offset
FILE_OFFSETS: dict[str, int] = {}

# 파일별 "마지막 처리한 (end_day,end_time,contents)" (중복 라인/재시작 방어 보조)
LAST_EVENT_FINGERPRINT: dict[str, tuple[str, str, str]] = {}


# ============================================
# 5) 유틸: contents 정리 (DB 저장용 포맷 유지)
# ============================================
def _clean_contents(s: str) -> str:
    if s is None:
        return ""
    s = s.replace("\x00", "").strip()
    # 원본과 동일하게 핵심 키워드만 비교하므로, 과도한 정규화는 피함
    return s


def _normalize_end_time(hms: str, frac: str | None) -> str:
    """
    원본 로그가 [HH:MM:SS(.1~6)] 형태라서 그대로 사용하되,
    - frac이 있으면 그대로 두되 (최대 6자리)
    - 없으면 .00 붙여도 되지만, 기존 DB 적재(머신로그)와 호환 위해
      여기서는 "없으면 HH:MM:SS" 형태 유지하고, ts 파싱 시 %f 유연 처리.
    다만 출력(from_time/to_time)은 기존과 동일하게 "소수점 2자리"로 맞춘다.
    """
    if frac:
        frac2 = frac[:6]  # 최대 6자리
        return f"{hms}.{frac2}"
    return hms


def _ts_from_end_day_time(end_day_yyyymmdd: str, end_time: str) -> datetime | None:
    """
    end_day(YYYYMMDD) + end_time(HH:MM:SS(.ffffff)) -> datetime
    """
    if not re.fullmatch(r"\d{8}", str(end_day_yyyymmdd)):
        return None
    if not END_TIME_REGEX.fullmatch(str(end_time)):
        return None

    # pandas 없이 datetime만으로 안전 파싱
    try:
        # 소수점이 있으면 %f, 없으면 일반
        if "." in end_time:
            # %f는 1~6자리 허용이 아니므로 6자리로 패딩
            hms, frac = end_time.split(".", 1)
            frac6 = (frac + "000000")[:6]
            dt_str = f"{end_day_yyyymmdd} {hms}.{frac6}"
            return datetime.strptime(dt_str, "%Y%m%d %H:%M:%S.%f")
        else:
            dt_str = f"{end_day_yyyymmdd} {end_time}"
            return datetime.strptime(dt_str, "%Y%m%d %H:%M:%S")
    except Exception:
        return None


def _fmt_time_2dec(ts: datetime) -> str:
    """
    기존 코드와 동일하게 "소수점 2자리 수준"으로 절삭 출력
    - 기존: ts.strftime("%H:%M:%S.%f")[:-4]
    """
    return ts.strftime("%H:%M:%S.%f")[:-4]


# ============================================
# 6) 오늘 파일 찾기
# ============================================
def list_today_fct_log_files(today_ymd: str) -> dict[str, str]:
    """
    오늘자 FCT1~4 로그파일 경로를 찾아 station->path 반환
    - BASE_DIR 아래 YYYY/MM 구조를 전부 훑되, 오늘 파일만
    - PDI도 FCT로 취급(기존 머신로그 수집기와 동일 정책)
    """
    result: dict[str, str] = {}
    if not BASE_DIR.exists():
        return result

    # 성능: year/month만 2-depth 순회 (파일이 많지 않다는 전제)
    for year_dir in BASE_DIR.iterdir():
        if not (year_dir.is_dir() and year_dir.name.isdigit() and len(year_dir.name) == 4):
            continue
        for month_dir in year_dir.iterdir():
            if not (month_dir.is_dir() and month_dir.name.isdigit() and len(month_dir.name) == 2):
                continue
            for fp in month_dir.iterdir():
                if not fp.is_file():
                    continue
                m = FILENAME_PATTERN.search(fp.name)
                if not m:
                    continue
                file_ymd = m.group(1)
                no = m.group(3)
                station = f"FCT{no}"
                if file_ymd != today_ymd:
                    continue
                # station 1개만 필요. 같은 station 파일이 여러 개면 최신 mtime 우선
                fp_str = str(fp)
                if station not in result:
                    result[station] = fp_str
                else:
                    try:
                        if Path(fp_str).stat().st_mtime > Path(result[station]).stat().st_mtime:
                            result[station] = fp_str
                    except Exception:
                        pass

    return result


# ============================================
# 7) tail-follow: 파일에서 "새로 추가된 라인"만 읽기
# ============================================
def _open_text_file_best_effort(path: Path):
    """
    인코딩 방어: utf-8 시도 -> cp949
    """
    try:
        return path.open("r", encoding="utf-8", errors="ignore")
    except Exception:
        return path.open("r", encoding="cp949", errors="ignore")


def tail_read_new_lines(path_str: str) -> list[str]:
    """
    path_str 파일에서 마지막 오프셋 이후 새로 추가된 텍스트 라인만 반환.
    - 파일이 truncate/rotate 되어 size < offset이면 offset=0으로 리셋
    - EXE/Nuitka에서도 안전하게 동작하도록 예외 방어
    """
    p = Path(path_str)
    if not p.exists() or not p.is_file():
        return []

    try:
        size = p.stat().st_size
    except Exception:
        return []

    last_off = FILE_OFFSETS.get(path_str, 0)
    if size < last_off:
        # 로그 파일이 재생성/초기화 된 경우
        last_off = 0

    lines: list[str] = []
    try:
        with _open_text_file_best_effort(p) as f:
            f.seek(last_off)
            chunk = f.read()
            new_off = f.tell()

        if not chunk:
            FILE_OFFSETS[path_str] = new_off
            return []

        # 줄 단위로 분리
        # - 마지막이 개행 없이 끝나면 다음 루프에 이어 읽혀야 하므로 보수적으로 처리
        parts = chunk.splitlines()
        # splitlines()는 마지막 개행 여부에 관계없이 잘라주지만,
        # 마지막 라인이 "중간에 끊긴 라인"일 수 있어, 너무 공격적으로 버리진 않음.
        # (실제 장비 로그는 보통 한 줄씩 flush되므로 큰 문제 없음)
        lines.extend(parts)

        FILE_OFFSETS[path_str] = new_off
        return lines

    except Exception:
        return []


# ============================================
# 8) station별 이벤트 처리(state machine) + 결과 생성
# ============================================
def process_lines_for_station(station: str, end_day_ymd: str, file_path: str, lines: list[str], now_dt: datetime):
    """
    station의 새 라인을 받아서 상태를 업데이트하고,
    생성된 wasted_time 결과 row(dict)를 반환
    """
    out_rows = []

    st = STATES.get(station)
    if st is None:
        st = StationState()
        STATES[station] = st

    # soft lookback (재시작/오프셋 리셋 등에서 과거 이벤트가 들어오는 경우 탈락시키기 위함)
    soft_cutoff = now_dt - timedelta(seconds=LOOKBACK_SEC_SOFT)
    ts_filter_cutoff = now_dt - timedelta(seconds=TS_WINDOW_SEC)

    for raw in lines:
        if not raw:
            continue

        m = LINE_PATTERN.match(raw.strip())
        if not m:
            continue

        hms = m.group(1)
        frac = m.group(2) or None
        contents_raw = m.group(3) or ""
        contents = _clean_contents(contents_raw)

        # 관심 이벤트만
        if contents not in VALID_CONTENTS:
            continue

        end_time = _normalize_end_time(hms, frac)

        # 기본 유효성
        if not END_TIME_REGEX.fullmatch(end_time):
            continue

        ts = _ts_from_end_day_time(end_day_ymd, end_time)
        if ts is None:
            continue

        # soft lookback 이전이면 state도 건드리지 않음(재시작 시 과거 데이터로 상태 오염 방지)
        if ts < soft_cutoff:
            continue

        # 파일별 중복 라인 방어(같은 라인이 여러 번 들어오는 케이스)
        fp_key = file_path
        last_fp = LAST_EVENT_FINGERPRINT.get(fp_key)
        cur_fp = (end_day_ymd, end_time, contents)
        if last_fp == cur_fp:
            continue
        LAST_EVENT_FINGERPRINT[fp_key] = cur_fp

        # --- 상태기계 로직 (기존과 동일 의미) ---
        if contents == MANUAL_TEXT:
            st.in_manual = True
            # manual 진입 시 pending을 유지/삭제는 기존 코드상 명시가 없었으나
            # 현장 안정성 위해 pending을 비우는 게 안전(Manual 동안 NG 무시이므로)
            st.pending_ng_ts = None
            continue

        if contents == AUTO_TEXT:
            st.in_manual = False
            # auto 전환 시 pending은 초기화(이전 NG가 살아있으면 오탐 가능)
            st.pending_ng_ts = None
            continue

        if contents == NG_TEXT:
            if st.in_manual:
                continue
            if st.pending_ng_ts is None:
                st.pending_ng_ts = ts
            continue

        if contents == OFF_TEXT:
            if st.pending_ng_ts is None:
                continue

            from_ts = st.pending_ng_ts
            to_ts = ts
            st.pending_ng_ts = None

            # 저장 조건: from/to 모두 최근 60초 이내(ts_filter_cutoff 이상)
            if from_ts < ts_filter_cutoff or to_ts < ts_filter_cutoff:
                continue

            wasted = round(abs((to_ts - from_ts).total_seconds()), 2)

            out_rows.append({
                "end_day": str(end_day_ymd),
                "station": station,
                "from_contents": NG_TEXT,
                "from_time": _fmt_time_2dec(from_ts),
                "to_contents": OFF_TEXT,
                "to_time": _fmt_time_2dec(to_ts),
                "wasted_time": wasted,
            })

    return out_rows


# ============================================
# 9) 저장 (기존과 동일: execute_values + ON CONFLICT DO NOTHING)
# ============================================
def save_to_db_append_on_conflict(rows: list[dict]):
    if not rows:
        return 0

    cols = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time", "wasted_time"]
    values = [tuple(r[c] for c in cols) for r in rows]

    insert_sql = f"""
        INSERT INTO {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME}
        (end_day, station, from_contents, from_time, to_contents, to_time, wasted_time)
        VALUES %s
        ON CONFLICT (end_day, station, from_time, to_time, from_contents, to_contents)
        DO NOTHING
    """

    conn = None
    try:
        conn = get_psycopg2_conn(DB_CONFIG)
        conn.autocommit = False
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, values, page_size=1000)
        conn.commit()
        return len(values)
    finally:
        if conn is not None:
            conn.close()


# ============================================
# 10) worker (스레드): station 1개 처리
# ============================================
def worker_process_station(station: str, path_str: str, today_ymd: str, now_dt: datetime):
    # tail-read
    new_lines = tail_read_new_lines(path_str)
    if not new_lines:
        return station, 0, 0, []

    # 이벤트 처리
    rows = process_lines_for_station(
        station=station,
        end_day_ymd=today_ymd,
        file_path=path_str,
        lines=new_lines,
        now_dt=now_dt
    )
    return station, len(new_lines), len(rows), rows


# ============================================
# 11) main loop (1초 무한루프)
# ============================================
def run_realtime_loop():
    ensure_save_table()

    log("=" * 78)
    log(f"[START] AFA FAIL wasted time REALTIME (TAIL-FOLLOW) | {datetime.now():%Y-%m-%d %H:%M:%S}")
    log(f"[INFO] DB = {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}")
    log(f"[INFO] SAVE TABLE = {TABLE_SAVE_SCHEMA}.{TABLE_SAVE_NAME} (append + dedup)")
    log(f"[INFO] BASE_DIR = {BASE_DIR}")
    log(f"[INFO] THREAD_WORKERS = {THREAD_WORKERS}")
    log(f"[INFO] LOOP_INTERVAL_SEC = {LOOP_INTERVAL_SEC}")
    log(f"[INFO] TS_WINDOW_SEC = {TS_WINDOW_SEC} (from/to 모두 최근 60초 이내만 저장)")
    log(f"[INFO] SOFT_LOOKBACK_SEC = {LOOKBACK_SEC_SOFT} (재시작 시 과거 이벤트 상태오염 방지)")
    log("=" * 78)

    loop_i = 0
    last_day = datetime.now().strftime("%Y%m%d")

    while True:
        loop_i += 1
        loop_t0 = time.perf_counter()

        now_dt = datetime.now()
        today_ymd = now_dt.strftime("%Y%m%d")

        # 날짜 변경: 상태/오프셋 초기화
        if today_ymd != last_day:
            log(f"[DAY-CHANGE] {last_day} -> {today_ymd} | reset states/offsets")
            last_day = today_ymd
            FILE_OFFSETS.clear()
            LAST_EVENT_FINGERPRINT.clear()
            for k in list(STATES.keys()):
                STATES[k] = StationState()

        try:
            station_files = list_today_fct_log_files(today_ymd)
            if not station_files:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(f"[LOOP] no today files (today={today_ymd}) | BASE_DIR={BASE_DIR}")
                time.sleep(LOOP_INTERVAL_SEC)
                continue

            # FCT1~4만 대상으로 고정 (있으면 처리, 없으면 스킵)
            tasks = []
            for st in ("FCT1", "FCT2", "FCT3", "FCT4"):
                if st in station_files:
                    tasks.append((st, station_files[st]))

            if not tasks:
                if loop_i % LOG_EVERY_LOOP == 0:
                    log(f"[LOOP] today files found but no FCT1~4 matched (today={today_ymd})")
                time.sleep(LOOP_INTERVAL_SEC)
                continue

            all_rows = []
            total_new_lines = 0
            total_pairs = 0

            # 스레드 2개 고정 (기존 요구사항 동일)
            with ThreadPoolExecutor(max_workers=THREAD_WORKERS) as ex:
                futs = [ex.submit(worker_process_station, st, fp, today_ymd, now_dt) for st, fp in tasks]
                for f in as_completed(futs):
                    st, nlines, npairs, rows = f.result()
                    total_new_lines += nlines
                    total_pairs += npairs
                    if rows:
                        all_rows.extend(rows)

            inserted = 0
            if all_rows:
                inserted = save_to_db_append_on_conflict(all_rows)

            if loop_i % LOG_EVERY_LOOP == 0 or inserted > 0:
                log(f"[LOOP] files={len(tasks)} | new_lines={total_new_lines:,} | pairs={total_pairs:,} | inserted={inserted:,}")

        except KeyboardInterrupt:
            log("\n[STOP] 사용자 중단(Ctrl+C).")
            break
        except Exception as e:
            log("\n[ERROR] 루프 처리 중 예외가 발생했습니다. (continue)")
            log(f"  - {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()

        elapsed = time.perf_counter() - loop_t0
        time.sleep(max(0.0, LOOP_INTERVAL_SEC - elapsed))


def main():
    run_realtime_loop()


if __name__ == "__main__":
    freeze_support()
    try:
        main()
    except KeyboardInterrupt:
        log("\n[STOP] 사용자 중단(Ctrl+C).")
        pause_console()
    except Exception as e:
        log("\n[ERROR] 예외가 발생했습니다.")
        log(f"  - {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        pause_console()
        sys.exit(1)
