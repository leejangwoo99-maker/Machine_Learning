# -*- coding: utf-8 -*-
"""
MES 불량 소요 시간 계산 (Vision1/Vision2) - Realtime Loop Version
✅ Tail-Follow + State Machine + MP=2 (요구사항 유지, 기존 로직 동일)

[원본 로직 유지]
- Vision1/Vision2 로그에서
  1) 교대 경계(08:20~08:30, 20:20~20:30) 제외
  2) 'MES 바코드 공정 불량' 연속 5회 이상 구간의 "첫 행"을 from_row로 채택 (조회 완료 전까지 1회만)
  3) 이후 첫 'MES 바코드 조회 완료'를 to_row로 매칭
  4) wasted_time=|to-from| (초), 600초 초과 제외
  5) 결과를 d1_machine_log.mes_fail_wasted_time 에 DROP→CREATE→INSERT

[추가 요구사항 5개 유지]
1) 1초마다 재실행 무한루프 (윈도우 안에서만)
2) 멀티프로세스 2개 고정  ✅ (그룹 처리 워커)
3) end_day = 오늘 기준 같은 월(YYYYMM)만 처리 ✅
4) 중복 제거(SELECT DISTINCT 효과) ✅ (입력/출력 모두 distinct)
5) 실시간: 현재 시간 기준 120초 이내 데이터만 처리 ✅

[추가 실행 타이밍 2회 유지]
- 08:27:00~08:29:59
- 20:27:00~20:29:59
(그 외 시간은 대기)

[구조 변경(성능 개선)]
- ❌ DB에서 매번 Vision1/Vision2를 read_sql로 대량 로딩 후 groupby
- ✅ Vision1/Vision2 "머신 로그 파일"을 tail-follow로 따라가며
  최근 120초 범위의 이벤트만 메모리 버퍼에 유지하고,
  윈도우 내 1초마다 "버퍼→그룹→MP 처리→DROP/CREATE/INSERT" 수행

[Nuitka/EXE 콘솔 강제 종료 방지(가능한 범위)]
- Ctrl+C / Ctrl+Break: 즉시 종료 대신 STOP_REQUESTED 플래그로 정상 종료 유도
- 콘솔 X 닫기/로그오프/시스템 종료 이벤트: 가능한 경우 STOP_REQUESTED로 유도
  (Windows 정책상 '완전 차단'은 불가할 수 있으나, 대부분 케이스에서 정상 종료 루트로 유도)
- 어떤 종료 경로에서도 pause_console()로 콘솔이 바로 닫히지 않게 유지
"""

import os
import re
import sys
import signal
import time as pytime
from dataclasses import dataclass
from datetime import datetime, time as dtime, timedelta
from pathlib import Path
import urllib.parse

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text
from multiprocessing import Pool, freeze_support


# ============================================
# 공용: 로그/콘솔 유지
# ============================================
def log(msg: str):
    print(msg, flush=True)


def pause_console():
    """
    EXE/Nuitka에서 콘솔이 즉시 닫히는 걸 막기 위한 '마지막 멈춤'.
    """
    try:
        input("\n[END] 작업이 종료되었습니다. 콘솔을 닫으려면 Enter를 누르세요...")
    except Exception:
        pass


# ============================================
# [Nuitka/EXE] 종료 신호 처리 (강제 종료 방지/정상 종료 유도)
# ============================================
STOP_REQUESTED = False


def _request_stop(reason: str = ""):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    if reason:
        log(f"[STOP-REQ] {reason}")


def _install_signal_handlers():
    # Ctrl+C
    try:
        signal.signal(signal.SIGINT, lambda s, f: _request_stop("SIGINT(Ctrl+C)"))
    except Exception:
        pass

    # Ctrl+Break (Windows)
    try:
        signal.signal(signal.SIGBREAK, lambda s, f: _request_stop("SIGBREAK(Ctrl+Break)"))
    except Exception:
        pass

    # (가능한 환경에서) SIGTERM
    try:
        signal.signal(signal.SIGTERM, lambda s, f: _request_stop("SIGTERM"))
    except Exception:
        pass


def _install_windows_console_ctrl_handler():
    """
    Windows 콘솔 닫기/로그오프/시스템종료 이벤트 처리.
    - Windows 정책상 완전 차단은 불가할 수 있습니다.
    - 가능한 경우 STOP_REQUESTED로 전환해 루프가 정상 종료되도록 유도합니다.
    """
    if os.name != "nt":
        return

    try:
        import ctypes

        CTRL_C_EVENT = 0
        CTRL_BREAK_EVENT = 1
        CTRL_CLOSE_EVENT = 2
        CTRL_LOGOFF_EVENT = 5
        CTRL_SHUTDOWN_EVENT = 6

        HandlerRoutine = ctypes.WINFUNCTYPE(ctypes.c_bool, ctypes.c_uint)

        def _handler(ctrl_type):
            if ctrl_type == CTRL_C_EVENT:
                _request_stop("CTRL_C_EVENT")
                return True
            if ctrl_type == CTRL_BREAK_EVENT:
                _request_stop("CTRL_BREAK_EVENT")
                return True
            if ctrl_type == CTRL_CLOSE_EVENT:
                _request_stop("CTRL_CLOSE_EVENT(콘솔닫기)")
                return True
            if ctrl_type == CTRL_LOGOFF_EVENT:
                _request_stop("CTRL_LOGOFF_EVENT")
                return True
            if ctrl_type == CTRL_SHUTDOWN_EVENT:
                _request_stop("CTRL_SHUTDOWN_EVENT")
                return True
            return False

        ctypes.windll.kernel32.SetConsoleCtrlHandler(HandlerRoutine(_handler), True)
    except Exception:
        pass


# ============================================
# [1] DB 설정
# ============================================
DB_CONFIG = {
    "host": "192.168.108.162",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "leejangwoo1!",
}

SCHEMA = "d1_machine_log"

OUT_TABLE_SCHEMA = "d1_machine_log"
OUT_TABLE_NAME   = "mes_fail_wasted_time"

# (2) 멀티프로세스 2개 고정
WORKERS = 2

# (5) 실시간 윈도우(초)
REALTIME_WINDOW_SEC = 120

# (선택) epoch cutoff 강제 적용하고 싶으면 float epoch 넣기 (없으면 None)
FORCE_CUTOFF_TS = None


def get_engine(config=DB_CONFIG):
    user = config["user"]
    password = urllib.parse.quote_plus(config["password"])
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    conn_str = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(conn_str)


# ============================================
# 실행 타이밍(추가)
# ============================================
RUN_WINDOWS = [
    (dtime(8, 27, 0),  dtime(8, 29, 59)),
    (dtime(20, 27, 0), dtime(20, 29, 59)),
]


def _now_time() -> dtime:
    return datetime.now().time().replace(microsecond=0)


def _is_in_run_window(t: dtime) -> bool:
    for start_t, end_t in RUN_WINDOWS:
        if start_t <= t <= end_t:
            return True
    return False


def _seconds_until_next_window(t: dtime) -> int:
    if _is_in_run_window(t):
        return 0

    now_sec = t.hour * 3600 + t.minute * 60 + t.second
    starts = []
    for start_t, _ in RUN_WINDOWS:
        s = start_t.hour * 3600 + start_t.minute * 60 + start_t.second
        starts.append(s)

    future = [s for s in starts if s > now_sec]
    if future:
        return min(future) - now_sec

    return (24 * 3600 - now_sec) + min(starts)


# ============================================
# [2] Tail-Follow 설정 (Vision 원본 파일)
# ============================================
BASE_DIR_VISION = Path(r"\\192.168.108.155\FCT LogFile\Machine Log")

VISION_FILE_PATTERN = re.compile(r"(\d{8})_Vision([12])_Machine_Log", re.IGNORECASE)

VISION_LINE_PATTERN = re.compile(r"^\[(\d{2}:\d{2}:\d{2}\.\d{2})\]\s*(.*)$")

FILE_OFFSETS: dict[str, int] = {}
LAST_FP: dict[str, tuple[str, str, str]] = {}


def _open_text_file_best_effort(path: Path):
    try:
        return path.open("r", encoding="utf-8", errors="ignore")
    except Exception:
        return path.open("r", encoding="cp949", errors="ignore")


def tail_read_new_lines(path_str: str) -> list[str]:
    p = Path(path_str)
    if not p.exists() or not p.is_file():
        return []

    try:
        size = p.stat().st_size
    except Exception:
        return []

    last_off = FILE_OFFSETS.get(path_str, 0)
    if size < last_off:
        last_off = 0

    try:
        with _open_text_file_best_effort(p) as f:
            f.seek(last_off)
            chunk = f.read()
            new_off = f.tell()

        FILE_OFFSETS[path_str] = new_off
        if not chunk:
            return []

        return chunk.splitlines()
    except Exception:
        return []


def _walk_fast(base: Path):
    """
    os.walk 래퍼 (예외 안전)
    """
    import os as _os
    try:
        for root, dirs, files in _os.walk(str(base)):
            yield root, dirs, files
    except Exception:
        return


def list_today_vision_files(today_ymd: str) -> dict[str, str]:
    """
    오늘자 Vision1/Vision2 파일 경로 탐색
    - 폴더 구조가 YYYY/MM 또는 YYYY/MM/DD 등 어떤 형태여도 전체 순회
    """
    out: dict[str, str] = {}
    if not BASE_DIR_VISION.exists():
        return out

    for root, dirs, files in _walk_fast(BASE_DIR_VISION):
        for fn in files:
            m = VISION_FILE_PATTERN.search(fn)
            if not m:
                continue
            ymd = m.group(1)
            no = m.group(2)
            if ymd != today_ymd:
                continue
            st = f"Vision{no}"
            full = str(Path(root) / fn)

            if st not in out:
                out[st] = full
            else:
                try:
                    if Path(full).stat().st_mtime > Path(out[st]).stat().st_mtime:
                        out[st] = full
                except Exception:
                    pass

    return out


# ============================================
# [3] 유틸 (기존 유지)
# ============================================
def time_to_seconds(series: pd.Series) -> pd.Series:
    return pd.to_timedelta(series.astype(str)).dt.total_seconds()


def secs_to_hhmmss_ss(sec: float) -> str:
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec - h * 3600 - m * 60
    return f"{h:02d}:{m:02d}:{s:05.2f}"


def filter_shift_boundary(df: pd.DataFrame) -> pd.DataFrame:
    """
    교대 경계(08:20~08:30, 20:20~20:30) 제거
    (변경: time -> end_time 기준)
    """
    df = df.copy()
    df["time_secs"] = pd.to_timedelta(df["end_time"].astype(str)).dt.total_seconds()

    start_0820 = 8 * 3600 + 20 * 60
    end_0830   = 8 * 3600 + 30 * 60
    start_2020 = 20 * 3600 + 20 * 60
    end_2030   = 20 * 3600 + 30 * 60

    mask_shift = ~(
        ((df["time_secs"] >= start_0820) & (df["time_secs"] <= end_0830)) |
        ((df["time_secs"] >= start_2020) & (df["time_secs"] <= end_2030))
    )
    df = df[mask_shift].copy()
    df = df.drop(columns=["time_secs"], errors="ignore")
    return df


# ============================================
# [4] 그룹 처리(멀티프로세스 워커) - 기존 그대로
# ============================================
def process_one_group(args):
    (station, end_day), g = args
    g = g.sort_values("end_time").reset_index(drop=True).copy()

    g["is_mes_ng"] = g["contents"].astype(str).str.contains("MES 바코드 공정 불량", na=False)
    g["is_done"]   = g["contents"].astype(str).str.contains("MES 바코드 조회 완료", na=False)

    used_before_done = False
    from_indices = []

    i = 0
    n = len(g)
    while i < n:
        if bool(g["is_done"].iloc[i]):
            used_before_done = False
            i += 1
            continue

        if bool(g["is_mes_ng"].iloc[i]):
            run_start = i
            j = i + 1
            while j < n and bool(g["is_mes_ng"].iloc[j]):
                j += 1
            run_len = j - run_start

            if run_len >= 5 and not used_before_done:
                from_indices.append(run_start)
                used_before_done = True

            i = j
        else:
            i += 1

    if not from_indices:
        return []

    from_rows = g.loc[from_indices, ["end_day", "station", "end_time", "contents"]].copy()
    from_rows = from_rows.rename(columns={"end_time": "from_time", "contents": "from_contents"})
    from_rows["from_key"] = time_to_seconds(from_rows["from_time"])

    done_rows = g[g["is_done"]][["end_time", "contents"]].copy()
    if done_rows.empty:
        return []

    done_rows = done_rows.rename(columns={"end_time": "to_time", "contents": "to_contents"})
    done_rows["to_key"] = time_to_seconds(done_rows["to_time"])
    done_rows = done_rows.sort_values("to_key").reset_index(drop=True)

    keys_done = done_rows["to_key"].to_numpy()
    keys_from = from_rows["from_key"].to_numpy()

    idx = np.searchsorted(keys_done, keys_from, side="left")
    mask = idx < len(keys_done)
    if not mask.any():
        return []

    from_valid = from_rows.loc[mask].reset_index(drop=True)
    done_valid = done_rows.iloc[idx[mask]].reset_index(drop=True)

    merged = pd.concat([from_valid, done_valid[["to_time", "to_key", "to_contents"]]], axis=1)

    merged["wasted_time"] = (merged["to_key"] - merged["from_key"]).abs().round(2)
    merged = merged[merged["wasted_time"] <= 600].copy()
    if merged.empty:
        return []

    merged["from_time"] = merged["from_key"].map(secs_to_hhmmss_ss)
    merged["to_time"]   = merged["to_key"].map(secs_to_hhmmss_ss)

    out = merged[[
        "end_day",
        "station",
        "from_contents",
        "from_time",
        "to_contents",
        "to_time",
        "wasted_time",
    ]].copy()

    return out.to_dict("records")


# ============================================
# [5] DB 출력 테이블 DROP→CREATE (기존 유지)
# ============================================
def recreate_out_table(engine):
    drop_sql = f"DROP TABLE IF EXISTS {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME};"
    create_sql = f"""
    CREATE TABLE {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (
        id            INTEGER,
        end_day       CHAR(8),
        station       TEXT,
        from_contents TEXT,
        from_time     TEXT,
        to_contents   TEXT,
        to_time       TEXT,
        wasted_time   NUMERIC(10,2)
    );
    """
    with engine.begin() as conn:
        conn.execute(text(drop_sql))
        conn.execute(text(create_sql))


# ============================================
# [6] Tail-Follow 버퍼(최근 120초 유지)
# ============================================
@dataclass
class EventRow:
    end_day: str
    station: str
    end_time: str
    contents: str
    ts_epoch: float


BUFFER: dict[str, list[EventRow]] = {
    "Vision1": [],
    "Vision2": [],
}


def _event_epoch(today_ymd: str, end_time_str: str) -> float | None:
    try:
        dt = datetime.strptime(f"{today_ymd} {end_time_str}", "%Y%m%d %H:%M:%S.%f")
        return dt.timestamp()
    except Exception:
        return None


def _append_events_from_lines(station: str, today_ymd: str, file_path: str, lines: list[str]):
    if not lines:
        return

    buf = BUFFER.get(station)
    if buf is None:
        buf = []
        BUFFER[station] = buf

    for raw in lines:
        m = VISION_LINE_PATTERN.match((raw or "").strip())
        if not m:
            continue

        end_time = m.group(1)
        contents = (m.group(2) or "").strip()

        # 파일별 동일 라인 반복 방어(보조)
        fp = file_path
        cur_fp = (today_ymd, end_time, contents)
        if LAST_FP.get(fp) == cur_fp:
            continue
        LAST_FP[fp] = cur_fp

        ep = _event_epoch(today_ymd, end_time)
        if ep is None:
            continue

        buf.append(EventRow(
            end_day=today_ymd,
            station=station,
            end_time=end_time,
            contents=contents,
            ts_epoch=ep
        ))


def _prune_buffer(now_epoch: float):
    cutoff = now_epoch - REALTIME_WINDOW_SEC
    for st, rows in list(BUFFER.items()):
        if not rows:
            continue
        BUFFER[st] = [r for r in rows if r.ts_epoch >= cutoff]


def _buffer_to_dataframe(today_ymd: str) -> pd.DataFrame:
    rows = []
    for st, lst in BUFFER.items():
        for r in lst:
            rows.append({
                "end_day": r.end_day,
                "end_time": r.end_time,
                "contents": r.contents,
                "station": st,
            })

    if not rows:
        return pd.DataFrame(columns=["end_day", "end_time", "contents", "station"])

    df = pd.DataFrame(rows)

    # (3) 오늘 기준 같은 월만 (YYYYMM%)
    yyyymm = datetime.now().strftime("%Y%m")
    df = df[df["end_day"].astype(str).str.startswith(yyyymm)].copy()

    # (4) DISTINCT 효과
    df = df.drop_duplicates(subset=["end_day", "station", "end_time", "contents"], keep="first").reset_index(drop=True)

    return df


# ============================================
# [7] 단일 실행(루프에서 호출) - Tail-Follow 버전
# ============================================
def run_once(engine):
    now = datetime.now()
    today_ymd = now.strftime("%Y%m%d")
    now_epoch = now.timestamp()

    if FORCE_CUTOFF_TS is not None:
        now_epoch = float(FORCE_CUTOFF_TS) + REALTIME_WINDOW_SEC

    station_files = list_today_vision_files(today_ymd)
    v1 = station_files.get("Vision1")
    v2 = station_files.get("Vision2")

    if not v1 and not v2:
        recreate_out_table(engine)
        log(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows=0)")
        return

    if v1:
        lines = tail_read_new_lines(v1)
        _append_events_from_lines("Vision1", today_ymd, v1, lines)
    if v2:
        lines = tail_read_new_lines(v2)
        _append_events_from_lines("Vision2", today_ymd, v2, lines)

    _prune_buffer(now_epoch)

    df = _buffer_to_dataframe(today_ymd)
    if df.empty:
        recreate_out_table(engine)
        log(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows=0)")
        return

    df = filter_shift_boundary(df)
    df = df.sort_values(["station", "end_day", "end_time"]).reset_index(drop=True)

    group_items = [((st, day), g.copy()) for (st, day), g in df.groupby(["station", "end_day"], sort=False)]
    with Pool(processes=WORKERS) as pool:
        results = pool.map(process_one_group, group_items)

    flat_rows = [r for sub in results for r in sub]

    if not flat_rows:
        recreate_out_table(engine)
        log(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows=0)")
        return

    result_df = pd.DataFrame(flat_rows)

    result_df["end_day"] = (
        result_df["end_day"].astype(str)
        .str.replace(",", "", regex=False)
        .str.zfill(8)
    )

    distinct_cols = ["end_day", "station", "from_contents", "from_time", "to_contents", "to_time"]
    result_df = result_df.drop_duplicates(subset=distinct_cols, keep="first").reset_index(drop=True)
    result_df.insert(0, "id", result_df.index + 1)

    recreate_out_table(engine)
    result_df.to_sql(
        OUT_TABLE_NAME,
        engine,
        schema=OUT_TABLE_SCHEMA,
        if_exists="append",
        index=False
    )

    log(f"[DONE] saved: {OUT_TABLE_SCHEMA}.{OUT_TABLE_NAME} (rows={len(result_df)})")


# ============================================
# [8] main: 타임윈도우 기반 1초 루프 (기존 유지)
# ============================================
def main_loop():
    global STOP_REQUESTED

    engine = get_engine()
    log("[INFO] MES fail wasted time scheduled realtime loop start")
    log(f"[INFO] workers={WORKERS}, realtime_window={REALTIME_WINDOW_SEC}s, force_cutoff_ts={FORCE_CUTOFF_TS}")
    log(f"[INFO] run_windows = {RUN_WINDOWS}")
    log(f"[INFO] tail-follow BASE_DIR_VISION = {str(BASE_DIR_VISION)}")
    log("[INFO] 종료: Ctrl+C 또는 Ctrl+Break 를 누르면 '정상 종료'로 빠집니다. (콘솔 유지됨)")

    while not STOP_REQUESTED:
        now_t = _now_time()

        if not _is_in_run_window(now_t):
            wait_sec = _seconds_until_next_window(now_t)
            log(f"[WAIT] now={now_t} -> next window in {wait_sec}s")

            while wait_sec > 0 and not STOP_REQUESTED:
                pytime.sleep(1)
                wait_sec -= 1
                now_t = _now_time()
                if _is_in_run_window(now_t):
                    break
            continue

        try:
            run_once(engine)
        except Exception as e:
            log("[ERROR] " + repr(e))

        # 윈도우 안에서 1초 템포
        for _ in range(1):
            if STOP_REQUESTED:
                break
            pytime.sleep(1)

    log("[STOP] 정상 종료 루트로 루프를 종료합니다.")


def main():
    _install_signal_handlers()
    _install_windows_console_ctrl_handler()
    main_loop()


if __name__ == "__main__":
    freeze_support()
    try:
        main()
    except Exception as e:
        log("\n[ERROR] 예외가 발생했습니다.")
        log(f"  - {type(e).__name__}: {e}")
        import traceback
        traceback.print_exc()
        pause_console()
        sys.exit(1)
    else:
        pause_console()
