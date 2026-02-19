# -*- coding: utf-8 -*-
"""
total_demon_report_daemon.py

사양 확정본:
1) 감시 대상: k_demon_heath_check 스키마 내에서
   end_day/end_time/info/contents 컬럼이 있는 테이블/뷰만 자동 감시
2) 이벤트 누적: 상태가 바뀔 때만 INSERT (clean)
3) end_day/end_time: 원본 로그 테이블 최신 row의 발생시각
4) status: 정상/주의/비정상 3종 매핑
5) ensure_output_tables failed => 비정상
6) stall: 30초 이상 신규 로그 없음 => 비정상
7) 증분 PK: end_day,end_time,md5(info||contents)
8) loop: 단일 스레드, 5초, DB 끊김 무한 재접속(블로킹)
9) pool_size=1, PG_WORK_MEM 연결 시 SET
10) 재시작: DELETE/TRUNCATE 금지, bootstrap은 in-memory 초기화만 (이벤트 폭주 방지)
"""

from __future__ import annotations

import os
import time as time_mod
import signal
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple, List

# ---------- KST timezone (ZoneInfo fallback-safe) ----------
try:
    from zoneinfo import ZoneInfo
    try:
        KST = ZoneInfo("Asia/Seoul")
    except Exception:
        KST = timezone(timedelta(hours=9))
except Exception:
    KST = timezone(timedelta(hours=9))

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# Config
# =========================
SLEEP_SEC = 5
STALE_SEC = int(os.getenv("STALE_SEC", "30"))  # 30초 이상 새 로그 없음 => 비정상
DEFAULT_WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")

HEALTH_SCHEMA = "k_demon_heath_check"
REPORT_TABLE = "total_demon_report"

DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "leejangwoo1!"),
}

# =========================
# Logging (console only)
# =========================
def _now_kst() -> datetime:
    return datetime.now(tz=KST)

def _ts() -> str:
    return _now_kst().strftime("%Y-%m-%d %H:%M:%S")

def log_boot(msg: str) -> None:
    print(f"{_ts()} [BOOT] {msg}", flush=True)

def log_info(msg: str) -> None:
    print(f"{_ts()} [INFO] {msg}", flush=True)

def log_warn(msg: str) -> None:
    print(f"{_ts()} [WARN] {msg}", flush=True)

def log_retry(msg: str) -> None:
    print(f"{_ts()} [RETRY] {msg}", flush=True)

def log_down(msg: str) -> None:
    print(f"{_ts()} [DOWN] {msg}", flush=True)

def log_exc(prefix: str, e: BaseException) -> None:
    tb = traceback.format_exc()
    print(f"{_ts()} [ERROR] {prefix}: {type(e).__name__}: {e}\n{tb}", flush=True)

# =========================
# Engine (pool fixed=1, work_mem set)
# =========================
def make_engine() -> Engine:
    url = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    eng = create_engine(
        url,
        pool_size=1,
        max_overflow=0,
        pool_pre_ping=True,
        pool_recycle=1800,
        connect_args={
            "keepalives": 1,
            "keepalives_idle": 30,
            "keepalives_interval": 10,
            "keepalives_count": 3,
        },
    )

    @event.listens_for(eng, "connect")
    def _on_connect(dbapi_conn, _):
        try:
            cur = dbapi_conn.cursor()
            cur.execute(f"SET work_mem = '{DEFAULT_WORK_MEM}';")
            cur.close()
            dbapi_conn.commit()
        except Exception:
            pass

    return eng

def connect_blocking() -> Engine:
    log_boot("DB connect blocking loop start")
    log_info(
        f"DB target={DB_CONFIG['host']}:{DB_CONFIG['port']} db={DB_CONFIG['dbname']} "
        f"user={DB_CONFIG['user']} work_mem={DEFAULT_WORK_MEM}"
    )
    while True:
        try:
            eng = make_engine()
            with eng.begin() as conn:
                conn.exec_driver_sql("SELECT 1;")
            log_info("DB connected and ready (pool_size=1)")
            return eng
        except Exception as e:
            log_retry("DB connect failed; retrying in 5s")
            log_exc("DB connect failed", e)
            time_mod.sleep(SLEEP_SEC)

# =========================
# Ensure report table (NO DELETE/TRUNCATE)
# =========================
def ensure_report_table(engine: Engine) -> None:
    ddl_schema = text(f'CREATE SCHEMA IF NOT EXISTS "{HEALTH_SCHEMA}";')
    ddl_table = text(f"""
        CREATE TABLE IF NOT EXISTS "{HEALTH_SCHEMA}"."{REPORT_TABLE}" (
            end_day   text NOT NULL,
            end_time  text NOT NULL,
            log       text NOT NULL,
            status    text NOT NULL
        );
    """)
    idx = text(f"""
        CREATE INDEX IF NOT EXISTS ix_total_demon_report_log_time
        ON "{HEALTH_SCHEMA}"."{REPORT_TABLE}" (log, end_day, end_time);
    """)
    with engine.begin() as conn:
        conn.execute(ddl_schema)
        conn.execute(ddl_table)
        conn.execute(idx)

# =========================
# Discover targets: objects with 4 columns
# =========================
def discover_log_objects(conn) -> List[str]:
    sql = text("""
        WITH objs AS (
            SELECT table_name AS obj_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type IN ('BASE TABLE','VIEW')
        ),
        cols AS (
            SELECT table_name AS obj_name,
                   count(*) FILTER (WHERE column_name IN ('end_day','end_time','info','contents')) AS cnt4
            FROM information_schema.columns
            WHERE table_schema = :schema
            GROUP BY table_name
        )
        SELECT o.obj_name
        FROM objs o
        JOIN cols c ON c.obj_name = o.obj_name
        WHERE c.cnt4 = 4
        ORDER BY o.obj_name;
    """)
    rows = conn.execute(sql, {"schema": HEALTH_SCHEMA}).fetchall()
    return [r[0] for r in rows]

# =========================
# Status classification (3종)
# =========================
def classify(info: str, contents: str) -> str:
    i = (info or "").strip().lower()
    c = (contents or "").lower()

    # 비정상
    if i in ("down", "retry", "error"):
        return "비정상"
    if "ensure_output_tables failed" in c:
        return "비정상"
    if "db connect failed" in c or "operational error" in c or "reconnect" in c:
        return "비정상"
    if "traceback" in c or "loop error" in c:
        return "비정상"

    # 주의
    if i == "warn":
        return "주의"
    if "source row missing" in c:
        return "주의"

    # 그 외 정상
    return "정상"

def parse_end_ts(end_day: str, end_time: str) -> Optional[datetime]:
    try:
        dt = datetime.strptime(end_day + end_time, "%Y%m%d%H:%M:%S")
        return dt.replace(tzinfo=KST)
    except Exception:
        return None

# =========================
# Incremental PK: end_day,end_time,md5(info||contents)
# =========================
@dataclass
class LastPK:
    end_day: str
    end_time: str
    h: str

def _bootstrap_window_start(now: datetime) -> datetime:
    # 오늘 00:00:00 ~ now 범위
    return datetime(now.year, now.month, now.day, 0, 0, 0, tzinfo=KST)

def fetch_rows_incremental(conn, obj_name: str, now: datetime, last_pk: Optional[LastPK], limit: int = 5000):
    ws = _bootstrap_window_start(now)
    sd = ws.strftime("%Y%m%d")
    st = ws.strftime("%H:%M:%S")
    ed = now.strftime("%Y%m%d")
    et = now.strftime("%H:%M:%S")

    if last_pk is None:
        sql = text(f"""
            SELECT end_day, end_time, info, contents,
                   md5(coalesce(info,'') || '|' || coalesce(contents,'')) AS h
            FROM "{HEALTH_SCHEMA}"."{obj_name}"
            WHERE (end_day > :sd OR (end_day = :sd AND end_time >= :st))
              AND (end_day < :ed OR (end_day = :ed AND end_time <= :et))
            ORDER BY end_day, end_time, h
            LIMIT :lim
        """)
        return conn.execute(sql, {"sd": sd, "st": st, "ed": ed, "et": et, "lim": limit}).fetchall()

    sql = text(f"""
        SELECT end_day, end_time, info, contents,
               md5(coalesce(info,'') || '|' || coalesce(contents,'')) AS h
        FROM "{HEALTH_SCHEMA}"."{obj_name}"
        WHERE
            (
                end_day > :ld
                OR (end_day = :ld AND end_time > :lt)
                OR (end_day = :ld AND end_time = :lt AND md5(coalesce(info,'') || '|' || coalesce(contents,'')) > :lh)
            )
          AND (end_day < :ed OR (end_day = :ed AND end_time <= :et))
        ORDER BY end_day, end_time, h
        LIMIT :lim
    """)
    return conn.execute(
        sql,
        {"ld": last_pk.end_day, "lt": last_pk.end_time, "lh": last_pk.h, "ed": ed, "et": et, "lim": limit},
    ).fetchall()

# =========================
# Dedup cache
# =========================
class SeenPK:
    def __init__(self, maxlen: int = 50000):
        self.maxlen = maxlen
        self.q: List[str] = []
        self.s = set()

    def add(self, k: str) -> None:
        if k in self.s:
            return
        self.q.append(k)
        self.s.add(k)
        if len(self.q) > self.maxlen:
            old = self.q.pop(0)
            self.s.discard(old)

    def has(self, k: str) -> bool:
        return k in self.s

# =========================
# Report event insert
# - end_day/end_time: 원본 로그 최신 row 발생시각 사용
# - 상태 변경시에만 insert
# =========================
def insert_event(conn, end_day: str, end_time: str, log_name: str, status: str) -> None:
    sql = text(f"""
        INSERT INTO "{HEALTH_SCHEMA}"."{REPORT_TABLE}" (end_day, end_time, log, status)
        VALUES (:end_day, :end_time, :log, :status)
    """)
    conn.execute(sql, {"end_day": end_day, "end_time": end_time, "log": log_name, "status": status})

# =========================
# SIGTERM handling
# =========================
_STOP = False
def _handle_sigterm(signum, frame):
    global _STOP
    _STOP = True

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# =========================
# Main daemon
# =========================
def run_daemon():
    log_boot("total_demon_report daemon starting")
    engine = connect_blocking()
    ensure_report_table(engine)

    with engine.begin() as conn:
        targets = discover_log_objects(conn)
    log_info(f"discovered targets={len(targets)}")

    if not targets:
        log_warn("No targets found (no objects with end_day/end_time/info/contents). Sleeping...")
        while not _STOP:
            time_mod.sleep(SLEEP_SEC)
        return

    # per target state
    last_pk: Dict[str, Optional[LastPK]] = {t: None for t in targets}
    seen: Dict[str, SeenPK] = {t: SeenPK(maxlen=50000) for t in targets}
    last_seen_ts: Dict[str, Optional[datetime]] = {t: None for t in targets}
    last_seen_event_time: Dict[str, Optional[Tuple[str, str]]] = {t: None for t in targets}  # (end_day,end_time)
    last_status: Dict[str, Optional[str]] = {t: None for t in targets}
    last_reported_status: Dict[str, Optional[str]] = {t: None for t in targets}

    # BOOT bootstrap: 오늘 00:00~now 범위 스캔하여 in-memory만 세팅 (이벤트 폭주 방지)
    log_info("bootstrap start (in-memory only)")
    now = _now_kst()
    with engine.begin() as conn:
        for t in targets:
            rows = fetch_rows_incremental(conn, t, now, last_pk=None, limit=20000)

            lp = None
            lts = None
            levent = None
            ls = None

            for (ed, et, info, contents, h) in rows:
                pk = f"{ed}|{et}|{h}"
                if seen[t].has(pk):
                    continue
                seen[t].add(pk)

                ts = parse_end_ts(ed, et)
                if ts is not None:
                    lts = ts
                    levent = (ed, et)

                ls = classify(str(info), str(contents))
                lp = LastPK(ed, et, h)

            last_pk[t] = lp
            last_seen_ts[t] = lts
            last_seen_event_time[t] = levent
            last_status[t] = ls
            last_reported_status[t] = ls  # 부트스트랩을 "현재 상태 기준"으로 초기화

    log_info("bootstrap done")

    while not _STOP:
        try:
            now = _now_kst()

            # 자정 전환: in-memory reset + bootstrap(in-memory)
            if now.time().hour == 0 and now.time().minute == 0:
                log_info("midnight detected => reset in-memory and bootstrap")
                last_pk = {t: None for t in targets}
                seen = {t: SeenPK(maxlen=50000) for t in targets}
                last_seen_ts = {t: None for t in targets}
                last_seen_event_time = {t: None for t in targets}
                last_status = {t: None for t in targets}
                last_reported_status = {t: None for t in targets}

                with engine.begin() as conn:
                    for t in targets:
                        rows = fetch_rows_incremental(conn, t, now, last_pk=None, limit=20000)
                        lp = None
                        lts = None
                        levent = None
                        ls = None
                        for (ed, et, info, contents, h) in rows:
                            pk = f"{ed}|{et}|{h}"
                            if seen[t].has(pk):
                                continue
                            seen[t].add(pk)
                            ts = parse_end_ts(ed, et)
                            if ts is not None:
                                lts = ts
                                levent = (ed, et)
                            ls = classify(str(info), str(contents))
                            lp = LastPK(ed, et, h)
                        last_pk[t] = lp
                        last_seen_ts[t] = lts
                        last_seen_event_time[t] = levent
                        last_status[t] = ls
                        last_reported_status[t] = ls

            # 증분 루프
            with engine.begin() as conn:
                for t in targets:
                    lp = last_pk[t]
                    log_info(f"[last_pk] {t} last_pk={'None' if lp is None else (lp.end_day, lp.end_time, lp.h)}")

                    rows = fetch_rows_incremental(conn, t, now, lp, limit=5000)
                    log_info(f"[fetch] {t} new_rows={len(rows)}")

                    latest_event_ed = None
                    latest_event_et = None
                    latest_ts = last_seen_ts[t]

                    for (ed, et, info, contents, h) in rows:
                        pk = f"{ed}|{et}|{h}"
                        if seen[t].has(pk):
                            continue
                        seen[t].add(pk)

                        ts = parse_end_ts(ed, et)
                        if ts is not None:
                            latest_ts = ts
                            latest_event_ed = ed
                            latest_event_et = et

                        # 최신 1건 상태 결정
                        last_status[t] = classify(str(info), str(contents))
                        last_pk[t] = LastPK(ed, et, h)

                    # 최신 발생시각 갱신
                    last_seen_ts[t] = latest_ts
                    if latest_event_ed is not None and latest_event_et is not None:
                        last_seen_event_time[t] = (latest_event_ed, latest_event_et)

                    # stall 판정: 30초 이상 신규 로그 없음 => 비정상
                    stall = False
                    if latest_ts is None:
                        stall = True
                    else:
                        if (now - latest_ts).total_seconds() > STALE_SEC:
                            stall = True

                    final_status = last_status[t] or "비정상"
                    if stall:
                        final_status = "비정상"

                    prev_reported = last_reported_status[t]
                    if prev_reported != final_status:
                        # 이벤트 시각은 "원본 최신 row 발생시각"
                        le = last_seen_event_time[t]
                        if le is None:
                            # 원본 발생시각이 없으면(테이블 empty 등) 이벤트 생성 자체를 스킵(오탐 방지)
                            # 필요하면 여기서 강제로 now를 기록하도록 바꿀 수 있음.
                            log_warn(f"[insert] {t} status changed but no source timestamp => skip insert")
                        else:
                            ed, et = le
                            log_info(f"[insert] {t} {prev_reported} -> {final_status} @({ed} {et})")
                            insert_event(conn, ed, et, t, final_status)
                            last_reported_status[t] = final_status

            time_mod.sleep(SLEEP_SEC)

        except (OperationalError, DBAPIError) as e:
            log_down("DB operational error; reconnecting (blocking)")
            log_exc("DB error", e)
            time_mod.sleep(SLEEP_SEC)
            try:
                engine.dispose()
            except Exception:
                pass
            engine = connect_blocking()
            ensure_report_table(engine)

            # 재연결 후 대상 재탐색 (새 테이블 생길 수 있음)
            with engine.begin() as conn:
                new_targets = discover_log_objects(conn)
            if set(new_targets) != set(targets):
                log_info(f"targets changed: {len(targets)} -> {len(new_targets)} (re-init states)")
                targets = new_targets
                last_pk = {t: None for t in targets}
                seen = {t: SeenPK(maxlen=50000) for t in targets}
                last_seen_ts = {t: None for t in targets}
                last_seen_event_time = {t: None for t in targets}
                last_status = {t: None for t in targets}
                last_reported_status = {t: None for t in targets}

        except Exception as e:
            log_exc("loop error", e)
            time_mod.sleep(SLEEP_SEC)

    # graceful stop
    try:
        log_info("SIGTERM received => stopping")
        try:
            engine.dispose()
        except Exception:
            pass
    finally:
        log_info("stopped cleanly")

if __name__ == "__main__":
    run_daemon()
