# -*- coding: utf-8 -*-
"""
total_demon_report_daemon.py

- pool_size=1 고정 환경 대응(단일 커넥션으로 SELECT+UPSERT)
- 콘솔 로그 파일 저장: C:\\AptivAgent\\t (일자별 롤링)
- report 테이블: log_desc/apply_machine 컬럼 자동 추가 + 고정값 UPSERT
- ✅ 하트비트: 1분당 1줄 출력
"""

from __future__ import annotations

import os
import sys
import io
import time as time_mod
import signal
import traceback
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional, Tuple, List

# ---------- KST timezone ----------
try:
    from zoneinfo import ZoneInfo
    try:
        KST = ZoneInfo("Asia/Seoul")
    except Exception:
        KST = timezone(timedelta(hours=9))
except Exception:
    KST = timezone(timedelta(hours=9))

from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.exc import OperationalError, DBAPIError

# =========================
# Config
# =========================
SLEEP_SEC = float(os.getenv("SLEEP_SEC", "2"))
RR_BATCH = int(os.getenv("RR_BATCH", "8"))

# ✅ heartbeat (1분 1줄)
HEARTBEAT_SEC = int(os.getenv("HEARTBEAT_SEC", "60"))

STALL_WARN_SEC = int(os.getenv("STALL_WARN_SEC", "300"))
STALL_DOWN_SEC = int(os.getenv("STALL_DOWN_SEC", "400"))
STALL_IDLE_SEC = int(os.getenv("STALL_IDLE_SEC", "500"))

DEFAULT_WORK_MEM = os.getenv("PG_WORK_MEM", "4MB")
DB_STMT_TIMEOUT_MS = int(os.getenv("DB_STMT_TIMEOUT_MS", "8000"))
DB_LOCK_TIMEOUT_MS = int(os.getenv("DB_LOCK_TIMEOUT_MS", "1500"))

HEALTH_SCHEMA_PRIMARY = os.getenv("HEALTH_SCHEMA_PRIMARY", "k_demon_health_check")
HEALTH_SCHEMA_FALLBACK = os.getenv("HEALTH_SCHEMA_FALLBACK", "k_demon_heath_check")

REPORT_LATEST_TABLE = os.getenv("REPORT_LATEST_TABLE", "total_demon_report")

DB_CONFIG = {
    "host": os.getenv("PGHOST", "100.105.75.47"),
    "port": int(os.getenv("PGPORT", "5432")),
    "dbname": os.getenv("PGDATABASE", "postgres"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", ""),
}

LOG_DIR = os.getenv("DAEMON_LOG_DIR", r"C:\AptivAgent\t")
LOG_PREFIX = os.getenv("DAEMON_LOG_PREFIX", "total_demon_report_daemon")

# log -> (log_desc, apply_machine)
LOG_META: Dict[str, Tuple[str, str]] = {
    "a1_log": ("테스트로그 MES 히스토리", "Main PC"),
    "a2_log": ("FCT 시험 항목 별 로그", "Main PC"),
    "a3_log": ("Vison 시험 항목 별 로그", "Main PC"),
    "c1_log": ("FCT 시험 관련 상세 사항", "FCT4"),
    "d1_log": ("FCT 머신 로그", "FCT2"),
    "d2_log": ("Vision 머신 로그", "FCT2"),
    "d3_log": ("PLC 머신 로그", "FCT2"),
    "d4_log": ("조립 불량 시간", "FCT3"),
    "d5_log": ("Vision에서의 MES 불량 시간", "FCT2"),
    "d6_log": ("FCT에서의 MES 불량 시간", "FCT2"),
    "e1_1_log": ("FCT operation 시간", "FCT1"),
    "e1_2_log": ("FCT run 시간", "FCT2"),
    "e2_1_log": ("Vision operation 시간", "FCT3"),
    "e2_2_log": ("Vision run 시간", "FCT4"),
    "e5_log": ("PD 보드 체크", "FCT1"),
    "e6_log": ("Non-PD 시험 지연 리스트", "FCT2"),
    "e7_log": ("PD 시험 지연 리스트", "FCT3"),
    "f_log": ("FCT 종합 데이터", "Server PC"),
    "gc_log": ("FCT operation 시간 기준", "FCT3"),
    "gf_log": ("FCT 비가동 집계", "FCT3"),
    "gv_log": ("Vision 비가동 집계", "FCT4"),
    "h_log": ("스페어파트 교체 타이밍", "FCT1"),
    "1_log": ("생산 수량", "FCT1"),
    "2_log": ("시험 합격률", "FCT1"),
    "3_log": ("FCT 불합격 항목 리스트", "FCT1"),
    "4_log": ("Vision 불합격 항목 리스트", "FCT1"),
    "5_log": ("마스터 샘플 테스트 체크", "FCT3"),
    "6_log": ("FCT 시험 시간 지연 리스트", "FCT3"),
    "7_log": ("총 조립 불량 시간", "FCT4"),
    "8_log": ("총 MES 불량 시간", "FCT4"),
    "9_log": ("총 계획 및 비가동 시간", "FCT4"),
    "10_log": ("제품 사양 변경 순간", "FCT2"),
    "11_log": ("OEE 계산", "FCT4"),
    "total_non_operation": ("총 비가동 집계", "FCT2"),
}

def meta_for_log(log_name: str) -> Tuple[str, str]:
    return LOG_META.get(log_name, ("", ""))

# =========================
# Logging (console + file tee)
# =========================
def _now_kst() -> datetime:
    return datetime.now(tz=KST)

def _ts() -> str:
    return _now_kst().strftime("%Y-%m-%d %H:%M:%S")

class DailyFileTee(io.TextIOBase):
    def __init__(self, base_dir: str, prefix: str, console_stream):
        super().__init__()
        self.base_dir = base_dir
        self.prefix = prefix
        self.console = console_stream
        self._current_day = None
        self._fh = None
        try:
            os.makedirs(self.base_dir, exist_ok=True)
        except Exception:
            pass

    def _path_for_day(self, yyyymmdd: str) -> str:
        return os.path.join(self.base_dir, f"{self.prefix}_{yyyymmdd}.log")

    def _open_if_needed(self):
        day = _now_kst().strftime("%Y%m%d")
        if self._current_day == day and self._fh is not None:
            return
        try:
            if self._fh is not None:
                self._fh.flush()
                self._fh.close()
        except Exception:
            pass
        self._current_day = day
        try:
            os.makedirs(self.base_dir, exist_ok=True)
            self._fh = open(self._path_for_day(day), "a", encoding="utf-8", buffering=1)
        except Exception:
            self._fh = None

    def write(self, s: str) -> int:
        if not s:
            return 0
        try:
            self.console.write(s)
            self.console.flush()
        except Exception:
            pass
        try:
            self._open_if_needed()
            if self._fh is not None:
                self._fh.write(s)
                self._fh.flush()
        except Exception:
            pass
        return len(s)

def setup_logging():
    sys.stdout = DailyFileTee(LOG_DIR, LOG_PREFIX, sys.__stdout__)
    sys.stderr = DailyFileTee(LOG_DIR, LOG_PREFIX, sys.__stderr__)
    print(f"{_ts()} [BOOT] log_dir={LOG_DIR} prefix={LOG_PREFIX}", flush=True)

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
# Engine (pool_size=1 유지)
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
            cur.execute(f"SET statement_timeout = '{DB_STMT_TIMEOUT_MS}ms';")
            cur.execute(f"SET lock_timeout = '{DB_LOCK_TIMEOUT_MS}ms';")
            cur.close()
            dbapi_conn.commit()
        except Exception:
            pass

    return eng

def connect_blocking() -> Engine:
    log_boot("DB connect blocking loop start")
    log_info(
        f"DB target={DB_CONFIG['host']}:{DB_CONFIG['port']} db={DB_CONFIG['dbname']} "
        f"user={DB_CONFIG['user']} work_mem={DEFAULT_WORK_MEM} stmt_timeout={DB_STMT_TIMEOUT_MS}ms"
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
            time_mod.sleep(5)

def _sel_conn(engine: Engine) -> Connection:
    return engine.connect().execution_options(isolation_level="AUTOCOMMIT")

# =========================
# Schema + ensure
# =========================
def choose_health_schema(conn) -> str:
    sql = text("""
        SELECT EXISTS(
            SELECT 1
            FROM information_schema.schemata
            WHERE schema_name = :s
        );
    """)
    if conn.execute(sql, {"s": HEALTH_SCHEMA_PRIMARY}).scalar():
        return HEALTH_SCHEMA_PRIMARY
    if conn.execute(sql, {"s": HEALTH_SCHEMA_FALLBACK}).scalar():
        return HEALTH_SCHEMA_FALLBACK
    return HEALTH_SCHEMA_PRIMARY

def ensure_report_table(engine: Engine, health_schema: str) -> None:
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{health_schema}";'))
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{health_schema}"."{REPORT_LATEST_TABLE}" (
                log           text,
                end_day       text,
                end_time      text,
                status        text,
                log_desc      text,
                apply_machine text,
                updated_ts    timestamptz NOT NULL DEFAULT now()
            );
        """))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS log text;'))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS end_day text;'))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS end_time text;'))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS status text;'))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS log_desc text;'))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS apply_machine text;'))
        conn.execute(text(f'ALTER TABLE "{health_schema}"."{REPORT_LATEST_TABLE}" ADD COLUMN IF NOT EXISTS updated_ts timestamptz NOT NULL DEFAULT now();'))
        conn.execute(text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS ux_total_demon_report_log
            ON "{health_schema}"."{REPORT_LATEST_TABLE}" (log);
        """))

# =========================
# Discover targets
# =========================
@dataclass(frozen=True)
class TargetSpec:
    name: str
    mode: str  # STD / LOGTS

def discover_targets(conn, health_schema: str) -> List[TargetSpec]:
    sql = text("""
        WITH objs AS (
            SELECT table_name AS obj_name
            FROM information_schema.tables
            WHERE table_schema = :schema
              AND table_type IN ('BASE TABLE','VIEW')
        ),
        cols AS (
            SELECT table_name AS obj_name,
                   bool_or(column_name = 'end_day')   AS has_end_day,
                   bool_or(column_name = 'end_time')  AS has_end_time,
                   bool_or(column_name = 'info')      AS has_info,
                   bool_or(column_name = 'contents')  AS has_contents,
                   bool_or(column_name = 'log_ts')    AS has_log_ts,
                   bool_or(column_name = 'message')   AS has_message
            FROM information_schema.columns
            WHERE table_schema = :schema
            GROUP BY table_name
        )
        SELECT o.obj_name,
               CASE
                   WHEN (c.has_end_day AND c.has_end_time AND c.has_info AND c.has_contents) THEN 'STD'
                   WHEN (c.has_log_ts AND c.has_message) THEN 'LOGTS'
                   ELSE NULL
               END AS mode
        FROM objs o
        JOIN cols c ON c.obj_name = o.obj_name
        WHERE
            (c.has_end_day AND c.has_end_time AND c.has_info AND c.has_contents)
            OR
            (c.has_log_ts AND c.has_message)
        ORDER BY o.obj_name;
    """)
    rows = conn.execute(sql, {"schema": health_schema}).fetchall()
    out: List[TargetSpec] = []
    for (name, mode) in rows:
        if mode in ("STD", "LOGTS"):
            out.append(TargetSpec(str(name), str(mode)))
    return out

# =========================
# Status
# =========================
def classify(info: str, contents: str) -> str:
    i = (info or "").strip().lower()
    c = (contents or "").lower()

    if i in ("down", "retry", "error"):
        return "비정상"
    if "ensure_output_tables failed" in c:
        return "비정상"
    if "db connect failed" in c or "operational error" in c or "reconnect" in c:
        return "비정상"
    if "traceback" in c or "loop error" in c:
        return "비정상"

    if i == "warn":
        return "주의"
    if "source row missing" in c:
        return "주의"

    return "정상"

def parse_end_ts(end_day: str, end_time: str) -> Optional[datetime]:
    try:
        dt = datetime.strptime(end_day + end_time, "%Y%m%d%H:%M:%S")
        return dt.replace(tzinfo=KST)
    except Exception:
        return None

SEVERITY = {"정상": 0, "주의": 1, "비정상": 2, "비운행": 3}

def time_based_status(age_sec: Optional[float]) -> str:
    if age_sec is None:
        return "비정상"
    if age_sec >= float(STALL_IDLE_SEC):
        return "비운행"
    if age_sec >= float(STALL_DOWN_SEC):
        return "비정상"
    if age_sec >= float(STALL_WARN_SEC):
        return "주의"
    return "정상"

def max_severity(a: str, b: str) -> str:
    return a if SEVERITY.get(a, 2) >= SEVERITY.get(b, 2) else b

# =========================
# PK
# =========================
@dataclass
class LastPK:
    k1: str
    k2: str
    h: str

# =========================
# Queries
# =========================
def std_latest_row(sel: Connection, schema: str, obj: str):
    return sel.execute(text(f"""
        SELECT end_day, end_time, info, contents,
               md5(coalesce(info,'') || '|' || coalesce(contents,'')) AS h
        FROM "{schema}"."{obj}"
        ORDER BY end_day DESC, end_time DESC, md5(coalesce(info,'') || '|' || coalesce(contents,'')) DESC
        LIMIT 1
    """)).fetchone()

def logts_latest_row(sel: Connection, schema: str, obj: str):
    return sel.execute(text(f"""
        SELECT log_ts, level, phase, src, message,
               md5(coalesce(level,'') || '|' || coalesce(phase,'') || '|' || coalesce(src,'') || '|' || coalesce(message,'')) AS h
        FROM "{schema}"."{obj}"
        ORDER BY log_ts DESC,
                 md5(coalesce(level,'') || '|' || coalesce(phase,'') || '|' || coalesce(src,'') || '|' || coalesce(message,'')) DESC
        LIMIT 1
    """)).fetchone()

def fetch_rows_incremental_std(sel: Connection, schema: str, obj: str, last_pk: LastPK, limit: int = 2000):
    return sel.execute(text(f"""
        SELECT end_day, end_time, info, contents,
               md5(coalesce(info,'') || '|' || coalesce(contents,'')) AS h
        FROM "{schema}"."{obj}"
        WHERE
            (
                end_day > :ld
                OR (end_day = :ld AND end_time > :lt)
                OR (end_day = :ld AND end_time = :lt
                    AND md5(coalesce(info,'') || '|' || coalesce(contents,'')) > :lh)
            )
        ORDER BY end_day, end_time, h
        LIMIT :lim
    """), {"ld": last_pk.k1, "lt": last_pk.k2, "lh": last_pk.h, "lim": limit}).fetchall()

def fetch_rows_incremental_logts(sel: Connection, schema: str, obj: str, last_pk: LastPK, limit: int = 2000):
    try:
        lts = datetime.fromisoformat(last_pk.k1)
        if lts.tzinfo is None:
            lts = lts.replace(tzinfo=KST)
    except Exception:
        lts = datetime(1970, 1, 1, tzinfo=KST)

    return sel.execute(text(f"""
        SELECT log_ts, level, phase, src, message,
               md5(coalesce(level,'') || '|' || coalesce(phase,'') || '|' || coalesce(src,'') || '|' || coalesce(message,'')) AS h
        FROM "{schema}"."{obj}"
        WHERE
            (
                log_ts > :lts
                OR (log_ts = :lts
                    AND md5(coalesce(level,'') || '|' || coalesce(phase,'') || '|' || coalesce(src,'') || '|' || coalesce(message,'')) > :lh)
            )
        ORDER BY log_ts, h
        LIMIT :lim
    """), {"lts": lts, "lh": last_pk.h, "lim": limit}).fetchall()

def upsert_latest(sel: Connection, schema: str, end_day: str, end_time: str, log_name: str, status: str) -> None:
    log_desc, apply_machine = meta_for_log(log_name)
    sel.execute(text(f"""
        INSERT INTO "{schema}"."{REPORT_LATEST_TABLE}"
            (log, end_day, end_time, status, log_desc, apply_machine, updated_ts)
        VALUES
            (:log, :end_day, :end_time, :status, :log_desc, :apply_machine, now())
        ON CONFLICT (log) DO UPDATE SET
            end_day       = EXCLUDED.end_day,
            end_time      = EXCLUDED.end_time,
            status        = EXCLUDED.status,
            log_desc      = EXCLUDED.log_desc,
            apply_machine = EXCLUDED.apply_machine,
            updated_ts    = now();
    """), {
        "log": log_name,
        "end_day": end_day,
        "end_time": end_time,
        "status": status,
        "log_desc": log_desc,
        "apply_machine": apply_machine,
    })

# =========================
# SIGTERM
# =========================
_STOP = False
def _handle_sigterm(signum, frame):
    global _STOP
    _STOP = True

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# =========================
# Main
# =========================
def run_daemon():
    setup_logging()
    log_boot("total_demon_report daemon starting")

    engine = connect_blocking()

    with engine.begin() as conn:
        health_schema = choose_health_schema(conn)
    log_info(f"health_schema selected = {health_schema}")

    ensure_report_table(engine, health_schema)

    with engine.begin() as conn:
        targets = discover_targets(conn, health_schema)
    log_info(f"discovered targets={len(targets)}")
    log_info("targets=" + ", ".join([f"{t.name}:{t.mode}" for t in targets]))

    if not targets:
        log_warn("No targets found. Sleeping...")
        while not _STOP:
            time_mod.sleep(5)
        return

    last_pk: Dict[str, Optional[LastPK]] = {t.name: None for t in targets}
    last_seen_ts: Dict[str, Optional[datetime]] = {t.name: None for t in targets}
    last_seen_event_time: Dict[str, Optional[Tuple[str, str]]] = {t.name: None for t in targets}
    last_status: Dict[str, Optional[str]] = {t.name: None for t in targets}
    last_reported_status: Dict[str, Optional[str]] = {t.name: None for t in targets}

    rr_idx = 0
    loop_ticks = 0
    last_hb_ts = time_mod.time()

    # bootstrap
    log_info("bootstrap start (LATEST ONLY)")
    sel: Optional[Connection] = None
    try:
        sel = _sel_conn(engine)
        now = _now_kst()

        for spec in targets:
            try:
                if spec.mode == "STD":
                    row = std_latest_row(sel, health_schema, spec.name)
                    if row is None:
                        continue
                    ed, et, info, contents, h = row
                    ts = parse_end_ts(str(ed), str(et))
                    if ts is not None:
                        last_seen_ts[spec.name] = ts
                        last_seen_event_time[spec.name] = (str(ed), str(et))
                    last_status[spec.name] = classify(str(info), str(contents))
                    last_pk[spec.name] = LastPK(str(ed), str(et), str(h))
                else:
                    row = logts_latest_row(sel, health_schema, spec.name)
                    if row is None:
                        continue
                    log_ts, level, phase, src, message, h = row
                    if not isinstance(log_ts, datetime):
                        continue
                    lts = log_ts.astimezone(KST) if log_ts.tzinfo else log_ts.replace(tzinfo=KST)
                    last_seen_ts[spec.name] = lts
                    ed = lts.strftime("%Y%m%d")
                    et = lts.strftime("%H:%M:%S")
                    last_seen_event_time[spec.name] = (ed, et)
                    last_status[spec.name] = classify(str(level), str(message))
                    last_pk[spec.name] = LastPK(lts.isoformat(), "", str(h))

                latest_ts = last_seen_ts.get(spec.name)
                age_sec = None
                if latest_ts is not None:
                    age_sec = max(0.0, (now - latest_ts).total_seconds())
                base = last_status.get(spec.name) or "비정상"
                final_status = max_severity(base, time_based_status(age_sec))
                last_reported_status[spec.name] = final_status

                le = last_seen_event_time.get(spec.name)
                if le is not None:
                    upsert_latest(sel, health_schema, le[0], le[1], spec.name, final_status)

            except (OperationalError, DBAPIError) as e:
                log_exc(f"bootstrap select/upsert failed => reset sel: {spec.name}", e)
                try:
                    sel.close()
                except Exception:
                    pass
                sel = _sel_conn(engine)

            except Exception as e:
                log_exc(f"bootstrap skip: {spec.name}", e)

    finally:
        try:
            if sel is not None:
                sel.close()
        except Exception:
            pass

    log_info("bootstrap done")

    # loop
    sel = None
    while not _STOP:
        try:
            if sel is None:
                sel = _sel_conn(engine)

            now = _now_kst()
            n = len(targets)

            # ✅ heartbeat: 1분당 1줄
            now_s = time_mod.time()
            if now_s - last_hb_ts >= float(HEARTBEAT_SEC):
                last_hb_ts = now_s
                # 최근 업데이트(log_ts 기준) 하나만 보여주기
                latest_log = ""
                latest_age = None
                try:
                    # 가장 최근 last_seen_ts 찾기
                    best_name = None
                    best_ts = None
                    for k, v in last_seen_ts.items():
                        if v is None:
                            continue
                        if best_ts is None or v > best_ts:
                            best_ts = v
                            best_name = k
                    if best_name is not None and best_ts is not None:
                        latest_log = best_name
                        latest_age = max(0.0, (now - best_ts).total_seconds())
                except Exception:
                    pass

                log_info(
                    f"[HEARTBEAT] alive tick={loop_ticks} rr_idx={rr_idx} "
                    f"targets={n} latest_log={latest_log} latest_age_sec={latest_age}"
                )

            for _ in range(min(RR_BATCH, n)):
                spec = targets[rr_idx % n]
                rr_idx += 1
                name = spec.name

                try:
                    lp = last_pk.get(name)

                    if lp is None:
                        row = std_latest_row(sel, health_schema, name) if spec.mode == "STD" else logts_latest_row(sel, health_schema, name)
                        if row is None:
                            continue
                        if spec.mode == "STD":
                            ed, et, info, contents, h = row
                            ts = parse_end_ts(str(ed), str(et))
                            if ts is not None:
                                last_seen_ts[name] = ts
                                last_seen_event_time[name] = (str(ed), str(et))
                            last_status[name] = classify(str(info), str(contents))
                            last_pk[name] = LastPK(str(ed), str(et), str(h))
                        else:
                            log_ts, level, phase, src, message, h = row
                            if isinstance(log_ts, datetime):
                                lts = log_ts.astimezone(KST) if log_ts.tzinfo else log_ts.replace(tzinfo=KST)
                                last_seen_ts[name] = lts
                                ed = lts.strftime("%Y%m%d")
                                et = lts.strftime("%H:%M:%S")
                                last_seen_event_time[name] = (ed, et)
                                last_status[name] = classify(str(level), str(message))
                                last_pk[name] = LastPK(lts.isoformat(), "", str(h))
                    else:
                        if spec.mode == "STD":
                            rows = fetch_rows_incremental_std(sel, health_schema, name, lp, limit=2000)
                            if rows:
                                for (ed, et, info, contents, h) in rows:
                                    ts = parse_end_ts(str(ed), str(et))
                                    if ts is not None:
                                        last_seen_ts[name] = ts
                                        last_seen_event_time[name] = (str(ed), str(et))
                                    last_status[name] = classify(str(info), str(contents))
                                    last_pk[name] = LastPK(str(ed), str(et), str(h))
                        else:
                            rows = fetch_rows_incremental_logts(sel, health_schema, name, lp, limit=2000)
                            if rows:
                                for (log_ts, level, phase, src, message, h) in rows:
                                    if not isinstance(log_ts, datetime):
                                        continue
                                    lts = log_ts.astimezone(KST) if log_ts.tzinfo else log_ts.replace(tzinfo=KST)
                                    last_seen_ts[name] = lts
                                    ed = lts.strftime("%Y%m%d")
                                    et = lts.strftime("%H:%M:%S")
                                    last_seen_event_time[name] = (ed, et)
                                    last_status[name] = classify(str(level), str(message))
                                    last_pk[name] = LastPK(lts.isoformat(), "", str(h))

                    latest_ts = last_seen_ts.get(name)
                    age_sec = None
                    if latest_ts is not None:
                        age_sec = max(0.0, (now - latest_ts).total_seconds())

                    base = last_status.get(name) or "비정상"
                    final_status = max_severity(base, time_based_status(age_sec))
                    prev = last_reported_status.get(name)

                    if prev != final_status:
                        le = last_seen_event_time.get(name)
                        if le is not None:
                            upsert_latest(sel, health_schema, le[0], le[1], name, final_status)
                            last_reported_status[name] = final_status

                except (OperationalError, DBAPIError) as e:
                    log_exc(f"select/upsert failed => reset sel: {name}", e)
                    try:
                        sel.close()
                    except Exception:
                        pass
                    sel = None
                    break

                except Exception as e:
                    log_exc(f"per-target error: {name}", e)

            loop_ticks += 1
            time_mod.sleep(SLEEP_SEC)

        except (OperationalError, DBAPIError) as e:
            log_down("DB operational error; reconnecting (blocking)")
            log_exc("DB error", e)
            try:
                if sel is not None:
                    sel.close()
            except Exception:
                pass
            sel = None
            try:
                engine.dispose()
            except Exception:
                pass
            time_mod.sleep(2)
            engine = connect_blocking()

        except Exception as e:
            log_exc("loop error", e)
            time_mod.sleep(1)

    try:
        if sel is not None:
            sel.close()
    except Exception:
        pass
    log_info("stopped cleanly")

if __name__ == "__main__":
    run_daemon()