# app/core/db.py
from __future__ import annotations

import os
from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DisconnectionError
from app.core.config import settings


def _env_int(name: str, default: int) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return default


def _env_bool(name: str, default: bool) -> bool:
    v = str(os.getenv(name, str(default))).strip().lower()
    return v in {"1", "true", "yes", "y", "on"}


def _env_str(name: str, default: str) -> str:
    v = os.getenv(name, default)
    return str(v).strip() if v is not None else default


def _pg_work_mem() -> str:
    wm = getattr(settings, "PG_WORK_MEM", None) or os.getenv("PG_WORK_MEM") or "256MB"
    wm = str(wm).strip()
    return wm if wm else "256MB"


def make_engine() -> Engine:
    if not settings.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty. Set it in environment (.env).")

    # ----------------------------
    # Pool tuning (체감속도 핵심)
    # ----------------------------
    # Streamlit + FastAPI 로컬에서 '동시 요청'이 생겨도 너무 큰 풀은 오히려 DB를 괴롭힘.
    pool_size = _env_int("PG_POOL_SIZE", 6)
    max_overflow = _env_int("PG_MAX_OVERFLOW", 6)

    # "멈춘 느낌" 방지: 타임아웃을 짧게(=빨리 실패하고 재시도/대체로직 타게)
    pool_timeout = _env_int("PG_POOL_TIMEOUT", 3)

    # 현장망 idle 끊김 방지
    pool_recycle = _env_int("PG_POOL_RECYCLE", 900)  # 15min
    use_lifo = _env_bool("PG_POOL_LIFO", True)

    # ----------------------------
    # Server-side timeouts
    # ----------------------------
    # 기본을 짧게 두고(예: 알람/대시보드), "무거운 리포트"는 API 쿼리에서 개별적으로 늘리는게 정답.
    statement_timeout_ms = _env_int("PG_STATEMENT_TIMEOUT_MS", 12000)  # 12s
    lock_timeout_ms = _env_int("PG_LOCK_TIMEOUT_MS", 2000)             # 2s
    idle_in_tx_ms = _env_int("PG_IDLE_IN_TX_TIMEOUT_MS", 15000)        # 15s

    # connect timeout (libpq)
    connect_timeout = _env_int("PG_CONNECT_TIMEOUT", 3)

    work_mem = _pg_work_mem()

    enable_keepalive = _env_bool("PG_TCP_KEEPALIVE", True)

    # SQLAlchemy -> psycopg2 connect args
    connect_args = {
        "client_encoding": "utf8",
        "connect_timeout": connect_timeout,
        # ✅ 세션 레벨 기본값 (핵심: statement_timeout/lock_timeout)
        "options": (
            "-c client_encoding=UTF8 "
            "-c TimeZone=Asia/Seoul "
            f"-c work_mem={work_mem} "
            f"-c statement_timeout={statement_timeout_ms} "
            f"-c lock_timeout={lock_timeout_ms} "
            f"-c idle_in_transaction_session_timeout={idle_in_tx_ms} "
        ),
    }

    if enable_keepalive:
        connect_args.update(
            {
                "keepalives": 1,
                "keepalives_idle": _env_int("PG_KEEPALIVE_IDLE", 30),
                "keepalives_interval": _env_int("PG_KEEPALIVE_INTERVAL", 10),
                "keepalives_count": _env_int("PG_KEEPALIVE_COUNT", 5),
            }
        )

    engine = create_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        pool_recycle=pool_recycle,
        pool_timeout=pool_timeout,
        pool_use_lifo=use_lifo,
        future=True,
        connect_args=connect_args,
        # ✅ 기본적으로 트랜잭션 종료시 rollback (연결 깨끗이)
        pool_reset_on_return="rollback",
    )

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn, _):
        cur = None
        try:
            cur = dbapi_conn.cursor()
            # connect_args options에도 있지만, 일부 드라이버/환경에서 확실하게 한번 더
            cur.execute("SET client_encoding TO 'UTF8'")
            cur.execute("SET TIME ZONE 'Asia/Seoul'")
            cur.execute(f"SET work_mem TO '{work_mem}'")
            cur.execute(f"SET statement_timeout TO '{statement_timeout_ms}'")
            cur.execute(f"SET lock_timeout TO '{lock_timeout_ms}'")
            cur.execute(f"SET idle_in_transaction_session_timeout TO '{idle_in_tx_ms}'")
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass

    @event.listens_for(engine, "engine_connect")
    def _on_engine_connect(conn, branch):
        if branch:
            return
        try:
            conn.execute(text("SELECT 1"))
        except Exception as e:
            raise DisconnectionError() from e

    return engine