from __future__ import annotations

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from app.core.config import settings


def _pg_work_mem() -> str:
    # settings???놁쑝硫?湲곕낯 32MB
    wm = getattr(settings, "PG_WORK_MEM", None) or "32MB"
    wm = str(wm).strip()
    return wm if wm else "32MB"


def make_engine() -> Engine:
    if not settings.DATABASE_URL:
        raise RuntimeError("DATABASE_URL is empty. Set it in environment (.env).")

    work_mem = _pg_work_mem()

    engine = create_engine(
        settings.DATABASE_URL,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
        pool_recycle=1800,   # ?ㅻ옒??idle 而ㅻ꽖???ъ깮??
        pool_timeout=30,     # ? ?湲?timeout
        pool_use_lifo=True,  # 理쒓렐 而ㅻ꽖???곗꽑 ?ъ궗??
        future=True,
        connect_args={
            # psycopg2 湲곗? ?쒕씪?대쾭 ?덈꺼 ?몄퐫???몄뀡 GUC
            "client_encoding": "utf8",
            "options": f"-c client_encoding=UTF8 -c work_mem={work_mem} -c TimeZone=Asia/Seoul",
        },
    )

    @event.listens_for(engine, "connect")
    def _on_connect(dbapi_conn, _):
        cur = None
        try:
            cur = dbapi_conn.cursor()
            cur.execute("SET client_encoding TO 'UTF8'")
            cur.execute(f"SET work_mem TO '{work_mem}'")
            cur.execute("SET TIME ZONE 'Asia/Seoul'")
        finally:
            if cur is not None:
                try:
                    cur.close()
                except Exception:
                    pass

    return engine
