# app/services/email_list_svc.py
from __future__ import annotations
from sqlalchemy.engine import Engine
from app.repos import email_list_repo


def list_emails(engine: Engine) -> list[str]:
    return email_list_repo.list_all(engine)


def sync_emails(engine: Engine, emails: list[str]) -> tuple[int, int]:
    """
    주의: 전달 목록으로 전체 동기화(없는 건 삭제)
    """
    current = set(email_list_repo.list_all(engine))
    wanted = set(emails)
    to_insert = sorted(wanted - current)
    to_delete = sorted(current - wanted)

    ins = 0
    dele = 0
    with engine.begin() as conn:
        for e in to_insert:
            ins += email_list_repo.upsert_one(conn, e)
        for e in to_delete:
            dele += email_list_repo.delete_one(conn, e)
    return ins, dele


# ✅ 단건 추가(이미 있으면 그대로 0)
def add_email(engine: Engine, email: str) -> int:
    with engine.begin() as conn:
        return email_list_repo.upsert_one(conn, email)
