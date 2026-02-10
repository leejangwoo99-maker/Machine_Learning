# app/services/email_list_svc.py
from __future__ import annotations
from sqlalchemy.engine import Engine
from app.repos import email_list_repo


def list_emails(engine: Engine) -> list[str]:
    return email_list_repo.list_all(engine)


def sync_emails(engine: Engine, emails: list[str]) -> tuple[int, int]:
    """
    二쇱쓽: ?꾨떖 紐⑸줉?쇰줈 ?꾩껜 ?숆린???녿뒗 嫄???젣)
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


# ???④굔 異붽?(?대? ?덉쑝硫?洹몃?濡?0)
def add_email(engine: Engine, email: str) -> int:
    with engine.begin() as conn:
        return email_list_repo.upsert_one(conn, email)
