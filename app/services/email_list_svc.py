from __future__ import annotations
from sqlalchemy.engine import Engine
from app.repos import email_list_repo


def list_emails(engine: Engine) -> list[str]:
    return email_list_repo.list_all(engine)


def sync_emails(engine: Engine, emails: list[str]) -> tuple[int, int]:
    return email_list_repo.sync(engine, emails)
