from __future__ import annotations
from sqlalchemy import text
from sqlalchemy.engine import Engine
from app.core.config import settings


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.EMAIL_LIST_TABLE}"'


def list_all(engine: Engine) -> list[str]:
    sql = text(f"""SELECT email_list FROM {fqn()} ORDER BY email_list""")
    with engine.connect() as conn:
        rows = conn.execute(sql).all()
    return [r[0] for r in rows]


def sync(engine: Engine, emails: list[str]) -> tuple[int, int]:
    normalized = sorted({e.strip() for e in emails if e and e.strip()})
    with engine.begin() as conn:
        existing = conn.execute(text(f"SELECT email_list FROM {fqn()}")).scalars().all()
        existing_set = set(existing)
        incoming_set = set(normalized)

        to_delete = list(existing_set - incoming_set)
        to_insert = list(incoming_set - existing_set)

        if to_delete:
            conn.execute(
                text(f"DELETE FROM {fqn()} WHERE email_list = ANY(:arr)"),
                {"arr": to_delete},
            )

        if to_insert:
            conn.execute(
                text(f"""
                    INSERT INTO {fqn()} (email_list)
                    SELECT unnest(:arr)
                    ON CONFLICT (email_list) DO NOTHING
                """),
                {"arr": to_insert},
            )

    return (len(to_insert), len(to_delete))
