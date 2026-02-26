from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings


def fqn() -> str:
    return f'{settings.GPF_SCHEMA}."{settings.EMAIL_LIST_TABLE}"'


def _norm_email(x: str) -> str:
    # 삭제/중복 방지를 위해 강제 정규화
    return (x or "").strip().lower()


def list_all(engine: Engine) -> list[str]:
    # 항상 정규화된 형태로 반환
    sql = text(
        f"""
        SELECT lower(btrim(email_list)) AS email_list
        FROM {fqn()}
        WHERE email_list IS NOT NULL AND btrim(email_list) <> ''
        ORDER BY lower(btrim(email_list))
        """
    )
    with engine.connect() as conn:
        rows = conn.execute(sql).scalars().all()
    return [r for r in rows if r]


def sync(engine: Engine, emails: list[str]) -> tuple[int, int]:
    """
    DB(email_list)를 incoming 리스트와 완전히 동기화.
    - 공백/대소문자 차이로 delete가 안 되는 문제 해결을 위해
      비교/삭제/저장 모두 lower(btrim()) 기준으로 처리한다.
    - incoming이 빈 리스트면: 테이블의 모든 행을 삭제한다.
    """
    normalized = sorted({_norm_email(e) for e in (emails or []) if _norm_email(e)})

    with engine.begin() as conn:
        # 현재 DB에 있는 정규화 값 목록
        existing_norm = conn.execute(
            text(
                f"""
                SELECT lower(btrim(email_list)) AS email
                FROM {fqn()}
                WHERE email_list IS NOT NULL AND btrim(email_list) <> ''
                """
            )
        ).scalars().all()
        existing_set = set(existing_norm)
        incoming_set = set(normalized)

        to_delete_norm = sorted(existing_set - incoming_set)
        to_insert_norm = sorted(incoming_set - existing_set)

        deleted = 0
        inserted = 0

        # 1) 삭제: 정규화 기준으로 삭제 (공백/대소문자 달라도 삭제되게)
        if to_delete_norm:
            r = conn.execute(
                text(
                    f"""
                    DELETE FROM {fqn()}
                    WHERE lower(btrim(email_list)) = ANY(:arr)
                    """
                ),
                {"arr": to_delete_norm},
            )
            deleted = int(r.rowcount or 0)

        # 2) 삽입: 정규화된 값만 저장 (향후 충돌/중복 최소화)
        if to_insert_norm:
            conn.execute(
                text(
                    f"""
                    INSERT INTO {fqn()} (email_list)
                    SELECT unnest(:arr)
                    ON CONFLICT (email_list) DO NOTHING
                    """
                ),
                {"arr": to_insert_norm},
            )
            inserted = len(to_insert_norm)

        # 3) 추가 정리: 혹시 DB에 "같은 이메일인데 공백/대소문자만 다른 중복 row"가 이미 있으면 정리
        #    (유니크 제약이 email_list 원문 기준이면 이런 찌꺼기가 남을 수 있음)
        conn.execute(
            text(
                f"""
                DELETE FROM {fqn()} a
                USING {fqn()} b
                WHERE a.ctid < b.ctid
                  AND lower(btrim(a.email_list)) = lower(btrim(b.email_list))
                """
            )
        )

    return inserted, deleted