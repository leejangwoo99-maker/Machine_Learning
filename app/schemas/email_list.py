# app/schemas/email_list.py
from __future__ import annotations

from pydantic import BaseModel, Field, field_validator


def _normalize_and_check_email(v: str) -> str:
    if v is None:
        raise ValueError("email is required")
    s = v.strip().lower()
    if not s:
        raise ValueError("email is required")

    # 최소한의 형식 검증 (email-validator 없이 동작)
    # 너무 빡세지 않게 실무용 최소 체크
    if "@" not in s or s.count("@") != 1:
        raise ValueError("invalid email format")
    local, domain = s.split("@", 1)
    if not local or not domain or "." not in domain:
        raise ValueError("invalid email format")
    if s.startswith(".") or s.endswith(".") or ".." in s:
        raise ValueError("invalid email format")

    return s


class EmailOneIn(BaseModel):
    email: str = Field(..., min_length=3, max_length=254)

    @field_validator("email")
    @classmethod
    def validate_email(cls, v: str) -> str:
        return _normalize_and_check_email(v)


class EmailListSyncIn(BaseModel):
    emails: list[str] = Field(default_factory=list)

    @field_validator("emails")
    @classmethod
    def validate_emails(cls, v: list[str]) -> list[str]:
        normalized = [_normalize_and_check_email(x) for x in v]
        # 중복 제거 + 정렬
        return sorted(set(normalized))
