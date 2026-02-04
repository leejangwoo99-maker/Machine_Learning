from __future__ import annotations

from fastapi import Header, HTTPException
from app.core.config import settings


def require_admin_pass(
    x_admin_pass: str | None = Header(default=None, alias="X-ADMIN-PASS"),
) -> None:
    if x_admin_pass is None or x_admin_pass != settings.ADMIN_PASS:
        raise HTTPException(status_code=401, detail="Invalid admin password")
