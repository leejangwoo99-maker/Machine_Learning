from __future__ import annotations
import os
from pydantic import BaseModel

class Settings(BaseModel):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    ADMIN_PASS: str = os.getenv("APP_POPUP_PASSWORD", os.getenv("ADMIN_PASS", "leejangwoo1!"))
    TZ: str = os.getenv("TZ", "Asia/Seoul")
    GPF_SCHEMA: str = os.getenv("GPF_SCHEMA", "g_production_film")
    IDR_SCHEMA: str = os.getenv("IDR_SCHEMA", "i_daily_report")
    E4PM_SCHEMA: str = os.getenv("E4PM_SCHEMA", "e4_predictive_maintenance")
settings = Settings()
