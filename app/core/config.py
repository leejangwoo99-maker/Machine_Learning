from __future__ import annotations

import os
from pydantic import BaseModel


class Settings(BaseModel):
    # runtime
    DATABASE_URL: str = os.getenv("DATABASE_URL", "")
    ADMIN_PASS: str = os.getenv("ADMIN_PASS", "leejangwoo1!")
    TZ: str = os.getenv("TZ", "Asia/Seoul")

    # schemas (db)
    GPF_SCHEMA: str = os.getenv("GPF_SCHEMA", "g_production_film")
    IDR_SCHEMA: str = os.getenv("IDR_SCHEMA", "i_daily_report")
    E4PM_SCHEMA: str = os.getenv("E4PM_SCHEMA", "e4_predictive_maintenance")

    # g_production_film tables
    WORKER_INFO_TABLE: str = os.getenv("WORKER_INFO_TABLE", "worker_info")
    EMAIL_LIST_TABLE: str = os.getenv("EMAIL_LIST_TABLE", "email_list")
    REMARK_INFO_TABLE: str = os.getenv("REMARK_INFO_TABLE", "remark_info")
    PLANNED_TIME_TABLE: str = os.getenv("PLANNED_TIME_TABLE", "planned_time")
    FCT_NONOP_TABLE: str = os.getenv("FCT_NONOP_TABLE", "fct_non_operation_time")
    VISION_NONOP_TABLE: str = os.getenv("VISION_NONOP_TABLE", "vision_non_operation_time")
    ALARM_RECORD_TABLE: str = os.getenv("ALARM_RECORD_TABLE", "alarm_record")

    # e4_predictive_maintenance tables
    PD_BOARD_CHECK_TABLE: str = os.getenv("PD_BOARD_CHECK_TABLE", "pd_board_check")


settings = Settings()
