from __future__ import annotations

from pathlib import Path
from dotenv import load_dotenv

# Load .env from PROJECT ROOT (one level above /app)
# e.g. C:\Users\user\PycharmProjects\PythonProject\.env
load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env", override=True)

from fastapi import FastAPI

from app.routers.worker_info import router as worker_info_router
from app.routers.email_list import router as email_list_router
from app.routers.remark_info import router as remark_info_router
from app.routers.planned_time import router as planned_time_router
from app.routers.nonop_time import router as nonop_time_router
from app.routers.alarm_record import router as alarm_record_router
from app.routers.predictive import router as predictive_router
from app.routers.reports import router as reports_router

app = FastAPI(
    title="Aptiv Film API",
    version="2.1",
    debug=True,
)


@app.get("/health")
def health() -> dict[str, bool]:
    return {"ok": True}


# Router registration
app.include_router(worker_info_router)
app.include_router(email_list_router)
app.include_router(remark_info_router)
app.include_router(planned_time_router)
app.include_router(nonop_time_router)
app.include_router(alarm_record_router)
app.include_router(predictive_router)
app.include_router(reports_router)
