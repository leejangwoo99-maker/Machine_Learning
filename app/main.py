from __future__ import annotations

import os
import uuid
import traceback
from pathlib import Path
from contextlib import asynccontextmanager

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

# -------------------------------------------------
# .env 로드 우선순위:
# 1) app/.env
# 2) 프로젝트 루트 .env
# -------------------------------------------------
APP_DIR = Path(__file__).resolve().parent
ROOT_DIR = APP_DIR.parent

ENV_APP = APP_DIR / ".env"
ENV_ROOT = ROOT_DIR / ".env"

if ENV_APP.exists():
    load_dotenv(dotenv_path=ENV_APP, override=True)
else:
    load_dotenv(dotenv_path=ENV_ROOT, override=True)

# dotenv 로드 이후 라우터 import
from app.routers.health import router as health_router
from app.routers.events import router as events_router
from app.routers.worker_info import router as worker_info_router
from app.routers.email_list import router as email_list_router
from app.routers.remark_info import router as remark_info_router
from app.routers.planned_today import router as planned_time_router
from app.routers.non_operation_time import router as non_operation_time_router
from app.routers.alarm_record import router as alarm_record_router
from app.routers.pd_board_check import router as pd_board_check_router
from app.routers.reports import router as reports_router
from app.routers.master_sample_info import router as master_sample_info_router
from app.routers.production_progress_graph import router as production_progress_graph_router
from app.routers.events_sse import router as events_sse_router
from app.routers.events_sections import router as events_sections_router

def _mask_db_url(url: str | None) -> str:
    """로그 출력용 DB URL 마스킹."""
    if not url:
        return "<EMPTY>"
    if "://" in url and "@" in url:
        scheme, rest = url.split("://", 1)
        creds, hostpart = rest.split("@", 1)
        if ":" in creds:
            user = creds.split(":", 1)[0]
            return f"{scheme}://{user}:***@{hostpart}"
    return url


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[BOOT] ENV_APP exists:", ENV_APP.exists(), str(ENV_APP), flush=True)
    print("[BOOT] ENV_ROOT exists:", ENV_ROOT.exists(), str(ENV_ROOT), flush=True)
    print("[BOOT] DATABASE_URL:", _mask_db_url(os.getenv("DATABASE_URL")), flush=True)
    print("[BOOT] ADMIN_PASS set?:", bool(os.getenv("ADMIN_PASS")), flush=True)
    print("[BOOT] PG_WORK_MEM:", os.getenv("PG_WORK_MEM", "<EMPTY>"), flush=True)
    yield
    print("[SHUTDOWN] app stopped", flush=True)


app = FastAPI(
    title="Aptiv Film API",
    version="3.0",
    debug=True,  # 개발 중 추적 용도
    lifespan=lifespan,
)


# -------------------------------------------------
# 공통 미들웨어:
# - 요청별 Request-ID 부여
# - JSON 응답 charset 보정
# - 예외 발생 시 트레이스 출력 + 구조화된 500 응답
# -------------------------------------------------
@app.middleware("http")
async def app_middlewares(request: Request, call_next):
    request_id = str(uuid.uuid4())[:8]

    try:
        response = await call_next(request)

        # JSON 응답 Content-Type에 charset이 없으면 보정
        ctype = response.headers.get("content-type", "")
        if ctype.startswith("application/json") and "charset=" not in ctype.lower():
            response.headers["content-type"] = "application/json; charset=utf-8"

        response.headers["X-Request-ID"] = request_id
        return response

    except Exception as e:
        path = request.url.path
        query = str(request.url.query)
        method = request.method

        print(
            f"[ERROR] request_id={request_id} {method} {path}?{query} -> {type(e).__name__}: {e}",
            flush=True,
        )
        traceback.print_exc()

        return JSONResponse(
            status_code=500,
            content={
                "detail": f"{type(e).__name__}: {e}",
                "request_id": request_id,
                "path": path,
                "method": method,
            },
        )


# -------------------------------------------------
# Router 등록
# -------------------------------------------------
app.include_router(health_router)
app.include_router(events_router)
app.include_router(worker_info_router)
app.include_router(email_list_router)
app.include_router(remark_info_router)
app.include_router(planned_time_router)
app.include_router(non_operation_time_router)
app.include_router(alarm_record_router)
app.include_router(pd_board_check_router)
app.include_router(reports_router)
app.include_router(master_sample_info_router)
app.include_router(production_progress_graph_router)
app.include_router(events_sse_router)
app.include_router(events_sections_router)