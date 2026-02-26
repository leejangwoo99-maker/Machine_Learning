# app/routers/events_sse.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, Query, Header, HTTPException, Request
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


# =========================================================
# ✅ ENGINE: 공용 엔진 우선 사용 (DB 튜닝/일관성)
# =========================================================
def _get_shared_engine() -> Optional[Engine]:
    """
    app/db/session.py 등에 이미 생성된 ENGINE/engine이 있으면 최우선 사용.
    """
    try:
        from app.db import session as s  # type: ignore
        eng = getattr(s, "ENGINE", None) or getattr(s, "engine", None)
        return eng
    except Exception:
        return None


def _build_db_url() -> str:
    """
    우선순위:
    1) DATABASE_URL / DB_URL / SQLALCHEMY_DATABASE_URI
    2) 개별 PG_* 환경변수 조합
    """
    direct = (
        os.getenv("DATABASE_URL")
        or os.getenv("DB_URL")
        or os.getenv("SQLALCHEMY_DATABASE_URI")
    )
    if direct:
        return direct

    host = os.getenv("PGHOST", "127.0.0.1")
    port = os.getenv("PGPORT", "5432")
    user = os.getenv("PGUSER", "postgres")
    password = os.getenv("PGPASSWORD", "postgres")
    dbname = os.getenv("PGDATABASE", "postgres")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"


_SHARED = _get_shared_engine()
ENGINE: Engine = _SHARED or create_engine(
    _build_db_url(),
    pool_pre_ping=True,
    pool_size=1,
    max_overflow=0,
    future=True,
)

router = APIRouter(prefix="/events", tags=["events-sse"])

KST = ZoneInfo("Asia/Seoul")
ADMIN_PASS = os.getenv("ADMIN_PASS", "leejangwoo1!")

ALLOWED_SECTIONS = {
    "alarm",
    "chart",
    "nonop_detail",
    "worker_info",
    "mastersample",
    "planned",
    "email",
    "barcode",
}


# =========================================================
# ✅ CORS helpers (SSE에서도 명시적으로 헤더 포함)
# =========================================================
def _cors_allow_origins() -> Set[str]:
    raw = (os.getenv("CORS_ALLOW_ORIGINS") or "").strip()
    if raw:
        s = {x.strip() for x in raw.split(",") if x.strip()}
        if s:
            return s
    return {"http://localhost:8501", "http://127.0.0.1:8501"}


def _cors_headers_for_request(request: Request) -> Dict[str, str]:
    """
    - Origin이 허용 목록이면 그대로 echo
    - 없으면 빈 dict (동일 오리진/서버 호출 등)
    """
    origin = (request.headers.get("origin") or "").strip()
    if origin and origin in _cors_allow_origins():
        return {
            "Access-Control-Allow-Origin": origin,
            "Access-Control-Allow-Credentials": "true",
            "Vary": "Origin",
        }
    return {}


# =========================================================
# Utils
# =========================================================
def _now_iso_kst() -> str:
    return datetime.now(tz=KST).isoformat(timespec="seconds")


def _normalize_day(v: Any) -> str:
    s = str(v or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _safe_shift(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in ("day", "night") else ""


def _pk_to_token(pk: Dict[str, Any]) -> str:
    return json.dumps(pk, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _has_column(schema: str, table: str, col: str) -> bool:
    sql = text("""
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = :schema
          AND table_name   = :table
          AND column_name  = :col
        LIMIT 1
    """)
    try:
        with ENGINE.connect() as conn:
            r = conn.execute(sql, {"schema": schema, "table": table, "col": col}).first()
            return bool(r)
    except Exception:
        return False


def _check_admin(request: Request, token: Optional[str], x_admin_pass: Optional[str]) -> None:
    """
    ✅ SSE(EventSource)는 헤더를 못 보내므로 query token 허용
    - 헤더(X-ADMIN-PASS) 우선
    - 없으면 token 허용
    - ADMIN_PASS 환경변수가 비어있으면 검증 비활성(운영 편의)
    """
    expected = (ADMIN_PASS or "").strip()
    if not expected:
        return

    hp = (x_admin_pass or "").strip()
    if hp and hp == expected:
        return

    tp = (token or "").strip()
    if tp and tp == expected:
        return

    raise HTTPException(status_code=401, detail="invalid admin pass")


# =========================================================
# Token fetch
# =========================================================
def _fetch_latest_tokens(end_day: str, shift_type: str) -> Dict[str, str]:
    """
    섹션별 최신 1건 token.
    - 테이블/컬럼 없으면 해당 섹션만 빈값 유지.
    - alarm은 id 포함해서 프론트에서 pk로 쓸 수 있게 함.
    """
    out: Dict[str, str] = {
        "alarm": "",
        "chart": "",
        "nonop_detail": "",
        "worker_info": "",
        "mastersample": "",
        "planned": "",
        "email": "",
        "barcode": "",
    }

    alarm_has_shift = _has_column("g_production_film", "alarm_record", "shift_type")
    alarm_has_created = _has_column("g_production_film", "alarm_record", "created_at")

    alarm_where_shift = "AND COALESCE(shift_type, :shift_type) = :shift_type" if alarm_has_shift else ""
    alarm_order_created = ", created_at DESC NULLS LAST" if alarm_has_created else ""

    sql_map = {
        "alarm": text(f"""
            SELECT
                id,
                replace(end_day::text,'-','') AS end_day,
                COALESCE(end_time::text,'')   AS end_time,
                station,
                sparepart,
                type_alarm
            FROM g_production_film.alarm_record
            WHERE replace(end_day::text,'-','') = :end_day
              {alarm_where_shift}
              AND COALESCE(type_alarm::text,'') IN ('권고','긴급','교체','준비')
            ORDER BY
                end_time::time DESC NULLS LAST
                {alarm_order_created},
                id DESC
            LIMIT 1
        """),

        "chart": text("""
            SELECT end_day, station, from_time, to_time
            FROM g_production_film.fct_non_operation_time
            WHERE replace(end_day::text,'-','') = :end_day
              AND COALESCE(shift_type, :shift_type) = :shift_type
            ORDER BY from_time DESC NULLS LAST
            LIMIT 1
        """),
        "nonop_detail": text("""
            SELECT end_day, station, from_time, to_time
            FROM g_production_film.fct_non_operation_time
            WHERE replace(end_day::text,'-','') = :end_day
              AND COALESCE(shift_type, :shift_type) = :shift_type
            ORDER BY from_time DESC NULLS LAST
            LIMIT 1
        """),
        "worker_info": text("""
            SELECT replace(end_day::text,'-','') AS end_day, shift_type, worker_name, COALESCE(order_number,'') AS order_number
            FROM g_production_film.worker_info
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY worker_name DESC
            LIMIT 1
        """),
        "mastersample": text("""
            SELECT replace(end_day::text,'-','') AS end_day, shift_type, station, COALESCE(updated_at::text,'') AS updated_at
            FROM g_production_film.mastersample_test_info
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
        """),
        "planned": text("""
            SELECT replace(prod_day::text,'-','') AS prod_day, shift_type, from_time, to_time
            FROM g_production_film.planned_time
            WHERE replace(prod_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY from_time DESC NULLS LAST
            LIMIT 1
        """),
        "email": text("""
            SELECT replace(end_day::text,'-','') AS end_day, shift_type, email
            FROM g_production_film.email_list
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY email DESC
            LIMIT 1
        """),
        "barcode": text("""
            SELECT replace(end_day::text,'-','') AS end_day, shift_type, barcode_information, COALESCE(pn,'') AS pn, COALESCE(remark,'') AS remark
            FROM g_production_film.remark_info
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY barcode_information DESC
            LIMIT 1
        """),
    }

    params = {"end_day": end_day, "shift_type": shift_type}

    with ENGINE.connect() as conn:
        for sec, sql in sql_map.items():
            try:
                row = conn.execute(sql, params).mappings().first()
                if row:
                    out[sec] = _pk_to_token(dict(row))
            except Exception:
                out[sec] = ""

    return out


def _token_row(token: str) -> Optional[Dict[str, Any]]:
    if not token:
        return None
    try:
        obj = json.loads(token)
        return obj if isinstance(obj, dict) else None
    except Exception:
        return None


# =========================================================
# REST: latest (기존 유지)
# =========================================================
@router.get("/latest")
def events_latest(
    request: Request,
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: str = Query(..., description="day|night"),
):
    d = _normalize_day(end_day)
    s = _safe_shift(shift_type)
    if not d:
        raise HTTPException(status_code=422, detail="end_day must be YYYYMMDD")
    if not s:
        raise HTTPException(status_code=422, detail="shift_type must be day|night")

    try:
        tokens = _fetch_latest_tokens(d, s)
        headers = _cors_headers_for_request(request)
        return JSONResponse(
            {
                "ok": True,
                "end_day": d,
                "shift_type": s,
                "tokens": tokens,
                "ts": _now_iso_kst(),
            },
            headers=headers,
        )
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


# =========================================================
# SSE generator
# - 기존 이벤트(hello/ping/error/{sec}.updated) 유지
# - 추가: init, alarm (front 편의)
# =========================================================
def _sse_event(event: str, data_obj: Any) -> str:
    return f"event: {event}\ndata: {json.dumps(data_obj, ensure_ascii=False, default=str)}\n\n"


async def _sse_gen(end_day: str, shift_type: str, sections: Set[str], interval_sec: float = 2.0):
    prev = _fetch_latest_tokens(end_day, shift_type)

    hello = {
        "event": "hello",
        "end_day": end_day,
        "shift_type": shift_type,
        "sections": sorted(list(sections)),
        "ts": _now_iso_kst(),
    }
    yield _sse_event("hello", hello)

    init_payload: Dict[str, Any] = {
        "event": "init",
        "end_day": end_day,
        "shift_type": shift_type,
        "sections": sorted(list(sections)),
        "ts": _now_iso_kst(),
    }
    if "alarm" in sections:
        arow = _token_row(prev.get("alarm", ""))
        if arow:
            init_payload["pk"] = str(arow.get("id", "") or "")
            init_payload["row"] = arow
    yield _sse_event("init", init_payload)

    while True:
        try:
            cur = _fetch_latest_tokens(end_day, shift_type)

            for sec in sections:
                if cur.get(sec, "") != prev.get(sec, ""):
                    payload = {
                        "event": f"{sec}.updated",
                        "section": sec,
                        "end_day": end_day,
                        "shift_type": shift_type,
                        "ts": _now_iso_kst(),
                    }
                    yield _sse_event(f"{sec}.updated", payload)

                    if sec == "alarm":
                        row = _token_row(cur.get("alarm", ""))
                        if row:
                            alarm_payload = {
                                "event": "alarm",
                                "end_day": end_day,
                                "shift_type": shift_type,
                                "ts": _now_iso_kst(),
                                "pk": str(row.get("id", "") or ""),
                                "row": row,
                            }
                            yield _sse_event("alarm", alarm_payload)

            prev = cur

            ping = {"event": "ping", "ts": _now_iso_kst()}
            yield _sse_event("ping", ping)

            await asyncio.sleep(interval_sec)

        except asyncio.CancelledError:
            break
        except Exception as e:
            err = {"event": "error", "message": str(e), "ts": _now_iso_kst()}
            yield _sse_event("error", err)
            await asyncio.sleep(interval_sec)


@router.get("/stream")
async def events_stream(
    request: Request,
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: str = Query(..., description="day|night"),
    sections: str = Query("alarm,chart,nonop_detail,worker_info,mastersample"),
    x_admin_pass: Optional[str] = Header(None, alias="X-ADMIN-PASS"),
    token: Optional[str] = Query(None, description="EventSource용 admin token"),
):
    _check_admin(request, token, x_admin_pass)

    d = _normalize_day(end_day)
    s = _safe_shift(shift_type)
    if not d:
        raise HTTPException(status_code=422, detail="end_day must be YYYYMMDD")
    if not s:
        raise HTTPException(status_code=422, detail="shift_type must be day|night")

    req_sections = {x.strip() for x in (sections or "").split(",") if x.strip()}
    use_sections = {x for x in req_sections if x in ALLOWED_SECTIONS}
    if not use_sections:
        use_sections = {"alarm", "chart", "nonop_detail", "worker_info", "mastersample"}

    # ✅ SSE에도 CORS 헤더 명시(브라우저 차단 방지)
    cors_h = _cors_headers_for_request(request)

    return StreamingResponse(
        _sse_gen(d, s, use_sections, interval_sec=2.0),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            **cors_h,
        },
    )