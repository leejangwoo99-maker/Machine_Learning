# app/routers/events_sse.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Set

from fastapi import APIRouter, Query, Header, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# =========================================================
# DB ENGINE: 프로젝트 내부 import 의존 제거 (독립 생성)
# =========================================================
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


DB_URL = _build_db_url()
ENGINE = create_engine(
    DB_URL,
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
    return json.dumps(pk, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _fetch_latest_tokens(end_day: str, shift_type: str) -> Dict[str, str]:
    """
    섹션별 최신 1건 token.
    테이블/컬럼 없으면 해당 섹션만 빈값 유지.
    """
    out = {
        "alarm": "",
        "chart": "",
        "nonop_detail": "",
        "worker_info": "",
        "mastersample": "",
        "planned": "",
        "email": "",
        "barcode": "",
    }

    sql_map = {
        "alarm": text("""
            SELECT end_day, end_time, station, sparepart, type_alarm
            FROM g_production_film.alarm_record
            WHERE end_day = :end_day
              AND COALESCE(shift_type, :shift_type) = :shift_type
              AND type_alarm IN ('권고','긴급','교체')
            ORDER BY end_time DESC NULLS LAST, created_at DESC NULLS LAST
            LIMIT 1
        """),
        "chart": text("""
            SELECT end_day, station, from_time, to_time
            FROM g_production_film.fct_non_operation_time
            WHERE end_day = :end_day
              AND COALESCE(shift_type, :shift_type) = :shift_type
            ORDER BY from_time DESC NULLS LAST
            LIMIT 1
        """),
        "nonop_detail": text("""
            SELECT end_day, station, from_time, to_time
            FROM g_production_film.fct_non_operation_time
            WHERE end_day = :end_day
              AND COALESCE(shift_type, :shift_type) = :shift_type
            ORDER BY from_time DESC NULLS LAST
            LIMIT 1
        """),
        "worker_info": text("""
            SELECT end_day, shift_type, worker_name, COALESCE(order_number,'') AS order_number
            FROM g_production_film.worker_info
            WHERE end_day = :end_day
              AND shift_type = :shift_type
            ORDER BY worker_name DESC
            LIMIT 1
        """),
        "mastersample": text("""
            SELECT end_day, shift_type, station, COALESCE(updated_at::text,'') AS updated_at
            FROM g_production_film.mastersample_test_info
            WHERE end_day = :end_day
              AND shift_type = :shift_type
            ORDER BY updated_at DESC NULLS LAST
            LIMIT 1
        """),
        "planned": text("""
            SELECT prod_day, shift_type, from_time, to_time
            FROM g_production_film.planned_time
            WHERE prod_day = :end_day
              AND shift_type = :shift_type
            ORDER BY from_time DESC NULLS LAST
            LIMIT 1
        """),
        "email": text("""
            SELECT end_day, shift_type, email
            FROM g_production_film.email_list
            WHERE end_day = :end_day
              AND shift_type = :shift_type
            ORDER BY email DESC
            LIMIT 1
        """),
        "barcode": text("""
            SELECT end_day, shift_type, barcode_information, COALESCE(pn,'') AS pn, COALESCE(remark,'') AS remark
            FROM g_production_film.remark_info
            WHERE end_day = :end_day
              AND shift_type = :shift_type
            ORDER BY barcode_information DESC
            LIMIT 1
        """),
    }

    with ENGINE.connect() as conn:
        for sec, sql in sql_map.items():
            try:
                row = conn.execute(sql, {"end_day": end_day, "shift_type": shift_type}).mappings().first()
                if row:
                    out[sec] = _pk_to_token(dict(row))
            except Exception:
                out[sec] = ""

    return out


@router.get("/latest")
def events_latest(
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
        return JSONResponse({
            "ok": True,
            "end_day": d,
            "shift_type": s,
            "tokens": tokens,
            "ts": _now_iso_kst(),
        })
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")


async def _sse_gen(end_day: str, shift_type: str, sections: Set[str], interval_sec: float = 2.0):
    prev = _fetch_latest_tokens(end_day, shift_type)

    hello = {
        "event": "hello",
        "end_day": end_day,
        "shift_type": shift_type,
        "sections": sorted(list(sections)),
        "ts": _now_iso_kst(),
    }
    yield f"event: hello\ndata: {json.dumps(hello, ensure_ascii=False)}\n\n"

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
                    yield f"event: {sec}.updated\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"

            prev = cur
            ping = {"event": "ping", "ts": _now_iso_kst()}
            yield f"event: ping\ndata: {json.dumps(ping, ensure_ascii=False)}\n\n"

            await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            break
        except Exception as e:
            err = {"event": "error", "message": str(e), "ts": _now_iso_kst()}
            yield f"event: error\ndata: {json.dumps(err, ensure_ascii=False)}\n\n"
            await asyncio.sleep(interval_sec)


@router.get("/stream")
async def events_stream(
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: str = Query(..., description="day|night"),
    sections: str = Query("alarm,chart,nonop_detail,worker_info,mastersample"),
    x_admin_pass: Optional[str] = Header(None, alias="X-ADMIN-PASS"),
):
    if (x_admin_pass or "") != ADMIN_PASS:
        raise HTTPException(status_code=401, detail="invalid X-ADMIN-PASS")

    d = _normalize_day(end_day)
    s = _safe_shift(shift_type)
    if not d:
        raise HTTPException(status_code=422, detail="end_day must be YYYYMMDD")
    if not s:
        raise HTTPException(status_code=422, detail="shift_type must be day|night")

    req_sections = {x.strip() for x in sections.split(",") if x.strip()}
    use_sections = {x for x in req_sections if x in ALLOWED_SECTIONS}
    if not use_sections:
        use_sections = {"alarm", "chart", "nonop_detail", "worker_info", "mastersample"}

    return StreamingResponse(
        _sse_gen(d, s, use_sections, interval_sec=2.0),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
