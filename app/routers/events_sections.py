# app/routers/events_sections.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import JSONResponse
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter(prefix="/events", tags=["events-sections"])
KST = ZoneInfo("Asia/Seoul")


# =========================================================
# DB ENGINE (app.db import 의존 제거)
# =========================================================
def _build_db_url() -> str:
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


ENGINE = create_engine(
    _build_db_url(),
    pool_pre_ping=True,
    pool_size=1,
    max_overflow=0,
    future=True,
)


def _normalize_day(v: Any) -> str:
    s = str(v or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _safe_shift(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in ("day", "night") else ""


def _token_of(obj: Dict[str, Any]) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _now_iso() -> str:
    return datetime.now(tz=KST).isoformat(timespec="seconds")


def _get_section_tokens(end_day: str, shift_type: str) -> Dict[str, str]:
    """
    섹션별 최신 1건 토큰 생성
    - 표준 컬럼이 없거나 테이블이 없으면 해당 섹션 빈 문자열 유지
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

    q = {
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
        for sec, sql in q.items():
            try:
                row = conn.execute(sql, {"end_day": end_day, "shift_type": shift_type}).mappings().first()
                if row:
                    out[sec] = _token_of(dict(row))
            except Exception:
                out[sec] = ""

    return out


@router.get("/sections/latest")
def sections_latest(
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
        tokens = _get_section_tokens(d, s)
        return JSONResponse({
            "ok": True,
            "end_day": d,
            "shift_type": s,
            "tokens": tokens,
            "ts": _now_iso(),
        })
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")
