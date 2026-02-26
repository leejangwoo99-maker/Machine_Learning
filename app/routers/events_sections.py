# app/routers/events_sections.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
import time
import hashlib
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, Optional, Iterable

from fastapi import APIRouter, Query, HTTPException, Header
from fastapi.responses import JSONResponse, StreamingResponse
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

router = APIRouter(prefix="/events", tags=["events-sections"])
KST = ZoneInfo("Asia/Seoul")


# =========================================================
# ✅ 공용 ENGINE 우선 사용 (DB 불일치 방지)
# =========================================================
def _get_shared_engine() -> Optional[Engine]:
    """
    app.db.session.py에 이미 엔진/세션이 있으면 그걸 최우선 사용.
    - 이름이 ENGINE 또는 engine 둘 다 케어
    """
    try:
        from app.db import session as s  # type: ignore
        eng = getattr(s, "ENGINE", None) or getattr(s, "engine", None)
        return eng
    except Exception:
        return None


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


_SHARED = _get_shared_engine()

ENGINE: Engine = _SHARED or create_engine(
    _build_db_url(),
    pool_pre_ping=True,
    pool_size=int(os.getenv("PG_POOL_SIZE", "6")),
    max_overflow=int(os.getenv("PG_MAX_OVERFLOW", "6")),
    pool_timeout=int(os.getenv("PG_POOL_TIMEOUT", "3")),
    pool_recycle=int(os.getenv("PG_POOL_RECYCLE", "900")),
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
    return json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )


def _now_iso() -> str:
    return datetime.now(tz=KST).isoformat(timespec="seconds")


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


def _ts_expr(schema: str, table: str, *, prefer_updated_at: bool = True) -> str:
    has_updated = prefer_updated_at and _has_column(schema, table, "updated_at")
    has_created = _has_column(schema, table, "created_at")

    if has_updated and has_created:
        return "COALESCE(updated_at, created_at, now())"
    if has_updated and not has_created:
        return "COALESCE(updated_at, now())"
    if (not has_updated) and has_created:
        return "COALESCE(created_at, now())"
    return "now()"


def _alarm_pk_from_row(row: Dict[str, Any]) -> str:
    key = {
        "end_day": _normalize_day(row.get("end_day", "")),
        "end_time": str(row.get("end_time", "") or "").strip(),
        "station": str(row.get("station", "") or "").strip(),
        "sparepart": str(row.get("sparepart", "") or "").strip(),
        "type_alarm": str(row.get("type_alarm", "") or "").strip(),
        "id": str(row.get("id", "") or "").strip(),
    }
    raw = json.dumps(key, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _sec_query_alarm_latest_ignore_shift() -> Any:
    """
    ✅ alarm 토큰은 'row 기반'으로만 만들어야 한다.
    - now() 같은 휘발값 절대 금지
    - end_day/end_time/station/sparepart/type_alarm 만 반환
    - PK는 현재 DB는 id 단독이므로, 정렬은 end_time + updated/created + id
    """
    ts_expr = _ts_expr("g_production_film", "alarm_record", prefer_updated_at=True)
    if ts_expr.strip().lower() == "now()":
        ts_expr = "TIMESTAMPTZ '1970-01-01 00:00:00+00'"

    return text(f"""
        SELECT
            id,
            replace(end_day::text,'-','') AS end_day,
            station,
            sparepart,
            type_alarm,
            COALESCE(end_time::text,'') AS end_time
        FROM g_production_film.alarm_record
        WHERE replace(end_day::text,'-','') = :end_day
          AND COALESCE(type_alarm::text,'') IN ('준비','권고','긴급','교체')
        ORDER BY
            end_time::time DESC NULLS LAST,
            {ts_expr} DESC NULLS LAST,
            id DESC
        LIMIT 1
    """)


def _sec_query_nonop_cursor_for_shift() -> Any:
    return text("""
        SELECT
            prod_day,
            shift_type,
            COALESCE(MAX(id), 0) AS max_id,
            COALESCE(MAX(updated_at), TIMESTAMPTZ '1970-01-01 00:00:00+00') AS max_updated_at
        FROM i_daily_report.total_non_operation_time
        WHERE prod_day = :end_day
          AND shift_type = :shift_type
        GROUP BY prod_day, shift_type
    """)


def _sec_query_mastersample_for_shift(shift_type: str) -> Any:
    table = (
        "i_daily_report.e_mastersample_test_day_daily"
        if shift_type == "day"
        else "i_daily_report.e_mastersample_test_night_daily"
    )
    schema, tbl = table.split(".", 1)
    ts = "COALESCE(updated_at, now())" if _has_column(schema, tbl, "updated_at") else "now()"

    return text(f"""
        SELECT
            prod_day,
            shift_type,
            COALESCE("Mastersample"::text,'') AS mastersample,
            COALESCE(first_time::text,'')     AS first_time,
            {ts}                              AS updated_at
        FROM {table}
        WHERE prod_day = :end_day
          AND shift_type = :shift_type
        ORDER BY {ts} DESC NULLS LAST
        LIMIT 1
    """)


def _get_section_tokens(end_day: str, shift_type: str, *, debug: bool = False) -> Dict[str, str]:
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

    planned_ts = _ts_expr("g_production_film", "planned_time", prefer_updated_at=True)
    email_ts = _ts_expr("g_production_film", "email_list", prefer_updated_at=True)
    remark_ts = _ts_expr("g_production_film", "remark_info", prefer_updated_at=True)

    nonop_cursor_sql = _sec_query_nonop_cursor_for_shift()

    queries = {
        "alarm": _sec_query_alarm_latest_ignore_shift(),

        "chart": nonop_cursor_sql,
        "nonop_detail": nonop_cursor_sql,

        "worker_info": text("""
            SELECT
                replace(end_day::text,'-','') AS end_day,
                shift_type,
                worker_name,
                COALESCE(order_number,'') AS order_number
            FROM g_production_film.worker_info
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY worker_name DESC
            LIMIT 1
        """),

        "mastersample": _sec_query_mastersample_for_shift(shift_type),

        "planned": text(f"""
            SELECT
                replace(prod_day::text,'-','') AS prod_day,
                shift_type,
                from_time,
                to_time,
                {planned_ts} AS ts
            FROM g_production_film.planned_time
            WHERE replace(prod_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY ts DESC NULLS LAST, from_time DESC NULLS LAST
            LIMIT 1
        """),

        "email": text(f"""
            SELECT
                replace(end_day::text,'-','') AS end_day,
                shift_type,
                email,
                {email_ts} AS ts
            FROM g_production_film.email_list
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY ts DESC NULLS LAST, email DESC
            LIMIT 1
        """),

        "barcode": text(f"""
            SELECT
                replace(end_day::text,'-','') AS end_day,
                shift_type,
                barcode_information,
                COALESCE(pn,'')     AS pn,
                COALESCE(remark,'') AS remark,
                {remark_ts} AS ts
            FROM g_production_film.remark_info
            WHERE replace(end_day::text,'-','') = :end_day
              AND shift_type = :shift_type
            ORDER BY ts DESC NULLS LAST, barcode_information DESC
            LIMIT 1
        """),
    }

    params = {"end_day": end_day, "shift_type": shift_type}

    for sec, sql in queries.items():
        try:
            with ENGINE.begin() as conn:
                row = conn.execute(sql, params).mappings().first()

                if sec in ("chart", "nonop_detail"):
                    if row:
                        out[sec] = _token_of(dict(row))
                    else:
                        out[sec] = _token_of(
                            {
                                "prod_day": end_day,
                                "shift_type": shift_type,
                                "max_id": 0,
                                "max_updated_at": "1970-01-01T00:00:00+00:00",
                            }
                        )
                    continue

                if row:
                    out[sec] = _token_of(dict(row))

        except Exception as e:
            out[sec] = f"__ERR__:{type(e).__name__}:{e}" if debug else ""

    return out


@router.get("/sections/latest")
def sections_latest(
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: str = Query(..., description="day|night"),
    debug: int = Query(0, description="1이면 에러를 토큰에 노출"),
):
    d = _normalize_day(end_day)
    s = _safe_shift(shift_type)
    if not d:
        raise HTTPException(status_code=422, detail="end_day must be YYYYMMDD")
    if not s:
        raise HTTPException(status_code=422, detail="shift_type must be day|night")

    try:
        tokens = _get_section_tokens(d, s, debug=(int(debug) == 1))
        return JSONResponse(
            {
                "ok": True,
                "end_day": d,
                "shift_type": s,
                "tokens": tokens,
                "ts": _now_iso(),
            }
        )
    except SQLAlchemyError as e:
        raise HTTPException(status_code=500, detail=f"DB error: {type(e).__name__}: {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Unexpected error: {type(e).__name__}: {e}")


# =========================================================
# ✅ SSE Stream: LISTEN/NOTIFY alarm_record
# =========================================================
def _sse_pack(event: str, data: Dict[str, Any]) -> str:
    # SSE spec: event + data lines + blank line
    payload = json.dumps(data, ensure_ascii=False, separators=(",", ":"), default=str)
    return f"event: {event}\ndata: {payload}\n\n"


def _sse_comment_keepalive() -> str:
    # comment line is ignored by client but keeps connection alive
    return ": keepalive\n\n"


def _listen_alarm_stream(
    end_day: str,
    shift_type: str,
    sections: str,
) -> Iterable[bytes]:
    """
    - DB raw_connection 1개로 LISTEN 유지
    - notify 오면 payload(json) -> event: alarm
    - 최초 접속시: 최신 alarm 1회 보내줌(init)
    """
    want_alarm = ("alarm" in {s.strip() for s in (sections or "alarm").split(",") if s.strip()})
    if not want_alarm:
        # 그래도 연결은 유지 (혹시 확장 대비), 하지만 현재는 alarm만 지원
        want_alarm = True

    raw_conn = None
    cur = None

    # 최초 init: 최신 alarm 토큰 1회 push (클라이언트가 "바로" 띄울 수 있게)
    try:
        row_init: Optional[Dict[str, Any]] = None
        with ENGINE.begin() as conn:
            r = conn.execute(_sec_query_alarm_latest_ignore_shift(), {"end_day": end_day}).mappings().first()
            if r:
                row_init = dict(r)

        init_payload: Dict[str, Any] = {
            "ts": _now_iso(),
            "end_day": end_day,
            "shift_type": shift_type,
            "row": row_init or None,
            "pk": _alarm_pk_from_row(row_init) if row_init else "",
        }
        yield _sse_pack("init", init_payload).encode("utf-8")

        # LISTEN 시작
        raw_conn = ENGINE.raw_connection()
        raw_conn.set_session(autocommit=True)

        cur = raw_conn.cursor()
        cur.execute("LISTEN alarm_record;")

        last_keepalive = time.time()

        while True:
            # psycopg2: poll()로 notify 확인
            raw_conn.poll()
            notifies = []
            try:
                notifies = list(getattr(raw_conn, "notifies", []))
                # consume
                raw_conn.notifies[:] = []
            except Exception:
                notifies = []

            if notifies:
                for n in notifies:
                    payload_txt = ""
                    try:
                        payload_txt = getattr(n, "payload", "") or ""
                    except Exception:
                        payload_txt = ""

                    row = None
                    if payload_txt:
                        try:
                            obj = json.loads(payload_txt)
                            row = obj if isinstance(obj, dict) else None
                        except Exception:
                            row = None

                    # payload에 end_day가 "YYYY-MM-DD"로 올 수도 있으니 normalize
                    if isinstance(row, dict):
                        row_end_day = _normalize_day(row.get("end_day", ""))
                        if row_end_day:
                            row["end_day"] = row_end_day

                    # end_day 필터: 오늘/조회일만 받고 싶으면 유지
                    # (너 사양: alarm은 row insert 기반이고 update 없음)
                    if isinstance(row, dict):
                        if _normalize_day(row.get("end_day")) != end_day:
                            continue

                    out = {
                        "ts": _now_iso(),
                        "end_day": end_day,
                        "shift_type": shift_type,
                        "row": row,
                        "pk": _alarm_pk_from_row(row) if isinstance(row, dict) else "",
                    }
                    yield _sse_pack("alarm", out).encode("utf-8")

            # keepalive every 15s
            now = time.time()
            if now - last_keepalive >= 15.0:
                last_keepalive = now
                yield _sse_comment_keepalive().encode("utf-8")

            time.sleep(0.25)

    except GeneratorExit:
        return
    except Exception as e:
        # 연결이 깨져도 클라이언트가 재연결하게끔 error event
        err_payload = {"ts": _now_iso(), "error": f"{type(e).__name__}: {e}"}
        yield _sse_pack("error", err_payload).encode("utf-8")
        return
    finally:
        try:
            if cur is not None:
                cur.close()
        except Exception:
            pass
        try:
            if raw_conn is not None:
                raw_conn.close()
        except Exception:
            pass


@router.get("/stream")
def events_stream(
    end_day: str = Query(..., description="YYYYMMDD"),
    shift_type: str = Query(..., description="day|night"),
    sections: str = Query("alarm", description="comma-separated; currently supports alarm"),
    x_admin_pass: Optional[str] = Header(default=None, alias="X-ADMIN-PASS"),
):
    """
    ✅ SSE endpoint
    - 브라우저(JS EventSource)가 직접 구독
    - 알람 insert 시 즉시 push
    """
    d = _normalize_day(end_day)
    s = _safe_shift(shift_type)
    if not d:
        raise HTTPException(status_code=422, detail="end_day must be YYYYMMDD")
    if not s:
        raise HTTPException(status_code=422, detail="shift_type must be day|night")

    # (선택) 간단 보호: ADMIN_PASS가 설정돼 있으면 헤더 검증
    admin_pass = os.getenv("ADMIN_PASS", "").strip()
    if admin_pass:
        if (x_admin_pass or "").strip() != admin_pass:
            raise HTTPException(status_code=401, detail="Unauthorized")

    gen = _listen_alarm_stream(d, s, sections or "alarm")
    return StreamingResponse(gen, media_type="text/event-stream")