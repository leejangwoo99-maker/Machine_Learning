# app/routers/reports.py
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Dict, List, Tuple

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.orm import Session

from app.db.session import get_db

router = APIRouter(prefix="/report", tags=["8.reports"])

SCHEMA = "i_daily_report"


# -------------------------
# Utils
# -------------------------
def norm_day(v: str) -> str:
    s = (v or "").strip()
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        s = s.replace("-", "")
    if len(s) != 8 or not s.isdigit():
        raise HTTPException(status_code=422, detail="prod_day must be YYYYMMDD or YYYY-MM-DD")
    return s


def norm_shift(v: str) -> str:
    s = (v or "").strip().lower()
    if s not in {"day", "night"}:
        raise HTTPException(status_code=422, detail="shift_type must be day/night")
    return s


_ident_re = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def q_ident(name: str) -> str:
    # SQL identifier-safe quoting
    if not isinstance(name, str) or not name:
        raise HTTPException(status_code=500, detail="invalid identifier")
    if _ident_re.match(name):
        return f'"{name}"'
    # fallback: quote and escape double quotes
    return '"' + name.replace('"', '""') + '"'


def load_columns(db: Session, schema: str, table: str) -> List[str]:
    rows = db.execute(
        text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema
              AND table_name = :table
            ORDER BY ordinal_position
            """
        ),
        {"schema": schema, "table": table},
    ).mappings().all()
    return [r["column_name"] for r in rows]


def resolve_table(logical_name: str, shift_type: str) -> str:
    # logical_name -> physical table suffix map
    # NOTE: decription 泥좎옄 intentionally kept as-is (actual table names)
    mapping: Dict[str, Tuple[str, str]] = {
        "a_station_final_amount": ("a_station_day_daily_final_amount", "a_station_night_daily_final_amount"),
        # ???꾨씫 ?ъ뼇 異붽?
        "b_station_percentage": ("b_station_day_daily_percentage", "b_station_night_daily_percentage"),
        "c_fct_step_1time": ("c_1time_step_decription_day_daily", "c_1time_step_decription_night_daily"),
        "c_fct_step_2time": ("c_2time_step_decription_day_daily", "c_2time_step_decription_night_daily"),
        "c_fct_step_3over": ("c_3time_over_step_decription_day_daily", "c_3time_over_step_decription_night_daily"),
        "d_vision_step_1time": ("d_vs_1time_step_decription_day_daily", "d_vs_1time_step_decription_night_daily"),
        "d_vision_step_2time": ("d_vs_2time_step_decription_day_daily", "d_vs_2time_step_decription_night_daily"),
        "d_vision_step_3over": ("d_vs_3time_over_step_decription_day_daily", "d_vs_3time_over_step_decription_night_daily"),
        "k_oee_line": ("k_line_oee_day_daily", "k_line_oee_night_daily"),
        "k_oee_station": ("k_station_oee_day_daily", "k_station_oee_night_daily"),
        "k_oee_total": ("k_total_oee_day_daily", "k_total_oee_night_daily"),
        "f_worst_case": ("f_worst_case_day_daily", "f_worst_case_night_daily"),
        "g_afa_wasted_time": ("g_afa_wasted_time_day_daily", "g_afa_wasted_time_night_daily"),
        "h_mes_wasted_time": ("h_mes_wasted_time_day_daily", "h_mes_wasted_time_night_daily"),
        "i_planned_stop_time": ("i_planned_stop_time_day_daily", "i_planned_stop_time_night_daily"),
        "i_non_time": ("i_non_time_day_daily", "i_non_time_night_daily"),
    }
    if logical_name not in mapping:
        raise HTTPException(status_code=500, detail=f"unknown report logical name: {logical_name}")
    day_tbl, night_tbl = mapping[logical_name]
    return day_tbl if shift_type == "day" else night_tbl


def fetch_report_rows(
    db: Session,
    logical_name: str,
    prod_day: str,
    shift_type: str,
):
    d = norm_day(prod_day)
    s = norm_shift(shift_type)

    table = resolve_table(logical_name, s)
    cols = load_columns(db, SCHEMA, table)
    if not cols:
        raise HTTPException(status_code=500, detail=f"{SCHEMA}.{table} has no selectable columns")

    if "prod_day" not in cols:
        raise HTTPException(status_code=500, detail=f"{SCHEMA}.{table} must have prod_day column")

    # shift_type 而щ읆 ?덉쑝硫??뺥솗???꾪꽣, ?놁쑝硫?prod_day留??꾪꽣
    has_shift = "shift_type" in cols

    # updated_at ?쒖쇅 (?ъ뼇)
    out_cols = [c for c in cols if c != "updated_at"]
    if not out_cols:
        raise HTTPException(status_code=500, detail=f"{SCHEMA}.{table} has no selectable columns after exclusion")

    col_sql = ", ".join(q_ident(c) for c in out_cols)

    where = ['replace(cast("prod_day" as text), \'-\', \'\') = :d']
    params = {"d": d}
    if has_shift:
        where.append('lower("shift_type") = :s')
        params["s"] = s

    # ?뺣젹: end_time ?곗꽑, 洹???station/line/prod_day
    if "end_time" in cols:
        order_sql = 'ORDER BY "end_time" DESC'
    elif "station" in cols:
        order_sql = 'ORDER BY "station" ASC'
    elif "line" in cols:
        order_sql = 'ORDER BY "line" ASC'
    else:
        order_sql = 'ORDER BY "prod_day" DESC'

    sql = f"""
        SELECT {col_sql}
        FROM {q_ident(SCHEMA)}.{q_ident(table)}
        WHERE {" AND ".join(where)}
        {order_sql}
    """

    rows = db.execute(text(sql), params).mappings().all()
    return [dict(r) for r in rows]


def _make_response(data: list):
    body = json.dumps(data, ensure_ascii=False, default=str)
    return body


# -------------------------
# Endpoints
# -------------------------
@router.get("/a_station_final_amount/{prod_day}")
def report_a_station_final_amount(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "a_station_final_amount", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


# ???꾨씫遺?異붽?
@router.get("/b_station_percentage/{prod_day}")
def report_b_station_percentage(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "b_station_percentage", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/c_fct_step_1time/{prod_day}")
def report_c_fct_step_1time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "c_fct_step_1time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/c_fct_step_2time/{prod_day}")
def report_c_fct_step_2time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "c_fct_step_2time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/c_fct_step_3over/{prod_day}")
def report_c_fct_step_3over(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "c_fct_step_3over", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/d_vision_step_1time/{prod_day}")
def report_d_vision_step_1time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "d_vision_step_1time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/d_vision_step_2time/{prod_day}")
def report_d_vision_step_2time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "d_vision_step_2time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/d_vision_step_3over/{prod_day}")
def report_d_vision_step_3over(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "d_vision_step_3over", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/k_oee_line/{prod_day}")
def report_k_oee_line(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "k_oee_line", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/k_oee_station/{prod_day}")
def report_k_oee_station(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "k_oee_station", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/k_oee_total/{prod_day}")
def report_k_oee_total(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "k_oee_total", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/f_worst_case/{prod_day}")
def report_f_worst_case(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "f_worst_case", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/g_afa_wasted_time/{prod_day}")
def report_g_afa_wasted_time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "g_afa_wasted_time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/h_mes_wasted_time/{prod_day}")
def report_h_mes_wasted_time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "h_mes_wasted_time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/i_planned_stop_time/{prod_day}")
def report_i_planned_stop_time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "i_planned_stop_time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e


@router.get("/i_non_time/{prod_day}")
def report_i_non_time(prod_day: str, shift_type: str, db: Session = Depends(get_db)):
    try:
        rows = fetch_report_rows(db, "i_non_time", prod_day, shift_type)
        return json.loads(_make_response(rows))
    except HTTPException:
        raise
    except (OperationalError, DBAPIError) as e:
        raise HTTPException(status_code=503, detail=f"report db error: {e}") from e
