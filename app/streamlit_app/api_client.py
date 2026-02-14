# app/streamlit_app/api_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
from typing import Any, Optional, Dict, List

import requests

API = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
TIMEOUT = float(os.getenv("API_TIMEOUT_SEC", "90"))
ADMIN_PASS = os.getenv("ADMIN_PASS", "leejangwoo1!")

_session = requests.Session()


def _url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{API}{path}"


def _req(
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    headers: Optional[dict] = None,
) -> Any:
    r = _session.request(
        method=method.upper(),
        url=_url(path),
        params=params,
        json=json_body,
        headers=headers,
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "application/json" in ct:
        return r.json()
    return r.text


# =========================
# 공통 유틸
# =========================
def _q(end_day: str, shift_type: str) -> Dict[str, str]:
    return {
        "end_day": str(end_day),
        "shift_type": str(shift_type),
    }


def _admin_header(password: str) -> Dict[str, str]:
    # 사양: header 이름은 X-ADMIN-PASS
    return {"X-ADMIN-PASS": str(password or "")}


def _extract_rows(resp: Any) -> List[dict]:
    """
    백엔드 응답 호환:
    - list 그대로
    - {"rows":[...]}
    - {"data":[...]}
    - {"items":[...]}
    """
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for k in ("rows", "data", "items"):
            v = resp.get(k)
            if isinstance(v, list):
                return v
    return []


# =========================
# Health / Misc
# =========================
def get_health() -> Any:
    return _req("GET", "/health")


def get_events(limit: int = 50) -> Any:
    return _req("GET", "/events", params={"limit": limit})


# =========================
# worker_info
# =========================
def get_worker_info(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/worker_info", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_worker_info_sync(end_day: str, shift_type: str, rows: List[dict]) -> Any:
    body = {
        "end_day": str(end_day),
        "shift_type": str(shift_type),
        "rows": rows or [],
    }
    return _req("POST", "/worker_info/sync", json_body=body)


# =========================
# email_list
# =========================
def get_email_list(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/email_list", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_email_list_sync(
    end_day: str,
    shift_type: str,
    rows: List[dict],
    *,
    password: str,
) -> Any:
    body = {
        "end_day": str(end_day),
        "shift_type": str(shift_type),
        "rows": rows or [],
    }
    return _req(
        "POST",
        "/email_list/sync",
        json_body=body,
        headers=_admin_header(password),
    )


# =========================
# remark_info (barcode)
# =========================
def get_remark_info(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/remark_info", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_remark_info_sync(
    end_day: str,
    shift_type: str,
    rows: List[dict],
    *,
    password: str,
) -> Any:
    body = {
        "end_day": str(end_day),
        "shift_type": str(shift_type),
        "rows": rows or [],
    }
    return _req(
        "POST",
        "/remark_info/sync",
        json_body=body,
        headers=_admin_header(password),
    )


# =========================
# planned_time
# =========================
def get_planned_time(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/planned_time", params=_q(end_day, shift_type))
    return _extract_rows(res)


def get_planned_time_today(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/planned_time/today", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_planned_time_sync(end_day: str, shift_type: str, rows: List[dict]) -> Any:
    body = {
        "end_day": str(end_day),
        "shift_type": str(shift_type),
        "rows": rows or [],
    }
    return _req("POST", "/planned_time/sync", json_body=body)


# =========================
# non_operation_time
# =========================
def get_non_operation_time(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/non_operation_time", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_non_operation_time(
    end_day: str,
    shift_type: str,
    rows: List[dict],
) -> Any:
    """
    UPDATE ONLY 저장:
    - /non_operation_time 로 POST
    - 서버가 query end_day/shift_type + body rows를 사용
    """
    return _req(
        "POST",
        "/non_operation_time",
        params={"end_day": str(end_day), "shift_type": str(shift_type)},
        json_body={"rows": rows or []},
    )


def post_non_operation_time_sync(
    end_day: str,
    shift_type: str,
    payload: Any,
) -> Any:
    """
    비가동 상세 저장(sync)

    payload 허용 형태:
    1) list[dict]  -> {"rows":[...]}로 변환
    2) {"rows":[...]} dict -> 그대로 사용
    """
    if isinstance(payload, dict):
        body = payload
    else:
        body = {"rows": payload or []}

    return _req(
        "POST",
        "/non_operation_time/sync",
        params={"end_day": str(end_day), "shift_type": str(shift_type)},
        json_body=body,
    )


# =========================
# alarm_record
# =========================
def get_alarm_record(end_day: str, shift_type: str) -> List[dict]:
    # 필요 시 /alarm_record가 없을 수 있어 유지하되 호출처에서 recent 우선 사용
    res = _req("GET", "/alarm_record", params=_q(end_day, shift_type))
    return _extract_rows(res)


def get_alarm_record_recent(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/alarm_record/recent", params=_q(end_day, shift_type))
    return _extract_rows(res)


# =========================
# mastersample
# =========================
def get_mastersample_test_info(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/mastersample_test_info", params=_q(end_day, shift_type))
    return _extract_rows(res)


# =========================
# production progress chart
# =========================
def get_production_progress_graph(
    end_day: str,
    shift_type: str,
    slot_minutes: int = 60,
) -> Any:
    params = {
        "end_day": str(end_day),
        "shift_type": str(shift_type),
        "slot_minutes": int(slot_minutes),
    }
    return _req("GET", "/production_progress_graph", params=params)


# =========================
# report endpoints
# =========================
def get_report_a_station_final_amount(prod_day: str) -> Any:
    return _req("GET", f"/report/a_station_final_amount/{prod_day}")


def get_report_b_station_percentage(prod_day: str) -> Any:
    return _req("GET", f"/report/b_station_percentage/{prod_day}")


def get_report_c_fct_step_1time(prod_day: str) -> Any:
    return _req("GET", f"/report/c_fct_step_1time/{prod_day}")


def get_report_c_fct_step_2time(prod_day: str) -> Any:
    return _req("GET", f"/report/c_fct_step_2time/{prod_day}")


def get_report_c_fct_step_3over(prod_day: str) -> Any:
    return _req("GET", f"/report/c_fct_step_3over/{prod_day}")


def get_report_d_vision_step_1time(prod_day: str) -> Any:
    return _req("GET", f"/report/d_vision_step_1time/{prod_day}")


def get_report_d_vision_step_2time(prod_day: str) -> Any:
    return _req("GET", f"/report/d_vision_step_2time/{prod_day}")


def get_report_d_vision_step_3over(prod_day: str) -> Any:
    return _req("GET", f"/report/d_vision_step_3over/{prod_day}")


def get_report_k_oee_line(prod_day: str) -> Any:
    return _req("GET", f"/report/k_oee_line/{prod_day}")


def get_report_k_oee_station(prod_day: str) -> Any:
    return _req("GET", f"/report/k_oee_station/{prod_day}")


def get_report_k_oee_total(prod_day: str) -> Any:
    return _req("GET", f"/report/k_oee_total/{prod_day}")


def get_report_f_worst_case(prod_day: str) -> Any:
    return _req("GET", f"/report/f_worst_case/{prod_day}")


def get_report_g_afa_wasted_time(prod_day: str) -> Any:
    return _req("GET", f"/report/g_afa_wasted_time/{prod_day}")


def get_report_h_mes_wasted_time(prod_day: str) -> Any:
    return _req("GET", f"/report/h_mes_wasted_time/{prod_day}")


def get_report_i_planned_stop_time(prod_day: str) -> Any:
    return _req("GET", f"/report/i_planned_stop_time/{prod_day}")


def get_report_i_non_time(prod_day: str) -> Any:
    return _req("GET", f"/report/i_non_time/{prod_day}")


# =========================
# SSE / latest
# =========================
def get_events_latest(end_day: str, shift_type: str) -> Dict[str, Any]:
    return _req(
        "GET",
        "/events/latest",
        params={"end_day": str(end_day), "shift_type": str(shift_type)},
    )


def get_sections_latest(end_day: str, shift_type: str) -> Dict[str, Any]:
    return _req(
        "GET",
        "/events/sections/latest",
        params={"end_day": str(end_day), "shift_type": str(shift_type)},
    )


def open_sse_stream(end_day: str, shift_type: str, sections: Optional[str] = None):
    """
    requests.Response(stream=True) 반환
    streamlit에서 iter_lines로 소비
    """
    headers = {
        "Accept": "text/event-stream",
        "Cache-Control": "no-cache",
        "X-ADMIN-PASS": ADMIN_PASS,
    }
    params = {"end_day": str(end_day), "shift_type": str(shift_type)}
    if sections:
        params["sections"] = sections

    resp = _session.get(
        _url("/events/stream"),
        params=params,
        headers=headers,
        stream=True,
        timeout=None,  # SSE 장기 연결
    )
    resp.raise_for_status()
    return resp
