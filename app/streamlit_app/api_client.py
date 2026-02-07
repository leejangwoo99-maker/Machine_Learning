# app/streamlit_app/api_client.py
from __future__ import annotations

import os
from typing import Any, Optional

import requests

API = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
TIMEOUT = float(os.getenv("API_TIMEOUT_SEC", "30"))

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


# ---------- 2. worker_info ----------
def get_worker_info(end_day: str, shift_type: str):
    return _req("GET", "/worker_info", params={"end_day": end_day, "shift_type": shift_type})


def post_worker_info(end_day: str, shift_type: str, worker_name: str, order_number: Optional[str] = None):
    body = {"end_day": end_day, "shift_type": shift_type, "worker_name": worker_name}
    if order_number is not None and str(order_number).strip() != "":
        body["order_number"] = str(order_number).strip()
    return _req("POST", "/worker_info", json_body=body)


def post_worker_info_sync(rows: list[dict]):
    """
    rows 예시:
    [
      {"end_day":"20260205","shift_type":"day","worker_name":"홍길동","order_number":"123"},
      ...
    ]
    """
    return _req("POST", "/worker_info/sync", json_body={"rows": rows})


# ---------- 3. email_list ----------
def get_email_list():
    return _req("GET", "/email_list")


def post_email_list_sync(emails: list[str], admin_password: str):
    headers = {"X-ADMIN-PASS": admin_password}
    return _req("POST", "/email_list/sync", json_body={"emails": emails}, headers=headers)


# ---------- 4. remark_info ----------
def get_remark_info():
    return _req("GET", "/remark_info")


def post_remark_info_sync(rows: list[dict], admin_password: str):
    """
    rows 예시:
    [
      {"barcode_information":"J","pn":"TEST_PN","remark":"테스트 비고"},
      ...
    ]
    """
    headers = {"X-ADMIN-PASS": admin_password}
    return _req("POST", "/remark_info/sync", json_body={"rows": rows}, headers=headers)


# ---------- 5. mastersample ----------
def get_mastersample(prod_day: str, shift_type: str):
    # reports의 e_mastersample_test endpoint 재사용
    return _req("GET", f"/report/e_mastersample_test/{prod_day}", params={"shift_type": shift_type})


# ---------- 6. planned_time ----------
def get_planned_today():
    return _req("GET", "/planned_time/today")


def post_planned_today(from_time: str, to_time: str, reason: str, end_day: Optional[str] = None):
    body = {
        "from_time": from_time,
        "to_time": to_time,
        "reason": reason,
    }
    if end_day:
        body["end_day"] = end_day
    return _req("POST", "/planned_time/today", json_body=body)


def post_planned_time_sync(rows: list[dict]):
    # rows: [{"from_time":"HH:MM:SS","to_time":"HH:MM:SS","reason":"..."}, ...]
    return _req("POST", "/planned_time/sync", json_body={"rows": rows})


# ---------- 7. non_operation_time ----------
def get_non_operation_time(end_day: str, shift_type: str):
    return _req("GET", "/non_operation_time", params={"end_day": end_day, "shift_type": shift_type})


def post_non_operation_time(
    end_day: str,
    station: str,
    from_time: str,
    to_time: str,
    reason: Optional[str],
    sparepart: Optional[str],
):
    body = {
        "end_day": end_day,
        "station": station,
        "from_time": from_time,
        "to_time": to_time,
        "reason": reason,
        "sparepart": sparepart,
    }
    return _req("POST", "/non_operation_time", json_body=body)


def post_non_operation_time_sync(rows: list[dict]):
    return _req("POST", "/non_operation_time/sync", json_body={"rows": rows})


# ---------- 8. worst_case ----------
def get_worst_case(prod_day: str, shift_type: str):
    # worst_case 전용 호출
    return _req("GET", f"/report/f_worst_case/{prod_day}", params={"shift_type": shift_type})


# ---------- 9. alarm_record ----------
def get_alarm_records(end_day: str):
    paths = ["/alarm_record/recent5", "/alarm_record/recent", "/alarm_record"]
    last_err = None
    for p in paths:
        try:
            return _req("GET", p, params={"end_day": end_day})
        except requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 404:
                last_err = e
                continue
            raise
    if last_err:
        raise last_err
    raise RuntimeError("alarm_record endpoint not found")


# ---------- 10. pd_board_check ----------
def get_pd_board_check(prod_day: str):
    return _req("GET", f"/predictive/pd-board-check/{prod_day}")


# ---------- 11. reports ----------
_REPORT_ENDPOINTS = {
    "a_station_final_amount": "/report/a_station_final_amount/{prod_day}",
    "b_station_percentage": "/report/b_station_percentage/{prod_day}",
    "c_fct_step_1time": "/report/c_fct_step_1time/{prod_day}",
    "c_fct_step_2time": "/report/c_fct_step_2time/{prod_day}",
    "c_fct_step_3over": "/report/c_fct_step_3over/{prod_day}",
    "d_vision_step_1time": "/report/d_vision_step_1time/{prod_day}",
    "d_vision_step_2time": "/report/d_vision_step_2time/{prod_day}",
    "d_vision_step_3over": "/report/d_vision_step_3over/{prod_day}",
    "e_mastersample_test": "/report/e_mastersample_test/{prod_day}",
    "f_worst_case": "/report/f_worst_case/{prod_day}",
    "g_afa_wasted_time": "/report/g_afa_wasted_time/{prod_day}",
    "h_mes_wasted_time": "/report/h_mes_wasted_time/{prod_day}",
    "i_planned_stop_time": "/report/i_planned_stop_time/{prod_day}",
    "i_non_time": "/report/i_non_time/{prod_day}",
    "k_oee_line": "/report/k_oee_line/{prod_day}",
    "k_oee_station": "/report/k_oee_station/{prod_day}",
    "k_oee_total": "/report/k_oee_total/{prod_day}",
}


def get_report(name: str, prod_day: str, shift_type: str):
    if name not in _REPORT_ENDPOINTS:
        raise ValueError(f"unknown report name: {name}")
    path = _REPORT_ENDPOINTS[name].format(prod_day=prod_day)
    return _req("GET", path, params={"shift_type": shift_type})
