# app/streamlit_app/api_client.py
from __future__ import annotations

import os
from typing import Any, Optional

import requests

API = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
# 타임아웃 상향 (기본 30초)
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


# ---------- 3. email_list ----------
def get_email_list():
    return _req("GET", "/email_list")


def post_email_list(email: str, admin_password: Optional[str] = None):
    """
    권장:
    - POST /email_list
    - header: X-ADMIN-PASS
    - body: {"email":"..."}
    하위호환:
    - /email_list/sync, /email_list/post
    - 비밀번호를 body로 받는 서버도 대응
    """
    paths = [
        "/email_list",        # 권장
        "/email_list/sync",   # 하위호환
        "/email_list/post",   # 하위호환
    ]

    header_variants = [None]
    if admin_password:
        header_variants.extend([
            {"X-ADMIN-PASS": admin_password},
            {"x-admin-pass": admin_password},
        ])

    body_variants = [{"email": email}]
    if admin_password:
        body_variants.extend([
            {"email": email, "admin_password": admin_password},
            {"email": email, "password": admin_password},
        ])

    last_err = None
    for path in paths:
        for headers in header_variants:
            for body in body_variants:
                try:
                    return _req("POST", path, json_body=body, headers=headers)
                except requests.HTTPError as e:
                    last_err = e
                    continue

    if last_err:
        raise last_err
    raise RuntimeError("POST email_list failed")


# ---------- 4. remark_info ----------
def get_remark_info():
    return _req("GET", "/remark_info")


def post_remark_info(
    key: str,
    pn: str,
    remark: str,
    admin_password: Optional[str] = None,
    barcode_information: Optional[str] = None,
):
    """
    Swagger/서버 편차 대응:
    - /remark_info/{key} 또는 /remark_info
    - X-ADMIN-PASS 헤더 우선
    - body 키명 편차(admin_password/password)도 보조 시도
    - 서버가 barcode_information 필수인 경우 대응
    """
    path_candidates = [f"/remark_info/{key}", "/remark_info"]

    base1 = {"pn": pn, "remark": remark}
    base2 = {"key": key, "pn": pn, "remark": remark}

    if barcode_information is not None and str(barcode_information).strip() != "":
        base1["barcode_information"] = str(barcode_information).strip()
        base2["barcode_information"] = str(barcode_information).strip()

    body_candidates = [base1, base2]
    if admin_password:
        body_candidates.extend([
            {**base1, "admin_password": admin_password},
            {**base1, "password": admin_password},
            {**base2, "admin_password": admin_password},
            {**base2, "password": admin_password},
        ])

    header_variants = [None]
    if admin_password:
        header_variants.extend([
            {"X-ADMIN-PASS": admin_password},
            {"x-admin-pass": admin_password},
        ])

    last_err = None
    for path in path_candidates:
        for headers in header_variants:
            for body in body_candidates:
                try:
                    return _req("POST", path, json_body=body, headers=headers)
                except requests.HTTPError as e:
                    last_err = e
                    continue

    if last_err:
        raise last_err
    raise RuntimeError("POST remark_info failed")


# ---------- 6. planned_time ----------
def get_planned_today():
    return _req("GET", "/planned_time/today")


def post_planned_today(from_time: str, to_time: str, reason: str, end_day: Optional[str] = None):
    """
    station 없이 today 기준 POST
    서버가 end_day를 받는 경우를 위해 optional로 실어줌
    """
    body = {
        "from_time": from_time,
        "to_time": to_time,
        "reason": reason,
    }
    if end_day:
        body["end_day"] = end_day
    return _req("POST", "/planned_time/today", json_body=body)


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


# ---------- 9. alarm_record ----------
def get_alarm_records(end_day: str):
    # Swagger 기준: /alarm_record/recent?end_day=YYYYMMDD
    return _req("GET", "/alarm_record/recent", params={"end_day": end_day})


# ---------- 10. pd_board_check ----------
def get_pd_board_check(prod_day: str):
    # Swagger 기준: /predictive/pd-board-check/{prod_day}
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
