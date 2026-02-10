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


def _unwrap_rows(data: Any) -> Any:
    # 백엔드가 {"rows":[...]} 형태로 주는 endpoint와, 바로 list를 주는 endpoint 둘 다 흡수
    if isinstance(data, dict):
        for k in ("rows", "items", "data", "result"):
            v = data.get(k)
            if isinstance(v, list):
                return v
    return data


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


# ---------------- GET ----------------
def get_worker_info(end_day: str, shift_type: str):
    data = _req("GET", "/worker_info", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


def get_email_list(end_day: str, shift_type: str):
    data = _req("GET", "/email_list", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


def get_remark_info(end_day: str, shift_type: str):
    data = _req("GET", "/remark_info", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


def get_planned_time_today(end_day: str, shift_type: str):
    # 중요: /planned_time_today 아님 -> /planned_time/today
    data = _req("GET", "/planned_time/today", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


def get_non_operation_time(end_day: str, shift_type: str):
    data = _req("GET", "/non_operation_time", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


def get_mastersample_test_info(end_day: str, shift_type: str):
    data = _req("GET", "/mastersample_test_info", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


def get_alarm_record_recent(end_day: str, shift_type: str):
    # 중요: /alarm_records 아님 -> /alarm_record/recent
    data = _req("GET", "/alarm_record/recent", params={"end_day": end_day, "shift_type": shift_type})
    return _unwrap_rows(data)


# ---------------- POST(sync) ----------------
def post_worker_info_sync(end_day: str, shift_type: str, rows: list[dict]):
    return _req(
        "POST",
        "/worker_info/sync",
        json_body={"end_day": end_day, "shift_type": shift_type, "rows": rows},
    )


def post_email_list_sync(end_day: str, shift_type: str, rows: list[dict], password: str):
    return _req(
        "POST",
        "/email_list/sync",
        json_body={"end_day": end_day, "shift_type": shift_type, "password": password, "rows": rows},
    )


def post_remark_info_sync(end_day: str, shift_type: str, rows: list[dict], password: str):
    return _req(
        "POST",
        "/remark_info/sync",
        json_body={"end_day": end_day, "shift_type": shift_type, "password": password, "rows": rows},
    )


def post_planned_time_sync(end_day: str, shift_type: str, rows: list[dict]):
    return _req(
        "POST",
        "/planned_time/sync",
        json_body={"end_day": end_day, "shift_type": shift_type, "rows": rows},
    )


def post_non_operation_time_sync(end_day: str, shift_type: str, rows: list[dict]):
    return _req(
        "POST",
        "/non_operation_time/sync",
        json_body={"end_day": end_day, "shift_type": shift_type, "rows": rows},
    )
