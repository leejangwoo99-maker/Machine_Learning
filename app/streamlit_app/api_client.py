# app/streamlit_app/api_client.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
import json
from typing import Any, Optional, Dict, List, Iterator, Tuple

import requests

# ---------------------------------------------------------
# Base settings
# ---------------------------------------------------------
API = os.getenv("API_BASE_URL", "http://127.0.0.1:8000").strip().rstrip("/")
TIMEOUT = float(os.getenv("API_TIMEOUT_SEC", "90"))
ADMIN_PASS = os.getenv("ADMIN_PASS", "leejangwoo1!")

_session = requests.Session()

NONOP_WINDOW_LIMIT_DEFAULT = int(os.getenv("NONOP_WINDOW_LIMIT_DEFAULT", "2000"))
NONOP_WINDOW_LIMIT_MAX = int(os.getenv("NONOP_WINDOW_LIMIT_MAX", "2000"))

NONOP_CHANGES_LIMIT_DEFAULT = int(os.getenv("NONOP_CHANGES_LIMIT_DEFAULT", "8000"))
NONOP_CHANGES_LIMIT_MAX = int(os.getenv("NONOP_CHANGES_LIMIT_MAX", "20000"))


def _url(path: str) -> str:
    if not path.startswith("/"):
        path = "/" + path
    return f"{API}{path}"


def _http_error_with_detail(r: requests.Response) -> RuntimeError:
    status = getattr(r, "status_code", "NA")
    url = getattr(r, "url", "")
    text = ""
    detail = None

    try:
        ct = (r.headers.get("content-type", "") or "").lower()
        if "application/json" in ct:
            j = r.json()
            detail = j.get("detail", None) if isinstance(j, dict) else None
            text = str(j)
        else:
            text = r.text
    except Exception:
        try:
            text = r.text
        except Exception:
            text = ""

    msg = f"HTTP {status} for {url}"
    if detail is not None:
        msg += f" | detail={detail}"
    elif text:
        msg += f" | body={text[:500]}"
    return RuntimeError(msg)


def _req(
    method: str,
    path: str,
    *,
    params: Optional[dict] = None,
    json_body: Optional[dict] = None,
    headers: Optional[dict] = None,
    timeout: Optional[float] = None,
    retries: int = 2,
    retry_backoff: float = 0.35,
) -> Any:
    m = method.upper().strip()
    last_err: Optional[Exception] = None

    for attempt in range(int(retries) + 1):
        try:
            r = _session.request(
                method=m,
                url=_url(path),
                params=params,
                json=json_body,
                headers=headers,
                timeout=(TIMEOUT if timeout is None else timeout),
            )
            try:
                r.raise_for_status()
            except requests.HTTPError:
                raise _http_error_with_detail(r)

            ct = (r.headers.get("content-type", "") or "").lower()
            if "application/json" in ct:
                return r.json()
            return r.text

        except (requests.Timeout, requests.ConnectionError, requests.HTTPError, RuntimeError) as e:
            last_err = e
            # GET 계열만 재시도
            if m not in ("GET", "HEAD", "OPTIONS"):
                raise
            if attempt >= int(retries):
                raise
            time.sleep(retry_backoff * (attempt + 1))

    if last_err:
        raise last_err
    raise RuntimeError("request failed")


def _q(end_day: str, shift_type: str) -> Dict[str, str]:
    return {"end_day": str(end_day), "shift_type": str(shift_type)}


def _admin_header(password: str) -> Dict[str, str]:
    return {"X-ADMIN-PASS": str(password or "")}


def _extract_rows(resp: Any) -> List[dict]:
    if resp is None:
        return []
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for k in ("rows", "data", "items", "value"):
            v = resp.get(k)
            if isinstance(v, list):
                return v
    return []


def parse_token_json(token: Any) -> Any:
    if token is None:
        return None
    if isinstance(token, (dict, list)):
        return token
    s = str(token).strip()
    if not s:
        return None
    if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
        try:
            return json.loads(s)
        except Exception:
            return s
    return s


def _to_hhmmss(v: Any) -> str:
    s = "" if v is None else str(v).strip()
    if not s:
        return ""
    if len(s) == 5 and s[2] == ":":
        return s + ":00"
    if len(s) >= 8 and s[2] == ":" and s[5] == ":":
        return s[:8]
    if "T" in s:
        try:
            tpart = s.split("T", 1)[1]
            if len(tpart) >= 8 and tpart[2] == ":" and tpart[5] == ":":
                return tpart[:8]
        except Exception:
            pass
    if " " in s:
        try:
            tpart = s.split(" ", 1)[1]
            if len(tpart) >= 8 and tpart[2] == ":" and tpart[5] == ":":
                return tpart[:8]
        except Exception:
            pass
    return s


def _clamp_int(v: Any, *, default: int, min_v: int, max_v: int) -> int:
    try:
        x = int(v)
    except Exception:
        x = int(default)
    if x < min_v:
        return min_v
    if x > max_v:
        return max_v
    return x


def _should_retry_without_params(err: Exception) -> bool:
    s = str(err).lower()
    if "http 422" in s:
        if "unexpected" in s or "extra fields not permitted" in s or "unrecognized" in s:
            return True
    return False


# =========================================================
# ✅ Health / events
# =========================================================
def get_health() -> Any:
    return _req("GET", "/health")


def get_events(limit: int = 50) -> Any:
    return _req("GET", "/events", params={"limit": int(limit)})


def get_events_latest(end_day: str, shift_type: str) -> Dict[str, Any]:
    res = _req("GET", "/events/latest", params={"end_day": str(end_day), "shift_type": str(shift_type)})
    return res if isinstance(res, dict) else {}


# =========================================================
# ✅ sections latest (토큰)
# =========================================================
def get_sections_latest(end_day: str, shift_type: str, *, debug: int = 0, timeout: float = 2.5) -> Dict[str, Any]:
    params = {"end_day": str(end_day), "shift_type": str(shift_type), "debug": int(debug)}
    try:
        res = _req("GET", "/events/sections/latest", params=params, timeout=float(timeout), retries=0)
        return res if isinstance(res, dict) else {}
    except Exception:
        return {}


def get_sections_latest_legacy(end_day: str, shift_type: str) -> Dict[str, Any]:
    return get_sections_latest(end_day, shift_type, debug=0, timeout=2.5)


# =========================================================
# ✅ SSE client (python) - 서버 디버깅/테스트용
# (Streamlit 본 페이지는 JS EventSource를 쓰는게 더 안정적)
# =========================================================
def open_sse_stream(end_day: str, shift_type: str, sections: Optional[str] = "alarm"):
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
        timeout=None,
    )
    resp.raise_for_status()
    return resp


def iter_sse_events(resp: requests.Response) -> Iterator[Tuple[str, str]]:
    """
    yield (event, data_json_text)
    """
    event = "message"
    data_lines: List[str] = []
    for raw in resp.iter_lines(decode_unicode=True):
        if raw is None:
            continue
        line = raw.strip("\r")

        if line == "":
            if data_lines:
                yield event, "\n".join(data_lines)
            event = "message"
            data_lines = []
            continue

        if line.startswith(":"):
            continue

        if line.startswith("event:"):
            event = line.split(":", 1)[1].strip() or "message"
            continue

        if line.startswith("data:"):
            data_lines.append(line.split(":", 1)[1].lstrip())
            continue


# =========================================================
# ✅ worker/email/remark/planned
# =========================================================
def get_worker_info(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/worker_info", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_worker_info_sync(end_day: str, shift_type: str, rows: List[dict]) -> Any:
    norm_rows: List[Dict[str, Any]] = []
    for r in (rows or []):
        if not isinstance(r, dict):
            continue
        wn = str(r.get("worker_name", "") or "").strip()
        on = str(r.get("order_number", "") or "").strip()
        if not wn and not on:
            continue
        norm_rows.append({"worker_name": wn, "order_number": on})
    body = {"rows": norm_rows}
    return _req("POST", "/worker_info/sync", params=_q(end_day, shift_type), json_body=body)


def get_email_list(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/email_list", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_email_list_sync(
    end_day: str,
    shift_type: str,
    rows: List[dict],
    *,
    password: str,
    mode: Optional[str] = None,
    min_keep_ratio: Optional[float] = None,
) -> Any:
    body = {"end_day": str(end_day), "shift_type": str(shift_type), "rows": rows or []}
    params: Dict[str, Any] = {}
    if mode:
        params["mode"] = str(mode)
    if min_keep_ratio is not None:
        try:
            params["min_keep_ratio"] = float(min_keep_ratio)
        except Exception:
            pass

    try:
        return _req("POST", "/email_list/sync", json_body=body, headers=_admin_header(password), params=params or None)
    except Exception as e:
        if params and _should_retry_without_params(e):
            return _req("POST", "/email_list/sync", json_body=body, headers=_admin_header(password))
        raise


def get_remark_info(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/remark_info", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_remark_info_sync(
    end_day: str,
    shift_type: str,
    rows: List[dict],
    *,
    password: str,
    mode: Optional[str] = None,
    min_keep_ratio: Optional[float] = None,
) -> Any:
    body = {"end_day": str(end_day), "shift_type": str(shift_type), "rows": rows or []}
    params: Dict[str, Any] = {}
    if mode:
        params["mode"] = str(mode)
    if min_keep_ratio is not None:
        try:
            params["min_keep_ratio"] = float(min_keep_ratio)
        except Exception:
            pass

    try:
        return _req("POST", "/remark_info/sync", json_body=body, headers=_admin_header(password), params=params or None)
    except Exception as e:
        if params and _should_retry_without_params(e):
            return _req("POST", "/remark_info/sync", json_body=body, headers=_admin_header(password))
        raise


def get_planned_time(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/planned_time", params=_q(end_day, shift_type))
    return _extract_rows(res)


def get_planned_time_today(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/planned_time/today", params=_q(end_day, shift_type))
    return _extract_rows(res)


def post_planned_time_sync(end_day: str, shift_type: str, rows: List[dict]) -> Any:
    norm_rows = []
    for r in (rows or []):
        if not isinstance(r, dict):
            continue

        ed = str(r.get("end_day") or r.get("prod_day") or end_day).strip()
        ft = _to_hhmmss(r.get("from_time", ""))
        tt = _to_hhmmss(r.get("to_time", ""))

        reason = r.get("reason", None)
        if reason is not None:
            reason = str(reason).strip()
            if reason == "":
                reason = None

        if not ed or not ft or not tt:
            continue

        norm_rows.append({"end_day": ed, "from_time": ft, "to_time": tt, "reason": reason})

    body = {"rows": norm_rows}

    return _req(
        "POST",
        "/planned_time/sync",
        params={**_q(end_day, shift_type), "mode": "replace"},
        json_body=body,
    )


# =========================================================
# ✅ non operation (legacy + consolidated)
# =========================================================
def get_non_operation_time(end_day: str, shift_type: str) -> List[dict]:
    res = _req("GET", "/non_operation_time", params=_q(end_day, shift_type), timeout=30.0)
    return _extract_rows(res)


def post_non_operation_time_sync(end_day: str, shift_type: str, payload: Any) -> Any:
    rows = []
    if isinstance(payload, dict):
        rows = payload.get("rows", []) or []
    elif isinstance(payload, list):
        rows = payload
    else:
        rows = []

    norm_rows = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        stn = str(r.get("station", "")).strip()
        ft = _to_hhmmss(r.get("from_time", r.get("from_ts", "")))
        tt = _to_hhmmss(r.get("to_time", r.get("to_ts", "")))
        reason = str(r.get("reason", "") or "").strip()
        spare = str(r.get("sparepart", "") or "").strip()
        if reason != "sparepart 교체":
            spare = ""
        if not stn or not ft or not tt:
            continue

        norm_rows.append(
            {
                "end_day": str(r.get("end_day") or end_day),
                "shift_type": str(r.get("shift_type") or shift_type),
                "station": stn,
                "from_time": ft,
                "to_time": tt,
                "reason": reason,
                "sparepart": spare,
            }
        )

    body = {"end_day": str(end_day), "shift_type": str(shift_type), "rows": norm_rows}

    return _req(
        "POST",
        "/non_operation_time/sync",
        params={"end_day": str(end_day), "shift_type": str(shift_type)},
        json_body=body,
    )


def get_non_operation_time_head(
    end_day: str,
    *,
    limit: int = 50,
    timeout: float = 6.0,
) -> Dict[str, Any]:
    params = {"end_day": str(end_day), "limit": int(limit)}
    res = _req("GET", "/non_operation_time/head", params=params, timeout=float(timeout), retries=1)
    return res if isinstance(res, dict) else {"end_day": str(end_day), "items": [], "next_cursor": None}


def get_nonop_window(
    prod_day: str,
    shift_type: str,
    *,
    limit: Optional[int] = None,
    timeout: float = 8.0,
) -> Dict[str, Any]:
    params: Dict[str, Any] = {"prod_day": str(prod_day), "shift_type": str(shift_type)}

    if limit is not None:
        params["limit"] = _clamp_int(
            limit,
            default=NONOP_WINDOW_LIMIT_DEFAULT,
            min_v=1,
            max_v=NONOP_WINDOW_LIMIT_MAX,
        )

    res = _req("GET", "/nonop/window", params=params, timeout=float(timeout), retries=1)
    return res if isinstance(res, dict) else {
        "prod_day": str(prod_day),
        "shift_type": str(shift_type),
        "items": [],
        "max_id": 0,
        "max_updated_at": "",
    }


def get_nonop_changes(
    prod_day: str,
    shift_type: str,
    *,
    since_id: int = 0,
    since_updated_at: str = "1970-01-01T00:00:00+09:00",
    limit: int = NONOP_CHANGES_LIMIT_DEFAULT,
    timeout: float = 5.0,
) -> Dict[str, Any]:
    lim = _clamp_int(
        limit,
        default=NONOP_CHANGES_LIMIT_DEFAULT,
        min_v=1,
        max_v=NONOP_CHANGES_LIMIT_MAX,
    )

    params = {
        "prod_day": str(prod_day),
        "shift_type": str(shift_type),
        "since_id": int(since_id),
        "since_updated_at": str(since_updated_at),
        "limit": lim,
    }
    res = _req("GET", "/nonop/changes", params=params, timeout=float(timeout), retries=0)
    return res if isinstance(res, dict) else {
        "prod_day": str(prod_day),
        "shift_type": str(shift_type),
        "items": [],
        "max_id": int(since_id),
        "max_updated_at": str(since_updated_at),
    }


def post_nonop_update(rows: List[dict], *, timeout: float = 12.0) -> Any:
    body = {"rows": rows or []}
    return _req("POST", "/nonop/update", json_body=body, timeout=float(timeout))


# =========================================================
# ✅ alarm / mastersample / production graph
# (여기 누락되면 01_production_status import에서 터짐)
# =========================================================
def get_alarm_record(end_day: str, shift_type: str, *, timeout: float = 12.0) -> List[dict]:
    res = _req("GET", "/alarm_record", params=_q(end_day, shift_type), timeout=float(timeout))
    return _extract_rows(res)


def get_alarm_record_recent(end_day: str, shift_type: str, *, timeout: float = 6.0) -> List[dict]:
    res = _req("GET", "/alarm_record/recent", params=_q(end_day, shift_type), timeout=float(timeout), retries=0)
    return _extract_rows(res)


# (03 페이지 호환용 alias)
def get_alarm_record_recent(end_day: str, shift_type: str, *, timeout: float = 6.0) -> List[dict]:
    res = _req("GET", "/alarm_record/recent", params=_q(end_day, shift_type), timeout=float(timeout), retries=1)
    return _extract_rows(res)


def get_mastersample_test_info(end_day: str, shift_type: str, *, timeout: float = 12.0) -> List[dict]:
    res = _req("GET", "/mastersample_test_info", params=_q(end_day, shift_type), timeout=float(timeout))
    return _extract_rows(res)


def get_production_progress_graph(end_day: str, shift_type: str, slot_minutes: int = 60) -> Any:
    params = {"end_day": str(end_day), "shift_type": str(shift_type), "slot_minutes": int(slot_minutes)}
    return _req("GET", "/production_progress_graph", params=params)


# =========================================================
# ✅ PD board check (03 페이지에서 사용)
# - ✅ end_day 지정 조회 지원(가능하면 서버에 전달)
# - ✅ 서버가 파라미터를 모르면(422 등) 자동으로 무파라미터로 fallback
# =========================================================
def get_pd_board_check(*, end_day: Optional[str] = None, timeout: float = 8.0) -> Any:
    params: Optional[Dict[str, Any]] = None
    if end_day:
        params = {"end_day": str(end_day)}

    try:
        return _req("GET", "/pd_board_check", params=params, timeout=float(timeout), retries=0)
    except Exception as e:
        # 서버가 end_day 파라미터를 지원 안 하면 fallback
        if params and _should_retry_without_params(e):
            return _req("GET", "/pd_board_check", timeout=float(timeout), retries=0)
        raise


def get_pd_board_check_latest(*, end_day: Optional[str] = None, timeout: float = 8.0) -> Any:
    return get_pd_board_check(end_day=end_day, timeout=timeout)


def get_pd_board_check_recent(*, end_day: Optional[str] = None, timeout: float = 8.0) -> Any:
    return get_pd_board_check(end_day=end_day, timeout=timeout)

# =========================================================
# ✅ reports
# =========================================================
def _report_get(path: str, prod_day: str, shift_type: str) -> Any:
    return _req("GET", path.format(prod_day=str(prod_day)), params={"shift_type": str(shift_type)})


def get_report_a_station_final_amount(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/a_station_final_amount/{prod_day}", prod_day, shift_type)


def get_report_b_station_percentage(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/b_station_percentage/{prod_day}", prod_day, shift_type)


def get_report_c_fct_step_1time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/c_fct_step_1time/{prod_day}", prod_day, shift_type)


def get_report_c_fct_step_2time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/c_fct_step_2time/{prod_day}", prod_day, shift_type)


def get_report_c_fct_step_3over(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/c_fct_step_3over/{prod_day}", prod_day, shift_type)


def get_report_d_vision_step_1time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/d_vision_step_1time/{prod_day}", prod_day, shift_type)


def get_report_d_vision_step_2time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/d_vision_step_2time/{prod_day}", prod_day, shift_type)


def get_report_d_vision_step_3over(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/d_vision_step_3over/{prod_day}", prod_day, shift_type)


def get_report_k_oee_line(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/k_oee_line/{prod_day}", prod_day, shift_type)


def get_report_k_oee_station(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/k_oee_station/{prod_day}", prod_day, shift_type)


def get_report_k_oee_total(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/k_oee_total/{prod_day}", prod_day, shift_type)


def get_report_f_worst_case(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/f_worst_case/{prod_day}", prod_day, shift_type)


def get_report_g_afa_wasted_time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/g_afa_wasted_time/{prod_day}", prod_day, shift_type)


def get_report_h_mes_wasted_time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/h_mes_wasted_time/{prod_day}", prod_day, shift_type)


def get_report_i_planned_stop_time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/i_planned_stop_time/{prod_day}", prod_day, shift_type)


def get_report_i_non_time(prod_day: str, shift_type: str) -> Any:
    return _report_get("/report/i_non_time/{prod_day}", prod_day, shift_type)