from __future__ import annotations
import time
from dataclasses import dataclass
from typing import Callable, Any
import streamlit as st

@dataclass
class RetryResult:
    ok: bool
    value: Any = None
    error: str | None = None
    attempts: int = 0

def call_with_retry(fn: Callable[[], Any], max_try: int = 10, delay_sec: float = 0.8) -> RetryResult:
    last_err = None
    for i in range(1, max_try+1):
        try:
            return RetryResult(ok=True, value=fn(), attempts=i)
        except Exception as e:
            last_err = str(e)
            time.sleep(delay_sec)
    return RetryResult(ok=False, error=last_err, attempts=max_try)

def normalize_day_key(s: str) -> str:
    s=(s or "").strip()
    if len(s)==10 and s[4]=="-" and s[7]=="-":
        s=s.replace("-","")
    return s

def ensure_state():
    if "popup_queue" not in st.session_state:
        st.session_state.popup_queue = []
    if "last_seen_alarm_updated_at" not in st.session_state:
        st.session_state.last_seen_alarm_updated_at = ""
    if "last_poll_ts" not in st.session_state:
        st.session_state.last_poll_ts = 0.0

def push_popup(msg: str, level: str = "info"):
    st.session_state.popup_queue.append({"msg": msg, "level": level})

def render_popup_queue():
    if not st.session_state.popup_queue:
        return
    item = st.session_state.popup_queue.pop(0)
    level = item.get("level","info")
    msg = item.get("msg","")
    if level == "error":
        st.error(msg)
    elif level == "success":
        st.success(msg)
    else:
        st.info(msg)
