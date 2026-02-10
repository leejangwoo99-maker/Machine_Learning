from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import streamlit as st

KST = ZoneInfo("Asia/Seoul")


def _default_window():
    now = datetime.now(KST)
    t = now.time()
    if (t.hour, t.minute, t.second) >= (8, 30, 0) and (t.hour, t.minute, t.second) <= (20, 29, 59):
        return now.strftime("%Y%m%d"), "day"
    if (t.hour, t.minute, t.second) < (8, 30, 0):
        return (now - timedelta(days=1)).strftime("%Y%m%d"), "night"
    return now.strftime("%Y%m%d"), "night"


def render_window_filter():
    if "window_prod_day" not in st.session_state or "window_shift_type" not in st.session_state:
        d, s = _default_window()
        st.session_state.window_prod_day = d
        st.session_state.window_shift_type = s

    d = st.text_input("?앹궛?쇱옄(YYYYMMDD)", value=st.session_state.window_prod_day)
    s = st.selectbox(
        "二쇱빞媛?,
        ["day", "night"],
        index=0 if st.session_state.window_shift_type == "day" else 1
    )

    d = d.strip().replace("-", "")
    st.session_state.window_prod_day = d
    st.session_state.window_shift_type = s

    return d, s
