# streamlit_app/pages/03_?앹궛_遺꾩꽍.py
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import streamlit as st

from api_client import (
    get_alarm_records,
    get_alarm_recent,
    get_events,
    get_pd_board_check,
    get_report_f_worst_case,
    get_report_g_afa_wasted_time,
    get_report_h_mes_wasted_time,
)

KST = ZoneInfo("Asia/Seoul")
st.set_page_config(page_title="?앹궛 遺꾩꽍", page_icon="?뵩", layout="wide")


def _safe_list(payload):
    if isinstance(payload, dict) and "value" in payload and isinstance(payload["value"], list):
        return payload["value"]
    if isinstance(payload, list):
        return payload
    return []


def _now_kst_day_shift():
    now = datetime.now(KST)
    day = now.strftime("%Y%m%d")
    hhmmss = now.strftime("%H%M%S")
    shift = "day" if "083000" <= hhmmss <= "202959" else "night"
    return day, shift


def _init_defaults():
    if "prod_day" not in st.session_state or "shift_type" not in st.session_state:
        try:
            ev = get_events()
            st.session_state["prod_day"] = ev.get("prod_day")
            st.session_state["shift_type"] = ev.get("shift_type")
        except Exception:
            d, s = _now_kst_day_shift()
            st.session_state["prod_day"] = d
            st.session_state["shift_type"] = s


def _alarm_msg(row: dict) -> str:
    t = str(row.get("type_alarm", "")).strip()
    station = row.get("station", "")
    sparepart = row.get("sparepart", "")
    if t == "沅뚭퀬":
        return f"{station}, {sparepart} 援먯껜 沅뚭퀬 ?쒕┰?덈떎."
    if t == "湲닿툒":
        return f"{station}, {sparepart} 援먯껜 湲닿툒?⑸땲??"
    if t == "援먯껜":
        return f"{station}, {sparepart} 援먯껜 ??대컢??吏?ъ뒿?덈떎."
    return ""


_init_defaults()

h1, h2, h3, h4 = st.columns([2.2, 1, 1, 1])
with h1:
    st.subheader("YYYYMMDD 二쇨컙 or ?쇨컙 ?앹궛 遺꾩꽍")
with h2:
    prod_day = st.text_input("prod_day", value=st.session_state["prod_day"], key="p03_day")
with h3:
    shift_type = st.selectbox("shift_type", ["day", "night"], index=0 if st.session_state["shift_type"] == "day" else 1, key="p03_shift")
with h4:
    auto = st.toggle("Auto 15s", value=False, key="p03_auto")

st.session_state["prod_day"] = prod_day
st.session_state["shift_type"] = shift_type

# 1) alarm record
st.markdown("??[alarm_record] AI ?덉긽 寃쎄퀬 SPAREPART 援먯껜 ?뚮엺 由ъ뒪??)
alarm_rows = _safe_list(get_alarm_records())
st.dataframe(pd.DataFrame(alarm_rows), use_container_width=True, height=220)

# 理쒖떊 ?뚮엺 ?앹뾽 移대뱶
recent_rows = _safe_list(get_alarm_recent())
if recent_rows:
    r = recent_rows[0]
    msg = _alarm_msg(r)
    if msg:
        st.markdown(
            f"""
            <div style="border:1px solid #444; border-radius:10px; padding:12px; text-align:center; margin:6px 0 16px 0;">
              <div style="font-size:22px;">{msg}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

st.markdown("---")

# 2) pd board check
st.markdown("??[pd_board_check] AI 寃쎄퀬 PD-BOARD(?λ퉬) ?댄솕 紐⑤땲?곕쭅")
pd_payload = get_pd_board_check(end_day=prod_day, shift_type=shift_type)

if isinstance(pd_payload, dict):
    # table
    table_rows = []
    if "value" in pd_payload and isinstance(pd_payload["value"], list):
        table_rows = pd_payload["value"]
    elif "rows" in pd_payload and isinstance(pd_payload["rows"], list):
        table_rows = pd_payload["rows"]
    if table_rows:
        st.dataframe(pd.DataFrame(table_rows), use_container_width=True, height=220)

    # chart
    chart_rows = []
    if "chart" in pd_payload and isinstance(pd_payload["chart"], list):
        chart_rows = pd_payload["chart"]
    elif "series" in pd_payload and isinstance(pd_payload["series"], list):
        chart_rows = pd_payload["series"]

    if chart_rows:
        cdf = pd.DataFrame(chart_rows)
        # 理쒖냼 而щ읆 異붿젙: date/station/value 瑜?
        cands_x = [c for c in cdf.columns if "date" in c.lower() or "day" in c.lower()]
        cands_y = [c for c in cdf.columns if ("cos" in c.lower()) or ("sim" in c.lower()) or ("value" in c.lower())]
        cands_color = [c for c in cdf.columns if "station" in c.lower() or "line" in c.lower()]

        if cands_x and cands_y:
            xcol = cands_x[0]
            ycol = cands_y[0]
            color_col = cands_color[0] if cands_color else None
            fig = px.line(cdf, x=xcol, y=ycol, color=color_col, markers=True)
            st.plotly_chart(fig, use_container_width=True)
else:
    st.json(pd_payload)

st.markdown("---")

# 3) worst / afa / mes
st.markdown("??[f_worst_case] FCT worst case")
st.dataframe(pd.DataFrame(_safe_list(get_report_f_worst_case(prod_day, shift_type))), use_container_width=True, height=220)

st.markdown("??[g_afa_wasted_time] 議곕┰ 怨듭젙 遺덈웾???곕Ⅸ ??퉬 ?쒓컙")
st.dataframe(pd.DataFrame(_safe_list(get_report_g_afa_wasted_time(prod_day, shift_type))), use_container_width=True, height=120)

st.markdown("??[h_mes_wasted_time] MES 遺덈웾???곕Ⅸ ??퉬 ?쒓컙")
st.dataframe(pd.DataFrame(_safe_list(get_report_h_mes_wasted_time(prod_day, shift_type))), use_container_width=True, height=160)

if auto:
    time.sleep(15)
    st.rerun()
