# streamlit_app/pages/02_?앹궛_?뺣낫.py
from __future__ import annotations

import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from api_client import (
    get_events,
    get_report_a_station_final_amount,
    get_report_b_station_percentage,
    get_report_c_fct_step_1time,
    get_report_c_fct_step_2time,
    get_report_c_fct_step_3over,
    get_report_d_vision_step_1time,
    get_report_d_vision_step_2time,
    get_report_d_vision_step_3over,
    get_report_i_non_time,
    get_report_i_planned_stop_time,
    get_report_k_oee_line,
    get_report_k_oee_station,
    get_report_k_oee_total,
)

KST = ZoneInfo("Asia/Seoul")
st.set_page_config(page_title="?앹궛 ?뺣낫", page_icon="?㎨", layout="wide")


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


def _draw_df(title: str, rows: list, height=220, hide_if_empty=False):
    if hide_if_empty and not rows:
        return
    st.markdown(f"**{title}**")
    if not rows:
        st.caption("?곗씠???놁쓬")
        return
    st.dataframe(pd.DataFrame(rows), use_container_width=True, height=height)


_init_defaults()

h1, h2, h3, h4 = st.columns([2.2, 1, 1, 1])
with h1:
    st.subheader("YYYYMMDD 二쇨컙 or ?쇨컙 ?앹궛 ?뺣낫")
with h2:
    prod_day = st.text_input("prod_day", value=st.session_state["prod_day"], key="p02_day")
with h3:
    shift_type = st.selectbox("shift_type", ["day", "night"], index=0 if st.session_state["shift_type"] == "day" else 1, key="p02_shift")
with h4:
    auto = st.toggle("Auto 15s", value=False, key="p02_auto")

st.session_state["prod_day"] = prod_day
st.session_state["shift_type"] = shift_type

# 1) OEE 3醫?
st.markdown("??[k_oee_total], [k_oee_line], [k_oee_station] OEE")
o1, o2, o3 = st.columns([1, 1, 1.2])

with o1:
    _draw_df("?꾩껜 OEE", _safe_list(get_report_k_oee_total(prod_day, shift_type)), height=180)

with o2:
    _draw_df("Line蹂?OEE", _safe_list(get_report_k_oee_line(prod_day, shift_type)), height=180)

with o3:
    _draw_df("Station蹂?OEE", _safe_list(get_report_k_oee_station(prod_day, shift_type)), height=220)

st.markdown("---")

# 2) planned / non time
st.markdown("??[i_planned_stop_time] 珥?怨꾪쉷 ?뺤? ?쒓컙")
_draw_df("怨꾪쉷 ?뺤? ?쒓컙", _safe_list(get_report_i_planned_stop_time(prod_day, shift_type)), height=200)

st.markdown("??[i_non_time] 珥?鍮꾧????쒓컙 (Vision1=FCT1&2 援먯쭛?? Vision2=FCT3&4 援먯쭛??")
_draw_df("鍮꾧????쒓컙", _safe_list(get_report_i_non_time(prod_day, shift_type)), height=220)

st.markdown("---")

# 3) ?쒓컙?蹂?PASS/FAIL + 寃?ш린 PASS/total/PASS_pct
st.markdown("??[a_station_final_amount] ?쒓컙?蹂?PASS/FAIL 吏묎퀎")
_draw_df("?쒓컙?蹂?吏묎퀎", _safe_list(get_report_a_station_final_amount(prod_day, shift_type)), height=260)

st.markdown("??[b_station_percentage] 寃?ш린 蹂?PASS, total, PASS_pct")
_draw_df("寃?ш린 ?깅뒫 吏묎퀎", _safe_list(get_report_b_station_percentage(prod_day, shift_type)), height=150)

st.markdown("---")

# 4) FCT/Vision fail step (empty 異쒕젰 ?쒖쇅)
st.markdown("??FCT 1/2/3???댁긽 FAIL step [empty ??異쒕젰 ?쒖쇅]")
_draw_df("FCT 1??FAIL", _safe_list(get_report_c_fct_step_1time(prod_day, shift_type)), hide_if_empty=True)
_draw_df("FCT 2??FAIL", _safe_list(get_report_c_fct_step_2time(prod_day, shift_type)), hide_if_empty=True)
_draw_df("FCT 3???댁긽 FAIL", _safe_list(get_report_c_fct_step_3over(prod_day, shift_type)), hide_if_empty=True)

st.markdown("??Vision 1/2/3???댁긽 FAIL step [empty ??異쒕젰 ?쒖쇅]")
_draw_df("Vision 1??FAIL", _safe_list(get_report_d_vision_step_1time(prod_day, shift_type)), hide_if_empty=True)
_draw_df("Vision 2??FAIL", _safe_list(get_report_d_vision_step_2time(prod_day, shift_type)), hide_if_empty=True)
_draw_df("Vision 3???댁긽 FAIL", _safe_list(get_report_d_vision_step_3over(prod_day, shift_type)), hide_if_empty=True)

if auto:
    time.sleep(15)
    st.rerun()
