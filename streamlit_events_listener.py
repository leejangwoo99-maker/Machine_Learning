import json
import threading
import queue
import time
import requests
import pandas as pd
import streamlit as st

API = "http://127.0.0.1:8000"
EVENTS_URL = f"{API}/events"
AUTO_REFRESH_SEC = 5

if "evt_q" not in st.session_state:
    st.session_state.evt_q = queue.Queue()

if "listener_started" not in st.session_state:
    st.session_state.listener_started = False

if "last_event" not in st.session_state:
    st.session_state.last_event = None

if "a_df" not in st.session_state:
    st.session_state.a_df = pd.DataFrame()

if "b_df" not in st.session_state:
    st.session_state.b_df = pd.DataFrame()

if "last_refresh_ts" not in st.session_state:
    st.session_state.last_refresh_ts = 0.0


def sse_listener(evt_q: queue.Queue):
    while True:
        try:
            with requests.get(EVENTS_URL, stream=True, timeout=3600) as r:
                r.raise_for_status()
                event_name = None
                for raw in r.iter_lines(decode_unicode=True):
                    if raw is None:
                        continue
                    line = raw.strip()
                    if not line or line.startswith(":"):
                        continue
                    if line.startswith("event:"):
                        event_name = line[len("event:"):].strip()
                    elif line.startswith("data:"):
                        data_str = line[len("data:"):].strip()
                        try:
                            data = json.loads(data_str)
                        except Exception:
                            data = {"raw": data_str}
                        evt_q.put({"event": event_name, "data": data})
        except Exception:
            # 끊기면 잠시 후 재연결
            time.sleep(1.0)


def ensure_listener():
    if st.session_state.listener_started:
        return
    t = threading.Thread(target=sse_listener, args=(st.session_state.evt_q,), daemon=True)
    t.start()
    st.session_state.listener_started = True


def api_get_nonop(end_day: str, shift_type: str):
    url = f"{API}/non_operation_time"
    r = requests.get(url, params={"end_day": end_day, "shift_type": shift_type}, timeout=10)
    r.raise_for_status()
    return r.json().get("rows", [])


def api_get_i_non_time(prod_day: str, shift_type: str):
    url = f"{API}/report/i_non_time/{prod_day}"
    r = requests.get(url, params={"shift_type": shift_type}, timeout=10)
    r.raise_for_status()
    return r.json()


st.set_page_config(page_title="Realtime Dashboards", layout="wide")
ensure_listener()

st.title("Realtime Dashboards (SSE)")

c1, c2 = st.columns(2)
with c1:
    end_day = st.text_input("end_day/prod_day", value="20260205")
with c2:
    shift_type = st.selectbox("shift_type", ["day", "night"], index=0)

left, right = st.columns(2)

with left:
    st.markdown("### Dashboard A: non_operation_time")
    if st.button("A 수동조회"):
        st.session_state.a_df = pd.DataFrame(api_get_nonop(end_day, shift_type))
    if st.session_state.a_df.empty:
        st.session_state.a_df = pd.DataFrame(api_get_nonop(end_day, shift_type))
    st.dataframe(st.session_state.a_df, use_container_width=True, height=420)

with right:
    st.markdown("### Dashboard B: report i_non_time")
    if st.button("B 수동조회"):
        st.session_state.b_df = pd.DataFrame(api_get_i_non_time(end_day, shift_type))
    if st.session_state.b_df.empty:
        st.session_state.b_df = pd.DataFrame(api_get_i_non_time(end_day, shift_type))
    st.dataframe(st.session_state.b_df, use_container_width=True, height=420)

# 이벤트 drain
drained = 0
last_evt = None
while not st.session_state.evt_q.empty() and drained < 200:
    last_evt = st.session_state.evt_q.get()
    drained += 1

if last_evt is not None:
    st.session_state.last_event = last_evt

if st.session_state.last_event:
    evt_name = st.session_state.last_event["event"]
    evt_data = st.session_state.last_event["data"]
    st.info(f"last_event = {evt_name} / {evt_data}")

    if evt_name == "non_operation_time_event":
        st.session_state.a_df = pd.DataFrame(api_get_nonop(end_day, shift_type))
    elif evt_name in ("planned_time_event", "report_i_non_time_event"):
        st.session_state.b_df = pd.DataFrame(api_get_i_non_time(end_day, shift_type))

# 5초 주기 렌더 갱신(이벤트 반영 확인용)
st.caption(f"auto refresh: {AUTO_REFRESH_SEC}s")
now = time.time()
if now - st.session_state.last_refresh_ts >= AUTO_REFRESH_SEC:
    st.session_state.last_refresh_ts = now
    st.rerun()
