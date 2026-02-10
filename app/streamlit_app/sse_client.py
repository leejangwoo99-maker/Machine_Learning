import json, queue, threading, requests, streamlit as st

API = "http://127.0.0.1:8000"
EVENTS_URL = f"{API}/events"

if "evt_q" not in st.session_state:
    st.session_state.evt_q = queue.Queue()
if "listener_started" not in st.session_state:
    st.session_state.listener_started = False
if "last_event" not in st.session_state:
    st.session_state.last_event = None

def _listener(evt_q: queue.Queue):
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
            # ?곌껐 ?딄? ???먮룞 ?ъ떆??
            continue

def ensure_sse():
    if st.session_state.listener_started:
        return
    t = threading.Thread(target=_listener, args=(st.session_state.evt_q,), daemon=True)
    t.start()
    st.session_state.listener_started = True

def drain_events(max_n=200):
    drained = []
    while (not st.session_state.evt_q.empty()) and len(drained) < max_n:
        drained.append(st.session_state.evt_q.get())
    if drained:
        st.session_state.last_event = drained[-1]
    return drained
