# app/streamlit_app/pages/02_production_info.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import api_client as api
from api_client import (
    get_report_k_oee_total,
    get_report_k_oee_line,
    get_report_k_oee_station,
    get_report_c_fct_step_1time,
    get_report_c_fct_step_2time,
    get_report_c_fct_step_3over,
    get_report_d_vision_step_1time,
    get_report_d_vision_step_2time,
    get_report_d_vision_step_3over,
)

KST = ZoneInfo("Asia/Seoul")

ALARM_ALLOWED_TYPES = {"권고", "긴급", "교체"}


# -----------------------------
# Helpers
# -----------------------------
def _now_prod_day_shift() -> tuple[str, str]:
    """
    Shift window (KST):
      - day   : 08:30 ~ 20:30  (same prod_day)
      - night : 20:30 ~ next day 08:30
        * if now < 08:30 => prod_day = yesterday, shift=night
        * if now >= 20:30 => prod_day = today, shift=night
    """
    now = datetime.now(tz=KST)

    day_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
    night_start = now.replace(hour=20, minute=30, second=0, microsecond=0)

    if day_start <= now < night_start:
        # ✅ 08:30~20:30 => day
        return now.strftime("%Y%m%d"), "day"

    if now >= night_start:
        # ✅ 20:30~24:00 => night (prod_day = today)
        return now.strftime("%Y%m%d"), "night"

    # ✅ 00:00~08:30 => night (prod_day = yesterday)
    y = (now - timedelta(days=1)).strftime("%Y%m%d")
    return y, "night"


def _norm_day(v: Any) -> str:
    s = str(v or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _norm_shift(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in ("day", "night") else "day"


def _df_from_resp(resp: Any) -> pd.DataFrame:
    if resp is None:
        return pd.DataFrame()
    if isinstance(resp, list):
        return pd.DataFrame(resp)
    if isinstance(resp, dict):
        for k in ("rows", "items", "data", "value"):
            v = resp.get(k)
            if isinstance(v, list):
                return pd.DataFrame(v)
        return pd.DataFrame([resp])
    return pd.DataFrame()


def _drop_cols(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    drop = [c for c in cols if c in df.columns]
    return df.drop(columns=drop) if drop else df


def _drop_updated_at(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    if "updated_at" in df.columns:
        return df.drop(columns=["updated_at"])
    return df


def _safe_call(fn, *args, **kwargs):
    try:
        if fn is None:
            return None
        return fn(*args, **kwargs)
    except Exception:
        return None


def _is_empty_df(df: pd.DataFrame) -> bool:
    return (df is None) or (not isinstance(df, pd.DataFrame)) or df.empty


def _get_api_fn(name: str):
    fn = getattr(api, name, None)
    return fn if callable(fn) else None


def _make_alarm_message(type_alarm: str, station: str, sparepart: str) -> str:
    t = (type_alarm or "").strip()
    stn = (station or "").strip()
    sp = (sparepart or "").strip()

    if t == "권고":
        return f"{stn}, {sp} 교체 권고 드립니다."
    if t == "긴급":
        return f"{stn}, {sp} 교체 긴급합니다."
    if t == "교체":
        return f"{stn}, {sp} 교체 타이밍이 지났습니다."
    return f"{stn}, {sp} 알람({t})"


def _api_base_url() -> str:
    for attr in ("API_BASE_URL", "BASE_URL", "API_URL", "API"):
        v = getattr(api, attr, None)
        if isinstance(v, str) and v.strip().startswith("http"):
            return v.strip().rstrip("/")
    return "http://127.0.0.1:8000"


def _mount_alarm_sse(cur_day: str, cur_shift: str):
    """
    ✅ 브라우저가 FastAPI SSE 직접 구독
    - event:hello / event:init / event:alarm 수신
    - ✅ 비차단(Non-blocking) 모달:
        * overlay는 pointer-events: none (뒤 UI 클릭 가능)
        * box만 pointer-events: auto (버튼 클릭 가능)
    - X/확인 즉시 닫힘
    - 중복 방지: sessionStorage acked_alarm_pks
    - 디버그: 콘솔 로그
    """
    base = _api_base_url().rstrip("/")

    # ✅ EventSource는 헤더를 못 보내므로 query token 사용
    admin_pass = (getattr(api, "ADMIN_PASS", "") or "").strip() or (os.getenv("ADMIN_PASS", "") or "").strip()
    token_qs = f"&token={admin_pass}" if admin_pass else ""

    sse_url = f"{base}/events/stream?end_day={cur_day}&shift_type={cur_shift}&sections=alarm{token_qs}"

    components.html(
        f"""
        <script>
        (function() {{
          const W = window.parent;
          const url = {json.dumps(sse_url)};
          const ALLOWED = new Set(["권고","긴급","교체"]);

          // 세션 저장 키
          const ACK_KEY = "acked_alarm_pks";

          function log(...args) {{
            try {{ console.log("[ALARM_SSE]", ...args); }} catch(e) {{}}
          }}

          function safeText(s) {{
            return String(s||"")
              .replaceAll("&","&amp;")
              .replaceAll("<","&lt;")
              .replaceAll(">","&gt;")
              .replaceAll('"',"&quot;")
              .replaceAll("'","&#39;");
          }}

          function getAcked() {{
            try {{
              const v = JSON.parse(W.sessionStorage.getItem(ACK_KEY) || "[]");
              return Array.isArray(v) ? v : [];
            }} catch(e) {{
              return [];
            }}
          }}

          function addAck(pk) {{
            try {{
              const arr = getAcked();
              if (!arr.includes(pk)) arr.push(pk);
              W.sessionStorage.setItem(ACK_KEY, JSON.stringify(arr));
            }} catch(e) {{}}
          }}

          function closeExistingModal() {{
            const doc = W.document;
            const old = doc.querySelector('[id^="alarmModal_"]');
            if (old) old.remove();
          }}

          function showModal(pk, message) {{
            const doc = W.document;

            // acked면 무시
            const acked = getAcked();
            if (acked.includes(pk)) {{
              log("skip acked pk=", pk);
              return;
            }}

            // 동일 pk 중복 모달 방지
            if (doc.getElementById("alarmModal_" + pk)) {{
              log("already showing pk=", pk);
              return;
            }}

            closeExistingModal();

            // ✅ overlay는 뒤 UI를 막지 않도록 pointer-events: none + 투명 배경
            const overlay = doc.createElement("div");
            overlay.id = "alarmModal_" + pk;
            overlay.style.cssText = `
              position: fixed; z-index: 2147483647;
              left: 0; top: 0; width: 100%; height: 100%;
              background: transparent;
              pointer-events: none;              /* ✅ 뒤 클릭 허용 */
              display: flex; align-items: center; justify-content: center;
            `;

            // ✅ box만 클릭 가능(pointer-events: auto)
            const box = doc.createElement("div");
            box.style.cssText = `
              width: 62%;
              max-width: 980px;
              background: white;
              border-radius: 14px;
              padding: 18px 22px;
              box-shadow: 0 10px 28px rgba(0,0,0,0.25);
              font-family: sans-serif;
              pointer-events: auto;              /* ✅ 버튼/닫기 클릭 가능 */
              border: 1px solid rgba(0,0,0,0.08);
            `;

            const closeId = "alarmCloseX_" + pk;
            const ackId = "alarmAckBtn_" + pk;

            box.innerHTML = `
              <div style="display:flex; align-items:center; justify-content:space-between;">
                <div style="font-size:22px; font-weight:800;">⚠️ 알람 발생</div>
                <button id="${{closeId}}" style="border:none;background:transparent;font-size:22px;cursor:pointer;">✕</button>
              </div>

              <div style="margin-top:14px; padding:12px 14px; border-radius:10px;
                          background:#fff9db; color:#5c4a00; font-size:16px;">
                ${{safeText(message)}}
              </div>

              <div style="margin-top:16px; display:flex; gap:12px;">
                <button id="${{ackId}}" style="flex:1; padding:10px 0; border-radius:10px;
                        border:1px solid #ddd; background:white; cursor:pointer; font-size:15px;">
                  확인
                </button>
              </div>

              <div style="margin-top:10px; font-size:12px; color:#666;">
                ※ 이 알람은 비차단 모달입니다. 뒤 화면 조작이 가능합니다.
              </div>
            `;

            overlay.appendChild(box);
            doc.body.appendChild(overlay);

            function closeModal() {{
              const el = doc.getElementById("alarmModal_" + pk);
              if (el) el.remove();
            }}

            const closeBtn = box.querySelector("#" + closeId);
            const ackBtn = box.querySelector("#" + ackId);

            if (closeBtn) {{
              closeBtn.addEventListener("click", function(ev) {{
                try {{ ev.preventDefault(); ev.stopPropagation(); }} catch(e) {{}}
                log("close X pk=", pk);
                closeModal();
              }});
            }}

            if (ackBtn) {{
              ackBtn.addEventListener("click", function(ev) {{
                try {{ ev.preventDefault(); ev.stopPropagation(); }} catch(e) {{}}
                log("ack pk=", pk);
                addAck(pk);
                closeModal();
              }});
            }}
          }}

          function makeMessage(row) {{
            const t = String(row.type_alarm || "").trim().replaceAll(" ","");
            const stn = String(row.station || "").trim();
            const sp  = String(row.sparepart || "").trim();

            if (t === "권고") return `${{stn}}, ${{sp}} 교체 권고 드립니다.`;
            if (t === "긴급") return `${{stn}}, ${{sp}} 교체 긴급합니다.`;
            if (t === "교체") return `${{stn}}, ${{sp}} 교체 타이밍이 지났습니다.`;
            return `${{stn}}, ${{sp}} 알람(${{t}})`;
          }}

          function handleAlarm(obj, fromEvent) {{
            if (!obj || typeof obj !== "object") return;

            const row = obj.row;
            const pk  = String(obj.pk || "");
            if (!row || typeof row !== "object") {{
              log(fromEvent, "no row", obj);
              return;
            }}

            const t = String(row.type_alarm || "").trim().replaceAll(" ","");
            if (!ALLOWED.has(t)) {{
              log(fromEvent, "type not allowed:", t, row);
              return;
            }}

            if (!pk) {{
              // 서버가 pk를 id로 넣도록 했는데 혹시 없으면 fallback
              const id = row.id != null ? String(row.id) : "";
              if (!id) {{
                log(fromEvent, "no pk/id", row);
                return;
              }}
              log(fromEvent, "fallback id as pk:", id);
              showModal(id, makeMessage(row));
              return;
            }}

            log(fromEvent, "show pk=", pk, row);
            showModal(pk, makeMessage(row));
          }}

          // Streamlit rerun 대비: 전역 singleton
          if (!W.__alarmSSE) {{
            W.__alarmSSE = {{ url: null, es: null }};
          }}

          function start() {{
            if (W.__alarmSSE.es && W.__alarmSSE.url === url) {{
              log("already connected:", url);
              return;
            }}
            if (W.__alarmSSE.es) {{
              try {{ W.__alarmSSE.es.close(); }} catch(e) {{}}
              W.__alarmSSE.es = null;
            }}
            W.__alarmSSE.url = url;

            log("connect:", url);
            const es = new EventSource(url);
            W.__alarmSSE.es = es;

            es.addEventListener("hello", (ev) => {{
              log("hello", ev.data);
            }});

            es.addEventListener("init", (ev) => {{
              log("init", ev.data);
              try {{
                const obj = JSON.parse(ev.data || "{{}}");
                // init에도 alarm row 들어올 수 있음(서버 구현)
                if (obj && obj.row) handleAlarm(obj, "init");
              }} catch(e) {{
                log("init parse err", e);
              }}
            }});

            es.addEventListener("alarm", (ev) => {{
              log("alarm", ev.data);
              try {{
                const obj = JSON.parse(ev.data || "{{}}");
                handleAlarm(obj, "alarm");
              }} catch(e) {{
                log("alarm parse err", e);
              }}
            }});

            es.addEventListener("error", (ev) => {{
              // 브라우저가 자동 재연결 시도함
              log("error event", ev);
            }});
          }}

          start();
        }})();
        </script>
        """,
        height=0,
    )


# -----------------------------
# Session init: 조회 날짜/쉬프트
# -----------------------------
if "p02_active_day" not in st.session_state or "p02_active_shift" not in st.session_state:
    d0, s0 = _now_prod_day_shift()
    st.session_state["p02_active_day"] = d0
    st.session_state["p02_active_shift"] = s0
if "p02_prod_day_ui" not in st.session_state or "p02_shift_ui" not in st.session_state:
    st.session_state["p02_prod_day_ui"] = st.session_state["p02_active_day"]
    st.session_state["p02_shift_ui"] = st.session_state["p02_active_shift"]


# -----------------------------
# ✅ 알람 SSE는 "현재 시각 기준 prod_day/shift"로 구독 (조회 UI와 분리)
# -----------------------------
cur_day, cur_shift = _now_prod_day_shift()
_mount_alarm_sse(cur_day, cur_shift)


# -----------------------------
# Top UI
# -----------------------------
st.markdown("## 📌 생산 정보 (주간/야간)")

if st.session_state.get("p02_do_full_refresh", False):
    nd, ns = _now_prod_day_shift()
    st.session_state["p02_active_day"] = nd
    st.session_state["p02_active_shift"] = ns
    st.session_state["p02_prod_day_ui"] = nd
    st.session_state["p02_shift_ui"] = ns
    st.session_state["p02_do_full_refresh"] = False

c_day, c_shift, c_search, c_refresh = st.columns([2.2, 1.2, 0.8, 1.0])

with c_day:
    st.text_input("prod_day", key="p02_prod_day_ui")

with c_shift:
    st.selectbox("shift_type", options=["day", "night"], key="p02_shift_ui")

with c_search:
    if st.button("검색", use_container_width=True):
        d = _norm_day(st.session_state.get("p02_prod_day_ui"))
        s = _norm_shift(st.session_state.get("p02_shift_ui"))
        if not d:
            st.error("prod_day는 YYYYMMDD 형식이어야 합니다.")
        else:
            st.session_state["p02_active_day"] = d
            st.session_state["p02_active_shift"] = s
            st.rerun()

with c_refresh:
    if st.button("전체 새로고침", use_container_width=True):
        st.session_state["p02_do_full_refresh"] = True
        st.rerun()

active_day = str(st.session_state.get("p02_active_day", ""))
active_shift = str(st.session_state.get("p02_active_shift", "day"))
st.caption(f"현재 조회: prod_day={active_day} / shift_type={active_shift} | 모드: 수동(버튼 기반)")

st.divider()

# -----------------------------
# Data fetch (수동)
# -----------------------------
oee_total = _safe_call(get_report_k_oee_total, active_day, active_shift)
oee_line = _safe_call(get_report_k_oee_line, active_day, active_shift)
oee_station = _safe_call(get_report_k_oee_station, active_day, active_shift)

df_oee_total = _drop_updated_at(_df_from_resp(oee_total))
df_oee_line = _drop_updated_at(_df_from_resp(oee_line))
df_oee_station = _drop_updated_at(_df_from_resp(oee_station))

has_oee = (not _is_empty_df(df_oee_total)) or (not _is_empty_df(df_oee_line)) or (not _is_empty_df(df_oee_station))
if has_oee:
    st.markdown("### 📌 OEE 결과 (Total / Line / Station)")
    col1, col2, col3 = st.columns([1.05, 1.05, 1.15])

    with col1:
        if not _is_empty_df(df_oee_total):
            st.markdown("**전체 OEE**")
            st.dataframe(df_oee_total, use_container_width=True, height=210)

    with col2:
        if not _is_empty_df(df_oee_line):
            st.markdown("**Line별 OEE**")
            st.dataframe(df_oee_line, use_container_width=True, height=210)

    with col3:
        if not _is_empty_df(df_oee_station):
            st.markdown("**Station별 OEE**")
            st.dataframe(df_oee_station, use_container_width=True, height=260)

    st.divider()

fn_planned = _get_api_fn("get_report_i_planned_stop_time")
df_planned = _df_from_resp(_safe_call(fn_planned, active_day, active_shift)) if fn_planned else pd.DataFrame()
df_planned = _drop_cols(df_planned, ["total_planned_time", "updated_at"])
if not _is_empty_df(df_planned):
    st.markdown("### 📌 계획 정지 시간")
    st.dataframe(df_planned, use_container_width=True, height=260)
    st.divider()

fn_non = _get_api_fn("get_report_i_non_time")
df_non = _df_from_resp(_safe_call(fn_non, active_day, active_shift)) if fn_non else pd.DataFrame()
df_non = _drop_cols(df_non, ["vision1_non_time", "vision2_non_time", "total_vision_non_time", "updated_at"])
if not _is_empty_df(df_non):
    st.markdown("### 📌 비가동 시간")
    st.dataframe(df_non, use_container_width=True, height=260)
    st.divider()

fn_amt = _get_api_fn("get_report_a_station_final_amount")
df_amt = _drop_updated_at(_df_from_resp(_safe_call(fn_amt, active_day, active_shift))) if fn_amt else pd.DataFrame()
if not _is_empty_df(df_amt):
    st.markdown("### 📌 품번별 총 생산량")
    st.dataframe(df_amt, use_container_width=True, height=320)
    st.divider()

fn_pct = (
    _get_api_fn("get_report_b_station_daily_percentage")
    or _get_api_fn("get_report_b_station_percentage")
    or _get_api_fn("get_report_b_station_daily")
    or _get_api_fn("get_report_b_station_percentage_daily")
    or _get_api_fn("get_report_b_station_percentage")
)
df_pct = _drop_updated_at(_df_from_resp(_safe_call(fn_pct, active_day, active_shift))) if fn_pct else pd.DataFrame()
if not _is_empty_df(df_pct):
    st.markdown("### 📌 TEST 합격률")
    st.dataframe(df_pct, use_container_width=True, height=240)
    st.divider()

fail_sections = [
    ("FCT 1회 검사 FAIL LIST", get_report_c_fct_step_1time),
    ("FCT 2회 검사 FAIL LIST", get_report_c_fct_step_2time),
    ("FCT 3회 이상 검사 FAIL LIST", get_report_c_fct_step_3over),
    ("Vision 1회 검사 FAIL LIST", get_report_d_vision_step_1time),
    ("Vision 2회 검사 FAIL LIST", get_report_d_vision_step_2time),
    ("Vision 3회 이상 검사 FAIL LIST", get_report_d_vision_step_3over),
]

shown_any = False
for title, fn in fail_sections:
    resp = _safe_call(fn, active_day, active_shift)
    df = _drop_updated_at(_df_from_resp(resp))
    if _is_empty_df(df):
        continue
    if not shown_any:
        st.markdown("### 📌 FAIL LIST")
        shown_any = True
    st.markdown(f"**{title}**")
    st.dataframe(df, use_container_width=True, height=260)
    st.markdown("")