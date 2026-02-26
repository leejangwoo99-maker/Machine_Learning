# app/streamlit_app/pages/01_production_status.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Any, Optional
import hashlib
import json
import time as time_mod
import os

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import api_client as api
from api_client import (
    get_worker_info,
    get_email_list,
    get_remark_info,
    get_planned_time_today,
    get_mastersample_test_info,
    post_worker_info_sync,
    post_email_list_sync,
    post_remark_info_sync,
    post_planned_time_sync,
    get_sections_latest,
    get_nonop_window,
    get_nonop_changes,
    post_nonop_update,
)

matplotlib.rcParams["font.family"] = ["Malgun Gothic", "NanumGothic", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

KST = ZoneInfo("Asia/Seoul")
STATIONS = ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"]

COLOR_RUN = "#8FD3B6"
COLOR_PLAN = "#F6E7A7"
COLOR_STOP = "#F4B4B4"
COLOR_IDLE = "#D9D9D9"

REASON_OPTIONS = ["", "sparepart 교체", "기타"]
SPAREPART_OPTIONS = ["", "usb_a", "usb_c", "mini_b", "probe_pin", "relay_board", "pd_board", "pass_mark"]

ALARM_ALLOWED_TYPES = {"권고", "긴급", "교체"}
MIN_SEG_MIN = 0.12  # 약 7.2초

NONOP_MAX_BUFFER = 500
NONOP_VIEW_STEP = 10
NONOP_VIEW_DEFAULT = 10


# =========================================================
# ✅ ENV helpers (운영에서 .env로 조절)
# =========================================================
def _env_int(name: str, default: int, min_v: int, max_v: int) -> int:
    raw = os.getenv(name, "")
    if raw is None:
        return default
    s = str(raw).strip()
    if not s:
        return default
    try:
        v = int(float(s))  # "5.0" 같은 값도 허용
    except Exception:
        return default
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


def _env_float(name: str, default: float, min_v: float, max_v: float) -> float:
    raw = os.getenv(name, "")
    if raw is None:
        return default
    s = str(raw).strip()
    if not s:
        return default
    try:
        v = float(s)
    except Exception:
        return default
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


# ✅ 주기 분리 (기본 5초, 운영에서 ST_*로 조절)
SYNC_EVERY_SEC = _env_int("ST_SYNC_SEC", default=5, min_v=1, max_v=60)
CHART_EVERY_SEC = _env_int("ST_CHART_SEC", default=5, min_v=1, max_v=60)
NONOP_EVERY_SEC = _env_int("ST_NONOP_SEC", default=5, min_v=1, max_v=60)

# ✅ changes API 호출 상한 (기본 2000)
NONOP_CHANGES_LIMIT = _env_int("ST_NONOP_CHANGES_LIMIT", default=2000, min_v=100, max_v=20000)

# ✅ API timeout
SECTIONS_LATEST_TIMEOUT = _env_float("ST_SECTIONS_LATEST_TIMEOUT", default=8.0, min_v=1.0, max_v=30.0)
NONOP_WINDOW_TIMEOUT = _env_float("ST_NONOP_WINDOW_TIMEOUT", default=8.0, min_v=1.0, max_v=30.0)
NONOP_CHANGES_TIMEOUT = _env_float("ST_NONOP_CHANGES_TIMEOUT", default=4.0, min_v=1.0, max_v=30.0)
NONOP_UPDATE_TIMEOUT = _env_float("ST_NONOP_UPDATE_TIMEOUT", default=10.0, min_v=1.0, max_v=60.0)

# ✅ 편집 중 lock(초) - 요청: 15초 고정 (env 무시)
NONOP_EDIT_LOCK_SEC = 15.0


# =========================================================
# ✅ "희미해짐(디밍)" 제거 + 클릭 블로킹 방지 (강화본)
# =========================================================
def inject_no_dim_fade_keep_loading():
    components.html(
        """
        <style>
          html, body {
            opacity: 1 !important;
            filter: none !important;
            transition: none !important;
            animation: none !important;
          }

          div[data-testid="stApp"],
          div[data-testid="stAppViewContainer"],
          div[data-testid="stAppViewContainer"] > div,
          section.main,
          div[data-testid="stMainBlockContainer"],
          div[data-testid="stVerticalBlock"],
          div.block-container,
          div[data-testid="stHeader"],
          header,
          footer {
            opacity: 1 !important;
            filter: none !important;
            transition: none !important;
            animation: none !important;
          }

          div[style*="position: fixed"][style*="inset: 0px"],
          div[style*="position:fixed"][style*="inset:0px"] {
            background: rgba(0,0,0,0) !important;
            pointer-events: none !important;
          }
        </style>

        <script>
        (function(){
          try{
            const doc = window.parent.document;

            const ROOT_SEL = [
              'div[data-testid="stApp"]',
              'div[data-testid="stAppViewContainer"]',
              'section.main',
              'div.block-container',
              'div[data-testid="stMainBlockContainer"]'
            ];

            function forceOpacity1Deep(root){
              if(!root) return;

              root.style.opacity = "1";
              root.style.filter = "none";
              root.style.transition = "none";
              root.style.animation = "none";

              const all = root.querySelectorAll("*");
              for(const el of all){
                const cs = window.getComputedStyle(el);
                const op = parseFloat(cs.opacity || "1");
                if(op < 0.98){
                  el.style.opacity = "1";
                }
                if((cs.filter || "") !== "none"){
                  el.style.filter = "none";
                }
              }
            }

            function neutralizeFullScreenOverlay(){
              const allDiv = Array.from(doc.querySelectorAll("div"));
              const w = window.innerWidth, h = window.innerHeight;

              for(const el of allDiv){
                const cs = window.getComputedStyle(el);
                if(cs.position !== "fixed") continue;

                const r = el.getBoundingClientRect();
                const isFull =
                  r.left <= 0 && r.top <= 0 &&
                  r.width >= (w - 2) && r.height >= (h - 2);

                if(!isFull) continue;

                el.style.background = "rgba(0,0,0,0)";
                el.style.pointerEvents = "none";
              }
            }

            function applyAll(){
              for(const sel of ROOT_SEL){
                doc.querySelectorAll(sel).forEach(forceOpacity1Deep);
              }
              neutralizeFullScreenOverlay();
            }

            applyAll();

            const obs = new MutationObserver(applyAll);
            obs.observe(doc.body, {childList:true, subtree:true, attributes:true});

            let n = 0;
            const itv = setInterval(()=>{
              applyAll();
              n += 1;
              if(n > 120) clearInterval(itv);
            }, 100);

          }catch(e){}
        })();
        </script>
        """,
        height=0,
    )


# =========================================================
# Core helpers
# =========================================================
def _rerun_fragment_safe():
    try:
        st.rerun(scope="fragment")
    except TypeError:
        st.rerun()


def now_kst() -> datetime:
    return datetime.now(tz=KST)


def detect_shift(dt: datetime) -> str:
    if dt.hour > 8 and dt.hour < 20:
        return "day"
    if dt.hour == 8 and dt.minute >= 30:
        return "day"
    if dt.hour == 20 and dt.minute < 30:
        return "day"
    return "night"


def get_window(dt: datetime, shift: str) -> Tuple[datetime, datetime]:
    if shift == "day":
        start = dt.replace(hour=8, minute=30, second=0, microsecond=0)
        end = dt.replace(hour=20, minute=30, second=0, microsecond=0)
        return start, end

    if dt.time() >= dt.replace(hour=20, minute=30, second=0, microsecond=0).time():
        start = dt.replace(hour=20, minute=30, second=0, microsecond=0)
        end = (start + timedelta(days=1)).replace(hour=8, minute=30, second=0, microsecond=0)
    else:
        start = (dt - timedelta(days=1)).replace(hour=20, minute=30, second=0, microsecond=0)
        end = dt.replace(hour=8, minute=30, second=0, microsecond=0)
    return start, end


def parse_hms(base_start: datetime, s: Any) -> Optional[datetime]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None

    hh = mm = 0
    sec_float = 0.0

    try:
        if ":" in t:
            p = t.split(":")
            hh = int(p[0])
            mm = int(p[1]) if len(p) > 1 else 0
            sec_float = float(p[2]) if len(p) > 2 else 0.0
        else:
            if len(t) == 6 and t.isdigit():
                hh, mm, sec_float = int(t[:2]), int(t[2:4]), float(int(t[4:6]))
            elif len(t) == 4 and t.isdigit():
                hh, mm, sec_float = int(t[:2]), int(t[2:4]), 0.0
            else:
                return None
    except Exception:
        return None

    sec_i = int(sec_float)
    micro = int(round((sec_float - sec_i) * 1_000_000))
    if micro >= 1_000_000:
        sec_i += 1
        micro = 0

    dt = base_start.replace(hour=hh, minute=mm, second=sec_i, microsecond=micro)

    # 야간: 00~11시는 다음날로
    if base_start.hour == 20 and hh < 12:
        dt = dt + timedelta(days=1)
    return dt


def _parse_iso_any(s: Any) -> Optional[datetime]:
    if s is None:
        return None
    t = str(s).strip()
    if not t:
        return None
    try:
        return datetime.fromisoformat(t)
    except Exception:
        return None


def fmt_hms(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if not s:
        return ""

    if ("T" in s) or (len(s) >= 10 and s[4] == "-" and s[7] == "-"):
        dt = _parse_iso_any(s)
        if dt is None:
            return ""
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt.astimezone(KST).strftime("%H:%M:%S")

    if ":" in s:
        parts = s.split(":")
        if len(parts) >= 3:
            sec = parts[2].split(".")[0]
            return f"{parts[0].zfill(2)}:{parts[1].zfill(2)}:{sec.zfill(2)}"
    return ""


def parse_any_ts(win_start: datetime, v: Any) -> Optional[datetime]:
    s = "" if v is None else str(v).strip()
    if not s:
        return None

    if ("T" in s) or (len(s) >= 10 and s[4] == "-" and s[7] == "-"):
        dt = _parse_iso_any(s)
        if dt is None:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=KST)
        return dt

    return parse_hms(win_start, s)


def normalize_day_yyyymmdd(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


# =========================================================
# perf log
# =========================================================
def _perf_push(name: str, ms: float, extra: Optional[Dict[str, Any]] = None):
    if "perf_logs" not in st.session_state:
        st.session_state.perf_logs = []
    item = {"ts": now_kst().isoformat(timespec="seconds"), "name": name, "ms": float(ms)}
    if extra:
        item.update(extra)
    st.session_state.perf_logs.append(item)
    if len(st.session_state.perf_logs) > 120:
        st.session_state.perf_logs = st.session_state.perf_logs[-120:]


# =========================================================
# Alarm helpers (pk/message)
# =========================================================
def alarm_message(station: str, sparepart: str, type_alarm: str) -> str:
    stn = station or "Unknown"
    sp = sparepart or "sparepart"
    if type_alarm == "권고":
        return f"{stn}, {sp} 교체 권고 드립니다."
    if type_alarm == "긴급":
        return f"{stn}, {sp} 교체 긴급합니다."
    if type_alarm == "교체":
        return f"{stn}, {sp} 교체 타이밍이 지났습니다."
    return ""


def alarm_pk_from_token(row: Dict[str, Any]) -> str:
    key = {
        "end_day": normalize_day_yyyymmdd(row.get("end_day", "")),
        "end_time": str(row.get("end_time", "")).strip(),
        "station": str(row.get("station", "")).strip(),
        "sparepart": str(row.get("sparepart", "")).strip(),
        "type_alarm": str(row.get("type_alarm", "")).strip(),
    }
    raw = json.dumps(key, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# =========================================================
# ✅ SSE alarm (01페이지에도 적용)
# - 비차단 toast panel
# - sessionStorage acked_alarm_pks 로 중복 방지
# =========================================================
def _api_base_url() -> str:
    for attr in ("API_BASE_URL", "BASE_URL", "API_URL", "API"):
        v = getattr(api, attr, None)
        if isinstance(v, str) and v.strip().startswith("http"):
            return v.strip().rstrip("/")
    return "http://127.0.0.1:8000"


def _mount_alarm_sse(cur_day: str, cur_shift: str):
    base = _api_base_url().rstrip("/")

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
          const ACK_KEY = "acked_alarm_pks";

          // ✅ 02와 동일 계열: 중앙 모달(비차단)
          const OVERLAY_ID_PREFIX = "alarmOverlay_";

          function log(...args) {{
            try {{ console.log("[ALARM_SSE_01]", ...args); }} catch(e) {{}}
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

          function removeExistingOverlays(doc) {{
            const olds = Array.from(doc.querySelectorAll('div[id^="alarmOverlay_"]'));
            for (const el of olds) el.remove();
          }}

          function closeOverlay(doc, pk) {{
            const el = doc.getElementById(OVERLAY_ID_PREFIX + pk);
            if (el) el.remove();
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

          // ✅ 중앙 배치 + 02처럼 큰 박스 + 비차단(overlay 클릭 막지 않음)
          function showCenteredNonBlockingModal(pk, message) {{
            const acked = getAcked();
            if (acked.includes(pk)) {{
              log("skip acked pk=", pk);
              return;
            }}

            const doc = W.document;

            // 이미 같은 pk가 떠있으면 스킵
            if (doc.getElementById(OVERLAY_ID_PREFIX + pk)) return;

            // ✅ 단일 알람만 유지(02와 동일 UX)
            removeExistingOverlays(doc);

            const overlay = doc.createElement("div");
            overlay.id = OVERLAY_ID_PREFIX + pk;
            overlay.style.cssText = `
              position: fixed;
              z-index: 2147483647;
              left: 0; top: 0; width: 100%; height: 100%;
              display: flex; align-items: center; justify-content: center;
              background: rgba(0,0,0,0.0);     /* ✅ 배경 딤 제거 */
              pointer-events: none;            /* ✅ 비차단 핵심 */
            `;

            const box = doc.createElement("div");
            box.style.cssText = `
              width: 62%;
              max-width: 980px;
              background: white;
              border-radius: 14px;
              padding: 18px 22px;
              box-shadow: 0 10px 28px rgba(0,0,0,0.25);
              font-family: sans-serif;
              border-left: 10px solid #f2c94c;
              pointer-events: auto;            /* ✅ 박스만 클릭 가능 */
            `;

            box.innerHTML = `
              <div style="display:flex; align-items:center; justify-content:space-between;">
                <div style="font-size:22px; font-weight:800;">⚠️ 알람 발생</div>
                <button id="alarmCloseX_${{pk}}" style="border:none;background:transparent;font-size:22px;cursor:pointer;">✕</button>
              </div>

              <div style="margin-top:14px; padding:12px 14px; border-radius:10px;
                          background:#fff9db; color:#5c4a00; font-size:16px;">
                ${{safeText(message)}}
              </div>

              <div style="margin-top:16px; display:flex; gap:12px;">
                <button id="alarmAckBtn_${{pk}}" style="flex:1; padding:10px 0; border-radius:10px;
                        border:1px solid #ddd; background:white; cursor:pointer; font-size:15px;">
                  확인
                </button>
              </div>
            `;

            overlay.appendChild(box);
            doc.body.appendChild(overlay);

            const xBtn = box.querySelector("#alarmCloseX_" + pk);
            const aBtn = box.querySelector("#alarmAckBtn_" + pk);

            if (xBtn) xBtn.addEventListener("click", function() {{
              // ✅ X는 "닫기만" (ACK 저장 안 함)
              closeOverlay(doc, pk);
            }});

            if (aBtn) aBtn.addEventListener("click", function() {{
              // ✅ 확인은 ACK 저장 + 닫기
              addAck(pk);
              closeOverlay(doc, pk);
            }});
          }}

          function handleAlarm(obj, fromEvent) {{
            if (!obj || typeof obj !== "object") return;
            const row = obj.row;
            let pk  = String(obj.pk || "");
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
              const id = row.id != null ? String(row.id) : "";
              if (!id) {{
                log(fromEvent, "no pk/id", row);
                return;
              }}
              pk = id;
            }}

            showCenteredNonBlockingModal(pk, makeMessage(row));
          }}

          if (!W.__alarmSSE01) {{
            W.__alarmSSE01 = {{ url: null, es: null }};
          }}

          function start() {{
            if (W.__alarmSSE01.es && W.__alarmSSE01.url === url) return;

            if (W.__alarmSSE01.es) {{
              try {{ W.__alarmSSE01.es.close(); }} catch(e) {{}}
              W.__alarmSSE01.es = null;
            }}

            W.__alarmSSE01.url = url;

            log("connect:", url);
            const es = new EventSource(url);
            W.__alarmSSE01.es = es;

            es.addEventListener("hello", (ev) => {{
              log("hello", ev.data);
            }});

            es.addEventListener("init", (ev) => {{
              log("init", ev.data);
              try {{
                const obj = JSON.parse(ev.data || "{{}}");
                if (obj && obj.row) handleAlarm(obj, "init");
              }} catch(e) {{}}
            }});

            es.addEventListener("alarm", (ev) => {{
              log("alarm", ev.data);
              try {{
                const obj = JSON.parse(ev.data || "{{}}");
                handleAlarm(obj, "alarm");
              }} catch(e) {{}}
            }});

            es.addEventListener("error", (ev) => {{
              log("error event", ev);
            }});
          }}

          start();
        }})();
        </script>
        """,
        height=0,
    )

# =========================================================
# Alarm modal (기존 유지 - fallback)
# =========================================================
def show_alarm_modal_no_rerun(message: str, pk: str):
    safe_msg = (
        str(message)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )

    html = f"""
    <script>
    (function() {{
      const pk = "{pk}";
      const doc = window.parent.document;

      const existing = doc.getElementById("alarmModal_" + pk);
      if (existing) return;

      const old = doc.querySelector('[id^="alarmModal_"]');
      if (old) old.remove();

      const overlay = doc.createElement("div");
      overlay.id = "alarmModal_" + pk;
      overlay.style.cssText = `
        position: fixed; z-index: 2147483647;
        left: 0; top: 0; width: 100%; height: 100%;
        background: rgba(0,0,0,0.35);
        display: flex; align-items: center; justify-content: center;
      `;

      const box = doc.createElement("div");
      box.style.cssText = `
        width: 62%; background: white; border-radius: 14px;
        padding: 18px 22px; box-shadow: 0 10px 28px rgba(0,0,0,0.25);
        font-family: sans-serif;
      `;

      box.innerHTML = `
        <div style="display:flex; align-items:center; justify-content:space-between;">
          <div style="font-size:22px; font-weight:800;">⚠️ 알람 발생</div>
          <button id="alarmCloseX" style="border:none;background:transparent;font-size:22px;cursor:pointer;">✕</button>
        </div>

        <div style="margin-top:14px; padding:12px 14px; border-radius:10px;
                    background:#fff9db; color:#5c4a00; font-size:16px;">
          {safe_msg}
        </div>

        <div style="margin-top:16px; display:flex; gap:12px;">
          <button id="alarmAckBtn" style="flex:1; padding:10px 0; border-radius:10px;
                  border:1px solid #ddd; background:white; cursor:pointer; font-size:15px;">
            확인
          </button>

          <button id="alarmRerunBtn" style="flex:1; padding:10px 0; border-radius:10px;
                  border:1px solid #ddd; background:white; cursor:pointer; font-size:15px;">
            새로고침
          </button>
        </div>
      `;

      overlay.appendChild(box);
      doc.body.appendChild(overlay);

      function closeModal() {{
        const el = doc.getElementById("alarmModal_" + pk);
        if (el) el.remove();
      }}

      doc.getElementById("alarmCloseX").addEventListener("click", closeModal);

      doc.getElementById("alarmAckBtn").addEventListener("click", function() {{
        closeModal();
        try {{
          const url = new URL(window.parent.location.href);
          url.searchParams.set("ack_alarm_pk", pk);
          window.parent.history.replaceState({{}}, "", url.toString());
        }} catch(e) {{}}
      }});

      doc.getElementById("alarmRerunBtn").addEventListener("click", function() {{
        try {{
          window.parent.postMessage({{isStreamlitMessage: true, type: 'streamlit:rerunScript'}}, '*');
        }} catch(e) {{}}
        closeModal();
      }});
    }})();
    </script>
    """
    components.html(html, height=0)


def _clear_ack_param_in_browser():
    components.html(
        """
        <script>
          (function(){
            try{
              const url = new URL(window.parent.location.href);
              if (url.searchParams.has("ack_alarm_pk")) {
                url.searchParams.delete("ack_alarm_pk");
                window.parent.history.replaceState({}, "", url.toString());
              }
            }catch(e){}
          })();
        </script>
        """,
        height=0,
    )


# =========================================================
# ✅ st.dialog X-close watchdog
# =========================================================
def is_any_modal_open() -> bool:
    return bool(str(st.session_state.get("active_modal", "") or "").strip())


def _inject_modal_dom_watchdog():
    if not is_any_modal_open():
        return

    components.html(
        """
        <script>
        (function(){
          try{
            const doc = window.parent.document;

            const closeSelectors = [
              'button[aria-label="Close"]',
              'button[title="Close"]',
              'button[data-testid="close-button"]',
              'button[aria-label="close"]'
            ];

            function clickOurCloseButton(){
              const labels = ["닫기(Email)", "닫기(Barcode)", "닫기(Planned)"];
              const btns = Array.from(doc.querySelectorAll("button"));
              for (const b of btns){
                const t = (b.innerText || "").trim();
                if (labels.includes(t)){
                  b.click();
                  return true;
                }
              }
              return false;
            }

            function attachToXButtons(){
              let xs = [];
              for (const sel of closeSelectors){
                xs = xs.concat(Array.from(doc.querySelectorAll(sel)));
              }
              xs = Array.from(new Set(xs));

              xs.forEach(x=>{
                if (x.__jw_bound) return;
                x.__jw_bound = true;
                x.addEventListener("click", function(){
                  setTimeout(()=>{ clickOurCloseButton(); }, 0);
                }, true);
              });
            }

            attachToXButtons();

            const obs = new MutationObserver(function(){
              attachToXButtons();
            });
            obs.observe(doc.body, {childList:true, subtree:true});
          }catch(e){}
        })();
        </script>
        """,
        height=0,
    )


# =========================================================
# (안전망) query param 기반 close_modal 핸들러
# =========================================================
def _qparam_has(name: str) -> bool:
    try:
        q = st.query_params
        v = q.get(name, None)
        if v is None:
            return False
        if isinstance(v, (list, tuple)):
            return any(str(x).strip() for x in v)
        return bool(str(v).strip())
    except Exception:
        return False


def _handle_close_modal_param():
    try:
        if _qparam_has("close_modal"):
            st.session_state["active_modal"] = ""
            st.session_state["modal_email"] = False
            st.session_state["modal_barcode"] = False
            st.session_state["modal_planned"] = False

            st.session_state["modal_email_need_load"] = True
            st.session_state["modal_barcode_need_load"] = True
            st.session_state["modal_planned_need_load"] = True

            st.session_state["nonop_lock_until_ts"] = 0.0
            st.session_state["nonop_is_saving"] = False

            try:
                st.query_params.pop("close_modal")
            except Exception:
                pass

            components.html(
                """
                <script>
                  (function(){
                    try{
                      const url = new URL(window.parent.location.href);
                      if (url.searchParams.has("close_modal")) {
                        url.searchParams.delete("close_modal");
                        window.parent.history.replaceState({}, "", url.toString());
                      }
                    }catch(e){}
                  })();
                </script>
                """,
                height=0,
            )
            st.rerun()
    except Exception:
        pass


# =========================================================
# nonop: state helpers
# =========================================================
def nonop_key(row: Dict[str, Any]) -> str:
    return (
        f"{normalize_day_yyyymmdd(row.get('prod_day', row.get('end_day','')))}|"
        f"{str(row.get('shift_type', '')).strip().lower()}|"
        f"{str(row.get('station','')).strip()}|"
        f"{str(row.get('from_ts', row.get('from_time',''))).strip()}|"
        f"{str(row.get('to_ts', row.get('to_time',''))).strip()}"
    )


def nonop_sort_ts(prod_day: str, shift: str, row: Dict[str, Any], win_start: datetime) -> float:
    ft = parse_any_ts(win_start, row.get("from_ts", row.get("from_time")))
    if ft is None:
        return 0.0
    try:
        return ft.timestamp()
    except Exception:
        return 0.0


def _nonop_reset_if_needed(prod_day: str, shift: str):
    scope = f"{prod_day}:{shift}"
    if st.session_state.get("nonop_scope") != scope:
        st.session_state.nonop_scope = scope

        st.session_state.nonop_chart_rows = []
        st.session_state.nonop_chart_loaded_once = False

        st.session_state.nonop_all_buf = []
        st.session_state.nonop_all_keyset = set()
        st.session_state.nonop_view_limit = NONOP_VIEW_DEFAULT
        st.session_state.nonop_loaded_once = False

        st.session_state.nonop_cursor = {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"}

        st.session_state.nonop_last_poll_ts = 0.0

        st.session_state["nonop_is_saving"] = False
        st.session_state["nonop_editor_snap"] = ""


def _cursor_from_token(tok: str) -> Dict[str, Any]:
    cur = st.session_state.get("nonop_cursor", None)
    if not isinstance(cur, dict):
        cur = {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"}

    tok = str(tok or "").strip()
    if not tok or tok.startswith("__ERR__"):
        return cur

    try:
        obj = json.loads(tok)
        if not isinstance(obj, dict):
            return cur
        max_id = int(obj.get("max_id", cur.get("max_id", 0)) or 0)
        max_u = str(obj.get("max_updated_at", obj.get("max_updated_at", "")) or "").strip()
        if not max_u:
            max_u = str(cur.get("max_updated_at", "1970-01-01T00:00:00+09:00"))
        return {"max_id": max_id, "max_updated_at": max_u}
    except Exception:
        return cur


def _merge_nonop_changes(prod_day: str, shift: str, changes: List[Dict[str, Any]]):
    if not changes:
        return

    buf: List[Dict[str, Any]] = st.session_state.get("nonop_all_buf", []) or []
    keyset = st.session_state.get("nonop_all_keyset", set()) or set()

    idx_map: Dict[str, int] = {}
    for i, r in enumerate(buf):
        idx_map[nonop_key(r)] = i

    for r in changes:
        k = nonop_key(r)
        if k in idx_map:
            buf[idx_map[k]].update(r)
        else:
            keyset.add(k)
            buf.insert(0, r)

    win_start, _ = get_window(now_kst(), shift)
    buf.sort(key=lambda rr: nonop_sort_ts(prod_day, shift, rr, win_start), reverse=True)
    if len(buf) > NONOP_MAX_BUFFER:
        buf = buf[:NONOP_MAX_BUFFER]

    st.session_state.nonop_all_buf = buf
    st.session_state.nonop_all_keyset = keyset
    st.session_state.nonop_loaded_once = True

    chart_rows: List[Dict[str, Any]] = st.session_state.get("nonop_chart_rows", []) or []
    cidx: Dict[int, int] = {}
    for i, r in enumerate(chart_rows):
        try:
            cidx[int(r.get("id") or 0)] = i
        except Exception:
            pass

    for r in changes:
        try:
            rid = int(r.get("id") or 0)
        except Exception:
            rid = 0
        if rid > 0 and rid in cidx:
            chart_rows[cidx[rid]].update(r)
        else:
            chart_rows.append(r)

    win_start, _ = get_window(now_kst(), shift)

    def _chart_sort_key(x: Dict[str, Any]) -> float:
        dt = parse_any_ts(win_start, x.get("from_ts", x.get("from_time")))
        return dt.timestamp() if dt else 0.0

    chart_rows.sort(key=_chart_sort_key)
    st.session_state.nonop_chart_rows = chart_rows
    st.session_state.nonop_chart_loaded_once = True


def _nonop_load_window_once(prod_day: str, shift: str) -> Dict[str, Any]:
    if st.session_state.get("nonop_chart_loaded_once", False) and st.session_state.get("nonop_loaded_once", False):
        return {"ok": True, "count": len(st.session_state.get("nonop_chart_rows", []) or [])}

    t0 = time_mod.perf_counter()
    try:
        payload = get_nonop_window(prod_day, shift, timeout=float(NONOP_WINDOW_TIMEOUT))
        ms = (time_mod.perf_counter() - t0) * 1000.0
        _perf_push("api_nonop_window", ms, {"count": len((payload or {}).get("items", []) or [])})

        items = (payload or {}).get("items", []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            items = []

        win_start, _ = get_window(now_kst(), shift)

        def _chart_sort_key(x: Dict[str, Any]) -> float:
            dt = parse_any_ts(win_start, x.get("from_ts", x.get("from_time")))
            return dt.timestamp() if dt else 0.0

        items.sort(key=_chart_sort_key)
        st.session_state.nonop_chart_rows = items
        st.session_state.nonop_chart_loaded_once = True

        items2 = list(items)
        items2.sort(key=lambda r: nonop_sort_ts(prod_day, shift, r, win_start), reverse=True)

        keyset = set()
        buf = []
        for r in items2:
            k = nonop_key(r)
            if k in keyset:
                continue
            keyset.add(k)
            buf.append(r)

        st.session_state.nonop_all_buf = buf[:NONOP_MAX_BUFFER]
        st.session_state.nonop_all_keyset = keyset
        st.session_state.nonop_loaded_once = True

        cur = st.session_state.get("nonop_cursor", {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"})
        if isinstance(payload, dict):
            try:
                cur["max_id"] = max(int(cur.get("max_id", 0) or 0), int(payload.get("max_id", 0) or 0))
            except Exception:
                pass
            mu = str(payload.get("max_updated_at", "") or "").strip()
            if mu:
                cur["max_updated_at"] = mu
        st.session_state.nonop_cursor = cur

        return {"ok": True, "count": len(items)}
    except Exception as e:
        ms = (time_mod.perf_counter() - t0) * 1000.0
        _perf_push("api_nonop_window_err", ms, {"err": str(e)[:120]})
        return {"ok": False, "error": str(e)}


def _nonop_apply_changes_if_needed(prod_day: str, shift: str, new_cursor: Dict[str, Any]) -> Dict[str, Any]:
    cur = st.session_state.get("nonop_cursor", {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"})
    if not isinstance(cur, dict):
        cur = {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"}

    if int(new_cursor.get("max_id", 0) or 0) == int(cur.get("max_id", 0) or 0) and str(
        new_cursor.get("max_updated_at", "") or ""
    ) == str(cur.get("max_updated_at", "") or ""):
        return {"ok": True, "changed": False}

    lock_until = float(st.session_state.get("nonop_lock_until_ts", 0.0) or 0.0)
    if time_mod.time() < lock_until:
        st.session_state.nonop_cursor = new_cursor
        return {"ok": True, "changed": True, "skipped_by_lock": True}

    since_id = int(cur.get("max_id", 0) or 0)
    since_u = str(cur.get("max_updated_at", "1970-01-01T00:00:00+09:00") or "")

    t0 = time_mod.perf_counter()
    try:
        payload = get_nonop_changes(
            prod_day=prod_day,
            shift_type=shift,
            since_id=since_id,
            since_updated_at=since_u,
            limit=int(NONOP_CHANGES_LIMIT),
            timeout=float(NONOP_CHANGES_TIMEOUT),
        )
        api_ms = (time_mod.perf_counter() - t0) * 1000.0

        rows = (payload or {}).get("items", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []

        t1 = time_mod.perf_counter()
        _merge_nonop_changes(prod_day, shift, rows)
        merge_ms = (time_mod.perf_counter() - t1) * 1000.0

        _perf_push("api_nonop_changes(token)", api_ms, {"fetched": len(rows), "since_id": since_id})
        _perf_push("merge_nonop_changes(token)", merge_ms, {"fetched": len(rows)})

        st.session_state.nonop_cursor = new_cursor
        return {"ok": True, "changed": True, "fetched": len(rows)}
    except Exception as e:
        api_ms = (time_mod.perf_counter() - t0) * 1000.0
        _perf_push("api_nonop_changes_err(token)", api_ms, {"err": str(e)[:120]})
        return {"ok": False, "changed": True, "error": str(e)}


def _nonop_poll_changes(prod_day: str, shift: str) -> Dict[str, Any]:
    lock_until = float(st.session_state.get("nonop_lock_until_ts", 0.0) or 0.0)
    if time_mod.time() < lock_until:
        return {"ok": True, "skipped_by_lock": True}

    cur = st.session_state.get("nonop_cursor", {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"})
    if not isinstance(cur, dict):
        cur = {"max_id": 0, "max_updated_at": "1970-01-01T00:00:00+09:00"}

    since_id = int(cur.get("max_id", 0) or 0)
    since_u = str(cur.get("max_updated_at", "1970-01-01T00:00:00+09:00") or "")

    t0 = time_mod.perf_counter()
    try:
        payload = get_nonop_changes(
            prod_day=prod_day,
            shift_type=shift,
            since_id=since_id,
            since_updated_at=since_u,
            limit=int(NONOP_CHANGES_LIMIT),
            timeout=float(NONOP_CHANGES_TIMEOUT),
        )
        api_ms = (time_mod.perf_counter() - t0) * 1000.0

        rows = (payload or {}).get("items", []) if isinstance(payload, dict) else []
        if not isinstance(rows, list):
            rows = []

        t1 = time_mod.perf_counter()
        _merge_nonop_changes(prod_day, shift, rows)
        merge_ms = (time_mod.perf_counter() - t1) * 1000.0

        if isinstance(payload, dict):
            try:
                cur["max_id"] = max(int(cur.get("max_id", 0) or 0), int(payload.get("max_id", 0) or 0))
            except Exception:
                pass
            mu = str(payload.get("max_updated_at", "") or "").strip()
            if mu:
                cur["max_updated_at"] = mu
        st.session_state.nonop_cursor = cur

        _perf_push("api_nonop_changes(poll)", api_ms, {"fetched": len(rows), "since_id": since_id})
        _perf_push("merge_nonop_changes(poll)", merge_ms, {"fetched": len(rows)})

        return {"ok": True, "fetched": len(rows)}
    except Exception as e:
        api_ms = (time_mod.perf_counter() - t0) * 1000.0
        _perf_push("api_nonop_changes_err(poll)", api_ms, {"err": str(e)[:120]})
        return {"ok": False, "error": str(e)}


# =========================================================
# planned: chart cache
# =========================================================
def _planned_reset_if_needed(end_day: str, shift: str):
    scope = f"{end_day}:{shift}"
    if st.session_state.get("planned_scope") != scope:
        st.session_state.planned_scope = scope
        st.session_state.planned_rows_cache = []
        st.session_state.planned_loaded_once = False


def _planned_load_for_chart_if_needed(end_day: str, shift: str) -> Dict[str, Any]:
    try:
        _planned_reset_if_needed(end_day, shift)
        if not st.session_state.get("planned_loaded_once", False):
            t0 = time_mod.perf_counter()
            rows = get_planned_time_today(end_day, shift) or []
            ms = (time_mod.perf_counter() - t0) * 1000.0
            _perf_push("api_planned_seed", ms, {"count": len(rows)})
            st.session_state.planned_rows_cache = rows
            st.session_state.planned_loaded_once = True
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _planned_refresh_for_chart(end_day: str, shift: str) -> Dict[str, Any]:
    try:
        t0 = time_mod.perf_counter()
        rows = get_planned_time_today(end_day, shift) or []
        ms = (time_mod.perf_counter() - t0) * 1000.0
        _perf_push("api_planned_refresh", ms, {"count": len(rows)})
        st.session_state.planned_rows_cache = rows
        st.session_state.planned_loaded_once = True
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# =========================================================
# fragments
# =========================================================
@st.fragment(run_every=SYNC_EVERY_SEC)
def frag_sync_data(end_day: str, shift: str):
    if is_any_modal_open():
        return

    prev_tokens = dict(st.session_state.get("section_tokens", {}) or {})
    prev_alarm_effective = str(st.session_state.get("alarm_token_effective", "") or "")

    t0 = time_mod.perf_counter()
    try:
        latest = get_sections_latest(end_day, shift, timeout=float(SECTIONS_LATEST_TIMEOUT))
        ms = (time_mod.perf_counter() - t0) * 1000.0
        _perf_push("api_sections_latest", ms)
        latest_tokens = (latest or {}).get("tokens", {}) if isinstance(latest, dict) else {}
    except Exception as e:
        st.session_state["sync_err"] = f"sections_latest: {e}"
        return

    if not latest_tokens:
        return

    alarm_tok = str(latest_tokens.get("alarm", "") or "")
    if not alarm_tok:
        try:
            latest_alarm = get_sections_latest(end_day, "day", timeout=float(SECTIONS_LATEST_TIMEOUT))
            tok2 = (latest_alarm or {}).get("tokens", {}) if isinstance(latest_alarm, dict) else {}
            alarm_tok = str(tok2.get("alarm", "") or "")
        except Exception:
            alarm_tok = ""

    nonop_changed = bool(prev_tokens) and (prev_tokens.get("nonop_detail") != latest_tokens.get("nonop_detail"))
    planned_changed = bool(prev_tokens) and (prev_tokens.get("planned") != latest_tokens.get("planned"))

    st.session_state.section_tokens = latest_tokens
    st.session_state.alarm_token_effective = alarm_tok

    if nonop_changed:
        _nonop_reset_if_needed(end_day, shift)
        new_cursor = _cursor_from_token(str(latest_tokens.get("nonop_detail", "") or ""))
        _nonop_apply_changes_if_needed(end_day, shift, new_cursor)

    if planned_changed:
        _planned_reset_if_needed(end_day, shift)
        _planned_refresh_for_chart(end_day, shift)

    # ✅ alarm fallback: 토큰 변화 조건 제거 (pk 기준)
    if alarm_tok and (not alarm_tok.startswith("__ERR__")):
        try:
            alarm_row = json.loads(alarm_tok)
            t_alarm = str(alarm_row.get("type_alarm", "")).strip().replace(" ", "")
            if t_alarm in ALARM_ALLOWED_TYPES:
                pk = alarm_pk_from_token(alarm_row)
                if pk not in st.session_state.seen_alarm_pks:
                    msg = alarm_message(
                        station=str(alarm_row.get("station", "")).strip(),
                        sparepart=str(alarm_row.get("sparepart", "")).strip(),
                        type_alarm=t_alarm,
                    )
                    if msg:
                        st.session_state.pending_alarm_pk = pk
                        st.session_state.pending_alarm_msg = msg
        except Exception:
            pass


@st.fragment
def frag_alarm_once():
    pk = str(st.session_state.get("pending_alarm_pk", "") or "")
    msg = str(st.session_state.get("pending_alarm_msg", "") or "")
    if not pk or not msg:
        return
    if pk in st.session_state.seen_alarm_pks:
        st.session_state.pending_alarm_pk = ""
        st.session_state.pending_alarm_msg = ""
        return

    # fallback: 차단 모달(기존 유지)
    show_alarm_modal_no_rerun(msg, pk)
    st.session_state.pending_alarm_pk = ""
    st.session_state.pending_alarm_msg = ""


@st.fragment(run_every=NONOP_EVERY_SEC)
def frag_nonop_table(end_day: str, shift: str):
    c_title, c_unl = st.columns([3.2, 1.0])
    with c_title:
        st.subheader("비가동 시간 상세(sparepart 교체시 반드시 입력)")
    with c_unl:
        if st.button("수동 update", use_container_width=True, key="btn_nonop_unlock"):
            for k in [
                "nonop_lock_until_ts",
                "nonop_is_saving",
                "nonop_editor_snap",
                "nonop_busy",
                "nonop_edit_lock",
                "nonop_refresh_lock",
            ]:
                st.session_state.pop(k, None)
            st.session_state["nonop_lock_until_ts"] = 0.0
            st.session_state["nonop_is_saving"] = False
            try:
                st.session_state.nonop_last_poll_ts = time_mod.time()
                _nonop_poll_changes(end_day, shift)
            except Exception:
                pass
            _rerun_fragment_safe()

    if is_any_modal_open():
        st.info("모달 편집 중에는 자동 갱신을 잠시 멈춥니다.")
        return

    _nonop_reset_if_needed(end_day, shift)
    _nonop_load_window_once(end_day, shift)

    now_ts = float(time_mod.time())
    lock_until = float(st.session_state.get("nonop_lock_until_ts", 0.0) or 0.0)
    is_saving = bool(st.session_state.get("nonop_is_saving", False))
    nonop_disabled = is_saving or (now_ts < lock_until)

    last_poll = float(st.session_state.get("nonop_last_poll_ts", 0.0) or 0.0)
    if (not nonop_disabled) and ((now_ts - last_poll) >= max(1.0, float(NONOP_EVERY_SEC) * 0.8)):
        st.session_state.nonop_last_poll_ts = now_ts
        _nonop_poll_changes(end_day, shift)

    view_limit = int(st.session_state.get("nonop_view_limit", NONOP_VIEW_DEFAULT) or NONOP_VIEW_DEFAULT)

    c_more, c_fold = st.columns(2)
    if c_more.button("더보기(+10)", use_container_width=True, disabled=nonop_disabled, key="btn_nonop_more"):
        st.session_state.nonop_view_limit = view_limit + NONOP_VIEW_STEP
        st.session_state["nonop_lock_until_ts"] = time_mod.time() + 1.5
        _rerun_fragment_safe()

    if c_fold.button("최신10", use_container_width=True, disabled=nonop_disabled, key="btn_nonop_fold"):
        st.session_state.nonop_view_limit = NONOP_VIEW_DEFAULT
        _rerun_fragment_safe()

    nonop_view_rows = (st.session_state.get("nonop_all_buf", []) or [])[: int(st.session_state.nonop_view_limit)]

    display_cols = ["prod_day", "station", "from_ts", "to_ts", "reason", "sparepart"]
    ndf = pd.DataFrame(nonop_view_rows)
    if ndf.empty:
        ndf = pd.DataFrame(columns=display_cols)
    else:
        for c in display_cols:
            if c not in ndf.columns:
                if c == "prod_day":
                    ndf[c] = ndf.get("end_day", "")
                elif c == "from_ts":
                    ndf[c] = ndf.get("from_time", "")
                elif c == "to_ts":
                    ndf[c] = ndf.get("to_time", "")
                else:
                    ndf[c] = ""
        ndf = ndf[display_cols].copy()

    ndf["from_ts_raw"] = ndf["from_ts"].astype(str)
    ndf["to_ts_raw"] = ndf["to_ts"].astype(str)
    ndf_raw = ndf.copy()

    ndf["from_ts"] = ndf["from_ts"].apply(fmt_hms)
    ndf["to_ts"] = ndf["to_ts"].apply(fmt_hms)

    def _snapshot_reason_spare(df: pd.DataFrame) -> str:
        if df.empty:
            return ""
        sub = df[["reason", "sparepart"]].fillna("").astype(str)
        return hashlib.sha256(sub.to_csv(index=False).encode("utf-8")).hexdigest()

    prev_snap = str(st.session_state.get("nonop_editor_snap", "") or "")

    with st.form("nonop_form", clear_on_submit=False):
        edited_nonop = st.data_editor(
            ndf[display_cols].copy(),
            use_container_width=True,
            hide_index=True,
            num_rows="fixed",
            key="nonop_editor_single",
            column_order=display_cols,
            disabled=nonop_disabled,
            column_config={
                "prod_day": st.column_config.TextColumn("prod_day", disabled=True),
                "station": st.column_config.SelectboxColumn("station", options=STATIONS, disabled=True),
                "from_ts": st.column_config.TextColumn("from_ts", disabled=True),
                "to_ts": st.column_config.TextColumn("to_ts", disabled=True),
                "reason": st.column_config.SelectboxColumn("reason", options=REASON_OPTIONS),
                "sparepart": st.column_config.SelectboxColumn("sparepart", options=SPAREPART_OPTIONS),
            },
        )
        save_nonop = st.form_submit_button("비가동 상세 저장", use_container_width=True, disabled=nonop_disabled)

    try:
        cur_snap = _snapshot_reason_spare(edited_nonop)
    except Exception:
        cur_snap = ""

    if (not is_saving) and cur_snap and cur_snap != prev_snap:
        st.session_state["nonop_lock_until_ts"] = time_mod.time() + float(NONOP_EDIT_LOCK_SEC)
        st.session_state["nonop_editor_snap"] = cur_snap
    elif cur_snap and not prev_snap:
        st.session_state["nonop_editor_snap"] = cur_snap

    msg_slot = st.empty()

    if save_nonop:
        st.session_state["nonop_is_saving"] = True
        st.session_state["nonop_lock_until_ts"] = time_mod.time() + 0.6

        try:
            rows = edited_nonop.to_dict("records")

            payload = []
            for i, rr in enumerate(rows):
                reason = str(rr.get("reason", "")).strip()
                spare = str(rr.get("sparepart", "")).strip()
                if reason != "sparepart 교체":
                    spare = ""

                raw_from = str(ndf_raw.iloc[i].get("from_ts_raw", "")).strip()
                raw_to = str(ndf_raw.iloc[i].get("to_ts_raw", "")).strip()

                payload.append(
                    {
                        "prod_day": normalize_day_yyyymmdd(rr.get("prod_day", "")) or end_day,
                        "shift_type": shift,
                        "station": str(rr.get("station", "")).strip(),
                        "from_ts": raw_from,
                        "to_ts": raw_to,
                        "reason": reason,
                        "sparepart": spare,
                    }
                )

            payload = [x for x in payload if x["station"] and x["from_ts"] and x["to_ts"]]

            t0 = time_mod.perf_counter()
            post_nonop_update(payload, timeout=float(NONOP_UPDATE_TIMEOUT))
            ms = (time_mod.perf_counter() - t0) * 1000.0
            _perf_push("api_nonop_update", ms, {"updated_rows": len(payload)})

            msg_slot.success("비가동 상세 저장 성공")
            time_mod.sleep(0.25)
            msg_slot.empty()

            st.session_state["nonop_lock_until_ts"] = 0.0
            st.session_state["nonop_is_saving"] = False

            st.session_state.nonop_last_poll_ts = 0.0
            _nonop_poll_changes(end_day, shift)

            _rerun_fragment_safe()

        except Exception as e:
            msg_slot.error(f"비가동 상세 저장 실패: {e}")
            time_mod.sleep(1.0)
            msg_slot.empty()
            st.session_state["nonop_lock_until_ts"] = 0.0

        finally:
            st.session_state["nonop_is_saving"] = False
            st.session_state["nonop_lock_until_ts"] = 0.0


@st.fragment(run_every=CHART_EVERY_SEC)
def frag_chart(end_day: str, shift: str):
    if is_any_modal_open():
        return

    _planned_load_for_chart_if_needed(end_day, shift)
    _nonop_reset_if_needed(end_day, shift)
    _nonop_load_window_once(end_day, shift)

    now_ts = float(time_mod.time())
    lock_until = float(st.session_state.get("nonop_lock_until_ts", 0.0) or 0.0)
    is_saving = bool(st.session_state.get("nonop_is_saving", False))
    if (not is_saving) and (now_ts >= lock_until):
        last_poll = float(st.session_state.get("nonop_last_poll_ts", 0.0) or 0.0)
        target_gap = max(1.0, float(min(CHART_EVERY_SEC, NONOP_EVERY_SEC)) * 0.8)
        if (now_ts - last_poll) >= target_gap:
            st.session_state.nonop_last_poll_ts = now_ts
            _nonop_poll_changes(end_day, shift)

    dt_now = now_kst()
    win_start, win_end = get_window(dt_now, shift)
    progressed_end = min(max(dt_now, win_start), win_end)

    fig, ax = plt.subplots(figsize=(10, 7))

    total_min = (win_end - win_start).total_seconds() / 60.0
    progressed_min = max(0.0, (progressed_end - win_start).total_seconds() / 60.0)

    for i, _ in enumerate(STATIONS):
        ax.bar(i, total_min, bottom=0.0, width=0.58, color=COLOR_IDLE, edgecolor="none")
        ax.bar(i, progressed_min, bottom=0.0, width=0.58, color=COLOR_RUN, edgecolor="none")

    planned_rows = st.session_state.get("planned_rows_cache", []) or []
    for row in planned_rows:
        ft = parse_hms(win_start, row.get("from_time"))
        tt = parse_hms(win_start, row.get("to_time"))
        if ft is None or tt is None:
            continue
        sdt = max(ft, win_start)
        edt = min(tt, progressed_end)
        if edt <= sdt:
            continue
        b = (sdt - win_start).total_seconds() / 60.0
        h = max((edt - sdt).total_seconds() / 60.0, MIN_SEG_MIN)
        for i in range(len(STATIONS)):
            ax.bar(i, h, bottom=b, width=0.58, color=COLOR_PLAN, edgecolor="none")

    nonop_all = st.session_state.get("nonop_chart_rows", []) or []
    for row in nonop_all:
        stn = str(row.get("station", "")).strip()
        if stn not in STATIONS:
            continue

        ft = parse_any_ts(win_start, row.get("from_ts", row.get("from_time")))
        tt = parse_any_ts(win_start, row.get("to_ts", row.get("to_time")))
        if ft is None or tt is None:
            continue
        if tt <= ft:
            tt = tt + timedelta(days=1)

        sdt = max(ft, win_start)
        edt = min(tt, progressed_end)
        if edt <= sdt:
            continue

        i = STATIONS.index(stn)
        b = (sdt - win_start).total_seconds() / 60.0
        h = max((edt - sdt).total_seconds() / 60.0, MIN_SEG_MIN)
        ax.bar(i, h, bottom=b, width=0.58, color=COLOR_STOP, edgecolor="none")

    yt = list(range(0, int(total_min) + 1, 60))
    yl = [(win_start + timedelta(minutes=m)).strftime("%H:%M") for m in yt]
    ax.set_ylim(0.0, total_min)
    ax.set_yticks(yt)
    ax.set_yticklabels(yl)
    ax.invert_yaxis()

    ax.set_xticks(list(range(len(STATIONS))))
    ax.set_xticklabels(STATIONS, fontsize=10)
    ax.xaxis.tick_top()
    ax.tick_params(axis="x", top=True, labeltop=True, bottom=False, labelbottom=False)

    ax.legend(
        handles=[
            Patch(facecolor=COLOR_RUN, label="가동"),
            Patch(facecolor=COLOR_PLAN, label="계획 정지"),
            Patch(facecolor=COLOR_STOP, label="비가동"),
            Patch(facecolor=COLOR_IDLE, label="미작업시간"),
        ],
        loc="lower center",
        bbox_to_anchor=(0.5, 1.06),
        ncol=4,
        frameon=False,
    )
    ax.grid(axis="y", alpha=0.25)
    plt.tight_layout(rect=[0, 0, 1, 0.90])
    st.pyplot(fig, clear_figure=True)


@st.fragment
def frag_worker_manual(end_day: str, shift: str):
    st.subheader("작업자 정보")

    def _set_flash(key: str, msg: str, level: str = "success", ttl_sec: float = 5.0):
        st.session_state[key] = {
            "msg": str(msg),
            "level": str(level),
            "expire_ts": float(time_mod.time()) + float(ttl_sec),
        }

    def _render_flash(key: str):
        data = st.session_state.get(key, None)
        if not isinstance(data, dict):
            return
        exp = float(data.get("expire_ts", 0.0) or 0.0)
        if time_mod.time() >= exp:
            st.session_state.pop(key, None)
            return

        level = str(data.get("level", "success") or "success").lower()
        msg = str(data.get("msg", "") or "")
        slot = st.empty()
        if level == "error":
            slot.error(msg)
        elif level == "warning":
            slot.warning(msg)
        else:
            slot.success(msg)

    _render_flash("worker_flash")

    if "worker_rows_cache" not in st.session_state:
        st.session_state.worker_rows_cache = None

    if st.session_state.worker_rows_cache is None:
        try:
            st.session_state.worker_rows_cache = get_worker_info(end_day, shift) or []
        except Exception as e:
            st.session_state.worker_rows_cache = []
            st.error(f"worker 최초 조회 실패: {e}")

    rows = st.session_state.worker_rows_cache or []
    wdf = pd.DataFrame(rows) if rows else pd.DataFrame()

    if wdf.empty:
        wdf = pd.DataFrame([{"end_day": end_day, "shift_type": shift, "worker_name": "", "order_number": ""}])

    for c in ["end_day", "shift_type", "worker_name", "order_number"]:
        if c not in wdf.columns:
            wdf[c] = ""

    wdf = wdf[["end_day", "shift_type", "worker_name", "order_number"]].copy()
    wdf["end_day"] = str(end_day)
    wdf["shift_type"] = str(shift)

    edited_wdf = st.data_editor(
        wdf,
        use_container_width=True,
        hide_index=True,
        num_rows="dynamic",
        key="worker_editor_main",
        column_config={
            "end_day": st.column_config.TextColumn("end_day", disabled=True),
            "shift_type": st.column_config.TextColumn("shift_type", disabled=True),
            "worker_name": st.column_config.TextColumn("worker_name"),
            "order_number": st.column_config.TextColumn("order_number"),
        },
    )

    if st.button("worker_info 저장", use_container_width=True, key="worker_save_btn"):
        try:
            edited_wdf = edited_wdf.copy()
            edited_wdf["end_day"] = str(end_day)
            edited_wdf["shift_type"] = str(shift)

            rows2 = edited_wdf.to_dict("records")
            payload = []
            for rr in rows2:
                payload.append(
                    {
                        "end_day": normalize_day_yyyymmdd(rr.get("end_day", "")) or end_day,
                        "shift_type": str(rr.get("shift_type", "")).strip() or shift,
                        "worker_name": str(rr.get("worker_name", "")).strip(),
                        "order_number": str(rr.get("order_number", "")).strip(),
                    }
                )

            payload = [x for x in payload if (x["worker_name"] or x["order_number"])]

            post_worker_info_sync(end_day, shift, payload)

            st.session_state.worker_rows_cache = get_worker_info(end_day, shift) or []
            _set_flash("worker_flash", "worker_info 저장 + 재조회 성공", level="success", ttl_sec=5.0)
            st.rerun()

        except Exception as e:
            _set_flash("worker_flash", f"worker_info 저장 실패: {e}", level="error", ttl_sec=5.0)
            st.rerun()


@st.fragment
def frag_master_manual(end_day: str, shift: str):
    st.subheader("mastersample test")

    def _set_flash(key: str, msg: str, level: str = "success", ttl_sec: float = 5.0):
        st.session_state[key] = {
            "msg": str(msg),
            "level": str(level),
            "expire_ts": float(time_mod.time()) + float(ttl_sec),
        }

    def _render_flash(key: str):
        data = st.session_state.get(key, None)
        if not isinstance(data, dict):
            return
        exp = float(data.get("expire_ts", 0.0) or 0.0)
        if time_mod.time() >= exp:
            st.session_state.pop(key, None)
            return

        level = str(data.get("level", "success") or "success").lower()
        msg = str(data.get("msg", "") or "")
        slot = st.empty()
        if level == "error":
            slot.error(msg)
        elif level == "warning":
            slot.warning(msg)
        else:
            slot.success(msg)

    _render_flash("master_flash")

    if "master_rows_cache" not in st.session_state:
        st.session_state.master_rows_cache = None

    c1, _ = st.columns([1, 2])
    with c1:
        if st.button("mastersample 새로고침", use_container_width=True, key="btn_master_refresh"):
            try:
                st.session_state.master_rows_cache = get_mastersample_test_info(end_day, shift) or []
                _set_flash("master_flash", "mastersample 재조회 성공", level="success", ttl_sec=5.0)
                st.rerun()
            except Exception as e:
                _set_flash("master_flash", f"mastersample 재조회 실패: {e}", level="error", ttl_sec=5.0)
                st.rerun()

    if st.session_state.master_rows_cache is None:
        try:
            st.session_state.master_rows_cache = get_mastersample_test_info(end_day, shift) or []
        except Exception as e:
            st.session_state.master_rows_cache = []
            st.error(f"mastersample 최초 조회 실패: {e}")

    rows = st.session_state.master_rows_cache or []
    mdf = pd.DataFrame(rows) if rows else pd.DataFrame()
    if not mdf.empty:
        mdf = mdf.head(2)
    st.dataframe(mdf, use_container_width=True, hide_index=True)


# =========================================================
# Header buttons
# =========================================================
def render_header_buttons(end_day: str, shift: str):
    b1, b2, b3, b4 = st.columns(4)

    def _open_modal(which: str):
        st.session_state["active_modal"] = which

        st.session_state["modal_email"] = (which == "email")
        st.session_state["modal_barcode"] = (which == "barcode")
        st.session_state["modal_planned"] = (which == "planned")

        st.session_state["modal_email_need_load"] = (which == "email")
        st.session_state["modal_barcode_need_load"] = (which == "barcode")
        st.session_state["modal_planned_need_load"] = (which == "planned")

        st.session_state["nonop_lock_until_ts"] = 0.0
        st.session_state["nonop_is_saving"] = False
        st.rerun()

    if b1.button("새로고침", use_container_width=True, key="btn_header_refresh"):
        try:
            _planned_refresh_for_chart(end_day, shift)

            _nonop_reset_if_needed(end_day, shift)
            _nonop_load_window_once(end_day, shift)

            st.session_state.nonop_last_poll_ts = time_mod.time()
            _nonop_poll_changes(end_day, shift)

            latest = get_sections_latest(end_day, shift, timeout=float(SECTIONS_LATEST_TIMEOUT))
            tok = (latest or {}).get("tokens", {}).get("nonop_detail", "") if isinstance(latest, dict) else ""
            new_cursor = _cursor_from_token(tok)
            _nonop_apply_changes_if_needed(end_day, shift, new_cursor)
        except Exception:
            pass
        st.rerun()

    if b2.button("email_list", use_container_width=True, key="btn_header_email"):
        _open_modal("email")

    if b3.button("barcode", use_container_width=True, key="btn_header_barcode"):
        _open_modal("barcode")

    if b4.button("계획정지시간", use_container_width=True, key="btn_header_planned"):
        _open_modal("planned")


# =========================================================
# Modals (st.dialog)
# =========================================================
def render_modals(end_day: str, shift: str):
    active = str(st.session_state.get("active_modal", "") or "").strip().lower()

    if not active:
        if st.session_state.get("modal_email", False):
            active = "email"
            st.session_state["active_modal"] = "email"
        elif st.session_state.get("modal_barcode", False):
            active = "barcode"
            st.session_state["active_modal"] = "barcode"
        elif st.session_state.get("modal_planned", False):
            active = "planned"
            st.session_state["active_modal"] = "planned"

    if active == "email":

        @st.dialog("Email List 수정", width="large")
        def email_modal():
            def _reset_email_keys():
                for i in range(10):
                    st.session_state.pop(f"em_{i}", None)

            def _load_pad_10():
                try:
                    rows = get_email_list(end_day, shift) or []
                except Exception as e:
                    rows = []
                    st.session_state["email_flash"] = ("error", f"email_list 조회 실패: {e}")

                emails = []
                for r in rows:
                    v = "" if r is None else str((r or {}).get("email", "")).strip()
                    if v:
                        emails.append(v)

                emails = emails[:10]
                while len(emails) < 10:
                    emails.append("")

                for i in range(10):
                    st.session_state.setdefault(f"em_{i}", emails[i])

            if st.session_state.get("modal_email_need_load", True) or ("em_0" not in st.session_state):
                _reset_email_keys()
                _load_pad_10()
                st.session_state.modal_email_need_load = False

            flash = st.session_state.pop("email_flash", None)
            if isinstance(flash, tuple) and len(flash) == 2:
                typ, txt = flash
                (st.success if typ == "success" else st.error)(txt)

            st.markdown("### email_list (자유 입력)")
            st.caption("예: abc@company.com  / 최대 10개")

            h = st.columns([0.55, 4.0])
            h[0].markdown("**-**")
            h[1].markdown("**email**")

            for i in range(10):
                c = st.columns([0.55, 4.0])
                if c[0].button("－", key=f"em_minus_{i}"):
                    st.session_state[f"em_{i}"] = ""
                    st.rerun()

                c[1].text_input(
                    label=f"email_{i}",
                    key=f"em_{i}",
                    label_visibility="collapsed",
                    placeholder="예) name@aptiv.com",
                )

            st.divider()
            pw = st.text_input("관리자 비밀번호(X-ADMIN-PASS)", type="password", key="email_pw")

            c_save, c_close = st.columns(2)

            if c_save.button("저장", use_container_width=True, key="email_save_btn"):
                try:
                    pw2 = str(pw or "").strip()
                    if not pw2:
                        st.session_state["email_flash"] = ("error", "관리자 비밀번호가 필요합니다.")
                        st.rerun()

                    payload = []
                    for i in range(10):
                        v = str(st.session_state.get(f"em_{i}", "") or "").strip()
                        if v:
                            payload.append({"email": v})

                    seen = set()
                    payload2 = []
                    for x in payload:
                        e = x["email"].strip().lower()
                        if e in seen:
                            continue
                        seen.add(e)
                        payload2.append({"email": e})

                    post_email_list_sync(
                        end_day,
                        shift,
                        payload2,
                        password=pw2,
                        mode="replace",
                        min_keep_ratio=0.0,
                    )

                    st.session_state["email_flash"] = ("success", "email_list 저장(Replace) 성공")
                    st.session_state["modal_email_need_load"] = True
                    st.session_state["nonop_lock_until_ts"] = 0.0
                    st.session_state["nonop_is_saving"] = False
                    st.rerun()

                except Exception as e:
                    st.session_state["email_flash"] = ("error", f"email_list 저장 실패: {e}")
                    st.rerun()

            if c_close.button("닫기(Email)", use_container_width=True, key="email_close_btn"):
                st.session_state["active_modal"] = ""
                st.session_state["modal_email"] = False
                st.session_state["modal_email_need_load"] = True
                st.session_state["nonop_lock_until_ts"] = 0.0
                st.session_state["nonop_is_saving"] = False
                st.rerun()

        email_modal()

    elif active == "barcode":

        @st.dialog("Barcode(remark_info) 수정", width="large")
        def barcode_modal():
            def _reset_barcode_keys():
                for i in range(10):
                    st.session_state.pop(f"bc_info_{i}", None)
                    st.session_state.pop(f"bc_pn_{i}", None)
                    st.session_state.pop(f"bc_rem_{i}", None)

            def _load_pad_10():
                try:
                    rows = get_remark_info(end_day, shift) or []
                except Exception as e:
                    rows = []
                    st.session_state["barcode_flash"] = ("error", f"remark_info 조회 실패: {e}")

                items = []
                for r in (rows or [])[:10]:
                    rr = r or {}
                    items.append(
                        {
                            "barcode_information": "" if rr.get("barcode_information") is None else str(rr.get("barcode_information") or ""),
                            "pn": "" if rr.get("pn") is None else str(rr.get("pn") or ""),
                            "remark": "" if rr.get("remark") is None else str(rr.get("remark") or ""),
                        }
                    )

                while len(items) < 10:
                    items.append({"barcode_information": "", "pn": "", "remark": ""})

                for i in range(10):
                    st.session_state.setdefault(f"bc_info_{i}", items[i]["barcode_information"])
                    st.session_state.setdefault(f"bc_pn_{i}", items[i]["pn"])
                    st.session_state.setdefault(f"bc_rem_{i}", items[i]["remark"])

            if st.session_state.get("modal_barcode_need_load", True) or ("bc_info_0" not in st.session_state):
                _reset_barcode_keys()
                _load_pad_10()
                st.session_state.modal_barcode_need_load = False

            flash = st.session_state.pop("barcode_flash", None)
            if isinstance(flash, tuple) and len(flash) == 2:
                typ, txt = flash
                (st.success if typ == "success" else st.error)(txt)

            st.info("해당 barcode는 전체 barcode의 18번째 문자를 뜻함")

            h = st.columns([0.55, 1.6, 1.6, 2.6])
            h[0].markdown("**-**")
            h[1].markdown("**barcode_information (PK)**")
            h[2].markdown("**pn**")
            h[3].markdown("**remark**")

            for i in range(10):
                c = st.columns([0.55, 1.6, 1.6, 2.6])

                if c[0].button("－", key=f"bc_minus_{i}"):
                    st.session_state[f"bc_info_{i}"] = ""
                    st.session_state[f"bc_pn_{i}"] = ""
                    st.session_state[f"bc_rem_{i}"] = ""
                    st.rerun()

                c[1].text_input(label=f"barcode_information_{i}", key=f"bc_info_{i}", label_visibility="collapsed", placeholder="예) 18번째 문자 값")
                c[2].text_input(label=f"pn_{i}", key=f"bc_pn_{i}", label_visibility="collapsed", placeholder="예) 123-ABC")
                c[3].text_input(label=f"remark_{i}", key=f"bc_rem_{i}", label_visibility="collapsed", placeholder="비고")

            st.divider()
            pw = st.text_input("관리자 비밀번호(X-ADMIN-PASS)", type="password", key="barcode_pw")

            c_save, c_close = st.columns(2)

            if c_save.button("저장", use_container_width=True, key="barcode_save_btn"):
                try:
                    pw2 = str(pw or "").strip()
                    if not pw2:
                        st.session_state["barcode_flash"] = ("error", "관리자 비밀번호가 필요합니다.")
                        st.rerun()

                    payload = []
                    seen_pk = set()
                    for i in range(10):
                        bi = str(st.session_state.get(f"bc_info_{i}", "") or "").strip()
                        pn = str(st.session_state.get(f"bc_pn_{i}", "") or "").strip()
                        rm = str(st.session_state.get(f"bc_rem_{i}", "") or "").strip()
                        if not bi:
                            continue
                        if bi in seen_pk:
                            continue
                        seen_pk.add(bi)
                        payload.append({"barcode_information": bi, "pn": pn, "remark": rm})

                    post_remark_info_sync(end_day, shift, payload, password=pw2, mode="replace", min_keep_ratio=0.0)

                    st.session_state["barcode_flash"] = ("success", "remark_info 저장(Replace) 성공")
                    st.session_state["modal_barcode_need_load"] = True
                    st.session_state["nonop_lock_until_ts"] = 0.0
                    st.session_state["nonop_is_saving"] = False
                    st.rerun()
                except Exception as e:
                    st.session_state["barcode_flash"] = ("error", f"remark_info 저장 실패: {e}")
                    st.rerun()

            if c_close.button("닫기(Barcode)", use_container_width=True, key="barcode_close_btn"):
                st.session_state["active_modal"] = ""
                st.session_state["modal_barcode"] = False
                st.session_state["modal_barcode_need_load"] = True
                st.session_state["nonop_lock_until_ts"] = 0.0
                st.session_state["nonop_is_saving"] = False
                st.rerun()

        barcode_modal()

    elif active == "planned":

        @st.dialog("계획 정지 시간(planned_time) 수정", width="large")
        def planned_modal():
            def _to_hhmm(v: Any) -> str:
                s = str(v or "").strip()
                if not s:
                    return ""
                if len(s) >= 5 and s[2] == ":":
                    return s[:5]
                return ""

            def _to_hhmmss(v: Any) -> str:
                s = str(v or "").strip()
                if not s:
                    return ""
                if len(s) == 5 and s[2] == ":":
                    return s + ":00"
                if len(s) >= 8 and s[2] == ":" and s[5] == ":":
                    return s[:8]
                return s

            def _split_cross_midnight(ft: str, tt: str, reason: str) -> List[Dict[str, Any]]:
                if not ft or not tt:
                    return []
                if tt < ft:
                    return [
                        {"end_day": end_day, "from_time": ft, "to_time": "23:59:59", "reason": reason},
                        {"end_day": end_day, "from_time": "00:00:00", "to_time": tt, "reason": reason},
                    ]
                return [{"end_day": end_day, "from_time": ft, "to_time": tt, "reason": reason}]

            def _reset_planned_keys():
                for i in range(10):
                    st.session_state.pop(f"pl_ft_{i}", None)
                    st.session_state.pop(f"pl_tt_{i}", None)
                    st.session_state.pop(f"pl_rs_{i}", None)

            def _load_pad_10():
                try:
                    rows = get_planned_time_today(end_day, shift) or []
                except Exception as e:
                    rows = []
                    st.session_state["planned_flash"] = ("error", f"planned 조회 실패: {e}")

                try:
                    rows = sorted(rows, key=lambda r: str(r.get("from_time") or ""))
                except Exception:
                    pass

                padded: List[Dict[str, Any]] = []
                for r in rows[:10]:
                    padded.append(
                        {
                            "from_time": _to_hhmm(r.get("from_time", "")),
                            "to_time": _to_hhmm(r.get("to_time", "")),
                            "reason": "" if r.get("reason") is None else str(r.get("reason") or ""),
                        }
                    )
                while len(padded) < 10:
                    padded.append({"from_time": "", "to_time": "", "reason": ""})

                for i in range(10):
                    st.session_state.setdefault(f"pl_ft_{i}", padded[i]["from_time"])
                    st.session_state.setdefault(f"pl_tt_{i}", padded[i]["to_time"])
                    st.session_state.setdefault(f"pl_rs_{i}", padded[i]["reason"])

            if st.session_state.get("modal_planned_need_load", True) or ("pl_ft_0" not in st.session_state):
                _reset_planned_keys()
                _load_pad_10()
                st.session_state.modal_planned_need_load = False

            flash = st.session_state.pop("planned_flash", None)
            if isinstance(flash, tuple) and len(flash) == 2:
                typ, txt = flash
                (st.success if typ == "success" else st.error)(txt)

            time_opts = [f"{h:02d}:{m:02d}" for h in range(24) for m in range(0, 60, 5)]
            time_opts2 = [""] + time_opts

            h = st.columns([0.55, 1.1, 1.1, 3.2])
            h[0].markdown("**-**")
            h[1].markdown("**from_time**")
            h[2].markdown("**to_time**")
            h[3].markdown("**reason (자유 텍스트)**")

            for i in range(10):
                c = st.columns([0.55, 1.1, 1.1, 3.2])
                if c[0].button("－", key=f"pl_minus_{i}"):
                    st.session_state[f"pl_ft_{i}"] = ""
                    st.session_state[f"pl_tt_{i}"] = ""
                    st.session_state[f"pl_rs_{i}"] = ""
                    st.rerun()
                c[1].selectbox(label=f"from_{i}", options=time_opts2, key=f"pl_ft_{i}", label_visibility="collapsed")
                c[2].selectbox(label=f"to_{i}", options=time_opts2, key=f"pl_tt_{i}", label_visibility="collapsed")
                c[3].text_input(label=f"reason_{i}", key=f"pl_rs_{i}", label_visibility="collapsed", placeholder="예) 점심 / 저녁 / 설비 점검 등")

            st.divider()
            c_save, c_close = st.columns(2)

            if c_save.button("저장", use_container_width=True, key="planned_save_btn"):
                try:
                    payload: List[Dict[str, Any]] = []
                    for i in range(10):
                        ft_ui = str(st.session_state.get(f"pl_ft_{i}", "") or "").strip()
                        tt_ui = str(st.session_state.get(f"pl_tt_{i}", "") or "").strip()
                        reason = str(st.session_state.get(f"pl_rs_{i}", "") or "").strip()

                        ft = _to_hhmmss(ft_ui)
                        tt = _to_hhmmss(tt_ui)
                        if not ft or not tt:
                            continue
                        payload.extend(_split_cross_midnight(ft, tt, reason))

                    post_planned_time_sync(end_day, shift, payload)
                    _planned_refresh_for_chart(end_day, shift)

                    st.session_state["planned_flash"] = ("success", "planned_time 저장 성공")
                    st.session_state["modal_planned_need_load"] = True
                    st.session_state["nonop_lock_until_ts"] = 0.0
                    st.session_state["nonop_is_saving"] = False
                    st.rerun()
                except Exception as e:
                    st.session_state["planned_flash"] = ("error", f"planned_time 저장 실패: {e}")
                    st.rerun()

            if c_close.button("닫기(Planned)", use_container_width=True, key="planned_close_btn"):
                st.session_state["active_modal"] = ""
                st.session_state["modal_planned"] = False
                st.session_state["modal_planned_need_load"] = True
                st.session_state["nonop_lock_until_ts"] = 0.0
                st.session_state["nonop_is_saving"] = False
                st.rerun()

        planned_modal()


# =========================================================
# App start
# =========================================================
st.set_page_config(page_title="실시간 Dash board", layout="wide")
inject_no_dim_fade_keep_loading()
inject_no_dim_fade_keep_loading()

if "seen_alarm_pks" not in st.session_state:
    st.session_state.seen_alarm_pks = set()
if "section_tokens" not in st.session_state:
    st.session_state.section_tokens = {}
if "alarm_token_effective" not in st.session_state:
    st.session_state.alarm_token_effective = ""
if "pending_alarm_pk" not in st.session_state:
    st.session_state.pending_alarm_pk = ""
if "pending_alarm_msg" not in st.session_state:
    st.session_state.pending_alarm_msg = ""

if "nonop_lock_until_ts" not in st.session_state:
    st.session_state.nonop_lock_until_ts = 0.0
if "nonop_editor_snap" not in st.session_state:
    st.session_state.nonop_editor_snap = ""
if "nonop_is_saving" not in st.session_state:
    st.session_state.nonop_is_saving = False
if "active_modal" not in st.session_state:
    st.session_state["active_modal"] = ""

if "nonop_last_poll_ts" not in st.session_state:
    st.session_state["nonop_last_poll_ts"] = 0.0

for mk in ("modal_email", "modal_barcode", "modal_planned"):
    if mk not in st.session_state:
        st.session_state[mk] = False
for mk in ("modal_email_need_load", "modal_barcode_need_load", "modal_planned_need_load"):
    if mk not in st.session_state:
        st.session_state[mk] = True

_handle_close_modal_param()

dt_now = now_kst()
shift = detect_shift(dt_now)
end_day = dt_now.strftime("%Y%m%d")
weekday_kr = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"][dt_now.weekday()]
shift_kr = "주간" if shift == "day" else "야간"

# ✅ 01 페이지도 SSE 알람 구독(현재 시각 기준)
_mount_alarm_sse(end_day, shift)

_nonop_reset_if_needed(end_day, shift)
_planned_reset_if_needed(end_day, shift)

if st.session_state.get("last_shift") != shift:
    st.session_state["last_shift"] = shift
    _nonop_reset_if_needed(end_day, shift)
    _planned_reset_if_needed(end_day, shift)
    st.session_state.worker_rows_cache = None
    st.session_state.master_rows_cache = None

try:
    q = st.query_params
    ack_pk = q.get("ack_alarm_pk", "")
    if ack_pk:
        st.session_state.seen_alarm_pks.add(str(ack_pk))
        try:
            st.query_params.pop("ack_alarm_pk", None)
        except Exception:
            pass
        _clear_ack_param_in_browser()
except Exception:
    pass

h1, h2 = st.columns([3, 2])
with h1:
    st.markdown(f"## {dt_now:%Y-%m-%d} {weekday_kr} {shift_kr} 생산 현황")
with h2:
    render_header_buttons(end_day, shift)

st.divider()

frag_sync_data(end_day, shift)
frag_alarm_once()

render_modals(end_day, shift)
_inject_modal_dom_watchdog()

l, r = st.columns([1.25, 1.0])
with l:
    st.subheader("실시간 생산 진행 현황")
    frag_chart(end_day, shift)
with r:
    frag_nonop_table(end_day, shift)

c1, c2 = st.columns(2)
with c1:
    frag_worker_manual(end_day, shift)
with c2:
    frag_master_manual(end_day, shift)

with st.expander("PERF LOG (최근)", expanded=False):
    logs = st.session_state.get("perf_logs", []) or []
    if logs:
        st.dataframe(pd.DataFrame(logs).tail(40), use_container_width=True, hide_index=True)
    else:
        st.caption("로그 없음")