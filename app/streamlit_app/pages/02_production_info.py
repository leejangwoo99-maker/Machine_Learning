# app/streamlit_app/pages/02_production_info.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, date
from zoneinfo import ZoneInfo
from typing import Any, Dict, Callable, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

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
# ENV
# -----------------------------
def _env_int(name: str, default: int, min_v: int, max_v: int) -> int:
    raw = os.getenv(name, "")
    if raw is None:
        return default
    s = str(raw).strip()
    if not s:
        return default
    try:
        v = int(float(s))
    except Exception:
        return default
    if v < min_v:
        return min_v
    if v > max_v:
        return max_v
    return v


P02_FETCH_WORKERS = _env_int("P02_FETCH_WORKERS", default=8, min_v=2, max_v=32)


# -----------------------------
# UI hardening: dim/fade 제거
# -----------------------------
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


# ✅ snapshot(print) 모드 감지: ?snap=1
def is_snapshot_mode() -> bool:
    qp = st.query_params
    v = qp.get("snap")
    return str(v).strip().lower() in ("1", "true", "yes", "y")


SNAP_MODE = is_snapshot_mode()


# -----------------------------
# dataframe safe helpers
# -----------------------------
def safe_show_df(
    df_or_obj: Any,
    *,
    raw_df: Optional[pd.DataFrame] = None,
    use_container_width: bool = True,
    hide_index: bool = False,
    height: Optional[int] = None,
):
    try:
        kwargs = {"use_container_width": use_container_width}
        if height is not None:
            kwargs["height"] = height
        if hide_index:
            kwargs["hide_index"] = hide_index
        st.dataframe(df_or_obj, **kwargs)
        return
    except Exception:
        pass

    base_df = raw_df
    if base_df is None and isinstance(df_or_obj, pd.DataFrame):
        base_df = df_or_obj

    if base_df is not None:
        try:
            st.table(base_df)
            return
        except Exception:
            pass

        try:
            html = base_df.to_html(index=not hide_index)
            st.markdown(html, unsafe_allow_html=True)
            return
        except Exception:
            pass

    st.warning("표 렌더링 중 오류가 발생했습니다.")


# -----------------------------
# day/shift helpers
# -----------------------------
def _now_prod_day_shift() -> Tuple[str, str]:
    now = datetime.now(tz=KST)
    day_start = now.replace(hour=8, minute=30, second=0, microsecond=0)
    night_start = now.replace(hour=20, minute=30, second=0, microsecond=0)

    if day_start <= now < night_start:
        return now.strftime("%Y%m%d"), "day"
    if now >= night_start:
        return now.strftime("%Y%m%d"), "night"

    y = (now - timedelta(days=1)).strftime("%Y%m%d")
    return y, "night"


def _norm_day(v: Any) -> str:
    s = str(v or "").strip()
    digits = "".join(ch for ch in s if ch.isdigit())
    return digits[:8] if len(digits) >= 8 else ""


def _norm_shift(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in ("day", "night") else "day"


def _parse_day_to_date(v: Any) -> Optional[date]:
    s = str(v or "").strip()
    if not s:
        return None
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        try:
            y = int(digits[:4])
            m = int(digits[4:6])
            d = int(digits[6:8])
            return date(y, m, d)
        except Exception:
            return None
    return None


def _parse_time_hms(v: Any) -> Optional[Tuple[int, int, int]]:
    """
    허용:
      - "HH:MM"
      - "HH:MM:SS"
      - "HH:MM:SS.sss"
      - ISO string (시간만 추출)
    """
    s = str(v or "").strip()
    if not s:
        return None

    if "T" in s:
        try:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=KST)
            dt = dt.astimezone(KST)
            return (dt.hour, dt.minute, dt.second)
        except Exception:
            return None

    if ":" not in s:
        return None

    parts = s.split(":")
    try:
        hh = int(parts[0])
        mm = int(parts[1]) if len(parts) > 1 else 0
        ss = 0
        if len(parts) > 2:
            ss = int(str(parts[2]).split(".")[0] or "0")
        return (hh, mm, ss)
    except Exception:
        return None


def alarm_scope_from_row(row: Dict[str, Any]) -> Tuple[str, str]:
    """
    return: (prod_day_yyyymmdd, shift_type)
    shift rule:
      - day   : 08:30 ~ 20:30
      - night : otherwise
        * night & time < 08:30 => prod_day = (end_day - 1)
        * night & time >= 20:30 => prod_day = end_day
    """
    d = _parse_day_to_date(row.get("end_day") or row.get("prod_day") or "")
    if d is None:
        dd = _norm_day(row.get("end_day") or row.get("prod_day") or "")
        return (dd, "day")

    t = _parse_time_hms(row.get("end_time") or row.get("time") or "")
    if t is None:
        return (d.strftime("%Y%m%d"), "day")

    hh, mm, ss = t
    is_day = (hh > 8 and hh < 20) or (hh == 8 and mm >= 30) or (hh == 20 and mm < 30)
    if is_day:
        return (d.strftime("%Y%m%d"), "day")

    if (hh < 8) or (hh == 8 and mm < 30):
        pd = (d - timedelta(days=1)).strftime("%Y%m%d")
        return (pd, "night")

    return (d.strftime("%Y%m%d"), "night")


# -----------------------------
# dataframe helpers
# -----------------------------
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
    except TypeError:
        try:
            return fn(*args)
        except Exception:
            return None
    except Exception:
        return None


def _is_empty_df(df: pd.DataFrame) -> bool:
    return (df is None) or (not isinstance(df, pd.DataFrame)) or df.empty


def _get_api_fn(name: str):
    fn = getattr(api, name, None)
    return fn if callable(fn) else None


# -----------------------------
# SSE helpers
# -----------------------------
def _api_base_url() -> str:
    for attr in ("API_BASE_URL", "BASE_URL", "API_URL", "API"):
        v = getattr(api, attr, None)
        if isinstance(v, str) and v.strip().startswith("http"):
            return v.strip().rstrip("/")
    return "http://127.0.0.1:8000"


def _mount_alarm_sse(now_day: str, now_shift: str, view_day: str, view_shift: str):
    """
    ✅ (핵심) 서버 구독은 now_day/now_shift로 유지하되,
    클라이언트에서 row(end_day,end_time) 기반으로 prod_day/shift 재판정 후
    "현재 조회(view_day/view_shift)"와 일치하는 알람만 표시.
    """
    base = _api_base_url().rstrip("/")

    admin_pass = (getattr(api, "ADMIN_PASS", "") or "").strip() or (os.getenv("ADMIN_PASS", "") or "").strip()
    token_qs = f"&token={admin_pass}" if admin_pass else ""

    sse_url = f"{base}/events/stream?end_day={now_day}&shift_type={now_shift}&sections=alarm{token_qs}"

    view_day_norm = _norm_day(view_day) or _norm_day(now_day) or str(now_day)
    view_shift_norm = _norm_shift(view_shift)

    components.html(
        f"""
        <script>
        (function() {{
          const W = window.parent;
          const url = {json.dumps(sse_url)};
          const ALLOWED = new Set(["권고","긴급","교체"]);
          const ACK_KEY = "acked_alarm_pks";

          const TARGET_DAY   = {json.dumps(view_day_norm)};
          const TARGET_SHIFT = {json.dumps(view_shift_norm)};

          const OVERLAY_ID_PREFIX = "alarmOverlay_";

          function log(...args) {{
            try {{ console.log("[ALARM_SSE_P02]", ...args); }} catch(e) {{}}
          }}

          function safeText(s) {{
            return String(s||"")
              .replaceAll("&","&amp;")
              .replaceAll("<","&lt;")
              .replaceAll(">","&gt;")
              .replaceAll('"',"&quot;")
              .replaceAll("'","&#39;");
          }}

          function normDay(v) {{
            const s = String(v||"").trim();
            const digits = s.replace(/\\D/g,"");
            return (digits.length >= 8) ? digits.slice(0,8) : "";
          }}

          function parseHms(v) {{
            const s = String(v||"").trim();
            if(!s) return null;
            if(s.includes("T")) {{
              try {{
                const dt = new Date(s);
                if (isNaN(dt.getTime())) return null;
                return {{hh: dt.getHours(), mm: dt.getMinutes(), ss: dt.getSeconds()}};
              }} catch(e) {{
                return null;
              }}
            }}
            if(!s.includes(":")) return null;
            const p = s.split(":");
            const hh = parseInt(p[0]||"0",10);
            const mm = parseInt(p[1]||"0",10);
            let ss = 0;
            if (p.length >= 3) {{
              ss = parseInt(String(p[2]||"0").split(".")[0],10);
            }}
            if (Number.isNaN(hh) || Number.isNaN(mm) || Number.isNaN(ss)) return null;
            return {{hh,mm,ss}};
          }}

          function yyyymmddToDate(yyyymmdd) {{
            if (!yyyymmdd || yyyymmdd.length !== 8) return null;
            const y = parseInt(yyyymmdd.slice(0,4),10);
            const m = parseInt(yyyymmdd.slice(4,6),10) - 1;
            const d = parseInt(yyyymmdd.slice(6,8),10);
            const dt = new Date(y, m, d);
            return isNaN(dt.getTime()) ? null : dt;
          }}

          function dateToYyyymmdd(dt) {{
            const y = dt.getFullYear();
            const m = String(dt.getMonth()+1).padStart(2,"0");
            const d = String(dt.getDate()).padStart(2,"0");
            return String(y)+m+d;
          }}

          function computeScopeFromRow(row) {{
            const endDay = normDay(row.end_day || row.prod_day || "");
            const t = parseHms(row.end_time || row.time || "");
            if (!endDay || !t) {{
              return {{prod_day: endDay || "", shift: "day"}};
            }}

            const hh = t.hh, mm = t.mm;
            const isDay = (hh > 8 && hh < 20) || (hh === 8 && mm >= 30) || (hh === 20 && mm < 30);
            if (isDay) {{
              return {{prod_day: endDay, shift: "day"}};
            }}

            if (hh < 8 || (hh === 8 && mm < 30)) {{
              const dt = yyyymmddToDate(endDay);
              if (!dt) return {{prod_day: endDay, shift: "night"}};
              dt.setDate(dt.getDate()-1);
              return {{prod_day: dateToYyyymmdd(dt), shift: "night"}};
            }}

            return {{prod_day: endDay, shift: "night"}};
          }}

          function isRowInTargetScope(row) {{
            try {{
              const sc = computeScopeFromRow(row);
              if (!sc.prod_day) return false;
              if (sc.prod_day !== TARGET_DAY) return false;
              if (sc.shift !== TARGET_SHIFT) return false;
              return true;
            }} catch(e) {{
              return false;
            }}
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

            if (t === "권고") return stn + ", " + sp + " 교체 권고 드립니다.";
            if (t === "긴급") return stn + ", " + sp + " 교체 긴급합니다.";
            if (t === "교체") return stn + ", " + sp + " 교체 타이밍이 지났습니다.";
            return stn + ", " + sp + " 알람(" + t + ")";
          }}

          function showCenteredNonBlockingModal(pk, message) {{
            const acked = getAcked();
            if (acked.includes(pk)) {{
              log("skip acked pk=", pk);
              return;
            }}

            const doc = W.document;
            if (doc.getElementById(OVERLAY_ID_PREFIX + pk)) return;

            removeExistingOverlays(doc);

            const overlay = doc.createElement("div");
            overlay.id = OVERLAY_ID_PREFIX + pk;
            overlay.style.cssText = `
              position: fixed;
              z-index: 2147483647;
              left: 0; top: 0; width: 100%; height: 100%;
              display: flex; align-items: center; justify-content: center;
              background: rgba(0,0,0,0.0);
              pointer-events: none;
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
              pointer-events: auto;
            `;

            const closeId = "alarmCloseX_" + pk;
            const ackId = "alarmAckBtn_" + pk;

            box.innerHTML = `
              <div style="display:flex; align-items:center; justify-content:space-between;">
                <div style="font-size:22px; font-weight:800;">⚠️ 알람 발생</div>
                <button id="` + closeId + `" style="border:none;background:transparent;font-size:22px;cursor:pointer;">✕</button>
              </div>

              <div style="margin-top:14px; padding:12px 14px; border-radius:10px;
                          background:#fff9db; color:#5c4a00; font-size:16px;">
                ` + safeText(message) + `
              </div>

              <div style="margin-top:16px; display:flex; gap:12px;">
                <button id="` + ackId + `" style="flex:1; padding:10px 0; border-radius:10px;
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

            const xBtn = box.querySelector("#" + closeId);
            const aBtn = box.querySelector("#" + ackId);

            if (xBtn) xBtn.addEventListener("click", function(ev) {{
              try {{ ev.preventDefault(); ev.stopPropagation(); }} catch(e) {{}}
              closeOverlay(doc, pk);
            }}, true);

            if (aBtn) aBtn.addEventListener("click", function(ev) {{
              try {{ ev.preventDefault(); ev.stopPropagation(); }} catch(e) {{}}
              addAck(pk);
              closeOverlay(doc, pk);
            }}, true);
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

            if (!isRowInTargetScope(row)) {{
              const sc = computeScopeFromRow(row);
              log("skip by scope mismatch", {{target: {{day:TARGET_DAY, shift:TARGET_SHIFT}}, rowScope: sc, row}});
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

          if (!W.__alarmSSEP02) {{
            W.__alarmSSEP02 = {{ url: null, es: null }};
          }}

          function start() {{
            if (W.__alarmSSEP02.es && W.__alarmSSEP02.url === url) {{
              return;
            }}

            if (W.__alarmSSEP02.es) {{
              try {{ W.__alarmSSEP02.es.close(); }} catch(e) {{}}
              W.__alarmSSEP02.es = null;
            }}

            W.__alarmSSEP02.url = url;

            log("connect:", url, "target=", TARGET_DAY, TARGET_SHIFT);
            const es = new EventSource(url);
            W.__alarmSSEP02.es = es;

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
              }} catch(e) {{
                log("alarm parse err", e);
              }}
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


# -----------------------------
# Parallel fetch
# -----------------------------
def _parallel_fetch_reports(active_day: str, active_shift: str) -> Dict[str, pd.DataFrame]:
    fn_planned = _get_api_fn("get_report_i_planned_stop_time")
    fn_non = _get_api_fn("get_report_i_non_time")
    fn_amt = _get_api_fn("get_report_a_station_final_amount")
    fn_pct = (
        _get_api_fn("get_report_b_station_daily_percentage")
        or _get_api_fn("get_report_b_station_percentage")
        or _get_api_fn("get_report_b_station_daily")
        or _get_api_fn("get_report_b_station_percentage_daily")
        or _get_api_fn("get_report_b_station_percentage")
    )

    tasks: list[tuple[str, Optional[Callable], str]] = [
        ("oee_total", get_report_k_oee_total, "drop_updated_at"),
        ("oee_line", get_report_k_oee_line, "drop_updated_at"),
        ("oee_station", get_report_k_oee_station, "drop_updated_at"),
        ("planned", fn_planned, "planned"),
        ("nonop", fn_non, "nonop"),
        ("amt", fn_amt, "drop_updated_at"),
        ("pct", fn_pct, "drop_updated_at"),
        ("fct_1time", get_report_c_fct_step_1time, "drop_updated_at"),
        ("fct_2time", get_report_c_fct_step_2time, "drop_updated_at"),
        ("fct_3over", get_report_c_fct_step_3over, "drop_updated_at"),
        ("vis_1time", get_report_d_vision_step_1time, "drop_updated_at"),
        ("vis_2time", get_report_d_vision_step_2time, "drop_updated_at"),
        ("vis_3over", get_report_d_vision_step_3over, "drop_updated_at"),
    ]

    out: Dict[str, pd.DataFrame] = {}

    def _run_one(key: str, fn: Optional[Callable], mode: str) -> tuple[str, pd.DataFrame]:
        resp = _safe_call(fn, active_day, active_shift) if fn else None
        df = _df_from_resp(resp)

        if mode == "drop_updated_at":
            df = _drop_updated_at(df)
        elif mode == "planned":
            df = _drop_cols(df, ["total_planned_time", "updated_at"])
        elif mode == "nonop":
            df = _drop_cols(df, ["vision1_non_time", "vision2_non_time", "total_vision_non_time", "updated_at"])
        else:
            df = _drop_updated_at(df)

        return key, df

    workers = max(2, min(int(P02_FETCH_WORKERS), len(tasks)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = [ex.submit(_run_one, key, fn, mode) for (key, fn, mode) in tasks]
        for fut in as_completed(futs):
            try:
                k, df = fut.result()
                out[k] = df
            except Exception:
                pass

    for key, _, _ in tasks:
        if key not in out:
            out[key] = pd.DataFrame()

    return out


# =========================================================
# App start
# =========================================================
st.set_page_config(page_title="생산 정보", layout="wide")
inject_no_dim_fade_keep_loading()
inject_no_dim_fade_keep_loading()

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
st.caption(f"현재 조회: prod_day={active_day} / shift_type={active_shift} | 모드: 수동(버튼 기반){' | SNAP=ON' if SNAP_MODE else ''}")

st.divider()

with st.spinner("데이터 조회 중..."):
    reports = _parallel_fetch_reports(active_day, active_shift)

df_oee_total = reports.get("oee_total", pd.DataFrame())
df_oee_line = reports.get("oee_line", pd.DataFrame())
df_oee_station = reports.get("oee_station", pd.DataFrame())

has_oee = (not _is_empty_df(df_oee_total)) or (not _is_empty_df(df_oee_line)) or (not _is_empty_df(df_oee_station))
if has_oee:
    st.markdown("### 📌 OEE 결과 (Total / Line / Station)")
    col1, col2, col3 = st.columns([1.05, 1.05, 1.15])

    with col1:
        if not _is_empty_df(df_oee_total):
            st.markdown("**전체 OEE**")
            safe_show_df(df_oee_total, raw_df=df_oee_total, use_container_width=True, height=210)

    with col2:
        if not _is_empty_df(df_oee_line):
            st.markdown("**Line별 OEE**")
            safe_show_df(df_oee_line, raw_df=df_oee_line, use_container_width=True, height=210)

    with col3:
        if not _is_empty_df(df_oee_station):
            st.markdown("**Station별 OEE**")
            safe_show_df(df_oee_station, raw_df=df_oee_station, use_container_width=True, height=320)

    st.divider()

df_planned = reports.get("planned", pd.DataFrame())
if not _is_empty_df(df_planned):
    st.markdown("### 📌 계획 정지 시간")
    safe_show_df(df_planned, raw_df=df_planned, use_container_width=True, height=260)
    st.divider()

df_non = reports.get("nonop", pd.DataFrame())
if not _is_empty_df(df_non):
    st.markdown("### 📌 비가동 시간")
    safe_show_df(df_non, raw_df=df_non, use_container_width=True, height=260)
    st.divider()

df_amt = reports.get("amt", pd.DataFrame())
if not _is_empty_df(df_amt):
    st.markdown("### 📌 품번별 총 생산량")
    safe_show_df(df_amt, raw_df=df_amt, use_container_width=True, height=320)
    st.divider()

df_pct = reports.get("pct", pd.DataFrame())
if not _is_empty_df(df_pct):
    st.markdown("### 📌 TEST 합격률")
    safe_show_df(df_pct, raw_df=df_pct, use_container_width=True, height=240)
    st.divider()

fail_sections = [
    ("FCT 1회 검사 FAIL LIST", "fct_1time"),
    ("FCT 2회 검사 FAIL LIST", "fct_2time"),
    ("FCT 3회 이상 검사 FAIL LIST", "fct_3over"),
    ("Vision 1회 검사 FAIL LIST", "vis_1time"),
    ("Vision 2회 검사 FAIL LIST", "vis_2time"),
    ("Vision 3회 이상 검사 FAIL LIST", "vis_3over"),
]

shown_any = False
for title, key in fail_sections:
    df = reports.get(key, pd.DataFrame())
    if _is_empty_df(df):
        continue
    if not shown_any:
        st.markdown("### 📌 FAIL LIST")
        shown_any = True
    st.markdown(f"**{title}**")
    safe_show_df(df, raw_df=df, use_container_width=True, height=260)
    st.markdown("")

# ✅ SSE는 마지막 (현재 시각 구독 + 조회 scope 필터)
now_day, now_shift = _now_prod_day_shift()
_mount_alarm_sse(now_day, now_shift, active_day, active_shift)


def _snap_mark_ready():
    components.html('<div id="__snap_ready__" style="display:none">READY</div>', height=0)


# ... 모든 차트/데이터프레임 렌더링이 끝난 다음:
_snap_mark_ready()