# app/streamlit_app/pages/03_production_analysis.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Any, List, Optional, Tuple

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

import api_client as api

matplotlib.rcParams["font.family"] = ["Malgun Gothic", "NanumGothic", "DejaVu Sans"]
matplotlib.rcParams["axes.unicode_minus"] = False

KST = ZoneInfo("Asia/Seoul")

ALARM_ALLOWED_TYPES = {"권고", "긴급", "교체"}
STATION_ORDER = ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"]


# -----------------------------
# Helpers
# -----------------------------
def _now_prod_day_shift() -> tuple[str, str]:
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


def _drop_cols(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    drop = [c for c in cols if c in df.columns]
    return df.drop(columns=drop) if drop else df


def _safe_call(fn, *args, **kwargs):
    """
    - fn이 kwargs(timeout=...) 등을 못 받는 경우 자동으로 kwargs 제거 후 재시도
    - UI 크래시 방지: 실패 시 None
    """
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


def _get_api_fn(*names: str):
    for name in names:
        fn = getattr(api, name, None)
        if callable(fn):
            return fn
    return None


def _api_base_url() -> str:
    for attr in ("API_BASE_URL", "BASE_URL", "API_URL", "API"):
        v = getattr(api, attr, None)
        if isinstance(v, str) and v.strip().startswith("http"):
            return v.strip().rstrip("/")
    return "http://127.0.0.1:8000"


def _station_sort_key(stn: Any) -> int:
    s = str(stn or "").strip()
    try:
        return STATION_ORDER.index(s)
    except Exception:
        return 999


def _shift_window_for_prod_day(prod_day: str, shift_type: str) -> Tuple[datetime, datetime]:
    d = _norm_day(prod_day)
    if not d:
        d = datetime.now(tz=KST).strftime("%Y%m%d")

    base = datetime.strptime(d, "%Y%m%d").replace(tzinfo=KST)

    if shift_type == "day":
        start = base.replace(hour=8, minute=30, second=0, microsecond=0)
        end = base.replace(hour=20, minute=30, second=0, microsecond=0)
        return start, end

    start = base.replace(hour=20, minute=30, second=0, microsecond=0)
    end = (start + timedelta(days=1)).replace(hour=8, minute=30, second=0, microsecond=0)
    return start, end


def _parse_alarm_dt(end_day_yyyy_mm_dd: str, end_time_hms: str) -> Optional[datetime]:
    try:
        d = str(end_day_yyyy_mm_dd or "").strip()
        t = str(end_time_hms or "").strip()
        if not d or not t:
            return None
        dt = datetime.fromisoformat(f"{d}T{t}")
        return dt.replace(tzinfo=KST)
    except Exception:
        return None


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
# ✅ 02와 동일한 "비차단 + 중앙" 알람 SSE 모달 (안정버전)
# -----------------------------
def _mount_alarm_sse(now_day: str, now_shift: str, view_day: str, view_shift: str):
    base = _api_base_url().rstrip("/")

    admin_pass = (getattr(api, "ADMIN_PASS", "") or "").strip() or (os.getenv("ADMIN_PASS", "") or "").strip()
    token_qs = f"&token={admin_pass}" if admin_pass else ""

    sse_url = f"{base}/events/stream?end_day={now_day}&shift_type={now_shift}&sections=alarm{token_qs}"

    view_day_norm = _norm_day(view_day) or now_day
    view_shift_norm = _norm_shift(view_shift)

    components.html(
        f"""
        <script>
        (function() {{
          const W = window.parent;
          const url = {json.dumps(sse_url)};
          const ALLOWED = new Set(["권고","긴급","교체"]);
          const ACK_KEY = "acked_alarm_pks";

          const VIEW_DAY   = {json.dumps(view_day_norm)};
          const VIEW_SHIFT = {json.dumps(view_shift_norm)};

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

          function _digits8(s) {{
            const t = String(s||"").trim();
            const digits = t.replace(/\\D/g, "");
            return digits.length >= 8 ? digits.slice(0,8) : "";
          }}

          function parseYmdToUtcMs(ymd) {{
            const d = _digits8(ymd);
            if (!d) return null;
            const y = Number(d.slice(0,4));
            const m = Number(d.slice(4,6)) - 1;
            const dd = Number(d.slice(6,8));
            return Date.UTC(y, m, dd, 0, 0, 0, 0);
          }}

          function parseHmsToSec(hms) {{
            const t = String(hms||"").trim();
            if (t.length < 5) return 0;
            const hh = Number(t.slice(0,2) || 0);
            const mm = Number(t.slice(3,5) || 0);
            const ss = Number(t.slice(6,8) || 0);
            return hh*3600 + mm*60 + ss;
          }}

          function kstDateFromEndDayTime(endDay, endTime) {{
            const utc0 = parseYmdToUtcMs(endDay);
            if (utc0 == null) return null;
            const sec = parseHmsToSec(endTime);
            const ms = utc0 + (9*3600 + sec)*1000;
            return new Date(ms);
          }}

          function viewWindowKst(viewDay, viewShift) {{
            const utc0 = parseYmdToUtcMs(viewDay);
            if (utc0 == null) return [null, null];

            const kst0 = utc0 + 9*3600*1000;
            const dayStart   = kst0 + (8*3600 + 30*60)*1000;
            const nightStart = kst0 + (20*3600 + 30*60)*1000;

            const sh = String(viewShift||"").toLowerCase();
            if (sh === "day") {{
              return [new Date(dayStart), new Date(nightStart)];
            }}

            const nextKst0 = kst0 + 24*3600*1000;
            const nextDayStart = nextKst0 + (8*3600 + 30*60)*1000;
            return [new Date(nightStart), new Date(nextDayStart)];
          }}

          const VIEW_W = viewWindowKst(VIEW_DAY, VIEW_SHIFT);

          function inViewWindow(row) {{
            try {{
              const dt = kstDateFromEndDayTime(row.end_day, row.end_time);
              const ws = VIEW_W[0], we = VIEW_W[1];
              if (!dt || !ws || !we) return false;
              return (dt >= ws && dt <= we);
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

          function closeExistingModal() {{
            const doc = W.document;
            const old = doc.querySelector('[id^="alarmModal_"]');
            if (old) old.remove();
          }}

          function showModal(pk, message) {{
            const doc = W.document;
            const acked = getAcked();
            if (acked.includes(pk)) {{
              log("skip acked pk=", pk);
              return;
            }}

            if (doc.getElementById("alarmModal_" + pk)) {{
              log("already showing pk=", pk);
              return;
            }}

            closeExistingModal();

            const overlay = doc.createElement("div");
            overlay.id = "alarmModal_" + pk;
            overlay.style.cssText = `
              position: fixed; z-index: 2147483647;
              left: 0; top: 0; width: 100%; height: 100%;
              background: transparent;
              pointer-events: none;
              display: flex; align-items: center; justify-content: center;
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
              pointer-events: auto;
              border: 1px solid rgba(0,0,0,0.08);
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

            if (t === "권고") return stn + ", " + sp + " 교체 권고 드립니다.";
            if (t === "긴급") return stn + ", " + sp + " 교체 긴급합니다.";
            if (t === "교체") return stn + ", " + sp + " 교체 타이밍이 지났습니다.";
            return stn + ", " + sp + " 알람(" + t + ")";
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

            if (!inViewWindow(row)) {{
              log(fromEvent, "skip out-of-window(view)", row);
              return;
            }}

            if (!pk) {{
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
# UI
# -----------------------------
st.set_page_config(page_title="생산 분석", layout="wide")

default_day, default_shift = _now_prod_day_shift()
if "analysis_prod_day" not in st.session_state:
    st.session_state.analysis_prod_day = default_day
if "analysis_shift_type" not in st.session_state:
    st.session_state.analysis_shift_type = default_shift

st.markdown("### 📌 생산 분석 (주간/야간)")
c1, c2, c3, c4 = st.columns([2.2, 1.2, 0.9, 1.1])

with c1:
    prod_day_in = st.text_input("prod_day", value=str(st.session_state.analysis_prod_day), label_visibility="visible")
with c2:
    shift_in = st.selectbox(
        "shift_type",
        options=["day", "night"],
        index=0 if str(st.session_state.analysis_shift_type) == "day" else 1,
    )
with c3:
    if st.button("검색", use_container_width=True):
        st.session_state.analysis_prod_day = _norm_day(prod_day_in) or default_day
        st.session_state.analysis_shift_type = _norm_shift(shift_in)
        st.rerun()
with c4:
    if st.button("전체 새로고침", use_container_width=True):
        try:
            st.cache_data.clear()
        except Exception:
            pass
        try:
            st.cache_resource.clear()
        except Exception:
            pass
        st.rerun()

prod_day = _norm_day(st.session_state.analysis_prod_day) or default_day
shift_type = _norm_shift(st.session_state.analysis_shift_type)

st.caption(f"현재 조회: prod_day={prod_day} / shift_type={shift_type}")

# ✅ 실시간 알람 팝업(SSE)은 "현재 운영중인 shift" 조회일 때만
now_day, now_shift = _now_prod_day_shift()
if (prod_day == now_day) and (shift_type == now_shift):
    _mount_alarm_sse(now_day, now_shift, prod_day, shift_type)
else:
    st.caption("과거 날짜 조회 중: 실시간 알람 팝업(SSE)은 비활성화됩니다.")

st.divider()

# =========================================================
# 1) [AI 예상 경고] SPAREPART 교체 알람 리스트
# =========================================================
st.markdown("### [AI 예상 경고] SPAREPART 교체 알람 리스트 - '권고' 알람 시 SPAREPART 교체 필요")

fn_alarm_recent = _get_api_fn(
    "get_alarm_record_recent",
    "get_alarm_recent",
    "get_alarm_record_latest",
    "get_alarm_record",
)

alarm_df = pd.DataFrame()
if fn_alarm_recent:
    resp = _safe_call(fn_alarm_recent, prod_day, shift_type, timeout=5.0)
    alarm_df = _df_from_resp(resp)
    if alarm_df.empty:
        resp2 = _safe_call(fn_alarm_recent, prod_day, shift_type, timeout=8.0)
        alarm_df = _df_from_resp(resp2)

if not _is_empty_df(alarm_df):
    keep = ["end_day", "end_time", "station", "sparepart", "type_alarm"]
    for c in keep:
        if c not in alarm_df.columns:
            alarm_df[c] = ""
    alarm_df = alarm_df[keep].copy()

    w_start, w_end = _shift_window_for_prod_day(prod_day, shift_type)

    def _in_window(r) -> bool:
        dt = _parse_alarm_dt(r.get("end_day", ""), r.get("end_time", ""))
        if dt is None:
            return False
        return (dt >= w_start) and (dt <= w_end)

    alarm_df = alarm_df[alarm_df.apply(_in_window, axis=1)].copy()

    alarm_df["type_alarm"] = alarm_df["type_alarm"].astype(str).str.strip().str.replace(" ", "", regex=False)
    alarm_df = alarm_df[alarm_df["type_alarm"].isin(list(ALARM_ALLOWED_TYPES))].copy()

    alarm_df["__k"] = alarm_df["station"].apply(_station_sort_key)
    alarm_df = alarm_df.sort_values(["__k", "end_day", "end_time"], ascending=[True, True, True]).drop(columns=["__k"])

if _is_empty_df(alarm_df):
    st.info("현재 조건에서 표시할 알람이 없습니다.")
else:
    safe_show_df(alarm_df, raw_df=alarm_df, use_container_width=True, hide_index=True)

st.divider()

# -----------------------------
# 2) [AI 경고] PD-BOARD 열화 모니터링
# -----------------------------
st.markdown("### [AI 경고] PD-BOARD 열화 모니터링 (WARNING은 교체 검토 필요, 교체됬다면 모니터링 필요 / CRITICAL은 교체 필요)")

fn_pd = _get_api_fn(
    "get_pd_board_check",
    "get_pd_board_check_latest",
    "get_pd_board_check_recent",
)

pd_df = pd.DataFrame()
if fn_pd:
    resp = _safe_call(fn_pd, end_day=prod_day, timeout=8.0)
    pd_df = _df_from_resp(resp)

if not _is_empty_df(pd_df):
    need = ["end_day", "station", "last_status", "cosine_similarity"]
    for c in need:
        if c not in pd_df.columns:
            pd_df[c] = None
    pd_df = pd_df[need].copy()

    def _norm_end_day_any(v: Any) -> str:
        s = str(v or "").strip()
        digits = "".join(ch for ch in s if ch.isdigit())
        return digits[:8] if len(digits) >= 8 else ""

    pd_df["end_day_norm"] = pd_df["end_day"].apply(_norm_end_day_any)

    want_day = _norm_day(prod_day)
    pd_df_sel = pd_df[pd_df["end_day_norm"] == want_day].copy()

    if pd_df_sel.empty:
        st.info(f"PD-BOARD 데이터가 없습니다. (end_day={want_day})")
    else:
        pd_df_sel["__k"] = pd_df_sel["station"].apply(_station_sort_key)
        pd_df_sel = pd_df_sel.sort_values(["__k", "station"], ascending=[True, True]).drop(columns=["__k"])

        pd_table = pd_df_sel[["end_day", "station", "last_status"]].copy()

        def _style_status(v: Any) -> str:
            s = str(v or "").strip().upper()
            if s == "WARNING":
                return "background-color: #fff3bf; color: #5c4a00; font-weight: 700;"
            if s == "CRITICAL":
                return "background-color: #ffa8a8; color: #5c0000; font-weight: 800;"
            return ""

        safe_show_df(
            pd_table.style.applymap(_style_status, subset=["last_status"]),
            raw_df=pd_table,
            use_container_width=True,
            hide_index=True,
        )

        def _parse_cos(v: Any) -> Any:
            if isinstance(v, dict):
                return v
            if isinstance(v, str) and v.strip().startswith("{"):
                try:
                    return json.loads(v)
                except Exception:
                    return None
            return None

        series = []
        th_val = None

        for _, r in pd_df_sel.iterrows():
            stn = str(r.get("station", "") or "").strip()
            obj = _parse_cos(r.get("cosine_similarity", None))
            if not isinstance(obj, dict):
                continue

            xs = obj.get("x", [])
            ys = obj.get("y", [])
            th = obj.get("th", None)

            if th_val is None and th is not None:
                try:
                    th_val = float(th)
                except Exception:
                    th_val = None

            if not isinstance(xs, list) or not isinstance(ys, list) or len(xs) == 0 or len(xs) != len(ys):
                continue

            xdt = []
            for x in xs:
                s = str(x or "").strip()
                if len(s) == 8 and s.isdigit():
                    try:
                        xdt.append(datetime.strptime(s, "%Y%m%d").replace(tzinfo=KST))
                    except Exception:
                        xdt.append(None)
                else:
                    try:
                        dt = datetime.fromisoformat(s)
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=KST)
                        else:
                            dt = dt.astimezone(KST)
                        xdt.append(dt)
                    except Exception:
                        xdt.append(None)

            yv = []
            ok = True
            for y in ys:
                try:
                    yv.append(float(y))
                except Exception:
                    ok = False
                    break
            if not ok:
                continue

            xdt2, yv2 = [], []
            for a, b in zip(xdt, yv):
                if a is None:
                    continue
                xdt2.append(a)
                yv2.append(b)

            if xdt2:
                series.append((stn, xdt2, yv2))

        series.sort(key=lambda x: _station_sort_key(x[0]))

        if series:
            fig, ax = plt.subplots(figsize=(12, 4.8))
            for stn, xdt2, yv2 in series:
                ax.plot(xdt2, yv2, marker="o", linewidth=2, label=stn)

            if th_val is not None:
                ax.axhline(th_val, linestyle="--", linewidth=2, label="COS_TH")

            ax.set_title("Cosine Similarity to Abnormal Reference")
            ax.set_xlabel("Date")
            ax.set_ylabel("Cosine Similarity")
            ax.grid(alpha=0.25)
            ax.legend(loc="upper left", ncol=5, frameon=False)
            plt.tight_layout()
            st.pyplot(fig, clear_figure=True)
            plt.close(fig)
else:
    st.info("PD-BOARD 모니터링 데이터가 없습니다.")

st.divider()

# =========================================================
# 3) FCT worst case
# =========================================================
st.markdown("### FCT worst case-검사시간 이상치")

fn_f = _get_api_fn("get_report_f_worst_case", "get_report_f_worst_case_daily", "get_report_fct_worst_case")
df_f = _df_from_resp(_safe_call(fn_f, prod_day, shift_type)) if fn_f else pd.DataFrame()

if _is_empty_df(df_f):
    st.info("FCT worst case 데이터가 없습니다.")
else:
    keep = ["prod_day", "shift_type", "barcode_information", "pn", "remark", "run_time", "test_contents", "file_path"]
    for c in keep:
        if c not in df_f.columns:
            df_f[c] = ""
    df_f = df_f[keep].copy()
    safe_show_df(df_f, raw_df=df_f, use_container_width=True, hide_index=True)

st.divider()

# =========================================================
# 4) 조립 공정 불량에 따른 낭비시간
# =========================================================
st.markdown("### 조립 공정 불량에 따른 낭비시간")

fn_g = _get_api_fn("get_report_g_afa_wasted_time", "get_report_afa_wasted_time")
df_g = _df_from_resp(_safe_call(fn_g, prod_day, shift_type)) if fn_g else pd.DataFrame()

if _is_empty_df(df_g):
    st.info("조립 불량 낭비시간 데이터가 없습니다.")
else:
    candidates = ["Total 조립 불량 손실 시간", "Total", "total"]
    col = next((c for c in candidates if c in df_g.columns), None)
    if col is None:
        safe_show_df(df_g, raw_df=df_g, use_container_width=True, hide_index=True)
    else:
        safe_show_df(df_g[[col]].copy(), raw_df=df_g[[col]].copy(), use_container_width=True, hide_index=True)

st.divider()

# =========================================================
# 5) MES 불량에 따른 낭비시간
# =========================================================
st.markdown("### MES 불량에 따른 낭비시간")

fn_h = _get_api_fn("get_report_h_mes_wasted_time", "get_report_mes_wasted_time")
df_h = _df_from_resp(_safe_call(fn_h, prod_day, shift_type)) if fn_h else pd.DataFrame()

if _is_empty_df(df_h):
    st.info("MES 불량 낭비시간 데이터가 없습니다.")
else:
    df_h = _drop_cols(df_h, ["updated_at"])
    safe_show_df(df_h, raw_df=df_h, use_container_width=True, hide_index=True)


def _snap_mark_ready():
    components.html('<div id="__snap_ready__" style="display:none">READY</div>', height=0)


_snap_mark_ready()