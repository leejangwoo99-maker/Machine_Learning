# app/streamlit_app/pages/01_production_status.py
# -*- coding: utf-8 -*-
from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Tuple, Any
import hashlib
import json

import matplotlib
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd
import streamlit as st

from api_client import (
    get_worker_info,
    get_email_list,
    get_remark_info,
    get_planned_time_today,
    get_non_operation_time,
    get_mastersample_test_info,
    get_alarm_record_recent,
    post_worker_info_sync,
    post_email_list_sync,
    post_remark_info_sync,
    post_planned_time_sync,
    post_non_operation_time_sync,
    get_sections_latest,
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
NONOP_MAX_ROWS = 10  # 옵션 B: 최근 10행만


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


def parse_hms(base_start: datetime, s: Any):
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

    if base_start.hour == 20 and hh < 12:
        dt = dt + timedelta(days=1)
    return dt


def safe_df(rows: List[Dict], cols: List[str]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=cols)
    df = pd.DataFrame(rows)
    for c in cols:
        if c not in df.columns:
            df[c] = ""
    return df[cols]


def sort_nonop_df(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    def _time_to_sec(x: Any) -> float:
        s = str(x or "").strip()
        if not s:
            return -1.0
        try:
            p = s.split(":")
            hh = int(p[0])
            mm = int(p[1]) if len(p) > 1 else 0
            ss = float(p[2]) if len(p) > 2 else 0.0
            return hh * 3600 + mm * 60 + ss
        except Exception:
            return -1.0

    tmp = df.copy()
    tmp["_end_day_num"] = pd.to_numeric(tmp.get("end_day", ""), errors="coerce").fillna(0).astype(int)
    tmp["_from_sec"] = tmp.get("from_time", "").map(_time_to_sec)
    tmp = tmp.sort_values(["_end_day_num", "_from_sec"], ascending=[False, False], na_position="last")
    return tmp.drop(columns=["_end_day_num", "_from_sec"], errors="ignore")


def build_time_options() -> List[str]:
    out: List[str] = []
    for h in range(0, 24):
        for m in range(0, 60, 5):
            out.append(f"{h:02d}:{m:02d}")
    return out


def normalize_day_yyyymmdd(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 8:
        return digits[:8]
    return ""


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


def alarm_pk(row: Dict[str, Any]) -> str:
    key = {
        "end_day": normalize_day_yyyymmdd(row.get("end_day", "")),
        "end_time": str(row.get("end_time", "")).strip(),
        "station": str(row.get("station", "")).strip(),
        "sparepart": str(row.get("sparepart", "")).strip(),
        "type_alarm": str(row.get("type_alarm", "")).strip(),
    }
    raw = json.dumps(key, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# -------------------------
# 섹션별 캐시 로더
# -------------------------
@st.cache_data(ttl=15, show_spinner=False)
def load_chart_nonop(end_day_: str, shift_: str, tick_: int):
    return get_non_operation_time(end_day_, shift_)


@st.cache_data(ttl=15, show_spinner=False)
def load_planned(end_day_: str, shift_: str, tick_: int):
    return get_planned_time_today(end_day_, shift_)


@st.cache_data(ttl=15, show_spinner=False)
def load_alarm(end_day_: str, shift_: str, tick_: int):
    return get_alarm_record_recent(end_day_, shift_)


@st.cache_data(ttl=15, show_spinner=False)
def load_worker(end_day_: str, shift_: str, tick_: int):
    return get_worker_info(end_day_, shift_)


@st.cache_data(ttl=15, show_spinner=False)
def load_master(end_day_: str, shift_: str, tick_: int):
    return get_mastersample_test_info(end_day_, shift_)


# -------------------------
# 앱 시작
# -------------------------
st.set_page_config(page_title="생산현황", layout="wide")

# 섹션별 tick
if "chart_tick" not in st.session_state:
    st.session_state.chart_tick = 0
if "nonop_tick" not in st.session_state:
    st.session_state.nonop_tick = 0
if "planned_tick" not in st.session_state:
    st.session_state.planned_tick = 0
if "alarm_tick" not in st.session_state:
    st.session_state.alarm_tick = 0
if "worker_tick" not in st.session_state:
    st.session_state.worker_tick = 0
if "master_tick" not in st.session_state:
    st.session_state.master_tick = 0

if "seen_alarm_pks" not in st.session_state:
    st.session_state.seen_alarm_pks = set()
if "active_alarm_pk" not in st.session_state:
    st.session_state.active_alarm_pk = ""
if "active_alarm_text" not in st.session_state:
    st.session_state.active_alarm_text = ""
if "section_tokens" not in st.session_state:
    st.session_state.section_tokens = {}

dt_now = now_kst()
shift = detect_shift(dt_now)
end_day = dt_now.strftime("%Y%m%d")
weekday_kr = ["월요일", "화요일", "수요일", "목요일", "금요일", "토요일", "일요일"][dt_now.weekday()]
shift_kr = "주간" if shift == "day" else "야간"

if st.session_state.get("last_shift") != shift:
    st.session_state["last_shift"] = shift
    st.session_state["active_alarm_pk"] = ""
    st.session_state["active_alarm_text"] = ""
    # shift 변경 시 섹션만 갱신
    st.session_state.chart_tick += 1
    st.session_state.nonop_tick += 1
    st.session_state.planned_tick += 1
    st.session_state.alarm_tick += 1
    st.session_state.worker_tick += 1
    st.session_state.master_tick += 1

errs: Dict[str, str] = {}

# 데이터 로드 (섹션별)
try:
    chart_nonop_rows = load_chart_nonop(end_day, shift, st.session_state.nonop_tick)
except Exception as e:
    chart_nonop_rows = []
    errs["chart_nonop"] = str(e)

try:
    planned_rows = load_planned(end_day, shift, st.session_state.planned_tick)
except Exception as e:
    planned_rows = []
    errs["planned"] = str(e)

try:
    alarm_rows = load_alarm(end_day, shift, st.session_state.alarm_tick)
except Exception as e:
    alarm_rows = []
    errs["alarm"] = str(e)

try:
    worker_rows = load_worker(end_day, shift, st.session_state.worker_tick)
except Exception as e:
    worker_rows = []
    errs["worker"] = str(e)

try:
    master_rows = load_master(end_day, shift, st.session_state.master_tick)
except Exception as e:
    master_rows = []
    errs["master"] = str(e)

# 섹션 토큰 체크: 변경된 섹션 tick만 증가
try:
    latest = get_sections_latest(end_day, shift)
    new_tokens = (latest or {}).get("tokens", {}) if isinstance(latest, dict) else {}
    old_tokens = st.session_state.get("section_tokens", {})

    if old_tokens and new_tokens:
        # nonop_detail
        if old_tokens.get("nonop_detail") != new_tokens.get("nonop_detail"):
            st.session_state.nonop_tick += 1
            st.session_state.chart_tick += 1
        # planned
        if old_tokens.get("planned") != new_tokens.get("planned"):
            st.session_state.planned_tick += 1
            st.session_state.chart_tick += 1
        # alarm
        if old_tokens.get("alarm") != new_tokens.get("alarm"):
            st.session_state.alarm_tick += 1
        # worker
        if old_tokens.get("worker_info") != new_tokens.get("worker_info"):
            st.session_state.worker_tick += 1
        # mastersample
        if old_tokens.get("mastersample") != new_tokens.get("mastersample"):
            st.session_state.master_tick += 1

    st.session_state.section_tokens = new_tokens or old_tokens
except Exception:
    pass

h1, h2 = st.columns([3, 2])
with h1:
    st.markdown(f"## {dt_now:%Y-%m-%d} {weekday_kr} {shift_kr} 생산현황")
with h2:
    b1, b2, b3, b4 = st.columns(4)
    if b1.button("새로고침", use_container_width=True):
        st.session_state.chart_tick += 1
        st.session_state.nonop_tick += 1
        st.session_state.planned_tick += 1
        st.session_state.alarm_tick += 1
        st.session_state.worker_tick += 1
        st.session_state.master_tick += 1
        st.rerun()
    if b2.button("email_list", use_container_width=True):
        st.session_state["modal_email"] = True
    if b3.button("barcode", use_container_width=True):
        st.session_state["modal_barcode"] = True
    if b4.button("계획정지시간", use_container_width=True):
        st.session_state["modal_planned"] = True

st.divider()

if errs:
    with st.expander("조회 오류 상세", expanded=False):
        st.json(errs)

# 알람 팝업
candidate = None
for r in alarm_rows or []:
    t_alarm = str(r.get("type_alarm", "")).strip()
    if t_alarm not in ALARM_ALLOWED_TYPES:
        continue
    p = alarm_pk(r)
    if p in st.session_state.seen_alarm_pks:
        continue
    msg = alarm_message(
        station=str(r.get("station", "")).strip(),
        sparepart=str(r.get("sparepart", "")).strip(),
        type_alarm=t_alarm,
    )
    if msg:
        candidate = (p, msg)
        break

if candidate and not st.session_state.active_alarm_pk:
    st.session_state.active_alarm_pk = candidate[0]
    st.session_state.active_alarm_text = candidate[1]

if st.session_state.active_alarm_pk:
    @st.dialog("⚠️ 알람 발생", width="large")
    def alarm_dialog():
        st.warning(st.session_state.active_alarm_text)
        c1, c2 = st.columns(2)
        if c1.button("확인", use_container_width=True):
            st.session_state.seen_alarm_pks.add(st.session_state.active_alarm_pk)
            st.session_state.active_alarm_pk = ""
            st.session_state.active_alarm_text = ""
            st.rerun()
        if c2.button("새로고침", use_container_width=True):
            st.session_state.alarm_tick += 1
            st.rerun()

    alarm_dialog()

# chart + nonop detail
l, r = st.columns([1.25, 1.0])

with l:
    st.subheader("실시간 생산 진행 현황")

    win_start, win_end = get_window(dt_now, shift)
    progressed_end = min(max(dt_now, win_start), win_end)

    fig, ax = plt.subplots(figsize=(10, 7))
    total_min = (win_end - win_start).total_seconds() / 60.0
    progressed_min = max(0.0, (progressed_end - win_start).total_seconds() / 60.0)

    for i, _ in enumerate(STATIONS):
        ax.bar(i, total_min, bottom=0.0, width=0.58, color=COLOR_IDLE, edgecolor="none")
        ax.bar(i, progressed_min, bottom=0.0, width=0.58, color=COLOR_RUN, edgecolor="none")

    for row in planned_rows or []:
        ft = parse_hms(win_start, row.get("from_time"))
        tt = parse_hms(win_start, row.get("to_time"))
        if ft is None or tt is None:
            continue
        s = max(ft, win_start)
        e = min(tt, progressed_end)
        if e <= s:
            continue
        b = (s - win_start).total_seconds() / 60.0
        h = max((e - s).total_seconds() / 60.0, MIN_SEG_MIN)
        for i in range(len(STATIONS)):
            ax.bar(i, h, bottom=b, width=0.58, color=COLOR_PLAN, edgecolor="none")

    for row in chart_nonop_rows or []:
        stn = str(row.get("station", "")).strip()
        if stn not in STATIONS:
            continue
        i = STATIONS.index(stn)
        ft = parse_hms(win_start, row.get("from_time"))
        tt = parse_hms(win_start, row.get("to_time"))
        if ft is None or tt is None:
            continue
        s = max(ft, win_start)
        e = min(tt, progressed_end)
        if e <= s:
            continue
        b = (s - win_start).total_seconds() / 60.0
        h = max((e - s).total_seconds() / 60.0, MIN_SEG_MIN)
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

with r:
    st.subheader("비가동 시간 상세(sparepart 교체시 반드시 입력)")

    display_cols = ["end_day", "station", "from_time", "to_time", "reason", "sparepart"]
    raw_nonop = chart_nonop_rows or []
    ndf = pd.DataFrame(raw_nonop)

    if ndf.empty:
        ndf = pd.DataFrame(columns=display_cols)
    else:
        for c in display_cols:
            if c not in ndf.columns:
                ndf[c] = ""
        ndf = ndf[display_cols].copy()

    ndf = sort_nonop_df(ndf).head(NONOP_MAX_ROWS).reset_index(drop=True)
    if ndf.empty:
        ndf_edit = pd.DataFrame([{
            "end_day": end_day,
            "station": "",
            "from_time": "",
            "to_time": "",
            "reason": "",
            "sparepart": "",
        }])
    else:
        ndf_edit = ndf.copy()

    with st.form("nonop_form", clear_on_submit=False):
        edited_nonop = st.data_editor(
            ndf_edit,
            use_container_width=True,
            hide_index=True,
            num_rows="dynamic",
            key="nonop_editor_single",
            column_order=display_cols,
            column_config={
                "end_day": st.column_config.TextColumn("end_day"),
                "station": st.column_config.SelectboxColumn("station", options=STATIONS),
                "reason": st.column_config.SelectboxColumn("reason", options=REASON_OPTIONS),
                "sparepart": st.column_config.SelectboxColumn("sparepart", options=SPAREPART_OPTIONS),
            },
        )
        save_nonop = st.form_submit_button("비가동 상세 저장", use_container_width=True)

    if save_nonop:
        try:
            rows = edited_nonop.to_dict("records")
            payload = []
            for rr in rows:
                reason = str(rr.get("reason", "")).strip()
                spare = str(rr.get("sparepart", "")).strip()
                if reason != "sparepart 교체":
                    spare = ""

                payload.append({
                    "end_day": normalize_day_yyyymmdd(rr.get("end_day", "")) or end_day,
                    "station": str(rr.get("station", "")).strip(),
                    "from_time": str(rr.get("from_time", "")).strip(),
                    "to_time": str(rr.get("to_time", "")).strip(),
                    "reason": reason,
                    "sparepart": spare,
                })

            payload = [x for x in payload if x["station"] and x["from_time"] and x["to_time"]]
            post_non_operation_time_sync(end_day, shift, payload)
            st.success("비가동 상세 저장 성공")
            st.session_state.nonop_tick += 1
            st.session_state.chart_tick += 1
            st.rerun()
        except Exception as e:
            st.error(f"비가동 상세 저장 실패: {e}")

# worker / mastersample
c1, c2 = st.columns(2)

with c1:
    st.subheader("작업자 정보")
    wdf = safe_df(worker_rows or [], ["end_day", "shift_type", "worker_name", "order_number"])
    if wdf.empty:
        wdf = pd.DataFrame([{
            "end_day": end_day,
            "shift_type": shift,
            "worker_name": "",
            "order_number": "",
        }])

    with st.form("worker_form", clear_on_submit=False):
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
        save_worker = st.form_submit_button("worker_info 저장", use_container_width=True)

    if save_worker:
        try:
            rows = edited_wdf.to_dict("records")
            payload = []
            for rr in rows:
                payload.append({
                    "end_day": normalize_day_yyyymmdd(rr.get("end_day", "")) or end_day,
                    "shift_type": str(rr.get("shift_type", "")).strip() or shift,
                    "worker_name": str(rr.get("worker_name", "")).strip(),
                    "order_number": str(rr.get("order_number", "")).strip(),
                })
            payload = [x for x in payload if x["worker_name"]]
            post_worker_info_sync(end_day, shift, payload)
            st.success("worker_info 저장 성공")
            st.session_state.worker_tick += 1
            st.rerun()
        except Exception as e:
            st.error(f"worker_info 저장 실패: {e}")

with c2:
    st.subheader("mastersample test")
    mdf = pd.DataFrame(master_rows or []) if master_rows else pd.DataFrame()
    if not mdf.empty:
        mdf = mdf.head(2)
    st.dataframe(mdf, use_container_width=True, hide_index=True)

# email modal
if st.session_state.get("modal_email", False):
    @st.dialog("Email List 수정", width="large")
    def email_modal():
        rows = get_email_list(end_day, shift)
        edf = safe_df(rows, ["email"])
        if edf.empty:
            edf = pd.DataFrame([{"email": ""}])

        edited = st.data_editor(edf, use_container_width=True, num_rows="dynamic", hide_index=True)
        pw = st.text_input("관리자 비밀번호", type="password")
        c1m, c2m = st.columns(2)

        if c1m.button("저장", use_container_width=True):
            try:
                payload = [{"email": str(r.get("email", "")).strip()} for r in edited.to_dict("records")]
                payload = [x for x in payload if x["email"]]
                post_email_list_sync(end_day, shift, payload, password=pw)
                st.success("저장 성공")
                st.session_state["modal_email"] = False
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

        if c2m.button("닫기", use_container_width=True):
            st.session_state["modal_email"] = False
            st.rerun()

    email_modal()

# barcode modal
if st.session_state.get("modal_barcode", False):
    @st.dialog("Barcode(remark_info) 수정", width="large")
    def barcode_modal():
        rows = get_remark_info(end_day, shift)
        bdf = safe_df(rows, ["barcode_information", "pn", "remark"])
        if bdf.empty:
            bdf = pd.DataFrame([{"barcode_information": "", "pn": "", "remark": ""}])

        edited = st.data_editor(bdf, use_container_width=True, num_rows="dynamic", hide_index=True)
        pw = st.text_input("관리자 비밀번호", type="password", key="barcode_pw")
        c1m, c2m = st.columns(2)

        if c1m.button("저장", use_container_width=True):
            try:
                payload = edited.to_dict("records")
                post_remark_info_sync(end_day, shift, payload, password=pw)
                st.success("저장 성공")
                st.session_state["modal_barcode"] = False
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

        if c2m.button("닫기", use_container_width=True):
            st.session_state["modal_barcode"] = False
            st.rerun()

    barcode_modal()

# planned modal
if st.session_state.get("modal_planned", False):
    @st.dialog("계획 정지 시간(planned_time) 수정", width="large")
    def planned_modal():
        rows = get_planned_time_today(end_day, shift)
        pdf = safe_df(rows, ["prod_day", "shift_type", "from_time", "to_time", "reason"])
        if pdf.empty:
            pdf = pd.DataFrame([{
                "prod_day": end_day,
                "shift_type": shift,
                "from_time": "",
                "to_time": "",
                "reason": "",
            }])

        time_opts = build_time_options()
        edited = st.data_editor(
            pdf,
            use_container_width=True,
            num_rows="dynamic",
            hide_index=True,
            column_config={
                "from_time": st.column_config.SelectboxColumn("from_time", options=time_opts),
                "to_time": st.column_config.SelectboxColumn("to_time", options=time_opts),
            },
        )

        c1m, c2m = st.columns(2)
        if c1m.button("저장", use_container_width=True):
            try:
                payload = []
                for rr in edited.to_dict("records"):
                    ft = str(rr.get("from_time", "")).strip()
                    tt = str(rr.get("to_time", "")).strip()
                    if len(ft) == 5:
                        ft = f"{ft}:00"
                    if len(tt) == 5:
                        tt = f"{tt}:00"

                    payload.append({
                        "prod_day": normalize_day_yyyymmdd(rr.get("prod_day", "")) or end_day,
                        "shift_type": str(rr.get("shift_type", "")).strip() or shift,
                        "from_time": ft,
                        "to_time": tt,
                        "reason": str(rr.get("reason", "")).strip(),
                    })

                payload = [x for x in payload if x["from_time"] and x["to_time"]]
                post_planned_time_sync(end_day, shift, payload)
                st.success("저장 성공")
                st.session_state["modal_planned"] = False
                st.session_state.planned_tick += 1
                st.session_state.chart_tick += 1
                st.rerun()
            except Exception as e:
                st.error(f"저장 실패: {e}")

        if c2m.button("닫기", use_container_width=True):
            st.session_state["modal_planned"] = False
            st.rerun()

    planned_modal()

# 자동 업데이트(15초)
st.markdown(
    """
    <script>
      setTimeout(function() {
        window.parent.postMessage({isStreamlitMessage: true, type: "streamlit:rerunScript"}, "*");
      }, 15000);
    </script>
    """,
    unsafe_allow_html=True,
)
