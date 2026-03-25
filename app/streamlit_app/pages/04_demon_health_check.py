# app/streamlit_app/pages/04_demon_heath_check.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import time
from typing import Any, Dict, Optional

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

import api_client as api

st.set_page_config(page_title="프로그램 체크", layout="wide")

st.markdown("## 📌 프로그램 체크")
st.info(
    "log 컬럼의 번호 또는 문자로 시작되는 프로그램이 각각 apply_machine의 작업 관리자 프로세스에 2개 씩 작동하고 있을 것 "
    "(예시) 3_.....exe > FCT1 작업 관리자 프로세스에 2개 작동 중)"
)


# =========================================================
# ENV (운영에서 .env로 조절 가능)
# =========================================================
def _env_int(name: str, default: int, min_v: int, max_v: int) -> int:
    raw = os.getenv(name, "")
    s = str(raw or "").strip()
    if not s:
        return default
    try:
        v = int(float(s))
    except Exception:
        return default
    return max(min_v, min(max_v, v))


def _env_float(name: str, default: float, min_v: float, max_v: float) -> float:
    raw = os.getenv(name, "")
    s = str(raw or "").strip()
    if not s:
        return default
    try:
        v = float(s)
    except Exception:
        return default
    return max(min_v, min(max_v, v))


TTL_SEC = _env_float("ST_DEMON_TTL", default=3.0, min_v=1.0, max_v=60.0)
API_TIMEOUT = _env_float("ST_DEMON_TIMEOUT", default=12.0, min_v=2.0, max_v=60.0)
API_LIMIT = _env_int("ST_DEMON_LIMIT", default=200, min_v=10, max_v=5000)
API_RETRY = _env_int("ST_DEMON_RETRY", default=1, min_v=0, max_v=5)
API_RETRY_WAIT_SEC = _env_float("ST_DEMON_RETRY_WAIT", default=0.25, min_v=0.0, max_v=5.0)


# =========================================================
# Safe dataframe render
# =========================================================
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


c1, c2 = st.columns([8, 2])
with c2:
    if st.button("전체 새로고침", use_container_width=True):
        try:
            st.cache_data.clear()
        except Exception:
            pass
        st.rerun()

st.divider()


# =========================================================
# Safe load (cache 내부에서 예외 절대 밖으로 던지지 않기)
# =========================================================
@st.cache_data(ttl=float(TTL_SEC), show_spinner=False)
def _load_cached(limit: int, timeout: float, retry: int, retry_wait: float) -> Dict[str, Any]:
    last_err: Optional[str] = None
    for attempt in range(int(retry) + 1):
        try:
            rows = api.get_demon_health_latest(limit=int(limit), timeout=float(timeout))
            if rows is None:
                rows = []
            if isinstance(rows, dict):
                if isinstance(rows.get("items", None), list):
                    rows = rows["items"]
                else:
                    rows = [rows]
            if not isinstance(rows, list):
                rows = []
            return {"ok": True, "rows": rows, "error": ""}
        except Exception as e:
            last_err = str(e)
            if attempt < int(retry):
                try:
                    time.sleep(float(retry_wait))
                except Exception:
                    pass
                continue
            break
    return {"ok": False, "rows": [], "error": last_err or "unknown error"}


res = _load_cached(API_LIMIT, API_TIMEOUT, API_RETRY, API_RETRY_WAIT_SEC)
df = pd.DataFrame(res.get("rows", []) or [])

st.markdown("### demon health (k_demon_heath_check.total_demon_report)")

if not bool(res.get("ok", False)):
    st.warning(f"API 응답 지연/실패: {res.get('error','')}")
    st.caption(
        f"설정값: limit={API_LIMIT}, timeout={API_TIMEOUT:.1f}s, ttl={TTL_SEC:.1f}s, retry={API_RETRY} "
        f"(env: ST_DEMON_TIMEOUT / ST_DEMON_TTL / ST_DEMON_LIMIT / ST_DEMON_RETRY)"
    )

if df is None or df.empty:
    st.warning("표시할 데이터가 없습니다.")
else:
    # =========================
    # 1) end_day / end_time / updated_at 제거
    # =========================
    drop_cols = [c for c in ["end_day", "end_time", "updated_at"] if c in df.columns]
    if drop_cols:
        df = df.drop(columns=drop_cols)

    # =========================
    # 2) 컬럼 보정 + 순서 고정
    #    요구 순서: log, apply_machine, log_desc, status
    # =========================
    cols = ["log", "apply_machine", "log_desc", "status"]
    for c in cols:
        if c not in df.columns:
            df[c] = None
    df = df[cols].copy()

    # 표 fallback용 raw df
    df_plain = df.copy()

    # =========================
    # 3) status 색상 표시 (정상/비정상/주의/비운행)
    # =========================
    def _status_cell_style(v: Any) -> str:
        s = str(v or "").strip()
        if s == "정상":
            return "background-color: #d1fae5; color: #065f46; font-weight: 700;"
        if s == "비정상":
            return "background-color: #fee2e2; color: #991b1b; font-weight: 700;"
        if s == "주의":
            return "background-color: #fef9c3; color: #854d0e; font-weight: 700;"
        if s == "비운행":
            return "background-color: #111827; color: #ffffff; font-weight: 700;"
        return ""

    try:
        styler = df.style.applymap(_status_cell_style, subset=["status"])
    except Exception:
        styler = df_plain

    # =========================
    # 4) 스크롤 없이 전체표 출력 (행 수 기반 height 크게 잡기)
    # =========================
    n = int(len(df_plain))
    row_h = 35
    base_h = 80
    height = base_h + n * row_h
    height = max(240, min(3000, height))

    safe_show_df(
        styler,
        raw_df=df_plain,
        use_container_width=True,
        hide_index=True,
        height=height,
    )


def _snap_mark_ready():
    components.html('<div id="__snap_ready__" style="display:none">READY</div>', height=0)


_snap_mark_ready()