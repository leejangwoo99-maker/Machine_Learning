# app/streamlit_app/app.py
from __future__ import annotations

import streamlit as st

# ✅ scheduler/status (snap 모드에서는 실행 안 함)
#    UI에서 snapshot_mailer import가 실패해도 앱 전체는 뜨도록 보호
try:
    from app.job.snapshot_mailer import start_snapshot_scheduler_once, get_snapshot_scheduler_status
    _SNAP_IMPORT_OK = True
    _SNAP_IMPORT_ERR = ""
except Exception as e:
    _SNAP_IMPORT_OK = False
    _SNAP_IMPORT_ERR = str(e)

    def start_snapshot_scheduler_once():
        return None

    def get_snapshot_scheduler_status():
        return {
            "state": "unavailable",
            "ts": None,
            "note": _SNAP_IMPORT_ERR,
        }


def _qp_get(name: str) -> str:
    try:
        v = st.query_params.get(name)
        if v is None:
            return ""
        if isinstance(v, (list, tuple)):
            return (v[0] if v else "") or ""
        return str(v) or ""
    except Exception:
        return ""


def _is_snap_mode() -> bool:
    return _qp_get("snap").strip().lower() in {"1", "true", "yes", "y", "on"}


def _apply_snapshot_query_params() -> None:
    """
    Mailer가 붙여준 쿼리 파라미터(prod_day/shift_type)를 session_state에 주입.
    """
    prod_day = _qp_get("prod_day").strip()
    shift_type = _qp_get("shift_type").strip().lower()

    if not prod_day or shift_type not in {"day", "night"}:
        return

    keys = [
        ("prod_day", prod_day),
        ("shift_type", shift_type),

        ("status_prod_day", prod_day),
        ("status_shift_type", shift_type),

        ("info_prod_day", prod_day),
        ("info_shift_type", shift_type),

        ("analysis_prod_day", prod_day),
        ("analysis_shift_type", shift_type),

        ("demon_prod_day", prod_day),
        ("demon_shift_type", shift_type),
    ]
    for k, v in keys:
        st.session_state[k] = v


def _hide_chrome_for_snapshot() -> None:
    """
    snap=1일 때: 사이드바/상단바 최대한 숨김 (PDF 낭비 줄이기).
    """
    css = """
    <style>
    header { display: none !important; }
    [data-testid="stAppViewContainer"] .main .block-container { padding-top: 0.2rem !important; }
    section[data-testid="stSidebar"] { display: none !important; }
    button[kind="header"] { display: none !important; }
    button[data-testid="baseButton-headerNoPadding"] { display: none !important; }
    footer { display: none !important; }
    html, body { overflow: visible !important; }
    </style>
    """
    st.markdown(css, unsafe_allow_html=True)


def _page_key() -> str:
    page_key = _qp_get("page").strip()
    return page_key or "01_production_status"


def _render_scheduler_badge() -> None:
    """
    ✅ 일반 UI 모드에서만 표시되는 스케줄러 상태 배지
    """
    st.sidebar.markdown("### 📮 Snapshot Scheduler")
    stt = get_snapshot_scheduler_status()
    state = (stt.get("state") or "unknown")
    ts = (stt.get("ts") or "")

    badge = {
        "waiting": "🟡 waiting",
        "firing": "🟠 firing",
        "running": "🟠 running",
        "success": "🟢 success",
        "error": "🔴 error",
        "idle": "⚪ idle",
        "unavailable": "⚫ unavailable",
    }.get(state, f"⚪ {state}")

    st.sidebar.write(badge)

    # (선택) 부가 정보
    note = stt.get("note") or ""
    slot = stt.get("slot") or ""
    if note:
        st.sidebar.caption(f"note: {note}")
    if slot:
        st.sidebar.caption(f"slot: {slot}")
    if ts:
        st.sidebar.caption(ts)


def main() -> None:
    snap = _is_snap_mode()

    st.set_page_config(
        page_title="생산 현황",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state=("collapsed" if snap else "expanded"),
    )

    if snap:
        _hide_chrome_for_snapshot()

    # ✅ mailer가 준 prod_day/shift_type 고정 주입
    _apply_snapshot_query_params()

    # ✅ 스케줄러는 "UI 모드에서만" 시작 (snap=1에서는 절대 시작하지 않음)
    # ✅ import 성공한 경우에만 시작 시도
    if not snap:
        if _SNAP_IMPORT_OK:
            start_snapshot_scheduler_once()
        _render_scheduler_badge()

    target_key = _page_key()

    # ✅ 항상 pages를 먼저 "등록"해야 switch / navigation이 정상 동작함
    pages = [
        st.Page(
            "pages/01_production_status.py",
            title="생산 현황",
            icon="📊",
            default=(target_key == "01_production_status"),
        ),
        st.Page(
            "pages/02_production_info.py",
            title="생산 정보",
            icon="🧾",
            default=(target_key == "02_production_info"),
        ),
        st.Page(
            "pages/03_production_analysis.py",
            title="생산 분석",
            icon="📈",
            default=(target_key == "03_production_analysis"),
        ),
        st.Page(
            "pages/04_demon_health_check.py",
            title="프로그램 체크",
            icon="🖥️",
            default=(target_key == "04_demon_health_check"),
        ),
    ]

    nav = st.navigation(pages)
    nav.run()


if __name__ == "__main__":
    main()