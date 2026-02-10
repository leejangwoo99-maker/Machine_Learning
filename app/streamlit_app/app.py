# app/streamlit_app/app.py
from __future__ import annotations

import streamlit as st

st.set_page_config(
    page_title="생산 현황",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# 페이지 파일명은 영문/숫자/언더스코어만 쓰는 것을 강력 권장
pg = st.navigation(
    [
        st.Page("pages/01_production_status.py", title="생산현황", icon="📊", default=True),
        st.Page("pages/02_production_info.py", title="생산 정보", icon="🧾"),
        st.Page("pages/03_production_analysis.py", title="생산 분석", icon="📈"),
    ]
)
pg.run()
