from __future__ import annotations

import streamlit as st
import pandas as pd

from api_client import get_report

st.set_page_config(page_title="5.mastersample", layout="wide")
st.title("5.mastersample")

c1, c2 = st.columns(2)
with c1:
    prod_day = st.text_input("prod_day", value="20260205")
with c2:
    shift_type = st.selectbox("shift_type", ["day", "night"], index=0)

if st.button("조회##mastersample"):
    try:
        data = get_report("e_mastersample_test", prod_day, shift_type)
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            df = pd.DataFrame(data.get("rows", [data]) if isinstance(data.get("rows"), list) else [data])
        else:
            df = pd.DataFrame([{"value": str(data)}])

        st.dataframe(df, use_container_width=True)
    except Exception as e:
        st.error(str(e))
