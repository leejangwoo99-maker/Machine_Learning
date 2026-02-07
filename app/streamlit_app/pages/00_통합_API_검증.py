# app/streamlit_app/pages/00_통합_API_검증.py
from __future__ import annotations

import os
import sys
from typing import Any
import ast

import pandas as pd
import requests
import streamlit as st
import plotly.graph_objects as go

_CUR = os.path.dirname(__file__)
_ROOT = os.path.dirname(_CUR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from api_client import (  # noqa: E402
    get_worker_info, post_worker_info_sync,
    get_email_list, post_email_list_sync,
    get_remark_info, post_remark_info_sync,
    get_mastersample,
    get_planned_today, post_planned_time_sync,
    get_non_operation_time, post_non_operation_time_sync,
    get_alarm_records,
    get_pd_board_check,
    get_report,
)

st.set_page_config(page_title="통합 API 검증", layout="wide")
st.title("통합 API 검증")

# 공통 파라미터
c1, c2 = st.columns(2)
with c1:
    day = st.text_input("prod_day/end_day", value="20260205", key="common_day")
with c2:
    shift = st.selectbox("shift_type", ["day", "night"], index=0, key="common_shift")

tabs = st.tabs([
    "2.worker_info",
    "3.email_list",
    "4.remark_info",
    "5.mastersample",
    "6.planned_time",
    "7.non_operation_time",
    "8.worst_case",
    "9.alarm_record",
    "10.pd_board_check",
    "11.reports",
])


def show_exc(e: Exception):
    if isinstance(e, requests.HTTPError) and e.response is not None:
        msg = e.response.text or ""
        low = msg.lower()
        if (
            "operationalerror" in low
            or "connection timed out" in low
            or "connection refused" in low
            or "db unavailable" in low
        ):
            st.error("DB 연결 실패: 서버 접속 불가. 네트워크/VPN/DB 상태를 확인하세요.")
        else:
            st.error(f"HTTP {e.response.status_code}: {msg[:500]}")
    else:
        st.error(str(e))


def to_df(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        if "rows" in data and isinstance(data["rows"], list):
            return pd.DataFrame(data["rows"])
        return pd.DataFrame([data])
    return pd.DataFrame([{"value": str(data)}])


def _norm_text(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and pd.isna(v):
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "nan"}:
        return ""
    return s


def _norm_email(v: Any) -> str:
    return _norm_text(v).lower()


def _hms_only(v: Any) -> str:
    s = _norm_text(v)
    return s.split(".", 1)[0] if "." in s else s


def parse_cosine_similarity(cell: Any) -> dict | None:
    if cell is None:
        return None
    if isinstance(cell, dict):
        return cell
    if isinstance(cell, str):
        txt = cell.strip()
        if not txt:
            return None
        try:
            return ast.literal_eval(txt)
        except Exception:
            return None
    return None


# 2) worker_info (GET + sync, 비밀번호 없음)
# PK 성격: (end_day, shift_type, worker_name) 기준으로 sync 데이터 구성
with tabs[0]:
    st.subheader("GET worker_info")
    if st.button("조회##w"):
        try:
            data = get_worker_info(day, shift)
            df = to_df(data)

            for c in ["end_day", "shift_type", "worker_name", "order_number"]:
                if c not in df.columns:
                    df[c] = ""

            df = df[["end_day", "shift_type", "worker_name", "order_number"]].copy()

            # 조회 컨텍스트(day/shift) 고정 표기
            if df.empty:
                df = pd.DataFrame([{
                    "end_day": day,
                    "shift_type": shift,
                    "worker_name": "",
                    "order_number": "",
                }])
            else:
                df["end_day"] = day
                df["shift_type"] = shift

            st.session_state["worker_df"] = df
            st.session_state["worker_df_edited"] = df.copy()
            st.dataframe(df, use_container_width=True)
        except Exception as e:
            show_exc(e)

    st.divider()
    st.subheader("POST worker_info/sync (전체 동기화)")
    st.caption("표에서 행 추가/삭제/수정 후 저장하면 DB와 동기화됩니다. (비밀번호 불필요)")

    if "worker_df" in st.session_state:
        edited_worker_df = st.data_editor(
            st.session_state["worker_df"],
            use_container_width=True,
            num_rows="dynamic",
            key="worker_editor",
            disabled=["end_day", "shift_type"],
        )
        st.session_state["worker_df_edited"] = edited_worker_df

    if st.button("저장##w_sync"):
        try:
            if "worker_df_edited" not in st.session_state:
                st.warning("먼저 [조회]로 목록을 불러오세요.")
            else:
                ed: pd.DataFrame = st.session_state["worker_df_edited"].copy()

                # (end_day, shift_type, worker_name) 기준 dedupe
                last: dict[tuple[str, str, str], dict] = {}
                for _, r in ed.iterrows():
                    end_day_v = _norm_text(r.get("end_day")) or day
                    shift_v = _norm_text(r.get("shift_type")) or shift
                    name_v = _norm_text(r.get("worker_name"))
                    ord_v = _norm_text(r.get("order_number")) or None

                    # 필수값
                    if not end_day_v or not shift_v or not name_v:
                        continue

                    k = (end_day_v, shift_v, name_v)
                    last[k] = {
                        "end_day": end_day_v,
                        "shift_type": shift_v,
                        "worker_name": name_v,
                        "order_number": ord_v,
                    }

                rows = [last[k] for k in sorted(last.keys())]
                if not rows:
                    st.warning("저장할 유효 행이 없습니다. worker_name은 필수입니다.")
                else:
                    out = post_worker_info_sync(rows)
                    st.success(out)

                    latest = to_df(get_worker_info(day, shift))
                    for c in ["end_day", "shift_type", "worker_name", "order_number"]:
                        if c not in latest.columns:
                            latest[c] = ""
                    latest = latest[["end_day", "shift_type", "worker_name", "order_number"]].copy()
                    latest["end_day"] = day
                    latest["shift_type"] = shift

                    st.session_state["worker_df"] = latest
                    st.session_state["worker_df_edited"] = latest.copy()
                    st.dataframe(latest, use_container_width=True)
        except Exception as e:
            show_exc(e)


# 3) email_list (GET + sync만)
with tabs[1]:
    st.subheader("GET email_list")
    c_get, c_pw = st.columns([1, 2])

    with c_get:
        if st.button("조회##e"):
            try:
                data = get_email_list()
                df = to_df(data)

                if "email_list" not in df.columns and "email" in df.columns:
                    df = df.rename(columns={"email": "email_list"})
                if "email_list" not in df.columns:
                    df["email_list"] = ""

                df = df[["email_list"]].copy()
                st.session_state["email_df"] = df
                st.session_state["email_df_edited"] = df.copy()
            except Exception as e:
                show_exc(e)

    with c_pw:
        email_admin_pw = st.text_input(
            "admin_password",
            type="password",
            value="",
            key="email_admin_pw",
        )

    st.divider()
    st.subheader("POST email_list/sync (전체 동기화)")
    st.caption("표에서 행 추가/삭제 후 저장하면 DB와 동기화됩니다. (중복/빈값 자동 정리)")

    if "email_df" in st.session_state:
        edited_email_df = st.data_editor(
            st.session_state["email_df"],
            use_container_width=True,
            num_rows="dynamic",
            key="email_editor",
        )
        st.session_state["email_df_edited"] = edited_email_df

    if st.button("저장##e_sync"):
        try:
            if "email_df_edited" not in st.session_state:
                st.warning("먼저 [조회]로 목록을 불러오세요.")
            elif not email_admin_pw:
                st.warning("admin_password를 입력해 주세요.")
            else:
                edited: pd.DataFrame = st.session_state["email_df_edited"].copy()
                if "email_list" not in edited.columns:
                    st.error("email_list 컬럼이 필요합니다.")
                else:
                    emails: list[str] = []
                    seen = set()
                    for v in edited["email_list"].tolist():
                        em = _norm_email(v)
                        if not em:
                            continue
                        if em not in seen:
                            seen.add(em)
                            emails.append(em)

                    out = post_email_list_sync(emails, email_admin_pw)
                    st.success(out)

                    latest = to_df(get_email_list())
                    if "email_list" not in latest.columns and "email" in latest.columns:
                        latest = latest.rename(columns={"email": "email_list"})
                    if "email_list" not in latest.columns:
                        latest["email_list"] = ""
                    st.session_state["email_df"] = latest[["email_list"]].copy()
                    st.session_state["email_df_edited"] = st.session_state["email_df"].copy()
        except Exception as e:
            show_exc(e)


# 4) remark_info (GET + sync만)
with tabs[2]:
    st.subheader("GET remark_info")
    c_get_r, c_pw_r = st.columns([1, 2])

    with c_get_r:
        if st.button("조회##r"):
            try:
                data = get_remark_info()
                df = to_df(data)

                if "barcode_information" not in df.columns and "key" in df.columns:
                    df = df.rename(columns={"key": "barcode_information"})

                if "barcode_information" not in df.columns:
                    df["barcode_information"] = ""
                if "pn" not in df.columns:
                    df["pn"] = ""
                if "remark" not in df.columns:
                    df["remark"] = ""

                df = df[["barcode_information", "pn", "remark"]].copy()
                st.session_state["remark_df"] = df
                st.session_state["remark_df_edited"] = df.copy()
            except Exception as e:
                show_exc(e)

    with c_pw_r:
        remark_admin_pw = st.text_input(
            "admin_password##remark",
            type="password",
            value="",
            key="remark_admin_pw",
        )

    st.divider()
    st.subheader("POST remark_info/sync (전체 동기화)")
    st.caption("표에서 행 추가/삭제/수정 후 저장하면 DB와 동기화됩니다. (barcode_information 기준)")

    if "remark_df" in st.session_state:
        edited_remark_df = st.data_editor(
            st.session_state["remark_df"],
            use_container_width=True,
            num_rows="dynamic",
            key="remark_editor",
        )
        st.session_state["remark_df_edited"] = edited_remark_df

    if st.button("저장##r_sync"):
        try:
            if "remark_df_edited" not in st.session_state:
                st.warning("먼저 [조회]로 목록을 불러오세요.")
            elif not remark_admin_pw:
                st.warning("admin_password를 입력해 주세요.")
            else:
                edited: pd.DataFrame = st.session_state["remark_df_edited"].copy()
                needed = {"barcode_information", "pn", "remark"}
                if not needed.issubset(set(edited.columns)):
                    st.error(f"필수 컬럼 누락: {needed - set(edited.columns)}")
                else:
                    last_by_barcode: dict[str, dict] = {}

                    for _, row in edited.iterrows():
                        barcode = _norm_text(row.get("barcode_information"))
                        if not barcode:
                            continue
                        last_by_barcode[barcode] = {
                            "barcode_information": barcode,
                            "pn": _norm_text(row.get("pn")) or None,
                            "remark": _norm_text(row.get("remark")) or None,
                        }

                    rows = [last_by_barcode[k] for k in sorted(last_by_barcode.keys())]
                    out = post_remark_info_sync(rows, remark_admin_pw)
                    st.success(out)

                    latest = to_df(get_remark_info())
                    if "barcode_information" not in latest.columns and "key" in latest.columns:
                        latest = latest.rename(columns={"key": "barcode_information"})
                    for c in ["barcode_information", "pn", "remark"]:
                        if c not in latest.columns:
                            latest[c] = ""
                    st.session_state["remark_df"] = latest[["barcode_information", "pn", "remark"]].copy()
                    st.session_state["remark_df_edited"] = st.session_state["remark_df"].copy()
        except Exception as e:
            show_exc(e)


# 5) mastersample
with tabs[3]:
    st.subheader("GET mastersample (e_mastersample_test)")
    if st.button("조회##m"):
        try:
            data = get_mastersample(day, shift)
            st.dataframe(to_df(data), use_container_width=True)
        except Exception as e:
            show_exc(e)


# 6) planned_time (GET + sync, 비밀번호 없음)
with tabs[4]:
    st.subheader("GET planned_time/today")
    if st.button("조회##p"):
        try:
            data = get_planned_today()
            df = to_df(data)

            for c in ["from_time", "to_time", "reason"]:
                if c not in df.columns:
                    df[c] = ""

            df["from_time"] = df["from_time"].map(_hms_only)
            df["to_time"] = df["to_time"].map(_hms_only)

            df = df[["from_time", "to_time", "reason"]].copy()
            st.session_state["planned_df"] = df
            st.session_state["planned_df_edited"] = df.copy()

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            show_exc(e)

    st.divider()
    st.subheader("POST planned_time/sync (전체 동기화)")
    st.caption("표에서 행 추가/삭제/수정 후 저장하면 DB와 동기화됩니다. (비밀번호 불필요, HH:MM:SS)")

    if "planned_df" in st.session_state:
        edited_planned_df = st.data_editor(
            st.session_state["planned_df"],
            use_container_width=True,
            num_rows="dynamic",
            key="planned_editor",
        )
        st.session_state["planned_df_edited"] = edited_planned_df

    if st.button("저장##p_sync"):
        try:
            if "planned_df_edited" not in st.session_state:
                st.warning("먼저 [조회]로 목록을 불러오세요.")
            else:
                edited: pd.DataFrame = st.session_state["planned_df_edited"].copy()
                needed = {"from_time", "to_time", "reason"}
                if not needed.issubset(set(edited.columns)):
                    st.error(f"필수 컬럼 누락: {needed - set(edited.columns)}")
                else:
                    last: dict[tuple[str, str], dict] = {}

                    for _, row in edited.iterrows():
                        ft = _hms_only(row.get("from_time"))
                        tt = _hms_only(row.get("to_time"))
                        if not ft or not tt:
                            continue
                        last[(ft, tt)] = {
                            "from_time": ft,
                            "to_time": tt,
                            "reason": _norm_text(row.get("reason")) or None,
                        }

                    rows = [last[k] for k in sorted(last.keys(), key=lambda x: (x[0], x[1]))]
                    out = post_planned_time_sync(rows)
                    st.success(out)

                    latest = to_df(get_planned_today())
                    for c in ["from_time", "to_time", "reason"]:
                        if c not in latest.columns:
                            latest[c] = ""
                    latest["from_time"] = latest["from_time"].map(_hms_only)
                    latest["to_time"] = latest["to_time"].map(_hms_only)
                    latest = latest[["from_time", "to_time", "reason"]].copy()

                    st.session_state["planned_df"] = latest
                    st.session_state["planned_df_edited"] = latest.copy()
                    st.dataframe(latest, use_container_width=True)
        except Exception as e:
            show_exc(e)


# 7) non_operation_time (GET + sync only, no password)
with tabs[5]:
    st.subheader("GET non_operation_time")
    if st.button("조회##nop_get"):
        try:
            data = get_non_operation_time(day, shift)
            df = to_df(data)

            for c in ["end_day", "station", "from_time", "to_time", "reason", "sparepart"]:
                if c not in df.columns:
                    df[c] = ""

            df = df[["end_day", "station", "from_time", "to_time", "reason", "sparepart"]].copy()

            st.session_state["nonop_df"] = df
            st.session_state["nonop_df_edited"] = df.copy()

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            show_exc(e)

    st.divider()
    st.subheader("POST non_operation_time/sync (전체 동기화)")
    st.caption("reason/sparepart 수정 후 저장하면 DB에 반영됩니다. (비밀번호 불필요)")

    if "nonop_df" in st.session_state:
        edited = st.data_editor(
            st.session_state["nonop_df"],
            use_container_width=True,
            num_rows="dynamic",
            key="nop_editor",
            disabled=["end_day", "station", "from_time", "to_time"],
        )
        st.session_state["nonop_df_edited"] = edited

    if st.button("저장##nop_sync"):
        try:
            if "nonop_df_edited" not in st.session_state:
                st.warning("먼저 [조회]로 목록을 불러오세요.")
            else:
                edited_df: pd.DataFrame = st.session_state["nonop_df_edited"].copy()
                needed = {"end_day", "station", "from_time", "to_time", "reason", "sparepart"}
                if not needed.issubset(set(edited_df.columns)):
                    st.error(f"필수 컬럼 누락: {needed - set(edited_df.columns)}")
                else:
                    last: dict[tuple[str, str, str, str], dict] = {}
                    for _, row in edited_df.iterrows():
                        end_day_v = _norm_text(row.get("end_day")) or day
                        station_v = _norm_text(row.get("station"))
                        from_v = _norm_text(row.get("from_time"))
                        to_v = _norm_text(row.get("to_time"))

                        if not station_v or not from_v or not to_v:
                            continue

                        k = (end_day_v, station_v, from_v, to_v)
                        last[k] = {
                            "end_day": end_day_v,
                            "station": station_v,
                            "from_time": from_v,
                            "to_time": to_v,
                            "reason": _norm_text(row.get("reason")) or None,
                            "sparepart": _norm_text(row.get("sparepart")) or None,
                        }

                    rows = [last[k] for k in sorted(last.keys())]
                    out = post_non_operation_time_sync(rows)
                    st.success(out)

                    latest = to_df(get_non_operation_time(day, shift))
                    for c in ["end_day", "station", "from_time", "to_time", "reason", "sparepart"]:
                        if c not in latest.columns:
                            latest[c] = ""
                    latest = latest[["end_day", "station", "from_time", "to_time", "reason", "sparepart"]].copy()

                    st.session_state["nonop_df"] = latest
                    st.session_state["nonop_df_edited"] = latest.copy()
                    st.dataframe(latest, use_container_width=True)
        except Exception as e:
            show_exc(e)


# 8) worst_case
with tabs[6]:
    st.subheader("GET worst_case (f_worst_case)")
    if st.button("조회##wc"):
        try:
            data = get_report("f_worst_case", day, shift)
            st.dataframe(to_df(data), use_container_width=True)
        except Exception as e:
            show_exc(e)


# 9) alarm_record
with tabs[7]:
    st.subheader("GET alarm_record/recent")
    st.caption("end_day 기준 조회")
    if st.button("조회##a"):
        try:
            data = get_alarm_records(day)
            st.info("‘권고’ 시 교체 바랍니다. Sparepart 알람 순서 : 준비, 권고, 긴급, 교체")
            st.dataframe(to_df(data), use_container_width=True)
        except Exception as e:
            show_exc(e)


# 10) pd_board_check
with tabs[8]:
    st.subheader("GET predictive/pd-board-check/{prod_day}")
    pd_day = st.text_input("prod_day", value=day, key="pd_day")
    if st.button("조회##pd"):
        try:
            data = get_pd_board_check(pd_day)
            df = to_df(data)

            df_show = df.drop(columns=["cosine_similarity"], errors="ignore")
            st.dataframe(df_show, use_container_width=True)

            if "cosine_similarity" in df.columns and not df.empty:
                series_df_list = []
                th_value = None

                for _, row in df.iterrows():
                    station = str(row.get("station", ""))
                    cs = parse_cosine_similarity(row.get("cosine_similarity"))
                    if not cs:
                        continue

                    x = cs.get("x", [])
                    y = cs.get("y", [])
                    th = cs.get("th", None)

                    if th is not None and th_value is None:
                        try:
                            th_value = float(th)
                        except Exception:
                            th_value = None

                    if (
                        isinstance(x, list)
                        and isinstance(y, list)
                        and len(x) == len(y)
                        and len(x) > 0
                        and station
                    ):
                        tmp = pd.DataFrame({"mmdd": x, station: y}).set_index("mmdd")
                        series_df_list.append(tmp)

                if series_df_list:
                    chart_df = pd.concat(series_df_list, axis=1)
                    chart_df = chart_df.loc[:, ~chart_df.columns.duplicated()]
                    chart_df = chart_df.apply(pd.to_numeric, errors="coerce")

                    if th_value is not None:
                        chart_df["COS_TH"] = float(th_value)

                    chart_df = chart_df.sort_index()
                    x_vals = chart_df.index.astype(str).tolist()

                    fig = go.Figure()

                    if "COS_TH" in chart_df.columns:
                        fig.add_trace(
                            go.Scatter(
                                x=x_vals,
                                y=chart_df["COS_TH"],
                                mode="lines",
                                name="COS_TH",
                                line=dict(width=3, dash="dash"),
                                hovertemplate="date=%{x}<br>COS_TH=%{y:.4f}<extra></extra>",
                            )
                        )

                    for col in chart_df.columns:
                        if col == "COS_TH":
                            continue
                        fig.add_trace(
                            go.Scatter(
                                x=x_vals,
                                y=chart_df[col],
                                mode="lines+markers",
                                name=col,
                                line=dict(width=3),
                                marker=dict(size=7),
                                hovertemplate=f"date=%{{x}}<br>{col}=%{{y:.4f}}<extra></extra>",
                            )
                        )

                    fig.update_layout(
                        title="Cosine Similarity to Abnormal Reference",
                        template="plotly_white",
                        height=560,
                        hovermode="x unified",
                        legend=dict(
                            orientation="h",
                            yanchor="bottom",
                            y=1.02,
                            xanchor="left",
                            x=0.0
                        ),
                        margin=dict(l=50, r=30, t=80, b=70),
                    )
                    fig.update_xaxes(
                        title_text="Date",
                        tickangle=0,
                        showgrid=True,
                        gridcolor="rgba(0,0,0,0.08)",
                    )
                    fig.update_yaxes(
                        title_text="Cosine Similarity",
                        showgrid=True,
                        gridcolor="rgba(0,0,0,0.08)",
                        zeroline=True,
                        zerolinecolor="rgba(0,0,0,0.20)",
                    )

                    st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})
                else:
                    st.warning("cosine_similarity 그래프 데이터가 없어 그래프를 표시하지 못했습니다.")
            else:
                st.warning("cosine_similarity 컬럼이 없어 그래프를 표시하지 못했습니다.")

        except Exception as e:
            show_exc(e)


# 11) reports
with tabs[9]:
    st.subheader("GET reports")
    report_name = st.selectbox(
        "report endpoint",
        [
            "a_station_final_amount",
            "b_station_percentage",
            "c_fct_step_1time",
            "c_fct_step_2time",
            "c_fct_step_3over",
            "d_vision_step_1time",
            "d_vision_step_2time",
            "d_vision_step_3over",
            "e_mastersample_test",
            "f_worst_case",
            "g_afa_wasted_time",
            "h_mes_wasted_time",
            "i_planned_stop_time",
            "i_non_time",
            "k_oee_line",
            "k_oee_station",
            "k_oee_total",
        ],
        index=0,
        key="report_name_select",
    )
    if st.button("조회##rep"):
        try:
            data = get_report(report_name, day, shift)
            df = to_df(data)

            if report_name == "b_station_percentage" and "updated_at" in df.columns:
                df = df.drop(columns=["updated_at"], errors="ignore")

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            show_exc(e)
