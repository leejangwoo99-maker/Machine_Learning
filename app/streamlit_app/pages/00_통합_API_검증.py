# app/streamlit_app/pages/00_통합_API_검증.py
from __future__ import annotations

import os
import sys
from typing import Any
import ast

import pandas as pd
import requests
import streamlit as st

_CUR = os.path.dirname(__file__)
_ROOT = os.path.dirname(_CUR)
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from api_client import (  # noqa: E402
    get_worker_info, post_worker_info,
    get_email_list, post_email_list,
    get_remark_info, post_remark_info,
    get_planned_today, post_planned_today,
    get_non_operation_time, post_non_operation_time,
    get_alarm_records,
    get_pd_board_check,
    get_report,
)

# /email_list/sync 직접 호출용 (api_client에 sync 함수가 없어도 동작)
API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
API_TIMEOUT_SEC = float(os.getenv("API_TIMEOUT_SEC", "30"))

st.set_page_config(page_title="통합 API 검증", layout="wide")
st.title("통합 API 검증")

# 공통 파라미터
c1, c2 = st.columns(2)
with c1:
    day = st.text_input("prod_day/end_day", value="20260205")
with c2:
    shift = st.selectbox("shift_type", ["day", "night"], index=0)

tabs = st.tabs([
    "2.worker_info",
    "3.email_list",
    "4.remark_info",
    "6.planned_time",
    "7.non_operation_time",
    "9.alarm_record",
    "10.pd_board_check",
    "11.reports",
])


def show_exc(e: Exception):
    if isinstance(e, requests.HTTPError) and e.response is not None:
        st.error(f"HTTP {e.response.status_code}: {e.response.text}")
    else:
        st.error(str(e))


def to_df(data: Any) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list):
        return pd.DataFrame(data)
    if isinstance(data, dict):
        # {"rows":[...]} 구조 대응
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
    s = _norm_text(v).lower()
    return s


def parse_cosine_similarity(cell: Any) -> dict | None:
    """
    cosine_similarity 컬럼이 dict 또는 문자열(dict 형태)일 수 있어 안전 파싱
    예: {"th":0.7,"type":"timeseries","x":["20260203",...],"y":[...]}
    """
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


def _post_email_sync(emails: list[str], admin_password: str):
    """
    /email_list/sync 직접 호출
    헤더: X-ADMIN-PASS
    바디: {"emails":[...]}
    """
    if not admin_password:
        raise ValueError("admin_password를 입력해 주세요.")

    url = f"{API_BASE_URL.rstrip('/')}/email_list/sync"
    headers = {"X-ADMIN-PASS": admin_password}
    body = {"emails": emails}
    r = requests.post(url, json=body, headers=headers, timeout=API_TIMEOUT_SEC)
    r.raise_for_status()
    ct = r.headers.get("content-type", "")
    if "application/json" in ct:
        return r.json()
    return r.text


# 2) worker_info
with tabs[0]:
    st.subheader("GET worker_info")
    if st.button("조회##w"):
        try:
            data = get_worker_info(day, shift)
            st.dataframe(to_df(data), use_container_width=True)
        except Exception as e:
            show_exc(e)

    st.subheader("POST worker_info")
    worker_name = st.text_input("worker_name", value="홍길동")
    order_number = st.text_input("order_number", value="")
    if st.button("저장##w"):
        try:
            out = post_worker_info(day, shift, worker_name, order_number=order_number)
            st.success(out)
        except Exception as e:
            show_exc(e)


# 3) email_list
with tabs[1]:
    st.subheader("GET email_list")
    c_get, c_pw = st.columns([1, 2])
    with c_get:
        if st.button("조회##e"):
            try:
                data = get_email_list()
                df = to_df(data)

                # 서버 응답 컬럼명 편차 대응: email / email_list
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
        email_admin_pw = st.text_input("admin_password", type="password", value="", key="email_admin_pw")

    st.subheader("POST email_list (단건)")
    email = st.text_input("email", value="hong.gildong@aptiv.com")
    if st.button("저장##e"):
        try:
            out = post_email_list(email, admin_password=email_admin_pw)
            st.success(out)
        except Exception as e:
            show_exc(e)

    st.divider()
    st.subheader("전체 목록 편집/동기화 저장")
    st.caption("표에서 행 추가/삭제 후 저장하면 DB와 동기화됩니다. (같은 값 중복은 자동 정리)")

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

                    out = _post_email_sync(emails, email_admin_pw)
                    st.success(out)

                    # 저장 후 재조회
                    latest = to_df(get_email_list())
                    if "email_list" not in latest.columns and "email" in latest.columns:
                        latest = latest.rename(columns={"email": "email_list"})
                    if "email_list" not in latest.columns:
                        latest["email_list"] = ""
                    st.session_state["email_df"] = latest[["email_list"]].copy()
        except Exception as e:
            show_exc(e)


# 4) remark_info
with tabs[2]:
    st.subheader("GET remark_info")
    if st.button("조회##r"):
        try:
            data = get_remark_info()
            st.dataframe(to_df(data), use_container_width=True)
        except Exception as e:
            show_exc(e)

    st.subheader("POST remark_info")
    key = st.text_input("key (예: J)", value="J")
    pn = st.text_input("pn", value="TEST_PN")
    remark = st.text_input("remark", value="테스트 비고")
    remark_admin_pw = st.text_input("admin_password##remark", type="password", value="")
    if st.button("저장##r"):
        try:
            out = post_remark_info(key, pn, remark, admin_password=remark_admin_pw)
            st.success(out)
        except Exception as e:
            show_exc(e)


# 6) planned_time
with tabs[3]:
    st.subheader("GET planned_time/today")
    if st.button("조회##p"):
        try:
            data = get_planned_today()
            st.dataframe(to_df(data), use_container_width=True)
        except Exception as e:
            show_exc(e)

    st.subheader("POST planned_time/today")
    p_from = st.text_input("from_time (HH:MM:SS.xx)", value="10:00:00.00", key="p_from")
    p_to = st.text_input("to_time (HH:MM:SS.xx)", value="10:10:00.00", key="p_to")
    p_reason = st.text_input("reason", value="계획정지", key="p_reason")
    if st.button("저장##p"):
        try:
            out = post_planned_today(p_from, p_to, p_reason, end_day=day)
            st.success(out)
        except Exception as e:
            show_exc(e)


# 7) non_operation_time
with tabs[4]:
    st.subheader("GET non_operation_time")
    if st.button("조회##n"):
        try:
            data = get_non_operation_time(day, shift)
            df = to_df(data)

            if not df.empty:
                st.session_state["nonop_df"] = df.copy()

            if "nonop_df" in st.session_state:
                base_df = st.session_state["nonop_df"].copy()

                # reason/sparepart 컬럼 없으면 생성
                if "reason" not in base_df.columns:
                    base_df["reason"] = ""
                if "sparepart" not in base_df.columns:
                    base_df["sparepart"] = ""

                edited = st.data_editor(
                    base_df,
                    use_container_width=True,
                    num_rows="fixed",
                    key="nonop_editor",
                    disabled=[c for c in base_df.columns if c not in ["reason", "sparepart"]],
                )
                st.session_state["nonop_df_edited"] = edited
        except Exception as e:
            show_exc(e)

    st.write("선택/수정된 행 저장 (reason, sparepart만 반영)")
    if st.button("저장##n_table"):
        try:
            if "nonop_df_edited" not in st.session_state:
                st.warning("먼저 조회 후 reason/sparepart를 수정해 주세요.")
            else:
                edited_df: pd.DataFrame = st.session_state["nonop_df_edited"]
                required_cols = {"end_day", "station", "from_time", "to_time"}
                if not required_cols.issubset(set(edited_df.columns)):
                    st.error(f"필수 컬럼 누락: {required_cols - set(edited_df.columns)}")
                else:
                    ok_count = 0
                    err_count = 0
                    for _, row in edited_df.iterrows():
                        try:
                            post_non_operation_time(
                                end_day=str(row["end_day"]),
                                station=str(row["station"]),
                                from_time=str(row["from_time"]),
                                to_time=str(row["to_time"]),
                                reason=_norm_text(row.get("reason")),
                                sparepart=_norm_text(row.get("sparepart")),
                            )
                            ok_count += 1
                        except Exception:
                            err_count += 1
                    st.success(f"저장 완료: {ok_count}건, 실패: {err_count}건")
                    st.info("저장 후 [조회]를 다시 눌러 반영값을 확인하세요.")
        except Exception as e:
            show_exc(e)

    st.divider()
    st.subheader("단건 저장 (기존 방식)")
    n_station = st.selectbox("station", ["FCT1", "FCT2", "FCT3", "FCT4", "Vision1", "Vision2"], key="n_station")
    n_from = st.text_input("from_time", value="20:20:43.75", key="n_from")
    n_to = st.text_input("to_time", value="20:20:58.34", key="n_to")
    n_reason = st.text_input("reason", value="카메라 청소", key="n_reason")
    n_sp = st.text_input("sparepart", value="lens_module", key="n_sp")
    if st.button("저장##n"):
        try:
            out = post_non_operation_time(day, n_station, n_from, n_to, n_reason, n_sp)
            st.success(out)
        except Exception as e:
            show_exc(e)


# 9) alarm_record
with tabs[5]:
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
with tabs[6]:
    st.subheader("GET predictive/pd-board-check/{prod_day}")
    pd_day = st.text_input("prod_day", value=day, key="pd_day")
    if st.button("조회##pd"):
        try:
            data = get_pd_board_check(pd_day)
            df = to_df(data)

            # 테이블: cosine_similarity 제외
            df_show = df.drop(columns=["cosine_similarity"], errors="ignore")
            st.dataframe(df_show, use_container_width=True)

            # 그래프: cosine_similarity 내부 x/y/th 사용
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

                    if isinstance(x, list) and isinstance(y, list) and len(x) == len(y) and len(x) > 0:
                        tmp = pd.DataFrame({"mmdd": x, station: y})
                        tmp = tmp.set_index("mmdd")
                        series_df_list.append(tmp)

                if series_df_list:
                    chart_df = pd.concat(series_df_list, axis=1)
                    chart_df = chart_df.loc[:, ~chart_df.columns.duplicated()]

                    # threshold 라인 추가
                    if th_value is not None:
                        chart_df["COS_TH"] = th_value

                    st.caption("Cosine Similarity to Abnormal Reference")
                    st.line_chart(chart_df, use_container_width=True)
                else:
                    st.warning("cosine_similarity 그래프 데이터가 없어 그래프를 표시하지 못했습니다.")
            else:
                st.warning("cosine_similarity 컬럼이 없어 그래프를 표시하지 못했습니다.")

        except Exception as e:
            show_exc(e)


# 11) reports
with tabs[7]:
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
    )
    if st.button("조회##rep"):
        try:
            data = get_report(report_name, day, shift)
            df = to_df(data)

            # 요청사항: b_station_percentage는 updated_at 컬럼 제거
            if report_name == "b_station_percentage" and "updated_at" in df.columns:
                df = df.drop(columns=["updated_at"], errors="ignore")

            st.dataframe(df, use_container_width=True)
        except Exception as e:
            show_exc(e)
