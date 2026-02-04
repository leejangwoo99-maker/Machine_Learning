from __future__ import annotations

from fastapi import APIRouter, Depends
from sqlalchemy.engine import Engine

from app.core.db import make_engine
from app.services import report_svc

router = APIRouter(prefix="/report", tags=["11.reports_i_daily_report"])


def get_engine() -> Engine:
    return make_engine()


# 9) 시간대별 PASS/FAIL 집계
@router.get("/a_station_final_amount/{prod_day}", response_model=list[dict])
def a_station_final_amount(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="a_station_day_daily_final_amount",
        night_table="a_station_night_daily_final_amount",
        prod_day_col="prod_day",
    )


# 7) 검사기별 PASS/total/PASS_pct
@router.get("/b_station_percentage/{prod_day}", response_model=list[dict])
def b_station_percentage(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="b_station_day_daily_percentage",
        night_table="b_station_night_daily_percentage",
        prod_day_col="prod_day",
    )


# 7) FCT fail step_description (1/2/3over)
@router.get("/c_fct_step_1time/{prod_day}", response_model=list[dict])
def c_fct_step_1time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="c_1time_step_decription_day_daily",
        night_table="c_1time_step_decription_night_daily",
        prod_day_col="prod_day",
    )


@router.get("/c_fct_step_2time/{prod_day}", response_model=list[dict])
def c_fct_step_2time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="c_2time_step_decription_day_daily",
        night_table="c_2time_step_decription_night_daily",
        prod_day_col="prod_day",
    )


@router.get("/c_fct_step_3over/{prod_day}", response_model=list[dict])
def c_fct_step_3over(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="c_3time_over_step_decription_day_daily",
        night_table="c_3time_over_step_decription_night_daily",
        prod_day_col="prod_day",
    )


# 8) Vision fail step_description (1/2/3over)
@router.get("/d_vision_step_1time/{prod_day}", response_model=list[dict])
def d_vision_step_1time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="d_vs_1time_step_decription_day_daily",
        night_table="d_vs_1time_step_decription_night_daily",
        prod_day_col="prod_day",
    )


@router.get("/d_vision_step_2time/{prod_day}", response_model=list[dict])
def d_vision_step_2time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="d_vs_2time_step_decription_day_daily",
        night_table="d_vs_2time_step_decription_night_daily",
        prod_day_col="prod_day",
    )


@router.get("/d_vision_step_3over/{prod_day}", response_model=list[dict])
def d_vision_step_3over(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="d_vs_3time_over_step_decription_day_daily",
        night_table="d_vs_3time_over_step_decription_night_daily",
        prod_day_col="prod_day",
    )


# 11) OEE (line/station/total)
@router.get("/k_oee_line/{prod_day}", response_model=list[dict])
def k_oee_line(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="k_line_oee_day_daily",
        night_table="k_line_oee_night_daily",
        prod_day_col="prod_day",
    )


@router.get("/k_oee_station/{prod_day}", response_model=list[dict])
def k_oee_station(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="k_station_oee_day_daily",
        night_table="k_station_oee_night_daily",
        prod_day_col="prod_day",
    )


@router.get("/k_oee_total/{prod_day}", response_model=list[dict])
def k_oee_total(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="k_total_oee_day_daily",
        night_table="k_total_oee_night_daily",
        prod_day_col="prod_day",
    )


# 15) worst case
@router.get("/f_worst_case/{prod_day}", response_model=list[dict])
def f_worst_case(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="f_worst_case_day_daily",
        night_table="f_worst_case_night_daily",
        prod_day_col="prod_day",
    )


# 16) AFA wasted time
@router.get("/g_afa_wasted_time/{prod_day}", response_model=list[dict])
def g_afa_wasted_time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="g_afa_wasted_time_day_daily",
        night_table="g_afa_wasted_time_night_daily",
        prod_day_col="prod_day",
    )


# 17) MES wasted time
@router.get("/h_mes_wasted_time/{prod_day}", response_model=list[dict])
def h_mes_wasted_time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="h_mes_wasted_time_day_daily",
        night_table="h_mes_wasted_time_night_daily",
        prod_day_col="prod_day",
    )


# ✅ [추가] Mastersample 완료 출력
# - 스키마: i_daily_report
# - 테이블:
#   - day   : e_mastersample_test_day_daily
#   - night : e_mastersample_test_night_daily
# - 컬럼: prod_day, shift_type, Mastersample, first_time, updated_at (updated_at은 제외하고 내려줌)
@router.get("/e_mastersample_test/{prod_day}", response_model=list[dict])
def e_mastersample_test(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="e_mastersample_test_day_daily",
        night_table="e_mastersample_test_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )
