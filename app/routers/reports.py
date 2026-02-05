from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.engine import Engine

from app.core.db import make_engine
from app.services import report_svc

router = APIRouter(prefix="/report", tags=["11.reports_i_daily_report"])


def get_engine() -> Engine:
    return make_engine()


def _safe_fetch(
    engine: Engine,
    prod_day: str,
    shift_type: str,
    *,
    day_table: str,
    night_table: str,
    prod_day_col: str = "prod_day",
    exclude_cols: list[str] | None = None,
):
    try:
        return report_svc.fetch_report_rows_by_shift(
            engine,
            prod_day,
            shift_type,
            day_table=day_table,
            night_table=night_table,
            prod_day_col=prod_day_col,
            exclude_cols=exclude_cols,
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


# 9) 시간대별 PASS/FAIL 집계
@router.get("/a_station_final_amount/{prod_day}", response_model=list[dict])
def a_station_final_amount(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="a_station_day_daily_final_amount",
        night_table="a_station_night_daily_final_amount",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# 7) 검사기별 PASS/total/PASS_pct
@router.get("/b_station_percentage/{prod_day}", response_model=list[dict])
def b_station_percentage(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="b_station_day_daily_percentage",
        night_table="b_station_night_daily_percentage",
        prod_day_col="prod_day",
        # 필요 시 updated_at 제외하려면 아래 주석 해제
        # exclude_cols=["updated_at"],
    )


# 7) FCT fail step_description (1/2/3over)
@router.get("/c_fct_step_1time/{prod_day}", response_model=list[dict])
def c_fct_step_1time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="c_1time_step_decription_day_daily",
        night_table="c_1time_step_decription_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


@router.get("/c_fct_step_2time/{prod_day}", response_model=list[dict])
def c_fct_step_2time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="c_2time_step_decription_day_daily",
        night_table="c_2time_step_decription_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


@router.get("/c_fct_step_3over/{prod_day}", response_model=list[dict])
def c_fct_step_3over(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="c_3time_over_step_decription_day_daily",
        night_table="c_3time_over_step_decription_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# 8) Vision fail step_description (1/2/3over)
@router.get("/d_vision_step_1time/{prod_day}", response_model=list[dict])
def d_vision_step_1time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="d_vs_1time_step_decription_day_daily",
        night_table="d_vs_1time_step_decription_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


@router.get("/d_vision_step_2time/{prod_day}", response_model=list[dict])
def d_vision_step_2time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="d_vs_2time_step_decription_day_daily",
        night_table="d_vs_2time_step_decription_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


@router.get("/d_vision_step_3over/{prod_day}", response_model=list[dict])
def d_vision_step_3over(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="d_vs_3time_over_step_decription_day_daily",
        night_table="d_vs_3time_over_step_decription_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# 11) OEE (line/station/total)
@router.get("/k_oee_line/{prod_day}", response_model=list[dict])
def k_oee_line(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="k_line_oee_day_daily",
        night_table="k_line_oee_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


@router.get("/k_oee_station/{prod_day}", response_model=list[dict])
def k_oee_station(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="k_station_oee_day_daily",
        night_table="k_station_oee_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


@router.get("/k_oee_total/{prod_day}", response_model=list[dict])
def k_oee_total(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="k_total_oee_day_daily",
        night_table="k_total_oee_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# 15) worst case
@router.get("/f_worst_case/{prod_day}", response_model=list[dict])
def f_worst_case(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="f_worst_case_day_daily",
        night_table="f_worst_case_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# 16) AFA wasted time
@router.get("/g_afa_wasted_time/{prod_day}", response_model=list[dict])
def g_afa_wasted_time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="g_afa_wasted_time_day_daily",
        night_table="g_afa_wasted_time_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# 17) MES wasted time
@router.get("/h_mes_wasted_time/{prod_day}", response_model=list[dict])
def h_mes_wasted_time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="h_mes_wasted_time_day_daily",
        night_table="h_mes_wasted_time_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# e) Mastersample 완료 출력
@router.get("/e_mastersample_test/{prod_day}", response_model=list[dict])
def e_mastersample_test(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return _safe_fetch(
        engine, prod_day, shift_type,
        day_table="e_mastersample_test_day_daily",
        night_table="e_mastersample_test_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at"],
    )


# ✅ [추가] 총 계획 정지 시간
@router.get("/i_planned_stop_time/{prod_day}", response_model=list[dict])
def i_planned_stop_time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="i_planned_stop_time_day_daily",
        night_table="i_planned_stop_time_night_daily",
        prod_day_col="prod_day",
        exclude_cols=["updated_at", "total_planned_time"],  # total_planned_time 컬럼 제외
    )


# ✅ [추가] 총 비가동 시간
@router.get("/i_non_time/{prod_day}", response_model=list[dict])
def i_non_time(prod_day: str, shift_type: str, engine: Engine = Depends(get_engine)):
    return report_svc.fetch_report_rows_by_shift(
        engine, prod_day, shift_type,
        day_table="i_non_time_day_daily",
        night_table="i_non_time_night_daily",
        prod_day_col="prod_day",
        exclude_cols=[
            "updated_at",
            "vision1_non_time",
            "vision2_non_time",
            "total_vision_non_time",
        ],
    )


