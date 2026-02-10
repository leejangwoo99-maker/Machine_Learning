# app/routers/events.py
from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from fastapi import APIRouter

router = APIRouter(tags=["0.events"])

KST = ZoneInfo("Asia/Seoul")


def _now_kst() -> datetime:
    return datetime.now(tz=KST)


def _shift_type_by_now(now_kst: datetime) -> str:
    """
    ?щ궡 湲곗?:
    - day   : 08:30:00 ~ 20:29:59
    - night : 洹???
    """
    hhmmss = now_kst.strftime("%H%M%S")
    return "day" if "083000" <= hhmmss <= "202959" else "night"


@router.get("/events")
def get_events():
    """
    ??쒕낫???ㅻ뜑/?뺤콉 ?덈궡??硫뷀? endpoint.
    - ?ㅼ젣 ?뚮엺 popup ?곗씠?곕뒗 /alarm_record/recent?먯꽌 議고쉶
    - report 怨꾩뿴 ?뚮씪誘명꽣 洹쒖튃 怨듭?
    - POST ?덉슜 ?뺤콉 怨듭?
    """
    now_kst = _now_kst()
    prod_day = now_kst.strftime("%Y%m%d")
    shift_type = _shift_type_by_now(now_kst)

    return {
        "ok": True,
        "server_time_kst": now_kst.isoformat(),
        "prod_day": prod_day,
        "shift_type": shift_type,
        "title": f"{prod_day} {shift_type} ?앹궛 ?꾪솴",
        "post_policy": {
            "allowed_post_only": [
                "/worker_info/sync",
                "/email_list/sync",
                "/remark_info/sync",
                "/planned_time/sync",
                "/non_operation_time/sync",
            ]
        },
        "report_policy": {
            "base_prefix": "/report",
            "query_required": ["shift_type=day|night"],
            "path_required": ["{prod_day}=YYYYMMDD"],
            "must_have_get": [
                "/report/b_station_percentage/{prod_day}",
                "/report/a_station_final_amount/{prod_day}",
                "/report/c_fct_step_1time/{prod_day}",
                "/report/c_fct_step_2time/{prod_day}",
                "/report/c_fct_step_3over/{prod_day}",
                "/report/d_vision_step_1time/{prod_day}",
                "/report/d_vision_step_2time/{prod_day}",
                "/report/d_vision_step_3over/{prod_day}",
                "/report/k_oee_line/{prod_day}",
                "/report/k_oee_station/{prod_day}",
                "/report/k_oee_total/{prod_day}",
                "/report/f_worst_case/{prod_day}",
                "/report/g_afa_wasted_time/{prod_day}",
                "/report/h_mes_wasted_time/{prod_day}",
                "/report/i_planned_stop_time/{prod_day}",
                "/report/i_non_time/{prod_day}",
            ],
        },
        "alarm_policy": {
            "source": "/alarm_record/recent",
            "type_alarm_values": ["沅뚭퀬", "湲닿툒", "援먯껜"],
            "popup_message_templates": {
                "沅뚭퀬": "{station}, {sparepart} 援먯껜 沅뚭퀬 ?쒕┰?덈떎.",
                "湲닿툒": "{station}, {sparepart} 援먯껜 湲닿툒?⑸땲??",
                "援먯껜": "{station}, {sparepart} 援먯껜 ??대컢??吏?ъ뒿?덈떎.",
            },
            "close_action": "?뺤씤 踰꾪듉 ?대┃ ???ロ옒",
        },
    }
