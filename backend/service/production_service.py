from datetime import datetime
from sqlalchemy.orm import Session

from dbcore import dbsql
from calc.cal_shift import next_day_yyyymmdd
from calc.cal_pn import normalize_ct_map, apply_pn_ct
from calc.cal_passfail import build_passfail_table
from calc.cal_yield import calc_yield
from calc.cal_fail_analysis import analyze_first_second_fail
from calc.cal_oee import calc_standard_time_and_oee

def load_pn_ct_map(db: Session) -> dict:
    ct_rows = dbsql.select_ct_map(db)
    return normalize_ct_map(ct_rows)

def report_vision_passfail(db: Session, prod_day: str) -> dict:
    nd = next_day_yyyymmdd(prod_day)
    rows = dbsql.select_vision_history_for_prod_day(db, prod_day, nd)

    pn_map = load_pn_ct_map(db)
    rows = apply_pn_ct(rows, pn_map)

    table = build_passfail_table(prod_day, rows)
    return {"prod_day": prod_day, "day": table["DAY"], "night": table["NIGHT"]}

def report_yield(db: Session, prod_day: str) -> dict:
    nd = next_day_yyyymmdd(prod_day)
    rows = dbsql.select_yield_sources_for_prod_day(db, prod_day, nd)
    y = calc_yield(prod_day, rows)
    return {"prod_day": prod_day, "day": y["DAY"], "night": y["NIGHT"]}

def report_fct_fail(db: Session, prod_day: str) -> dict:
    nd = next_day_yyyymmdd(prod_day)
    rows = dbsql.select_fct_fail_rows_for_prod_day(db, prod_day, nd)

    pn_map = load_pn_ct_map(db)
    rows = apply_pn_ct(rows, pn_map)

    fa = analyze_first_second_fail(prod_day, rows)
    return {"prod_day": prod_day, "day": fa["DAY"], "night": fa["NIGHT"]}

def report_vision_fail(db: Session, prod_day: str) -> dict:
    nd = next_day_yyyymmdd(prod_day)
    rows = dbsql.select_vision_fail_rows_for_prod_day(db, prod_day, nd)

    pn_map = load_pn_ct_map(db)
    rows = apply_pn_ct(rows, pn_map)

    fa = analyze_first_second_fail(prod_day, rows)
    return {"prod_day": prod_day, "day": fa["DAY"], "night": fa["NIGHT"]}

def report_oee(db: Session, prod_day: str, planned_stop_sec: int, downtime_sec: int) -> dict:
    nd = next_day_yyyymmdd(prod_day)

    # 표준 생산 시간은 '양품수량' 기준 → Vision history에서 PASS 기반으로 계산하는 것이 가장 일관됨 :contentReference[oaicite:7]{index=7}
    rows = dbsql.select_vision_history_for_prod_day(db, prod_day, nd)

    pn_map = load_pn_ct_map(db)
    rows = apply_pn_ct(rows, pn_map)

    o = calc_standard_time_and_oee(prod_day, rows, planned_stop_sec, downtime_sec)
    return {"prod_day": prod_day, "day": o["DAY"], "night": o["NIGHT"]}

def status_mastersample(db: Session, prod_day: str) -> dict:
    nd = next_day_yyyymmdd(prod_day)
    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")

    # dbsql 함수 시그니처(3개 인자)에 맞춤
    ok = dbsql.select_mastersample_window(db, prod_day, nd)

    return {"prod_day": prod_day, "now": now_str, "mastersample_ok": ok}

