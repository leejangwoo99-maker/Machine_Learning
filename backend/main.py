from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text

from dbcore.session import get_db
from dbcore import dbwrite
from service.production_service import (
    report_vision_passfail, report_yield, report_fct_fail, report_vision_fail,
    report_oee, status_mastersample
)

app = FastAPI(title="SmartFactory API", version="0.1.0")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/db/ping")
def db_ping(db: Session = Depends(get_db)):
    v = db.execute(text("SELECT 1")).scalar()
    return {"db": "ok", "select_1": v}

# 3) Vision PASS/FAIL 시간대 테이블 :contentReference[oaicite:8]{index=8}
@app.get("/report/vision/passfail/{prod_day}")
def api_vision_passfail(prod_day: str, db: Session = Depends(get_db)):
    return report_vision_passfail(db, prod_day)

# 4) 양품률(FCT1~4, Vision1~2, VisionTotal) :contentReference[oaicite:9]{index=9}
@app.get("/report/yield/{prod_day}")
def api_yield(prod_day: str, db: Session = Depends(get_db)):
    return report_yield(db, prod_day)

# 5) FCT 1/2회 FAIL 분석 :contentReference[oaicite:10]{index=10}
@app.get("/report/fail/fct/{prod_day}")
def api_fct_fail(prod_day: str, db: Session = Depends(get_db)):
    return report_fct_fail(db, prod_day)

# 6) Vision 1/2회 FAIL 분석 :contentReference[oaicite:11]{index=11}
@app.get("/report/fail/vision/{prod_day}")
def api_vision_fail(prod_day: str, db: Session = Depends(get_db)):
    return report_vision_fail(db, prod_day)

# 7~12) 표준 생산 시간 + OEE :contentReference[oaicite:12]{index=12}
@app.get("/report/oee/{prod_day}")
def api_oee(
    prod_day: str,
    planned_stop_sec: int = Query(0, ge=0),
    downtime_sec: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    return report_oee(db, prod_day, planned_stop_sec, downtime_sec)

# 13) Mastersample All OK 메시지 존재 여부 :contentReference[oaicite:13]{index=13}
@app.get("/status/mastersample/{prod_day}")
def api_mastersample(prod_day: str, db: Session = Depends(get_db)):
    return status_mastersample(db, prod_day)

# 2) pn/ct 프론트 수정(비번 필요) :contentReference[oaicite:14]{index=14}
@app.post("/admin/ct-map/upsert")
def api_ctmap_upsert(
    key_char: str,
    pn: str,
    ct: float,
    password: str,
    db: Session = Depends(get_db),
):
    try:
        dbwrite.upsert_ct_map_row(db, password=password, key_char=key_char, pn=pn, ct=ct)
        return {"ok": True}
    except PermissionError as e:
        raise HTTPException(status_code=401, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
