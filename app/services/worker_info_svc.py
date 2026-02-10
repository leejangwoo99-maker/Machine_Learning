from __future__ import annotations
from typing import Any, Dict, List
from sqlalchemy import text
from sqlalchemy.orm import Session

SCHEMA="g_production_film"; TABLE="worker_info"

def ensure_unique_index(db: Session)->None:
    db.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_info_day_shift_worker ON g_production_film.worker_info (end_day, shift_type, worker_name)"))

def list_rows(db: Session, end_day: str, shift_type: str)->List[Dict[str,Any]]:
    rows=db.execute(text(
        "SELECT REPLACE(end_day::text,'-','') AS end_day, LOWER(shift_type::text) AS shift_type, worker_name, order_number "
        "FROM g_production_film.worker_info "
        "WHERE REPLACE(end_day::text,'-','')=:d AND LOWER(shift_type::text)=:s ORDER BY worker_name"
    ), {"d":end_day,"s":shift_type}).mappings().all()
    return [dict(r) for r in rows]

def upsert_one(db: Session, row: Dict[str,Any])->None:
    ensure_unique_index(db)
    db.execute(text(
        "INSERT INTO g_production_film.worker_info (end_day,shift_type,worker_name,order_number) "
        "VALUES (:end_day,:shift_type,:worker_name,:order_number) "
        "ON CONFLICT (end_day,shift_type,worker_name) DO UPDATE SET order_number=EXCLUDED.order_number"
    ), row)

def sync_rows(db: Session, rows: List[Dict[str,Any]])->Dict[str,int|bool]:
    ensure_unique_index(db)
    dedup={}
    for r in rows:
        dedup[(r["end_day"],r["shift_type"],r["worker_name"])]=r
    groups={}
    for _,v in dedup.items():
        groups.setdefault((v["end_day"],v["shift_type"]), []).append(v)
    ins=upd=dele=0
    for (d,s), target_rows in groups.items():
        current=list_rows(db,d,s)
        cmap={(x["end_day"],x["shift_type"],x["worker_name"]):x for x in current}
        tmap={(x["end_day"],x["shift_type"],x["worker_name"]):x for x in target_rows}
        ckeys=set(cmap); tkeys=set(tmap)
        for k in sorted(tkeys-ckeys):
            db.execute(text("INSERT INTO g_production_film.worker_info (end_day,shift_type,worker_name,order_number) VALUES (:end_day,:shift_type,:worker_name,:order_number)"), tmap[k]); ins+=1
        for k in sorted(ckeys&tkeys):
            if (cmap[k].get("order_number") or "") != (tmap[k].get("order_number") or ""):
                db.execute(text(
                    "UPDATE g_production_film.worker_info SET order_number=:order_number "
                    "WHERE REPLACE(end_day::text,'-','')=:end_day AND LOWER(shift_type::text)=:shift_type AND worker_name=:worker_name"
                ), tmap[k]); upd+=1
        for k in sorted(ckeys-tkeys):
            d2,s2,w2=k
            db.execute(text(
                "DELETE FROM g_production_film.worker_info WHERE REPLACE(end_day::text,'-','')=:d AND LOWER(shift_type::text)=:s AND worker_name=:w"
            ), {"d":d2,"s":s2,"w":w2}); dele+=1
    return {"ok":True,"inserted":ins,"updated":upd,"deleted":dele,"total_after":len(dedup)}
