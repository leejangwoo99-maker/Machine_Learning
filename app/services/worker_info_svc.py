# app/services/worker_info_svc.py
from __future__ import annotations

from typing import Any, Dict, List, Tuple
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.schemas.worker_info import WorkerInfoIn, WorkerInfoSyncIn

SCHEMA = "g_production_film"
TABLE = "worker_info"

COL_END_DAY = "end_day"
COL_SHIFT = "shift_type"
COL_WORKER = "worker_name"
COL_ORDER = "order_number"


def _norm(v: Any) -> str:
    if v is None:
        return ""
    s = str(v).strip()
    if s.lower() in {"none", "nan"}:
        return ""
    return s


def ensure_worker_info_unique_index(db: Session) -> None:
    """
    (end_day, shift_type, worker_name) 유니크 인덱스 보장
    """
    db.execute(
        text(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_info_day_shift_worker
            ON {SCHEMA}.{TABLE} ({COL_END_DAY}, {COL_SHIFT}, {COL_WORKER})
        """)
    )


def list_worker_info(db: Session, end_day: str, shift_type: str) -> List[Dict[str, Any]]:
    rows = db.execute(
        text(f"""
            SELECT
                {COL_END_DAY}   AS end_day,
                {COL_SHIFT}     AS shift_type,
                {COL_WORKER}    AS worker_name,
                {COL_ORDER}     AS order_number
            FROM {SCHEMA}.{TABLE}
            WHERE {COL_END_DAY} = :end_day
              AND {COL_SHIFT} = :shift_type
            ORDER BY {COL_WORKER}
        """),
        {"end_day": end_day, "shift_type": shift_type},
    ).mappings().all()
    return [dict(r) for r in rows]


def upsert_worker_info(db: Session, body: WorkerInfoIn) -> None:
    ensure_worker_info_unique_index(db)

    end_day = _norm(body.end_day)
    shift_type = _norm(body.shift_type)
    worker_name = _norm(body.worker_name)
    order_number = _norm(body.order_number) if body.order_number is not None else ""

    if not end_day or not shift_type or not worker_name:
        raise ValueError("end_day, shift_type, worker_name are required")

    db.execute(
        text(f"""
            INSERT INTO {SCHEMA}.{TABLE}
            ({COL_END_DAY}, {COL_SHIFT}, {COL_WORKER}, {COL_ORDER})
            VALUES (:end_day, :shift_type, :worker_name, :order_number)
            ON CONFLICT ({COL_END_DAY}, {COL_SHIFT}, {COL_WORKER})
            DO UPDATE SET
                {COL_ORDER} = EXCLUDED.{COL_ORDER}
        """),
        {
            "end_day": end_day,
            "shift_type": shift_type,
            "worker_name": worker_name,
            "order_number": (order_number or None),
        },
    )


def sync_worker_info(db: Session, body: WorkerInfoSyncIn) -> Dict[str, Any]:
    ensure_worker_info_unique_index(db)

    # 1) rows 정규화 + 키 dedupe(마지막 값 우선)
    dedup: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
    for r in body.rows:
        end_day = _norm(r.end_day)
        shift_type = _norm(r.shift_type)
        worker_name = _norm(r.worker_name)
        order_number = _norm(r.order_number) if r.order_number is not None else ""

        if not end_day or not shift_type or not worker_name:
            continue

        k = (end_day, shift_type, worker_name)
        dedup[k] = {
            "end_day": end_day,
            "shift_type": shift_type,
            "worker_name": worker_name,
            "order_number": (order_number or None),
        }

    if not dedup:
        raise ValueError("no valid rows (required: end_day, shift_type, worker_name)")

    # 2) end_day+shift_type 단위 그룹
    groups: Dict[Tuple[str, str], Dict[Tuple[str, str, str], Dict[str, Any]]] = {}
    for k, v in dedup.items():
        day = v["end_day"]
        sh = v["shift_type"]
        gk = (day, sh)
        if gk not in groups:
            groups[gk] = {}
        groups[gk][k] = v

    inserted = 0
    updated = 0
    deleted = 0

    for (end_day, shift_type), target_map in groups.items():
        current_rows = db.execute(
            text(f"""
                SELECT
                    {COL_END_DAY} AS end_day,
                    {COL_SHIFT} AS shift_type,
                    {COL_WORKER} AS worker_name,
                    {COL_ORDER} AS order_number
                FROM {SCHEMA}.{TABLE}
                WHERE {COL_END_DAY} = :end_day
                  AND {COL_SHIFT} = :shift_type
            """),
            {"end_day": end_day, "shift_type": shift_type},
        ).mappings().all()

        current_map: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        for c in current_rows:
            k = (
                _norm(c["end_day"]),
                _norm(c["shift_type"]),
                _norm(c["worker_name"]),
            )
            current_map[k] = dict(c)

        current_keys = set(current_map.keys())
        target_keys = set(target_map.keys())

        to_insert = target_keys - current_keys
        to_delete = current_keys - target_keys
        to_update = current_keys & target_keys

        # INSERT
        for k in sorted(to_insert):
            row = target_map[k]
            r = db.execute(
                text(f"""
                    INSERT INTO {SCHEMA}.{TABLE}
                    ({COL_END_DAY}, {COL_SHIFT}, {COL_WORKER}, {COL_ORDER})
                    VALUES (:end_day, :shift_type, :worker_name, :order_number)
                """),
                row,
            )
            inserted += int(r.rowcount or 0)

        # UPDATE (order_number만)
        for k in sorted(to_update):
            cur = current_map[k]
            tgt = target_map[k]
            cur_order = _norm(cur.get("order_number"))
            tgt_order = _norm(tgt.get("order_number"))
            if cur_order != tgt_order:
                r = db.execute(
                    text(f"""
                        UPDATE {SCHEMA}.{TABLE}
                        SET {COL_ORDER} = :order_number
                        WHERE {COL_END_DAY} = :end_day
                          AND {COL_SHIFT} = :shift_type
                          AND {COL_WORKER} = :worker_name
                    """),
                    tgt,
                )
                updated += int(r.rowcount or 0)

        # DELETE
        for k in sorted(to_delete):
            day_k, shift_k, worker_k = k
            r = db.execute(
                text(f"""
                    DELETE FROM {SCHEMA}.{TABLE}
                    WHERE {COL_END_DAY} = :end_day
                      AND {COL_SHIFT} = :shift_type
                      AND {COL_WORKER} = :worker_name
                """),
                {
                    "end_day": day_k,
                    "shift_type": shift_k,
                    "worker_name": worker_k,
                },
            )
            deleted += int(r.rowcount or 0)

    return {
        "ok": True,
        "inserted": inserted,
        "updated": updated,
        "deleted": deleted,
        "total_after": len(dedup),
    }
