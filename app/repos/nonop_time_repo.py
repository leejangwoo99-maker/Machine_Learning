from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.nonop_time import NonOpRow


def _fqn(table: str) -> str:
    return f'{settings.GPF_SCHEMA}."{table}"'


def _normalize_end_day_yyyymmdd(end_day: str) -> str:
    return (end_day or "").replace("-", "").strip()


def _is_day_shift(from_time: str, to_time: str) -> bool:
    ft = (from_time or "")[:8]
    tt = (to_time or "")[:8]
    if not ft or not tt:
        return False
    # day: 08:30:00 ~ 20:29:59
    return not (tt < "08:30:00" or ft > "20:29:59")


def _match_shift(from_time: str, to_time: str, shift_type: str) -> bool:
    s = (shift_type or "").strip().lower()
    if s not in ("day", "night"):
        raise ValueError("shift_type must be 'day' or 'night'")
    day_hit = _is_day_shift(from_time, to_time)
    return day_hit if s == "day" else (not day_hit)


def list_all_by_day_shift(engine: Engine, end_day: str, shift_type: str) -> list[NonOpRow]:
    end_day_yyyymmdd = _normalize_end_day_yyyymmdd(end_day)

    sql = text(
        f"""
        WITH merged AS (
            SELECT
                replace(end_day, '-', '') AS end_day,
                station,
                from_time,
                to_time,
                reason,
                sparepart
            FROM {_fqn(settings.FCT_NONOP_TABLE)}
            WHERE replace(end_day, '-', '') = :end_day

            UNION ALL

            SELECT
                replace(end_day, '-', '') AS end_day,
                station,
                from_time,
                to_time,
                reason,
                sparepart
            FROM {_fqn(settings.VISION_NONOP_TABLE)}
            WHERE replace(end_day, '-', '') = :end_day
        )
        SELECT end_day, station, from_time, to_time, reason, sparepart
        FROM merged
        ORDER BY from_time DESC, station ASC
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day_yyyymmdd}).mappings().all()

    out: list[NonOpRow] = []
    for r in rows:
        if _match_shift(str(r["from_time"]), str(r["to_time"]), shift_type):
            out.append(NonOpRow(**dict(r)))
    return out


def update_reason_sparepart_by_key(
    engine: Engine,
    *,
    end_day: str,
    station: str,
    from_time: str,
    to_time: str,
    reason: str | None,
    sparepart: str | None,
) -> None:
    """
    station prefix濡?????뚯씠釉??좏깮 ??
    key row??reason/sparepart留??섏젙?쒕떎. (INSERT ?놁쓬)
    """
    s = (station or "").strip()
    table = settings.FCT_NONOP_TABLE if s.upper().startswith("FCT") else settings.VISION_NONOP_TABLE

    sql = text(
        f"""
        UPDATE {_fqn(table)}
           SET reason = :reason,
               sparepart = :sparepart
         WHERE replace(end_day, '-', '') = :end_day
           AND station = :station
           AND from_time::text = :from_time
           AND to_time::text = :to_time
        """
    )

    payload = {
        "end_day": _normalize_end_day_yyyymmdd(end_day),
        "station": station,
        "from_time": from_time,
        "to_time": to_time,
        "reason": reason,
        "sparepart": sparepart,
    }

    with engine.begin() as conn:
        res = conn.execute(sql, payload)
        if res.rowcount == 0:
            raise ValueError("target row not found")
