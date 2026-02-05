from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from app.core.config import settings
from app.schemas.nonop_time import NonOpRow


def _fqn(table: str) -> str:
    return f'{settings.GPF_SCHEMA}."{table}"'


def _normalize_end_day_yyyymmdd(end_day: str) -> str:
    # "2026-02-05" -> "20260205", "20260205" -> "20260205"
    return (end_day or "").replace("-", "").strip()


def _is_day_shift(from_time: str, to_time: str) -> bool:
    """
    day:   08:30:00 ~ 20:29:59
    night: 그 외(20:30:00~23:59:59, 00:00:00~08:29:59)
    문자열 HH:MM:SS 비교
    """
    ft = (from_time or "")[:8]
    tt = (to_time or "")[:8]
    if not ft or not tt:
        return False
    return not (tt < "08:30:00" or ft > "20:29:59")


def _match_shift(from_time: str, to_time: str, shift_type: str) -> bool:
    s = (shift_type or "").strip().lower()
    if s not in ("day", "night"):
        raise ValueError("shift_type must be 'day' or 'night'")
    day_hit = _is_day_shift(from_time, to_time)
    return day_hit if s == "day" else (not day_hit)


def list_all_by_day_shift(engine: Engine, end_day: str, shift_type: str) -> list[NonOpRow]:
    end_day_yyyymmdd = _normalize_end_day_yyyymmdd(end_day)

    # DB에 shift_type 컬럼이 없으므로 SQL에서 필터하지 않고, Python에서 필터
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
        SELECT
            end_day, station, from_time, to_time, reason, sparepart
        FROM merged
        ORDER BY from_time DESC, station ASC
        """
    )

    with engine.connect() as conn:
        rows = conn.execute(sql, {"end_day": end_day_yyyymmdd}).mappings().all()

    filtered: list[NonOpRow] = []
    for r in rows:
        if _match_shift(str(r["from_time"]), str(r["to_time"]), shift_type):
            filtered.append(NonOpRow(**dict(r)))
    return filtered


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
    station prefix로 대상 테이블 선택 후,
    지정 key row의 reason/sparepart만 수정한다.
    INSERT는 절대 하지 않는다.
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
           AND from_time = :from_time
           AND to_time = :to_time
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
        if (res.rowcount or 0) == 0:
            raise ValueError("target row not found")
