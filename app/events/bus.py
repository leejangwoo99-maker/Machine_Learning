from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, asdict
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any

KST = ZoneInfo("Asia/Seoul")


@dataclass
class EventMessage:
    event: str                 # ex) "non_operation_time_event"
    channel: str               # ex) "non_operation_time"
    payload: dict[str, Any]
    ts_kst: str


class EventBus:
    """
    ?꾨줈?몄뒪 ?대? pub/sub (?⑥씪 uvicorn ?꾨줈?몄뒪 湲곗?).
    - 援щ룆?먮퀎 Queue
    - publish ??fan-out
    """
    def __init__(self) -> None:
        self._subs: set[asyncio.Queue[EventMessage]] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue[EventMessage]:
        q: asyncio.Queue[EventMessage] = asyncio.Queue(maxsize=1000)
        async with self._lock:
            self._subs.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue[EventMessage]) -> None:
        async with self._lock:
            self._subs.discard(q)

    async def publish(self, event: str, channel: str, payload: dict[str, Any]) -> None:
        msg = EventMessage(
            event=event,
            channel=channel,
            payload=payload,
            ts_kst=datetime.now(KST).isoformat(timespec="seconds"),
        )
        async with self._lock:
            dead: list[asyncio.Queue[EventMessage]] = []
            for q in self._subs:
                try:
                    q.put_nowait(msg)
                except asyncio.QueueFull:
                    # ?섎컻???덉슜: ?먮┛ 援щ룆???먮뒗 drop
                    pass
                except Exception:
                    dead.append(q)
            for q in dead:
                self._subs.discard(q)


bus = EventBus()


def to_sse_block(msg: EventMessage) -> str:
    data = json.dumps(asdict(msg), ensure_ascii=False)
    return f"event: {msg.event}\ndata: {data}\n\n"
