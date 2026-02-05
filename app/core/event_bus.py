from __future__ import annotations

import asyncio
from typing import Any


class EventBus:
    def __init__(self) -> None:
        self._subs: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()

    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=1000)
        async with self._lock:
            self._subs.add(q)
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        async with self._lock:
            self._subs.discard(q)

    async def publish(self, event_name: str, data: dict[str, Any]) -> None:
        async with self._lock:
            subs = list(self._subs)

        for q in subs:
            try:
                q.put_nowait({"event": event_name, "data": data})
            except asyncio.QueueFull:
                # 느린 구독자는 드랍
                pass


event_bus = EventBus()
