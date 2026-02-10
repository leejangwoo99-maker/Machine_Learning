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
        # lock 援ш컙 理쒖냼??
        async with self._lock:
            subs = list(self._subs)

        for q in subs:
            try:
                q.put_nowait({"event": event_name, "data": data})
            except asyncio.QueueFull:
                # ?먮┛ 援щ룆?먮뒗 ?ㅻ옒??1媛?踰꾨━怨?理쒖떊 ?대깽???ｊ린 ?쒕룄
                try:
                    _ = q.get_nowait()
                except Exception:
                    pass
                try:
                    q.put_nowait({"event": event_name, "data": data})
                except Exception:
                    pass


event_bus = EventBus()
