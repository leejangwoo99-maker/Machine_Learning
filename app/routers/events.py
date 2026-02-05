from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from app.core.event_bus import event_bus

router = APIRouter(tags=["0.events"])

KST = timezone(timedelta(hours=9))


@router.get(
    "/events",
    summary="Stream Events",
    description=(
        "통합 SSE endpoint\n\n"
        "- 모든 이벤트를 한 스트림으로 전달\n"
        "- 클라이언트는 event 이름으로 필터링"
    ),
)
async def stream_events(request: Request):
    async def gen():
        q = await event_bus.subscribe()
        try:
            # connected 이벤트
            connected_payload = {"ok": True, "ts_kst": datetime.now(KST).isoformat()}
            yield f"event: connected\ndata: {json.dumps(connected_payload, ensure_ascii=False)}\n\n"

            while True:
                if await request.is_disconnected():
                    break

                try:
                    item = await asyncio.wait_for(q.get(), timeout=15.0)
                    event = item["event"]
                    data = item["data"]
                    yield f"event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
                except asyncio.TimeoutError:
                    # keepalive
                    yield ": keepalive\n\n"
        finally:
            await event_bus.unsubscribe(q)

    return StreamingResponse(gen(), media_type="text/event-stream")
