"""WebSocket fanout for lifecycle events (WS /ws/events).

One shared Redis pub/sub subscriber serves the whole app: the lifespan
starts fanout_loop, which reads dtq:events via the shared dtq_events bus
subscriber and dispatches each event to every connected client's bounded
asyncio.Queue (1000). No per-client Redis polling.

Slow-client policy: when a client's queue is full, non-critical events are
dropped (counted); terminal events evict the oldest queued event to make
room. If even a terminal event cannot be queued, the client is disconnected
with code 1013. A ping/pong keepalive runs in both directions.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, WebSocket, WebSocketDisconnect

from dtq_api.compat import EventType, bus_subscribe, utcnow_iso
from dtq_api.store import RedisClient

log = logging.getLogger(__name__)

CLIENT_QUEUE_MAX = 1000
PING_INTERVAL_S = 20.0

_TERMINAL_EVENT_VALUES = {
    EventType.TASK_SUCCEEDED.value,
    EventType.TASK_FAILED.value,
    EventType.TASK_CANCELLED.value,
    EventType.TASK_DEAD_LETTERED.value,
}


@dataclass(eq=False)
class WSClient:
    queue: asyncio.Queue[dict[str, Any]]
    disconnect: bool = False
    dropped: int = 0


def _clients(app: FastAPI) -> set[WSClient]:
    return app.state.ws_clients  # type: ignore[no-any-return]


def _dispatch(app: FastAPI, event: dict[str, Any]) -> None:
    event_type = str(event.get("type", ""))
    for client in list(_clients(app)):
        try:
            client.queue.put_nowait(event)
        except asyncio.QueueFull:
            if event_type not in _TERMINAL_EVENT_VALUES:
                client.dropped += 1
                continue
            # Terminal events must not be lost: evict the oldest to make room.
            try:
                client.queue.get_nowait()
            except asyncio.QueueEmpty:
                log.debug("ws client queue unexpectedly empty on eviction")
            try:
                client.queue.put_nowait(event)
            except asyncio.QueueFull:
                # Past the hard cap: the sender loop closes with 1013.
                client.disconnect = True


async def fanout_loop(redis: RedisClient, app: FastAPI, stop: asyncio.Event) -> None:
    """Single shared subscriber: dtq:events -> per-client queues."""
    log.info("ws fanout subscribed to dtq:events")
    try:
        async for event in bus_subscribe(redis):
            if stop.is_set():
                break
            if isinstance(event, dict):
                _dispatch(app, event)
    except asyncio.CancelledError:
        raise
    finally:
        log.info("ws fanout stopped")


async def _sender_loop(websocket: WebSocket, client: WSClient) -> None:
    try:
        while True:
            if client.disconnect:
                await websocket.close(code=1013)
                return
            try:
                event = await asyncio.wait_for(client.queue.get(), timeout=PING_INTERVAL_S)
            except TimeoutError:
                await websocket.send_json({"type": "ping", "ts": utcnow_iso()})
                continue
            await websocket.send_json(event)
    except (WebSocketDisconnect, RuntimeError) as exc:
        log.debug("ws sender loop ended: %s", exc)


async def websocket_endpoint(websocket: WebSocket) -> None:
    """WS /ws/events: stream lifecycle events as JSON."""
    app: FastAPI = websocket.app
    await websocket.accept()
    client = WSClient(queue=asyncio.Queue(maxsize=CLIENT_QUEUE_MAX))
    _clients(app).add(client)
    sender = asyncio.create_task(_sender_loop(websocket, client))
    try:
        while True:
            try:
                message = await asyncio.wait_for(websocket.receive_text(), timeout=PING_INTERVAL_S)
            except TimeoutError:
                log.debug("ws receive timeout, continuing")
                continue
            try:
                data = json.loads(message)
            except json.JSONDecodeError as exc:
                log.debug("ignoring non-JSON ws message: %s", exc)
                continue
            if isinstance(data, dict) and data.get("type") == "ping":
                await websocket.send_json({"type": "pong", "ts": utcnow_iso()})
    except WebSocketDisconnect as exc:
        log.debug("ws client disconnected: %s", exc)
    finally:
        _clients(app).discard(client)
        sender.cancel()
        if client.dropped:
            log.info("ws client disconnected, dropped %d events", client.dropped)
