"""WebSocket integration tests (real Redis).

Run: DTQ_TEST_REDIS_DB=2 .venv/bin/pytest tests/integration/test_websocket.py -q
"""

from __future__ import annotations

from typing import Any

import pytest
from dtq_api.compat import Settings
from dtq_api.main import create_app
from dtq_tasks.registry import get_handler, task
from fastapi.testclient import TestClient

pytest_plugins = "tests.fixtures.redis"


def _register_demo_handlers() -> None:
    if get_handler("echo") is None:

        @task("echo")
        async def _echo(payload: dict[str, Any], ctx: Any) -> dict[str, Any]:
            return {"echo": payload}


_register_demo_handlers()


@pytest.fixture
def client(redis_client: Any) -> Any:
    with TestClient(create_app(Settings())) as test_client:
        yield test_client


def _receive_matching(ws: Any, predicate: Any, attempts: int = 20) -> dict[str, Any]:
    for _ in range(attempts):
        event: Any = ws.receive_json()
        if predicate(event):
            return dict(event)
    raise AssertionError("matching event never arrived")


async def test_ws_receives_task_queued(client: Any) -> None:
    with client.websocket_connect("/ws/events") as ws:
        response = client.post("/api/v1/tasks", json={"task_type": "echo", "payload": {"n": 1}})
        assert response.status_code == 202
        task_id = response.json()["task_id"]
        event = _receive_matching(
            ws,
            lambda e: e.get("type") == "TASK_QUEUED" and e.get("task_id") == task_id,
        )
        assert event["queue"] == "default"


async def test_ws_ping_pong(client: Any) -> None:
    with client.websocket_connect("/ws/events") as ws:
        ws.send_json({"type": "ping"})
        pong = _receive_matching(ws, lambda e: e.get("type") == "pong")
        assert pong["type"] == "pong"
