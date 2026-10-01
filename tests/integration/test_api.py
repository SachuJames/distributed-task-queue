"""Integration tests for the DTQ HTTP API (real Redis).

Run with DTQ_TEST_REDIS_DB set to the api/cli builder's db index:
  DTQ_TEST_REDIS_DB=2 .venv/bin/pytest tests/integration/test_api.py -q
"""

from __future__ import annotations

import uuid
from typing import Any

import pytest
from dtq_api import store
from dtq_api.compat import Settings
from dtq_api.main import create_app
from dtq_core.keys import AUDIT_STREAM, task_key
from dtq_queue import task_state
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
def settings() -> Settings:
    return Settings()


@pytest.fixture
def client(settings: Settings, redis_client: Any) -> Any:
    with TestClient(create_app(settings)) as test_client:
        yield test_client


@pytest.fixture
def authed_settings() -> Settings:
    return Settings(api_key="test-api-key")


@pytest.fixture
def authed_client(authed_settings: Settings, redis_client: Any) -> Any:
    with TestClient(create_app(authed_settings)) as test_client:
        yield test_client


def _submit(client: Any, task_type: str = "echo", queue: str = "default", **kwargs: Any) -> Any:
    body = {"task_type": task_type, "payload": {"n": 1}, "queue": queue}
    body.update(kwargs)
    return client.post("/api/v1/tasks", json=body)


def test_health_and_ready(client: Any) -> None:
    assert client.get("/health").status_code == 200
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["status"] == "ready"


def test_submit_valid_202(client: Any) -> None:
    response = _submit(client)
    assert response.status_code == 202
    data = response.json()
    assert data["task_id"]
    assert data["status"] == "queued"
    assert data["duplicate"] is False


def test_submit_unknown_task_type_400(client: Any) -> None:
    response = _submit(client, task_type="no-such-handler")
    assert response.status_code == 400
    assert response.json()["code"] == "UNKNOWN_TASK_TYPE"


def test_submit_oversized_payload_413(client: Any) -> None:
    response = client.post(
        "/api/v1/tasks",
        json={"task_type": "echo", "payload": {"blob": "x" * (2 * 1024 * 1024)}},
    )
    assert response.status_code == 413
    assert response.json()["code"] == "PAYLOAD_TOO_LARGE"


def test_submit_invalid_body_400(client: Any) -> None:
    response = client.post("/api/v1/tasks", json={"payload": {}})
    assert response.status_code == 400
    assert response.json()["code"] == "INVALID_TASK"


def test_duplicate_idempotency_inflight_409(client: Any) -> None:
    headers = {"Idempotency-Key": f"dup-{uuid.uuid4().hex}"}
    first = client.post("/api/v1/tasks", json={"task_type": "echo", "payload": {}}, headers=headers)
    second = client.post(
        "/api/v1/tasks", json={"task_type": "echo", "payload": {}}, headers=headers
    )
    assert first.status_code == 202
    # The first submission is still in flight (no worker ran it), so the
    # shared ingest reports 409 IDEMPOTENCY_IN_PROGRESS; the stored task is
    # unchanged.
    assert second.status_code == 409
    assert second.json()["code"] == "IDEMPOTENCY_IN_PROGRESS"
    tasks = client.get("/api/v1/tasks").json()["tasks"]
    assert [t["task_id"] for t in tasks] == [first.json()["task_id"]]


def test_get_task(client: Any) -> None:
    task_id = _submit(client).json()["task_id"]
    response = client.get(f"/api/v1/tasks/{task_id}")
    assert response.status_code == 200
    assert response.json()["task_id"] == task_id


def test_get_missing_task_404(client: Any) -> None:
    response = client.get(f"/api/v1/tasks/{uuid.uuid4().hex}")
    assert response.status_code == 404
    assert response.json()["code"] == "TASK_NOT_FOUND"


def test_list_tasks_status_filter(client: Any) -> None:
    _submit(client)
    _submit(client)
    queued = client.get("/api/v1/tasks", params={"status": "queued"})
    assert queued.status_code == 200
    assert len(queued.json()["tasks"]) == 2
    running = client.get("/api/v1/tasks", params={"status": "running"})
    assert running.json()["tasks"] == []


def test_cancel_queued_task(client: Any) -> None:
    task_id = _submit(client).json()["task_id"]
    response = client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert response.status_code == 202
    assert response.json()["status"] == "cancelled"
    assert client.get(f"/api/v1/tasks/{task_id}").json()["status"] == "cancelled"


async def test_cancel_running_task_409(client: Any, redis_client: Any, settings: Settings) -> None:
    task_id = _submit(client).json()["task_id"]
    await task_state.record_attempt_start(
        redis_client, task_id, "worker-1", settings.task_retention_s
    )
    response = client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert response.status_code == 409
    assert response.json()["code"] == "CANCEL_REQUESTED"
    flag = await redis_client.hget(task_key(task_id), "cancel_requested")
    assert flag == "1"


def test_cancel_terminal_task_410(client: Any) -> None:
    task_id = _submit(client).json()["task_id"]
    assert client.post(f"/api/v1/tasks/{task_id}/cancel").status_code == 202
    response = client.post(f"/api/v1/tasks/{task_id}/cancel")
    assert response.status_code == 410
    assert response.json()["code"] == "TASK_TERMINAL"


def test_queue_stats_depth(client: Any) -> None:
    _submit(client, queue="statq")
    _submit(client, queue="statq")
    response = client.get("/api/v1/queues/statq")
    assert response.status_code == 200
    data = response.json()
    assert data["depth"] == 2
    assert data["pending"] == 0
    assert data["paused"] is False


def test_queue_list(client: Any) -> None:
    _submit(client, queue="listq")
    response = client.get("/api/v1/queues")
    assert response.status_code == 200
    names = [q["queue"] for q in response.json()["queues"]]
    assert "listq" in names


def test_pause_resume_auth(authed_client: Any) -> None:
    no_key = authed_client.post("/api/v1/queue/authq/pause")
    assert no_key.status_code == 401
    wrong = authed_client.post("/api/v1/queue/authq/pause", headers={"X-API-Key": "wrong"})
    assert wrong.status_code == 403
    paused = authed_client.post("/api/v1/queue/authq/pause", headers={"X-API-Key": "test-api-key"})
    assert paused.status_code == 200
    assert paused.json()["paused"] is True
    stats = authed_client.get("/api/v1/queues/authq")
    assert stats.json()["paused"] is True
    resumed = authed_client.post(
        "/api/v1/queue/authq/resume", headers={"X-API-Key": "test-api-key"}
    )
    assert resumed.json()["paused"] is False


def test_pause_without_key_when_auth_disabled(client: Any) -> None:
    response = client.post("/api/v1/queue/openq/pause")
    assert response.status_code == 200
    assert response.json()["paused"] is True


async def test_dlq_requeue_flow(
    authed_client: Any, redis_client: Any, authed_settings: Settings
) -> None:
    task_id = _submit(authed_client).json()["task_id"]
    await store.dlq_add(
        redis_client,
        authed_settings,
        task_id,
        last_error="boom",
        worker_id="w-9",
    )
    listed = authed_client.get("/api/v1/dlq").json()["entries"]
    assert [e["task_id"] for e in listed] == [task_id]

    response = authed_client.post(
        f"/api/v1/dlq/{task_id}/requeue", headers={"X-API-Key": "test-api-key"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "queued"
    assert data["attempt"] == 1
    assert data["metadata"]["previous_attempts"] == [1]

    listed_after = authed_client.get("/api/v1/dlq").json()["entries"]
    assert listed_after == []
    assert authed_client.get(f"/api/v1/tasks/{task_id}").json()["status"] == "queued"


def test_dlq_requeue_missing_404(authed_client: Any) -> None:
    response = authed_client.post(
        f"/api/v1/dlq/{uuid.uuid4().hex}/requeue",
        headers={"X-API-Key": "test-api-key"},
    )
    assert response.status_code == 404
    assert response.json()["code"] == "DLQ_ENTRY_NOT_FOUND"


async def test_dlq_discard(
    authed_client: Any, redis_client: Any, authed_settings: Settings
) -> None:
    task_id = _submit(authed_client).json()["task_id"]
    await store.dlq_add(redis_client, authed_settings, task_id)
    response = authed_client.post(
        f"/api/v1/dlq/{task_id}/discard", headers={"X-API-Key": "test-api-key"}
    )
    assert response.status_code == 200
    assert authed_client.get("/api/v1/dlq").json()["entries"] == []
    record = authed_client.get(f"/api/v1/tasks/{task_id}").json()
    assert record["status"] == "dead_lettered"
    assert record["metadata"]["discarded"] is True


def test_metrics_exposes_dtq_metrics(client: Any) -> None:
    _submit(client)
    response = client.get("/metrics")
    assert response.status_code == 200
    text = response.text
    assert "dtq_tasks_submitted_total" in text
    assert "dtq_queue_depth" in text


async def test_audit_written_on_destructive_op(authed_client: Any, redis_client: Any) -> None:
    authed_client.post("/api/v1/queue/auditq/pause", headers={"X-API-Key": "test-api-key"})
    entries = await redis_client.xrevrange(AUDIT_STREAM, count=5)
    actions = [dict(fields).get("action") for _, fields in entries]
    assert "queue_pause" in actions
