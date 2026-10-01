"""Unit tests for Redis key builders."""

from __future__ import annotations

import pytest
from dtq_core import keys


def test_stream_name_bands() -> None:
    assert keys.stream_name("default", 0) == "dtq:stream:default:p0"
    assert keys.stream_name("default", 9) == "dtq:stream:default:p9"
    assert keys.stream_name("orders", 5) == "dtq:stream:orders:p5"


def test_stream_name_rejects_bad_priority() -> None:
    with pytest.raises(ValueError):
        keys.stream_name("default", -1)
    with pytest.raises(ValueError):
        keys.stream_name("default", 10)


def test_priority_streams_covers_all_bands() -> None:
    streams = keys.priority_streams("default")
    assert len(streams) == 10
    assert streams[0] == "dtq:stream:default:p0"
    assert streams[9] == "dtq:stream:default:p9"
    assert len(set(streams)) == 10


def test_exact_key_names() -> None:
    assert keys.DLQ_STREAM == "dtq:stream:dead-letter"
    assert keys.RETRY_SCHEDULE == "dtq:retry:schedule"
    assert keys.RETRY_CLAIMED == "dtq:retry:claimed"
    assert keys.WORKERS_SET == "dtq:workers"
    assert keys.CANCELLED_SET == "dtq:cancelled"
    assert keys.RECENT_ZSET == "dtq:tasks:recent"
    assert keys.SCHEDULER_LOCK == "dtq:lock:scheduler"
    assert keys.AUDIT_STREAM == "dtq:stream:audit"
    assert keys.EVENTS_CHANNEL == "dtq:events"
    assert keys.CONSUMER_GROUP == "workers"


def test_key_builders() -> None:
    assert keys.task_key("abc") == "dtq:task:abc"
    assert keys.idem_key("default", "k1") == "dtq:idempotency:default:k1"
    assert keys.worker_key("worker-1a2b3c4d") == "dtq:worker:worker-1a2b3c4d"
    assert keys.pause_key("orders") == "dtq:queue:orders:paused"
