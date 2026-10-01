"""Unit tests for settings validation (fail fast on bad values)."""

from __future__ import annotations

import pytest
from dtq_config.settings import Settings, load_settings
from pydantic import ValidationError


def test_defaults_match_contract() -> None:
    s = Settings()
    assert s.redis_url == "redis://localhost:6379/0"
    assert s.api_host == "0.0.0.0"  # noqa: S104 - asserting the documented default
    assert s.api_port == 8000
    assert s.ws_path == "/ws/events"
    assert s.worker_concurrency == 4
    assert s.visibility_timeout_s == 30
    assert s.heartbeat_interval_s == 5
    assert s.max_queue_depth == 10000
    assert s.max_payload_bytes == 262144
    assert s.task_timeout_ms == 30000
    assert s.max_attempts == 10
    assert s.retry_base_delay == 1.0
    assert s.retry_max_delay == 60.0
    assert s.retry_jitter == 0.2
    assert s.task_retention_s == 604800
    assert s.result_retention_s == 3600
    assert s.result_max_bytes == 8192
    assert s.idem_ttl_s == 86400
    assert s.drain_timeout_s == 30
    assert s.dlq_enabled is True
    assert s.dlq_maxlen == 10000
    assert s.api_key == ""
    assert s.log_level == "INFO"
    assert s.log_json is False


def test_env_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DTQ_API_PORT", "9001")
    monkeypatch.setenv("DTQ_MAX_QUEUE_DEPTH", "500")
    monkeypatch.setenv("DTQ_DLQ_ENABLED", "false")
    monkeypatch.setenv("DTQ_LOG_LEVEL", "debug")
    s = Settings()
    assert s.api_port == 9001
    assert s.max_queue_depth == 500
    assert s.dlq_enabled is False
    assert s.log_level == "DEBUG"


def test_bad_values_fail_fast() -> None:
    with pytest.raises(ValidationError):
        Settings(api_port=99999)
    with pytest.raises(ValidationError):
        Settings(worker_concurrency=0)
    with pytest.raises(ValidationError):
        Settings(retry_jitter=1.5)
    with pytest.raises(ValidationError):
        Settings(retry_max_delay=0.5, retry_base_delay=1.0)
    with pytest.raises(ValidationError):
        Settings(heartbeat_interval_s=30, visibility_timeout_s=30)
    with pytest.raises(ValidationError):
        Settings(log_level="VERBOSE")
    with pytest.raises(ValidationError):
        Settings(ws_path="ws/events")
    with pytest.raises(ValidationError):
        Settings(task_timeout_ms=3_600_001)


def test_load_settings() -> None:
    assert isinstance(load_settings(), Settings)
