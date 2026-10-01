"""Unit tests for the typed error taxonomy."""

from __future__ import annotations

import pytest
from dtq_core import errors


@pytest.mark.parametrize(
    ("cls", "code", "http_status"),
    [
        (errors.InvalidTaskError, "INVALID_TASK", 400),
        (errors.UnknownTaskTypeError, "UNKNOWN_TASK_TYPE", 400),
        (errors.TaskTimeoutError, "TASK_TIMEOUT", 408),
        (errors.TaskRetryExhaustedError, "TASK_RETRY_EXHAUSTED", 422),
        (errors.QueueFullError, "QUEUE_FULL", 429),
        (errors.RedisUnavailableError, "REDIS_UNAVAILABLE", 503),
        (errors.TaskCancelledError, "TASK_CANCELLED", 409),
        (errors.WorkerUnavailableError, "WORKER_UNAVAILABLE", 503),
        (errors.IdempotencyInProgressError, "IDEMPOTENCY_IN_PROGRESS", 409),
        (errors.PayloadTooLargeError, "PAYLOAD_TOO_LARGE", 413),
    ],
)
def test_code_and_http_status(cls: type[errors.DtqError], code: str, http_status: int) -> None:
    err = cls("boom")
    assert err.code == code
    assert err.http_status == http_status
    assert isinstance(err, errors.DtqError)
    assert isinstance(err, Exception)
    assert str(err) == "boom"
    assert err.message == "boom"


def test_default_message_is_code() -> None:
    err = errors.QueueFullError()
    assert err.message == "QUEUE_FULL"
    assert str(err) == "QUEUE_FULL"


def test_details_carried() -> None:
    err = errors.QueueFullError("full", details={"queue": "q"})
    assert err.details["queue"] == "q"
    assert err.details["retry_after_s"] == 5


def test_queue_full_retry_after() -> None:
    assert errors.QueueFullError(retry_after_s=30).retry_after_s == 30


def test_catch_as_base_class() -> None:
    with pytest.raises(errors.DtqError) as exc_info:
        raise errors.PayloadTooLargeError("too big")
    assert exc_info.value.code == "PAYLOAD_TOO_LARGE"
    assert exc_info.value.http_status == 413


def test_contract_api_statuses_match() -> None:
    # Statuses the contract's HTTP surface actually returns.
    assert errors.InvalidTaskError.http_status == 400
    assert errors.UnknownTaskTypeError.http_status == 400
    assert errors.IdempotencyInProgressError.http_status == 409
    assert errors.PayloadTooLargeError.http_status == 413
    assert errors.QueueFullError.http_status == 429
