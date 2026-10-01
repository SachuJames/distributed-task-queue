"""Unit tests for the retry delay formula and error classification."""

from __future__ import annotations

import random

import pytest
import redis.exceptions
from dtq_retry.policy import classify_retryable, compute_delay_s
from dtq_tasks.errors import PermanentError, RetryableError
from pydantic import BaseModel, ValidationError


def freeze_uniform(monkeypatch: pytest.MonkeyPatch, value: float) -> None:
    monkeypatch.setattr(random, "uniform", lambda a, b: value)


def test_formula_exponential_growth(monkeypatch: pytest.MonkeyPatch) -> None:
    freeze_uniform(monkeypatch, 0.0)
    assert compute_delay_s(1, 1.0, 60.0, 0.2) == 1.0
    assert compute_delay_s(2, 1.0, 60.0, 0.2) == 2.0
    assert compute_delay_s(3, 1.0, 60.0, 0.2) == 4.0
    assert compute_delay_s(4, 1.0, 60.0, 0.2) == 8.0


def test_formula_jitter_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    freeze_uniform(monkeypatch, -0.2)
    assert compute_delay_s(1, 1.0, 60.0, 0.2) == pytest.approx(0.8)
    freeze_uniform(monkeypatch, 0.2)
    assert compute_delay_s(1, 1.0, 60.0, 0.2) == pytest.approx(1.2)


def test_formula_cap(monkeypatch: pytest.MonkeyPatch) -> None:
    freeze_uniform(monkeypatch, 0.0)
    # base * 2**9 = 512, capped at max_delay
    assert compute_delay_s(10, 1.0, 60.0, 0.2) == 60.0
    assert compute_delay_s(100, 1.0, 60.0, 0.2) == 60.0


def test_formula_floor_at_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    # jitter beyond the contract range still clamps at 0, never negative
    freeze_uniform(monkeypatch, -2.0)
    assert compute_delay_s(1, 1.0, 60.0, 0.2) == 0.0


def test_formula_statistical_bounds() -> None:
    for _ in range(200):
        delay = compute_delay_s(2, 1.0, 60.0, 0.2)
        assert 0.0 <= delay <= 2.4


def test_formula_rejects_bad_attempt() -> None:
    with pytest.raises(ValueError):
        compute_delay_s(0, 1.0, 60.0, 0.2)


class _StrictModel(BaseModel):
    count: int


@pytest.mark.parametrize(
    "exc",
    [
        RetryableError("boom"),
        TimeoutError("timed out"),
        TimeoutError("async timed out"),
        redis.exceptions.ConnectionError("lost"),
        redis.exceptions.TimeoutError("redis slow"),
    ],
)
def test_retryable_errors(exc: BaseException) -> None:
    assert classify_retryable(exc) is True


@pytest.mark.parametrize(
    "exc",
    [
        PermanentError("nope"),
        ValueError("plain bug"),
        RuntimeError("plain bug"),
        KeyError("missing"),
    ],
)
def test_permanent_errors(exc: BaseException) -> None:
    assert classify_retryable(exc) is False


def test_pydantic_validation_error_is_permanent() -> None:
    try:
        _StrictModel(count="not-an-int")  # type: ignore[arg-type]
    except ValidationError as exc:
        assert classify_retryable(exc) is False
    else:  # pragma: no cover
        pytest.fail("expected a ValidationError")


def test_unknown_exceptions_never_retry() -> None:
    class CustomError(Exception):
        pass

    assert classify_retryable(CustomError()) is False


def test_retryable_error_beats_timeout_subclass() -> None:
    # RetryableError must stay retryable even though it is not a TimeoutError
    assert not isinstance(RetryableError("x"), TimeoutError)
    assert classify_retryable(RetryableError("x")) is True

    # PermanentError wins even for timeout-looking subclasses
    class Both(PermanentError, TimeoutError):
        pass

    assert classify_retryable(Both()) is False
