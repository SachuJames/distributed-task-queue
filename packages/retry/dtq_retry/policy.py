"""Retry delay computation and error classification (contract section 4)."""

from __future__ import annotations

import asyncio
import random

import redis.exceptions
from dtq_tasks.errors import PermanentError, RetryableError
from pydantic import ValidationError


def compute_delay_s(attempt: int, base: float, max_delay: float, jitter: float) -> float:
    """Compute the backoff delay in seconds for a failed attempt.

    Exact contract formula::

        capped = min(max_delay, base * 2 ** (attempt - 1))
        actual = capped * (1 + random.uniform(-jitter, jitter))
        actual = max(0, actual)

    ``attempt`` is the attempt number that just failed (1-based).
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")
    capped = min(max_delay, base * 2.0 ** (attempt - 1))
    # S311: jitter needs uniform spread, not cryptographic randomness.
    actual = capped * (1.0 + random.uniform(-jitter, jitter))  # noqa: S311
    return max(0.0, actual)


def classify_retryable(exc: BaseException) -> bool:
    """Decide whether a handler failure may be retried.

    Retryable: TimeoutError/asyncio.TimeoutError, Redis connection errors,
    handler-raised dtq_tasks.RetryableError. Everything else is permanent,
    including dtq_tasks.PermanentError, Pydantic validation errors, unknown
    task types, payload decode errors, and over-limit error messages.
    Never retries unknown exceptions: infinite loops are forbidden.
    """
    if isinstance(exc, PermanentError):
        return False
    if isinstance(exc, RetryableError):
        return True
    # asyncio.TimeoutError is an alias of TimeoutError on 3.11+; both named
    # explicitly per the contract's retryable list.
    if isinstance(exc, TimeoutError | asyncio.TimeoutError):
        return True
    if isinstance(exc, redis.exceptions.ConnectionError | redis.exceptions.TimeoutError):
        return True
    if isinstance(exc, ValidationError):
        return False
    return False
