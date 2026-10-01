"""Retry delay computation (contract section 4).

Formula: ``capped = min(max_delay, base * 2 ** (attempt - 1))``,
``actual = capped * (1 + uniform(-jitter, jitter))``, floored at 0.
"""

from __future__ import annotations

import random


def compute_retry_delay_ms(
    attempt: int,
    base_delay_s: float,
    max_delay_s: float,
    jitter: float,
) -> int:
    """Return the backoff delay in ms before the next attempt.

    ``attempt`` is the attempt number that just failed (starting at 1).
    """
    capped = min(max_delay_s, base_delay_s * (2.0 ** (attempt - 1)))
    # S311: jitter needs uniform spread, not cryptographic randomness.
    actual = capped * (1.0 + random.uniform(-jitter, jitter))  # noqa: S311
    return max(0, int(actual * 1000))
