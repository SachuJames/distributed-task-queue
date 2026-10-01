"""Idempotency record store (contract section 5).

Scope is (queue, key). Records are ``dtq:idempotency:<queue>:<key>`` holding
JSON {task_id, state, updated_at} with state in {processing, completed,
failed}, written with SET NX and a TTL of DTQ_IDEM_TTL_S.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Literal

from dtq_core.keys import idem_key
from redis.asyncio import Redis

IdemOutcome = Literal["created", "processing", "completed", "failed"]


def _record(task_id: str, state: str) -> str:
    return json.dumps(
        {
            "task_id": task_id,
            "state": state,
            "updated_at": datetime.now(UTC).isoformat(),
        }
    )


async def peek(redis: Redis, queue: str, key: str) -> tuple[str, str] | None:
    """Read the idempotency record without writing. None when absent."""
    raw: Any = await redis.get(idem_key(queue, key))
    if raw is None:
        return None
    data: dict[str, Any] = json.loads(str(raw))
    return str(data["state"]), str(data["task_id"])


async def acquire(
    redis: Redis, queue: str, key: str, task_id: str, ttl_s: int
) -> tuple[IdemOutcome, str]:
    """Claim the idempotency record with SET NX.

    Returns ("created", task_id) for the winner, or the existing record's
    (state, task_id) for a duplicate submission.
    """
    k = idem_key(queue, key)
    created: Any = await redis.set(k, _record(task_id, "processing"), nx=True, ex=ttl_s)
    if created:
        return "created", task_id
    raw: Any = await redis.get(k)
    if raw is None:
        # Raced with expiry between SET and GET; retry the claim once.
        created = await redis.set(k, _record(task_id, "processing"), nx=True, ex=ttl_s)
        if created:
            return "created", task_id
        raw = await redis.get(k)
        if raw is None:  # pragma: no cover - vanishing key, cannot proceed
            raise RuntimeError(f"idempotency key vanished during acquire: {k}")
    data: dict[str, Any] = json.loads(str(raw))
    state = str(data["state"])
    if state not in ("processing", "completed", "failed"):  # pragma: no cover
        raise RuntimeError(f"corrupt idempotency record at {k}: {state!r}")
    outcome: IdemOutcome = (
        "processing" if state == "processing" else "completed" if state == "completed" else "failed"
    )
    return outcome, str(data["task_id"])


async def complete(redis: Redis, queue: str, key: str, state: str) -> bool:
    """Mark the record completed|failed when the task reaches terminal status.

    Returns True when a record was updated, False when none existed.
    The original TTL is preserved.
    """
    if state not in ("completed", "failed"):
        raise ValueError(f"state must be 'completed' or 'failed', got {state!r}")
    k = idem_key(queue, key)
    raw: Any = await redis.get(k)
    if raw is None:
        return False
    data: dict[str, Any] = json.loads(str(raw))
    data["state"] = state
    data["updated_at"] = datetime.now(UTC).isoformat()
    await redis.set(k, json.dumps(data), keepttl=True)
    return True
