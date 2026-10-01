"""Idempotency record helpers (contract section 5).

Record: ``dtq:idempotency:<queue>:<key>`` -> JSON
``{task_id, state, updated_at}``, ``state`` in {processing, completed, failed}.

The API creates the record (SET NX) at ingest. The worker moves it to
``completed``/``failed`` when the task reaches a terminal status. Cancelled
and expired tasks map to ``failed``: they will never produce a result, and a
duplicate submission with the same key replays the stored (terminal) task.

Guarantee: duplicate submissions map to one logical task. The engine cannot
make arbitrary handler side effects exactly-once; see the delivery
guarantees doc.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime

from dtq_core.keys import idem_key as _idem_key
from redis.asyncio import Redis

#: States stored in the idempotency record.
STATE_PROCESSING = "processing"
STATE_COMPLETED = "completed"
STATE_FAILED = "failed"

_VALID_STATES = frozenset({STATE_PROCESSING, STATE_COMPLETED, STATE_FAILED})


def _record_json(task_id: str, state: str) -> str:
    return json.dumps(
        {
            "task_id": task_id,
            "state": state,
            "updated_at": datetime.now(UTC).isoformat(),
        },
        separators=(",", ":"),
    )


async def complete_idem(
    redis: Redis,
    queue: str,
    key: str,
    state: str,
    task_id: str,
    ttl_s: int,
) -> bool:
    """Move an existing idempotency record to a terminal ``state``.

    Uses XX so tasks ingested without a key (no record exists) are skipped.
    Returns True when a record was updated.
    """
    if state not in _VALID_STATES:
        raise ValueError(f"invalid idempotency state: {state!r}")
    if not key:
        return False
    updated: object = await redis.set(
        _idem_key(queue, key), _record_json(task_id, state), ex=ttl_s, xx=True
    )
    return bool(updated)


async def claim_idem(redis: Redis, queue: str, key: str, task_id: str, ttl_s: int) -> bool:
    """Create a ``processing`` record with SET NX. Used by ingest (and tests
    simulating ingest). Returns True when this caller won the key."""
    if not key:
        return False
    acquired: object = await redis.set(
        _idem_key(queue, key),
        _record_json(task_id, STATE_PROCESSING),
        ex=ttl_s,
        nx=True,
    )
    return bool(acquired)
