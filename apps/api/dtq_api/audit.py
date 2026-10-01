"""Audit log for destructive operations (CONTRACT section 13).

Every destructive op appends to the dtq:stream:audit stream (MAXLEN ~10000)
with {actor, action, task_id, ts, request_id} and emits a structured log
line. The stream is the durable record; the log line is for operators.
"""

from __future__ import annotations

import logging
from typing import Any

from dtq_core.keys import AUDIT_STREAM

from dtq_api.compat import utcnow_iso
from dtq_api.store import RedisClient

log = logging.getLogger(__name__)


async def log_admin_action(
    redis: RedisClient,
    *,
    actor: str,
    action: str,
    task_id: str | None,
    request_id: str,
) -> None:
    """Record a destructive operation in the audit stream and the logs."""
    # dict[Any, Any] is the concrete mapping type redis-py's xadd expects.
    entry: dict[Any, Any] = {
        "actor": actor,
        "action": action,
        "task_id": task_id or "",
        "ts": utcnow_iso(),
        "request_id": request_id,
    }
    await redis.xadd(AUDIT_STREAM, entry, maxlen=10000, approximate=True)
    log.info(
        "admin action %s",
        action,
        extra={
            "actor": actor.replace("\n", " "),
            "task_id": task_id or "",
            "request_id": request_id,
            "event_type": "ADMIN_ACTION",
        },
    )
