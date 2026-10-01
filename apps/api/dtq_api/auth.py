"""API key auth for destructive operations (CONTRACT section 13).

When DTQ_API_KEY is set, destructive operations (task cancel, DLQ
requeue/discard/purge, queue pause/resume) require the X-API-Key header,
compared in constant time. Reads and task submission stay open for local
use. When DTQ_API_KEY is empty, everything is open (documented local-dev
mode); the dependency then returns the actor "local" instead of "api-key".
"""

from __future__ import annotations

from fastapi import Request

from dtq_api.compat import (
    ForbiddenError,
    Settings,
    UnauthorizedError,
    api_key_matches,
)


async def check_api_key(request: Request, settings: Settings) -> str:
    """Return the actor name when authorized; raise 401/403 otherwise."""
    if not settings.api_key:
        return "local"
    provided = request.headers.get("X-API-Key")
    if not provided:
        raise UnauthorizedError("missing X-API-Key header")
    if not api_key_matches(provided, settings.api_key):
        raise ForbiddenError("invalid API key")
    return "api-key"
