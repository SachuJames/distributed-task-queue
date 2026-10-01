"""Logging configuration: stdlib logging, JSON when DTQ_LOG_JSON=true.

JSON records carry timestamp, level, service, worker_id, task_id,
task_type, queue, attempt, status, duration_ms, event_type, plus the
message. Payloads and secrets are never logged; newlines in logged fields
are sanitized to blunt log injection.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

from dtq_api.compat import Settings

_FIELDS = (
    "worker_id",
    "task_id",
    "task_type",
    "queue",
    "attempt",
    "status",
    "duration_ms",
    "event_type",
    "actor",
    "request_id",
    "path",
)


def _clean(value: object) -> object:
    if isinstance(value, str):
        return value.replace("\n", " ").replace("\r", " ")
    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": "dtq-api",
            "message": _clean(record.getMessage()),
        }
        for name in _FIELDS:
            payload[name] = _clean(getattr(record, name, ""))
        return json.dumps(payload)


def setup_logging(settings: Settings) -> None:
    """Configure the root logger once at startup."""
    level = getattr(logging, settings.log_level.upper(), logging.INFO)
    handler = logging.StreamHandler()
    if settings.log_json:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    root = logging.getLogger()
    root.handlers = []
    root.addHandler(handler)
    root.setLevel(level)
    # Keep uvicorn's access logs quiet unless debugging.
    logging.getLogger("uvicorn.access").setLevel(max(level, logging.WARNING))
