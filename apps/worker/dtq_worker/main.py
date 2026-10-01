"""Worker entrypoint: run_worker(settings, queues).

Logging follows contract section 13: JSON lines when DTQ_LOG_JSON=true with
the documented fields, human-readable otherwise. Payloads and secrets are
never logged; newlines in logged fields are sanitized.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import json
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path

from dtq_config.settings import Settings, load_settings

from .worker import Worker

logger = logging.getLogger("dtq.worker")

_LOG_FIELDS = (
    "timestamp",
    "level",
    "service",
    "worker_id",
    "task_id",
    "task_type",
    "queue",
    "attempt",
    "status",
    "duration_ms",
    "event_type",
)


def _sanitize(value: object) -> object:
    if isinstance(value, str):
        return value.replace("\r", " ").replace("\n", " ")
    return value


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        doc: dict[str, object] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": "dtq-worker",
        }
        for name in _LOG_FIELDS[3:]:
            doc[name] = _sanitize(getattr(record, name, ""))
        doc["message"] = _sanitize(record.getMessage())
        return json.dumps(doc, separators=(",", ":"))


def _configure_logging(settings: Settings) -> None:
    root = logging.getLogger()
    root.setLevel(settings.log_level)
    handler = logging.StreamHandler(stream=sys.stdout)
    if settings.log_json:
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s"))
    root.handlers.clear()
    root.addHandler(handler)


def _ensure_examples_importable() -> None:
    """Make ``import examples.tasks`` work from a repo checkout."""
    repo_root = Path(__file__).resolve().parents[3]
    if str(repo_root) not in sys.path:
        sys.path.insert(0, str(repo_root))


def _load_task_modules(modules: list[str]) -> None:
    for name in modules:
        importlib.import_module(name)


def run_worker(
    settings: Settings | None = None,
    queues: list[str] | None = None,
    task_modules: list[str] | None = None,
) -> None:
    """Build settings, load task handlers, and run the worker until signaled."""
    resolved = settings or load_settings()
    _configure_logging(resolved)
    _ensure_examples_importable()
    _load_task_modules(task_modules or ["examples.tasks"])
    from .registry import registered_names

    logger.info(
        "starting worker",
        extra={
            "worker_id": resolved.worker_id or "(auto)",
            "queue": ",".join(queues or ["default"]),
        },
    )
    logger.info("registered task types: %s", registered_names())
    worker = Worker(resolved, queues=queues)
    asyncio.run(worker.run())


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="dtq-worker", description="Run a DTQ worker.")
    parser.add_argument("--concurrency", type=int, default=None)
    parser.add_argument("--queue", action="append", default=None, dest="queues")
    parser.add_argument("--task-module", action="append", default=None, dest="task_modules")
    args = parser.parse_args(argv)

    settings = load_settings()
    if args.concurrency is not None:
        settings.worker_concurrency = args.concurrency
    run_worker(settings, queues=args.queues, task_modules=args.task_modules)


if __name__ == "__main__":
    main()
