"""dtq command line interface.

Talks to Redis directly through the shared data layer (dtq_api.store),
no HTTP needed. Entry point: dtq_cli.cli:main (console script `dtq`).

Destructive commands (cancel, retry-dlq, discard-dlq, purge-dlq, purge,
queue pause/resume) require --api-key when DTQ_API_KEY is set, mirroring
the HTTP API's auth rules. Every destructive command is audit-logged with
actor "cli".
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import logging
import sys
import uuid
from datetime import UTC, datetime
from typing import Any

import redis.asyncio
from dtq_api import audit, store
from dtq_api.compat import (
    DTQError,
    Settings,
    Task,
    TaskStatus,
    api_key_matches,
    is_valid_queue_name,
)
from dtq_api.schemas import DlqEntryResponse, TaskResponse

log = logging.getLogger(__name__)

DESTRUCTIVE = {
    "cancel",
    "retry-dlq",
    "discard-dlq",
    "purge-dlq",
    "purge",
    "queue-pause",
    "queue-resume",
}


class CLIError(Exception):
    def __init__(self, message: str, code: str = "CLI_ERROR") -> None:
        super().__init__(message)
        self.message = message
        self.code = code


def _task_json(record: Task, *, duplicate: bool = False) -> str:
    return json.dumps(
        TaskResponse.from_record(record, duplicate=duplicate).model_dump(mode="json"),
        indent=2,
    )


def _require_admin(args: argparse.Namespace, settings: Settings) -> None:
    if not settings.api_key:
        return
    provided = args.api_key or ""
    if not api_key_matches(provided, settings.api_key):
        raise CLIError(
            "destructive command requires --api-key matching DTQ_API_KEY",
            code="UNAUTHORIZED",
        )


def _check_queue(queue: str) -> None:
    if not is_valid_queue_name(queue):
        raise CLIError(f"invalid queue name {queue!r}", code="INVALID_TASK")


# ---------------------------------------------------------------------------
# Command implementations (async, share one Redis client per invocation)
# ---------------------------------------------------------------------------


async def cmd_submit(redis: store.RedisClient, settings: Settings, args: argparse.Namespace) -> int:
    try:
        payload = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        raise CLIError(f"payload is not valid JSON: {exc}", code="INVALID_TASK") from exc
    metadata: dict[str, Any] = {}
    if args.metadata:
        try:
            metadata = json.loads(args.metadata)
        except json.JSONDecodeError as exc:
            raise CLIError(f"metadata is not valid JSON: {exc}", code="INVALID_TASK") from exc
    try:
        outcome = await store.submit_task(
            redis,
            settings,
            task_type=args.type,
            payload=payload,
            queue=args.queue,
            idempotency_key=args.idempotency_key,
            max_attempts=args.max_attempts,
            priority=args.priority,
            timeout_ms=args.timeout_ms,
            delay_seconds=args.delay_seconds,
            metadata=metadata,
        )
    except DTQError as exc:
        raise CLIError(exc.message, code=exc.code) from exc
    print(_task_json(outcome.task, duplicate=not outcome.created))
    return 0


async def cmd_get(redis: store.RedisClient, settings: Settings, args: argparse.Namespace) -> int:
    record = await store.get_task(redis, args.task_id)
    if record is None:
        raise CLIError(f"task {args.task_id} not found", code="TASK_NOT_FOUND")
    print(_task_json(record))
    return 0


async def cmd_list(redis: store.RedisClient, settings: Settings, args: argparse.Namespace) -> int:
    status: TaskStatus | None = None
    if args.status:
        try:
            status = TaskStatus(args.status)
        except ValueError:
            raise CLIError(f"invalid status {args.status!r}", code="INVALID_TASK") from None
    if args.queue:
        _check_queue(args.queue)
    records = await store.list_recent_tasks(
        redis, queue=args.queue, status=status, limit=args.limit
    )
    body = {
        "tasks": [TaskResponse.from_record(r).model_dump(mode="json") for r in records],
        "count": len(records),
    }
    print(json.dumps(body, indent=2))
    return 0


async def cmd_cancel(redis: store.RedisClient, settings: Settings, args: argparse.Namespace) -> int:
    _require_admin(args, settings)
    try:
        outcome = await store.cancel_task(redis, settings, args.task_id)
    except DTQError as exc:
        raise CLIError(exc.message, code=exc.code) from exc
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="cancel_requested" if outcome.cancel_requested else "cancel",
        task_id=args.task_id,
        request_id=uuid.uuid4().hex,
    )
    if outcome.cancel_requested:
        print(f"cancel requested for running task {args.task_id}")
    else:
        print(f"cancelled task {args.task_id}")
    return 0


async def cmd_dlq_list(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    entries = await store.dlq_list(redis, limit=args.limit)
    body = {
        "entries": [DlqEntryResponse.from_entry(e).model_dump(mode="json") for e in entries],
        "count": len(entries),
    }
    print(json.dumps(body, indent=2))
    return 0


async def cmd_retry_dlq(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    _require_admin(args, settings)
    try:
        record = await store.dlq_requeue(redis, settings, args.task_id)
    except DTQError as exc:
        raise CLIError(exc.message, code=exc.code) from exc
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="dlq_requeue",
        task_id=args.task_id,
        request_id=uuid.uuid4().hex,
    )
    print(f"requeued task {args.task_id} status={record.status.value} attempt={record.attempt}")
    return 0


async def cmd_discard_dlq(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    _require_admin(args, settings)
    try:
        await store.dlq_discard(redis, settings, args.task_id)
    except DTQError as exc:
        raise CLIError(exc.message, code=exc.code) from exc
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="dlq_discard",
        task_id=args.task_id,
        request_id=uuid.uuid4().hex,
    )
    print(f"discarded DLQ entry for task {args.task_id}")
    return 0


async def cmd_purge_dlq(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    _require_admin(args, settings)
    if not args.yes:
        raise CLIError("refusing to purge the DLQ without --yes", code="CLI_ERROR")
    removed = await store.dlq_purge(redis)
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="dlq_purge",
        task_id=None,
        request_id=uuid.uuid4().hex,
    )
    print(f"purged {removed} DLQ entries")
    return 0


async def cmd_purge(redis: store.RedisClient, settings: Settings, args: argparse.Namespace) -> int:
    _require_admin(args, settings)
    _check_queue(args.queue)
    if not args.yes:
        raise CLIError(f"refusing to purge queue {args.queue!r} without --yes", code="CLI_ERROR")
    counts = await store.purge_queue(redis, args.queue)
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="queue_purge",
        task_id=None,
        request_id=uuid.uuid4().hex,
    )
    print(
        f"purged queue {args.queue}: "
        f"{counts['stream_entries_removed']} stream entries, "
        f"{counts['schedule_entries_removed']} schedule entries"
    )
    return 0


async def cmd_queue_pause(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    _require_admin(args, settings)
    _check_queue(args.queue)
    await store.pause_queue(redis, args.queue)
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="queue_pause",
        task_id=None,
        request_id=uuid.uuid4().hex,
    )
    print(f"paused queue {args.queue}")
    return 0


async def cmd_queue_resume(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    _require_admin(args, settings)
    _check_queue(args.queue)
    await store.resume_queue(redis, args.queue)
    await audit.log_admin_action(
        redis,
        actor="cli",
        action="queue_resume",
        task_id=None,
        request_id=uuid.uuid4().hex,
    )
    print(f"resumed queue {args.queue}")
    return 0


async def cmd_queue_stats(
    redis: store.RedisClient, settings: Settings, args: argparse.Namespace
) -> int:
    _check_queue(args.queue)
    retry_by_queue = await store.retry_scheduled_by_queue(redis)
    workers = await store.list_workers(redis)
    now_s = datetime.now(UTC).timestamp()
    body = {
        "queue": args.queue,
        "depth": await store.queue_depth(redis, args.queue),
        "pending": await store.queue_pending(redis, args.queue),
        "paused": await store.is_paused(redis, args.queue),
        "retry_scheduled": retry_by_queue.get(args.queue, 0),
        "dlq_depth": await store.dlq_depth(redis),
        "workers_active": sum(
            1 for w in workers if w.is_live(now_s, settings.heartbeat_interval_s)
        ),
    }
    print(json.dumps(body, indent=2))
    return 0


def cmd_worker_run(settings: Settings, args: argparse.Namespace) -> int:
    """Run the worker app in-process. The worker builder owns apps/worker.

    Best-effort adapter: tries dtq_worker entry points and passes the
    arguments each one accepts. Prints a clear error when unavailable.
    """
    candidates = [
        ("dtq_worker.main", "main"),
        ("dtq_worker.app", "main"),
        ("dtq_worker.app", "run"),
        ("dtq_worker", "main"),
        ("dtq_worker.worker", "main"),
    ]
    fn = None
    for module_name, attr in candidates:
        try:
            module = __import__(module_name, fromlist=[attr])
        except ImportError as exc:
            log.debug("worker entry point %s.%s not importable: %s", module_name, attr, exc)
            continue
        candidate = getattr(module, attr, None)
        if callable(candidate):
            fn = candidate
            break
    if fn is None:
        print(
            "error: the worker app is not available in this environment "
            "(dtq_worker is not importable yet; apps/worker is owned by the "
            "worker builder)",
            file=sys.stderr,
        )
        return 2
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        sig = None
    kwargs: dict[str, Any] = {}
    if sig is not None:
        if "argv" in sig.parameters:
            # dtq_worker.main:main(argv) style entry point.
            argv = ["--queue", args.queue]
            if args.concurrency is not None:
                argv += ["--concurrency", str(args.concurrency)]
            for module_name in args.task_modules or []:
                argv += ["--task-module", module_name]
            kwargs["argv"] = argv
        else:
            if "concurrency" in sig.parameters:
                kwargs["concurrency"] = args.concurrency
            if "queue" in sig.parameters:
                kwargs["queue"] = args.queue
            if "settings" in sig.parameters:
                kwargs["settings"] = settings
    try:
        result = fn(**kwargs)
        if inspect.iscoroutine(result):
            asyncio.run(result)
    except DTQError as exc:
        print(f"error: {exc.code}: {exc.message}", file=sys.stderr)
        return 1
    except Exception as exc:  # noqa: BLE001 - report worker startup failures
        print(f"error: worker failed to start: {exc}", file=sys.stderr)
        return 1
    return 0


# ---------------------------------------------------------------------------
# Parser
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="dtq", description="Distributed Task Queue Engine CLI.")
    parser.add_argument(
        "--api-key",
        default=None,
        help="API key for destructive commands when DTQ_API_KEY is set.",
    )
    parser.add_argument(
        "--task-module",
        action="append",
        default=None,
        dest="task_modules",
        help="Task handler module to import (repeatable). Needed for submit "
        "so the CLI can validate --type against registered handlers.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("submit", help="Submit a task.")
    p.add_argument("--type", required=True, help="Registered task type.")
    p.add_argument("--payload", default="{}", help="JSON object payload.")
    p.add_argument("--queue", default="default", help="Queue name.")
    p.add_argument("--idempotency-key", default=None)
    p.add_argument("--max-attempts", type=int, default=5)
    p.add_argument("--priority", type=int, default=5)
    p.add_argument("--timeout-ms", type=int, default=None)
    p.add_argument("--delay-seconds", type=float, default=0.0)
    p.add_argument("--metadata", default=None, help="JSON object metadata.")

    p = sub.add_parser("get", help="Get a task by id.")
    p.add_argument("task_id")

    p = sub.add_parser("list", help="List recent tasks.")
    p.add_argument("--queue", default=None)
    p.add_argument("--status", default=None)
    p.add_argument("--limit", type=int, default=50)

    p = sub.add_parser("cancel", help="Cancel a task.")
    p.add_argument("task_id")

    p = sub.add_parser("dlq", help="Dead-letter queue commands.")
    dlq_sub = p.add_subparsers(dest="dlq_command", required=True)
    q = dlq_sub.add_parser("list", help="List dead-letter entries.")
    q.add_argument("--limit", type=int, default=50)

    sub.add_parser("retry-dlq", help="Requeue a dead-lettered task.").add_argument("task_id")
    sub.add_parser("discard-dlq", help="Discard a dead-letter entry.").add_argument("task_id")
    p = sub.add_parser("purge-dlq", help="Purge the dead-letter stream.")
    p.add_argument("--yes", action="store_true", help="Confirm the purge.")

    p = sub.add_parser("purge", help="Purge a queue's streams and schedule entries.")
    p.add_argument("--queue", required=True)
    p.add_argument("--yes", action="store_true", help="Confirm the purge.")

    p = sub.add_parser("queue", help="Queue admin commands.")
    qsub = p.add_subparsers(dest="queue_command", required=True)
    qsub.add_parser("pause", help="Pause a queue.").add_argument("queue")
    qsub.add_parser("resume", help="Resume a queue.").add_argument("queue")
    qsub.add_parser("stats", help="Show queue stats.").add_argument("queue")

    p = sub.add_parser("worker", help="Worker commands.")
    wsub = p.add_subparsers(dest="worker_command", required=True)
    q = wsub.add_parser("run", help="Run a worker in-process.")
    q.add_argument("--concurrency", type=int, default=None)
    q.add_argument("--queue", default="default")
    q.add_argument(
        "--task-module",
        action="append",
        default=None,
        dest="task_modules",
        help="Task handler module to import (repeatable).",
    )

    return parser


async def _dispatch(redis: store.RedisClient, settings: Settings, args: argparse.Namespace) -> int:
    cmd = args.command
    if cmd == "submit":
        return await cmd_submit(redis, settings, args)
    if cmd == "get":
        return await cmd_get(redis, settings, args)
    if cmd == "list":
        return await cmd_list(redis, settings, args)
    if cmd == "cancel":
        return await cmd_cancel(redis, settings, args)
    if cmd == "dlq":
        if args.dlq_command == "list":
            return await cmd_dlq_list(redis, settings, args)
        raise CLIError(f"unknown dlq command {args.dlq_command}", code="CLI_ERROR")
    if cmd == "retry-dlq":
        return await cmd_retry_dlq(redis, settings, args)
    if cmd == "discard-dlq":
        return await cmd_discard_dlq(redis, settings, args)
    if cmd == "purge-dlq":
        return await cmd_purge_dlq(redis, settings, args)
    if cmd == "purge":
        return await cmd_purge(redis, settings, args)
    if cmd == "queue":
        if args.queue_command == "pause":
            return await cmd_queue_pause(redis, settings, args)
        if args.queue_command == "resume":
            return await cmd_queue_resume(redis, settings, args)
        if args.queue_command == "stats":
            return await cmd_queue_stats(redis, settings, args)
        raise CLIError(f"unknown queue command {args.queue_command}", code="CLI_ERROR")
    if cmd == "worker":
        raise CLIError("worker subcommand handled separately", code="CLI_ERROR")
    raise CLIError(f"unknown command {cmd}", code="CLI_ERROR")


def _load_task_modules(modules: list[str]) -> None:
    """Import task handler modules so type validation sees registered handlers."""
    import importlib
    from pathlib import Path

    # Make `import examples.tasks` work from a repo checkout: the repo root
    # is three levels above this file (apps/cli/dtq_cli/cli.py).
    repo_root = str(Path(__file__).resolve().parents[3])
    if repo_root not in sys.path:
        sys.path.insert(0, repo_root)
    for name in modules:
        importlib.import_module(name)


def main(argv: list[str] | None = None) -> int:
    """Entry point: dtq_cli.cli:main."""
    parser = build_parser()
    args = parser.parse_args(argv)
    settings = Settings()

    if args.command == "worker" and args.worker_command == "run":
        if args.concurrency is None:
            args.concurrency = settings.worker_concurrency
        return cmd_worker_run(settings, args)

    _load_task_modules(args.task_modules or [])

    async def _run() -> int:
        client: store.RedisClient = redis.asyncio.Redis.from_url(
            settings.effective_redis_url, decode_responses=True
        )
        try:
            await client.ping()
        except Exception as exc:
            raise CLIError(f"cannot reach redis: {exc}", code="CLI_ERROR") from exc
        try:
            return await _dispatch(client, settings, args)
        finally:
            await client.aclose()

    try:
        return asyncio.run(_run())
    except CLIError as exc:
        print(f"error: {exc.code}: {exc.message}", file=sys.stderr)
        return 1
    except DTQError as exc:
        print(f"error: {exc.code}: {exc.message}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
