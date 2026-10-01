# Contributing

How the repo is organized, how to run it locally, and how to add a task
handler.

## Repo layout

```
distributed-task-queue/
  CONTRACT.md                 # normative spec, sections 1-15; read first
  pyproject.toml              # build, deps, ruff, mypy, pytest config
  Makefile                    # install / test / lint / docker targets
  docker-compose.yml          # full stack: redis, api, worker, dashboard, prometheus
  docker/                     # Dockerfiles, nginx.conf, prometheus.yml
  docs/                       # you are here
  packages/                   # shared libraries (dtq_*), no app code
    core/dtq_core/            # models, Redis key names, error taxonomy
    config/dtq_config/        # DTQ_ settings, validated at startup
    queue/dtq_queue/          # streams, ingest, task state, Lua scripts
    retry/dtq_retry/          # retry policy math, scheduler claim scripts
    idempotency/dtq_idem/     # idempotency key store
    events/dtq_events/        # event schemas, pub/sub bus
    metrics/dtq_metrics/      # Prometheus metric definitions
    tasks/dtq_tasks/          # handler registry, TaskContext, errors, helpers
  apps/
    api/dtq_api/              # FastAPI: routes, WS fanout, auth, audit, /metrics
    worker/dtq_worker/        # poll/reaper/scheduler/heartbeat loops, executor
    cli/dtq_cli/              # `dtq` command (argparse, talks to Redis directly)
    dashboard/src/            # React + TypeScript SPA (Vite)
  examples/tasks/             # demo handlers: echo, sleep, fibonacci, flaky,
                              # always_fail, counter, slow (synthetic data only)
  scripts/                    # demo.sh, load_test.py
  tests/
    unit/                     # no Redis needed
    integration/              # real local Redis
    concurrency/              # races, duplicate absorption, scheduler contention
    e2e/                      # full stack
    fixtures/                 # shared Redis fixtures
```

Rules of thumb: shared behavior goes in `packages/`; only one app may own a
behavior. Apps may depend on packages, never on each other, except the CLI
which reuses `dtq_api.store` as its data layer. The contract is normative;
when code and contract disagree, document the code and note the difference.

## Dev setup

Prerequisites: Python 3.12 and a local `redis-server`.

```
python3 -m venv .venv
.venv/bin/pip install -e .
.venv/bin/pip install -e ".[test,lint]"   # or: make dev
```

Start a local Redis (no persistence, for dev only):

```
make redis
# equivalent: redis-server --daemonize yes --save '' --appendonly no
```

Run the pieces in separate terminals:

```
# API on :8000
.venv/bin/python -m uvicorn dtq_api.main:app --host 0.0.0.0 --port 8000

# worker (loads examples/tasks by default)
.venv/bin/python -m dtq_worker.main
# or: .venv/bin/dtq worker run --concurrency 4 --queue default

# submit a task
.venv/bin/dtq submit --type echo_task --payload '{"hello":"world"}'
.venv/bin/dtq get <task-id>
```

Or bring up everything with `make docker-up` (`docker compose up --build -d`).

## Running tests

Tests use a **real** local Redis, never a fake. Isolate your DB index with
`DTQ_TEST_REDIS_DB` so parallel runs do not interfere:

```
# unit tests (no Redis)
make test                                    # pytest tests/unit -q

# integration + concurrency (needs local redis-server running)
DTQ_TEST_REDIS_DB=1 make integration-test    # pytest tests/integration tests/concurrency -q

# end to end
DTQ_TEST_REDIS_DB=2 make e2e-test            # pytest tests/e2e -q
```

By convention: packages work uses DB 1, API/CLI uses 2, worker uses 3, and a
final full run goes serially on DB 0. `asyncio_mode = "auto"` is set in
`pyproject.toml`, so async test functions just work.

## Lint and typecheck

```
make lint        # ruff check packages apps tests examples scripts
make format      # ruff format ...
make typecheck   # mypy packages apps (strict = true)
```

mypy runs in strict mode over `packages` and `apps` with the source trees on
`mypy_path` so the `dtq_*` editable installs resolve to real types. Keep it
clean: the build treats new strict violations as failures. Ruff selects
`E, F, I, UP, B, ASYNC, S` with a 100-column line length; security-sensitive
rules (`S`) mean things like `random.uniform` for jitter need an explicit
`# noqa: S311` with a comment, as the existing code does.

## Commit conventions

- Small, logical commits: one behavior or fix per commit, with a message that
  says what changed and why.
- Never reference AI tooling, generated work, or automation in commit
  messages, code comments, or docs. Write commits that read like a developer
  wrote them.
- Do not commit secrets, `.env` files, or local test artifacts.
- Update `CONTRACT.md` only when the spec itself changes; when the code
  intentionally diverges, note the difference in `docs/` instead.

## Adding a new task handler

Handlers live in importable Python modules. The worker loads them at startup
via `--task-module` (repeatable); the default is `examples.tasks`.

1. Create a module, e.g. `examples/tasks/my_task.py`:

```python
from typing import Any
from dtq_tasks import RetryableError, TaskContext, task

@task("my_task")
async def my_task(payload: dict[str, Any], ctx: TaskContext) -> dict[str, Any]:
    # validate the payload shape yourself; dtq only checks it is a JSON object
    name = payload.get("name")
    if not isinstance(name, str):
        # permanent: retrying cannot fix a bad payload
        from dtq_tasks import PermanentError
        raise PermanentError(f"payload needs a string 'name', got {name!r}")
    ...
    return {"greeting": f"hello {name}", "attempt": ctx.attempt}
```

2. Rules the executor enforces:
   - The function signature is `fn(payload: dict, ctx: TaskContext)`.
     Async functions are awaited; sync functions run in a thread (GIL-bound,
     not interruptible: on timeout the thread is detached and the result
     discarded).
   - Return value must be JSON-serializable; a non-serializable return is a
     permanent failure. Results are truncated to `DTQ_RESULT_MAX_BYTES`.
   - `TaskContext` gives you `task_id`, `idempotency_key`, `attempt`,
     `worker_id`, `queue`, and `deadline` (monotonic seconds; finish before
     it, the executor enforces `timeout_ms` anyway).
   - Raise `RetryableError` for transient failures (retried with backoff),
     `PermanentError` for failures retrying cannot fix. Timeouts and Redis
     connection errors are retryable automatically. Any other exception is
     treated as permanent: dtq never retries blindly.
   - Handler names must be unique; re-registering a name raises `ValueError`.
   - Unknown task types submitted by producers fail permanently without ever
     executing. Never derive behavior from the payload beyond data.

3. Make handlers idempotent. At-least-once delivery means a crash after your
   side effects but before the completion write re-runs the handler. For
   side effects that must happen once, use the helper:

```python
from dtq_tasks import idempotent_operation

ran, result = await idempotent_operation(
    redis, f"myop:{ctx.task_id}", ttl_seconds=3600, fn=do_the_thing
)
```

   (`redis` here is your own connection; handlers do not get one from dtq.)

4. Load and try it:

```
.venv/bin/python -m dtq_worker.main --task-module examples.tasks
.venv/bin/dtq submit --type my_task --payload '{"name":"ada"}'
.venv/bin/dtq get <task-id>
```

5. Add tests: a unit test for the handler logic in `tests/unit`, and if the
   handler has interesting retry behavior, an integration test that submits
   through the real stack.

## See also

- [observability.md](observability.md): metrics and logs for the handler you
  add.
- [failure-modes.md](failure-modes.md): the at-least-once semantics your
  handler must be written against.
- [security.md](security.md): why handlers must validate their own payloads.
