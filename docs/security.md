# Security

Trust boundaries, what the code enforces, and the limits of the protection.

## Trust boundaries

```
untrusted                         trusted
-----------                       -------
task submitters ---> API ----------> Redis (streams, hashes, schedules)
(validation only)   (auth on        |
 destructive ops)   v
                 workers ---> registered handlers only
                 dashboard (read-only SPA, no secrets)
```

- **Submitters** are untrusted. Everything they send is validated at ingest:
  schema, sizes, queue names, registered task types. Their payload is opaque
  data, never code.
- **The API** is the policy enforcement point: admission control, idempotency,
  and API-key auth for destructive operations.
- **Redis** is trusted storage. Anything that can reach Redis can read and
  rewrite tasks, schedules, and worker registrations. There is no per-client
  ACL inside dtq; network reachability to Redis is the boundary.
- **Workers** execute only `@task`-registered handlers from modules loaded at
  startup. They never interpret payload contents as code.
- **The dashboard** is a static SPA. It holds no credentials; destructive
  buttons in the UI call the same authenticated API endpoints as any other
  client.

## Handler execution sandbox (the core guarantee)

Workers execute **only** handlers registered via `@task("name")` in
`dtq_tasks.registry`. The executor:

- looks the handler up by name with `get_handler`; there is no `import`,
  `eval`, `exec`, `shell=True`, `subprocess`, or dynamic import of anything
  derived from the payload or the `task_type` string;
- sends an unknown `task_type` down the permanent failure path (one attempt,
  then DLQ or `FAILED`, `last_error` set to `unknown_task_type:<name>`), and
  never executes it;
- runs sync handlers in a thread via `asyncio.to_thread` and async handlers
  awaited directly, with a hard `timeout_ms` enforced by `asyncio.timeout`.

No Python source can enter the system through the API or through Redis: task
payloads are JSON objects, and the JSON is decoded to a dict before the
handler sees it. A payload that is not a JSON object is a permanent failure.

## Authentication

Set `DTQ_API_KEY` to a long random value. Behavior:

- Destructive operations require the `X-API-Key` header: task cancel, DLQ
  requeue, DLQ discard, DLQ purge, queue pause, queue resume, and the CLI's
  queue purge. Missing key: 401 `UNAUTHORIZED`. Wrong key: 403 `FORBIDDEN`.
- Reads (`GET` endpoints) and task submission stay open, by design, for local
  use and for producers that cannot hold a secret.
- Key comparison uses `hmac.compare_digest` (constant time).
- When `DTQ_API_KEY` is empty, everything is open. This is the documented
  local-dev mode; the audit actor is recorded as `local` instead of `api-key`.
- The CLI mirrors the HTTP rules: destructive commands need `--api-key` when
  `DTQ_API_KEY` is set, and audit as actor `cli`.

Treat the API key as a single shared admin secret. There are no per-user
accounts, roles, or scopes.

## Audit trail

Every destructive op appends to the Redis stream `dtq:stream:audit`
(`MAXLEN ~10000`) with `{actor, action, task_id, ts, request_id}` and emits a
structured log line. The CLI audits with actor `cli`. Because the stream is
trimmed, export it if you need retention beyond the last ten thousand admin
actions.

## Input validation limits

Enforced with Pydantic at the API boundary and re-checked in the shared
ingest path used by the CLI:

| Input | Limit | Violation |
|---|---|---|
| Payload size | `DTQ_MAX_PAYLOAD_BYTES`, default 262144 bytes (serialized JSON) | 413 `PAYLOAD_TOO_LARGE` |
| Metadata size | 4 KB serialized | 400 `INVALID_TASK` |
| Queue name | `^[a-z0-9][a-z0-9_-]{0,63}$` | 400 `INVALID_TASK` |
| Priority | 0 to 9 | 400 `INVALID_TASK` |
| `max_attempts` | 1 to `DTQ_MAX_ATTEMPTS`, default cap 10 | 400 `INVALID_TASK` |
| `timeout_ms` | 1 to 3600000 (1 hour) | 400 `INVALID_TASK` |
| Queue depth (admission) | `DTQ_MAX_QUEUE_DEPTH`, default 10000 entries across the queue's priority streams | 429 `QUEUE_FULL` with `Retry-After` |
| Error messages stored | sanitized, max 500 chars | truncated server-side |
| Handler results stored | `DTQ_RESULT_MAX_BYTES`, default 8192 bytes | truncated server-side |

Additional hardening:

- Log injection: newlines and carriage returns in logged fields are replaced
  with spaces in both API and worker formatters.
- Payloads and secrets are never logged, and never used as metric labels.
- Error responses are `{code, message, request_id}`; unexpected exceptions
  become 500 `INTERNAL_ERROR` with a generic message, never a traceback.
- Container images run as a non-root user, Redis has no host port published
  in the default compose file, and images are minimal `python:3.12-slim`.

## What is NOT protected

Stated plainly, so nobody assumes otherwise:

- **No TLS.** The API serves plain HTTP. Terminate TLS at a reverse proxy or
  load balancer in any non-local deployment.
- **No authentication on submit or reads.** Anyone who can reach the API can
  submit tasks and read tasks, queues, workers, and DLQ entries. Payloads may
  contain sensitive data; do not put secrets in payloads.
- **Redis has no password in the default compose file** and dtq has no
  Redis ACL integration. Anyone with network access to Redis owns the system:
  tasks, schedules, idempotency keys, worker registrations, and the audit
  stream.
- **The API key is a shared admin secret**, not identity. It cannot tell
  operators apart, and it travels in a header over (by default) plain HTTP.
- **No rate limiting on the API** beyond the queue-depth admission gate.
  Delayed tasks bypass even that gate (they wait in the retry schedule, not
  a stream). A hostile or buggy producer can fill the retry schedule; see
  [failure-modes.md](failure-modes.md).
- **Handlers receive the raw payload dict.** dtq validates that the payload
  is a JSON object within the size limit, nothing more. Shape validation is
  the handler author's job; a registered handler called with an unexpected
  payload shape fails the way its own code fails.
- **Workers are not sandboxed from each other or from Redis.** A malicious
  handler module loaded into a worker runs with the worker's full privileges.
  Only load handler code you trust.
- **The dashboard has no login.** It is a read-only view plus the same
  authenticated admin endpoints; protect it at the network layer if the API
  key is set and you do not want the UI reachable.

## See also

- [threat-model.md](threat-model.md): assets, actors, attack surface, and
  residual risks in more detail.
- [deployment.md](deployment.md): how to close the obvious gaps (TLS,
  Redis auth, network policy) when deploying.
- [observability.md](observability.md): the audit stream and structured
  logging fields.
