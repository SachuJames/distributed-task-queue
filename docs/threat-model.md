# Threat model

For operators deciding what dtq is safe to expose and where it needs help
from the surrounding infrastructure.

## Assets

1. **Task payloads and results.** Opaque JSON supplied by producers and
   returned by handlers. May contain business data; dtq treats it as opaque
   but stores it in Redis streams, task hashes, the DLQ stream, and result
   keys, and serves it back over the API and CLI.
2. **Execution capacity.** Workers, concurrency slots, the retry schedule,
   and queue depth. Exhausting them denies service to legitimate tasks.
3. **Control plane state.** Queue pause flags, the scheduler leader lock,
   worker registrations, the audit stream. Corrupting them disrupts or
   blinds operations.
4. **The API key.** A single shared admin secret guarding destructive
   operations.
5. **Operational visibility.** Metrics, logs, and the audit stream. Losing
   them removes the ability to detect the other attacks.

## Actors

| Actor | Capability | Trust |
|---|---|---|
| Task producer | Submits tasks, reads tasks/queues/DLQ | Untrusted; validated at ingest only |
| Dashboard user | Same as producer plus the admin UI (key-gated actions) | Untrusted |
| Admin (API key holder) | Cancel, requeue, discard, purge, pause/resume | Trusted, but the key is shared and bearer |
| Handler author | Code loaded into workers at startup | Fully trusted; code runs with worker privileges |
| Redis network neighbor | Raw Redis protocol access | Must be prevented; no in-system defense |
| Passive network observer | Sees API traffic | Prevented only by external TLS |

## Attack surface

**API (FastAPI, port 8000).** Ingest, reads, admin endpoints, WebSocket,
`/metrics`. Input validation is the main control; auth covers only
destructive ops. No rate limiting beyond the queue-depth gate.

**Redis (internal network, no host port by default).** All state lives here:
streams, hashes, schedules, idempotency keys, worker registrations, audit
stream, pub/sub. No dtq-level ACL; network reachability equals full control.

**Dashboard (nginx SPA, port 3000).** Static files plus proxied API calls.
No credentials stored; no login. Whatever the network exposes, the UI
exposes.

**Worker processes.** Load handler modules at startup and execute them per
task. A worker that processes a hostile payload is safe as long as the
handler validates its input; a worker loaded with a hostile *module* is fully
compromised.

## Abuse cases

### Task-type confusion

A producer submits a registered task type with a hostile or malformed
payload, hoping the handler mishandles it.

- **Present mitigation:** the payload must be a JSON object within the size
  limit; unknown types fail permanently without executing; handlers are
  looked up, never imported or evaluated.
- **Residual risk:** shape validation is the handler author's job. A handler
  that trusts `payload["url"]` and fetches it has an SSRF problem dtq cannot
  fix. Validate every field in the handler.

### Payload bombs

A producer submits payloads at the size cap, or deeply nested structures,
to burn CPU in JSON parsing or in a handler.

- **Present mitigation:** 256 KB cap at ingest (413 beyond it); 4 KB cap on
  metadata; handler `timeout_ms` (default 30s, max 1h) bounds one execution.
- **Residual risk:** the cap is per task, not per producer; a fast producer
  can submit many max-size payloads. Depth/nesting of JSON is not limited.
  There is no per-producer rate limit.

### Idempotency-key squatting

Keys are scoped `(queue, key)` and created with `SET NX`. Anyone can claim
a key.

- **Present mitigation:** scope includes the queue, so a squatter must target
  the right queue; keys expire after `DTQ_IDEM_TTL_S` (24h).
- **Residual risk:** a squatter who guesses or learns a producer's key
  pattern can hold keys in `processing` state, forcing the legitimate
  producer's submissions to 409 `IDEMPOTENCY_IN_PROGRESS` until expiry. There
  is no ownership check because submit is unauthenticated. Use
  unguessable keys (uuids) if this matters to you.

### DLQ as data exfiltration

DLQ entries contain the failed task's payload plus error details, and DLQ
reads are unauthenticated (like all reads).

- **Present mitigation:** none inside dtq; this is by design for
  debuggability.
- **Residual risk:** anyone who can reach the API can read every failed
  payload via `GET /api/v1/dlq`. Do not put secrets in task payloads, and
  restrict network access to the API.

### Retry-schedule flooding

`delay_seconds` parks tasks in the retry schedule, which the depth gate does
not count.

- **Present mitigation:** none; the gate sums stream `XLEN` only.
- **Residual risk:** a producer can grow `dtq:retry:schedule` without bound
  and without triggering 429. The scheduler batch is capped at 100 per pass,
  so a flooded schedule also delays legitimate retries. Monitor
  `dtq_retry_scheduled` and alert on it.

### Cancel and admin abuse

Cancel, requeue, discard, purge, pause, and resume change or destroy work.

- **Present mitigation:** API-key auth when `DTQ_API_KEY` is set; every
  action is audit-logged with actor and request id; discards remove the DLQ
  entry but keep the task hash marked `DEAD_LETTERED` with
  `metadata.discarded=true`, never silently destroying the record.
- **Residual risk:** with the key unset (local-dev default), all of this is
  open. The key is shared, so the audit trail cannot attribute actions to a
  person.

### Event-stream eavesdropping and spoofing

Events carry task ids, types, queues, attempts, and worker ids.

- **Present mitigation:** none beyond network controls; the WS endpoint has
  no auth separate from the API's.
- **Residual risk:** a passive observer learns workload patterns and task
  metadata. Events are not signed; a party that can publish to Redis can
  forge `TASK_SUCCEEDED` events the dashboard will display (task state in
  Redis is unaffected, since workers decide from the task hash, not events).

### Dependency confusion in handler modules

Workers `--task-module` load arbitrary Python modules at startup.

- **Present mitigation:** none inside dtq.
- **Residual risk:** this is the highest-privilege action in the system.
  Pin dependencies, review handler code, and treat the worker image build as
  a trusted pipeline.

## Residual risks, summarized

What the operator still owns after deploying dtq as documented:

1. Network isolation for Redis and, if the data is sensitive, for the API.
2. TLS termination in front of the API.
3. A strong, rotated `DTQ_API_KEY` in any shared environment.
4. Per-producer rate limiting if producers are untrusted (dtq has none).
5. Payload shape validation inside every handler.
6. NTP on all hosts (schedule, heartbeats, TTLs, and the claim reaper are
   clock-dependent).
7. Backup and restore for Redis (the only durable state).
8. Review of every handler module loaded into workers.

## See also

- [security.md](security.md): the controls that *are* implemented, and what
  is explicitly not protected.
- [failure-modes.md](failure-modes.md): how these abuses manifest as
  operational failures and how to recover.
- [deployment.md](deployment.md): concrete steps to close the network and
  TLS gaps.
