# Idempotency

Idempotency in dtq works at two levels: submission idempotency, which maps
duplicate submissions to one logical task, and handler-level dedup, which
guards side effects inside the handler. Neither level makes arbitrary side
effects exactly-once; the limits are stated plainly at the end.

## Submission idempotency keys

A submission may carry an idempotency key in the `Idempotency-Key` header
or the `idempotency_key` body field. **The header wins** when both are
present. The key is scoped to `(queue, key)`: the same key on different
queues refers to different records.

Records live at `dtq:idempotency:<queue>:<key>` as JSON
`{task_id, state, updated_at}` with `state` in `processing`, `completed`,
`failed`. The lifecycle, implemented in `dtq_idem.store` and
`dtq_queue.ingest`:

1. **Acquire** with `SET NX EX DTQ_IDEM_TTL_S` (default 86400s, 24h). The
   winner's record starts as `processing` and its task is ingested
   normally.
2. **Key present, state `processing`** -> the submission is rejected with
   **409**, code `IDEMPOTENCY_IN_PROGRESS`, including the in-flight
   `task_id` in the error details. The caller should poll or wait; the
   original task is still running.
3. **Key present, state `completed` or `failed`** -> the stored task is
   returned with **200** and `duplicate: true`. No second logical task is
   created, ever, for the same key while the record lives.
4. **Race on expiry**: if the record vanishes between the failed `SET NX`
   and the follow-up `GET`, the code retries the claim once rather than
   erroring.
5. **Stale record**: if the record points at a task hash that no longer
   exists (retention expiry), ingest deletes the record and re-acquires it,
   creating a fresh logical task. A resubmission after the task record is
   gone is treated as new work, not a replay.

Replay is read-only and happens *before* the queue-depth check, so
replaying a completed key is never blocked by backpressure.

## Worker-side state transitions

The API creates the record; the worker moves it to a terminal state when
the task reaches one. `dtq_worker.idem.complete_idem` writes with `SET ...
XX` (only if the record exists) and `KEEPTTL`, preserving the original
expiry:

- `SUCCEEDED` -> record to `completed`.
- `FAILED` / `DEAD_LETTERED` -> record to `failed`.
- `CANCELLED` / `EXPIRED` -> record to `failed`. (The record vocabulary is
  only `processing`/`completed`/`failed`; cancelled and expired tasks will
  never produce a result, so they map to `failed`. A resubmission with the
  same key replays the stored terminal task as a duplicate instead of
  blocking for the TTL.)
- `RETRYING` -> record stays `processing`. The task is not terminal; a
  resubmission still gets 409 until it finishes.
- Tasks submitted without a key have no record; `complete_idem` is a no-op
  for them.

On DLQ requeue (admin), the record is reset to `processing` so that a
resubmission with the same key gets 409 (the requeued task is running
again) rather than replaying the old dead-lettered task.

## Crash window

The completion write order is: task hash to terminal status, lifecycle
event, follow-up write, idempotency state update, then ACK. If the worker
crashes after the side effect but before the record moves to `completed`,
the redelivered execution re-runs the handler and then completes the
record. The guarantee is: **duplicate submissions map to one logical
task**. It is not a guarantee that the handler's side effects happen once.
Concurrent duplicate submissions are serialized by `SET NX`; exactly one
wins and the loser gets 409 or a replay.

## Handler-level dedup: `idempotent_operation`

For side effects inside the handler that must not repeat across
redeliveries, `dtq_tasks.helpers.idempotent_operation` runs a function at
most once per key via `SET NX`:

```python
from dtq_tasks import idempotent_operation, task

@task("send_receipt")
async def send_receipt(payload, ctx):
    # Runs at most once per key within the TTL, even if this handler
    # is redelivered after a crash.
    executed, result = await idempotent_operation(
        redis, f"receipt:{payload['order_id']}", ttl_seconds=86400,
        fn=lambda: charge_and_email(payload),
    )
    if not executed:
        return {"deduped": True}
    return result
```

The first caller executes `fn` (sync or async) and gets `(True, result)`;
later callers within the TTL get `(False, None)` without running it. The
key namespace is yours to choose; it is independent of submission
idempotency keys. The demo `counter_task` in `examples/` uses a plain
Redis `INCR` for the same idea.

## Limits

- **TTL expiry.** Records live `DTQ_IDEM_TTL_S` (default 24h). After
  expiry, the same key submits a brand-new logical task. Idempotency keys
  are a duplicate-submission window, not a forever dedup log.
- **Cancelled tasks map to `failed`.** A resubmission with the same key
  after a cancel replays the cancelled task (`duplicate: true`); it does
  not create a new task and does not return 409. Use a fresh key (or wait
  for TTL expiry) to submit the work again.
- **Side effects are the handler's responsibility.** The record prevents
  duplicate *tasks*, not duplicate *effects*. A handler that charges a
  card and then crashes before the completion write will charge again on
  redelivery unless the handler itself is idempotent (for example via
  `idempotent_operation` or an idempotency key on the downstream call).
- **Scope is per queue.** The same key string on two queues creates two
  tasks. If the key must be global, use a single queue or prefix the key
  yourself.

## See also

- [delivery-guarantees](delivery-guarantees.md): what can duplicate and what is not promised
- [retries](retries.md): why retries keep the record `processing`
- [task-lifecycle](task-lifecycle.md): terminal states and the cancel flow
- [redis-streams](redis-streams.md): the `dtq:idempotency:<queue>:<key>` record layout
