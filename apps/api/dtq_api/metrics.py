"""Prometheus metrics for the DTQ API process.

Built with the shared dtq_metrics.metrics.build_metrics against a private
CollectorRegistry (never the default registry, so imports stay test-safe).
Labels follow CONTRACT section 9 exactly; refresh_gauges() recomputes gauge
values from Redis so /metrics reports truthful point-in-time values.
"""

from __future__ import annotations

import logging
from typing import Any

from dtq_metrics.metrics import DtqMetrics, build_metrics
from prometheus_client import CONTENT_TYPE_LATEST, CollectorRegistry, generate_latest

log = logging.getLogger(__name__)

_registry = CollectorRegistry()
_metrics: DtqMetrics = build_metrics(_registry)


def get_metrics() -> DtqMetrics:
    """The process-wide metrics bundle."""
    return _metrics


async def refresh_gauges(redis: Any) -> None:
    """Recompute gauge values from Redis. Best effort; never raises."""
    try:
        from dtq_api import store

        for queue in await store.discover_queues(redis):
            _metrics.queue_depth.labels(queue=queue).set(await store.queue_depth(redis, queue))
            _metrics.queue_pending.labels(queue=queue).set(await store.queue_pending(redis, queue))
        retry_by_queue = await store.retry_scheduled_by_queue(redis)
        _metrics.retry_scheduled.set(sum(retry_by_queue.values()))
        _metrics.dlq_depth.set(await store.dlq_depth(redis))
        workers = await store.list_workers(redis)
        _metrics.workers_active.set(sum(1 for w in workers if w.is_live()))
    except Exception as exc:  # noqa: BLE001 - metrics must never break /metrics
        log.warning("gauge refresh failed: %s", exc)


def exposition() -> tuple[bytes, str]:
    """Return (body, content_type) for the /metrics endpoint."""
    return generate_latest(_registry), CONTENT_TYPE_LATEST
