"""Prometheus metric definitions and helpers for DataFlow."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

if TYPE_CHECKING:
    from src.config import MetricsConfig

logger = logging.getLogger(__name__)

# ── Counters ────────────────────────────────────────────────────

EVENTS_INGESTED = Counter(
    "events_ingested_total",
    "Total events received from all sources",
    labelnames=["source"],
)

EVENTS_PROCESSED = Counter(
    "events_processed_total",
    "Total events successfully processed and sent to storage",
)

EVENTS_FAILED = Counter(
    "events_failed_total",
    "Total events that failed processing (validation, dedup, errors)",
    labelnames=["reason"],
)

EVENTS_DLQ = Counter(
    "events_dlq_total",
    "Total events routed to the Dead-Letter Queue",
    labelnames=["source"],
)

# ── Gauges ──────────────────────────────────────────────────────

QUEUE_LENGTH = Gauge(
    "queue_length",
    "Current number of items in the internal event queue",
)

DLQ_SIZE = Gauge(
    "dlq_size",
    "Current number of events in the Dead-Letter Queue",
)

# ── Histograms ──────────────────────────────────────────────────

EVENT_PROCESSING_LATENCY = Histogram(
    "event_processing_latency_seconds",
    "Time spent processing a single event (normalize + validate + dedup)",
    buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
)

BATCH_FLUSH_DURATION = Histogram(
    "batch_flush_duration_seconds",
    "Time spent flushing a batch to storage",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0),
)

EVENTS_PER_BATCH = Histogram(
    "events_per_batch",
    "Number of events in each flushed batch",
    buckets=(1, 10, 50, 100, 250, 500, 1000, 2500, 5000),
)

# ── Server ──────────────────────────────────────────────────────


def start_metrics_server(config: MetricsConfig) -> None:
    """Start the Prometheus metrics HTTP server on the configured port."""
    if not config.enabled:
        logger.info("Metrics server disabled by configuration")
        return

    start_http_server(config.port)
    logger.info("Prometheus metrics server started", extra={"port": config.port})
