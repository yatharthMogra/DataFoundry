"""Worker pool — orchestrates the normalize → validate → dedup → route pipeline."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING

from src.models import DLQRecord, RawEvent
from src.pipeline.dedup import DeduplicationCache
from src.pipeline.normalizer import normalize
from src.pipeline.validator import validate

if TYPE_CHECKING:
    from src.dlq.store import DLQStore
    from src.queue import BoundedAsyncQueue
    from src.storage.batch import BatchAccumulator

logger = logging.getLogger(__name__)


class WorkerPool:
    """Manages a pool of async workers that consume from the queue and
    run the full processing pipeline.

    Each worker:
      1. Pulls a :class:`RawEvent` from the queue.
      2. Normalizes it to an :class:`InternalEvent`.
      3. Validates the event.
      4. Checks for duplicates.
      5. Routes the event to the batch accumulator (valid) or DLQ (invalid/dup).

    Parameters
    ----------
    num_workers:
        Number of concurrent async worker tasks.
    queue:
        Shared bounded async queue to consume from.
    dedup:
        Deduplication cache instance.
    dlq:
        Dead-letter queue store.
    batch:
        Batch accumulator for valid, deduplicated events.
    """

    def __init__(
        self,
        num_workers: int,
        queue: BoundedAsyncQueue,
        dedup: DeduplicationCache,
        dlq: DLQStore,
        batch: BatchAccumulator,
    ) -> None:
        self._num_workers = num_workers
        self._queue = queue
        self._dedup = dedup
        self._dlq = dlq
        self._batch = batch
        self._tasks: list[asyncio.Task] = []
        self._running = False

    # ── Lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        """Spawn worker tasks."""
        self._running = True
        for i in range(self._num_workers):
            task = asyncio.create_task(self._worker_loop(i), name=f"worker-{i}")
            self._tasks.append(task)
        logger.info("Worker pool started", extra={"num_workers": self._num_workers})

    async def stop(self) -> None:
        """Signal workers to drain and wait for completion."""
        self._running = False
        # Give workers a moment to finish current items
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()
        logger.info("Worker pool stopped")

    # ── Worker loop ─────────────────────────────────────────────

    async def _worker_loop(self, worker_id: int) -> None:
        logger.debug("Worker started", extra={"worker_id": worker_id})

        while self._running:
            try:
                raw: RawEvent = await self._queue.get(timeout=1.0)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break

            try:
                await self._process(raw)
            except Exception:
                logger.exception(
                    "Unhandled error processing event",
                    extra={"event_id": raw.event_id, "worker_id": worker_id},
                )
                # Route unexpected failures to DLQ
                await self._dlq.push(
                    DLQRecord(
                        event_id=raw.event_id,
                        source=raw.source,
                        raw_payload=raw.payload,
                        error_reason="unhandled processing error",
                    )
                )

    # ── Processing pipeline ─────────────────────────────────────

    async def _process(self, raw: RawEvent) -> None:
        start = time.monotonic()

        # 1. Normalize
        event = normalize(raw)

        # 2. Validate
        is_valid, errors = validate(event)
        if not is_valid:
            await self._dlq.push(
                DLQRecord(
                    event_id=raw.event_id,
                    source=raw.source,
                    raw_payload=raw.payload,
                    error_reason="; ".join(errors),
                )
            )
            return

        # 3. Dedup
        is_dup = await self._dedup.is_duplicate(event.event_id)
        if is_dup:
            logger.debug("Duplicate event dropped", extra={"event_id": event.event_id})
            return

        # 4. Route to batch
        await self._batch.add(event)

        elapsed = time.monotonic() - start
        logger.debug(
            "Event processed",
            extra={
                "event_id": event.event_id,
                "source": event.source,
                "processing_time_ms": round(elapsed * 1000, 2),
            },
        )
