"""Batch accumulator with time/size flush triggers and retry logic."""

from __future__ import annotations

import asyncio
import logging
import random
import time

from src.config import BatchConfig, RetryConfig
from src.metrics.prometheus import BATCH_FLUSH_DURATION, EVENTS_PER_BATCH
from src.models import InternalEvent
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)


class BatchAccumulator:
    """Collects events and flushes them in batches to the storage backend.

    A flush is triggered by whichever comes first:
    - The batch reaching ``batch_config.max_size`` events.
    - The ``batch_config.max_flush_interval_seconds`` timer expiring.

    Failed writes are retried with exponential backoff + jitter.

    Parameters
    ----------
    storage:
        The concrete storage backend to write batches to.
    batch_config:
        Batch size and flush interval settings.
    retry_config:
        Retry policy (max attempts, backoff parameters).
    """

    def __init__(
        self,
        storage: StorageBackend,
        batch_config: BatchConfig,
        retry_config: RetryConfig,
    ) -> None:
        self._storage = storage
        self._max_size = batch_config.max_size
        self._flush_interval = batch_config.max_flush_interval_seconds
        self._retry = retry_config

        self._buffer: list[InternalEvent] = []
        self._lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._running = False

    # ── Lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        self._running = True
        self._flush_task = asyncio.create_task(
            self._periodic_flush(), name="batch-flush"
        )
        logger.info(
            "Batch accumulator started",
            extra={
                "max_size": self._max_size,
                "flush_interval_s": self._flush_interval,
            },
        )

    async def stop(self) -> None:
        self._running = False
        if self._flush_task is not None:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        # Final flush
        await self._flush()
        await self._storage.close()
        logger.info("Batch accumulator stopped")

    # ── Public API ──────────────────────────────────────────────

    async def add(self, event: InternalEvent) -> None:
        """Add an event to the current batch.  Triggers flush if full."""
        async with self._lock:
            self._buffer.append(event)
            if len(self._buffer) >= self._max_size:
                await self._flush_locked()

    # ── Flush logic ─────────────────────────────────────────────

    async def _periodic_flush(self) -> None:
        """Background task that flushes on a timer."""
        while self._running:
            await asyncio.sleep(self._flush_interval)
            await self._flush()

    async def _flush(self) -> None:
        async with self._lock:
            await self._flush_locked()

    async def _flush_locked(self) -> None:
        """Flush the buffer under the lock.  Caller must hold ``_lock``."""
        if not self._buffer:
            return

        batch = self._buffer.copy()
        self._buffer.clear()

        start = time.monotonic()
        success = await self._write_with_retry(batch)
        elapsed = time.monotonic() - start

        if success:
            BATCH_FLUSH_DURATION.observe(elapsed)
            EVENTS_PER_BATCH.observe(len(batch))
            logger.info(
                "Batch flushed",
                extra={
                    "batch_size": len(batch),
                    "flush_duration_s": round(elapsed, 3),
                },
            )
        else:
            logger.error(
                "Batch write failed after retries — events lost",
                extra={"batch_size": len(batch)},
            )

    async def _write_with_retry(self, batch: list[InternalEvent]) -> bool:
        """Attempt to write with exponential backoff + jitter."""
        delay = self._retry.backoff_base_seconds

        for attempt in range(1, self._retry.max_attempts + 1):
            try:
                await self._storage.write_batch(batch)
                return True
            except Exception:
                logger.warning(
                    "Batch write failed",
                    extra={"attempt": attempt, "max_attempts": self._retry.max_attempts},
                    exc_info=True,
                )
                if attempt < self._retry.max_attempts:
                    jitter = random.uniform(0, delay * 0.1)
                    await asyncio.sleep(delay + jitter)
                    delay = min(
                        delay * self._retry.backoff_multiplier,
                        self._retry.backoff_max_seconds,
                    )

        return False
