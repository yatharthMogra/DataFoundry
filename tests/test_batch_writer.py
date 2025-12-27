"""Tests for the batch accumulator and flush logic."""

import asyncio
import uuid
from datetime import datetime, timezone

import pytest

from src.config import BatchConfig, RetryConfig
from src.models import InternalEvent
from src.storage.base import StorageBackend
from src.storage.batch import BatchAccumulator


# ── Mock storage backend ────────────────────────────────────────


class MockStorage(StorageBackend):
    def __init__(self, fail_count: int = 0):
        self.batches: list[list[InternalEvent]] = []
        self._fail_count = fail_count
        self._call_count = 0

    async def write_batch(self, events: list[InternalEvent]) -> None:
        self._call_count += 1
        if self._call_count <= self._fail_count:
            raise RuntimeError("Simulated write failure")
        self.batches.append(events)

    async def close(self) -> None:
        pass


def _make_event() -> InternalEvent:
    return InternalEvent(
        event_id=str(uuid.uuid4()),
        source="test",
        event_timestamp=datetime.now(timezone.utc),
        payload={"k": "v"},
    )


class TestBatchAccumulator:
    @pytest.mark.asyncio
    async def test_flush_on_size(self):
        storage = MockStorage()
        batch = BatchAccumulator(
            storage=storage,
            batch_config=BatchConfig(max_size=3, max_flush_interval_seconds=60),
            retry_config=RetryConfig(),
        )
        await batch.start()

        for _ in range(3):
            await batch.add(_make_event())

        # Give the flush a moment to complete
        await asyncio.sleep(0.1)

        assert len(storage.batches) == 1
        assert len(storage.batches[0]) == 3

        await batch.stop()

    @pytest.mark.asyncio
    async def test_flush_on_interval(self):
        storage = MockStorage()
        batch = BatchAccumulator(
            storage=storage,
            batch_config=BatchConfig(max_size=1000, max_flush_interval_seconds=0.2),
            retry_config=RetryConfig(),
        )
        await batch.start()

        await batch.add(_make_event())

        # Wait for periodic flush
        await asyncio.sleep(0.5)

        assert len(storage.batches) >= 1
        await batch.stop()

    @pytest.mark.asyncio
    async def test_final_flush_on_stop(self):
        storage = MockStorage()
        batch = BatchAccumulator(
            storage=storage,
            batch_config=BatchConfig(max_size=1000, max_flush_interval_seconds=60),
            retry_config=RetryConfig(),
        )
        await batch.start()

        await batch.add(_make_event())
        await batch.add(_make_event())

        # No flush triggered by size or time yet
        assert len(storage.batches) == 0

        # Stop should trigger final flush
        await batch.stop()
        assert len(storage.batches) == 1
        assert len(storage.batches[0]) == 2

    @pytest.mark.asyncio
    async def test_retry_on_failure(self):
        # Fail first attempt, succeed on second
        storage = MockStorage(fail_count=1)
        batch = BatchAccumulator(
            storage=storage,
            batch_config=BatchConfig(max_size=1, max_flush_interval_seconds=60),
            retry_config=RetryConfig(
                max_attempts=3,
                backoff_base_seconds=0.01,
                backoff_multiplier=1.0,
                backoff_max_seconds=0.1,
            ),
        )
        await batch.start()

        await batch.add(_make_event())
        await asyncio.sleep(0.5)

        assert len(storage.batches) == 1
        await batch.stop()
