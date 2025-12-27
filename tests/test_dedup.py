"""Tests for the deduplication cache."""

import pytest

from src.config import DedupConfig
from src.pipeline.dedup import DeduplicationCache


@pytest.fixture
def dedup_config():
    return DedupConfig(enabled=True, backend="memory", max_size=5)


@pytest.fixture
def disabled_config():
    return DedupConfig(enabled=False, backend="memory", max_size=5)


class TestDeduplicationCache:
    @pytest.mark.asyncio
    async def test_first_seen_not_duplicate(self, dedup_config):
        cache = DeduplicationCache(dedup_config)
        await cache.start()
        assert await cache.is_duplicate("event-1") is False

    @pytest.mark.asyncio
    async def test_second_seen_is_duplicate(self, dedup_config):
        cache = DeduplicationCache(dedup_config)
        await cache.start()
        await cache.is_duplicate("event-1")
        assert await cache.is_duplicate("event-1") is True

    @pytest.mark.asyncio
    async def test_different_ids_not_duplicate(self, dedup_config):
        cache = DeduplicationCache(dedup_config)
        await cache.start()
        await cache.is_duplicate("event-1")
        assert await cache.is_duplicate("event-2") is False

    @pytest.mark.asyncio
    async def test_lru_eviction(self, dedup_config):
        """After max_size items, oldest entries should be evicted."""
        cache = DeduplicationCache(dedup_config)
        await cache.start()

        # Fill cache to capacity (max_size=5)
        for i in range(5):
            await cache.is_duplicate(f"event-{i}")

        # Add one more to trigger eviction of event-0
        await cache.is_duplicate("event-5")

        # event-0 should have been evicted
        assert await cache.is_duplicate("event-0") is False
        # event-1 should still be there
        assert await cache.is_duplicate("event-1") is True

    @pytest.mark.asyncio
    async def test_disabled_never_deduplicates(self, disabled_config):
        cache = DeduplicationCache(disabled_config)
        await cache.start()
        await cache.is_duplicate("event-1")
        assert await cache.is_duplicate("event-1") is False  # never dup when disabled

    @pytest.mark.asyncio
    async def test_size_property(self, dedup_config):
        cache = DeduplicationCache(dedup_config)
        await cache.start()
        assert cache.size == 0
        await cache.is_duplicate("a")
        assert cache.size == 1
        await cache.is_duplicate("b")
        assert cache.size == 2
