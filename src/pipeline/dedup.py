"""Deduplication cache — detects and drops duplicate events by event_id."""

from __future__ import annotations

import logging
from collections import OrderedDict

from src.config import DedupConfig

logger = logging.getLogger(__name__)


class DeduplicationCache:
    """In-memory LRU cache for detecting duplicate ``event_id`` values.

    When the cache exceeds *max_size*, the oldest entries are evicted
    automatically (LRU order).  An optional Redis backend can be plugged
    in for distributed deployments.

    Parameters
    ----------
    config:
        Dedup configuration (backend type, max size, Redis URL, TTL).
    """

    def __init__(self, config: DedupConfig) -> None:
        self._config = config
        self._cache: OrderedDict[str, bool] = OrderedDict()
        self._max_size = config.max_size
        self._redis = None  # lazy init if backend == "redis"

    async def start(self) -> None:
        """Initialize the Redis connection if configured."""
        if self._config.backend == "redis":
            try:
                import redis.asyncio as aioredis

                self._redis = aioredis.from_url(
                    self._config.redis_url, decode_responses=True
                )
                logger.info("Dedup cache using Redis backend", extra={"url": self._config.redis_url})
            except Exception:
                logger.warning("Redis unavailable, falling back to in-memory dedup", exc_info=True)
                self._redis = None
        else:
            logger.info("Dedup cache using in-memory LRU backend", extra={"max_size": self._max_size})

    async def stop(self) -> None:
        if self._redis is not None:
            await self._redis.aclose()

    async def is_duplicate(self, event_id: str) -> bool:
        """Check whether *event_id* has been seen before.

        If it has **not** been seen, the ID is recorded and ``False`` is
        returned.  If it **has** been seen, ``True`` is returned.
        """
        if not self._config.enabled:
            return False

        if self._redis is not None:
            return await self._check_redis(event_id)

        return self._check_memory(event_id)

    # ── In-memory backend ───────────────────────────────────────

    def _check_memory(self, event_id: str) -> bool:
        if event_id in self._cache:
            # Move to end (most recently seen)
            self._cache.move_to_end(event_id)
            return True

        # Add new entry; evict oldest if over capacity
        self._cache[event_id] = True
        if len(self._cache) > self._max_size:
            self._cache.popitem(last=False)

        return False

    # ── Redis backend ───────────────────────────────────────────

    async def _check_redis(self, event_id: str) -> bool:
        assert self._redis is not None
        key = f"dedup:{event_id}"
        # SETNX returns True if the key was set (i.e. first time)
        was_set = await self._redis.set(key, "1", nx=True, ex=self._config.ttl_seconds)
        return not was_set  # True means duplicate (key already existed)

    # ── Stats ───────────────────────────────────────────────────

    @property
    def size(self) -> int:
        return len(self._cache)
