"""Bounded async queue with backpressure signaling for DataFlow."""

from __future__ import annotations

import asyncio
from enum import Enum, auto


class BackpressureState(Enum):
    """Indicates the current pressure level of the queue."""

    OK = auto()
    HIGH = auto()


class BoundedAsyncQueue:
    """An ``asyncio.Queue`` wrapper with high/low water-mark backpressure.

    Parameters
    ----------
    max_size:
        Maximum number of items the queue can hold.
    high_water_mark:
        Fraction of *max_size* (0-1) at which backpressure is signalled.
    low_water_mark:
        Fraction of *max_size* (0-1) at which backpressure is released.
    """

    def __init__(
        self,
        max_size: int = 10_000,
        high_water_mark: float = 0.9,
        low_water_mark: float = 0.5,
    ) -> None:
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=max_size)
        self._max_size = max_size
        self._high = int(max_size * high_water_mark)
        self._low = int(max_size * low_water_mark)
        self._state = BackpressureState.OK
        self._backpressure_event = asyncio.Event()
        self._backpressure_event.set()  # start in OK state (event is "clear")

    # ── Public properties ───────────────────────────────────────

    @property
    def size(self) -> int:
        return self._queue.qsize()

    @property
    def max_size(self) -> int:
        return self._max_size

    @property
    def full(self) -> bool:
        return self._queue.full()

    @property
    def state(self) -> BackpressureState:
        return self._state

    @property
    def is_backpressured(self) -> bool:
        return self._state is BackpressureState.HIGH

    # ── Core operations ─────────────────────────────────────────

    async def put(self, item: object, timeout: float | None = None) -> bool:
        """Enqueue an item.

        Returns ``True`` if the item was placed successfully. If the queue
        is completely full and *timeout* expires, returns ``False``.
        """
        try:
            if timeout is not None:
                await asyncio.wait_for(self._queue.put(item), timeout=timeout)
            else:
                await self._queue.put(item)
        except (asyncio.QueueFull, asyncio.TimeoutError):
            return False

        self._check_high_water()
        return True

    def put_nowait(self, item: object) -> bool:
        """Non-blocking enqueue.  Returns ``False`` when the queue is full."""
        try:
            self._queue.put_nowait(item)
        except asyncio.QueueFull:
            return False
        self._check_high_water()
        return True

    async def get(self, timeout: float | None = None) -> object:
        """Dequeue an item.

        Raises ``asyncio.TimeoutError`` if *timeout* is set and expires.
        """
        if timeout is not None:
            item = await asyncio.wait_for(self._queue.get(), timeout=timeout)
        else:
            item = await self._queue.get()

        self._check_low_water()
        return item

    # ── Backpressure helpers ────────────────────────────────────

    async def wait_until_ready(self) -> None:
        """Block until the queue drops below the high-water mark."""
        await self._backpressure_event.wait()

    def _check_high_water(self) -> None:
        if self._state is BackpressureState.OK and self.size >= self._high:
            self._state = BackpressureState.HIGH
            self._backpressure_event.clear()

    def _check_low_water(self) -> None:
        if self._state is BackpressureState.HIGH and self.size <= self._low:
            self._state = BackpressureState.OK
            self._backpressure_event.set()
