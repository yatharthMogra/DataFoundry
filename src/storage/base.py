"""Abstract base class for storage backends."""

from __future__ import annotations

from abc import ABC, abstractmethod

from src.models import InternalEvent


class StorageBackend(ABC):
    """Interface that all storage backends must implement."""

    @abstractmethod
    async def write_batch(self, events: list[InternalEvent]) -> None:
        """Write a batch of events to the underlying store."""
        ...

    @abstractmethod
    async def close(self) -> None:
        """Release any resources held by the backend."""
        ...
