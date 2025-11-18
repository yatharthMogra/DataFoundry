"""Dead-Letter Queue store — persists rejected events for inspection."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Query

from src.config import DLQConfig
from src.models import DLQRecord

logger = logging.getLogger(__name__)


class DLQStore:
    """Stores rejected events so operators can inspect and replay them.

    Supports multiple backends:
    - ``memory``: in-process list (non-durable, good for dev/test).
    - ``file``: append-only JSON-lines file (durable, simple).

    Parameters
    ----------
    config:
        DLQ configuration (backend, file path).
    """

    def __init__(self, config: DLQConfig) -> None:
        self._config = config
        self._records: list[DLQRecord] = []
        self._file_handle: Any = None

    async def start(self) -> None:
        if self._config.backend == "file":
            path = Path(self._config.file_path)
            path.parent.mkdir(parents=True, exist_ok=True)
            self._file_handle = open(path, "a", encoding="utf-8")
            logger.info("DLQ store using file backend", extra={"path": str(path)})
        else:
            logger.info("DLQ store using in-memory backend")

    async def stop(self) -> None:
        if self._file_handle is not None:
            self._file_handle.close()

    async def push(self, record: DLQRecord) -> None:
        """Persist a rejected event to the dead-letter queue."""
        self._records.append(record)

        if self._file_handle is not None:
            line = record.model_dump_json() + "\n"
            self._file_handle.write(line)
            self._file_handle.flush()

        logger.warning(
            "Event sent to DLQ",
            extra={
                "event_id": record.event_id,
                "source": record.source,
                "error_reason": record.error_reason,
            },
        )

    async def list_records(self, limit: int = 50, offset: int = 0) -> list[DLQRecord]:
        """Return a page of DLQ records (most recent first)."""
        ordered = list(reversed(self._records))
        return ordered[offset : offset + limit]

    async def count(self) -> int:
        return len(self._records)

    # ── FastAPI sub-router ──────────────────────────────────────

    def create_router(self) -> APIRouter:
        """Return a FastAPI router that exposes DLQ inspection endpoints."""
        router = APIRouter(prefix="/dlq", tags=["Dead-Letter Queue"])

        @router.get("")
        async def list_dlq(
            limit: int = Query(50, ge=1, le=500),
            offset: int = Query(0, ge=0),
        ) -> dict:
            records = await self.list_records(limit=limit, offset=offset)
            return {
                "total": await self.count(),
                "offset": offset,
                "limit": limit,
                "records": [r.model_dump(mode="json") for r in records],
            }

        @router.get("/count")
        async def dlq_count() -> dict:
            return {"count": await self.count()}

        return router
