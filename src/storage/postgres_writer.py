"""PostgreSQL storage backend — writes batches via asyncpg."""

from __future__ import annotations

import json
import logging

import asyncpg

from src.config import PostgresStorageConfig
from src.models import InternalEvent
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

_DDL = """\
CREATE TABLE IF NOT EXISTS events (
    event_id       TEXT PRIMARY KEY,
    source         TEXT NOT NULL,
    ingest_timestamp TIMESTAMPTZ NOT NULL,
    event_timestamp  TIMESTAMPTZ NOT NULL,
    payload        JSONB NOT NULL,
    version        INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_events_source ON events (source);
CREATE INDEX IF NOT EXISTS idx_events_event_ts ON events (event_timestamp);
"""

_INSERT = """\
INSERT INTO events (event_id, source, ingest_timestamp, event_timestamp, payload, version)
VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (event_id) DO NOTHING;
"""


class PostgresWriter(StorageBackend):
    """Writes event batches to a PostgreSQL ``events`` table.

    Uses ``asyncpg`` for async, high-throughput batch inserts.  The table
    and indexes are auto-created on first connection.

    Parameters
    ----------
    config:
        Postgres-specific configuration (DSN).
    """

    def __init__(self, config: PostgresStorageConfig) -> None:
        self._dsn = config.dsn
        self._pool: asyncpg.Pool | None = None

    async def start(self) -> None:
        """Create the connection pool and ensure the schema exists."""
        self._pool = await asyncpg.create_pool(dsn=self._dsn, min_size=2, max_size=10)
        async with self._pool.acquire() as conn:
            await conn.execute(_DDL)
        logger.info("PostgreSQL writer initialized", extra={"dsn": self._dsn})

    async def write_batch(self, events: list[InternalEvent]) -> None:
        if not events or self._pool is None:
            return

        records = [
            (
                e.event_id,
                e.source,
                e.ingest_timestamp,
                e.event_timestamp,
                json.dumps(e.payload),
                e.version,
            )
            for e in events
        ]

        async with self._pool.acquire() as conn:
            await conn.executemany(_INSERT, records)

        logger.info(
            "PostgreSQL batch written",
            extra={"num_rows": len(events)},
        )

    async def close(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            logger.info("PostgreSQL writer closed")
