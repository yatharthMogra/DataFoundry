"""Parquet storage backend — writes batches as partitioned Parquet files."""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import ParquetStorageConfig
from src.models import InternalEvent
from src.storage.base import StorageBackend

logger = logging.getLogger(__name__)

# Parquet schema matching InternalEvent
_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("ingest_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("event_timestamp", pa.timestamp("us", tz="UTC")),
        pa.field("payload", pa.string()),  # JSON-encoded
        pa.field("version", pa.int32()),
    ]
)


class ParquetWriter(StorageBackend):
    """Writes event batches to date-partitioned Parquet files.

    Output layout::

        {output_dir}/{YYYY-MM-DD}/part-{sequence}.parquet

    Parameters
    ----------
    config:
        Parquet-specific configuration (output directory).
    """

    def __init__(self, config: ParquetStorageConfig) -> None:
        self._output_dir = Path(config.output_dir)
        self._sequence = 0

    async def write_batch(self, events: list[InternalEvent]) -> None:
        if not events:
            return

        # Partition by date of first event's ingest_timestamp
        partition_date = events[0].ingest_timestamp.strftime("%Y-%m-%d")
        out_dir = self._output_dir / partition_date
        out_dir.mkdir(parents=True, exist_ok=True)

        file_path = out_dir / f"part-{self._sequence:05d}.parquet"
        self._sequence += 1

        # Build columnar arrays
        table = pa.table(
            {
                "event_id": [e.event_id for e in events],
                "source": [e.source for e in events],
                "ingest_timestamp": [e.ingest_timestamp for e in events],
                "event_timestamp": [e.event_timestamp for e in events],
                "payload": [json.dumps(e.payload) for e in events],
                "version": [e.version for e in events],
            },
            schema=_SCHEMA,
        )

        pq.write_table(table, file_path, compression="snappy")

        logger.info(
            "Parquet file written",
            extra={
                "path": str(file_path),
                "num_rows": len(events),
                "size_bytes": file_path.stat().st_size,
            },
        )

    async def close(self) -> None:
        logger.info("Parquet writer closed")
