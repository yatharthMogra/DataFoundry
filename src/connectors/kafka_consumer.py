"""Kafka source connector with backpressure-aware consumption."""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from typing import Any

from aiokafka import AIOKafkaConsumer, TopicPartition

from src.config import KafkaSourceConfig
from src.models import RawEvent
from src.queue import BoundedAsyncQueue

logger = logging.getLogger(__name__)


class KafkaSourceConnector:
    """Consumes messages from one or more Kafka topics and pushes them
    into the shared :class:`BoundedAsyncQueue`.

    Implements at-least-once semantics: offsets are committed only after
    the message has been successfully enqueued.  When the queue signals
    backpressure, the consumer pauses its assigned partitions and waits
    for the queue to drain before resuming.

    Parameters
    ----------
    config:
        Kafka source configuration (bootstrap servers, topics, group, etc.).
    queue:
        Shared bounded async queue for downstream processing.
    """

    def __init__(self, config: KafkaSourceConfig, queue: BoundedAsyncQueue) -> None:
        self._config = config
        self._queue = queue
        self._consumer: AIOKafkaConsumer | None = None
        self._running = False
        self._paused_partitions: set[TopicPartition] = set()

    # ── Lifecycle ───────────────────────────────────────────────

    async def start(self) -> None:
        """Create the Kafka consumer, subscribe, and begin consuming."""
        self._consumer = AIOKafkaConsumer(
            *self._config.topics,
            bootstrap_servers=self._config.bootstrap_servers,
            group_id=self._config.group_id,
            auto_offset_reset=self._config.auto_offset_reset,
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        await self._consumer.start()
        self._running = True
        logger.info(
            "Kafka consumer started",
            extra={
                "topics": self._config.topics,
                "group_id": self._config.group_id,
                "bootstrap_servers": self._config.bootstrap_servers,
            },
        )

    async def stop(self) -> None:
        """Gracefully stop the consumer, committing final offsets."""
        self._running = False
        if self._consumer is not None:
            try:
                await self._consumer.commit()
            except Exception:
                logger.warning("Failed to commit offsets during shutdown", exc_info=True)
            await self._consumer.stop()
            logger.info("Kafka consumer stopped")

    # ── Main consume loop ───────────────────────────────────────

    async def run(self) -> None:
        """Main consumption loop.  Call :meth:`start` before this."""
        assert self._consumer is not None, "call start() first"

        while self._running:
            try:
                await self._consume_batch()
            except asyncio.CancelledError:
                logger.info("Kafka consume loop cancelled")
                break
            except Exception:
                logger.exception("Unexpected error in Kafka consume loop")
                await asyncio.sleep(1)  # back off briefly on unexpected errors

    async def _consume_batch(self) -> None:
        """Fetch one batch of records and enqueue them."""
        assert self._consumer is not None

        # If backpressure is active, pause partitions and wait
        if self._queue.is_backpressured:
            await self._pause_all()
            await self._queue.wait_until_ready()
            await self._resume_all()

        # Poll for records (100 ms timeout)
        records = await self._consumer.getmany(timeout_ms=100, max_records=100)

        for tp, messages in records.items():
            source_label = f"kafka-{tp.topic}"
            for msg in messages:
                raw = self._to_raw_event(msg.value, source_label)
                # Blocking put — will naturally throttle if queue is near full
                await self._queue.put(raw)

            # Commit after the partition's batch is enqueued
            await self._consumer.commit({tp: messages[-1].offset + 1})

    # ── Pause / resume helpers ──────────────────────────────────

    async def _pause_all(self) -> None:
        assert self._consumer is not None
        partitions = self._consumer.assignment()
        if partitions:
            self._consumer.pause(*partitions)
            self._paused_partitions = set(partitions)
            logger.warning(
                "Kafka consumer paused due to backpressure",
                extra={"partitions": [str(p) for p in partitions]},
            )

    async def _resume_all(self) -> None:
        assert self._consumer is not None
        if self._paused_partitions:
            self._consumer.resume(*self._paused_partitions)
            logger.info(
                "Kafka consumer resumed after backpressure cleared",
                extra={"partitions": [str(p) for p in self._paused_partitions]},
            )
            self._paused_partitions.clear()

    # ── Helpers ─────────────────────────────────────────────────

    @staticmethod
    def _to_raw_event(value: Any, source: str) -> RawEvent:
        """Convert a deserialized Kafka message value into a RawEvent."""
        if isinstance(value, dict):
            return RawEvent(
                event_id=value.get("event_id", None) or RawEvent.__fields__["event_id"].default_factory(),
                source=source,
                payload=value,
                received_at=datetime.now(timezone.utc),
            )
        # Non-dict payloads get wrapped
        return RawEvent(
            source=source,
            payload={"raw": value},
            received_at=datetime.now(timezone.utc),
        )
