"""DataFlow — main entrypoint and lifecycle orchestration."""

from __future__ import annotations

import asyncio
import logging
import signal
import sys

import uvicorn

from src.config import load_config
from src.connectors.kafka_consumer import KafkaSourceConnector
from src.connectors.webhook import create_webhook_app
from src.dlq.store import DLQStore
from src.logging.setup import setup_logging
from src.metrics.prometheus import DLQ_SIZE, QUEUE_LENGTH, start_metrics_server
from src.pipeline.dedup import DeduplicationCache
from src.pipeline.worker import WorkerPool
from src.queue import BoundedAsyncQueue
from src.storage.batch import BatchAccumulator
from src.storage.parquet_writer import ParquetWriter
from src.storage.postgres_writer import PostgresWriter

logger = logging.getLogger(__name__)


async def _run() -> None:
    # ── Load config ─────────────────────────────────────────────
    config = load_config()
    setup_logging(config.logging)

    logger.info("DataFlow starting", extra={"config": config.model_dump(mode="json")})

    # ── Metrics server (runs in a background thread) ────────────
    start_metrics_server(config.metrics)

    # ── Internal queue ──────────────────────────────────────────
    queue = BoundedAsyncQueue(
        max_size=config.queue.max_size,
        high_water_mark=config.queue.high_water_mark,
        low_water_mark=config.queue.low_water_mark,
    )

    # ── Dedup cache ─────────────────────────────────────────────
    dedup = DeduplicationCache(config.dedup)
    await dedup.start()

    # ── DLQ store ───────────────────────────────────────────────
    dlq = DLQStore(config.dlq)
    await dlq.start()

    # ── Storage backend ─────────────────────────────────────────
    if config.storage.backend == "postgres":
        storage = PostgresWriter(config.storage.postgres)
        await storage.start()
    else:
        storage = ParquetWriter(config.storage.parquet)

    # ── Batch accumulator ───────────────────────────────────────
    batch = BatchAccumulator(
        storage=storage,
        batch_config=config.batch,
        retry_config=config.retry,
    )
    await batch.start()

    # ── Worker pool ─────────────────────────────────────────────
    workers = WorkerPool(
        num_workers=config.pipeline.num_workers,
        queue=queue,
        dedup=dedup,
        dlq=dlq,
        batch=batch,
    )
    await workers.start()

    # ── Webhook server ──────────────────────────────────────────
    webhook_app = create_webhook_app(queue, source_name="webhook")
    # Attach DLQ inspection routes
    webhook_app.include_router(dlq.create_router())

    webhook_config = uvicorn.Config(
        app=webhook_app,
        host=config.sources.webhook.host,
        port=config.sources.webhook.port,
        log_level="warning",
        access_log=False,
    )
    webhook_server = uvicorn.Server(webhook_config)

    # ── Kafka consumer ──────────────────────────────────────────
    kafka_connector: KafkaSourceConnector | None = None
    kafka_task: asyncio.Task | None = None

    if config.sources.kafka.enabled:
        kafka_connector = KafkaSourceConnector(config.sources.kafka, queue)
        try:
            await kafka_connector.start()
            kafka_task = asyncio.create_task(
                kafka_connector.run(), name="kafka-consumer"
            )
        except Exception:
            logger.warning(
                "Kafka consumer failed to start — running without Kafka",
                exc_info=True,
            )
            kafka_connector = None

    # ── Metrics gauge updater ───────────────────────────────────
    async def _update_gauges() -> None:
        while True:
            QUEUE_LENGTH.set(queue.size)
            DLQ_SIZE.set(await dlq.count())
            await asyncio.sleep(5)

    gauge_task = asyncio.create_task(_update_gauges(), name="gauge-updater")

    # ── Graceful shutdown ───────────────────────────────────────
    shutdown_event = asyncio.Event()

    def _handle_signal(sig: signal.Signals) -> None:
        logger.info("Received signal, initiating graceful shutdown", extra={"signal": sig.name})
        shutdown_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal, sig)

    # ── Run everything ──────────────────────────────────────────
    logger.info("DataFlow is ready — all components running")

    webhook_task = asyncio.create_task(webhook_server.serve(), name="webhook-server")

    # Wait for shutdown signal
    await shutdown_event.wait()

    logger.info("Shutting down...")

    # 1. Stop accepting new events
    webhook_server.should_exit = True
    await webhook_task

    # 2. Stop Kafka consumer
    if kafka_connector is not None:
        await kafka_connector.stop()
    if kafka_task is not None:
        kafka_task.cancel()
        try:
            await kafka_task
        except asyncio.CancelledError:
            pass

    # 3. Drain worker pool
    await workers.stop()

    # 4. Flush remaining batches and close storage
    await batch.stop()

    # 5. Close dedup and DLQ
    await dedup.stop()
    await dlq.stop()

    # 6. Cancel gauge updater
    gauge_task.cancel()
    try:
        await gauge_task
    except asyncio.CancelledError:
        pass

    logger.info("DataFlow shutdown complete")


def main() -> None:
    """CLI entrypoint."""
    try:
        asyncio.run(_run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
