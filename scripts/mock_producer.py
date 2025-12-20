#!/usr/bin/env python3
"""Mock Kafka producer — generates realistic events and publishes them to Kafka topics.

Usage:
    python scripts/mock_producer.py --topics orders clicks --rate 50 --duration 60
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import random
import sys
import time
import uuid
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# ── Event generators ────────────────────────────────────────────

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
URLS = [
    "/products/123",
    "/products/456",
    "/checkout",
    "/cart",
    "/home",
    "/search?q=shoes",
    "/categories/electronics",
    "/deals",
]
REFERRERS = [
    "https://google.com",
    "https://facebook.com",
    "https://twitter.com",
    "direct",
    None,
]


def _generate_order() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{random.randint(10000, 99999)}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "amount": round(random.uniform(5.0, 500.0), 2),
        "currency": random.choice(CURRENCIES),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "items": random.randint(1, 10),
    }


def _generate_click() -> dict:
    return {
        "event_id": str(uuid.uuid4()),
        "click_id": str(uuid.uuid4()),
        "url": random.choice(URLS),
        "user_id": f"USER-{random.randint(1000, 9999)}",
        "ts": int(time.time() * 1000),  # millis
        "referrer": random.choice(REFERRERS),
        "device": random.choice(["mobile", "desktop", "tablet"]),
    }


GENERATORS = {
    "orders": _generate_order,
    "clicks": _generate_click,
}


# ── Main ────────────────────────────────────────────────────────


async def produce(
    bootstrap_servers: str,
    topics: list[str],
    rate: float,
    duration: float,
) -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )
    await producer.start()
    logger.info(
        "Producer started: topics=%s rate=%.1f/s duration=%.0fs",
        topics, rate, duration,
    )

    interval = 1.0 / rate if rate > 0 else 0.1
    end_time = time.monotonic() + duration
    count = 0

    try:
        while time.monotonic() < end_time:
            topic = random.choice(topics)
            gen = GENERATORS.get(topic, _generate_order)
            event = gen()

            await producer.send_and_wait(topic, event)
            count += 1

            if count % 100 == 0:
                logger.info("Produced %d events so far", count)

            await asyncio.sleep(interval)
    finally:
        await producer.stop()
        logger.info("Producer finished: %d events sent", count)


def main() -> None:
    parser = argparse.ArgumentParser(description="Mock Kafka event producer")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topics", nargs="+", default=["orders", "clicks"])
    parser.add_argument("--rate", type=float, default=10.0, help="Events per second")
    parser.add_argument("--duration", type=float, default=30.0, help="Seconds to run")
    args = parser.parse_args()

    asyncio.run(produce(args.bootstrap_servers, args.topics, args.rate, args.duration))


if __name__ == "__main__":
    main()
