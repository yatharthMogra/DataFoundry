#!/usr/bin/env python3
"""Seed bad events — sends intentionally malformed payloads for DLQ demonstration.

Usage:
    python scripts/seed_bad_events.py --url http://localhost:8080/ingest --count 20
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _bad_events() -> list[dict]:
    """Generate a variety of malformed events."""
    now = datetime.now(timezone.utc)
    future = now + timedelta(days=365)

    return [
        # 1. Missing payload entirely
        {
            "event_id": str(uuid.uuid4()),
            "source": "bad-source",
            "payload": {},
        },
        # 2. Timestamp far in the future
        {
            "event_id": str(uuid.uuid4()),
            "source": "webhook-orders",
            "payload": {
                "order_id": "ORD-BAD-1",
                "timestamp": future.isoformat(),
                "amount": 42.0,
            },
        },
        # 3. Invalid event_id (not a UUID)
        {
            "event_id": "NOT-A-UUID-AT-ALL",
            "source": "webhook-clicks",
            "payload": {
                "url": "/test",
                "user_id": "U-1",
                "timestamp": now.isoformat(),
            },
        },
        # 4. Empty source
        {
            "event_id": str(uuid.uuid4()),
            "source": "",
            "payload": {
                "data": "test",
                "timestamp": now.isoformat(),
            },
        },
        # 5. Duplicate ID (will be sent twice)
        {
            "event_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "source": "webhook-orders",
            "payload": {
                "order_id": "ORD-DUP",
                "amount": 100.0,
                "timestamp": now.isoformat(),
            },
        },
        # 6. Same duplicate ID again
        {
            "event_id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
            "source": "webhook-orders",
            "payload": {
                "order_id": "ORD-DUP",
                "amount": 100.0,
                "timestamp": now.isoformat(),
            },
        },
        # 7. Negative version (won't match but payload-level issue)
        {
            "event_id": str(uuid.uuid4()),
            "source": "webhook",
            "payload": {
                "timestamp": now.isoformat(),
                "data": None,
            },
        },
        # 8. Extremely nested payload
        {
            "event_id": str(uuid.uuid4()),
            "source": "webhook-nested",
            "payload": {
                "level1": {"level2": {"level3": {"level4": {"level5": "deep"}}}},
                "timestamp": now.isoformat(),
            },
        },
    ]


async def send_bad_events(url: str, count: int) -> None:
    templates = _bad_events()
    logger.info("Sending %d bad events to %s", count, url)

    async with httpx.AsyncClient(timeout=10.0) as client:
        sent = 0
        for i in range(count):
            event = templates[i % len(templates)].copy()
            # Give non-duplicate events fresh IDs on repeat cycles
            if i >= len(templates) and event.get("event_id") != "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee":
                event["event_id"] = str(uuid.uuid4())

            try:
                resp = await client.post(url, json=event)
                sent += 1
                logger.info(
                    "Sent event %d/%d: id=%s status=%d",
                    i + 1, count, event.get("event_id", "?")[:12], resp.status_code,
                )
            except Exception as e:
                logger.error("Failed to send event %d: %s", i + 1, e)

    logger.info("Done — sent %d bad events", sent)


def main() -> None:
    parser = argparse.ArgumentParser(description="Send malformed events for DLQ demo")
    parser.add_argument("--url", default="http://localhost:8080/ingest")
    parser.add_argument("--count", type=int, default=20, help="Number of bad events to send")
    args = parser.parse_args()

    asyncio.run(send_bad_events(args.url, args.count))


if __name__ == "__main__":
    main()
