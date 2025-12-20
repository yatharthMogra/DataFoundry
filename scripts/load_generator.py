#!/usr/bin/env python3
"""HTTP load generator — sends concurrent POST requests to the webhook endpoint.

Usage:
    python scripts/load_generator.py --url http://localhost:8080/ingest --rate 100 --duration 30 --concurrency 10
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import statistics
import time
import uuid
from datetime import datetime, timezone

import httpx

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def _random_event() -> dict:
    """Generate a random webhook event payload."""
    event_type = random.choice(["order", "click", "signup", "pageview"])

    base = {
        "event_id": str(uuid.uuid4()),
        "source": f"webhook-{event_type}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    if event_type == "order":
        base["payload"] = {
            "order_id": f"ORD-{random.randint(10000, 99999)}",
            "amount": round(random.uniform(10.0, 999.99), 2),
            "currency": random.choice(["USD", "EUR", "GBP"]),
            "customer_id": f"C-{random.randint(1, 5000)}",
        }
    elif event_type == "click":
        base["payload"] = {
            "url": random.choice(["/home", "/products", "/checkout", "/search"]),
            "user_id": f"U-{random.randint(1, 5000)}",
            "device": random.choice(["mobile", "desktop"]),
        }
    elif event_type == "signup":
        base["payload"] = {
            "user_id": f"U-{random.randint(5000, 9999)}",
            "plan": random.choice(["free", "pro", "enterprise"]),
            "referral": random.choice(["google", "friend", "ad", None]),
        }
    else:
        base["payload"] = {
            "url": f"/page/{random.randint(1, 100)}",
            "session_id": str(uuid.uuid4()),
        }

    return base


async def _send_events(
    client: httpx.AsyncClient,
    url: str,
    rate: float,
    duration: float,
    results: list[dict],
) -> None:
    """Worker coroutine that sends events at approximately the target rate."""
    interval = 1.0 / rate if rate > 0 else 0.01
    end_time = time.monotonic() + duration

    while time.monotonic() < end_time:
        event = _random_event()
        start = time.monotonic()
        try:
            resp = await client.post(url, json=event)
            elapsed = time.monotonic() - start
            results.append({
                "status": resp.status_code,
                "latency": elapsed,
            })
        except Exception as e:
            elapsed = time.monotonic() - start
            results.append({
                "status": 0,
                "latency": elapsed,
                "error": str(e),
            })

        # Pace to target rate
        sleep_time = interval - elapsed
        if sleep_time > 0:
            await asyncio.sleep(sleep_time)


async def run_load_test(
    url: str,
    rate: float,
    duration: float,
    concurrency: int,
) -> None:
    per_worker_rate = rate / concurrency
    results: list[dict] = []

    logger.info(
        "Starting load test: url=%s rate=%.0f/s concurrency=%d duration=%.0fs",
        url, rate, concurrency, duration,
    )

    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [
            _send_events(client, url, per_worker_rate, duration, results)
            for _ in range(concurrency)
        ]
        start = time.monotonic()
        await asyncio.gather(*tasks)
        total_time = time.monotonic() - start

    # ── Report ──────────────────────────────────────────────────
    total = len(results)
    success = sum(1 for r in results if r["status"] == 202)
    rejected = sum(1 for r in results if r["status"] == 429)
    errors = sum(1 for r in results if r["status"] not in (202, 429))
    latencies = [r["latency"] for r in results if r["status"] == 202]

    logger.info("=" * 60)
    logger.info("LOAD TEST RESULTS")
    logger.info("=" * 60)
    logger.info("Total requests:   %d", total)
    logger.info("Successful (202): %d (%.1f%%)", success, 100 * success / max(total, 1))
    logger.info("Rejected (429):   %d (%.1f%%)", rejected, 100 * rejected / max(total, 1))
    logger.info("Errors:           %d", errors)
    logger.info("Throughput:       %.1f req/s", total / max(total_time, 0.001))

    if latencies:
        latencies.sort()
        logger.info("Latency p50:      %.3f ms", statistics.median(latencies) * 1000)
        logger.info("Latency p95:      %.3f ms", latencies[int(len(latencies) * 0.95)] * 1000)
        logger.info("Latency p99:      %.3f ms", latencies[int(len(latencies) * 0.99)] * 1000)
    logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="HTTP load generator for DataFlow webhook")
    parser.add_argument("--url", default="http://localhost:8080/ingest")
    parser.add_argument("--rate", type=float, default=100.0, help="Target requests/sec")
    parser.add_argument("--duration", type=float, default=30.0, help="Test duration in seconds")
    parser.add_argument("--concurrency", type=int, default=10, help="Number of concurrent workers")
    args = parser.parse_args()

    asyncio.run(run_load_test(args.url, args.rate, args.duration, args.concurrency))


if __name__ == "__main__":
    main()
