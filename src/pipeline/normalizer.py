"""Schema normalization: maps heterogeneous source payloads to InternalEvent."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Callable

from src.models import InternalEvent, RawEvent

logger = logging.getLogger(__name__)

# Type alias for a source-specific mapping function.
# Each mapper receives the raw payload dict and returns
# (event_timestamp, cleaned_payload).
SourceMapper = Callable[[dict[str, Any]], tuple[datetime, dict[str, Any]]]


# ── Built-in source mappers ─────────────────────────────────────


def _parse_timestamp(value: Any) -> datetime:
    """Best-effort timestamp parsing from various formats."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, (int, float)):
        # Treat as epoch seconds (or millis if > 1e12)
        ts = value if value < 1e12 else value / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)

    if isinstance(value, str):
        # Try ISO-8601 first, then common variants
        for fmt in (
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(value, fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except ValueError:
                continue

    # Fallback: use current time
    logger.warning("Could not parse timestamp, using utcnow", extra={"raw_value": value})
    return datetime.now(timezone.utc)


def _map_orders(payload: dict[str, Any]) -> tuple[datetime, dict[str, Any]]:
    """Normalizer for ``kafka-orders`` events.

    Expected source fields: ``order_id``, ``created_at``, ``amount``, ``currency``, ``customer_id``.
    """
    event_ts = _parse_timestamp(payload.get("created_at") or payload.get("timestamp"))
    cleaned = {
        "order_id": payload.get("order_id"),
        "amount": payload.get("amount"),
        "currency": payload.get("currency", "USD"),
        "customer_id": payload.get("customer_id"),
    }
    return event_ts, cleaned


def _map_clicks(payload: dict[str, Any]) -> tuple[datetime, dict[str, Any]]:
    """Normalizer for ``kafka-clicks`` events.

    Expected source fields: ``click_id``, ``url``, ``user_id``, ``ts``.
    """
    event_ts = _parse_timestamp(payload.get("ts") or payload.get("timestamp"))
    cleaned = {
        "click_id": payload.get("click_id"),
        "url": payload.get("url"),
        "user_id": payload.get("user_id"),
        "referrer": payload.get("referrer"),
    }
    return event_ts, cleaned


def _map_generic(payload: dict[str, Any]) -> tuple[datetime, dict[str, Any]]:
    """Fallback normalizer for unknown sources."""
    event_ts = _parse_timestamp(
        payload.get("timestamp")
        or payload.get("event_timestamp")
        or payload.get("ts")
        or payload.get("created_at")
    )
    return event_ts, payload


# ── Registry ────────────────────────────────────────────────────

_SOURCE_MAPPERS: dict[str, SourceMapper] = {
    "kafka-orders": _map_orders,
    "kafka-clicks": _map_clicks,
}


def register_mapper(source: str, mapper: SourceMapper) -> None:
    """Register a custom mapper for a given source name."""
    _SOURCE_MAPPERS[source] = mapper


# ── Public API ──────────────────────────────────────────────────


def normalize(raw: RawEvent) -> InternalEvent:
    """Transform a :class:`RawEvent` into a canonical :class:`InternalEvent`.

    Uses the source-specific mapper if one is registered, otherwise falls
    back to the generic mapper.
    """
    mapper = _SOURCE_MAPPERS.get(raw.source, _map_generic)
    event_ts, cleaned_payload = mapper(raw.payload)

    return InternalEvent(
        event_id=raw.event_id,
        source=raw.source,
        ingest_timestamp=raw.received_at,
        event_timestamp=event_ts,
        payload=cleaned_payload,
        version=1,
    )
