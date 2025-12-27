"""Tests for the schema normalizer."""

import uuid
from datetime import datetime, timezone

from src.models import RawEvent
from src.pipeline.normalizer import normalize


class TestNormalizeOrders:
    def test_order_event(self):
        raw = RawEvent(
            event_id=str(uuid.uuid4()),
            source="kafka-orders",
            payload={
                "order_id": "ORD-123",
                "created_at": "2025-01-15T10:30:00Z",
                "amount": 99.99,
                "currency": "EUR",
                "customer_id": "CUST-42",
            },
        )
        event = normalize(raw)
        assert event.event_id == raw.event_id
        assert event.source == "kafka-orders"
        assert event.payload["order_id"] == "ORD-123"
        assert event.payload["amount"] == 99.99
        assert event.payload["currency"] == "EUR"

    def test_order_default_currency(self):
        raw = RawEvent(
            source="kafka-orders",
            payload={
                "order_id": "ORD-456",
                "created_at": "2025-06-01T00:00:00Z",
                "amount": 10.0,
            },
        )
        event = normalize(raw)
        assert event.payload["currency"] == "USD"


class TestNormalizeClicks:
    def test_click_event_with_millis_timestamp(self):
        ts_millis = 1705000000000  # ~2024-01-11
        raw = RawEvent(
            source="kafka-clicks",
            payload={
                "click_id": "CK-1",
                "url": "/products",
                "user_id": "U-10",
                "ts": ts_millis,
            },
        )
        event = normalize(raw)
        assert event.source == "kafka-clicks"
        assert event.payload["click_id"] == "CK-1"
        assert event.payload["url"] == "/products"
        assert event.event_timestamp.year == 2024


class TestNormalizeGeneric:
    def test_unknown_source_uses_generic(self):
        raw = RawEvent(
            source="unknown-source",
            payload={
                "timestamp": "2025-03-01T12:00:00Z",
                "custom_field": "hello",
            },
        )
        event = normalize(raw)
        assert event.source == "unknown-source"
        assert event.payload["custom_field"] == "hello"

    def test_missing_timestamp_fallback(self):
        raw = RawEvent(
            source="webhook",
            payload={"data": "no timestamp here"},
        )
        event = normalize(raw)
        # Should fallback to roughly now
        assert event.event_timestamp is not None
        assert event.event_timestamp.tzinfo is not None
