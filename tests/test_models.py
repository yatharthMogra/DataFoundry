"""Tests for core data models."""

import uuid
from datetime import datetime, timezone

from src.models import DLQRecord, InternalEvent, RawEvent


class TestRawEvent:
    def test_default_event_id_is_uuid(self):
        raw = RawEvent(source="test", payload={"key": "value"})
        parsed = uuid.UUID(raw.event_id)
        assert parsed.version == 4

    def test_explicit_event_id(self):
        raw = RawEvent(event_id="custom-id", source="test", payload={})
        assert raw.event_id == "custom-id"

    def test_received_at_auto_set(self):
        raw = RawEvent(source="test", payload={"k": 1})
        assert raw.received_at is not None
        assert raw.received_at.tzinfo is not None

    def test_serialization_roundtrip(self):
        raw = RawEvent(source="kafka-orders", payload={"order_id": "123", "amount": 42.5})
        data = raw.model_dump(mode="json")
        restored = RawEvent(**data)
        assert restored.event_id == raw.event_id
        assert restored.source == raw.source
        assert restored.payload == raw.payload


class TestInternalEvent:
    def test_creation(self):
        event = InternalEvent(
            event_id=str(uuid.uuid4()),
            source="kafka-clicks",
            event_timestamp=datetime.now(timezone.utc),
            payload={"url": "/home"},
        )
        assert event.version == 1
        assert event.ingest_timestamp is not None

    def test_version_default(self):
        event = InternalEvent(
            source="test",
            event_timestamp=datetime.now(timezone.utc),
            payload={"a": 1},
        )
        assert event.version == 1


class TestDLQRecord:
    def test_creation(self):
        record = DLQRecord(
            event_id="abc-123",
            source="webhook",
            raw_payload={"bad": "data"},
            error_reason="payload is empty",
        )
        assert record.event_id == "abc-123"
        assert record.first_seen_at is not None

    def test_json_serialization(self):
        record = DLQRecord(
            event_id="xyz",
            source="test",
            raw_payload={"k": "v"},
            error_reason="validation failed",
        )
        json_str = record.model_dump_json()
        assert "xyz" in json_str
        assert "validation failed" in json_str
