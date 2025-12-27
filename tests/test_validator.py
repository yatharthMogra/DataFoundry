"""Tests for validation rules."""

import uuid
from datetime import datetime, timedelta, timezone

from src.models import InternalEvent
from src.pipeline.validator import validate


def _make_event(**overrides) -> InternalEvent:
    defaults = {
        "event_id": str(uuid.uuid4()),
        "source": "test-source",
        "event_timestamp": datetime.now(timezone.utc),
        "payload": {"key": "value"},
        "version": 1,
    }
    defaults.update(overrides)
    return InternalEvent(**defaults)


class TestValidEvent:
    def test_valid_event_passes(self):
        event = _make_event()
        is_valid, errors = validate(event)
        assert is_valid is True
        assert errors == []


class TestInvalidEventId:
    def test_non_uuid_event_id(self):
        event = _make_event(event_id="not-a-uuid")
        is_valid, errors = validate(event)
        assert is_valid is False
        assert any("UUID" in e for e in errors)

    def test_empty_event_id(self):
        event = _make_event(event_id="")
        is_valid, errors = validate(event)
        assert is_valid is False


class TestInvalidSource:
    def test_empty_source(self):
        event = _make_event(source="")
        is_valid, errors = validate(event)
        assert is_valid is False
        assert any("source" in e for e in errors)

    def test_whitespace_source(self):
        event = _make_event(source="   ")
        is_valid, errors = validate(event)
        assert is_valid is False


class TestFutureTimestamp:
    def test_far_future_timestamp(self):
        future = datetime.now(timezone.utc) + timedelta(days=365)
        event = _make_event(event_timestamp=future)
        is_valid, errors = validate(event)
        assert is_valid is False
        assert any("future" in e for e in errors)

    def test_slightly_future_within_tolerance(self):
        # 1 minute in the future should pass (within 5-min tolerance)
        slight_future = datetime.now(timezone.utc) + timedelta(minutes=1)
        event = _make_event(event_timestamp=slight_future)
        is_valid, errors = validate(event)
        assert is_valid is True


class TestEmptyPayload:
    def test_empty_dict_payload(self):
        event = _make_event(payload={})
        is_valid, errors = validate(event)
        assert is_valid is False
        assert any("payload" in e for e in errors)


class TestInvalidVersion:
    def test_zero_version(self):
        event = _make_event(version=0)
        is_valid, errors = validate(event)
        assert is_valid is False
        assert any("version" in e for e in errors)


class TestMultipleErrors:
    def test_multiple_failures(self):
        event = _make_event(
            event_id="bad",
            source="",
            payload={},
            version=0,
        )
        is_valid, errors = validate(event)
        assert is_valid is False
        assert len(errors) >= 3
