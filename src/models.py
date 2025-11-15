"""Core data models for the DataFlow ingestion pipeline."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_event_id() -> str:
    return str(uuid.uuid4())


class RawEvent(BaseModel):
    """Lightweight wrapper around an incoming event before normalization.

    Produced by source connectors (Kafka consumer, webhook handler) and
    placed onto the internal queue for downstream processing.
    """

    event_id: str = Field(default_factory=_new_event_id)
    source: str
    payload: dict[str, Any]
    received_at: datetime = Field(default_factory=_utcnow)


class InternalEvent(BaseModel):
    """Canonical, normalized event that flows through the processing pipeline.

    Every event—regardless of its origin—is mapped to this schema before
    validation, deduplication, and storage.
    """

    event_id: str = Field(default_factory=_new_event_id)
    source: str
    ingest_timestamp: datetime = Field(default_factory=_utcnow)
    event_timestamp: datetime
    payload: dict[str, Any]
    version: int = 1


class DLQRecord(BaseModel):
    """A record destined for the Dead-Letter Queue.

    Captures the original raw payload together with the reason it was
    rejected so operators can inspect and replay events.
    """

    event_id: str
    source: str
    raw_payload: dict[str, Any]
    error_reason: str
    first_seen_at: datetime = Field(default_factory=_utcnow)
