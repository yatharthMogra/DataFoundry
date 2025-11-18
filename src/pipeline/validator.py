"""Validation rules for normalized events."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone

from src.models import InternalEvent

logger = logging.getLogger(__name__)

# Loose UUID pattern (accepts UUID v1–v5, with or without hyphens)
_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$",
    re.IGNORECASE,
)

# Allow a small clock-skew tolerance when checking future timestamps
_FUTURE_TOLERANCE = timedelta(minutes=5)


def validate(event: InternalEvent) -> tuple[bool, list[str]]:
    """Run validation rules against a normalized event.

    Returns
    -------
    (is_valid, errors)
        A boolean indicating overall validity and a (possibly empty) list
        of human-readable error descriptions.
    """
    errors: list[str] = []

    # 1. event_id must be a valid UUID
    if not event.event_id or not _UUID_RE.match(event.event_id):
        errors.append(f"event_id is not a valid UUID: {event.event_id!r}")

    # 2. source must be non-empty
    if not event.source or not event.source.strip():
        errors.append("source is empty")

    # 3. event_timestamp must not be significantly in the future
    now = datetime.now(timezone.utc)
    if event.event_timestamp > now + _FUTURE_TOLERANCE:
        errors.append(
            f"event_timestamp is in the future: {event.event_timestamp.isoformat()}"
        )

    # 4. payload must be non-empty
    if not event.payload:
        errors.append("payload is empty")

    # 5. version must be positive
    if event.version < 1:
        errors.append(f"version must be >= 1, got {event.version}")

    is_valid = len(errors) == 0

    if not is_valid:
        logger.debug(
            "Event failed validation",
            extra={"event_id": event.event_id, "errors": errors},
        )

    return is_valid, errors
