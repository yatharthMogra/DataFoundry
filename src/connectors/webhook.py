"""FastAPI webhook handler for event ingestion."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, Request, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.models import RawEvent
from src.queue import BoundedAsyncQueue

# ── Request / response schemas ──────────────────────────────────


class IngestRequest(BaseModel):
    """Envelope for an incoming webhook event."""

    event_id: str | None = None
    source: str | None = None
    timestamp: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    event_id: str
    status: str = "accepted"


class HealthResponse(BaseModel):
    status: str = "ok"


class ReadyResponse(BaseModel):
    status: str
    queue_size: int
    queue_max: int


# ── App factory ─────────────────────────────────────────────────


def create_webhook_app(queue: BoundedAsyncQueue, source_name: str = "webhook") -> FastAPI:
    """Create and return a FastAPI application wired to the given queue.

    Parameters
    ----------
    queue:
        Shared bounded queue where accepted events are placed.
    source_name:
        Default source label when the client does not supply one.
    """

    app = FastAPI(
        title="DataFlow Webhook Ingestion",
        description="Accepts JSON events via POST and feeds them into the processing pipeline.",
        version="0.1.0",
    )

    # ── POST /ingest ────────────────────────────────────────────

    @app.post(
        "/ingest",
        status_code=status.HTTP_202_ACCEPTED,
        response_model=IngestResponse,
        responses={
            429: {"description": "Queue is full — backpressure active"},
        },
    )
    async def ingest(body: IngestRequest, response: Response) -> IngestResponse | JSONResponse:
        # Reject early when backpressure is active
        if queue.is_backpressured:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Server is overloaded, please retry later."},
                headers={"Retry-After": "5"},
            )

        event_id = body.event_id or str(uuid.uuid4())

        raw = RawEvent(
            event_id=event_id,
            source=body.source or source_name,
            payload=body.payload,
            received_at=datetime.now(timezone.utc),
        )

        enqueued = raw  # attempt non-blocking put
        ok = queue.put_nowait(raw)
        if not ok:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Queue is full, please retry later."},
                headers={"Retry-After": "5"},
            )

        return IngestResponse(event_id=event_id)

    # ── GET /health ─────────────────────────────────────────────

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(status="ok")

    # ── GET /ready ──────────────────────────────────────────────

    @app.get(
        "/ready",
        response_model=ReadyResponse,
        responses={503: {"description": "Not ready — queue backpressure active"}},
    )
    async def ready() -> ReadyResponse | JSONResponse:
        is_ready = not queue.is_backpressured
        payload = ReadyResponse(
            status="ready" if is_ready else "not_ready",
            queue_size=queue.size,
            queue_max=queue.max_size,
        )
        if not is_ready:
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content=payload.model_dump(),
            )
        return payload

    return app
