"""Tests for the webhook ingestion endpoint."""

import uuid

import pytest
from fastapi.testclient import TestClient

from src.connectors.webhook import create_webhook_app
from src.queue import BoundedAsyncQueue


@pytest.fixture
def queue():
    return BoundedAsyncQueue(max_size=100, high_water_mark=0.9, low_water_mark=0.5)


@pytest.fixture
def client(queue):
    app = create_webhook_app(queue, source_name="test-webhook")
    return TestClient(app)


class TestIngestEndpoint:
    def test_accept_valid_event(self, client, queue):
        resp = client.post("/ingest", json={
            "event_id": str(uuid.uuid4()),
            "source": "partner-A",
            "payload": {"order_id": "123", "amount": 42.0},
        })
        assert resp.status_code == 202
        data = resp.json()
        assert data["status"] == "accepted"
        assert "event_id" in data
        assert queue.size == 1

    def test_auto_assign_event_id(self, client):
        resp = client.post("/ingest", json={
            "payload": {"test": True},
        })
        assert resp.status_code == 202
        event_id = resp.json()["event_id"]
        # Should be a valid UUID
        uuid.UUID(event_id)

    def test_default_source(self, client):
        resp = client.post("/ingest", json={
            "payload": {"data": 1},
        })
        assert resp.status_code == 202

    def test_empty_payload_accepted(self, client):
        # Empty payload is accepted at webhook level; validation happens later
        resp = client.post("/ingest", json={
            "payload": {},
        })
        assert resp.status_code == 202


class TestBackpressure:
    def test_returns_429_when_queue_full(self):
        # Create a tiny queue
        queue = BoundedAsyncQueue(max_size=2, high_water_mark=0.5, low_water_mark=0.25)
        app = create_webhook_app(queue)
        client = TestClient(app)

        # Fill the queue to trigger backpressure
        client.post("/ingest", json={"payload": {"a": 1}})
        client.post("/ingest", json={"payload": {"b": 2}})

        # Next request should get 429
        resp = client.post("/ingest", json={"payload": {"c": 3}})
        assert resp.status_code == 429
        assert "Retry-After" in resp.headers


class TestHealthEndpoints:
    def test_health_ok(self, client):
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"

    def test_ready_ok(self, client):
        resp = client.get("/ready")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ready"

    def test_ready_not_ready_when_backpressured(self):
        queue = BoundedAsyncQueue(max_size=2, high_water_mark=0.5, low_water_mark=0.25)
        app = create_webhook_app(queue)
        client = TestClient(app)

        # Fill queue
        client.post("/ingest", json={"payload": {"a": 1}})
        client.post("/ingest", json={"payload": {"b": 2}})

        resp = client.get("/ready")
        assert resp.status_code == 503
        assert resp.json()["status"] == "not_ready"
