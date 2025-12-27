# DataFlow — Distributed Data Ingestion & Processing Framework

A fault-tolerant, high-throughput event ingestion service that consumes heterogeneous data from multiple sources, normalizes it, enforces data quality, and writes to an analytics-friendly store with observability built in.

---

## Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Data Flow & Design](#data-flow--design)
- [Configuration Reference](#configuration-reference)
- [Monitoring & Metrics](#monitoring--metrics)
- [Failure Handling & DLQ](#failure-handling--dlq)
- [Storage & Schema](#storage--schema)
- [Demo Guide](#demo-guide)
- [Development](#development)

---

## Overview

Many systems need to ingest events from APIs, webhooks, and message queues — each with different schemas, reliability guarantees, and latency profiles. **DataFlow** provides a unified, scalable framework to pull those events in, clean them, and expose them for downstream analytics or services.

It handles the hard parts:
- Heterogeneous schema normalization
- At-least-once delivery with deduplication
- Backpressure propagation from storage to sources
- Dead-letter queues for bad data inspection
- Production-grade observability out of the box

---

## Features

- **Multi-source ingestion** — Kafka topics, REST webhooks, and mock upstream services
- **Schema normalization** — Maps diverse JSON payloads to a consistent internal schema with source-specific mappers
- **Data quality enforcement** — Validation rules, deduplication (in-memory LRU or Redis), and dead-letter queues
- **Tunable backpressure** — Bounded async queue with high/low water marks; Kafka pauses partitions, webhooks return 429
- **Batch storage** — Parquet files (data lake) or PostgreSQL, with configurable batch size and flush intervals
- **Retry policies** — Exponential backoff with jitter for storage write failures
- **Prometheus metrics** — Counters, gauges, and histograms for all pipeline stages
- **Structured logging** — JSON-formatted logs via structlog with contextual event fields
- **One-command deployment** — Docker Compose with Kafka, PostgreSQL, Prometheus, and Grafana

---

## Architecture

```
┌─────────────────────┐     ┌─────────────────────┐
│   Kafka Topics      │     │   Webhook Clients    │
│  (orders, clicks)   │     │   (POST /ingest)     │
└────────┬────────────┘     └────────┬─────────────┘
         │                           │
         ▼                           ▼
┌────────────────────────────────────────────────────┐
│              Ingestion Service                      │
│  ┌──────────────┐  ┌──────────────┐                │
│  │ Kafka        │  │ Webhook      │                │
│  │ Consumer     │  │ Handler      │                │
│  └──────┬───────┘  └──────┬───────┘                │
│         │                 │                        │
│         ▼                 ▼                        │
│  ┌─────────────────────────────┐                   │
│  │    Bounded Async Queue      │◄── backpressure   │
│  │  (high/low water marks)     │                   │
│  └──────────┬──────────────────┘                   │
│             ▼                                      │
│  ┌─────────────────────────┐                       │
│  │     Worker Pool (N)     │                       │
│  │  ┌───────────────────┐  │                       │
│  │  │ 1. Normalize      │  │                       │
│  │  │ 2. Validate       │──┼──► DLQ Store          │
│  │  │ 3. Deduplicate    │  │                       │
│  │  └────────┬──────────┘  │                       │
│  └───────────┼─────────────┘                       │
│              ▼                                     │
│  ┌─────────────────────────┐                       │
│  │   Batch Accumulator     │                       │
│  │  (size + time trigger)  │                       │
│  └──────────┬──────────────┘                       │
└─────────────┼──────────────────────────────────────┘
              ▼
┌─────────────────────────────┐   ┌──────────────────┐
│  Parquet Files (Data Lake)  │   │  PostgreSQL      │
│  output/{date}/part-*.parq  │   │  events table    │
└─────────────────────────────┘   └──────────────────┘
              │
              ▼
┌─────────────────────────────┐
│  Prometheus + Grafana       │
│  (metrics & dashboards)     │
└─────────────────────────────┘
```

---

## Getting Started

### Prerequisites

- Docker & Docker Compose (v2+)
- Python 3.11+ (for local development)

### Quick Start (Docker)

```bash
# Clone and start all services
git clone <repo-url> && cd dataflow
docker compose up -d --build

# Verify services are running
docker compose ps
```

Services will be available at:

| Service            | URL                          |
|--------------------|------------------------------|
| Webhook API        | http://localhost:8080         |
| Prometheus         | http://localhost:9091         |
| Grafana            | http://localhost:3000         |
| Kafka (external)   | localhost:29092               |
| PostgreSQL         | localhost:5432                |

Grafana credentials: `admin` / `dataflow`

### Local Development

```bash
# Create virtual environment
python -m venv .venv && source .venv/bin/activate

# Install dependencies
make install

# Run the service (requires Kafka & Postgres, or disable in config)
make run

# Run tests
make test
```

---

## Data Flow & Design

### Processing Pipeline

```
Source → RawEvent → Queue → Worker → Normalize → Validate → Dedup → Batch → Storage
                                                    │          │
                                                    ▼          ▼
                                                   DLQ        DLQ
```

1. **Source connectors** (Kafka consumer, webhook handler) receive raw data and wrap it as `RawEvent`
2. Events are placed on a **bounded async queue** with backpressure signaling
3. **Worker pool** (N async tasks) consumes from the queue:
   - **Normalize**: source-specific mapper transforms payload to `InternalEvent`
   - **Validate**: checks UUID format, timestamp range, non-empty payload
   - **Deduplicate**: LRU cache (or Redis) drops already-seen event IDs
4. Valid events go to the **batch accumulator** which flushes on size or timer
5. **Storage backend** writes Parquet files or inserts into PostgreSQL

### At-Least-Once Semantics

- Kafka offsets are committed only after successful enqueue
- Deduplication handles the "at least once" duplicates
- Batch writer retries with exponential backoff on failures

### Backpressure

- Queue reaches **high-water mark** (90%) → backpressure activated
- Kafka consumer **pauses** all partitions
- Webhook endpoint returns **429 Too Many Requests** with `Retry-After` header
- Queue drops below **low-water mark** (50%) → backpressure released

---

## Configuration Reference

All configuration lives in `config/default.yaml`. Key sections:

```yaml
sources:
  kafka:
    enabled: true
    bootstrap_servers: "kafka:9092"
    topics: ["orders", "clicks"]
    group_id: "dataflow-ingestion"
  webhook:
    enabled: true
    host: "0.0.0.0"
    port: 8080

queue:
  max_size: 10000
  high_water_mark: 0.9
  low_water_mark: 0.5

pipeline:
  num_workers: 4

dedup:
  enabled: true
  backend: "memory"       # or "redis"
  max_size: 100000

batch:
  max_size: 1000
  max_flush_interval_seconds: 10.0

storage:
  backend: "parquet"      # or "postgres"
  parquet:
    output_dir: "./output/data"
  postgres:
    dsn: "postgresql://dataflow:dataflow@postgres:5432/dataflow"

retry:
  max_attempts: 3
  backoff_base_seconds: 1.0
  backoff_multiplier: 2.0

metrics:
  enabled: true
  port: 9090
```

Override with environment variable: `DATAFLOW_CONFIG=/path/to/config.yaml`

---

## Monitoring & Metrics

### Prometheus Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `events_ingested_total` | Counter | Total events received (by source) |
| `events_processed_total` | Counter | Successfully processed events |
| `events_failed_total` | Counter | Failed events (by reason) |
| `events_dlq_total` | Counter | Events sent to DLQ (by source) |
| `queue_length` | Gauge | Current queue depth |
| `dlq_size` | Gauge | Current DLQ size |
| `event_processing_latency_seconds` | Histogram | Per-event processing time |
| `batch_flush_duration_seconds` | Histogram | Batch write duration |
| `events_per_batch` | Histogram | Batch size distribution |

### Grafana Dashboard

A pre-provisioned dashboard is available at http://localhost:3000 with panels for:
- Ingested/processed event rates
- Processing latency percentiles (p50/p95/p99)
- Queue length gauge
- DLQ size indicator
- Batch flush duration
- Failure/DLQ rates by source and reason

---

## Failure Handling & DLQ

Events that fail validation or processing are routed to the **Dead-Letter Queue**:

```
POST invalid event → Normalize → Validate (FAIL) → DLQ Store
```

### DLQ Record Schema

| Field | Type | Description |
|-------|------|-------------|
| `event_id` | string | Original event ID |
| `source` | string | Source connector that produced the event |
| `raw_payload` | JSON | Original unmodified payload |
| `error_reason` | string | Why the event was rejected |
| `first_seen_at` | timestamp | When the event was first seen |

### DLQ API

```bash
# List DLQ entries (most recent first)
curl http://localhost:8080/dlq?limit=10

# Get DLQ count
curl http://localhost:8080/dlq/count
```

### Common Rejection Reasons

- `event_id is not a valid UUID` — malformed or missing event ID
- `event_timestamp is in the future` — clock skew beyond 5-minute tolerance
- `payload is empty` — no data in the event payload
- `source is empty` — missing source identifier

---

## Storage & Schema

### Internal Event Schema

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | TEXT (PK) | Unique event identifier (UUID) |
| `source` | TEXT | Source connector label |
| `ingest_timestamp` | TIMESTAMPTZ | When DataFlow received the event |
| `event_timestamp` | TIMESTAMPTZ | Original event timestamp |
| `payload` | JSONB / JSON string | Normalized event data |
| `version` | INTEGER | Schema version (default: 1) |

### Parquet Output

```
output/data/
├── 2025-01-15/
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── ...
├── 2025-01-16/
│   └── part-00000.parquet
└── ...
```

Files use Snappy compression and can be read with any Parquet-compatible tool:

```python
import pyarrow.parquet as pq
table = pq.read_table("output/data/2025-01-15/part-00000.parquet")
print(table.to_pandas())
```

### PostgreSQL

```sql
SELECT event_id, source, event_timestamp, payload
FROM events
ORDER BY ingest_timestamp DESC
LIMIT 10;
```

---

## Demo Guide

### 1. Start the Stack

```bash
docker compose up -d --build
docker compose ps  # verify all services are healthy
```

### 2. Send Events via Webhook

```bash
# Single event
curl -X POST http://localhost:8080/ingest \
  -H "Content-Type: application/json" \
  -d '{"source": "demo", "payload": {"message": "hello", "timestamp": "2025-01-15T12:00:00Z"}}'

# Load test (100 req/s for 30 seconds)
python scripts/load_generator.py --rate 100 --duration 30
```

### 3. Produce Kafka Events

```bash
python scripts/mock_producer.py --topics orders clicks --rate 50 --duration 60
```

### 4. Observe Metrics

Open Grafana at http://localhost:3000 (admin/dataflow) and watch the "DataFlow Ingestion Pipeline" dashboard update in real-time.

### 5. Trigger DLQ

```bash
# Send malformed events
python scripts/seed_bad_events.py --count 20

# Inspect the DLQ
curl http://localhost:8080/dlq | python -m json.tool
curl http://localhost:8080/dlq/count
```

### 6. Inspect Stored Data

```bash
# Parquet files
ls -la output/data/

# Or query PostgreSQL (if using postgres backend)
docker compose exec postgres psql -U dataflow -c "SELECT * FROM events LIMIT 10;"
```

---

## Development

### Project Structure

```
├── src/
│   ├── main.py                # Entrypoint & lifecycle orchestration
│   ├── config.py              # Pydantic config loading
│   ├── models.py              # RawEvent, InternalEvent, DLQRecord
│   ├── queue.py               # Bounded async queue with backpressure
│   ├── connectors/
│   │   ├── webhook.py         # FastAPI webhook handler
│   │   └── kafka_consumer.py  # aiokafka consumer
│   ├── pipeline/
│   │   ├── normalizer.py      # Schema normalization
│   │   ├── validator.py       # Validation rules
│   │   ├── dedup.py           # Deduplication cache
│   │   └── worker.py          # Worker pool orchestration
│   ├── storage/
│   │   ├── base.py            # Storage backend ABC
│   │   ├── batch.py           # Batch accumulator
│   │   ├── parquet_writer.py  # Parquet backend
│   │   └── postgres_writer.py # PostgreSQL backend
│   ├── dlq/
│   │   └── store.py           # Dead-letter queue
│   ├── metrics/
│   │   └── prometheus.py      # Metric definitions
│   └── logging/
│       └── setup.py           # structlog configuration
├── scripts/                   # Mock producer, load generator, bad event seeder
├── tests/                     # Unit test suite
├── config/                    # YAML config, Prometheus, Grafana dashboards
├── Dockerfile                 # Multi-stage build
├── docker-compose.yml         # Full stack deployment
└── Makefile                   # Dev shortcuts
```

### Running Tests

```bash
make test
# or directly:
python -m pytest tests/ -v
```

### Adding a New Source Mapper

1. Define a mapper function in `src/pipeline/normalizer.py`:

```python
def _map_my_source(payload: dict) -> tuple[datetime, dict]:
    event_ts = _parse_timestamp(payload.get("created_at"))
    cleaned = {"field": payload.get("field")}
    return event_ts, cleaned
```

2. Register it in the `_SOURCE_MAPPERS` dict:

```python
_SOURCE_MAPPERS["kafka-my-topic"] = _map_my_source
```

---

## License

MIT
