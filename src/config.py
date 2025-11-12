"""Configuration loading and validation for DataFlow."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field


# ── Source configs ───────────────────────────────────────────────

class KafkaSourceConfig(BaseModel):
    enabled: bool = True
    bootstrap_servers: str = "kafka:9092"
    topics: list[str] = Field(default_factory=lambda: ["orders", "clicks"])
    group_id: str = "dataflow-ingestion"
    auto_offset_reset: Literal["earliest", "latest"] = "earliest"


class WebhookSourceConfig(BaseModel):
    enabled: bool = True
    host: str = "0.0.0.0"
    port: int = 8080


class SourcesConfig(BaseModel):
    kafka: KafkaSourceConfig = Field(default_factory=KafkaSourceConfig)
    webhook: WebhookSourceConfig = Field(default_factory=WebhookSourceConfig)


# ── Queue config ─────────────────────────────────────────────────

class QueueConfig(BaseModel):
    max_size: int = 10_000
    high_water_mark: float = 0.9
    low_water_mark: float = 0.5


# ── Pipeline config ─────────────────────────────────────────────

class PipelineConfig(BaseModel):
    num_workers: int = 4


# ── Dedup config ─────────────────────────────────────────────────

class DedupConfig(BaseModel):
    enabled: bool = True
    backend: Literal["memory", "redis"] = "memory"
    max_size: int = 100_000
    redis_url: str = "redis://redis:6379/0"
    ttl_seconds: int = 3600


# ── Batch config ─────────────────────────────────────────────────

class BatchConfig(BaseModel):
    max_size: int = 1000
    max_flush_interval_seconds: float = 10.0


# ── Storage configs ──────────────────────────────────────────────

class ParquetStorageConfig(BaseModel):
    output_dir: str = "./output/data"


class PostgresStorageConfig(BaseModel):
    dsn: str = "postgresql://dataflow:dataflow@postgres:5432/dataflow"


class StorageConfig(BaseModel):
    backend: Literal["parquet", "postgres"] = "parquet"
    parquet: ParquetStorageConfig = Field(default_factory=ParquetStorageConfig)
    postgres: PostgresStorageConfig = Field(default_factory=PostgresStorageConfig)


# ── DLQ config ───────────────────────────────────────────────────

class DLQConfig(BaseModel):
    backend: Literal["memory", "file", "postgres"] = "file"
    file_path: str = "./output/dlq/dead_letters.jsonl"


# ── Retry config ─────────────────────────────────────────────────

class RetryConfig(BaseModel):
    max_attempts: int = 3
    backoff_base_seconds: float = 1.0
    backoff_multiplier: float = 2.0
    backoff_max_seconds: float = 30.0


# ── Metrics config ───────────────────────────────────────────────

class MetricsConfig(BaseModel):
    enabled: bool = True
    port: int = 9090


# ── Logging config ───────────────────────────────────────────────

class LoggingConfig(BaseModel):
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = "INFO"
    format: Literal["json", "console"] = "json"


# ── Root config ──────────────────────────────────────────────────

class AppConfig(BaseModel):
    """Root configuration for the DataFlow ingestion service."""

    sources: SourcesConfig = Field(default_factory=SourcesConfig)
    queue: QueueConfig = Field(default_factory=QueueConfig)
    pipeline: PipelineConfig = Field(default_factory=PipelineConfig)
    dedup: DedupConfig = Field(default_factory=DedupConfig)
    batch: BatchConfig = Field(default_factory=BatchConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    dlq: DLQConfig = Field(default_factory=DLQConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    metrics: MetricsConfig = Field(default_factory=MetricsConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)


def load_config(path: str | Path | None = None) -> AppConfig:
    """Load configuration from a YAML file.

    Resolution order:
      1. Explicit *path* argument.
      2. ``DATAFLOW_CONFIG`` environment variable.
      3. ``config/default.yaml`` relative to the working directory.
    """
    if path is None:
        path = os.environ.get("DATAFLOW_CONFIG", "config/default.yaml")

    config_path = Path(path)
    if not config_path.exists():
        return AppConfig()

    with open(config_path) as fh:
        raw = yaml.safe_load(fh) or {}

    return AppConfig(**raw)
