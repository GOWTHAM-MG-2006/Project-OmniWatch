"""
OmniWatch — Orchestration + Policy
Component: Settings (Pydantic v2 BaseSettings)
Phase: 9
Purpose: Centralized configuration for Kafka, OPA, K8s executor, storage
         endpoints, and API port — loaded from env vars / .env file.
Inputs: Environment variables (uppercase) and .env file
Outputs: Settings instance with typed, validated fields
"""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pydantic v2 settings for the orchestration + policy layer.

    Field names map to uppercase env vars (e.g. ``kafka_bootstrap_servers``
    reads from ``KAFKA_BOOTSTRAP_SERVERS``).  Defaults match the
    docker-compose.yml local-dev configuration.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Kafka consumer / producer
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "omniwatch-orchestration-group"
    kafka_auto_offset_reset: str = "earliest"

    # OPA (Open Policy Agent)
    opa_url: str = "http://localhost:8181"
    opa_confidence_threshold: float = 95.0

    # K8s executor
    enable_real_k8s: bool = False
    dry_run: bool = False

    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123

    # MinIO
    minio_endpoint: str = "localhost:9010"

    # FastAPI
    orchestration_api_port: int = 8010
