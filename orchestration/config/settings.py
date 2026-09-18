"""
OmniWatch — Orchestration + Policy
Component: Settings (Pydantic v2 BaseSettings)
Phase: 9
Purpose: Centralized configuration for Kafka, OPA, K8s executor, storage
         endpoints, and API port — loaded from env vars / .env file.
Inputs: Environment variables (OMNIWATCH_* primary, old bare names fallback)
        and .env file
Outputs: Settings instance with typed, validated fields
"""

from __future__ import annotations

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pydantic v2 settings for the orchestration + policy layer.

    OMNIWATCH_* names take precedence; old bare names are honored as a
    deprecated fallback. Defaults match the docker-compose.yml local-dev
    configuration.
    """

    model_config = SettingsConfigDict(
        env_file=".env", extra="ignore", populate_by_name=True)

    # Kafka consumer / producer
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"))
    kafka_group_id: str = Field(
        default="omniwatch-orchestration-group",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_GROUP_ORCHESTRATION", "KAFKA_GROUP_ID"))
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_AUTO_OFFSET_RESET", "KAFKA_AUTO_OFFSET_RESET"))

    # Topics (registry-mapped, env-overridable; semantics unchanged)
    kafka_topic_created: str = Field(
        default="omniwatch.incidents.created",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_CREATED", "KAFKA_TOPIC_CREATED"))
    kafka_topic_actions: str = Field(
        default="omniwatch.remediation.actions",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_ACTIONS", "KAFKA_TOPIC_ACTIONS"))

    # OPA (Open Policy Agent)
    opa_url: str = Field(
        default="http://localhost:8181",
        validation_alias=AliasChoices("OMNIWATCH_OPA_URL", "OPA_URL"))
    opa_confidence_threshold: float = Field(
        default=95.0,
        validation_alias=AliasChoices(
            "OMNIWATCH_OPA_CONFIDENCE_THRESHOLD", "OPA_CONFIDENCE_THRESHOLD"))

    # K8s executor
    enable_real_k8s: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "OMNIWATCH_ORCH_ENABLE_REAL_K8S", "ENABLE_REAL_K8S"))
    dry_run: bool = Field(
        default=False,
        validation_alias=AliasChoices("OMNIWATCH_ORCH_DRY_RUN", "DRY_RUN"))

    # Orchestration execution knobs
    orch_max_retries: int = Field(
        default=3,
        validation_alias=AliasChoices(
            "OMNIWATCH_ORCH_MAX_RETRIES", "ORCH_MAX_RETRIES"))
    orch_retry_backoff_seconds: float = Field(
        default=2.0,
        validation_alias=AliasChoices(
            "OMNIWATCH_ORCH_RETRY_BACKOFF_SECONDS",
            "ORCH_RETRY_BACKOFF_SECONDS"))
    orch_action_timeout_seconds: int = Field(
        default=300,
        validation_alias=AliasChoices(
            "OMNIWATCH_ORCH_ACTION_TIMEOUT_SECONDS",
            "ORCH_ACTION_TIMEOUT_SECONDS"))

    # Simulation fallback endpoint (cluster-DNS-aware default; localhost
    # override for local dev). executor.py:31 self-call consumes this.
    simulation_endpoint: str = Field(
        default="http://orchestration:8010/api/v1/simulate-action",
        validation_alias=AliasChoices(
            "OMNIWATCH_SIMULATION_ENDPOINT", "SIMULATION_ENDPOINT"))

    # ClickHouse
    clickhouse_host: str = Field(
        default="localhost",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_HOST", "CLICKHOUSE_HOST"))
    clickhouse_port: int = Field(
        default=8123,
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_PORT"))
    clickhouse_db: str = Field(
        default="omniwatch",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_DB", "CLICKHOUSE_DB"))

    # MinIO
    minio_endpoint: str = Field(
        default="localhost:9010",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_ENDPOINT", "MINIO_ENDPOINT"))
    minio_audit_bucket: str = Field(
        default="omniwatch-audit-logs",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_BUCKETS_AUDIT", "MINIO_AUDIT_BUCKET"))

    # FastAPI
    orchestration_api_port: int = Field(
        default=8010,
        validation_alias=AliasChoices(
            "OMNIWATCH_ORCHESTRATION_PORT", "ORCHESTRATION_API_PORT"))
