"""
OmniWatch — Generative AI Layer
Component: Settings (Pydantic v2 BaseSettings)
Phase: 10
Purpose: Centralized configuration for Ollama/vLLM LLM endpoint, ClickHouse,
         MinIO, Kafka, and API port — loaded from env vars / .env file.
Inputs: Environment variables (OMNIWATCH_* primary, old bare names fallback)
        and .env file
Outputs: Settings instance with typed, validated fields
"""

from __future__ import annotations

from typing import Literal

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Pydantic v2 settings for the Generative AI layer.

    OMNIWATCH_* names take precedence; old bare names are honored as a
    deprecated fallback. Defaults match the docker-compose.yml local-dev
    configuration.
    """

    model_config = SettingsConfigDict(
        env_file=None, extra="ignore", populate_by_name=True)

    # LLM Backend
    llm_backend: Literal["ollama", "vllm"] = "ollama"
    ollama_url: str = Field(
        default="http://localhost:11434",
        validation_alias=AliasChoices(
            "OMNIWATCH_LLM_URL", "OLLAMA_URL", "LLM_URL"))
    vllm_base: str = Field(
        default="http://localhost:8000",
        validation_alias=AliasChoices(
            "OMNIWATCH_VLLM_BASE", "VLLM_BASE"))
    llm_model: str = Field(
        default="qwen3:8b",
        validation_alias=AliasChoices(
            "OMNIWATCH_LLM_MODEL", "LLM_MODEL"))
    llm_max_tokens: int = Field(
        default=2048,
        validation_alias=AliasChoices(
            "OMNIWATCH_LLM_MAX_TOKENS", "LLM_MAX_TOKENS"))
    llm_temperature: float = Field(
        default=0.3,
        validation_alias=AliasChoices(
            "OMNIWATCH_LLM_TEMPERATURE", "LLM_TEMPERATURE"))
    llm_concurrency: int = Field(
        default=2,
        validation_alias=AliasChoices(
            "OMNIWATCH_LLM_CONCURRENCY", "LLM_CONCURRENCY"))

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
    clickhouse_user: str = Field(
        default="default",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_USER", "CLICKHOUSE_USER"))
    clickhouse_password: str = Field(
        default="",
        validation_alias=AliasChoices(
            "OMNIWATCH_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD"))

    # MinIO
    minio_endpoint: str = Field(
        default="localhost:9010",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_ENDPOINT", "MINIO_ENDPOINT"))
    minio_access_key: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY"))
    minio_secret_key: str = Field(
        default="minioadmin",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_SECRET_KEY", "MINIO_SECRET_KEY"))
    minio_secure: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_SECURE", "MINIO_SECURE"))
    minio_auto_create: bool = True

    # Kafka
    kafka_bootstrap: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"),
        description="Kafka bootstrap servers (repo-wide env convention)",
    )
    kafka_group: str = Field(
        default="omniwatch-genai-group",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_GROUP_GENAI", "KAFKA_GROUP"))

    # Topics / buckets (registry-mapped, env-overridable; semantics unchanged)
    kafka_topic_created: str = Field(
        default="omniwatch.incidents.created",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_CREATED", "KAFKA_TOPIC_CREATED"))
    kafka_topic_actions: str = Field(
        default="omniwatch.remediation.actions",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_ACTIONS", "KAFKA_TOPIC_ACTIONS"))
    kafka_topic_summaries: str = Field(
        default="omniwatch.generated.summaries",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_SUMMARIES", "KAFKA_TOPIC_SUMMARIES"))
    kafka_topic_runbooks: str = Field(
        default="omniwatch.generated.runbooks",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_RUNBOOKS", "KAFKA_TOPIC_RUNBOOKS"))
    kafka_topic_reports: str = Field(
        default="omniwatch.generated.reports",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_REPORTS", "KAFKA_TOPIC_REPORTS"))
    minio_audit_bucket: str = Field(
        default="omniwatch-audit-logs",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_BUCKETS_AUDIT", "MINIO_AUDIT_BUCKET"))
    minio_runbooks_bucket: str = Field(
        default="omniwatch-runbooks",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_BUCKETS_RUNBOOKS", "MINIO_RUNBOOKS_BUCKET"))
    minio_incidents_bucket: str = Field(
        default="omniwatch-incidents",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_BUCKETS_INCIDENTS", "MINIO_INCIDENTS_BUCKET"))

    # FastAPI
    genai_api_port: int = Field(
        default=8020,
        validation_alias=AliasChoices(
            "OMNIWATCH_GENAI_PORT", "GENAI_API_PORT"))
