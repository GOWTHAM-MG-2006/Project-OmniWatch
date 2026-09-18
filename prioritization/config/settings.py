"""
OmniWatch — Incident Prioritization Engine
Component: Settings
Phase: 8
Purpose: Typed environment configuration for the prioritization layer (Kafka + dedup + MinIO).
Inputs: Environment variables (OMNIWATCH_* primary, old bare names fallback) / .env file
Outputs: A validated Settings singleton consumed by prioritization services
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PRIORITIZATION_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Runtime configuration for the incident prioritization engine."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # Kafka
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"))
    kafka_group_id: str = Field(
        default="omniwatch-prioritization-group",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_GROUP_PRIORITIZATION", "KAFKA_GROUP_ID"))
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_AUTO_OFFSET_RESET", "KAFKA_AUTO_OFFSET_RESET"))
    kafka_client_id: str = Field(
        default="omniwatch-prioritization",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_CLIENT_PRIORITIZATION", "KAFKA_CLIENT_ID"))

    # Topics (registry-mapped, env-overridable; semantics unchanged)
    kafka_topic_causal: str = Field(
        default="omniwatch.incidents.causal",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_CAUSAL", "KAFKA_TOPIC_CAUSAL"))
    kafka_topic_created: str = Field(
        default="omniwatch.incidents.created",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_CREATED", "KAFKA_TOPIC_CREATED"))

    # ClickHouse (delegated to StorageConfig contract)
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

    # Deduplicate Engine (GAP 3)
    dedup_ttl_seconds: int = Field(
        default=300,
        validation_alias=AliasChoices(
            "OMNIWATCH_DEDUP_TTL_SECONDS", "DEDUP_TTL_SECONDS"))
    dedup_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "OMNIWATCH_DEDUP_ENABLED", "DEDUP_ENABLED"))

    # SLA risk calculator knobs
    sla_p1_minutes: int = Field(
        default=15,
        validation_alias=AliasChoices(
            "OMNIWATCH_SLA_P1_MINUTES", "SLA_P1_MINUTES"))
    sla_p2_minutes: int = Field(
        default=60,
        validation_alias=AliasChoices(
            "OMNIWATCH_SLA_P2_MINUTES", "SLA_P2_MINUTES"))
    sla_p3_minutes: int = Field(
        default=240,
        validation_alias=AliasChoices(
            "OMNIWATCH_SLA_P3_MINUTES", "SLA_P3_MINUTES"))
    sla_p4_minutes: int = Field(
        default=1440,
        validation_alias=AliasChoices(
            "OMNIWATCH_SLA_P4_MINUTES", "SLA_P4_MINUTES"))

    # Classification rules file (env-overridable absolute path defaulting to
    # today's Path(__file__) resolution; ConfigMap-mount path for K8s).
    classification_rules_path: str = Field(
        default=str(_PRIORITIZATION_ROOT / "config" / "classification_rules.yaml"),
        validation_alias=AliasChoices(
            "OMNIWATCH_CLASSIFICATION_RULES_PATH",
            "PRIORITIZATION_CLASSIFICATION_RULES_PATH"),
    )

    # MinIO evidence archive (Phase 5 storage layer — single ENDPOINT truth;
    # the old ":9000" default here was a live bug, fixed to ":9010")
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
    minio_incidents_bucket: str = Field(
        default="omniwatch-incidents",
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_BUCKETS_INCIDENTS", "MINIO_INCIDENTS_BUCKET"))
    minio_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_ENABLED", "MINIO_ENABLED"))

    # Service
    port: int = Field(
        default=8009,
        validation_alias=AliasChoices(
            "OMNIWATCH_PRIORITIZATION_PORT", "PRIORITIZATION_API_PORT"))

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from environment / .env, applying defaults."""
        return cls()
