"""
OmniWatch — Incident Prioritization Engine
Component: Settings
Phase: 8
Purpose: Typed environment configuration for the prioritization layer (Kafka + dedup + MinIO).
Inputs: Environment variables / .env file
Outputs: A validated Settings singleton consumed by prioritization services
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the incident prioritization engine."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_group_id: str = Field(default="omniwatch-prioritization-group")
    kafka_auto_offset_reset: str = Field(default="earliest")
    kafka_client_id: str = Field(default="omniwatch-prioritization")

    # Deduplicate Engine (GAP 3)
    dedup_ttl_seconds: int = Field(default=300)
    dedup_enabled: bool = Field(default=True)

    # MinIO evidence archive (Phase 5 storage layer)
    minio_endpoint: str = Field(default="localhost:9000")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_incidents_bucket: str = Field(default="omniwatch-incidents")
    minio_enabled: bool = Field(default=True)

    # Service
    port: int = Field(default=8009)

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from environment / .env, applying defaults."""
        return cls()
