"""
OmniWatch — Causal Graph Engine
Component: Settings
Phase: 7
Purpose: Typed environment configuration for the causal layer (Kafka + causal knobs).
Inputs: Environment variables (OMNIWATCH_* primary, old bare names fallback) / .env file
Outputs: A validated Settings singleton consumed by causal services
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_CAUSAL_ROOT = Path(__file__).resolve().parent.parent


class Settings(BaseSettings):
    """Runtime configuration for the causal graph engine."""

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
        default="omniwatch-causal-group",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_GROUP_CAUSAL", "KAFKA_GROUP_ID"))
    kafka_auto_offset_reset: str = Field(
        default="earliest",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_AUTO_OFFSET_RESET", "KAFKA_AUTO_OFFSET_RESET"))

    # Topics (registry-mapped, env-overridable)
    kafka_topic_anomalies: str = Field(
        default="omniwatch.anomalies.detected",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_ANOMALIES", "KAFKA_TOPIC_ANOMALIES"))
    kafka_topic_causal: str = Field(
        default="omniwatch.incidents.causal",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_TOPICS_CAUSAL", "KAFKA_TOPIC_CAUSAL"))

    # Causal engine runtime knobs (algorithm parameters live in causal_rules.yaml)
    causal_max_depth: int = Field(
        default=10,
        validation_alias=AliasChoices(
            "OMNIWATCH_CAUSAL_MAX_DEPTH", "CAUSAL_MAX_DEPTH"))
    causal_min_confidence: float = Field(
        default=0.3,
        validation_alias=AliasChoices(
            "OMNIWATCH_CAUSAL_MIN_CONFIDENCE", "CAUSAL_MIN_CONFIDENCE"))

    # Rules file (env-overridable absolute path defaulting to today's
    # Path(__file__) resolution; ConfigMap-mount path documented for K8s).
    causal_rules_path: str = Field(
        default=str(_CAUSAL_ROOT / "config" / "causal_rules.yaml"),
        validation_alias=AliasChoices(
            "OMNIWATCH_CAUSAL_RULES_PATH", "CAUSAL_RULES_PATH"),
    )

    # Serve port
    causal_api_port: int = Field(
        default=8008,
        validation_alias=AliasChoices(
            "OMNIWATCH_CAUSAL_PORT", "CAUSAL_API_PORT"),
    )

    # Cloud defaults (never "gcp" by default — unknown until resolved)
    default_cloud_provider: str = Field(
        default="unknown",
        validation_alias=AliasChoices(
            "OMNIWATCH_DEFAULT_CLOUD_PROVIDER", "DEFAULT_CLOUD_PROVIDER"))
    default_region: str = Field(
        default="unknown",
        validation_alias=AliasChoices(
            "OMNIWATCH_DEFAULT_REGION", "DEFAULT_REGION"))

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from environment / .env, applying defaults."""
        return cls()
