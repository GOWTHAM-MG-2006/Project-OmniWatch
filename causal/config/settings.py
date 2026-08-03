"""
OmniWatch — Causal Graph Engine
Component: Settings
Phase: 7
Purpose: Typed environment configuration for the causal layer (Kafka + causal knobs).
Inputs: Environment variables / .env file
Outputs: A validated Settings singleton consumed by causal services
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration for the causal graph engine."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_group_id: str = Field(default="omniwatch-causal-group")
    kafka_auto_offset_reset: str = Field(default="earliest")

    # Causal engine runtime knobs (algorithm parameters live in causal_rules.yaml)
    causal_max_depth: int = Field(default=10)
    causal_min_confidence: float = Field(default=0.3)

    @classmethod
    def from_env(cls) -> Settings:
        """Build settings from environment / .env, applying defaults."""
        return cls()