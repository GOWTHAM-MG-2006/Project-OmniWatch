"""
OmniWatch — Predictive Intelligence Layer
Component: Settings
Phase: 6
Purpose: Configuration for the predictive layer
Inputs: Environment variables (OMNIWATCH_* primary, old bare names fallback)
Outputs: Settings object
"""

from __future__ import annotations

import os
from pathlib import Path

from pydantic import AliasChoices, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

_PREDICTIVE_ROOT = Path(__file__).resolve().parent.parent


def _path_env(primary: str, fallback: str, default: str) -> str:
    """Rules/model path with OMNIWATCH_* primary + old bare-name fallback."""
    return os.getenv(primary, os.getenv(fallback, default))


class Settings(BaseSettings):
    """Configuration for the Predictive Intelligence Layer (Phase 6).

    Inherits storage back-end connection parameters from ``StorageConfig``
    and adds predictive-specific thresholds / tuning knobs with sensible
    defaults that work against the local docker-compose stack.

    Every field is overridable via its OMNIWATCH_* variable (primary) or the
    old bare name (deprecated fallback). Set ``env_file=".env"`` to
    optionally load from a dot-env file at the repo root.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    # ------------------------------------------------------------------ #
    # Kafka (producer / consumer bootstrap)
    # ------------------------------------------------------------------ #
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        validation_alias=AliasChoices(
            "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS"),
        description="Kafka broker addresses (comma-separated)",
    )

    # ------------------------------------------------------------------ #
    # Storage back-end connection params (delegated to StorageConfig)
    # ------------------------------------------------------------------ #
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

    neo4j_uri: str = Field(
        default="bolt://localhost:7687",
        validation_alias=AliasChoices("OMNIWATCH_NEO4J_URI", "NEO4J_URI"))
    neo4j_user: str = Field(
        default="neo4j",
        validation_alias=AliasChoices("OMNIWATCH_NEO4J_USER", "NEO4J_USER"))
    neo4j_password: str = Field(
        default="omniwatch",
        validation_alias=AliasChoices(
            "OMNIWATCH_NEO4J_PASSWORD", "NEO4J_PASSWORD"))

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
    minio_console_port: int = Field(
        default=9001,
        validation_alias=AliasChoices(
            "OMNIWATCH_MINIO_CONSOLE_PORT", "MINIO_CONSOLE_PORT"))

    # ------------------------------------------------------------------ #
    # Predictive-layer thresholds & tuning knobs
    # ------------------------------------------------------------------ #
    predictive_anomaly_score_threshold: float = Field(
        default=0.7,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_SCORE_THRESHOLD",
            "PREDICTIVE_ANOMALY_SCORE_THRESHOLD"),
        description="Minimum anomaly score to flag an observation (0.0-1.0)",
    )
    predictive_confidence_threshold: float = Field(
        default=60.0,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_CONFIDENCE",
            "PREDICTIVE_CONFIDENCE_THRESHOLD"),
        description="Minimum confidence percentage to accept an anomaly (0-100)",
    )
    predictive_cold_start_sample_count: int = Field(
        default=30,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_COLD_START_SAMPLE_COUNT",
            "OMNIWATCH_COLD_START_SAMPLE_COUNT",
            "PREDICTIVE_COLD_START_SAMPLE_COUNT"),
        description="Minimum data points before baseline models activate",
    )
    predictive_model_path: str = Field(
        default="artifacts/anomaly_detector.joblib",
        validation_alias=AliasChoices(
            "OMNIWATCH_MODEL_PATH", "PREDICTIVE_MODEL_PATH"),
        description=(
            "File path where the trained AnomalyDetector state is persisted "
            "(joblib). Relative paths resolve against the predictive package "
            "root so the /health model_loaded glob finds the artifact."
        ),
    )
    predictive_noise_filter_window: int = Field(
        default=5,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_NOISE_FILTER_WINDOW",
            "PREDICTIVE_NOISE_FILTER_WINDOW"),
        description="Sliding window size (samples) for noise smoothing",
    )
    predictive_seasonality_period: int = Field(
        default=24,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_SEASONALITY_PERIOD",
            "PREDICTIVE_SEASONALITY_PERIOD"),
        description="Expected seasonality period in data-point units (e.g. 24h)",
    )
    predictive_lookback_window: int = Field(
        default=168,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_LOOKBACK_WINDOW",
            "PREDICTIVE_LOOKBACK_WINDOW"),
        description="Historical look-back window in data-point units",
    )
    predictive_security_enabled: bool = Field(
        default=True,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_SECURITY_ENABLED",
            "PREDICTIVE_SECURITY_ENABLED"),
        description="Enable the security-signal classifier (GAP-1)",
    )

    # Rules files (env-overridable absolute paths; default to today's
    # Path(__file__) resolution. Hot file reads keep working — for K8s,
    # mount the ConfigMap at this path; no reload daemon by design).
    predictive_detection_rules_path: str = Field(
        default=str(_PREDICTIVE_ROOT / "config" / "detection_rules.yaml"),
        validation_alias=AliasChoices(
            "OMNIWATCH_DETECTION_RULES_PATH", "PREDICTIVE_DETECTION_RULES_PATH"),
    )
    predictive_security_rules_path: str = Field(
        default=str(_PREDICTIVE_ROOT / "config" / "security_rules.yaml"),
        validation_alias=AliasChoices(
            "OMNIWATCH_SECURITY_RULES_PATH", "PREDICTIVE_SECURITY_RULES_PATH"),
    )
    thresholder_state_path: str = Field(
        default=str(_PREDICTIVE_ROOT / "artifacts" / "thresholder_state.json"),
        validation_alias=AliasChoices(
            "OMNIWATCH_THRESHOLDER_STATE_PATH", "PREDICTIVE_THRESHOLDER_STATE_PATH"),
    )

    # Serve port
    predictive_api_port: int = Field(
        default=8007,
        validation_alias=AliasChoices(
            "OMNIWATCH_PREDICTIVE_PORT", "PREDICTIVE_API_PORT"),
    )

    # ------------------------------------------------------------------ #
    # Factory
    # ------------------------------------------------------------------ #
    @classmethod
    def from_env(cls) -> "Settings":
        """Build a ``Settings`` instance from environment variables.

        Unset variables fall back to the defaults defined above, which
        match ``docker-compose.yml`` for local development.
        """
        return cls()
