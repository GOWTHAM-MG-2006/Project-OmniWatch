"""
OmniWatch — Predictive Intelligence Layer
Component: Settings
Phase: 6
Purpose: Configuration for the predictive layer
Inputs: Environment variables
Outputs: Settings object
"""

from __future__ import annotations

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Configuration for the Predictive Intelligence Layer (Phase 6).

    Inherits storage back-end connection parameters from ``StorageConfig``
    and adds predictive-specific thresholds / tuning knobs with sensible
    defaults that work against the local docker-compose stack.

    Every field is overridable via its matching UPPER_CASE environment
    variable.  Set ``env_file=".env"`` to optionally load from a dot-env
    file at the repo root.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ------------------------------------------------------------------ #
    # Kafka (producer / consumer bootstrap)
    # ------------------------------------------------------------------ #
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka broker addresses (comma-separated)",
    )

    # ------------------------------------------------------------------ #
    # Storage back-end connection params (delegated to StorageConfig)
    # ------------------------------------------------------------------ #
    clickhouse_host: str = Field(default="localhost")
    clickhouse_port: int = Field(default=8123)
    clickhouse_db: str = Field(default="omniwatch")
    clickhouse_user: str = Field(default="default")
    clickhouse_password: str = Field(default="")

    neo4j_uri: str = Field(default="bolt://localhost:7687")
    neo4j_user: str = Field(default="neo4j")
    neo4j_password: str = Field(default="omniwatch")

    minio_endpoint: str = Field(default="localhost:9010")
    minio_access_key: str = Field(default="minioadmin")
    minio_secret_key: str = Field(default="minioadmin")
    minio_secure: bool = Field(default=False)
    minio_console_port: int = Field(default=9001)

    # ------------------------------------------------------------------ #
    # Predictive-layer thresholds & tuning knobs
    # ------------------------------------------------------------------ #
    predictive_anomaly_score_threshold: float = Field(
        default=0.7,
        description="Minimum anomaly score to flag an observation (0.0-1.0)",
    )
    predictive_confidence_threshold: float = Field(
        default=60.0,
        description="Minimum confidence percentage to accept an anomaly (0-100)",
    )
    predictive_cold_start_sample_count: int = Field(
        default=30,
        description="Minimum data points before baseline models activate",
    )
    predictive_model_path: str = Field(
        default="artifacts/anomaly_detector.joblib",
        description=(
            "File path where the trained AnomalyDetector state is persisted "
            "(joblib). Relative paths resolve against the predictive package "
            "root so the /health model_loaded glob finds the artifact."
        ),
    )
    predictive_noise_filter_window: int = Field(
        default=5,
        description="Sliding window size (samples) for noise smoothing",
    )
    predictive_seasonality_period: int = Field(
        default=24,
        description="Expected seasonality period in data-point units (e.g. 24h)",
    )
    predictive_lookback_window: int = Field(
        default=168,
        description="Historical look-back window in data-point units",
    )
    predictive_security_enabled: bool = Field(
        default=True,
        description="Enable the security-signal classifier (GAP-1)",
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
