"""
OmniWatch — Predictive Intelligence Layer
Component: Settings unit tests
Phase: 6
Purpose: Verify Settings loads correctly from environment variables
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

import os

import pytest

from predictive.config.settings import Settings


class TestSettingsDefaults:
    """Settings should have sensible defaults matching docker-compose.

    Environment variables that may already be set (e.g. CLICKHOUSE_PORT)
    are cleared so we can verify the class-level defaults.
    """

    _ENV_KEYS_TO_CLEAR = [
        "KAFKA_BOOTSTRAP_SERVERS",
        "CLICKHOUSE_HOST",
        "CLICKHOUSE_PORT",
        "CLICKHOUSE_DB",
        "CLICKHOUSE_USER",
        "CLICKHOUSE_PASSWORD",
        "NEO4J_URI",
        "NEO4J_USER",
        "NEO4J_PASSWORD",
        "MINIO_ENDPOINT",
        "MINIO_ACCESS_KEY",
        "MINIO_SECRET_KEY",
        "MINIO_SECURE",
        "MINIO_CONSOLE_PORT",
        "PREDICTIVE_ANOMALY_SCORE_THRESHOLD",
        "PREDICTIVE_CONFIDENCE_THRESHOLD",
        "PREDICTIVE_COLD_START_SAMPLE_COUNT",
        "PREDICTIVE_NOISE_FILTER_WINDOW",
        "PREDICTIVE_SEASONALITY_PERIOD",
        "PREDICTIVE_LOOKBACK_WINDOW",
        "PREDICTIVE_SECURITY_ENABLED",
    ]

    @pytest.fixture(autouse=True)
    def _clear_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        for key in self._ENV_KEYS_TO_CLEAR:
            monkeypatch.delenv(key, raising=False)

    def _make_settings(self) -> Settings:
        """Create Settings without dotenv loading (env_file=None).

        This isolates the test from any .env file so we verify the
        class-level field defaults.
        """
        return Settings(_env_file=None)

    def test_default_kafka_bootstrap(self) -> None:
        s = self._make_settings()
        assert s.kafka_bootstrap_servers == "localhost:9092"

    def test_default_clickhouse(self) -> None:
        s = self._make_settings()
        assert s.clickhouse_host == "localhost"
        assert s.clickhouse_port == 8123
        assert s.clickhouse_db == "omniwatch"
        assert s.clickhouse_user == "default"
        assert s.clickhouse_password == ""

    def test_default_neo4j(self) -> None:
        s = self._make_settings()
        assert s.neo4j_uri == "bolt://localhost:7687"
        assert s.neo4j_user == "neo4j"
        assert s.neo4j_password == "omniwatch"

    def test_default_minio(self) -> None:
        s = self._make_settings()
        assert s.minio_endpoint == "localhost:9010"
        assert s.minio_access_key == "minioadmin"
        assert s.minio_secret_key == "minioadmin"
        assert s.minio_secure is False
        assert s.minio_console_port == 9001

    def test_default_predictive_thresholds(self) -> None:
        s = self._make_settings()
        assert s.predictive_anomaly_score_threshold == 0.7
        assert s.predictive_confidence_threshold == 60.0
        assert s.predictive_cold_start_sample_count == 30
        assert s.predictive_noise_filter_window == 5
        assert s.predictive_seasonality_period == 24
        assert s.predictive_lookback_window == 168
        assert s.predictive_security_enabled is True


class TestSettingsFromEnv:
    """from_env() should pick up environment variables."""

    def test_from_env_with_overrides(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-broker:19092")
        monkeypatch.setenv("CLICKHOUSE_HOST", "clickhouse.prod")
        monkeypatch.setenv("CLICKHOUSE_PORT", "9000")
        monkeypatch.setenv("NEO4J_URI", "bolt://neo4j.prod:7687")
        monkeypatch.setenv("MINIO_ENDPOINT", "minio.prod:9000")
        monkeypatch.setenv("PREDICTIVE_ANOMALY_SCORE_THRESHOLD", "0.85")
        monkeypatch.setenv("PREDICTIVE_COLD_START_SAMPLE_COUNT", "100")
        monkeypatch.setenv("PREDICTIVE_SECURITY_ENABLED", "false")

        s = Settings.from_env()

        assert s.kafka_bootstrap_servers == "kafka-broker:19092"
        assert s.clickhouse_host == "clickhouse.prod"
        assert s.clickhouse_port == 9000
        assert s.neo4j_uri == "bolt://neo4j.prod:7687"
        assert s.minio_endpoint == "minio.prod:9000"
        assert s.predictive_anomaly_score_threshold == 0.85
        assert s.predictive_cold_start_sample_count == 100
        assert s.predictive_security_enabled is False

    def test_from_env_defaults_when_unset(self) -> None:
        """When no env vars are set, defaults apply."""
        s = Settings.from_env()
        assert s.kafka_bootstrap_servers == "localhost:9092"
        assert s.predictive_anomaly_score_threshold == 0.7
