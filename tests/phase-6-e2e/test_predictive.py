"""
OmniWatch — Phase 6 E2E Test Scenarios

14 mock-based end-to-end scenarios for the Predictive Intelligence layer.
No Docker / Kafka / ClickHouse required — all infrastructure is mocked.
Run: python -m pytest tests/phase-6-e2e/ -v -W error::DeprecationWarning
"""
import json
import math
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Scenario 1: normal_telemetry — No incidents created
# ---------------------------------------------------------------------------


class TestNormalTelemetry:
    """Normal telemetry should produce no anomaly signals."""

    def test_normal_feature_vectors_produce_no_signal(self, engine):
        """Feed normal (low-variance) feature vectors — expect None output."""
        # Train the engine past cold-start gate
        for i in range(60):
            msg = {
                "entity_id": "order-service",
                "latency_p50": 50.0 + (i % 5) * 0.1,
                "error_rate": 0.01,
                "request_volume": 1000,
            }
            result = engine.process_message(msg)
            # During cold start (first 30 samples) result is always None
            if i < engine._cold_start_count:
                assert result is None, "Cold-start gate should suppress"

        # After training, normal values should still not trigger anomaly
        normal_msg = {
            "entity_id": "order-service",
            "latency_p50": 50.5,
            "error_rate": 0.01,
            "request_volume": 1000,
        }
        # The mock detector.detect returns None, so no anomaly
        result = engine.process_message(normal_msg)
        assert result is None, "Normal telemetry must not produce anomaly signal"

    def test_engine_learns_normal_baseline(self, engine):
        """After cold-start training, the detector should be marked as trained."""
        for i in range(50):
            engine.process_message({
                "entity_id": "svc",
                "latency_p50": 100.0,
                "error_rate": 0.001,
            })
        assert engine._is_trained is True, "Engine should be trained after cold-start samples"


# ---------------------------------------------------------------------------
# Scenario 2: database_cascade — Anomaly signal published via full pipeline
# ---------------------------------------------------------------------------


class TestDatabaseCascade:
    """Simulate a database latency cascade triggering anomaly detection."""

    def test_anomaly_signal_published_through_pipeline(self, engine, mock_kafka_producer):
        """High-latency feature vector → anomaly signal published to Kafka."""
        # Train past cold-start
        for i in range(50):
            engine.process_message({
                "entity_id": "order-service",
                "latency_p50": 50.0,
                "error_rate": 0.01,
            })

        # Now configure the mock detector to return an anomaly signal
        anomaly_signal = {
            "entity_id": "anomaly-latency_p50-20260101",
            "entity_type": "API_NODE",
            "metric_name": "latency_p50",
            "anomaly_score": 0.92,
            "confidence": 92.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "deviation_from_baseline": 1.5,
            "source_type": "performance",
        }
        engine._detector.detect.return_value = anomaly_signal

        # Feed a high-latency feature vector
        result = engine.process_message({
            "entity_id": "order-service",
            "latency_p50": 500.0,  # 10x normal
            "error_rate": 0.15,
        })

        # Signal should pass through the full pipeline
        assert result is not None, "Anomaly signal should be returned"
        assert result["anomaly_score"] == 0.92
        assert result["enriched"] is True  # SignalEnricher added enriched=True
        # AnomalyProducer.publish should have been called
        engine._producer.publish.assert_called_once()


# ---------------------------------------------------------------------------
# Scenario 3: memory_leak — Second anomaly signal path
# ---------------------------------------------------------------------------


class TestMemoryLeak:
    """Simulate a memory-leak style anomaly."""

    def test_memory_anomaly_detected(self, engine):
        """High memory usage triggers anomaly detection."""
        for i in range(50):
            engine.process_message({
                "entity_id": "background-worker",
                "memory_mb": 512.0,
                "cpu_percent": 10.0,
            })

        engine._detector.detect.return_value = {
            "entity_id": "anomaly-memory_mb-20260101",
            "entity_type": "SERVICE",
            "metric_name": "memory_mb",
            "anomaly_score": 0.88,
            "confidence": 88.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "deviation_from_baseline": 2.0,
            "source_type": "performance",
        }

        result = engine.process_message({
            "entity_id": "background-worker",
            "memory_mb": 4096.0,  # 8x normal
            "cpu_percent": 95.0,
        })

        assert result is not None
        assert result["anomaly_score"] == 0.88
        engine._producer.publish.assert_called_once()


# ---------------------------------------------------------------------------
# Scenario 4: alert_storm — NoiseFilter suppresses rapid duplicates
# ---------------------------------------------------------------------------


class TestAlertStorm:
    """Rapid repeated anomalies from the same entity are noise-filtered."""

    def test_rapid_repeated_anomalies_suppressed(self, engine):
        """Multiple anomaly signals within the noise window are suppressed."""
        for i in range(50):
            engine.process_message({
                "entity_id": "order-service",
                "latency_p50": 50.0,
            })

        engine._detector.detect.return_value = {
            "entity_id": "anomaly-latency_p50-20260101",
            "entity_type": "API_NODE",
            "metric_name": "latency_p50",
            "anomaly_score": 0.75,
            "confidence": 75.0,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "deviation_from_baseline": 0.5,
            "source_type": "performance",
        }

        # Configure noise filter to suppress short-lived spikes
        # First call: noise filter sees the entity for the first time
        # (should_suppress returns True for first spike within duration threshold)
        call_count = [0]

        def _side_effect(entity_id, metric, timestamp, affected_neighbors=0,
                         source_type="performance", anomaly_score=0.0):
            call_count[0] += 1
            # Suppress the first few rapid calls (same entity, same metric, short duration)
            if call_count[0] <= 5:
                return True
            return False

        engine._noise_filter.should_suppress.side_effect = _side_effect

        # Send multiple rapid messages
        results = []
        for _ in range(10):
            r = engine.process_message({
                "entity_id": "order-service",
                "latency_p50": 200.0,
            })
            results.append(r)

        # First few should be suppressed, later ones pass through
        suppressed = [r for r in results if r is None]
        passed = [r for r in results if r is not None]
        assert len(suppressed) > 0, "Some anomalies should be suppressed by noise filter"
        assert len(passed) > 0, "Some anomalies should pass through"


# ---------------------------------------------------------------------------
# Scenario 5: security_attack — Brute force detection
# ---------------------------------------------------------------------------


class TestSecurityAttack:
    """Brute force attack detection via SecuritySignalClassifier."""

    def test_brute_force_detected(self):
        """10+ failed logins from same IP triggers BRUTE_FORCE_ATTEMPT."""
        from predictive.security.brute_force_detector import BruteForceDetector

        detector = BruteForceDetector(failures_threshold=5, window_minutes=5)

        # Send 5 auth failure events from the same IP
        signal = None
        for i in range(5):
            event = {
                "event_type": "auth_failure",
                "source_ip": "192.168.1.100",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": f"Failed login attempt #{i+1}",
            }
            signal = detector.detect(event)

        # After threshold events, signal should be returned
        assert signal is not None, "Brute force should be detected after threshold"
        assert signal["attack_type"] == "BRUTE_FORCE_ATTEMPT"
        assert signal["entity_id"] == "brute-force-192.168.1.100"
        assert signal["severity"] in ("HIGH", "CRITICAL")
        assert signal["confidence"] > 0
        assert len(signal["evidence_logs"]) > 0
        assert "192.168.1.100" in signal["recommended_action"]

    def test_security_signal_classifier_routes_brute_force(self):
        """SecuritySignalClassifier routes auth_failure to BruteForceDetector."""
        from predictive.security.security_signal_classifier import SecuritySignalClassifier

        classifier = SecuritySignalClassifier(bootstrap_servers="localhost:9999")

        # Send enough events to trigger detection
        signal = None
        for i in range(12):
            event = {
                "event_type": "auth_failure",
                "source_ip": "10.0.0.50",
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "message": "Invalid credentials",
            }
            signal = classifier.process_event(event)

        # At least one signal should have been produced
        assert signal is not None, "Classifier should produce a brute force signal"
        assert signal["attack_type"] == "BRUTE_FORCE_ATTEMPT"
        assert signal["source_type"] == "security"


# ---------------------------------------------------------------------------
# Scenario 6: config_drift — UNAUTHORIZED_CONFIG_CHANGE
# ---------------------------------------------------------------------------


class TestConfigDrift:
    """Unauthorized configuration change detection."""

    def test_config_drift_detected(self):
        """Config file change event triggers UNAUTHORIZED_CONFIG_CHANGE."""
        from predictive.security.config_drift_detector import ConfigDriftDetector

        detector = ConfigDriftDetector()

        event = {
            "attack_type": "config_file_changed",
            "entity_id": "api-gateway",
            "source_ip": "10.0.0.25",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "description": "nginx.conf modified outside deployment pipeline",
        }

        signal = detector.detect(event)

        assert signal is not None, "Config drift should be detected"
        assert signal["attack_type"] == "UNAUTHORIZED_CONFIG_CHANGE"
        assert signal["entity_id"] == "api-gateway"
        assert signal["severity"] == "CRITICAL"
        assert signal["confidence"] == 90.0

    def test_approved_config_change_not_flagged(self):
        """Pre-approved config changes should not trigger an alert."""
        from predictive.security.config_drift_detector import ConfigDriftDetector

        detector = ConfigDriftDetector(approved_changes={"change-001"})

        event = {
            "attack_type": "config_file_changed",
            "entity_id": "api-gateway",
            "change_id": "change-001",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        signal = detector.detect(event)
        assert signal is None, "Approved config change should not produce signal"


# ---------------------------------------------------------------------------
# Scenario 7: feature_vector_consumption — FeatureReader reads from ClickHouse
# ---------------------------------------------------------------------------


class TestFeatureVectorConsumption:
    """FeatureReader reads feature vectors from ClickHouse."""

    def test_read_features_returns_ascending_order(self, mock_clickhouse):
        """Feature vectors should be returned oldest→newest."""
        from predictive.feature_reader import FeatureReader
        from predictive.config.settings import Settings

        settings = Settings(_env_file=None)  # type: ignore[call-arg]

        # Seed mock ClickHouse with feature vectors (newest first, as CH returns)
        mock_clickhouse._seed_feature_vectors([
            {"entity_id": "order-service", "latency_p50": 55.0, "timestamp": "2026-01-01T00:10:00Z"},
            {"entity_id": "order-service", "latency_p50": 52.0, "timestamp": "2026-01-01T00:05:00Z"},
            {"entity_id": "order-service", "latency_p50": 50.0, "timestamp": "2026-01-01T00:00:00Z"},
        ])

        # Patch ClickHouseClient to use our mock
        with patch("predictive.feature_reader.ClickHouseClient") as MockCH:
            MockCH.return_value = mock_clickhouse
            reader = FeatureReader(settings)
            rows = reader.read_features("order-service")

        assert len(rows) == 3
        # Should be reversed to ascending order (oldest first)
        assert rows[0]["timestamp"] < rows[1]["timestamp"] < rows[2]["timestamp"]

    def test_read_features_empty_on_missing_entity(self, mock_clickhouse):
        """Missing entity should return empty list."""
        from predictive.feature_reader import FeatureReader
        from predictive.config.settings import Settings

        settings = Settings(_env_file=None)  # type: ignore[call-arg]

        with patch("predictive.feature_reader.ClickHouseClient") as MockCH:
            MockCH.return_value = mock_clickhouse
            reader = FeatureReader(settings)
            rows = reader.read_features("nonexistent-service")

        assert rows == [], "Missing entity should return empty list"


# ---------------------------------------------------------------------------
# Scenario 8: kafka_anomalies_consumption — AnomalyProducer publishes to Kafka
# ---------------------------------------------------------------------------


class TestKafkaAnomaliesConsumption:
    """AnomalyProducer publishes anomaly signals to Kafka topic."""

    def _build_producer(self, settings, mock_kafka, mock_ch):
        from predictive.anomaly_producer import AnomalyProducer

        producer = AnomalyProducer.__new__(AnomalyProducer)
        producer._settings = settings
        producer._kafka = mock_kafka
        producer._ch_client = mock_ch
        producer._cb_failure_threshold = 5
        producer._cb_cooldown = 30.0
        producer._cb_consecutive_failures = 0
        producer._cb_open_until = 0.0
        return producer

    def test_publish_sends_to_kafka_topic(self, settings):
        from predictive.anomaly_producer import TOPIC_ANOMALIES_DETECTED

        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        producer = self._build_producer(settings, mock_kafka, mock_ch)

        signal = {
            "entity_id": "anomaly-test",
            "anomaly_score": 0.85,
            "source_type": "performance",
        }
        producer.publish(signal)

        mock_kafka.send.assert_called_once()
        call_args = mock_kafka.send.call_args
        assert call_args[0][0] == TOPIC_ANOMALIES_DETECTED
        assert call_args[0][1]["entity_id"] == "anomaly-test"

    def test_publish_also_writes_to_clickhouse(self, settings):
        mock_kafka = MagicMock()
        mock_ch = MagicMock()
        producer = self._build_producer(settings, mock_kafka, mock_ch)

        signal = {"entity_id": "test-entity", "anomaly_score": 0.8}
        producer.publish(signal)

        mock_ch.insert_anomalies.assert_called_once()
        inserted_row = mock_ch.insert_anomalies.call_args[0][0][0]
        assert inserted_row["entity_id"] == "test-entity"
        assert inserted_row["status"] == "active"
        assert "anomaly_id" in inserted_row


# ---------------------------------------------------------------------------
# Scenario 9: clickhouse_persistence — ClickHouse storage round-trip
# ---------------------------------------------------------------------------


class TestClickHousePersistence:
    """ClickHouse anomaly storage round-trip via mock client."""

    def test_insert_and_select_anomalies(self, mock_clickhouse):
        """Insert anomaly rows and retrieve them by entity."""
        rows = [
            {
                "entity_id": "order-service",
                "anomaly_score": 0.9,
                "status": "active",
                "anomaly_id": "test-uuid-1",
                "timestamp": "2026-01-01T00:00:00Z",
            },
            {
                "entity_id": "order-service",
                "anomaly_score": 0.8,
                "status": "active",
                "anomaly_id": "test-uuid-2",
                "timestamp": "2026-01-01T00:05:00Z",
            },
        ]

        count = mock_clickhouse.insert_anomalies(rows)
        assert count == 2
        assert len(mock_clickhouse._anomalies) == 2

    def test_health_check_returns_true(self, mock_clickhouse):
        """ClickHouse health check should succeed."""
        assert mock_clickhouse.health_check() is True


# ---------------------------------------------------------------------------
# Scenario 10: neo4j_enrichment — SignalEnricher adds Neo4j context
# ---------------------------------------------------------------------------


class TestNeo4jEnrichment:
    """SignalEnricher attaches Neo4j entity context to anomaly signals."""

    def test_enrich_adds_entity_context(self, mock_neo4j):
        """Enriched signal should contain entity_context from Neo4j."""
        from predictive.signal_enricher import SignalEnricher

        enricher = SignalEnricher(neo4j_client=mock_neo4j)

        signal = {
            "entity_id": "postgresql-database",
            "anomaly_score": 0.9,
            "source_type": "performance",
        }

        enriched = enricher.enrich(signal)

        assert enriched["enriched"] is True
        assert "entity_context" in enriched
        ctx = enriched["entity_context"]
        assert ctx["name"] == "PostgreSQL Database"
        assert ctx["criticality"] == "HIGH"

    def test_enrich_unknown_entity_returns_enriched_false(self):
        """Unknown entity should return enriched=False."""
        from predictive.signal_enricher import SignalEnricher

        # Empty topology → no matching node
        mock_neo4j = MagicMock()
        mock_neo4j.get_topology.return_value = {"nodes": []}

        enricher = SignalEnricher(neo4j_client=mock_neo4j)

        signal = {"entity_id": "nonexistent-entity", "anomaly_score": 0.5}
        enriched = enricher.enrich(signal)

        assert enriched["enriched"] is False
        assert "entity_context" not in enriched

    def test_enrich_neo4j_timeout_returns_enriched_false(self):
        """Neo4j timeout should gracefully degrade to enriched=False."""
        from predictive.signal_enricher import SignalEnricher

        mock_neo4j = MagicMock()
        mock_neo4j.get_topology.side_effect = TimeoutError("Neo4j unreachable")

        enricher = SignalEnricher(neo4j_client=mock_neo4j, timeout=0.1)

        signal = {"entity_id": "some-entity", "anomaly_score": 0.5}
        enriched = enricher.enrich(signal)

        assert enriched["enriched"] is False


# ---------------------------------------------------------------------------
# Scenario 11: cold_start_degradation — Engine returns None before training
# ---------------------------------------------------------------------------


class TestColdStartDegradation:
    """Engine suppresses detection during cold-start window."""

    def test_cold_start_returns_none(self, engine):
        """First N messages should return None (cold-start gate)."""
        cold_start_count = engine._cold_start_count
        results = []

        for i in range(cold_start_count + 5):
            r = engine.process_message({
                "entity_id": "order-service",
                "latency_p50": 50.0 + i,
            })
            results.append(r)

        # All cold-start messages should return None
        cold_start_results = results[:cold_start_count]
        assert all(r is None for r in cold_start_results), \
            f"All {cold_start_count} cold-start messages should return None"

        # After cold-start, engine should be trained
        assert engine._is_trained is True

    def test_cold_start_training_buffer_cleared(self, engine):
        """Training buffer should be cleared after cold-start training."""
        for i in range(engine._cold_start_count + 5):
            engine.process_message({
                "entity_id": "svc",
                "latency_p50": 100.0,
            })

        assert len(engine._training_buffer) == 0, "Training buffer should be empty after training"


# ---------------------------------------------------------------------------
# Scenario 12: nan_inf_clamping — NaN/inf values clamped to 0.0
# ---------------------------------------------------------------------------


class TestNanInfClamping:
    """NaN and inf values in anomaly scores should be clamped to 0.0."""

    def test_nan_clamped_to_zero(self):
        """math.isnan check — NaN should become 0.0."""
        from predictive.anomaly_detector import _clamp

        assert _clamp(float("nan")) == 0.0
        assert _clamp(math.nan) == 0.0

    def test_inf_clamped_to_zero(self):
        """math.isinf check — inf should become 0.0."""
        from predictive.anomaly_detector import _clamp

        assert _clamp(float("inf")) == 0.0
        assert _clamp(float("-inf")) == 0.0

    def test_normal_value_passes_through(self):
        """Normal finite values should not be clamped."""
        from predictive.anomaly_detector import _clamp

        assert _clamp(0.5) == 0.5
        assert _clamp(-1.0) == -1.0
        assert _clamp(42.0) == 42.0

    def test_detector_handles_nan_in_feature_vector(self):
        """AnomalyDetector should handle NaN in feature vectors gracefully."""
        from predictive.anomaly_detector import AnomalyDetector
        from predictive.config.settings import Settings

        settings = Settings(_env_file=None)  # type: ignore[call-arg]
        detector = AnomalyDetector(settings)

        # Train with normal data
        import pandas as pd
        df = pd.DataFrame({
            "latency_p50": [50.0] * 120,
            "error_rate": [0.01] * 120,
        })
        detector.train(df)

        # Detect with NaN value — should not crash
        result = detector.detect({
            "latency_p50": float("nan"),
            "error_rate": 0.01,
        })
        # Result may be None (below threshold) or a valid signal, but should not crash
        if result is not None:
            assert isinstance(result["anomaly_score"], float)
            assert not math.isnan(result["anomaly_score"])


# ---------------------------------------------------------------------------
# Scenario 13: clickhouse_table_not_exists_guard — Graceful handling
# ---------------------------------------------------------------------------


class TestClickHouseTableNotExistsGuard:
    """ClickHouse table-missing errors handled gracefully."""

    def test_insert_raises_when_table_missing(self, mock_clickhouse):
        """insert_anomalies should raise when tables don't exist."""
        mock_clickhouse._tables_exist = False

        with pytest.raises(Exception, match="Table"):
            mock_clickhouse.insert_anomalies([{"entity_id": "test"}])

    def test_select_raises_when_table_missing(self, mock_clickhouse):
        """select_by_entity should raise when table doesn't exist."""
        mock_clickhouse._tables_exist = False

        with pytest.raises(Exception, match="Table"):
            mock_clickhouse.select_by_entity("test-entity")

    def test_feature_reader_returns_empty_on_ch_failure(self):
        """FeatureReader.read_features returns [] on ClickHouse error."""
        from predictive.feature_reader import FeatureReader
        from predictive.config.settings import Settings

        settings = Settings(_env_file=None)  # type: ignore[call-arg]

        mock_ch = MagicMock()
        mock_ch.select_by_entity.side_effect = Exception("Table doesn't exist")

        with patch("predictive.feature_reader.ClickHouseClient", return_value=mock_ch):
            reader = FeatureReader(settings)
            rows = reader.read_features("any-entity")

        assert rows == [], "FeatureReader should return empty list on CH failure"


# ---------------------------------------------------------------------------
# Scenario 14: security_event_generator — Security event processing
# ---------------------------------------------------------------------------


class TestSecurityEventGenerator:
    """Security event generator and classifier integration."""

    def test_brute_force_detector_threshold_property(self):
        """BruteForceDetector exposes configured threshold."""
        from predictive.security.brute_force_detector import BruteForceDetector

        detector = BruteForceDetector(failures_threshold=15)
        assert detector.failures_threshold == 15

    def test_brute_force_ignores_non_auth_events(self):
        """Non-auth events should not trigger brute force detection."""
        from predictive.security.brute_force_detector import BruteForceDetector

        detector = BruteForceDetector(failures_threshold=3)

        event = {
            "event_type": "config_change",
            "source_ip": "10.0.0.1",
            "message": "Configuration updated",
        }

        result = detector.detect(event)
        assert result is None, "Non-auth event should not trigger brute force"

    def test_config_drift_approved_changes_accessors(self):
        """ConfigDriftDetector approved-change management."""
        from predictive.security.config_drift_detector import ConfigDriftDetector

        detector = ConfigDriftDetector()
        assert len(detector.approved_changes) == 0

        detector.add_approved("change-abc")
        assert "change-abc" in detector.approved_changes

        detector.remove_approved("change-abc")
        assert "change-abc" not in detector.approved_changes

    def test_security_classifier_ignores_non_dict_events(self):
        """SecuritySignalClassifier should ignore non-dict events."""
        from predictive.security.security_signal_classifier import SecuritySignalClassifier

        classifier = SecuritySignalClassifier(bootstrap_servers="localhost:9999")

        # Non-dict event should return None
        result = classifier.process_event("not a dict")  # type: ignore[arg-type]
        assert result is None

        # None event should return None
        result = classifier.process_event(None)  # type: ignore[arg-type]
        assert result is None

    def test_detector_engine_close(self, engine):
        """DetectorEngine.close() should close producer."""
        engine.close()
        engine._producer.close.assert_called_once()
