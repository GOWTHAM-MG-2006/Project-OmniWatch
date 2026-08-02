"""
OmniWatch — Predictive Intelligence Layer
Component: SecuritySignalClassifier unit tests
Phase: 6
Purpose: Verify SecuritySignalClassifier routes events to sub-detectors,
         produces SecurityAnomalySignal with source_type="security", and
         aggregates evidence logs
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

import sys
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Pre-inject a fake ``kafka`` module so @patch("kafka.KafkaConsumer") resolves
# without importing the real kafka-python-ng (which is broken on Python 3.14).
# ---------------------------------------------------------------------------
_fake_kafka = ModuleType("kafka")
_fake_kafka.KafkaConsumer = MagicMock  # type: ignore[attr-defined]
_fake_kafka.KafkaProducer = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("kafka", _fake_kafka)

from predictive.security.security_signal_classifier import (
    SecuritySignalClassifier,
    TOPIC_SECURITY_EVENTS,
    TOPIC_ANOMALIES_DETECTED,
    CONSUMER_GROUP,
)


# ---------------------------------------------------------------------------
# Sample events
# ---------------------------------------------------------------------------


def _brute_force_event() -> dict[str, Any]:
    """Sample brute force security event."""
    return {
        "attack_type": "failed_login",
        "entity_id": "user-service",
        "source_ip": "192.168.1.100",
        "timestamp": "2026-08-02T12:00:00Z",
        "message": "Failed login attempt for user admin",
        "log": "Auth failure: invalid password for admin",
    }


def _config_drift_event() -> dict[str, Any]:
    """Sample config drift security event."""
    return {
        "attack_type": "config_drift",
        "entity_id": "api-gateway",
        "source_ip": "10.0.0.50",
        "timestamp": "2026-08-02T12:05:00Z",
        "description": "config_file_changed: /etc/nginx/nginx.conf modified",
        "config_file": "/etc/nginx/nginx.conf",
    }


def _priv_escalation_event() -> dict[str, Any]:
    """Sample privilege escalation security event."""
    return {
        "attack_type": "privilege_escalation",
        "entity_id": "backend-worker",
        "source_ip": "172.16.0.25",
        "timestamp": "2026-08-02T12:10:00Z",
        "log_message": "User nobody ran sudo -u root systemctl restart app",
    }


def _data_exfil_event() -> dict[str, Any]:
    """Sample data exfiltration security event."""
    return {
        "entity_id": "database-server",
        "source_ip": "10.0.0.99",
        "timestamp": "2026-08-02T12:15:00Z",
        "outbound_bytes": 50000000,  # 50MB spike
    }


def _unknown_event() -> dict[str, Any]:
    """Sample event with no recognized type."""
    return {
        "event_type": "unknown_type",
        "entity_id": "some-service",
        "timestamp": "2026-08-02T12:20:00Z",
    }


# ---------------------------------------------------------------------------
# Tests — Class initialisation
# ---------------------------------------------------------------------------


class TestInit:

    def test_default_topics(self) -> None:
        classifier = SecuritySignalClassifier()
        assert classifier._topic_in == TOPIC_SECURITY_EVENTS
        assert classifier._topic_out == TOPIC_ANOMALIES_DETECTED

    def test_default_consumer_group(self) -> None:
        classifier = SecuritySignalClassifier()
        assert classifier._consumer_group == CONSUMER_GROUP

    def test_custom_params(self) -> None:
        classifier = SecuritySignalClassifier(
            bootstrap_servers="broker:9093",
            topic_in="custom.in",
            topic_out="custom.out",
            consumer_group="custom-group",
        )
        assert classifier._bootstrap_servers == "broker:9093"
        assert classifier._topic_in == "custom.in"
        assert classifier._topic_out == "custom.out"
        assert classifier._consumer_group == "custom-group"

    def test_sub_detectors_created(self) -> None:
        classifier = SecuritySignalClassifier()
        assert classifier._brute_force is not None
        assert classifier._config_drift is not None
        assert classifier._priv_escalation is not None
        assert classifier._data_exfil is not None

    def test_evidence_aggregator_created(self) -> None:
        classifier = SecuritySignalClassifier()
        assert classifier._evidence is not None

    def test_kafka_not_initialised_before_start(self) -> None:
        classifier = SecuritySignalClassifier()
        assert classifier._consumer is None
        assert classifier._producer is None
        assert classifier._running is False


# ---------------------------------------------------------------------------
# Tests — Kafka lifecycle
# ---------------------------------------------------------------------------


class TestKafkaLifecycle:

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_start_creates_kafka_connections(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()

        mock_consumer_cls.assert_called_once()
        mock_producer_cls.assert_called_once()
        assert classifier._running is True
        classifier.stop()

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_start_consumer_subscribes_to_correct_topic(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()

        call_kwargs = mock_consumer_cls.call_args
        assert TOPIC_SECURITY_EVENTS in call_kwargs[0]  # positional arg
        assert call_kwargs[1]["group_id"] == CONSUMER_GROUP
        classifier.stop()

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_stop_closes_connections(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()
        classifier.stop()

        mock_consumer_cls.return_value.close.assert_called_once()
        mock_producer_cls.return_value.flush.assert_called_once()
        mock_producer_cls.return_value.close.assert_called_once()
        assert classifier._running is False

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_double_stop_is_safe(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()
        classifier.stop()
        classifier.stop()  # Should not raise


# ---------------------------------------------------------------------------
# Tests — Brute force detection
# ---------------------------------------------------------------------------


class TestBruteForceDetection:

    def test_brute_force_event_detected(self) -> None:
        classifier = SecuritySignalClassifier()
        # Send enough events to exceed threshold (default 10)
        event = _brute_force_event()
        signal = None
        for _ in range(12):
            signal = classifier.process_event(event)

        assert signal is not None
        assert signal["attack_type"] == "BRUTE_FORCE_ATTEMPT"
        assert signal["source_type"] == "security"
        assert signal["source_ip"] == "192.168.1.100"
        assert "evidence_logs" in signal
        assert isinstance(signal["evidence_logs"], list)

    def test_brute_force_below_threshold_returns_none(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _brute_force_event()
        # Send only 5 events (below default threshold of 10)
        signal = None
        for _ in range(5):
            signal = classifier.process_event(event)

        assert signal is None


# ---------------------------------------------------------------------------
# Tests — Config drift detection
# ---------------------------------------------------------------------------


class TestConfigDriftDetection:

    def test_config_drift_event_detected(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _config_drift_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert signal["attack_type"] == "UNAUTHORIZED_CONFIG_CHANGE"
        assert signal["source_type"] == "security"
        assert signal["entity_id"] == "api-gateway"
        assert "evidence_logs" in signal
        assert len(signal["evidence_logs"]) > 0

    def test_config_drift_severity_is_critical(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _config_drift_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert signal["severity"] == "CRITICAL"


# ---------------------------------------------------------------------------
# Tests — Privilege escalation detection
# ---------------------------------------------------------------------------


class TestPrivEscalationDetection:

    def test_priv_escalation_event_detected(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _priv_escalation_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert signal["attack_type"] == "PRIVILEGE_ESCALATION_ATTEMPT"
        assert signal["source_type"] == "security"
        assert signal["entity_id"] == "backend-worker"
        assert "evidence_logs" in signal

    def test_priv_escalation_severity_is_critical(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _priv_escalation_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert signal["severity"] == "CRITICAL"


# ---------------------------------------------------------------------------
# Tests — Data exfiltration detection
# ---------------------------------------------------------------------------


class TestDataExfilDetection:

    def test_data_exfil_event_detected(self) -> None:
        """Data exfil requires baseline — first event returns None."""
        classifier = SecuritySignalClassifier()
        # First event establishes baseline
        event1 = _data_exfil_event()
        signal1 = classifier.process_event(event1)
        assert signal1 is None  # No baseline yet

        # Send normal traffic to establish baseline
        normal_event = {
            "entity_id": "database-server",
            "outbound_bytes": 1000000,  # 1MB normal
            "timestamp": "2026-08-02T12:14:00Z",
        }
        for _ in range(3):
            classifier.process_event(normal_event)

        # Now send spike event
        spike_event = _data_exfil_event()
        signal = classifier.process_event(spike_event)

        assert signal is not None
        assert signal["attack_type"] == "DATA_EXFILTRATION"
        assert signal["source_type"] == "security"
        assert signal["entity_id"] == "database-server"
        assert "evidence_logs" in signal


# ---------------------------------------------------------------------------
# Tests — Event routing
# ---------------------------------------------------------------------------


class TestEventRouting:

    def test_unknown_event_returns_none(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _unknown_event()
        signal = classifier.process_event(event)
        assert signal is None

    def test_non_dict_event_returns_none(self) -> None:
        classifier = SecuritySignalClassifier()
        signal = classifier.process_event("not a dict")  # type: ignore[arg-type]
        assert signal is None

    def test_empty_event_returns_none(self) -> None:
        classifier = SecuritySignalClassifier()
        signal = classifier.process_event({})
        assert signal is None


# ---------------------------------------------------------------------------
# Tests — Source type
# ---------------------------------------------------------------------------


class TestSourceType:

    def test_source_type_is_security_for_brute_force(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _brute_force_event()
        signal = None
        for _ in range(12):
            signal = classifier.process_event(event)

        assert signal is not None
        assert signal["source_type"] == "security"

    def test_source_type_is_security_for_config_drift(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _config_drift_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert signal["source_type"] == "security"

    def test_source_type_is_security_for_priv_escalation(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _priv_escalation_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert signal["source_type"] == "security"


# ---------------------------------------------------------------------------
# Tests — Evidence aggregation
# ---------------------------------------------------------------------------


class TestEvidenceAggregation:

    def test_evidence_logs_populated_for_config_drift(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _config_drift_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert len(signal["evidence_logs"]) > 0
        # Evidence should contain event description
        evidence_text = " ".join(signal["evidence_logs"]).lower()
        assert "config" in evidence_text or "drift" in evidence_text

    def test_evidence_logs_populated_for_priv_escalation(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _priv_escalation_event()
        signal = classifier.process_event(event)

        assert signal is not None
        assert len(signal["evidence_logs"]) > 0
        # Evidence should contain the log message
        evidence_text = " ".join(signal["evidence_logs"]).lower()
        assert "sudo" in evidence_text or "privilege" in evidence_text

    def test_evidence_buffer_capped_at_5(self) -> None:
        classifier = SecuritySignalClassifier()
        # Send multiple config drift events
        for i in range(10):
            event = {
                "attack_type": "config_drift",
                "entity_id": "api-gateway",
                "timestamp": f"2026-08-02T12:{i:02d}:00Z",
                "description": f"Config change #{i}",
            }
            classifier.process_event(event)

        # Evidence should be capped at 5 lines
        evidence = classifier._evidence.get_evidence("api-gateway", "config_drift")
        assert len(evidence) <= 5


# ---------------------------------------------------------------------------
# Tests — SecurityAnomalySignal contract
# ---------------------------------------------------------------------------


class TestSignalContract:

    def test_brute_force_signal_has_all_required_fields(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _brute_force_event()
        signal = None
        for _ in range(12):
            signal = classifier.process_event(event)

        assert signal is not None
        required_fields = [
            "attack_type",
            "entity_id",
            "severity",
            "confidence",
            "evidence_logs",
            "recommended_action",
            "source_ip",
            "timestamp",
            "source_type",
        ]
        for field in required_fields:
            assert field in signal, f"Missing field: {field}"

    def test_config_drift_signal_has_all_required_fields(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _config_drift_event()
        signal = classifier.process_event(event)

        assert signal is not None
        required_fields = [
            "attack_type",
            "entity_id",
            "severity",
            "confidence",
            "evidence_logs",
            "recommended_action",
            "source_ip",
            "timestamp",
            "source_type",
        ]
        for field in required_fields:
            assert field in signal, f"Missing field: {field}"

    def test_priv_escalation_signal_has_all_required_fields(self) -> None:
        classifier = SecuritySignalClassifier()
        event = _priv_escalation_event()
        signal = classifier.process_event(event)

        assert signal is not None
        required_fields = [
            "attack_type",
            "entity_id",
            "severity",
            "confidence",
            "evidence_logs",
            "recommended_action",
            "source_ip",
            "timestamp",
            "source_type",
        ]
        for field in required_fields:
            assert field in signal, f"Missing field: {field}"


# ---------------------------------------------------------------------------
# Tests — Kafka publishing (via process_event with mocked producer)
# ---------------------------------------------------------------------------


class TestKafkaPublishing:

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_publish_called_on_detection(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()

        # Config drift should detect on first event
        event = _config_drift_event()
        classifier.process_event(event)

        # Producer should have been called with send()
        mock_producer = mock_producer_cls.return_value
        mock_producer.send.assert_called()

        classifier.stop()

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_publish_uses_correct_topic(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()

        event = _config_drift_event()
        classifier.process_event(event)

        mock_producer = mock_producer_cls.return_value
        # Check that send was called with the correct topic
        call_args = mock_producer.send.call_args
        assert call_args[0][0] == TOPIC_ANOMALIES_DETECTED

        classifier.stop()


# ---------------------------------------------------------------------------
# Tests — Consumer group
# ---------------------------------------------------------------------------


class TestConsumerGroup:

    @patch("kafka.KafkaProducer")
    @patch("kafka.KafkaConsumer")
    def test_consumer_group_is_correct(
        self,
        mock_consumer_cls: MagicMock,
        mock_producer_cls: MagicMock,
    ) -> None:
        classifier = SecuritySignalClassifier()
        classifier.start()

        call_kwargs = mock_consumer_cls.call_args
        assert call_kwargs[1]["group_id"] == CONSUMER_GROUP

        classifier.stop()

    def test_consumer_group_constant(self) -> None:
        assert CONSUMER_GROUP == "omniwatch-predictive-security"


# ---------------------------------------------------------------------------
# Tests — Multi-attack scenario
# ---------------------------------------------------------------------------


class TestMultiAttackScenario:

    def test_all_four_attack_types_detected(self) -> None:
        """Integration-style test: all 4 attack types in sequence."""
        classifier = SecuritySignalClassifier()
        detected = []

        # Config drift (detected on first event)
        signal = classifier.process_event(_config_drift_event())
        if signal:
            detected.append(signal["attack_type"])

        # Privilege escalation (detected on first event)
        signal = classifier.process_event(_priv_escalation_event())
        if signal:
            detected.append(signal["attack_type"])

        # Brute force (needs multiple events)
        for _ in range(12):
            signal = classifier.process_event(_brute_force_event())
        if signal:
            detected.append(signal["attack_type"])

        # Data exfil (needs baseline + spike)
        normal = {
            "entity_id": "database-server",
            "outbound_bytes": 1000000,
            "timestamp": "2026-08-02T12:14:00Z",
        }
        for _ in range(3):
            classifier.process_event(normal)
        signal = classifier.process_event(_data_exfil_event())
        if signal:
            detected.append(signal["attack_type"])

        # All 4 should be detected
        assert len(detected) == 4
        assert "UNAUTHORIZED_CONFIG_CHANGE" in detected
        assert "PRIVILEGE_ESCALATION_ATTEMPT" in detected
        assert "BRUTE_FORCE_ATTEMPT" in detected
        assert "DATA_EXFILTRATION" in detected
