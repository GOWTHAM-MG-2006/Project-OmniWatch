"""
OmniWatch — Predictive Intelligence Layer
Component: AnomalyProducer unit tests
Phase: 6
Purpose: Verify AnomalyProducer Kafka publish, ClickHouse write, and circuit breaker
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

import sys
import time
import uuid
from types import ModuleType
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Pre-inject a fake ``kafka`` module so @patch("kafka.KafkaProducer") resolves
# without importing the real kafka-python-ng (which is broken on Python 3.14).
# ---------------------------------------------------------------------------
_fake_kafka = ModuleType("kafka")
_fake_kafka.KafkaProducer = MagicMock  # type: ignore[attr-defined]
_fake_kafka.KafkaConsumer = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("kafka", _fake_kafka)

from predictive.anomaly_producer import AnomalyProducer, TOPIC_ANOMALIES_DETECTED
from predictive.config.settings import Settings


def _sample_signal() -> dict[str, Any]:
    return {
        "entity_id": "order-service",
        "entity_type": "SERVICE",
        "metric_name": "latency_p99",
        "anomaly_score": 0.85,
        "confidence": 78.0,
        "timestamp": "2026-08-02T12:00:00Z",
        "deviation_from_baseline": 2.3,
        "source_type": "performance",
    }


def _make_settings() -> Settings:
    return Settings(_env_file=None)


# ---------------------------------------------------------------------------
# Tests — publish()
# ---------------------------------------------------------------------------


class TestPublish:

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_publish_sends_to_kafka(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        signal = _sample_signal()

        producer.publish(signal)

        mock_kafka_inst = mock_kafka_cls.return_value
        mock_kafka_inst.send.assert_called_once_with(
            TOPIC_ANOMALIES_DETECTED, signal
        )
        producer.close()

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_publish_also_writes_to_clickhouse(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        signal = _sample_signal()

        producer.publish(signal)

        ch_inst = mock_ch_cls.return_value
        ch_inst.insert_anomalies.assert_called_once()
        inserted_row = ch_inst.insert_anomalies.call_args[0][0][0]
        assert inserted_row["anomaly_id"]
        assert inserted_row["status"] == "active"
        producer.close()

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_publish_survives_clickhouse_failure(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        mock_ch_cls.return_value.insert_anomalies.side_effect = RuntimeError("CH down")

        producer.publish(_sample_signal())

        mock_kafka_cls.return_value.send.assert_called_once()
        producer.close()


# ---------------------------------------------------------------------------
# Tests — write_to_clickhouse()
# ---------------------------------------------------------------------------


class TestWriteToClickHouse:

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_enriches_with_anomaly_id_and_status(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        signal = _sample_signal()

        producer.write_to_clickhouse(signal)

        ch_inst = mock_ch_cls.return_value
        ch_inst.insert_anomalies.assert_called_once()
        row = ch_inst.insert_anomalies.call_args[0][0][0]

        parsed = uuid.UUID(row["anomaly_id"])
        assert parsed.version == 4
        assert row["status"] == "active"
        assert row["entity_id"] == "order-service"
        assert row["anomaly_score"] == 0.85
        producer.close()

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_fills_optional_security_columns(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        signal = _sample_signal()

        producer.write_to_clickhouse(signal)

        row = mock_ch_cls.return_value.insert_anomalies.call_args[0][0][0]
        # Non-nullable String columns get "" (None would raise DataError);
        # nullable columns get None — matches ANOMALIES_COLUMNS schema.
        for col in ("attack_type", "severity", "evidence_logs"):
            assert col in row
            assert row[col] == ""
        for col in ("recommended_action", "source_ip"):
            assert col in row
            assert row[col] is None
        producer.close()

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_preserves_existing_security_fields(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        signal = _sample_signal()
        signal["attack_type"] = "BRUTE_FORCE"
        signal["severity"] = "HIGH"

        producer.write_to_clickhouse(signal)

        row = mock_ch_cls.return_value.insert_anomalies.call_args[0][0][0]
        assert row["attack_type"] == "BRUTE_FORCE"
        assert row["severity"] == "HIGH"
        producer.close()

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_insert_anomalies_called_with_list(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        producer.write_to_clickhouse(_sample_signal())

        args = mock_ch_cls.return_value.insert_anomalies.call_args
        assert isinstance(args[0][0], list)
        assert len(args[0][0]) == 1
        producer.close()


# ---------------------------------------------------------------------------
# Tests — circuit breaker
# ---------------------------------------------------------------------------


class TestCircuitBreaker:

    @patch("predictive.anomaly_producer.time.sleep")
    @patch("predictive.anomaly_producer.time.monotonic")
    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_opens_after_threshold_failures(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
        mock_monotonic: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        threshold = 3
        cooldown = 30.0
        producer = AnomalyProducer(
            settings=_make_settings(),
            failure_threshold=threshold,
            cooldown_seconds=cooldown,
        )
        mock_ch_cls.return_value.insert_anomalies.side_effect = RuntimeError("CH fail")
        mock_monotonic.return_value = 100.0

        for _ in range(threshold):
            with pytest.raises(RuntimeError):
                producer.write_to_clickhouse(_sample_signal())

        mock_monotonic.return_value = 100.0 + 5.0
        mock_ch_cls.return_value.insert_anomalies.side_effect = None

        producer.write_to_clickhouse(_sample_signal())
        mock_sleep.assert_called_once()
        producer.close()

    @patch("predictive.anomaly_producer.time.sleep")
    @patch("predictive.anomaly_producer.time.monotonic")
    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_resets_after_successful_write(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
        mock_monotonic: MagicMock,
        mock_sleep: MagicMock,
    ) -> None:
        threshold = 3
        producer = AnomalyProducer(
            settings=_make_settings(),
            failure_threshold=threshold,
            cooldown_seconds=30.0,
        )
        mock_monotonic.return_value = 100.0

        mock_ch_cls.return_value.insert_anomalies.side_effect = [
            RuntimeError("fail"),
            RuntimeError("fail"),
            None,
        ]
        for _ in range(2):
            with pytest.raises(RuntimeError):
                producer.write_to_clickhouse(_sample_signal())

        producer.write_to_clickhouse(_sample_signal())

        mock_ch_cls.return_value.insert_anomalies.side_effect = [
            RuntimeError("fail"),
            RuntimeError("fail"),
        ]
        for _ in range(2):
            with pytest.raises(RuntimeError):
                producer.write_to_clickhouse(_sample_signal())

        mock_sleep.assert_not_called()
        producer.close()

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_failure_threshold_not_reached_no_pause(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(
            settings=_make_settings(),
            failure_threshold=5,
            cooldown_seconds=30.0,
        )
        mock_ch_cls.return_value.insert_anomalies.side_effect = RuntimeError("fail")

        with pytest.raises(RuntimeError):
            producer.write_to_clickhouse(_sample_signal())

        assert producer._cb_consecutive_failures == 1
        assert producer._cb_open_until == 0.0
        producer.close()


# ---------------------------------------------------------------------------
# Tests — close()
# ---------------------------------------------------------------------------


class TestClose:

    @patch("predictive.anomaly_producer.ClickHouseClient")
    @patch("kafka.KafkaProducer")
    def test_close_flushes_kafka(
        self,
        mock_kafka_cls: MagicMock,
        mock_ch_cls: MagicMock,
    ) -> None:
        producer = AnomalyProducer(settings=_make_settings())
        producer.close()

        mock_kafka_cls.return_value.flush.assert_called_once_with(timeout=5.0)
