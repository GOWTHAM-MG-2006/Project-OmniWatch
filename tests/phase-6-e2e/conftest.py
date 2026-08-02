"""
OmniWatch — Phase 6 E2E Test Fixtures

Mock Kafka / ClickHouse / Neo4j fixtures for Predictive Intelligence
layer end-to-end scenarios.  Uses the ``_JsonCodec`` class-based
serializer/deserializer to avoid DeprecationWarning on kafka-python-ng
>= 3.0 when ``-W error::DeprecationWarning`` is active.
"""
import json
import time
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# _JsonCodec — kafka-python-ng class-based serializer (H1 CRITICAL)
# Replicates the exact pattern from tests/phase-2-e2e/conftest.py and
# entity-resolution/tests/conftest.py to avoid DeprecationWarning.
# ---------------------------------------------------------------------------


def _json_codec():
    """Return a kafka-python Serializer/Deserializer for JSON payloads.

    Defined lazily (kafka import guarded) to preserve the pytest.skip
    behavior of the helpers below when kafka-python is not installed.
    Replaces legacy callable-based serializers, which emit a
    DeprecationWarning on kafka-python >= 3 (plain callables no longer
    implement kafka.serializer.Serializer/Deserializer).
    """
    from typing import Any as _Any  # noqa: F811

    from kafka.serializer.abstract import Deserializer as KDeserializer
    from kafka.serializer.abstract import Serializer as KSerializer

    class _JsonCodec(KSerializer, KDeserializer):
        def serialize(self, topic, headers, data) -> _Any:
            if data is None:
                return None
            return json.dumps(data).encode("utf-8")

        def deserialize(self, topic, headers, data) -> _Any:
            if data is None:
                return None
            return json.loads(data.decode("utf-8"))

    return _JsonCodec()


# ---------------------------------------------------------------------------
# Settings fixture — bypasses .env file
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings():
    """Provide a Settings instance with no .env file (all defaults)."""
    from predictive.config.settings import Settings

    return Settings(_env_file=None)


# ---------------------------------------------------------------------------
# Kafka fixtures (mock-based, no real broker)
# ---------------------------------------------------------------------------


@pytest.fixture()
def mock_kafka_producer():
    """Mock KafkaProducer that captures published messages."""
    producer = MagicMock()
    published: List[Dict[str, Any]] = []

    def _capture_send(topic, value, **kwargs):
        published.append({"topic": topic, "value": value})

    producer.send.side_effect = _capture_send
    producer.flush.return_value = 0
    producer._published = published  # attach for test assertions
    return producer


@pytest.fixture()
def mock_kafka_consumer():
    """Mock KafkaConsumer that yields pre-loaded messages."""
    messages: List[Dict[str, Any]] = []

    def _factory(msgs: Optional[List[Dict[str, Any]]] = None):
        if msgs is not None:
            messages.clear()
            messages.extend(msgs)

        consumer = MagicMock()
        consumer._messages = messages
        consumer._poll_idx = [0]

        def _poll(timeout_ms=1000):
            idx = consumer._poll_idx[0]
            if idx >= len(messages):
                return {}
            msg = messages[idx]
            consumer._poll_idx[0] = idx + 1
            record = MagicMock()
            record.value = msg
            return {MagicMock(): [record]}

        consumer.poll.side_effect = _poll
        consumer.close.return_value = None
        return consumer

    return _factory


# ---------------------------------------------------------------------------
# ClickHouse mock
# ---------------------------------------------------------------------------


class _MockClickHouseClient:
    """In-memory mock of ClickHouseClient for test assertions."""

    def __init__(self):
        self._anomalies: List[Dict[str, Any]] = []
        self._feature_vectors: List[Dict[str, Any]] = []
        self._metrics: List[Dict[str, Any]] = []
        self._tables_exist: bool = True

    def insert_anomalies(self, rows: list) -> int:
        if not self._tables_exist:
            raise Exception("Table 'omniwatch.anomalies' doesn't exist")
        self._anomalies.extend(rows)
        return len(rows)

    def select_by_entity(
        self, entity_id: str, table: str = "metrics", limit: int = 100, order_by=None
    ) -> list:
        if not self._tables_exist:
            raise Exception(f"Table 'omniwatch.{table}' doesn't exist")
        if table == "feature_vectors":
            rows = [r for r in self._feature_vectors if r.get("entity_id") == entity_id]
        elif table == "metrics":
            rows = [r for r in self._metrics if r.get("entity_id") == entity_id]
        else:
            rows = []
        return rows[:limit]

    def health_check(self) -> bool:
        return True

    def close(self) -> None:
        pass

    def _seed_feature_vectors(self, rows: List[Dict[str, Any]]) -> None:
        self._feature_vectors.extend(rows)

    def _seed_anomalies(self, rows: List[Dict[str, Any]]) -> None:
        self._anomalies.extend(rows)


@pytest.fixture()
def mock_clickhouse():
    """Provide an in-memory mock ClickHouse client."""
    return _MockClickHouseClient()


# ---------------------------------------------------------------------------
# Neo4j mock
# ---------------------------------------------------------------------------


class _MockNeo4jClient:
    """Minimal mock of Neo4jClient for SignalEnricher tests."""

    def __init__(self, nodes: Optional[List[Dict[str, Any]]] = None):
        self._nodes = nodes or []

    def get_topology(self) -> Dict[str, Any]:
        return {"nodes": self._nodes}

    def close(self) -> None:
        pass


@pytest.fixture()
def mock_neo4j():
    """Provide a mock Neo4j client with a sample topology."""
    nodes = [
        {
            "properties": {
                "id": "postgresql-database",
                "name": "PostgreSQL Database",
                "type": "DATABASE_NODE",
                "criticality": "HIGH",
                "anomaly_score": 0.0,
                "last_seen": "2026-01-01T00:00:00Z",
            }
        },
        {
            "properties": {
                "id": "background-worker",
                "name": "Background Worker",
                "type": "SERVICE",
                "criticality": "MEDIUM",
                "anomaly_score": 0.0,
                "last_seen": "2026-01-01T00:00:00Z",
            }
        },
    ]
    return _MockNeo4jClient(nodes=nodes)


# ---------------------------------------------------------------------------
# Engine fixture — DetectorEngine with fully mocked pipeline
# ---------------------------------------------------------------------------


@pytest.fixture()
def engine(settings):
    """Build a DetectorEngine with all dependencies mocked.

    The engine's internal components (detector, thresholder, noise_filter,
    enricher, producer) are replaced by MagicMock objects so we can
    control their behaviour per test without real Kafka / ClickHouse /
    Neo4j / ML model invocations.
    """
    from predictive.detector_engine import DetectorEngine

    # Build the engine normally (Settings has defaults for everything)
    # but replace internal components with mocks
    eng = DetectorEngine.__new__(DetectorEngine)
    eng._settings = settings
    eng._cold_start_count = settings.predictive_cold_start_sample_count
    eng._training_buffer = []
    eng._is_trained = False

    import threading
    eng._model_lock = threading.Lock()

    # Mock detector (AnomalyDetector)
    eng._detector = MagicMock()
    eng._detector.detect.return_value = None
    eng._detector.train.return_value = None

    # Mock thresholder (AdaptiveThresholder)
    eng._thresholder = MagicMock()
    eng._thresholder.get_threshold.return_value = None

    # Mock noise_filter (NoiseFilter)
    eng._noise_filter = MagicMock()
    eng._noise_filter.should_suppress.return_value = False

    # Mock enricher (SignalEnricher)
    eng._enricher = MagicMock()
    eng._enricher.enrich.side_effect = lambda signal: {**signal, "enriched": True}

    # Mock producer (AnomalyProducer)
    eng._producer = MagicMock()
    eng._producer.publish.return_value = None
    eng._producer.close.return_value = None

    return eng


# ---------------------------------------------------------------------------
# Helper: train engine with normal telemetry, then detect anomaly
# ---------------------------------------------------------------------------


def train_engine_with_normal_data(engine, num_samples: int = 50):
    """Feed normal feature vectors through the engine's cold-start gate.

    After this call the engine is trained and ready to process messages.
    The mock detector.detect is configured to return None (no anomaly)
    for subsequent calls.
    """
    import random

    for i in range(num_samples):
        msg = {
            "entity_id": f"service-{i % 3}",
            "latency_p50": 50.0 + random.gauss(0, 5),
            "error_rate": 0.01 + random.gauss(0, 0.005),
        }
        engine.process_message(msg)

    # Ensure engine is trained
    assert engine._is_trained, "Engine should be trained after cold-start samples"


def make_anomaly_signal(
    entity_id: str = "anomaly-metric-20260101000000",
    entity_type: str = "API_NODE",
    metric_name: str = "latency_p50",
    anomaly_score: float = 0.85,
    confidence: float = 85.0,
    source_type: str = "performance",
) -> Dict[str, Any]:
    """Create a minimal AnomalySignal dict for test assertions."""
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "metric_name": metric_name,
        "anomaly_score": anomaly_score,
        "confidence": confidence,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "deviation_from_baseline": 0.5,
        "source_type": source_type,
    }


def make_security_signal(
    attack_type: str = "BRUTE_FORCE_ATTEMPT",
    entity_id: str = "brute-force-192.168.1.100",
    severity: str = "HIGH",
    confidence: float = 90.0,
) -> Dict[str, Any]:
    """Create a minimal SecurityAnomalySignal dict."""
    return {
        "attack_type": attack_type,
        "entity_id": entity_id,
        "severity": severity,
        "confidence": confidence,
        "evidence_logs": ["evidence line 1"],
        "recommended_action": "Block source IP",
        "source_ip": "192.168.1.100",
        "timestamp": "2026-01-01T00:00:00Z",
    }


def make_feature_vector(
    entity_id: str = "order-service",
    latency_p50: float = 50.0,
    error_rate: float = 0.01,
    request_volume: int = 1000,
) -> Dict[str, Any]:
    """Create a minimal feature vector dict."""
    return {
        "entity_id": entity_id,
        "latency_p50": latency_p50,
        "latency_p95": latency_p50 * 2,
        "latency_p99": latency_p50 * 3,
        "error_rate": error_rate,
        "request_volume": request_volume,
        "window_size": "5m",
        "timestamp": "2026-01-01T00:00:00Z",
    }
