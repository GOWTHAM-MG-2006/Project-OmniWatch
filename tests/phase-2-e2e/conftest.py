"""
OmniWatch — Phase 2 E2E Test Fixtures
"""
import json
import time

import pytest
import requests
from minio import Minio


def _json_codec():
    """Return a kafka-python Serializer/Deserializer for JSON payloads.

    Defined lazily (kafka import guarded) to preserve the pytest.skip
    behavior of the helpers below when kafka-python is not installed.
    Replaces legacy callable-based serializers, which emit a
    DeprecationWarning on kafka-python >= 3 (plain callables no longer
    implement kafka.serializer.Serializer/Deserializer).
    """
    from typing import Any

    from kafka.serializer.abstract import Deserializer as KDeserializer
    from kafka.serializer.abstract import Serializer as KSerializer

    class _JsonCodec(KSerializer, KDeserializer):
        def serialize(self, topic, headers, data) -> Any:
            if data is None:
                return None
            return json.dumps(data).encode("utf-8")

        def deserialize(self, topic, headers, data) -> Any:
            if data is None:
                return None
            return json.loads(data.decode("utf-8"))

    return _JsonCodec()

DOCKER_HOST = "localhost"


@pytest.fixture(scope="session")
def otelcol_service():
    """Wait for OTel Collector health endpoint."""
    for _ in range(30):
        try:
            r = requests.get("http://localhost:8888/metrics", timeout=2)
            if r.status_code == 200:
                return
        except Exception:  # ConnectionError, Timeout, OSError on Python 3.14+
            pass
        time.sleep(1)
    pytest.fail("OTel Collector not healthy within 30s")


@pytest.fixture(scope="session")
def flink_rest_api():
    """Flink REST API client."""
    class FlinkAPI:
        def get(self, path):
            return requests.get(f"http://localhost:8081{path}")
        def post(self, path, json=None):
            return requests.post(f"http://localhost:8081{path}", json=json)
    return FlinkAPI()


@pytest.fixture(scope="session")
def minio_client():
    """MinIO client for bucket verification."""
    return Minio(
        "localhost:9010",
        access_key="minioadmin",
        secret_key="minioadmin",
        secure=False,
    )


@pytest.fixture(scope="session")
def sample_metric_payload():
    """Return sample OTel metrics JSON payload."""
    return {
        "resource": {
            "attributes": [
                {"key": "service.name", "value": {"stringValue": "user-service"}},
                {"key": "service.namespace", "value": {"stringValue": "omniwatch"}},
            ]
        },
        "scopeMetrics": [
            {
                "scope": {"name": "test-scope"},
                "metrics": [
                    {
                        "name": "http.requests.total",
                        "description": "Total HTTP requests",
                        "unit": "count",
                        "sum": {
                            "dataPoints": [{"asDouble": 42.0}],
                            "isMonotonic": True,
                        },
                    }
                ],
            }
        ],
    }


@pytest.fixture(scope="session")
def sample_log_payload():
    """Return sample OTel log JSON payload."""
    return {
        "resourceLogs": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "order-service"}},
                    ]
                },
                "scopeLogs": [
                    {
                        "scope": {"name": "test-logger"},
                        "logRecords": [
                            {
                                "timeUnixNano": str(int(time.time() * 1e9)),
                                "severityNumber": 9,
                                "body": {"stringValue": "DB connection timeout"},
                                "attributes": [
                                    {"key": "db.system", "value": {"stringValue": "postgresql"}},
                                ],
                            }
                        ],
                    }
                ],
            }
        ],
    }


@pytest.fixture(scope="session")
def sample_trace_payload():
    """Return sample OTel trace JSON payload."""
    now = int(time.time() * 1e9)
    return {
        "resourceSpans": [
            {
                "resource": {
                    "attributes": [
                        {"key": "service.name", "value": {"stringValue": "api-gateway"}},
                    ]
                },
                "scopeSpans": [
                    {
                        "scope": {"name": "test-tracer"},
                        "spans": [
                            {
                                "traceId": "abc123def456abc123def456abc123de",
                                "spanId": "span001hex",
                                "parentSpanId": "parent000",
                                "name": "POST /api/orders",
                                "kind": 2,
                                "startTimeUnixNano": str(now),
                                "endTimeUnixNano": str(now + 50_000_000),
                                "status": {"code": 1},
                            }
                        ],
                    }
                ],
            }
        ],
    }


@pytest.fixture(scope="session")
def sample_security_payload():
    """Return sample security event JSON payload."""
    return {
        "entity_id": "api-gateway",
        "attack_type": "BRUTE_FORCE",
        "severity": "HIGH",
        "confidence": 0.95,
        "source_ip": "192.168.1.100",
        "timestamp": "2026-07-30T12:00:00.000000Z",
        "description": "Multiple failed login attempts detected",
    }


def kafka_consume_one(topic, timeout=10, quiet=False):
    """Consume one message from a Kafka topic. Returns parsed dict or None."""
    try:
        from kafka import KafkaConsumer as KConsumer
        consumer = KConsumer(
            topic,
            bootstrap_servers="localhost:9092",
            auto_offset_reset="earliest",
            consumer_timeout_ms=timeout * 1000,
            value_deserializer=_json_codec(),
        )
        for msg in consumer:
            consumer.close()
            return msg.value
        consumer.close()
        return None
    except ImportError:
        if not quiet:
            pytest.skip("kafka-python not installed")
        return None


def kafka_produce(topic, value):
    """Produce one message to a Kafka topic."""
    try:
        from kafka import KafkaProducer as KProducer
        producer = KProducer(
            bootstrap_servers="localhost:9092",
            value_serializer=_json_codec(),
        )
        producer.send(topic, value)
        producer.flush(timeout=5)
        producer.close()
    except ImportError:
        pytest.skip("kafka-python not installed")
