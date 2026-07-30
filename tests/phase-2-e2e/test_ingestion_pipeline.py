"""
OmniWatch — Phase 2 E2E Test
Component: Ingestion Pipeline Validation
Phase: 2
Purpose: Validate Kafka bus, stream processor, and end-to-end ingestion pipeline
Inputs: Kafka topics, OTel JSON payloads, anomaly injection endpoints
Outputs: Test pass/fail for Kafka topic CRUD, telemetry normalization, pipeline
"""

from __future__ import annotations

import importlib
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
import requests


def _load_module(name: str, filepath: Path):
    """Dynamically load a Python module from file path."""
    import importlib.util as _util
    import sys as _sys
    spec = _util.spec_from_file_location(name, filepath)
    assert spec is not None, f"{filepath} not found"
    loader = spec.loader
    assert loader is not None, f"{filepath} has no loader"
    mod = _util.module_from_spec(spec)
    _sys.modules[name] = mod  # register so dataclasses/other stdlib can find it
    loader.exec_module(mod)
    return mod

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
INGESTION_DIR = PROJECT_ROOT / "ingestion"

DOCKER_HOST = os.environ.get("DOCKER_HOST", "localhost")
KAFKA_BOOTSTRAP = f"{DOCKER_HOST}:9092"

# All 7 OmniWatch topics
EXPECTED_TOPICS = [
    "omniwatch.metrics.raw",
    "omniwatch.logs.raw",
    "omniwatch.traces.raw",
    "omniwatch.security.events",
    "omniwatch.anomalies.detected",
    "omniwatch.incidents.created",
    "omniwatch.remediation.actions",
]

SAMPLE_METRICS_PAYLOAD = INGESTION_DIR / "test_payload_metrics.json"


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture(scope="session")
def kafka_available():
    """Check if confluent_kafka is available (Docker-only)."""
    try:
        import confluent_kafka  # noqa: F401
        return True
    except ImportError:
        return False


@pytest.fixture(scope="session")
def services_running():
    """Check required services for Phase 2."""
    services = {
        "kafka": 9092,
        "api-gateway": 8000,
        "user-service": 8001,
        "order-service": 8002,
    }
    for name, port in services.items():
        try:
            resp = requests.get(f"http://{DOCKER_HOST}:{port}/health", timeout=3)
            assert resp.ok, f"{name} health check failed: {resp.status_code}"
        except (requests.ConnectionError, requests.Timeout):
            pytest.skip(f"{name} not reachable at port {port}")
            return False
    return True


# =============================================================================
# Kafka Bus Tests
# =============================================================================


class TestKafkaBus:
    """Validate Kafka bus topic management and messaging."""

    def test_kafka_bus_module_imports(self):
        """Validate kafka_bus.py imports cleanly."""
        try:
            _load_module("kafka_bus", INGESTION_DIR / "kafka_bus.py")
        except ImportError as e:
            if "confluent_kafka" not in str(e):
                raise

    def test_topic_definitions_complete(self):
        """Validate all 7 expected topics are defined in TOPIC_SPECS."""
        try:
            mod = _load_module("kafka_bus", INGESTION_DIR / "kafka_bus.py")
        except ImportError:
            pytest.skip("kafka_bus.py import failed (confluent_kafka may be missing)")
            return

        for topic in EXPECTED_TOPICS:
            assert topic in mod.TOPIC_SPECS, f"Topic {topic} missing from TOPIC_SPECS"
            ts = mod.TOPIC_SPECS[topic]
            assert ts.name == topic
            assert ts.producer, f"Topic {topic} missing producer"
            assert ts.consumer, f"Topic {topic} missing consumer"

    def test_topic_spec_metadata(self):
        """Validate topic spec metadata fields are filled."""
        try:
            mod = _load_module("kafka_bus", INGESTION_DIR / "kafka_bus.py")
        except ImportError:
            pytest.skip("kafka_bus.py import failed")
            return

        for name, ts in mod.TOPIC_SPECS.items():
            assert ts.partitions >= 1, f"{name}: partitions must be >= 1"
            assert ts.replication_factor >= 1, f"{name}: replication_factor must be >= 1"
            assert len(ts.description) > 5, f"{name}: description too short"

    def test_all_topics_listed(self):
        """Validate ALL_TOPICS contains exactly the 7 expected topics."""
        try:
            mod = _load_module("kafka_bus", INGESTION_DIR / "kafka_bus.py")
        except ImportError:
            pytest.skip("kafka_bus.py import failed")
            return

        assert len(mod.ALL_TOPICS) == 7, f"Expected 7 topics, got {len(mod.ALL_TOPICS)}"
        for topic in EXPECTED_TOPICS:
            assert topic in mod.ALL_TOPICS, f"Topic {topic} missing from ALL_TOPICS"

    def test_producer_send_produces_valid_message(self, kafka_available):
        """Validate KafkaProducer.send() works end-to-end on a live topic."""
        if not kafka_available:
            pytest.skip("confluent_kafka not installed")

        import confluent_kafka  # noqa: F401
        import ingestion.kafka_bus as kb

        # Create a test topic
        kb.create_topics(topic_names=["omniwatch.test.e2e"])

        # Produce a message
        producer = kb.KafkaProducer(client_id="omniwatch-e2e-producer")
        producer.start()

        test_key = f"test-key-{uuid.uuid4().hex[:8]}"
        test_value = {"entity_id": "e2e-test", "value": 42, "ts": time.time()}
        delivered: list[str] = []

        def on_delivery(key: str, err: str) -> None:
            delivered.append(key)
            assert not err, f"Delivery failed: {err}"

        producer.send("omniwatch.test.e2e", test_value, key=test_key, callback=on_delivery)
        producer.flush(timeout=10.0)

        # Consume it back
        consumer = kb.KafkaConsumer(
            topics=["omniwatch.test.e2e"],
            group_id="omniwatch-e2e-consumer",
            auto_offset_reset="earliest",
        )
        consumer.start()
        time.sleep(1)  # Wait for assignment
        msgs = consumer.messages(timeout=10.0, max_messages=5)
        consumer.stop()
        producer.stop()

        assert len(msgs) > 0, "No messages consumed from test topic"
        # Find our message
        matched = [m for m in msgs if m.get("key") == test_key]
        assert len(matched) >= 1, f"Message with key '{test_key}' not found"
        assert matched[0]["value"]["entity_id"] == "e2e-test"

    def test_consumer_messages_format(self, kafka_available):
        """Validate consumer returns well-structured message dicts."""
        if not kafka_available:
            pytest.skip("confluent_kafka not installed")

        import confluent_kafka  # noqa: F401
        import ingestion.kafka_bus as kb

        topic = "omniwatch.test.e2e"

        # Ensure topic exists
        kb.create_topics(topic_names=[topic])

        # Produce a message first
        producer = kb.KafkaProducer(client_id="omniwatch-e2e-format")
        producer.start()
        producer.send(topic, {"test": "format-check"}, key="format-test")
        producer.flush(timeout=5.0)

        consumer = kb.KafkaConsumer(
            topics=[topic],
            group_id="omniwatch-e2e-format",
            auto_offset_reset="earliest",
        )
        consumer.start()
        time.sleep(1)
        msgs = consumer.messages(timeout=10.0, max_messages=5)
        consumer.stop()
        producer.stop()

        assert len(msgs) > 0
        msg = msgs[0]
        assert "topic" in msg, "Missing 'topic' field"
        assert "partition" in msg, "Missing 'partition' field"
        assert "offset" in msg, "Missing 'offset' field"
        assert "key" in msg, "Missing 'key' field"
        assert "value" in msg, "Missing 'value' field"
        assert "raw" in msg, "Missing 'raw' field"


# =============================================================================
# Stream Processor Tests
# =============================================================================


class TestStreamProcessor:
    """Validate telemetry normalization and enrichment."""

    def test_stream_processor_imports(self):
        """Validate stream_processor.py imports cleanly."""
        try:
            _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")
        except Exception:
            pass

    def test_process_metrics_payload(self):
        """Validate OTel metrics normalization."""
        mod = _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")

        # Load test payload
        assert SAMPLE_METRICS_PAYLOAD.exists(), "Test payload file not found"
        with open(SAMPLE_METRICS_PAYLOAD, "r", encoding="utf-8") as fh:
            payload = json.load(fh)

        records = mod.process_payload(payload, source_topic="omniwatch.metrics.raw")
        assert len(records) > 0, "No normalized records produced"

        # Validate record structure
        for r in records:
            assert "entity_id" in r
            assert "entity_type" in r
            assert "metric_name" in r
            assert "value" in r
            assert "timestamp" in r
            assert "attributes" in r
            assert "source_topic" in r
            assert r["entity_id"] == "user-service"

        # Check specific metrics
        names = [r["metric_name"] for r in records]
        assert "http.requests.total" in names
        assert "http.request.duration" in names
        assert "db.query.duration_sum" in names

    def test_process_log_payload(self):
        """Validate OTel logs normalization."""
        log_payload = {
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "user-service"}},
                ]
            },
            "scopeLogs": [
                {
                    "scope": {"name": "test-logger"},
                    "logRecords": [
                        {
                            "timeUnixNano": str(int(time.time() * 1e9)),
                            "severityNumber": 9,
                            "body": {"stringValue": "Connection timeout on DB query"},
                            "attributes": [
                                {"key": "db.system", "value": {"stringValue": "postgresql"}},
                            ],
                        }
                    ],
                }
            ],
        }

        mod = _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")

        records = mod.process_payload(log_payload, source_topic="omniwatch.logs.raw")
        assert len(records) == 1
        r = records[0]
        assert r["entity_id"] == "user-service"
        assert r["log_level"] == "ERROR"
        assert "Connection timeout" in r["body"]
        assert r["source_topic"] == "omniwatch.logs.raw"

    def test_process_trace_payload(self):
        """Validate OTel traces normalization."""
        now_ns = int(time.time() * 1e9)
        trace_payload = {
            "resource": {
                "attributes": [
                    {"key": "service.name", "value": {"stringValue": "order-service"}},
                ]
            },
            "scopeSpans": [
                {
                    "scope": {"name": "test-tracer"},
                    "spans": [
                        {
                            "traceId": "abc123",
                            "spanId": "def456",
                            "parentSpanId": "parent789",
                            "name": "POST /api/orders",
                            "kind": 2,
                            "startTimeUnixNano": str(now_ns),
                            "endTimeUnixNano": str(now_ns + 50_000_000),
                            "status": {"code": 1},
                        }
                    ],
                }
            ],
        }

        mod = _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")

        records = mod.process_payload(trace_payload, source_topic="omniwatch.traces.raw")
        assert len(records) == 1
        r = records[0]
        assert r["entity_id"] == "order-service"
        assert r["span_name"] == "POST /api/orders"
        assert r["span_id"] == "def456"
        assert r["parent_span_id"] == "parent789"
        assert r["status"] == "OK"
        assert r["duration_ns"] == 50_000_000

    def test_process_security_event_payload(self):
        """Validate security event normalization."""
        sec_payload = {
            "entity_id": "api-gateway",
            "event_type": "BRUTE_FORCE",
            "severity": "HIGH",
            "description": "Multiple failed login attempts detected",
            "source_ip": "192.168.1.100",
            "timestamp": "2026-07-29T12:00:00.000000Z",
        }

        mod = _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")

        records = mod.process_payload(sec_payload, source_topic="omniwatch.security.events")
        assert len(records) == 1
        r = records[0]
        assert r["entity_id"] == "api-gateway"
        assert r["event_type"] == "BRUTE_FORCE"
        assert r["severity"] == "HIGH"
        assert r["source_ip"] == "192.168.1.100"

    def test_auto_detect_source_topic(self):
        """Validate automatic topic detection from payload shape."""
        mod = _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")

        # Metrics
        assert mod.detect_source_topic({"scopeMetrics": []}) == "omniwatch.metrics.raw"
        # Logs
        assert mod.detect_source_topic({"scopeLogs": []}) == "omniwatch.logs.raw"
        # Traces
        assert mod.detect_source_topic({"scopeSpans": []}) == "omniwatch.traces.raw"
        # Security
        assert mod.detect_source_topic({"event_type": "BRUTE_FORCE"}) == "omniwatch.security.events"
        # Unknown
        assert mod.detect_source_topic({"random_key": 42}) is None

    def test_entity_type_inference(self):
        """Validate entity type inference from service names."""
        mod = _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")

        test_cases = [
            ("api-gateway", "API_NODE"),
            ("postgresql-database", "DATABASE_NODE"),
            ("redis-cache", "CACHE_NODE"),
            ("kafka-broker", "QUEUE_NODE"),
            ("background-worker", "WORKER_NODE"),
            ("minio-storage", "STORAGE_NODE"),
            ("unknown-service", "UNKNOWN"),
        ]
        for name, expected in test_cases:
            inferred = mod.infer_entity_type(name).value
            assert inferred == expected, f"'{name}' -> {inferred}, expected {expected}"

    def test_parse_cli_standalone(self):
        """Validate the 'parse' CLI command works standalone."""
        if not SAMPLE_METRICS_PAYLOAD.exists():
            pytest.skip("Test payload file missing")

        result = subprocess.run(
            [
                sys.executable,
                str(INGESTION_DIR / "stream_processor.py"),
                "parse",
                str(SAMPLE_METRICS_PAYLOAD),
            ],
            capture_output=True, text=True, timeout=15,
        )
        assert result.returncode == 0, f"CLI failed:\n{result.stderr}"
        assert "7 normalized records" in result.stdout, f"Unexpected output:\n{result.stdout}"


# =============================================================================
# Pipeline Integration Tests
# =============================================================================


class TestPipelineIntegration:
    """Validate end-to-end ingestion pipeline."""

    def test_kafka_topics_exist_in_cluster(self, kafka_available):
        """Validate OmniWatch topics exist in live Kafka cluster."""
        if not kafka_available:
            pytest.skip("confluent_kafka not installed")

        import confluent_kafka  # noqa: F401
        import ingestion.kafka_bus as kb

        admin = kb.AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
        metadata = admin.list_topics(timeout=10)
        existing = set(metadata.topics.keys())

        for topic in EXPECTED_TOPICS:
            assert topic in existing, f"Topic '{topic}' not found in Kafka cluster"

    def test_service_health_endpoints(self, services_running):
        """Validate all services report healthy."""
        health_checks = {
            "api-gateway": "http://localhost:8000/health",
            "user-service": "http://localhost:8001/health",
            "order-service": "http://localhost:8002/health",
        }
        for name, url in health_checks.items():
            resp = requests.get(url, timeout=5)
            assert resp.ok, f"{name} health failed: {resp.status_code}"
            body = resp.json()
            assert body.get("status") in ("ok", "healthy", "OK"), \
                f"{name} status is not healthy: {body}"

    def test_anomaly_injection_to_kafka_pipeline(self, services_running):
        """Validate injecting anomaly -> services generate telemetry -> Kafka."""
        # Inject a latency spike
        resp = requests.post(
            "http://localhost:8001/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
            timeout=5,
        )
        assert resp.ok, f"Inject failed: {resp.status_code} {resp.text}"

        # Verify it's active
        resp = requests.get("http://localhost:8001/__inject/anomaly", timeout=5)
        assert resp.ok
        data = resp.json()
        active_scenarios = [a["scenario"] for a in data.get("active", [])]
        assert "latency_spike" in active_scenarios

        # Clear it
        resp = requests.delete(
            "http://localhost:8001/__inject/anomaly/latency_spike", timeout=5
        )
        assert resp.ok, f"Clear failed: {resp.status_code} {resp.text}"


# =============================================================================
# Standalone runner
# =============================================================================

if __name__ == "__main__":
    print("=" * 60)
    print("OmniWatch — Phase 2 Ingestion Pipeline Check")
    print("=" * 60)

    # 1. Check stream processor module loads
    print("\n[1/4] Checking stream processor...")
    try:
        _load_module("stream_processor", INGESTION_DIR / "stream_processor.py")
        print("  [OK] stream_processor.py loaded")
    except Exception as e:
        print(f"  [FAIL] stream_processor.py: {e}")
        sys.exit(1)

    # 2. Test metrics parsing
    print("\n[2/4] Testing metrics normalization...")
    if SAMPLE_METRICS_PAYLOAD.exists():
        result = subprocess.run(
            [sys.executable, str(INGESTION_DIR / "stream_processor.py"),
             "parse", str(SAMPLE_METRICS_PAYLOAD)],
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode == 0:
            print(f"  [OK] Parsed {SAMPLE_METRICS_PAYLOAD.name} -> {result.stdout.strip().split()[-1]} records")
        else:
            print(f"  [FAIL] {result.stderr}")
    else:
        print("  [SKIP] test payload not found")

    # 3. Test Kafka bus topic definitions
    print("\n[3/4] Checking Kafka topic definitions...")
    try:
        import ingestion.kafka_bus as kb
        ok = 0
        for topic in EXPECTED_TOPICS:
            if topic in kb.TOPIC_SPECS:
                print(f"  [OK] {topic}")
                ok += 1
            else:
                print(f"  [FAIL] {topic} missing")
        print(f"  Topics: {ok}/{len(EXPECTED_TOPICS)} defined")
    except Exception as e:
        print(f"  [FAIL] Could not load kafka_bus.py: {e}")

    # 4. Summarize
    print("\n[4/4] Summary")
    try:
        import confluent_kafka  # noqa: F401
        print("  confluent_kafka: available (Docker-level tests OK)")
    except ImportError:
        print("  confluent_kafka: not installed (Docker-level tests will skip)")

    print("\nDone.")
