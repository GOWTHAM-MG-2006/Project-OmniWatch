"""
OmniWatch — Phase 2 E2E Test Suite
Purpose: Validate OTel Collector -> Kafka -> Flink -> Normalized Kafka + MinIO
"""
import json
import os
import time
import requests

DOCKER_HOST = os.environ.get("DOCKER_HOST", "localhost")
PROTO_CONTENT_TYPE = "application/x-protobuf"

# ---------------------------------------------------------------------------
# Phase 2 Pipeline E2E Tests
# ---------------------------------------------------------------------------


class TestPhase2E2E:
    """
    Phase 2 End-to-End Test Suite.
    Validates: OTel Collector -> Kafka -> Flink -> Normalized Kafka + MinIO.
    """

    def test_otel_collector_health(self, otelcol_service):
        """Verify OTel Collector is reachable and reporting metrics."""
        resp = requests.get("http://localhost:8888/metrics", timeout=5)
        assert resp.status_code == 200
        assert "otelcol" in resp.text

    def test_raw_metric_topic_receives_data(
        self, otelcol_service, sample_metric_payload
    ):
        """Inject OTel metric via OTLP HTTP; verify it arrives in omniwatch.metrics.raw."""
        resp = requests.post(
            "http://localhost:4318/v1/metrics",
            data=json.dumps(sample_metric_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        assert resp.status_code == 200 or resp.status_code == 202
        # Give pipeline time to propagate
        time.sleep(2)
        from conftest import kafka_consume_one
        msg = kafka_consume_one("omniwatch.metrics.raw", timeout=10)
        assert msg is not None, "No message in omniwatch.metrics.raw"
        assert "resourceMetrics" in msg

    def test_raw_log_topic_receives_data(
        self, otelcol_service, sample_log_payload
    ):
        """Inject OTel log via OTLP HTTP; verify it arrives in omniwatch.logs.raw."""
        resp = requests.post(
            "http://localhost:4318/v1/logs",
            data=json.dumps(sample_log_payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
        assert resp.status_code == 200 or resp.status_code == 202
        time.sleep(2)
        from conftest import kafka_consume_one
        msg = kafka_consume_one("omniwatch.logs.raw", timeout=10)
        assert msg is not None, "No message in omniwatch.logs.raw"
        assert "resourceLogs" in msg

    def test_flink_job_is_running(self, flink_rest_api):
        """Verify Flink job is in RUNNING state."""
        resp = flink_rest_api.get("/jobs")
        assert resp.status_code == 200
        jobs = resp.json()
        running = [j for j in jobs.get("jobs", []) if j["status"] == "RUNNING"]
        assert len(running) >= 1, "No RUNNING Flink jobs found"

    def test_normalized_metric_topic_has_data(self, flink_rest_api):
        """Flink has consumed from raw and produced to normalized topic (anomaly signals)."""
        from conftest import kafka_consume_one
        msg = kafka_consume_one("omniwatch.metrics.normalized", timeout=30)
        assert msg is not None, "No data in omniwatch.metrics.normalized"
        # Validate anomaly signal schema (entity-resolved anomalies)
        assert "entity_id" in msg
        assert "entity_type" in msg
        assert "timestamp" in msg
        assert "source_type" in msg
        assert "source_topic" in msg

    def test_normalized_log_topic_has_data(self):
        """Flink has produced normalized log records."""
        from conftest import kafka_consume_one
        msg = kafka_consume_one("omniwatch.logs.normalized", timeout=30)
        assert msg is not None, "No data in omniwatch.logs.normalized"
        # Validate normalized schema
        assert "severity" in msg or "logLevel" in msg or "level" in msg
        assert "body" in msg or "message" in msg

    def test_normalized_trace_topic_has_data(self, sample_trace_payload):
        """Flink has produced normalized trace records."""
        from conftest import kafka_consume_one, kafka_produce
        # Seed the raw trace topic (OTel collector may not forward traces to Kafka)
        kafka_produce("omniwatch.traces.raw", sample_trace_payload)
        time.sleep(5)
        msg = kafka_consume_one("omniwatch.traces.normalized", timeout=30)
        assert msg is not None, "No data in omniwatch.traces.normalized"
        assert "traceId" in msg or "trace_id" in msg
        assert "spanId" in msg or "span_id" in msg
        assert "durationMs" in msg or "duration_ms" in msg

    def test_minio_bucket_has_data(self, minio_client):
        """Flink's sink has written at least one file to MinIO."""
        objects = list(
            minio_client.list_objects(
                "omniwatch-telemetry-archive",
                prefix="entity-resolution/dt=",
                recursive=True,
            )
        )
        assert len(objects) >= 1, "No objects in omniwatch-telemetry-archive/ with entity-resolution/dt= prefix"
        assert objects[0].size > 0, "First object is empty"

    def test_security_events_are_routed(self, sample_security_payload):
        """Security events arrive in omniwatch.security.normalized."""
        from conftest import kafka_produce, kafka_consume_one
        # Produce directly to the security events topic Flink consumes from
        kafka_produce("omniwatch.security.events", sample_security_payload)
        time.sleep(5)
        msg = kafka_consume_one("omniwatch.security.normalized", timeout=30)
        assert msg is not None, "No data in omniwatch.security.normalized"
        assert "attackType" in msg or "attack_type" in msg
        # Should NOT be in metrics.normalized
        non_sec = kafka_consume_one(
            "omniwatch.metrics.normalized", timeout=5, quiet=True
        )
        if non_sec:
            src = non_sec.get("sourceType") or non_sec.get("source_type") or ""
            assert "security" not in src
