"""
OmniWatch — Phase 3 E2E Test Fixtures

Fixtures and helpers for end-to-end testing of the entity resolution Flink job.
Produces TelemetryEvent JSON to normalized Kafka topics and consumes
verified output from omniwatch.entities.resolved / omniwatch.entities.relationships.
"""
import json
import time
import uuid
from typing import Optional

import pytest
import requests

KAFKA_BROKERS = "127.0.0.1:9092"
OUTPUT_RESOLVED = "omniwatch.entities.resolved"
OUTPUT_RELATIONSHIPS = "omniwatch.entities.relationships"
FLINK_REST = "http://localhost:8081"


# --------------------------------------------------------------------------- #
# Pytest configuration
# --------------------------------------------------------------------------- #

def pytest_configure(config):
    config.addinivalue_line("markers", "slow: marks tests as slow (deselect with '-m \"not slow\"')")


@pytest.fixture(scope="session", autouse=True)
def require_kafka():
    """Skip all tests if Kafka is not reachable."""
    try:
        from kafka import KafkaProducer
        p = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
        p.close()
    except Exception:
        pytest.skip("Kafka not reachable at " + KAFKA_BROKERS)


@pytest.fixture(scope="session", autouse=True)
def require_flink_job():
    """Skip all tests if the Flink entity-resolution job is not RUNNING."""
    for _ in range(10):
        try:
            r = requests.get(f"{FLINK_REST}/jobs", timeout=2)
            if r.status_code == 200:
                jobs = r.json().get("jobs", [])
                if any(j["status"] == "RUNNING" for j in jobs):
                    return
        except Exception:
            pass
        time.sleep(1)
    pytest.skip("Flink job not RUNNING")


# --------------------------------------------------------------------------- #
# Fixtures
# --------------------------------------------------------------------------- #

@pytest.fixture
def kafka_producer():
    """A Kafka producer for sending TelemetryEvent JSON."""
    from kafka import KafkaProducer
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKERS,
        value_serializer=lambda m: json.dumps(m).encode("utf-8"),
    )
    yield producer
    producer.close()


@pytest.fixture
def test_group_id():
    """A unique consumer group ID for isolation."""
    return f"e2e-{uuid.uuid4().hex[:8]}"


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def make_event(entity_id: str, entity_type: str = "API_NODE",
               source_type: str = "performance",
               trace_id: str = None, span_id: str = None,
               parent_span_id: str = None,
               span_name: str = None,
               duration_ms: int = None,
               status: str = None) -> dict:
    """Build a TelemetryEvent JSON dict (snake_case for Jackson SNAKE_CASE mapper)."""
    event = {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "timestamp": int(time.time() * 1000),
        "source_type": source_type,
        "source_topic": "omniwatch.metrics.normalized",
    }
    if trace_id:
        event["trace_id"] = trace_id
    if span_id:
        event["span_id"] = span_id
    if parent_span_id:
        event["parent_span_id"] = parent_span_id
    if span_name:
        event["span_name"] = span_name
    if duration_ms is not None:
        event["duration_ms"] = duration_ms
    if status:
        event["status"] = status
    return event


def produce_event(kafka_producer, topic: str, event: dict):
    """Produce a single event to a Kafka topic and flush."""
    kafka_producer.send(topic, event)
    kafka_producer.flush(timeout=5)


def consume_filtered(topic: str, group_id: str, predicate, timeout: int = 30):
    """
    Consume from a Kafka topic (earliest offset) until predicate(msg) is True.
    Returns the first matching message value, or None on timeout.
    """
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        enable_auto_commit=False,
    )
    try:
        for msg in consumer:
            if predicate(msg.value):
                return msg.value
    finally:
        consumer.close()
    return None


def consume_all_matching(topic: str, group_id: str, predicate, timeout: int = 30):
    """
    Consume from a Kafka topic (earliest offset) for up to *timeout* seconds,
    collecting ALL messages where predicate(msg) is True.  Returns the list.
    """
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=KAFKA_BROKERS,
        group_id=group_id,
        auto_offset_reset="earliest",
        consumer_timeout_ms=timeout * 1000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")) if m else None,
        enable_auto_commit=False,
    )
    try:
        matching = []
        for msg in consumer:
            if predicate(msg.value):
                matching.append(msg.value)
        return matching
    finally:
        consumer.close()


def wait_for_output(topic: str, group_id: str, expected_entity_id: str,
                    timeout: int = 30) -> Optional[dict]:
    """Poll an output topic until a message with the expected entity_id appears."""
    return consume_filtered(
        topic, group_id,
        lambda v: v is not None and v.get("entity_id") == expected_entity_id,
        timeout=timeout,
    )


def produce_and_consume(kafka_producer, test_group_id, input_topic: str,
                        output_topic: str, event: dict, expected_entity_id: str,
                        timeout: int = 30) -> Optional[dict]:
    """Produce an event, then wait for the resolved entity in the output topic."""
    produce_event(kafka_producer, input_topic, event)
    return wait_for_output(output_topic, test_group_id, expected_entity_id, timeout)
