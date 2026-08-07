"""
OmniWatch — Telemetry Ingestion: Kafka Message Bus
Component: kafka_bus.py
Phase: 2
Purpose: Central message bus for all telemetry — auto-creates topics,
         provides producer/consumer classes with retry and graceful shutdown.
Inputs: Caller-provided topic name + message payload (bytes)
Outputs: Kafka topic messages
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable

import confluent_kafka
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, Producer

logger = logging.getLogger("omniwatch.kafka_bus")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
)

# All OmniWatch topics with their configuration
# Raw topics (OTel Collector → Kafka → Flink)
TOPIC_METRICS_RAW = "omniwatch.metrics.raw"
TOPIC_LOGS_RAW = "omniwatch.logs.raw"
TOPIC_TRACES_RAW = "omniwatch.traces.raw"
TOPIC_EVENTS_RAW = "omniwatch.events.raw"
TOPIC_SECURITY_RAW = "omniwatch.security.raw"
# Legacy security events topic (kept for backward compat)
TOPIC_SECURITY_EVENTS = "omniwatch.security.events"
# Normalized topics (Flink → downstream layers)
TOPIC_METRICS_NORMALIZED = "omniwatch.metrics.normalized"
TOPIC_LOGS_NORMALIZED = "omniwatch.logs.normalized"
TOPIC_TRACES_NORMALIZED = "omniwatch.traces.normalized"
TOPIC_EVENTS_NORMALIZED = "omniwatch.events.normalized"
TOPIC_SECURITY_NORMALIZED = "omniwatch.security.normalized"
# Downstream topics (Phase 3+)
TOPIC_ANOMALIES_DETECTED = "omniwatch.anomalies.detected"
TOPIC_INCIDENTS_CAUSAL = "omniwatch.incidents.causal"
TOPIC_INCIDENTS_CREATED = "omniwatch.incidents.created"
TOPIC_REMEDIATION_ACTIONS = "omniwatch.remediation.actions"
# Generated topics (Phase 10 — GenAI layer)
TOPIC_GENERATED_SUMMARIES = "omniwatch.generated.summaries"
TOPIC_GENERATED_RUNBOOKS = "omniwatch.generated.runbooks"
TOPIC_GENERATED_REPORTS = "omniwatch.generated.reports"

ALL_TOPICS: list[str] = [
    # Raw
    TOPIC_METRICS_RAW,
    TOPIC_LOGS_RAW,
    TOPIC_TRACES_RAW,
    TOPIC_EVENTS_RAW,
    TOPIC_SECURITY_RAW,
    TOPIC_SECURITY_EVENTS,
    # Normalized
    TOPIC_METRICS_NORMALIZED,
    TOPIC_LOGS_NORMALIZED,
    TOPIC_TRACES_NORMALIZED,
    TOPIC_EVENTS_NORMALIZED,
    TOPIC_SECURITY_NORMALIZED,
    # Downstream
    TOPIC_ANOMALIES_DETECTED,
    TOPIC_INCIDENTS_CAUSAL,
    TOPIC_INCIDENTS_CREATED,
    TOPIC_REMEDIATION_ACTIONS,
]

# Topic metadata for documentation / routing
@dataclass
class TopicSpec:
    name: str
    producer: str
    consumer: str
    description: str
    partitions: int = 3
    replication_factor: int = 1

TOPIC_SPECS: dict[str, TopicSpec] = {
    # Raw topics (OTel Collector → Kafka → Flink)
    TOPIC_METRICS_RAW: TopicSpec(
        name=TOPIC_METRICS_RAW,
        producer="otel-collector",
        consumer="flink-ingestion",
        description="Raw metric time-series data from OTel Collector",
        partitions=3,
    ),
    TOPIC_LOGS_RAW: TopicSpec(
        name=TOPIC_LOGS_RAW,
        producer="otel-collector",
        consumer="flink-ingestion",
        description="Raw log events from OTel Collector",
        partitions=3,
    ),
    TOPIC_TRACES_RAW: TopicSpec(
        name=TOPIC_TRACES_RAW,
        producer="otel-collector",
        consumer="flink-ingestion",
        description="Raw trace spans from OTel Collector",
        partitions=3,
    ),
    TOPIC_EVENTS_RAW: TopicSpec(
        name=TOPIC_EVENTS_RAW,
        producer="otel-collector",
        consumer="flink-ingestion",
        description="Raw event data from OTel Collector",
        partitions=2,
    ),
    TOPIC_SECURITY_RAW: TopicSpec(
        name=TOPIC_SECURITY_RAW,
        producer="otel-collector",
        consumer="flink-ingestion",
        description="Raw security events from OTel Collector",
        partitions=2,
    ),
    TOPIC_SECURITY_EVENTS: TopicSpec(
        name=TOPIC_SECURITY_EVENTS,
        producer="simulation",
        consumer="flink-ingestion",
        description="Security events from simulation / external sources (legacy)",
        partitions=2,
    ),
    # Normalized topics (Flink → downstream layers)
    TOPIC_METRICS_NORMALIZED: TopicSpec(
        name=TOPIC_METRICS_NORMALIZED,
        producer="flink-ingestion",
        consumer="entity-resolution",
        description="Normalized metric data from Flink",
        partitions=3,
    ),
    TOPIC_LOGS_NORMALIZED: TopicSpec(
        name=TOPIC_LOGS_NORMALIZED,
        producer="flink-ingestion",
        consumer="entity-resolution",
        description="Normalized log data from Flink",
        partitions=3,
    ),
    TOPIC_TRACES_NORMALIZED: TopicSpec(
        name=TOPIC_TRACES_NORMALIZED,
        producer="flink-ingestion",
        consumer="entity-resolution",
        description="Normalized trace data from Flink",
        partitions=3,
    ),
    TOPIC_EVENTS_NORMALIZED: TopicSpec(
        name=TOPIC_EVENTS_NORMALIZED,
        producer="flink-ingestion",
        consumer="entity-resolution",
        description="Normalized event data from Flink",
        partitions=2,
    ),
    TOPIC_SECURITY_NORMALIZED: TopicSpec(
        name=TOPIC_SECURITY_NORMALIZED,
        producer="flink-ingestion",
        consumer="entity-resolution",
        description="Normalized security events from Flink",
        partitions=2,
    ),
    # Downstream topics (Phase 3+)
    TOPIC_ANOMALIES_DETECTED: TopicSpec(
        name=TOPIC_ANOMALIES_DETECTED,
        producer="predictive",
        consumer="prioritization",
        description="Detected anomaly signals from predictive layer",
    ),
    TOPIC_INCIDENTS_CAUSAL: TopicSpec(
        name=TOPIC_INCIDENTS_CAUSAL,
        producer="causal",
        consumer="prioritization",
        description="Root cause analysis results from causal engine",
    ),
    TOPIC_INCIDENTS_CREATED: TopicSpec(
        name=TOPIC_INCIDENTS_CREATED,
        producer="prioritization",
        consumer="causal, orchestration",
        description="Prioritized incident records",
    ),
    TOPIC_REMEDIATION_ACTIONS: TopicSpec(
        name=TOPIC_REMEDIATION_ACTIONS,
        producer="orchestration",
        consumer="learning, dashboard",
        description="Remediation action results",
    ),
}


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------

class KafkaBusError(Exception):
    """Base exception for Kafka bus operations."""


class TopicCreationError(KafkaBusError):
    """Raised when topic creation fails."""


class PublishError(KafkaBusError):
    """Raised when message publish fails after retries."""


# ---------------------------------------------------------------------------
# Topic Admin
# ---------------------------------------------------------------------------

def create_topics(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    topic_names: list[str] | None = None,
) -> dict[str, str]:
    """Create OmniWatch Kafka topics if they don't exist.

    Returns a dict mapping topic name to status ("created" or "already exists").
    """
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    topics_to_create = topic_names or ALL_TOPICS

    # Check which topics already exist
    existing = admin.list_topics(timeout=10).topics

    new_topics: list[NewTopic] = []
    for name in topics_to_create:
        if name in existing:
            logger.info("[kafka_bus] topic already exists: %s", name)
            continue
        spec = TOPIC_SPECS.get(name)
        if spec:
            new_topics.append(
                NewTopic(
                    name,
                    num_partitions=spec.partitions,
                    replication_factor=spec.replication_factor,
                )
            )
        else:
            # Fallback for topics not in spec (e.g., custom topics)
            new_topics.append(
                NewTopic(name, num_partitions=1, replication_factor=1)
            )

    if not new_topics:
        return {name: "already exists" for name in topics_to_create}

    results = admin.create_topics(new_topics, request_timeout=15)
    statuses: dict[str, str] = {}
    for name, future in results.items():
        try:
            future.result()  # Will raise if creation failed
            statuses[name] = "created"
            logger.info("[kafka_bus] topic created: %s", name)
        except Exception as exc:
            if "already exists" in str(exc).lower():
                statuses[name] = "already exists"
            else:
                statuses[name] = f"error: {exc}"
                logger.warning("[kafka_bus] topic creation failed: %s — %s", name, exc)

    # Fill in already-existing topics not in results
    for name in topics_to_create:
        if name not in statuses:
            statuses[name] = "already exists"

    return statuses


# ---------------------------------------------------------------------------
# KafkaProducer
# ---------------------------------------------------------------------------

class KafkaProducer:
    """High-level Kafka producer with delivery confirmation and retry.

    Usage::

        producer = KafkaProducer()
        producer.start()
        producer.send("omniwatch.metrics.raw", {"entity_id": "abc", "value": 42})
        producer.flush()
    """

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        client_id: str | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id or f"omniwatch-producer-{os.getpid()}"
        self._producer: Producer | None = None
        self._running = False

    def start(self) -> None:
        """Initialize the producer connection."""
        conf: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "client.id": self._client_id,
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 500,
            "compression.type": "snappy",
            "linger.ms": 10,
            "batch.num.messages": 500,
        }
        self._producer = Producer(conf)
        self._running = True
        logger.info(
            "[kafka_bus] producer started: id=%s servers=%s",
            self._client_id,
            self._bootstrap_servers,
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Flush and shutdown the producer."""
        self._running = False
        if self._producer:
            remaining = self._producer.flush(timeout)
            if remaining > 0:
                logger.warning(
                    "[kafka_bus] producer stop: %d messages unsent", remaining
                )
        logger.info("[kafka_bus] producer stopped")

    def send(
        self,
        topic: str,
        value: dict[str, Any],
        key: str | None = None,
        callback: Callable[[str, str], None] | None = None,
    ) -> None:
        """Publish a message to a Kafka topic.

        Args:
            topic: Target topic name.
            value: Dict payload (will be JSON-serialized).
            key: Optional message key for partitioning.
            callback: Optional callback(key, error_msg) on delivery.

        Raises:
            PublishError: If the producer is not started.
        """
        if not self._producer:
            raise PublishError("Producer not started. Call start() first.")

        payload = json.dumps(value, default=str).encode("utf-8")
        encoded_key = key.encode("utf-8") if key else None

        # Delivery callback
        def _delivery(err: Any, msg: Any) -> None:
            topic_name = msg.topic() if msg else "unknown"
            if err:
                logger.error(
                    "[kafka_bus] delivery failed: topic=%s error=%s",
                    topic_name,
                    err,
                )
                if callback:
                    callback(key or "", str(err))
            else:
                partition = msg.partition() if msg else -1
                offset = msg.offset() if msg else -1
                logger.debug(
                    "[kafka_bus] delivered: topic=%s partition=%s offset=%s",
                    topic_name,
                    partition,
                    offset,
                )
                if callback:
                    callback(key or "", "")

        self._producer.produce(
            topic=topic,
            value=payload,
            key=encoded_key,
            on_delivery=_delivery,
        )

        # Trigger delivery on full batch
        if self._producer.poll(0) < 0:
            pass  # No immediate callback

    def flush(self, timeout: float = 5.0) -> int:
        """Flush pending messages. Returns remaining count."""
        if not self._producer:
            return 0
        return self._producer.flush(timeout)

    @property
    def is_running(self) -> bool:
        return self._running


# ---------------------------------------------------------------------------
# KafkaConsumer
# ---------------------------------------------------------------------------

class KafkaConsumer:
    """High-level Kafka consumer with auto-commit and graceful shutdown.

    Usage::

        consumer = KafkaConsumer(
            topics=["omniwatch.metrics.raw"],
            group_id="entity-resolution",
        )
        consumer.start()
        for msg in consumer.messages(timeout=30.0):
            print(msg)
        consumer.stop()
    """

    def __init__(
        self,
        topics: list[str],
        group_id: str,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        client_id: str | None = None,
        auto_offset_reset: str = "earliest",
    ) -> None:
        self._topics = topics
        self._group_id = group_id
        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id or f"omniwatch-consumer-{os.getpid()}"
        self._auto_offset_reset = auto_offset_reset
        self._consumer: Consumer | None = None
        self._running = False

    def start(self) -> None:
        """Initialize the consumer and subscribe to topics."""
        conf: dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": self._group_id,
            "client.id": self._client_id,
            "auto.offset.reset": self._auto_offset_reset,
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }
        self._consumer = Consumer(conf)
        assert self._consumer is not None
        self._consumer.subscribe(self._topics)
        self._running = True
        logger.info(
            "[kafka_bus] consumer started: id=%s group=%s topics=%s",
            self._client_id,
            self._group_id,
            self._topics,
        )

    def stop(self) -> None:
        """Close the consumer cleanly."""
        self._running = False
        if self._consumer:
            self._consumer.close()
        logger.info("[kafka_bus] consumer stopped")

    def messages(
        self, timeout: float = 5.0, max_messages: int = 100
    ) -> list[dict[str, Any]]:
        """Poll for messages. Returns list of parsed message dicts.

        Each message dict contains:
            - topic: str
            - partition: int
            - offset: int
            - key: str | None
            - value: dict | None (parsed from JSON)
            - raw: bytes | None
        """
        if not self._consumer:
            logger.warning("[kafka_bus] consumer not started")
            return []
        # Type-safe consumer reference
        consumer: confluent_kafka.Consumer = self._consumer

        results: list[dict[str, Any]] = []
        start = time.time()

        while len(results) < max_messages and (time.time() - start) < timeout:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                logger.error("[kafka_bus] consumer error: %s", msg.error())
                continue

            parsed: dict[str, Any] = {
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "raw": msg.value(),
            }
            # Attempt JSON decode
            try:
                parsed["value"] = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                parsed["value"] = None

            results.append(parsed)

            if len(results) >= max_messages:
                break

        return results

    @property
    def is_running(self) -> bool:
        return self._running


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def cli_create_topics() -> None:
    """CLI handler: create all OmniWatch Kafka topics."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    print(f"Creating topics on {KAFKA_BOOTSTRAP_SERVERS} ...")
    statuses = create_topics()
    print(f"{'Topic':45s} {'Status':s}")
    print("-" * 60)
    for name in ALL_TOPICS:
        spec = TOPIC_SPECS.get(name)
        label = f"{name}  ({spec.producer} -> {spec.consumer})" if spec else name
        status = statuses.get(name, "unknown")
        print(f"{label:45s} {status:s}")
    print("\nDone.")


def cli_list_topics() -> None:
    """CLI handler: list existing topics in the Kafka cluster."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    metadata = admin.list_topics(timeout=10)
    print(f"Topics on {KAFKA_BOOTSTRAP_SERVERS}:")
    print(f"{'Name':45s} {'Partitions':s}")
    print("-" * 60)
    for name, topic_meta in sorted(metadata.topics.items()):
        print(f"{name:45s} {topic_meta.partitions}")
    print(f"\nTotal: {len(metadata.topics)} topics")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    if len(sys.argv) > 1:
        command = sys.argv[1]
        if command == "create-topics":
            cli_create_topics()
        elif command == "list-topics":
            cli_list_topics()
        else:
            print(f"Unknown command: {command}")
            print("Usage: python kafka_bus.py [create-topics | list-topics]")
            sys.exit(1)
    else:
        print("Usage: python kafka_bus.py [create-topics | list-topics]")
        sys.exit(1)
