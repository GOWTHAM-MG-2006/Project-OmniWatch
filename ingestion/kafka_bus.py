"""
OmniWatch — Telemetry Ingestion Layer
Component: Kafka Bus
Phase: 2
Purpose: Central message bus for all telemetry — creates topics, provides producer/consumer classes
Inputs: Telemetry events from simulators and OTel Collector
Outputs: Structured messages to Kafka topics for downstream consumption
"""

import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ---------------------------------------------------------------------------
# All Kafka topics defined in AGENTS.md
# ---------------------------------------------------------------------------
ALL_TOPICS = [
    "omniwatch.metrics.raw",
    "omniwatch.logs.raw",
    "omniwatch.traces.raw",
    "omniwatch.security.events",
    "omniwatch.anomalies.detected",
    "omniwatch.incidents.created",
    "omniwatch.remediation.actions",
]

# Topic metadata: name → (partitions, replication_factor)
TOPIC_CONFIG = {
    "omniwatch.metrics.raw": (3, 1),
    "omniwatch.logs.raw": (3, 1),
    "omniwatch.traces.raw": (3, 1),
    "omniwatch.security.events": (3, 1),
    "omniwatch.anomalies.detected": (3, 1),
    "omniwatch.incidents.created": (3, 1),
    "omniwatch.remediation.actions": (3, 1),
}


# ---------------------------------------------------------------------------
# Topic creation
# ---------------------------------------------------------------------------

def create_topics(bootstrap_servers: str = None):
    """Create all required Kafka topics if they don't exist."""
    from confluent_kafka.admin import AdminClient, NewTopic

    servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
    admin = AdminClient({"bootstrap.servers": servers})

    # Check existing topics
    metadata = admin.list_topics(timeout=10)
    existing = set(metadata.topics.keys())

    # Build list of topics to create
    topics_to_create = []
    for topic_name in ALL_TOPICS:
        if topic_name not in existing:
            num_partitions, replication = TOPIC_CONFIG.get(topic_name, (3, 1))
            topics_to_create.append(
                NewTopic(
                    topic_name,
                    num_partitions=num_partitions,
                    replication_factor=replication,
                )
            )

    if not topics_to_create:
        print(f"[kafka_bus] All {len(ALL_TOPICS)} topics already exist")
        return True

    # Create topics
    futures = admin.create_topics(topics_to_create)
    success_count = 0
    for topic, future in futures.items():
        try:
            future.result()
            print(f"[kafka_bus] Created topic: {topic}")
            success_count += 1
        except KafkaException as e:
            if e.args[0].code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[kafka_bus] Topic already exists: {topic}")
                success_count += 1
            else:
                print(f"[kafka_bus] FAILED to create topic {topic}: {e}")

    print(f"[kafka_bus] Topic creation complete: {success_count}/{len(topics_to_create)}")
    return success_count == len(topics_to_create)


def list_topics(bootstrap_servers: str = None) -> list:
    """List all Kafka topics."""
    from confluent_kafka.admin import AdminClient

    servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
    admin = AdminClient({"bootstrap.servers": servers})
    metadata = admin.list_topics(timeout=10)
    return sorted(metadata.topics.keys())


# ---------------------------------------------------------------------------
# Kafka Producer class
# ---------------------------------------------------------------------------

class KafkaProducer:
    """Wrapper around confluent_kafka Producer with retry logic."""

    def __init__(self, bootstrap_servers: str = None):
        servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self._conf = {
            "bootstrap.servers": servers,
            "client.id": "omniwatch-producer",
            "acks": "all",
            "retries": 3,
            "retry.backoff.ms": 1000,
            "delivery.timeout.ms": 30000,
        }
        self._producer = Producer(self._conf)

    def send(self, topic: str, message: dict, key: str = None) -> bool:
        """
        Send a JSON message to a Kafka topic.

        Args:
            topic: Kafka topic name
            message: Dictionary to send as JSON
            key: Optional message key (for partitioning)

        Returns:
            True if message was queued, False on error
        """
        try:
            value = json.dumps(message, default=str).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None

            self._producer.produce(
                topic=topic,
                key=key_bytes,
                value=value,
                on_delivery=self._delivery_callback,
            )
            self._producer.poll(0)
            return True
        except KafkaException as e:
            print(f"[kafka_bus] WARNING: Failed to produce to {topic}: {e}")
            return False
        except Exception as e:
            print(f"[kafka_bus] WARNING: Unexpected error producing to {topic}: {e}")
            return False

    def flush(self, timeout: float = 10.0):
        """Flush all pending messages."""
        self._producer.flush(timeout)

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            print(f"[kafka_bus] Delivery failed: {err}")


# ---------------------------------------------------------------------------
# Kafka Consumer class
# ---------------------------------------------------------------------------

class KafkaConsumer:
    """Wrapper around confluent_kafka Consumer with subscription management."""

    def __init__(self, group_id: str, bootstrap_servers: str = None):
        servers = bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS
        self._conf = {
            "bootstrap.servers": servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "session.timeout.ms": 30000,
        }
        self._consumer = Consumer(self._conf)
        self._subscribed = False

    def subscribe(self, topics: list):
        """Subscribe to a list of Kafka topics."""
        self._consumer.subscribe(topics)
        self._subscribed = True
        print(f"[kafka_bus] Subscribed to: {', '.join(topics)}")

    def consume(self, timeout: float = 1.0, max_messages: int = 100) -> list:
        """
        Consume messages from subscribed topics.

        Args:
            timeout: Poll timeout in seconds
            max_messages: Maximum messages to consume per call

        Returns:
            List of dicts with keys: topic, key, value, timestamp, partition, offset
        """
        messages = []
        for _ in range(max_messages):
            msg = self._consumer.poll(timeout)
            if msg is None:
                break
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[kafka_bus] Consumer error: {msg.error()}")
                break

            try:
                value = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError):
                value = msg.value().decode("utf-8", errors="replace")

            messages.append({
                "topic": msg.topic(),
                "key": msg.key().decode("utf-8") if msg.key() else None,
                "value": value,
                "timestamp": msg.timestamp(),
                "partition": msg.partition(),
                "offset": msg.offset(),
            })

        return messages

    def consume_loop(self, callback: Callable, timeout: float = 1.0):
        """
        Continuously consume messages and call callback for each.

        Args:
            callback: Function called with each message dict
            timeout: Poll timeout in seconds
        """
        print("[kafka_bus] Entering consume loop — Ctrl+C to stop")
        try:
            while True:
                messages = self.consume(timeout=timeout)
                for msg in messages:
                    callback(msg)
        except KeyboardInterrupt:
            print("\n[kafka_bus] Consumer loop interrupted")
        finally:
            self.close()

    def close(self):
        """Close the consumer and commit offsets."""
        self._consumer.close()
        print("[kafka_bus] Consumer closed")


# ---------------------------------------------------------------------------
# Convenience functions
# ---------------------------------------------------------------------------

def publish_metrics(message: dict) -> bool:
    """Publish a metric message to omniwatch.metrics.raw topic."""
    producer = KafkaProducer()
    entity_id = message.get("entity_id", "unknown")
    return producer.send("omniwatch.metrics.raw", message, key=entity_id)


def publish_log(message: dict) -> bool:
    """Publish a log message to omniwatch.logs.raw topic."""
    producer = KafkaProducer()
    entity_id = message.get("entity_id", "unknown")
    return producer.send("omniwatch.logs.raw", message, key=entity_id)


def publish_trace(message: dict) -> bool:
    """Publish a trace message to omniwatch.traces.raw topic."""
    producer = KafkaProducer()
    return producer.send("omniwatch.traces.raw", message)


def publish_security_event(message: dict) -> bool:
    """Publish a security event to omniwatch.security.events topic."""
    producer = KafkaProducer()
    entity_id = message.get("entity_id", "unknown")
    return producer.send("omniwatch.security.events", message, key=entity_id)


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------

def main():
    """CLI interface for Kafka bus operations."""
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch Kafka Bus")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # create-topics command
    subparsers.add_parser("create-topics", help="Create all required Kafka topics")

    # list-topics command
    subparsers.add_parser("list-topics", help="List all Kafka topics")

    # produce-test command
    produce_parser = subparsers.add_parser("produce-test", help="Send a test message")
    produce_parser.add_argument("--topic", required=True, help="Topic to send to")
    produce_parser.add_argument("--message", required=True, help="JSON message string")

    # consume-test command
    consume_parser = subparsers.add_parser("consume-test", help="Consume test messages")
    consume_parser.add_argument("--topic", required=True, help="Topic to consume from")
    consume_parser.add_argument("--max-messages", type=int, default=5, help="Max messages")

    args = parser.parse_args()

    if args.command == "create-topics":
        success = create_topics()
        sys.exit(0 if success else 1)

    elif args.command == "list-topics":
        topics = list_topics()
        print(f"Kafka topics ({len(topics)}):")
        for t in topics:
            print(f"  {t}")

    elif args.command == "produce-test":
        try:
            message = json.loads(args.message)
        except json.JSONDecodeError:
            print(f"ERROR: Invalid JSON: {args.message}")
            sys.exit(1)

        producer = KafkaProducer()
        success = producer.send(args.topic, message)
        producer.flush()
        if success:
            print(f"Message sent to {args.topic}")
        else:
            print(f"Failed to send message to {args.topic}")
            sys.exit(1)

    elif args.command == "consume-test":
        consumer = KafkaConsumer(group_id="test-consumer")
        consumer.subscribe([args.topic])
        messages = consumer.consume(timeout=5.0, max_messages=args.max_messages)
        for msg in messages:
            print(json.dumps(msg, indent=2, default=str))
        consumer.close()

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
