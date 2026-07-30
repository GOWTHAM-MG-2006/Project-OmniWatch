"""
OmniWatch — Order Service
Component: Kafka Producer Client
Phase: 1
Purpose: Thin wrapper around confluent-kafka for publishing order events
Inputs: Topic name, event payload (dict)
Outputs: Kafka produce call with graceful failure handling
"""

import json
import logging
from typing import Any, Optional

logger = logging.getLogger("omniwatch.order_service.kafka")


class KafkaProducer:
    """Simple Kafka producer that publishes JSON-serialised events.

    Uses the ``confluent-kafka`` library under the hood.  Connection
    failures are logged as warnings — the service continues to operate
    locally even when Kafka is unavailable.
    """

    def __init__(self, bootstrap_servers: str = "kafka:9092") -> None:
        self.bootstrap_servers = bootstrap_servers
        self._producer: Any = None
        self._connected = False

    def _ensure_connected(self) -> bool:
        """Lazy-init the producer. Returns True if connected/ready."""
        if self._connected and self._producer is not None:
            return True

        try:
            from confluent_kafka import Producer

            self._producer = Producer(
                {"bootstrap.servers": self.bootstrap_servers, "client.id": "order-service"}
            )
            # Force a metadata call to verify connectivity
            self._producer.list_topics(timeout=3.0)
            self._connected = True
            logger.info(
                "KafkaProducer connected to %s",
                self.bootstrap_servers,
            )
        except Exception as exc:
            logger.warning(
                "KafkaProducer connection failed (%s) — "
                "continuing without Kafka: %s",
                self.bootstrap_servers,
                exc,
            )
            self._connected = False
            self._producer = None
        return self._connected

    def publish(self, topic: str, key: str, value: dict[str, Any]) -> None:
        """Publish a JSON event to the given Kafka topic.

        Args:
            topic: Target Kafka topic (e.g. ``"omniwatch.orders.events"``).
            key: Message key (usually the order ID).
            value: Dict payload to serialise as JSON.
        """
        if not self._ensure_connected():
            logger.debug(
                "Kafka not available — dropping event topic=%s key=%s",
                topic,
                key,
            )
            return

        try:
            self._producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=json.dumps(value, default=str).encode("utf-8"),
                callback=self._delivery_report,
            )
            self._producer.poll(0)  # Trigger delivery callbacks
        except Exception as exc:
            logger.warning(
                "Failed to publish event topic=%s key=%s: %s",
                topic,
                key,
                exc,
            )

    def flush(self, timeout: float = 5.0) -> None:
        """Flush any buffered messages."""
        if self._producer is not None:
            try:
                self._producer.flush(timeout)
            except Exception as exc:
                logger.warning("Kafka flush failed: %s", exc)

    @staticmethod
    def _delivery_report(err: Optional[Any], msg: Any) -> None:
        """Callback invoked by confluent-kafka after produce attempt."""
        if err is not None:
            logger.warning("Kafka delivery failed: %s", err)
        else:
            logger.debug(
                "Kafka delivered to %s [%s]",
                msg.topic(),
                msg.partition(),
            )


# Module-level singleton for convenience
_default_producer: Optional[KafkaProducer] = None


def get_default_producer() -> KafkaProducer:
    """Return the module-level KafkaProducer singleton."""
    global _default_producer
    if _default_producer is None:
        _default_producer = KafkaProducer()
    return _default_producer
