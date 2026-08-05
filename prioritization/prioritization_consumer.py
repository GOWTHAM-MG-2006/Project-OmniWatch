"""
OmniWatch — Incident Prioritization
Component: Prioritization Consumer
Phase: 8
Purpose: Kafka consumer for RootCauseObject records on topic
         omniwatch.incidents.causal (Phase 7 output). Deserializes,
         validates, and dispatches to the PrioritizationEngine.
Inputs: Kafka messages on omniwatch.incidents.causal (JSON dicts)
Outputs: RootCauseObject dicts fed to PrioritizationEngine
"""

from __future__ import annotations

import json
import logging
import threading
from typing import Any, Optional

from ingestion.kafka_bus import KafkaConsumer, TOPIC_INCIDENTS_CAUSAL

from prioritization.config.settings import Settings
from prioritization.models import RootCauseObject
from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.prioritization_consumer")

# No DLQ (D7): deserialization errors logged and message skipped


class PrioritizationConsumer:
    """Kafka consumer for the ``omniwatch.incidents.causal`` topic.

    Uses the shared ``ingestion.kafka_bus.KafkaConsumer`` wrapper over
    ``confluent_kafka``.  Messages that fail deserialization or Pydantic
    validation are logged and skipped (no DLQ per D7).

    Args:
        settings: Optional Settings; defaults to ``Settings.from_env()``.
        bootstrap_servers: Optional override for Kafka bootstrap servers.
        group_id: Optional override for consumer group id.
        auto_offset_reset: Optional override for offset reset policy.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: Optional[str] = None,
    ) -> None:
        self._settings = settings or Settings.from_env()
        self._bootstrap_servers = bootstrap_servers or self._settings.kafka_bootstrap_servers
        self._group_id = group_id or self._settings.kafka_group_id
        self._auto_offset_reset = auto_offset_reset or self._settings.kafka_auto_offset_reset
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    @property
    def topic(self) -> str:
        """Return the consumed topic name."""
        return TOPIC_INCIDENTS_CAUSAL

    def start(self) -> None:
        """Start the Kafka consumer and begin polling in a background thread."""
        self._consumer = KafkaConsumer(
            topics=[TOPIC_INCIDENTS_CAUSAL],
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset=self._auto_offset_reset,
        )
        self._consumer.start()
        self._running = True

        self._thread = threading.Thread(
            target=self._consume_loop,
            name="prioritization-consumer",
            daemon=True,
        )
        self._thread.start()
        _LOG.info(
            "prioritization consumer started: group=%s topic=%s",
            self._group_id,
            TOPIC_INCIDENTS_CAUSAL,
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Stop polling and close the Kafka consumer."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        if self._consumer is not None:
            self._consumer.stop()
            self._consumer = None
        _LOG.info("prioritization consumer stopped")

    def consume_once(
        self,
        timeout: float = 5.0,
        max_messages: int = 100,
    ) -> list[RootCauseObject]:
        """Poll for messages once (synchronous, non-blocking loop).

        Useful for testing and for manual batch processing without
        starting a background thread.
        """
        if self._consumer is None:
            raise StorageError("consumer not started. Call start() first.")

        messages = self._consumer.messages(timeout=timeout, max_messages=max_messages)
        results: list[RootCauseObject] = []

        for msg in messages:
            value = msg.get("value")
            if value is None:
                _LOG.warning("skipping message with unparseable value: %s", msg)
                continue

            try:
                rc = RootCauseObject(**value)
                results.append(rc)
            except Exception as exc:  # noqa: BLE001 - log and skip
                _LOG.warning(
                    "skipping invalid RootCauseObject message: %s — %s",
                    msg.get("key", ""),
                    exc,
                )

        return results

    def _consume_loop(self) -> None:
        """Background consumption loop (for the live service mode)."""
        # This is a placeholder; the actual dispatch to the engine
        # happens via the prioritization_engine's main loop.
        # The engine calls consume_once() in its own loop for testability.
        pass
