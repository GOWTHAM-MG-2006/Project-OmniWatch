"""
OmniWatch — Orchestration + Policy
Component: Orchestration Consumer
Phase: 9
Purpose: Kafka consumer for prioritized IncidentRecord messages on topic
         omniwatch.incidents.created (Phase 8 output). Deserializes JSON
         payloads, validates against the IncidentRecord shape, and
         dispatches to a pluggable handle_message callback for downstream
         processing by the orchestration engine.
Inputs: Kafka messages on omniwatch.incidents.created (JSON dicts)
Outputs: IncidentRecord dicts dispatched to handle_message callback
"""

from __future__ import annotations

import logging
import signal
import threading
from typing import Any, Callable, Optional

from ingestion.kafka_bus import KafkaConsumer, TOPIC_INCIDENTS_CREATED
from orchestration.config.settings import Settings
from prioritization.models import IncidentRecord
from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.orchestration.orchestration_consumer")

# IncidentRecord fields that MUST be present in the incoming payload.
# Extra fields are passed through — only these are validated.
_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "incident_id",
    "created_at",
    "severity",
    "business_impact_score",
    "root_cause",
    "related_anomalies",
    "deduplicated_count",
    "sla_breach_risk",
    "assigned_to",
    "status",
})

# Default handler: logs the incident at INFO level.
def _default_handler(incident: dict[str, Any]) -> None:
    """Log an incoming incident at INFO level (default handle_message handler)."""
    _LOG.info(
        "incident received: id=%s severity=%s status=%s assigned_to=%s",
        incident.get("incident_id"),
        incident.get("severity"),
        incident.get("status"),
        incident.get("assigned_to"),
    )


_DEFAULT_HANDLER: Callable[[dict[str, Any]], None] = _default_handler


class OrchestrationConsumer:
    """Kafka consumer for the ``omniwatch.incidents.created`` topic.

    Uses the shared ``ingestion.kafka_bus.KafkaConsumer`` wrapper over
    ``confluent_kafka``.  Messages that fail deserialization or field
    validation are logged and skipped (no DLQ).

    The ``handle_message`` callback is invoked once per valid message.
    Replace it with orchestration-engine processing logic as needed.

    Args:
        settings: Optional Settings; defaults to ``Settings(_env_file=None)``.
        bootstrap_servers: Optional override for Kafka bootstrap servers.
        group_id: Optional override for consumer group id.
        auto_offset_reset: Optional override for offset reset policy.
        handle_message: Optional callback invoked with each validated
            incident dict. Defaults to logging the incident.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        bootstrap_servers: Optional[str] = None,
        group_id: Optional[str] = None,
        auto_offset_reset: Optional[str] = None,
        handle_message: Optional[Callable[[dict[str, Any]], None]] = None,
    ) -> None:
        self._settings = settings or Settings(_env_file=None)  # type: ignore[call-arg]
        self._bootstrap_servers = (
            bootstrap_servers or self._settings.kafka_bootstrap_servers
        )
        self._group_id = group_id or self._settings.kafka_group_id
        self._auto_offset_reset = (
            auto_offset_reset or self._settings.kafka_auto_offset_reset
        )
        self._handle_message = handle_message or _DEFAULT_HANDLER
        self._consumer: Optional[KafkaConsumer] = None
        self._running = False
        self._thread: Optional[threading.Thread] = None

    @property
    def topic(self) -> str:
        """Return the consumed topic name."""
        return TOPIC_INCIDENTS_CREATED

    def start(self) -> None:
        """Start the Kafka consumer and begin polling in a background thread."""
        self._consumer = KafkaConsumer(
            topics=[TOPIC_INCIDENTS_CREATED],
            group_id=self._group_id,
            bootstrap_servers=self._bootstrap_servers,
            auto_offset_reset=self._auto_offset_reset,
        )
        self._consumer.start()
        self._running = True

        self._thread = threading.Thread(
            target=self._consume_loop,
            name="orchestration-consumer",
            daemon=True,
        )
        self._thread.start()
        _LOG.info(
            "orchestration consumer started: group=%s topic=%s",
            self._group_id,
            TOPIC_INCIDENTS_CREATED,
        )

    def close(self, timeout: float = 5.0) -> None:
        """Stop polling and close the Kafka consumer gracefully."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        if self._consumer is not None:
            self._consumer.stop()
            self._consumer = None
        _LOG.info("orchestration consumer stopped")

    def consume_once(
        self,
        timeout: float = 5.0,
        max_messages: int = 100,
    ) -> list[dict[str, Any]]:
        """Poll for messages once (synchronous, non-blocking loop).

        Useful for testing and for manual batch processing without
        starting a background thread.

        Returns:
            List of validated incident dicts ready for processing.
        """
        if self._consumer is None:
            raise StorageError("consumer not started. Call start() first.")

        messages = self._consumer.messages(timeout=timeout, max_messages=max_messages)
        results: list[dict[str, Any]] = []

        for msg in messages:
            value = msg.get("value")
            if value is None:
                _LOG.warning("skipping message with unparseable value: %s", msg)
                continue

            incident = self._validate_incident(value, msg_key=msg.get("key"))
            if incident is not None:
                results.append(incident)

        return results

    def _validate_incident(
        self,
        value: dict[str, Any],
        *,
        msg_key: Any = None,
    ) -> Optional[dict[str, Any]]:
        """Validate an incoming JSON payload against the IncidentRecord shape.

        Returns the validated dict if all required fields are present,
        otherwise logs a warning and returns ``None``.
        """
        try:
            missing = _REQUIRED_FIELDS - value.keys()
            if missing:
                _LOG.warning(
                    "skipping incident missing required fields %s: key=%s",
                    sorted(missing),
                    msg_key,
                )
                return None

            # Validate via Pydantic model for full schema check
            IncidentRecord(**value)
            return value

        except Exception as exc:  # noqa: BLE001 - log and skip
            _LOG.warning(
                "skipping invalid IncidentRecord message: key=%s — %s",
                msg_key,
                exc,
            )
            return None

    def _consume_loop(self) -> None:
        """Background consumption loop that polls and dispatches messages."""
        _LOG.info("orchestration consume loop starting")
        while self._running:
            try:
                incidents = self.consume_once(timeout=1.0, max_messages=50)
                for incident in incidents:
                    try:
                        self._handle_message(incident)
                    except Exception as exc:  # noqa: BLE001 - never crash loop
                        _LOG.error(
                            "handle_message failed for incident %s: %s",
                            incident.get("incident_id"),
                            exc,
                        )
            except StorageError:
                # consume_once called before start() — should not happen
                break
            except Exception as exc:  # noqa: BLE001 - never crash loop
                _LOG.error("consume loop error: %s", exc)
        _LOG.info("orchestration consume loop ended")

    def install_signal_handlers(self) -> None:
        """Register SIGINT/SIGTERM handlers for graceful shutdown."""
        def _shutdown(signum: int, _frame: Any) -> None:
            sig_name = signal.Signals(signum).name
            _LOG.info("received %s — initiating graceful shutdown", sig_name)
            self.close()

        signal.signal(signal.SIGINT, _shutdown)
        signal.signal(signal.SIGTERM, _shutdown)


def main() -> None:
    """Entry point for running the orchestration consumer as a standalone process."""
    consumer = OrchestrationConsumer()
    consumer.install_signal_handlers()
    consumer.start()

    # Block until close() is called by signal handler
    while consumer._running:
        try:
            threading.Event().wait(timeout=1.0)
        except KeyboardInterrupt:
            consumer.close()
            break


if __name__ == "__main__":
    main()
