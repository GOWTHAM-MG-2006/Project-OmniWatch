"""
OmniWatch — Causal Graph Engine
Component: Causal Consumer
Phase: 7
Purpose: Consume anomaly signals from the omniwatch.anomalies.detected Kafka topic and
         forward validated signals to the causal engine for root cause analysis.
Inputs: omniwatch.anomalies.detected (AnomalySignal JSON payloads)
Outputs: Parsed anomaly signals handed to a handler callback (the causal engine)
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

from causal.config.settings import Settings
from storage.common import StorageError, create_logger

TOPIC_ANOMALIES_DETECTED = "omniwatch.anomalies.detected"

_LOG = create_logger("omniwatch.causal.causal_consumer")
_LOG.setLevel(logging.INFO)

# AnomalySignal contract (AGENTS.md) — required keys for root cause analysis
_REQUIRED_KEYS = ("entity_id", "entity_type", "metric_name", "timestamp")


class CausalConsumer:
    """Kafka consumer for the causal layer.

    The consumer reads AnomalySignal dicts from ``omniwatch.anomalies.detected``
    and forwards each valid signal to a handler. The handler is injected so the
    consume loop stays testable without a live broker (tests exercise
    ``process_message`` directly).
    """

    def __init__(
        self,
        handler: Callable[[dict[str, Any]], None],
        settings: Settings | None = None,
    ) -> None:
        if handler is None:
            raise StorageError("CausalConsumer requires a handler callback")
        self._handler = handler
        self._settings = settings or Settings.from_env()
        self._consumer = None

    # ------------------------------------------------------------------ #
    def process_message(self, anomaly_signal: dict[str, Any]) -> bool:
        """Validate and forward a single anomaly signal to the handler.

        Returns True when the signal was accepted and forwarded; False when the
        message was malformed (logged and skipped, never raised — Kafka polling
        must not die on bad data).
        """
        if not isinstance(anomaly_signal, dict):
            _LOG.warning("dropping non-dict anomaly signal: %r", type(anomaly_signal))
            return False
        missing = [k for k in _REQUIRED_KEYS if k not in anomaly_signal]
        if missing:
            _LOG.warning(
                "dropping anomaly signal missing keys %s: %r", missing, anomaly_signal
            )
            return False
        try:
            self._handler(anomaly_signal)
        except Exception:  # noqa: BLE001 - handler failure must not kill the loop
            _LOG.exception("handler failed for anomaly signal entity_id=%s",
                           anomaly_signal.get("entity_id"))
            return False
        return True

    # ------------------------------------------------------------------ #
    def run(self) -> None:
        """Blocking consume loop. Creates the Kafka consumer lazily so the
        module imports cleanly when kafka-python-ng is not installed (e.g. on
        the Python 3.14 host during development)."""
        try:
            from kafka import KafkaConsumer as _KafkaConsumer
        except ImportError as exc:  # pragma: no cover - import guard
            raise StorageError(
                "kafka-python-ng is not installed; cannot start causal consumer"
            ) from exc

        self._consumer = _KafkaConsumer(
            TOPIC_ANOMALIES_DETECTED,
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            group_id=self._settings.kafka_group_id,
            auto_offset_reset=self._settings.kafka_auto_offset_reset,
            value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
        )
        _LOG.info(
            "causal consumer started on %s (group=%s)",
            TOPIC_ANOMALIES_DETECTED,
            self._settings.kafka_group_id,
        )
        try:
            for message in self._consumer:
                value = message.value if isinstance(message.value, dict) else None
                if value is None:
                    _LOG.warning("dropping non-dict kafka value: %r", message.value)
                    continue
                self.process_message(value)
        except KeyboardInterrupt:
            _LOG.info("causal consumer interrupted, shutting down")
        finally:
            self.close()

    # ------------------------------------------------------------------ #
    def close(self) -> None:
        """Close the consumer, if open. Idempotent."""
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:  # noqa: BLE001 - best effort shutdown
                _LOG.exception("error closing causal consumer")
            finally:
                self._consumer = None