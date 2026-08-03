"""
OmniWatch — Causal Graph Engine
Component: Causal Producer
Phase: 7
Purpose: Publish resolved incident root-cause records to Kafka
Inputs: RootCauseObject dictionaries from the causal engine
Outputs: omniwatch.incidents.causal Kafka topic
"""
from __future__ import annotations

import json
import logging
from typing import Any

from causal.config.settings import Settings
from storage.common import StorageError, create_logger

TOPIC_INCIDENTS_CAUSAL = "omniwatch.incidents.causal"

_LOG = create_logger("omniwatch.causal.causal_producer")
_LOG.setLevel(logging.INFO)


class CausalProducer:
    """Publishes RootCauseObject incident records to the Kafka causal topic."""

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings.from_env()
        self._producer: Any = None
        try:
            # Lazy import: keeps the module importable on hosts without kafka-python-ng.
            from kafka import KafkaProducer as _KafkaProducer
        except ImportError as exc:  # pragma: no cover - guarded import path
            raise StorageError("kafka-python-ng is required to publish causal incidents") from exc
        self._producer = _KafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
            acks="all",
            retries=3,
            retry_backoff_ms=500,
            compression_type="snappy",
            linger_ms=10,
            batch_num_messages=500,
        )

    # ------------------------------------------------------------------ #
    def publish(self, incident: dict[str, Any]) -> Any:
        """Asynchronously publish an incident record; returns the send future.

        Non-dict / empty payloads are rejected with a warning and return None.
        """
        if not isinstance(incident, dict) or not incident:
            _LOG.warning("skipping invalid incident payload: %r", type(incident).__name__)
            return None
        return self._producer.send(TOPIC_INCIDENTS_CAUSAL, value=incident)

    def close(self) -> None:
        """Flush and release the producer; idempotent and best-effort."""
        if self._producer is None:
            return
        try:
            self._producer.flush(timeout=5.0)
            _LOG.info("causal producer flushed")
        except Exception:  # noqa: BLE001 - best-effort shutdown
            _LOG.exception("error flushing causal producer")
        finally:
            self._producer = None
