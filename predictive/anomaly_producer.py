"""
OmniWatch — Predictive Intelligence Layer
Component: Anomaly Producer
Phase: 6
Purpose: Kafka + ClickHouse output for anomaly signals
Inputs: AnomalySignal dict
Outputs: Kafka omniwatch.anomalies.detected + ClickHouse anomalies row
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any

from storage.clickhouse.client import ClickHouseClient
from storage.config import StorageConfig

from .config.settings import Settings

logger = logging.getLogger("omniwatch.predictive.anomaly_producer")

# Canonical Kafka topic (mirrors ingestion/kafka_bus.py TOPIC_ANOMALIES_DETECTED)
TOPIC_ANOMALIES_DETECTED = "omniwatch.anomalies.detected"

# Circuit-breaker defaults
_CB_FAILURE_THRESHOLD = 5
_CB_COOLDOWN_SECONDS = 30.0


class AnomalyProducer:
    """Publishes anomaly signals to Kafka and persists them to ClickHouse.

    Usage::

        producer = AnomalyProducer()
        producer.publish(anomaly_signal)
        # or just ClickHouse:
        producer.write_to_clickhouse(anomaly_signal)
        producer.close()
    """

    def __init__(
        self,
        settings: Settings | None = None,
        *,
        failure_threshold: int = _CB_FAILURE_THRESHOLD,
        cooldown_seconds: float = _CB_COOLDOWN_SECONDS,
    ) -> None:
        self._settings = settings or Settings.from_env()

        # Kafka producer (kafka-python-ng — lazy import avoids import-time
        # failures when kafka-python-ng is not installed or incompatible)
        from kafka import KafkaProducer as _KafkaProducer

        self._kafka: _KafkaProducer = _KafkaProducer(
            bootstrap_servers=self._settings.kafka_bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            acks="all",
            retries=3,
            retry_backoff_ms=500,
            linger_ms=10,
            # batch_num_messages is kafka-python-specific; kafka-python-ng
            # uses batch_size (bytes) and message_size defaults instead.
        )

        # ClickHouse client (lazy — created on first use)
        self._ch_client: ClickHouseClient | None = None

        # Circuit-breaker state
        self._cb_failure_threshold = failure_threshold
        self._cb_cooldown = cooldown_seconds
        self._cb_consecutive_failures = 0
        self._cb_open_until: float = 0.0

        logger.info(
            "AnomalyProducer initialised — bootstrap=%s",
            self._settings.kafka_bootstrap_servers,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def publish(self, anomaly_signal: dict[str, Any]) -> None:
        """Publish an anomaly signal to Kafka ``omniwatch.anomalies.detected``.

        Also writes to ClickHouse (with enrichment). If the ClickHouse
        circuit-breaker is open, the Kafka publish still succeeds — storage
        failures never block the streaming pipeline.
        """
        self._kafka.send(TOPIC_ANOMALIES_DETECTED, anomaly_signal)
        logger.debug(
            "published anomaly to Kafka — entity=%s score=%.3f",
            anomaly_signal.get("entity_id"),
            anomaly_signal.get("anomaly_score", 0.0),
        )
        # Best-effort ClickHouse persistence (never blocks Kafka publish)
        try:
            self.write_to_clickhouse(anomaly_signal)
        except Exception:
            logger.warning(
                "ClickHouse write failed after Kafka publish — signal buffered in Kafka",
                exc_info=True,
            )

    def write_to_clickhouse(self, anomaly_signal: dict[str, Any]) -> None:
        """Enrich the signal and insert a single row into ClickHouse ``anomalies``.

        Enrichment (Decision 10):
        - ``anomaly_id``: UUID4 string
        - ``status``: ``"active"``
        """
        self._check_circuit_breaker()

        enriched = dict(anomaly_signal)
        enriched["anomaly_id"] = str(uuid.uuid4())
        enriched.setdefault("status", "active")

        # Fill optional security columns so the row aligns to the 15-column
        # ANOMALIES_COLUMNS schema in storage/clickhouse/client.py. Non-nullable
        # String columns get "" (None would raise DataError); nullable ones get None.
        for col in ("attack_type", "severity", "evidence_logs"):
            enriched.setdefault(col, "")
        for col in ("recommended_action", "source_ip"):
            enriched.setdefault(col, None)

        client = self._get_ch_client()
        try:
            client.insert_anomalies([enriched])
            self._cb_consecutive_failures = 0
            logger.debug(
                "inserted anomaly into ClickHouse — id=%s entity=%s",
                enriched["anomaly_id"],
                enriched.get("entity_id"),
            )
        except Exception:
            self._cb_consecutive_failures += 1
            if self._cb_consecutive_failures >= self._cb_failure_threshold:
                self._cb_open_until = time.monotonic() + self._cb_cooldown
                logger.warning(
                    "ClickHouse circuit-breaker OPEN — %d consecutive failures, "
                    "pausing for %.0fs",
                    self._cb_consecutive_failures,
                    self._cb_cooldown,
                )
            raise

    def close(self) -> None:
        """Flush Kafka and release resources."""
        if self._kafka:
            remaining = self._kafka.flush(timeout=5.0)
            if remaining:
                logger.warning("Kafka flush: %d messages still in queue", remaining)
        if self._ch_client:
            self._ch_client.close()
            self._ch_client = None
        logger.info("AnomalyProducer closed")

    # ------------------------------------------------------------------ #
    # Internals
    # ------------------------------------------------------------------ #

    def _get_ch_client(self) -> ClickHouseClient:
        if self._ch_client is None:
            cfg = StorageConfig(
                clickhouse_host=self._settings.clickhouse_host,
                clickhouse_port=self._settings.clickhouse_port,
                clickhouse_db=self._settings.clickhouse_db,
                clickhouse_user=self._settings.clickhouse_user,
                clickhouse_password=self._settings.clickhouse_password,
            )
            self._ch_client = ClickHouseClient(config=cfg)
        return self._ch_client

    def _check_circuit_breaker(self) -> None:
        """If the circuit-breaker is open, block until the cooldown expires."""
        if self._cb_consecutive_failures < self._cb_failure_threshold:
            return
        now = time.monotonic()
        if now < self._cb_open_until:
            wait = self._cb_open_until - now
            logger.info("Circuit-breaker cooldown — sleeping %.1fs", wait)
            time.sleep(wait)
        # After cooldown, allow one retry attempt
        self._cb_consecutive_failures = 0
