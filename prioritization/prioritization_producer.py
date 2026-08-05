"""
OmniWatch — Incident Prioritization
Component: Prioritization Producer
Phase: 8
Purpose: Kafka producer for prioritized IncidentRecord objects on topic
         omniwatch.incidents.created. Consumed by orchestration layer.
Inputs: IncidentRecord (Pydantic model)
Outputs: Kafka messages on omniwatch.incidents.created (JSON)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from ingestion.kafka_bus import KafkaProducer, TOPIC_INCIDENTS_CREATED

from prioritization.config.settings import Settings
from prioritization.models import IncidentRecord
from storage.common import StorageError, create_logger, retry_with_backoff

_LOG: logging.Logger = create_logger("omniwatch.prioritization.prioritization_producer")


class PrioritizationProducer:
    """Kafka producer for the ``omniwatch.incidents.created`` topic.

    Wraps the shared ``ingestion.kafka_bus.KafkaProducer`` and handles
    serialization of IncidentRecord objects plus retry-on-failure.

    Args:
        settings: Optional Settings; defaults to ``Settings.from_env()``.
        bootstrap_servers: Optional override for Kafka bootstrap servers.
        client_id: Optional override for Kafka client id.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        bootstrap_servers: Optional[str] = None,
        client_id: Optional[str] = None,
    ) -> None:
        self._settings = settings or Settings.from_env()
        self._bootstrap_servers = bootstrap_servers or self._settings.kafka_bootstrap_servers
        self._client_id = client_id or self._settings.kafka_client_id
        self._producer: Optional[KafkaProducer] = None

    @property
    def topic(self) -> str:
        """Return the produced topic name."""
        return TOPIC_INCIDENTS_CREATED

    def start(self) -> None:
        """Initialize the Kafka producer."""
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers,
            client_id=f"{self._client_id}-producer",
        )
        self._producer.start()
        _LOG.info(
            "prioritization producer started: client=%s topic=%s",
            self._client_id,
            TOPIC_INCIDENTS_CREATED,
        )

    def stop(self, timeout: float = 5.0) -> None:
        """Flush and stop the Kafka producer."""
        if self._producer is not None:
            self._producer.flush(timeout=timeout)
            self._producer.stop(timeout=timeout)
            self._producer = None
        _LOG.info("prioritization producer stopped")

    def publish_incident(
        self,
        incident: IncidentRecord,
        key: Optional[str] = None,
    ) -> str:
        """Publish a prioritized IncidentRecord to Kafka.

        Args:
            incident: The IncidentRecord to publish.
            key: Optional message key (defaults to incident_id).

        Returns:
            The incident_id of the published record.

        Raises:
            StorageError: If the producer is not started.
        """
        if self._producer is None:
            raise StorageError("Producer not started. Call start() first.")

        # Serialize IncidentRecord to JSON dict
        payload = incident.model_dump()
        # Ensure datetime fields are strings
        payload = json.loads(json.dumps(payload, default=str))

        msg_key = key or incident.incident_id
        retry_with_backoff(
            self._producer.send,
            retries=3,
            base_delay=0.5,
            max_delay=4.0,
            logger=_LOG,
            topic=TOPIC_INCIDENTS_CREATED,
            value=payload,
            key=msg_key,
        )
        _LOG.debug(
            "published incident: id=%s topic=%s key=%s severity=%s",
            incident.incident_id,
            TOPIC_INCIDENTS_CREATED,
            msg_key,
            incident.severity,
        )
        return incident.incident_id

    def flush(self, timeout: float = 5.0) -> int:
        """Flush pending messages. Returns remaining count."""
        if self._producer is None:
            return 0
        return self._producer.flush(timeout=timeout)
