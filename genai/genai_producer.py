"""
OmniWatch — Generative AI Layer
Component: Kafka Producer
Phase: 10
Purpose: Publishes generated artifacts to omniwatch.generated.{summaries,runbooks,reports}.
Inputs: GroundedArtifact / Runbook / PostMortem / GeneratedReport
Outputs: Kafka messages to generated topics
"""

from __future__ import annotations

import json
import logging
import os

from confluent_kafka import Producer

from genai.models import GroundedArtifact
from genai.settings import Settings

logger = logging.getLogger(__name__)

_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

_TOPIC_MAP: dict[str, str] = {
    "summary": "omniwatch.generated.summaries",
    "runbook": "omniwatch.generated.runbooks",
    "report": "omniwatch.generated.reports",
    "postmortem": "omniwatch.generated.reports",
}


class GenAIProducer:
    """Kafka producer for generated artifacts."""

    def __init__(self) -> None:
        settings = Settings()
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap,
        })
        self._stats: dict[str, int] = {"produced": 0, "errors": 0}

    def produce(
        self,
        artifact: GroundedArtifact,
        key: str | None = None,
    ) -> None:
        """Publish a generated artifact to the appropriate topic.

        Args:
            artifact: The generated artifact to publish.
            key: Optional Kafka message key (defaults to incident_id).
        """
        topic = _TOPIC_MAP.get(artifact.artifact_type, "omniwatch.generated.reports")
        msg_key = (key or artifact.incident_id).encode("utf-8")
        value = artifact.model_dump_json().encode("utf-8")

        try:
            self._producer.produce(topic, key=msg_key, value=value)
            self._producer.flush(timeout=5.0)
            self._stats["produced"] += 1
            logger.info(json.dumps({
                "event": "artifact_produced",
                "topic": topic,
                "incident_id": artifact.incident_id,
                "artifact_type": artifact.artifact_type,
            }))
        except Exception as exc:
            self._stats["errors"] += 1
            logger.error(json.dumps({
                "event": "produce_error",
                "error": str(exc),
            }))

    def get_stats(self) -> dict[str, int]:
        """Return producer statistics."""
        return dict(self._stats)

    def close(self) -> None:
        """Flush and close the producer."""
        self._producer.flush(timeout=5.0)
