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

_KAFKA_BOOTSTRAP = os.getenv(
    "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS",
    os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))


def _topic_map_from_env() -> dict[str, str]:
    def _t(primary: str, fallback: str, default: str) -> str:
        return os.getenv(primary, os.getenv(fallback, default))

    return {
        "summary": _t("OMNIWATCH_KAFKA_TOPICS_SUMMARIES", "KAFKA_TOPIC_SUMMARIES",
                       "omniwatch.generated.summaries"),
        "runbook": _t("OMNIWATCH_KAFKA_TOPICS_RUNBOOKS", "KAFKA_TOPIC_RUNBOOKS",
                       "omniwatch.generated.runbooks"),
        "report": _t("OMNIWATCH_KAFKA_TOPICS_REPORTS", "KAFKA_TOPIC_REPORTS",
                      "omniwatch.generated.reports"),
        "postmortem": _t("OMNIWATCH_KAFKA_TOPICS_REPORTS", "KAFKA_TOPIC_REPORTS",
                          "omniwatch.generated.reports"),
    }


_TOPIC_MAP: dict[str, str] = _topic_map_from_env()


class GenAIProducer:
    """Kafka producer for generated artifacts."""

    def __init__(self, settings: Settings | None = None) -> None:
        settings = settings or Settings()
        self._settings = settings
        self._producer = Producer({
            "bootstrap.servers": settings.kafka_bootstrap,
        })
        self._stats: dict[str, int] = {"produced": 0, "errors": 0}

    def _topic_for(self, artifact_type: str) -> str:
        """Resolve the topic for an artifact type (settings registry wins)."""
        settings_map = {
            "summary": getattr(
                self._settings, "kafka_topic_summaries", None),
            "runbook": getattr(
                self._settings, "kafka_topic_runbooks", None),
            "report": getattr(
                self._settings, "kafka_topic_reports", None),
            "postmortem": getattr(
                self._settings, "kafka_topic_reports", None),
        }
        configured = settings_map.get(artifact_type)
        if configured:
            return str(configured)
        return _TOPIC_MAP.get(
            artifact_type,
            _topic_map_from_env().get("report",
                                      "omniwatch.generated.reports"))

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
        topic = self._topic_for(artifact.artifact_type)
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
        except Exception as exc:  # noqa: BLE001 — produce fallback
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
