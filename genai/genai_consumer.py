"""
OmniWatch — Generative AI Layer
Component: Kafka Consumer
Phase: 10
Purpose: Consumes omniwatch.incidents.created + omniwatch.remediation.actions
         and routes to the appropriate generator (summary, runbook, report).
Inputs: Kafka messages from incidents.created and remediation.actions topics
Outputs: Generated artifacts via the 4 generators, persisted to MinIO + published to Kafka
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message

from genai.grounded_llm_client import GroundedLLMClient
from genai.models import RootCauseObject
from genai.settings import Settings

logger = logging.getLogger(__name__)

_KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
_KAFKA_GROUP = "omniwatch-genai-group"
_CONSUME_TOPICS = [
    "omniwatch.incidents.created",
    "omniwatch.remediation.actions",
]

# Timeout for async generator calls (seconds)
_GENERATOR_TIMEOUT = 300.0


class GenAIConsumer:
    """Kafka consumer that reads incidents and remediation actions,
    then dispatches to the appropriate generator."""

    def __init__(self) -> None:
        settings = Settings()
        self._consumer = Consumer({
            "bootstrap.servers": settings.kafka_bootstrap,
            "group.id": settings.kafka_group,
            "auto.offset.reset": "earliest",
        })
        self._stats: dict[str, int] = {"consumed": 0, "generated": 0, "errors": 0}

        # Lazy-initialised — created on first use so async objects
        # (httpx.AsyncClient, asyncio.Semaphore) bind to the correct
        # event loop inside _run_async rather than at init time.
        self._client: GroundedLLMClient | None = None
        self._summary_gen = None
        self._executive_gen = None
        self._postmortem_gen = None
        self._runbook_gen = None
        self._store = None
        self._producer = None

    # ------------------------------------------------------------------
    # Lazy initialisation helpers
    # ------------------------------------------------------------------

    def _get_client(self) -> GroundedLLMClient:
        if self._client is None:
            self._client = GroundedLLMClient()
        return self._client

    def _get_summary_gen(self):
        if self._summary_gen is None:
            from genai.incident_summary import IncidentSummaryGenerator
            self._summary_gen = IncidentSummaryGenerator(self._get_client())
        return self._summary_gen

    def _get_executive_gen(self):
        if self._executive_gen is None:
            from genai.executive_report import ExecutiveReportGenerator
            self._executive_gen = ExecutiveReportGenerator(self._get_client())
        return self._executive_gen

    def _get_postmortem_gen(self):
        if self._postmortem_gen is None:
            from genai.post_incident_analyser import PostIncidentAnalyser
            self._postmortem_gen = PostIncidentAnalyser(self._get_client())
        return self._postmortem_gen

    def _get_runbook_gen(self):
        if self._runbook_gen is None:
            from genai.runbook_generator import RunbookGenerator
            self._runbook_gen = RunbookGenerator(self._get_client())
        return self._runbook_gen

    def _get_store(self):
        if self._store is None:
            from genai.minio_store import MinioStore
            self._store = MinioStore()
        return self._store

    def _get_producer(self):
        if self._producer is None:
            from genai.genai_producer import GenAIProducer
            self._producer = GenAIProducer()
        return self._producer

    # ------------------------------------------------------------------
    # Consumer loop
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Subscribe to topics and consume messages."""
        self._consumer.subscribe(_CONSUME_TOPICS)
        logger.info(
            json.dumps({
                "event": "genai_consumer_started",
                "topics": _CONSUME_TOPICS,
            })
        )
        try:
            while True:
                msg = self._consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    err = msg.error()
                    if err is not None and err.code() != KafkaError._PARTITION_EOF:
                        logger.error(json.dumps({
                            "event": "kafka_error",
                            "error": str(msg.error()),
                        }))
                    continue
                self._handle_message(msg)
        except KeyboardInterrupt:
            logger.info(json.dumps({"event": "genai_consumer_shutdown"}))
        finally:
            self._consumer.close()

    def _handle_message(self, msg: Message) -> None:
        """Route a Kafka message to the appropriate generator."""
        self._stats["consumed"] += 1
        topic = msg.topic()
        try:
            raw_value = msg.value()
            if raw_value is None:
                self._stats["errors"] += 1
                return
            value = json.loads(raw_value.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            self._stats["errors"] += 1
            logger.error(json.dumps({"event": "message_parse_error", "error": str(exc)}))
            return

        if topic == "omniwatch.incidents.created":
            self._handle_incident(value)
        elif topic == "omniwatch.remediation.actions":
            self._handle_remediation(value)

    # ------------------------------------------------------------------
    # Incident handler — generate summary + executive report + postmortem
    # ------------------------------------------------------------------

    def _handle_incident(self, value: dict[str, Any]) -> None:
        """Handle an incident.created message — generate summary + report + postmortem,
        persist each to MinIO, and publish to Kafka."""
        root_cause_data = value.get("root_cause", value)
        try:
            root_cause = RootCauseObject(**root_cause_data)
        except Exception as exc:  # noqa: BLE001 — parse fallback
            self._stats["errors"] += 1
            logger.error(json.dumps({"event": "root_cause_parse_error", "error": str(exc)}))
            return

        logger.info(json.dumps({
            "event": "incident_received",
            "incident_id": root_cause.incident_id,
            "action": "generate_summary_and_report",
        }))

        # Build a coroutine that creates generators INSIDE the event loop
        # so httpx.AsyncClient + asyncio.Semaphore bind to the correct loop.
        async def _generate_all():
            from genai.executive_report import ExecutiveReportGenerator
            from genai.grounded_llm_client import GroundedLLMClient
            from genai.incident_summary import IncidentSummaryGenerator
            from genai.post_incident_analyser import PostIncidentAnalyser

            client = GroundedLLMClient()
            summary_gen = IncidentSummaryGenerator(client)
            exec_gen = ExecutiveReportGenerator(client)
            post_gen = PostIncidentAnalyser(client)
            try:
                return await asyncio.gather(
                    summary_gen.generate(root_cause),
                    exec_gen.generate(root_cause),
                    post_gen.generate(root_cause),
                )
            finally:
                await client.close()

        # Generate all three artifacts concurrently
        try:
            summary_analysis, executive_artifact, postmortem_artifact = (
                self._run_async(_generate_all())
            )
        except Exception as exc:  # noqa: BLE001 — generation fallback
            self._stats["errors"] += 1
            logger.error(json.dumps({
                "event": "generation_error",
                "incident_id": root_cause.incident_id,
                "error": str(exc),
            }))
            return

        # Wrap summary GroundedAnalysis into a proper GroundedArtifact
        from genai.models import GeneratedReport
        summary_artifact = GeneratedReport(
            incident_id=root_cause.incident_id,
            artifact_type="summary",
            content=summary_analysis.summary,
            report_type="incident_summary",
            audience="ops_engineers",
            sections=[
                {"title": "Root Cause", "content": summary_analysis.root_cause_entity},
                {"title": "Reasoning", "content": summary_analysis.reasoning},
                {"title": "Recommended Actions", "content": "\n".join(summary_analysis.recommended_actions)},
            ],
        )

        # Persist + publish each artifact
        store = self._get_store()
        producer = self._get_producer()
        for artifact in (summary_artifact, executive_artifact, postmortem_artifact):
            try:
                store.persist(artifact)
            except Exception as exc:  # noqa: BLE001 — persist fallback
                logger.error(json.dumps({
                    "event": "persist_error",
                    "incident_id": root_cause.incident_id,
                    "artifact_type": artifact.artifact_type,
                    "error": str(exc),
                }))
            try:
                producer.produce(artifact)
            except Exception as exc:  # noqa: BLE001 — produce fallback
                logger.error(json.dumps({
                    "event": "produce_error",
                    "incident_id": root_cause.incident_id,
                    "artifact_type": artifact.artifact_type,
                    "error": str(exc),
                }))

        self._stats["generated"] += 1
        logger.info(json.dumps({
            "event": "incident_artifacts_generated",
            "incident_id": root_cause.incident_id,
            "artifacts": ["summary", "executive_report", "postmortem"],
        }))

    # ------------------------------------------------------------------
    # Remediation handler — generate runbook
    # ------------------------------------------------------------------

    def _handle_remediation(self, value: dict[str, Any]) -> None:
        """Handle a remediation.actions message — generate runbook,
        persist to MinIO, and publish to Kafka."""
        incident_id = value.get("incident_id", "unknown")
        root_cause_data = value.get("root_cause")

        # Build a minimal RootCauseObject if not embedded
        if root_cause_data is None:
            root_cause_data = {
                "incident_id": incident_id,
                "root_cause_entity": value.get("entity_id", "unknown"),
                "entity_type": value.get("entity_type", "UNKNOWN"),
                "confidence": value.get("confidence", 0.0),
                "anomaly_score": value.get("anomaly_score", 0.0),
                "fault_path": [],
                "impacted_services": [],
                "impacted_count": 0,
                "evidence": {},
                "timestamp": value.get("executed_at", ""),
            }

        try:
            root_cause = RootCauseObject(**root_cause_data)
        except Exception as exc:  # noqa: BLE001 — parse fallback
            self._stats["errors"] += 1
            logger.error(json.dumps({
                "event": "remediation_root_cause_parse_error",
                "error": str(exc),
            }))
            return

        logger.info(json.dumps({
            "event": "remediation_received",
            "incident_id": incident_id,
            "action": "generate_runbook",
        }))

        self._stats["generated"] += 1

        # Extract action_result fields for the runbook generator
        action_result = {
            "action_type": value.get("action_type"),
            "success": value.get("success", False),
            "output": value.get("output", ""),
        }

        # Build a coroutine that creates the generator INSIDE the event loop
        # so httpx.AsyncClient + asyncio.Semaphore bind to the correct loop.
        async def _generate_runbook():
            from genai.grounded_llm_client import GroundedLLMClient
            from genai.runbook_generator import RunbookGenerator

            client = GroundedLLMClient()
            runbook_gen = RunbookGenerator(client)
            try:
                return await runbook_gen.generate(root_cause, action_result=action_result)
            finally:
                await client.close()

        try:
            runbook = self._run_async(_generate_runbook())
        except Exception as exc:  # noqa: BLE001 — generation fallback
            self._stats["errors"] += 1
            logger.error(json.dumps({
                "event": "runbook_generation_error",
                "incident_id": incident_id,
                "error": str(exc),
            }))
            return

        # Persist to MinIO
        store = self._get_store()
        try:
            store.persist(runbook)
        except Exception as exc:  # noqa: BLE001 — persist fallback
            logger.error(json.dumps({
                "event": "persist_error",
                "incident_id": incident_id,
                "artifact_type": "runbook",
                "error": str(exc),
            }))

        # Publish to Kafka
        producer = self._get_producer()
        try:
            producer.produce(runbook)
        except Exception as exc:  # noqa: BLE001 — produce fallback
            logger.error(json.dumps({
                "event": "produce_error",
                "incident_id": incident_id,
                "artifact_type": "runbook",
                "error": str(exc),
            }))

        logger.info(json.dumps({
            "event": "runbook_generated",
            "incident_id": incident_id,
            "steps": len(runbook.steps),
            "severity": runbook.severity,
        }))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _run_async(coro):
        """Run an async coroutine from a sync context with a timeout."""
        import concurrent.futures

        def _loop():
            return asyncio.run(asyncio.wait_for(coro, timeout=_GENERATOR_TIMEOUT))

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(_loop)
            return future.result(timeout=_GENERATOR_TIMEOUT + 10)

    def get_stats(self) -> dict[str, int]:
        """Return consumer statistics."""
        return dict(self._stats)

    async def close(self) -> None:
        """Close consumer and LLM client."""
        self._consumer.close()
        if self._client is not None:
            await self._client.close()


# ---------------------------------------------------------------------------
# Standalone entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    logger.info(json.dumps({"event": "genai_consumer_main_starting"}))
    consumer = GenAIConsumer()
    consumer.start()
