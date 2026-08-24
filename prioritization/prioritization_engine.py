"""
OmniWatch — Incident Prioritization
Component: Prioritization Engine (FastAPI orchestrator)
Phase: 8
Purpose: Orchestrates the full prioritization pipeline: consume RootCauseObject
         from omniwatch.incidents.causal → create IncidentRecord → deduplicate
         → publish to omniwatch.incidents.created. Exposes /health and /metrics
         endpoints for Kubernetes liveness/readiness probes.
Inputs: Kafka messages on omniwatch.incidents.causal (RootCauseObject JSON)
Outputs: Kafka messages on omniwatch.incidents.created (IncidentRecord JSON)
"""

from __future__ import annotations

import logging
import os
import threading
import time
from collections.abc import Callable
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pydantic import BaseModel

from prioritization.config.settings import Settings
from prioritization.deduplication_engine import DeduplicationEngine
from prioritization.incident_factory import IncidentFactory
from prioritization.models import IncidentRecord, RootCauseObject
from prioritization.prioritization_consumer import PrioritizationConsumer
from prioritization.prioritization_producer import PrioritizationProducer
from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.prioritization_engine")

# API port per phase8-build-plan.md
API_PORT = int(os.environ.get("PRIORITIZATION_API_PORT", "8009"))


class HealthResponse(BaseModel):
    """Health check response model."""

    status: str
    timestamp: str
    version: str = "1.0.0"


class StatsResponse(BaseModel):
    """Pipeline stats response model."""

    processed: int
    published: int
    deduplicated: int
    dedup_cache_stats: dict[str, Any] = {}


class PrioritizationEngine:
    """Core orchestrator for incident prioritization (Phase 8).

    Coordinates the consumer → factory → deduplicator → producer pipeline.
    Designed to be driven both by the FastAPI lifecycle (background thread)
    and by explicit calls to ``process_root_cause()`` (for testing).

    Args:
        settings: Optional Settings; defaults to ``Settings.from_env()``.
        factory: Optional pre-configured IncidentFactory.
        dedup_engine: Optional pre-configured DeduplicationEngine.
        consumer: Optional pre-configured PrioritizationConsumer.
        producer: Optional pre-configured PrioritizationProducer.
    """

    def __init__(
        self,
        settings: Settings | None = None,
        factory: IncidentFactory | None = None,
        dedup_engine: DeduplicationEngine | None = None,
        consumer: PrioritizationConsumer | None = None,
        producer: PrioritizationProducer | None = None,
        persist_fn: Callable[[IncidentRecord], None] | None = None,
    ) -> None:
        self._settings = settings or Settings.from_env()
        self._factory = factory or IncidentFactory(
            settings=self._settings,
            minio_client=getattr(self._settings, "minio_client", None),
            persist_fn=persist_fn,
        )
        self._dedup = dedup_engine or DeduplicationEngine(
            ttl_seconds=getattr(self._settings, "dedup_ttl_seconds", 300),
            enabled=getattr(self._settings, "dedup_enabled", True),
        )
        self._consumer = consumer or PrioritizationConsumer(
            settings=self._settings,
        )
        self._producer = producer or PrioritizationProducer(
            settings=self._settings,
        )

        self._processed = 0
        self._published = 0
        self._deduplicated = 0
        self._running = False
        self._thread: threading.Thread | None = None

    @property
    def processed(self) -> int:
        return self._processed

    @property
    def published(self) -> int:
        return self._published

    @property
    def deduplicated(self) -> int:
        return self._deduplicated

    def process_root_cause(
        self, root_cause: RootCauseObject | dict[str, Any]
    ) -> IncidentRecord:
        """Process a single RootCauseObject through the full pipeline.

        1. Build an IncidentRecord via the IncidentFactory
           (classify → score → sla → assign → archive to MinIO)
        2. Run deduplication (may merge into existing incident)
        3. Publish the final IncidentRecord to Kafka

        Args:
            root_cause: Phase 7 RootCauseObject (dict or model).

        Returns:
            The final IncidentRecord that was published.
        """
        self._processed += 1

        # Normalize input
        if isinstance(root_cause, dict):
            rc = RootCauseObject(**root_cause)
        elif isinstance(root_cause, RootCauseObject):
            rc = root_cause
        else:
            raise StorageError(
                f"process_root_cause expected RootCauseObject or dict, got {type(root_cause).__name__}"
            )

        _LOG.debug(
            "processing root_cause: entity=%s confidence=%s",
            rc.root_cause_entity,
            rc.confidence,
        )

        # Step 1: Build incident without side effects (defer persist until after dedup)
        incident = self._factory.build(rc)

        # Step 2: Deduplicate
        deduped = self._dedup.check_and_dedup(incident)
        is_dup = deduped.incident_id != incident.incident_id
        if is_dup:
            self._deduplicated += 1
            _LOG.info(
                "deduplicated incident: id=%s count=%d (incoming %s merged)",
                deduped.incident_id,
                deduped.deduplicated_count,
                incident.incident_id,
            )
            # Update ClickHouse deduplicated_count for the surviving row; do not insert orphan
            try:
                from storage.clickhouse.client import ClickHouseClient
                from storage.config import StorageConfig

                cfg = StorageConfig.from_env()
                client = ClickHouseClient(config=cfg)
                try:
                    client.get_client().command(
                        f"ALTER TABLE omniwatch.incidents UPDATE deduplicated_count = {deduped.deduplicated_count} WHERE incident_id = '{deduped.incident_id}'"
                    )
                    _LOG.info(
                        "Updated deduplicated_count in ClickHouse for incident %s to %d",
                        deduped.incident_id,
                        deduped.deduplicated_count,
                    )
                finally:
                    client.close()
            except Exception as exc:
                _LOG.warning(
                    "Failed to update deduplicated_count in ClickHouse for incident %s: %s",
                    deduped.incident_id,
                    exc,
                )
            incident = deduped
        else:
            # New incident — persist to ClickHouse/MinIO now (no orphan)
            try:
                self._factory.persist(deduped)
            except Exception as exc:  # noqa: BLE001 - persist logs internally
                _LOG.warning("Failed to persist new incident %s: %s", deduped.incident_id, exc)
            incident = deduped

        # Step 3: Publish to Kafka
        self._producer.publish_incident(incident)
        self._published += 1

        _LOG.info(
            "incident published: id=%s severity=%s impact=%.1f assigned_to=%s",
            incident.incident_id,
            incident.severity,
            incident.business_impact_score,
            incident.assigned_to,
        )

        return incident

    def start(self) -> None:
        """Start the engine: initialize consumer/producer, begin polling."""
        self._producer.start()
        self._consumer.start()
        self._running = True
        self._thread = threading.Thread(
            target=self._poll_loop,
            name="prioritization-poll",
            daemon=True,
        )
        self._thread.start()
        _LOG.info("prioritization engine started")

    def _poll_loop(self) -> None:
        """Background poll loop: consume and process until stopped."""
        while self._running:
            try:
                self.consume_and_process(timeout=5.0, max_messages=100)
            except Exception as exc:  # noqa: BLE001 - loop must survive transient errors
                _LOG.error("prioritization poll loop error: %s", exc)
            time.sleep(0.5)

    def stop(self, timeout: float = 5.0) -> None:
        """Stop the engine: close consumer/producer."""
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None
        self._consumer.stop(timeout=timeout)
        self._producer.stop(timeout=timeout)
        _LOG.info("prioritization engine stopped")

    def consume_and_process(self, timeout: float = 5.0, max_messages: int = 100) -> int:
        """Consume messages from Kafka and process them.

        Args:
            timeout: Poll timeout in seconds.
            max_messages: Maximum messages to process per batch.

        Returns:
            Number of root causes processed in this batch.
        """
        root_causes = self._consumer.consume_once(
            timeout=timeout, max_messages=max_messages
        )
        for rc in root_causes:
            try:
                self.process_root_cause(rc)
            except Exception as exc:  # noqa: BLE001 - log and continue
                _LOG.error("failed to process root cause: %s", exc)
        return len(root_causes)

    def get_stats(self) -> dict[str, Any]:
        """Return pipeline statistics (for monitoring / tests)."""
        return {
            "processed": self._processed,
            "published": self._published,
            "deduplicated": self._deduplicated,
            "dedup_cache_stats": self._dedup.get_stats(),
        }


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan: start/stop the prioritization engine."""
    engine: PrioritizationEngine = app.state.engine
    engine.start()
    _LOG.info("FastAPI lifespan: prioritization engine started")
    yield
    engine.stop()
    _LOG.info("FastAPI lifespan: prioritization engine stopped")


def create_app(engine: PrioritizationEngine | None = None) -> FastAPI:
    """Create and configure the FastAPI application.

    Args:
        engine: Optional pre-configured engine for testing/injection.
    """
    app = FastAPI(
        title="OmniWatch Prioritization Engine",
        description="Incident prioritization and deduplication service (Phase 8)",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.state.engine = engine or PrioritizationEngine()

    @app.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        """Health check endpoint for K8s liveness/readiness probes."""
        from datetime import datetime, timezone

        return HealthResponse(
            status="healthy",
            timestamp=datetime.now(timezone.utc).isoformat(),
        )

    @app.get("/stats", response_model=StatsResponse)
    async def stats() -> StatsResponse:
        """Return pipeline statistics."""
        s = app.state.engine.get_stats()
        return StatsResponse(
            processed=s["processed"],
            published=s["published"],
            deduplicated=s["deduplicated"],
            dedup_cache_stats=s["dedup_cache_stats"],
        )

    return app


# Module-level app for uvicorn: ``python -m prioritization.prioritization_engine``
app = create_app()


def main() -> None:
    """Entry point: run the FastAPI app with uvicorn."""
    import uvicorn

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=API_PORT,
        log_level="info",
    )


if __name__ == "__main__":
    main()
