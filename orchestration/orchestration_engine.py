"""
OmniWatch — Orchestration + Policy
Component: Orchestration Engine (FastAPI)
Phase: 9
Purpose: FastAPI application hosting the orchestration consumer and the approval
         API.  Manages the OrchestrationConsumer lifecycle via a lifespan context
         manager and exposes health/stats endpoints for monitoring.
Inputs: Kafka messages on omniwatch.incidents.created (via OrchestrationConsumer)
Outputs: ActionResult published to omniwatch.remediation.actions; ApprovalRecord
         stored in ClickHouse; approval decisions via the approval API
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator, Callable

from fastapi import FastAPI

from orchestration import approval_api
from orchestration.config.settings import Settings
from orchestration.orchestration_consumer import OrchestrationConsumer
from orchestration.orchestrator import Orchestrator
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.orchestration.engine")

# ---------------------------------------------------------------------------
# Approval API env flags — when truthy, real ClickHouse/Kafka callables are
# wired into the approval API at startup (see _resolve_approval_deps).
# ---------------------------------------------------------------------------
_ENV_SELECT_PENDING = "OMNIWATCH_APPROVAL_SELECT_PENDING"
_ENV_UPDATE_DECISION = "OMNIWATCH_APPROVAL_UPDATE_DECISION"
_ENV_LEARNING_PRODUCER = "OMNIWATCH_APPROVAL_LEARNING_PRODUCER"


def _env_flag(name: str) -> bool:
    """Return True when an env var is set to a truthy value."""
    return os.getenv(name, "").strip().lower() in {"1", "true", "yes", "on"}

# ---------------------------------------------------------------------------
# Module-level state — set by _build_orchestrator / configure()
# ---------------------------------------------------------------------------

_settings: Settings = Settings(_env_file=None)  # type: ignore[call-arg]
_consumer: OrchestrationConsumer | None = None
_consumer_started: bool = False


def _build_orchestrator(
    settings: Settings,
    *,
    opa: Any = None,
    executor: Any = None,
    producer: Any = None,
    clickhouse_fn: Any = None,
    archiver: Any = None,
) -> Orchestrator:
    """Build an Orchestrator with the given (or default) dependencies.

    When ``None``, defaults are created lazily at startup.  This function
    exists so tests can inject mocks before the lifespan runs.
    """
    return Orchestrator(
        opa=opa,
        executor=executor,
        producer=producer,
        clickhouse_fn=clickhouse_fn,
        archiver=archiver,
    )


def _create_consumer(
    settings: Settings,
    handle_message: Any,
) -> OrchestrationConsumer:
    """Create the OrchestrationConsumer bound to the given callback."""
    return OrchestrationConsumer(
        settings=settings,
        handle_message=handle_message,
    )


def _make_learning_producer(settings: Settings) -> Callable[[dict[str, Any]], None]:
    """Build a Kafka producer for denial records sent to the learning loop.

    Returns a callable ``(record: dict) -> None`` that publishes the denial
    to ``omniwatch.remediation.actions`` (consumed by the learning loop and
    dashboard per AGENTS.md).  Fail-soft: connection errors are logged and
    never raised, matching the approval API's fail-soft contract.
    """
    from ingestion.kafka_bus import KafkaProducer, TOPIC_REMEDIATION_ACTIONS

    producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id="omniwatch-orchestration-learning",
    )
    producer.start()

    def _publish(record: dict[str, Any]) -> None:
        producer.send(
            TOPIC_REMEDIATION_ACTIONS,
            record,
            key=str(record.get("approval_id", "unknown")),
        )
        producer.flush(timeout=5.0)

    return _publish


def _resolve_approval_deps(
    settings: Settings,
    *,
    approval_select_pending: Any,
    approval_update_decision: Any,
    approval_learning_producer: Any,
) -> tuple[Any, Any, Any]:
    """Resolve approval API dependencies from env flags when not injected.

    Explicitly injected callables (tests) always win.  When an argument is
    ``None`` and the corresponding ``OMNIWATCH_APPROVAL_*`` env flag is
    truthy, the real ClickHouse / Kafka callables are wired in.
    """
    select_pending = approval_select_pending
    update_decision = approval_update_decision
    learning_producer = approval_learning_producer

    if select_pending is None and _env_flag(_ENV_SELECT_PENDING):
        from storage.clickhouse.client import select_pending_approvals

        select_pending = select_pending_approvals
        _LOG.info("approval select_pending wired from env flag")

    if update_decision is None and _env_flag(_ENV_UPDATE_DECISION):
        from storage.clickhouse.client import update_approval_decision

        update_decision = update_approval_decision
        _LOG.info("approval update_decision wired from env flag")

    if learning_producer is None and _env_flag(_ENV_LEARNING_PRODUCER):
        learning_producer = _make_learning_producer(settings)
        _LOG.info("approval learning_producer wired from env flag")

    return select_pending, update_decision, learning_producer


# ---------------------------------------------------------------------------
# FastAPI lifespan — start consumer on startup, stop on shutdown
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager: start the orchestration consumer, yield, stop.

    The consumer is started in a background thread; the yield allows the
    FastAPI app to serve requests.  On shutdown the consumer is stopped
    gracefully.
    """
    global _consumer, _consumer_started  # noqa: PLW0603

    settings = app.state.settings
    orchestrator = app.state.orchestrator

    consumer = _create_consumer(settings, orchestrator.handle_message)
    try:
        consumer.start()
        _consumer = consumer
        _consumer_started = True
        _LOG.info(
            "orchestration engine started: topic=%s port=%d",
            consumer.topic,
            settings.orchestration_api_port,
        )
    except Exception as exc:  # noqa: BLE001
        _LOG.error("failed to start orchestration consumer: %s", exc)
        _consumer_started = False

    yield

    # Shutdown
    if _consumer is not None:
        try:
            _consumer.close()
        except Exception as exc:  # noqa: BLE001
            _LOG.error("error stopping orchestration consumer: %s", exc)
        _consumer = None
        _consumer_started = False
        _LOG.info("orchestration engine stopped")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

def create_app(
    *,
    settings: Settings | None = None,
    opa: Any = None,
    executor: Any = None,
    producer: Any = None,
    clickhouse_fn: Any = None,
    archiver: Any = None,
    approval_select_pending: Any = None,
    approval_update_decision: Any = None,
    approval_learning_producer: Any = None,
) -> FastAPI:
    """Build the orchestration FastAPI application.

    All dependencies are injectable for testing.  When ``None``, the
    production defaults from ``Settings()`` are used.
    """
    settings = settings or Settings(_env_file=None)  # type: ignore[call-arg]
    orchestrator = _build_orchestrator(
        settings,
        opa=opa,
        executor=executor,
        producer=producer,
        clickhouse_fn=clickhouse_fn,
        archiver=archiver,
    )

    app = FastAPI(
        title="OmniWatch Orchestration Engine",
        version="1.0.0",
        lifespan=lifespan,
    )

    # Store dependencies on app.state for lifespan access
    app.state.settings = settings
    app.state.orchestrator = orchestrator

    # Configure approval API dependencies — explicit injection wins, otherwise
    # the OMNIWATCH_APPROVAL_* env flags decide whether real callables are wired.
    select_pending, update_decision, learning_producer = _resolve_approval_deps(
        settings,
        approval_select_pending=approval_select_pending,
        approval_update_decision=approval_update_decision,
        approval_learning_producer=approval_learning_producer,
    )
    approval_api.configure(
        select_pending=select_pending,
        update_decision=update_decision,
        learning_producer=learning_producer,
    )

    # Mount approval API router
    app.include_router(approval_api.router)

    # Health + stats endpoints (registered per-app instance)
    @app.get("/health")
    def health() -> dict[str, Any]:
        return {"status": "ok", "component": "orchestration_engine"}

    @app.get("/stats")
    def stats() -> dict[str, Any]:
        return {
            "running": _consumer is not None and _consumer_started,
            "consumer_started": _consumer_started,
            "topic": "omniwatch.incidents.created",
            "api_port": settings.orchestration_api_port,
        }

    return app


# ---------------------------------------------------------------------------
# Default app instance (for uvicorn invocation)
# ---------------------------------------------------------------------------

app = create_app()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Run the orchestration engine via uvicorn (development mode)."""
    import uvicorn

    settings = Settings(_env_file=None)  # type: ignore[call-arg]
    uvicorn.run(
        "orchestration.orchestration_engine:app",
        host="0.0.0.0",
        port=settings.orchestration_api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
