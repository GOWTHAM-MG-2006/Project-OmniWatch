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
from contextlib import asynccontextmanager
from typing import Any, AsyncGenerator

from fastapi import FastAPI

from orchestration import approval_api
from orchestration.config.settings import Settings
from orchestration.orchestration_consumer import OrchestrationConsumer
from orchestration.orchestrator import Orchestrator
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.orchestration.engine")

# ---------------------------------------------------------------------------
# Module-level state — set by _build_orchestrator / configure()
# ---------------------------------------------------------------------------

_settings: Settings = Settings(_env_file=None)
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
    settings = settings or Settings(_env_file=None)
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

    # Configure approval API dependencies
    approval_api.configure(
        select_pending=approval_select_pending,
        update_decision=approval_update_decision,
        learning_producer=approval_learning_producer,
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

    settings = Settings(_env_file=None)
    uvicorn.run(
        "orchestration.orchestration_engine:app",
        host="0.0.0.0",
        port=settings.orchestration_api_port,
        reload=False,
    )


if __name__ == "__main__":
    main()
