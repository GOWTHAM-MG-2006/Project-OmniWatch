"""
OmniWatch — Feature Store (Windowing Layer)
Component: FastAPI Application Entry Point
Phase: 4
Purpose: Feature retrieval sidecar — serves windowed feature vectors from the
         ClickHouse ``feature_vectors`` table over REST on port 8005.
Inputs: HTTP requests (REST API)
Outputs: Feature vectors as JSON; GET /health -> {"status":"healthy",
         "service":"feature-store-api"}
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI

try:
    from . import routes
    from .clickhouse_client import JsonLogFormatter
except ImportError:  # pragma: no cover - python main.py direct-run mode
    import routes
    from clickhouse_client import JsonLogFormatter


def _configure_logging() -> None:
    """Structured JSON logs to stdout (AGENTS.md standard)."""
    if not logging.root.handlers:
        logging.basicConfig(
            level=logging.INFO,
            handlers=[logging.StreamHandler()],
        )
    for handler in logging.root.handlers:
        # Only shape handlers we own; skip ones pytest/uvicorn already configured.
        if (
            isinstance(handler, logging.StreamHandler)
            and getattr(handler, "formatter", None) is None
        ):
            handler.setFormatter(JsonLogFormatter())


_configure_logging()
logger = logging.getLogger("omniwatch.feature_store")

app = FastAPI(
    title="OmniWatch Feature Store API",
    version="0.1.0",
    description="Serves windowed feature vectors from the ClickHouse "
    "feature_vectors table to downstream consumers (Phase 6+).",
)

app.include_router(routes.router)


@app.get("/health", tags=["health"], summary="Health check")
def health_check() -> dict:
    """Return the health-check payload for container probes."""
    return {"status": "healthy", "service": "feature-store-api"}


if __name__ == "__main__":
    # Direct run:  python main.py          (from feature-store/api)
    # Package run: uvicorn feature-store.api.main:app --port 8005  (from repo root)
    uvicorn.run(app, host="0.0.0.0", port=8005, log_level="info")
