"""
OmniWatch — User Service
Component: FastAPI Application
Phase: 1
Purpose: User management microservice with OTel telemetry, anomaly injection, and health checks
Inputs: HTTP requests on port 8001
Outputs: JSON responses for user CRUD operations
"""

import logging
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from services.common.anomaly_injector import AnomalyEngine, add_routes
from services.common.otel_setup import init_otel

# ---------------------------------------------------------------------------
# Logging configuration — MUST be at module level so OTel init messages are visible
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# OpenTelemetry — initialise at module level BEFORE routes.py imports
# (routes.py creates module-level instruments via get_meter() at import time)
# ---------------------------------------------------------------------------
init_otel("user-service")

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app):  # type: ignore[override]
    """Application lifecycle — startup / shutdown.

    OTel SDK is already initialised at module level before routes.py import,
    so the module-level _meter in routes.py gets a real meter.
    """
    add_routes(app.router, engine)
    logger.info("user-service started — OTel initialized, anomaly routes registered")
    yield
    logger.info("user-service shutting down")


app = FastAPI(
    title="OmniWatch User Service",
    version="0.1.0",
    description="User management microservice with CRUD operations, "
    "OTel instrumentation, and anomaly injection for simulation testing.",
    lifespan=lifespan,
)

# Allow cross-origin requests from the dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Global state (attached to app.state for route access)
# ---------------------------------------------------------------------------

engine = AnomalyEngine(service_name="user-service")
app.state.engine = engine

# Logger (configured fully after OTel init)
logger = logging.getLogger("omniwatch.user_service")


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@app.get("/health", tags=["health"])
async def health() -> dict[str, str]:
    """Liveness and readiness probe."""
    return {"status": "healthy", "service": "user-service"}


# ---------------------------------------------------------------------------
# Include CRUD routers (registered after startup to avoid import-time issues)
# ---------------------------------------------------------------------------

from routes import router as user_router

app.include_router(user_router)
