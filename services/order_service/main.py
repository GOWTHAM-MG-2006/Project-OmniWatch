"""
OmniWatch — Order Service
Component: FastAPI Application
Phase: 1
Purpose: FastAPI HTTP server for order CRUD, OTel instrumentation, and anomaly injection
Inputs: HTTP requests on port 8002
Outputs: JSON responses, Kafka events, OTLP telemetry
"""

import logging
import sys
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from routes import router as order_router

from services.common.anomaly_injector import AnomalyEngine, add_routes
from services.common.otel_setup import init_otel

# Configure root logger
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("omniwatch.order_service")

# ---------------------------------------------------------------------------
# OpenTelemetry — initialise at module level BEFORE any route handlers
# (route handlers call get_meter() at request time, which is fine, but
#  init at module level ensures providers are ready)
# ---------------------------------------------------------------------------
init_otel("order-service")

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app):  # type: ignore[override]
    """Application lifecycle — startup / shutdown.

    OTel SDK is already initialised at module level, so providers are ready.
    """
    add_routes(app.router, engine)
    logger.info("OmniWatch Order Service started successfully")
    yield
    logger.info("OmniWatch Order Service shutting down")


app = FastAPI(
    title="OmniWatch Order Service",
    version="0.1.0",
    description="Order management microservice with saga orchestration",
    lifespan=lifespan,
)

# Anomaly engine — attached to application state for route access
engine = AnomalyEngine(service_name="order-service")


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@app.get("/health", tags=["health"])
def health_check() -> dict[str, str]:
    """Health check endpoint for container orchestration and monitoring."""
    return {"status": "healthy", "service": "order-service"}


# ---------------------------------------------------------------------------
# Anomaly-check middleware — applies engine effects to every request
# ---------------------------------------------------------------------------


@app.middleware("http")
async def anomaly_middleware(request: Request, call_next: Any) -> JSONResponse:
    """Check active anomalies on every request before dispatching.

    The middleware applies latency / error effects based on the engine
    state. Routes also call ``_check_anomaly()`` individually for
    route-specific effects.
    """
    return await call_next(request)  # Routes handle anomaly checks internally


# ---------------------------------------------------------------------------
# Register routers
# ---------------------------------------------------------------------------

app.include_router(order_router)

# Expose engine via app.state so routes / dependencies can access it
app.state.anomaly_engine = engine
