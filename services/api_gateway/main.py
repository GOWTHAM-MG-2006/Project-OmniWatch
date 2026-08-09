"""
OmniWatch — API Gateway
Component: Application Entry Point
Phase: 1
Purpose: FastAPI application that serves as the central API gateway — auth,
         rate limiting, OTel telemetry, anomaly injection, and proxy routing
         to downstream microservices.
Inputs: HTTP requests (REST API)
Outputs: Proxied responses from user-service, order-service + /health, /routes
"""

from __future__ import annotations

import logging

import uvicorn
from fastapi import FastAPI

import sys

from services.common.anomaly_injector import AnomalyEngine, add_routes
from services.common.otel_setup import get_logger, init_otel

from middleware import AuthMiddleware, OTelMiddleware
from routes import router

# ---------------------------------------------------------------------------
# Logging configuration — MUST be at module level so OTel init messages are visible
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)

# ---------------------------------------------------------------------------
# OpenTelemetry — initialise at module level BEFORE any code that calls
# get_meter(), so instruments are registered with the real MeterProvider
# instead of the no-op meter.
# ---------------------------------------------------------------------------
init_otel("api-gateway")

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------

app = FastAPI(
    title="OmniWatch API Gateway",
    version="0.1.0",
    description="Central API gateway for OmniWatch microservices — handles auth, "
    "telemetry, rate limiting, and request routing.",
)

# -- Anomaly injection engine (simulation layer) --
# Created at module level so the engine state persists across requests.
engine = AnomalyEngine(service_name="api-gateway")
add_routes(app.router, engine)

# -- Middleware stack (order matters: Auth runs before OTel metrics) --
app.add_middleware(AuthMiddleware)
app.add_middleware(OTelMiddleware)

# -- Include placeholder routes --
app.include_router(router)

# -- Logger (immediately available; OTel handler attached after init_otel) --
logger = logging.getLogger("omniwatch.api_gateway")


# ---------------------------------------------------------------------------
# Lifecycle hooks
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    """Log startup confirmation.

    OTel SDK is already initialised at module level (before FastAPI app creation)
    so the MeterProvider / TracerProvider / LoggerProvider are ready when
    OTelMiddleware instruments are constructed.
    """
    logger.info("API Gateway started — title=%s version=%s", app.title, app.version)


@app.on_event("shutdown")
async def on_shutdown() -> None:
    """Cleanup hook — gracefully shut down OTel providers."""
    from services.common.otel_setup import shutdown_otel

    logger.info("API Gateway shutting down")
    shutdown_otel()


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get(
    "/health",
    tags=["gateway"],
    summary="Health check",
    description="Returns the current health status of the API gateway service.",
)
def health_check() -> dict:
    """Return a simple health-check payload for container probes."""
    return {"status": "healthy", "service": "api-gateway"}


# ---------------------------------------------------------------------------
# Phase 1 placeholders — real proxy routing comes in later phases
# ---------------------------------------------------------------------------

# TODO (Phase 2): Proxy /users/* routes to user-service
#   - GET  /users/           → user-service.list_users()
#   - GET  /users/{id}       → user-service.get_user()
#   - POST /users/           → user-service.create_user()
#
# TODO (Phase 2): Proxy /orders/* routes to order-service
#   - GET  /orders/          → order-service.list_orders()
#   - GET  /orders/{id}      → order-service.get_order()
#   - POST /orders/          → order-service.create_order()
#
# TODO (Phase 3): Rate limiting via sliding-window counter (in-memory)
#
# TODO (Phase 3): Request validation / schema enforcement


# ---------------------------------------------------------------------------
# Direct run (for local development)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
