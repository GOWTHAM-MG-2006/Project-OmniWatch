"""
OmniWatch — API Gateway
Component: Application Entry Point
Phase: 1
Purpose: FastAPI application that serves as the central API gateway — auth,
         rate limiting, OTel telemetry, anomaly injection, and proxy routing
         to downstream microservices.
Inputs: HTTP requests (REST API)
Outputs: Proxied responses from user-service, order-service + /health, /routes,
         /__status (active anomalies)
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from middleware import AuthMiddleware, OTelMiddleware, RateLimitMiddleware
from routes import router

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
# OpenTelemetry — initialise at module level BEFORE any code that calls
# get_meter(), so instruments are registered with the real MeterProvider
# instead of the no-op meter.
# ---------------------------------------------------------------------------
init_otel("api-gateway")

# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app):  # type: ignore[override]
    """Application lifecycle — startup / shutdown.

    OTel SDK is already initialised at module level (before FastAPI app creation)
    so the MeterProvider / TracerProvider / LoggerProvider are ready when
    OTelMiddleware instruments are constructed.
    """
    logger.info("API Gateway started — title=%s version=%s", app.title, app.version)
    yield
    # Shutdown: gracefully shut down OTel providers
    from services.common.otel_setup import shutdown_otel

    logger.info("API Gateway shutting down")
    shutdown_otel()


app = FastAPI(
    title="OmniWatch API Gateway",
    version="0.1.0",
    description="Central API gateway for OmniWatch microservices — handles auth, "
    "telemetry, rate limiting, and request routing.",
    lifespan=lifespan,
)

# -- Anomaly injection engine (simulation layer) --
# Created at module level so the engine state persists across requests.
engine = AnomalyEngine(service_name="api-gateway")
add_routes(app.router, engine)

# -- Middleware stack (execution order: OTel → RateLimit → Auth) --
# Starlette wraps add_middleware calls in reverse, so the LAST added middleware
# executes FIRST. Registration order here:
#   1. OTelMiddleware   (added last → runs first: records metrics for all requests)
#   2. RateLimitMiddleware (added second → runs second: rate-limits all traffic)
#   3. AuthMiddleware    (added first → runs last: validates Bearer token on protected routes)
app.add_middleware(AuthMiddleware)
app.add_middleware(RateLimitMiddleware)
app.add_middleware(OTelMiddleware)

# -- Include routes (proxy + metadata) --
app.include_router(router)

# -- Logger (immediately available; OTel handler attached after init_otel) --
logger = logging.getLogger("omniwatch.api_gateway")


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


@app.get(
    "/__status",
    tags=["gateway"],
    summary="Active anomaly status",
    description="Returns the list of currently active anomaly injections on this service. "
    "Simulation-only endpoint — public (no auth required).",
)
def status_check() -> dict:
    """Return active anomalies from the AnomalyEngine.

    Lists all currently-injected anomaly scenarios with their remaining TTL.
    This endpoint is public (no auth required) for simulation and debugging.
    """
    active = engine.get_active()
    return {
        "active_anomalies": active,
        "service": "api-gateway",
    }


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
