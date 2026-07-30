"""
OmniWatch — API Gateway
Component: Router (Service routing placeholders)
Phase: 1
Purpose: Defines top-level routes for the API gateway — actual microservice proxy
         routing (/users/* → user-service, /orders/* → order-service) comes in
         later phases.
Inputs: HTTP requests
Outputs: Route metadata (GET /routes)
"""

from __future__ import annotations

import logging

from fastapi import APIRouter

logger = logging.getLogger("omniwatch.api_gateway.routes")

router = APIRouter(prefix="")


@router.get(
    "/routes",
    tags=["gateway"],
    summary="List registered gateway routes",
)
def list_routes() -> dict:
    """Return a summary of currently registered routes on the gateway.

    In Phase 1 this is a placeholder — it will list dynamic proxy targets
    once ``/users/*`` and ``/orders/*`` routing is implemented.
    """
    logger.debug("GET /routes called")
    return {
        "service": "api-gateway",
        "phase": 1,
        "routes": [
            {"path": "/health", "methods": ["GET"], "description": "Health check"},
            {"path": "/routes", "methods": ["GET"], "description": "Route listing"},
            {
                "path": "/__inject/anomaly",
                "methods": ["GET", "POST", "DELETE"],
                "description": "Anomaly injection (simulation only)",
            },
        ],
        "note": "User/Order proxy routes will be added in Phase 2+",
    }
