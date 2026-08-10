"""
OmniWatch — API Gateway
Component: Router (Proxy + Route Metadata)
Phase: 1
Purpose: Proxy routes /users/* and /orders/* to downstream microservices via
         httpx AsyncClient, plus route metadata endpoint.
Inputs: HTTP requests from clients
Outputs: Proxied responses from user-service (port 8001) and order-service
         (port 8002), route metadata (GET /routes)
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
from fastapi import APIRouter, Request, Response

logger = logging.getLogger("omniwatch.api_gateway.routes")

router = APIRouter(prefix="")

# ---------------------------------------------------------------------------
# Upstream service base URLs (Docker network DNS names)
# ---------------------------------------------------------------------------

_USER_SERVICE_URL = "http://user-service:8001"
_ORDER_SERVICE_URL = "http://order-service:8002"

# Timeout for upstream calls (connect, read)
_PROXY_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

# Headers to forward from the client to the upstream service
_FORWARD_HEADERS = {"Authorization", "Content-Type", "Accept", "X-Request-ID"}


# ---------------------------------------------------------------------------
# Shared proxy helper
# ---------------------------------------------------------------------------


async def _proxy_request(
    target_base: str,
    upstream_prefix: str,
    path_suffix: str,
    request: Request,
) -> Response:
    """Forward the incoming request to the target upstream service.

    Preserves HTTP method, path suffix, query string, JSON body, and selected
    headers (Authorization, Content-Type, Accept, X-Request-ID).

    The upstream URL is built as ``{target_base}{upstream_prefix}/{path_suffix}``.
    For example, a request to ``GET /users/abc`` with
    ``target_base="http://user-service:8001"`` and
    ``upstream_prefix="/api/v1/users"`` forwards to
    ``http://user-service:8001/api/v1/users/abc``.

    Returns the upstream status code and body on success.
    Returns 503 ``{"detail": "upstream unavailable"}`` on connection error.
    """
    upstream_url = f"{target_base}{upstream_prefix}/{path_suffix}"
    query_string = str(request.url.query) if request.url.query else ""

    # Build the full upstream URL with query string
    if query_string:
        upstream_url = f"{upstream_url}?{query_string}"

    # Read request body if present
    body: bytes | None = None
    if request.method in ("POST", "PUT", "PATCH"):
        body = await request.body()

    # Filter headers to forward
    forward_headers: dict[str, str] = {}
    for key, value in request.headers.items():
        if key.lower() in {h.lower() for h in _FORWARD_HEADERS}:
            forward_headers[key] = value

    logger.info(
        "Proxying %s %s → %s",
        request.method,
        request.url.path,
        upstream_url,
    )

    try:
        async with httpx.AsyncClient(
            timeout=_PROXY_TIMEOUT, follow_redirects=True
        ) as client:
            upstream_response = await client.request(
                method=request.method,
                url=upstream_url,
                headers=forward_headers,
                content=body,
            )

        # Filter upstream response headers to avoid hop-by-hop issues
        response_headers: dict[str, str] = {}
        skip = {"transfer-encoding", "connection", "content-encoding", "content-length"}
        for key, value in upstream_response.headers.items():
            if key.lower() not in skip:
                response_headers[key] = value

        return Response(
            status_code=upstream_response.status_code,
            content=upstream_response.content,
            headers=response_headers,
            media_type=upstream_response.headers.get(
                "content-type", "application/json"
            ),
        )

    except httpx.ConnectError:
        logger.error("Upstream unavailable: %s", upstream_url)
        return Response(
            status_code=503,
            content='{"detail": "upstream unavailable"}',
            media_type="application/json",
        )
    except httpx.TimeoutException:
        logger.error("Upstream timeout: %s", upstream_url)
        return Response(
            status_code=504,
            content='{"detail": "upstream timeout"}',
            media_type="application/json",
        )


# ---------------------------------------------------------------------------
# Proxy routes — /users/*
# ---------------------------------------------------------------------------


@router.api_route(
    "/users/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    tags=["proxy"],
    summary="Proxy to user-service",
    description="Forwards the request to http://user-service:8001/{path}.",
)
async def proxy_users(path: str, request: Request) -> Response:
    """Proxy all /users/* requests to the user-service."""
    return await _proxy_request(_USER_SERVICE_URL, "/api/v1/users", path, request)


# ---------------------------------------------------------------------------
# Proxy routes — /orders/*
# ---------------------------------------------------------------------------


@router.api_route(
    "/orders/{path:path}",
    methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
    tags=["proxy"],
    summary="Proxy to order-service",
    description="Forwards the request to http://order-service:8002/{path}.",
)
async def proxy_orders(path: str, request: Request) -> Response:
    """Proxy all /orders/* requests to the order-service."""
    return await _proxy_request(_ORDER_SERVICE_URL, "/api/v1/orders", path, request)


# ---------------------------------------------------------------------------
# Route metadata endpoint
# ---------------------------------------------------------------------------


@router.get(
    "/routes",
    tags=["gateway"],
    summary="List registered gateway routes",
)
def list_routes() -> dict[str, Any]:
    """Return a summary of currently registered routes on the gateway.

    Lists the health check, proxy targets, anomaly injection, and status
    endpoints available on this gateway.
    """
    logger.debug("GET /routes called")
    return {
        "service": "api-gateway",
        "phase": 1,
        "routes": [
            {"path": "/health", "methods": ["GET"], "description": "Health check"},
            {"path": "/routes", "methods": ["GET"], "description": "Route listing"},
            {
                "path": "/__status",
                "methods": ["GET"],
                "description": "Active anomalies status",
            },
            {
                "path": "/__inject/anomaly",
                "methods": ["GET", "POST", "DELETE"],
                "description": "Anomaly injection (simulation only)",
            },
            {
                "path": "/users/{path:path}",
                "methods": ["GET", "POST", "PUT", "PATCH", "DELETE"],
                "description": "Proxy to user-service (http://user-service:8001)",
            },
            {
                "path": "/orders/{path:path}",
                "methods": ["GET", "POST", "PUT", "PATCH", "DELETE"],
                "description": "Proxy to order-service (http://order-service:8002)",
            },
        ],
    }
