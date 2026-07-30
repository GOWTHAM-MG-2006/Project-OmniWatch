"""
OmniWatch — API Gateway
Component: Middleware (Auth, OTel, Request ID)
Phase: 1
Purpose: Auth token validation, OTel metrics middleware, and request ID propagation
Inputs: HTTP requests with Authorization header
Outputs: 401 on invalid auth, OTel metrics exported, X-Request-ID in response
"""

from __future__ import annotations

import logging
import time
import uuid
from contextvars import ContextVar
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.types import ASGIApp

from services.common.otel_setup import get_meter

logger = logging.getLogger("omniwatch.api_gateway.middleware")

# ---------------------------------------------------------------------------
# Request ID propagation via contextvars
# ---------------------------------------------------------------------------

REQUEST_ID_CTX: ContextVar[str] = ContextVar("request_id", default="")


def get_request_id() -> str:
    """Get the current request ID from context variable."""
    return REQUEST_ID_CTX.get()


def _generate_request_id() -> str:
    """Generate a short, unique request identifier."""
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# Auth middleware — Bearer token validation
# ---------------------------------------------------------------------------

_AUTH_HEADER = "Authorization"
_BEARER_PREFIX = "Bearer "
_EXPECTED_TOKEN = "omniwatch-token"  # Phase 1 static token; replace with OAuth2 later


class AuthMiddleware(BaseHTTPMiddleware):
    """Require a valid Bearer token on all requests except public endpoints.

    Public endpoints currently include:
    - ``/health``
    - ``/docs``, ``/openapi.json`` (Swagger UI)
    - ``/__inject/*`` (anomaly injection — simulation-only)

    Rejects with HTTP 401 if the header is missing or the token does not match
    ``expected_token``.
    """

    def __init__(self, app: ASGIApp, expected_token: str | None = None) -> None:
        super().__init__(app)
        self.expected_token: str = expected_token or _EXPECTED_TOKEN

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path

        # -- Skip auth for public / introspection endpoints --
        if path in ("/health",):
            return await call_next(request)
        if path.startswith(("/docs", "/openapi.json", "/__inject/")):
            return await call_next(request)

        # -- Validate Authorization header --
        auth_header = request.headers.get(_AUTH_HEADER, "")
        if not auth_header.startswith(_BEARER_PREFIX):
            logger.warning("Missing or malformed auth header on %s", path)
            return Response(
                status_code=401,
                content='{"detail":"Missing or invalid Authorization header"}',
                media_type="application/json",
            )

        token = auth_header[len(_BEARER_PREFIX):]
        if token != self.expected_token:
            logger.warning("Invalid token on %s", path)
            return Response(
                status_code=401,
                content='{"detail":"Invalid authentication token"}',
                media_type="application/json",
            )

        return await call_next(request)


# ---------------------------------------------------------------------------
# OTel metrics middleware — request count & latency histograms
# ---------------------------------------------------------------------------

class OTelMiddleware(BaseHTTPMiddleware):
    """Record HTTP request count and latency histograms via OpenTelemetry.

    Uses the global ``Meter`` from ``get_meter()`` — safe to call before or
    after ``init_otel()`` (returns a no-op meter if OTel is not initialised).

    Instruments:
    - ``http.server.request_count`` — counter, tagged by method + path
    - ``http.server.request_duration_ms`` — histogram, tagged by method + path

    Also sets the ``X-Request-ID`` response header on every request.
    """

    def __init__(self, app: ASGIApp) -> None:
        super().__init__(app)
        meter = get_meter("omniwatch.api_gateway")
        self._request_count = meter.create_counter(
            name="http.server.request_count",
            description="Total number of HTTP requests received",
            unit="1",
        )
        self._request_latency = meter.create_histogram(
            name="http.server.request_duration_ms",
            description="HTTP request latency in milliseconds",
            unit="ms",
        )

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        start = time.perf_counter()
        request_id = _generate_request_id()
        ctx_token = REQUEST_ID_CTX.set(request_id)

        try:
            response = await call_next(request)
            response.headers["X-Request-ID"] = request_id
            return response
        finally:
            REQUEST_ID_CTX.reset(ctx_token)
            elapsed_ms = (time.perf_counter() - start) * 1000
            attrs = {"http.method": request.method, "http.path": request.url.path}
            self._request_count.add(1, attrs)
            self._request_latency.record(elapsed_ms, attrs)
