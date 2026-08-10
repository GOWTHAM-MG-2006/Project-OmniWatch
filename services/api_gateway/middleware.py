"""
OmniWatch — API Gateway
Component: Middleware (Auth, OTel, Rate Limiting, Request ID)
Phase: 1
Purpose: Auth token validation, rate limiting, OTel metrics middleware, and
         request ID propagation
Inputs: HTTP requests with Authorization header
Outputs: 401 on invalid auth, 429 on rate limit exceeded, OTel metrics
         exported, X-Request-ID in response
"""

from __future__ import annotations

import logging
import time
import uuid
from collections import defaultdict
from collections.abc import Callable
from contextvars import ContextVar

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
# Rate-limit middleware — fixed-window counter keyed by client IP
# ---------------------------------------------------------------------------

_RATE_LIMIT_MAX_REQUESTS = 100  # requests per window
_RATE_LIMIT_WINDOW_SECONDS = 60  # 1-minute fixed window


class RateLimitMiddleware(BaseHTTPMiddleware):
    """In-memory fixed-window rate limiter keyed by client IP.

    Allows ``max_requests`` requests per ``window_seconds`` per client IP.
    Returns HTTP 429 ``{"detail": "rate limit exceeded"}`` when exceeded.

    Public endpoints (``/health``, ``/__status``, ``/docs``, ``/openapi.json``,
    ``/__inject/*``) are exempt from rate limiting.
    """

    def __init__(
        self,
        app: ASGIApp,
        max_requests: int = _RATE_LIMIT_MAX_REQUESTS,
        window_seconds: int = _RATE_LIMIT_WINDOW_SECONDS,
    ) -> None:
        super().__init__(app)
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        # client_ip -> (window_start, request_count)
        self._windows: dict[str, tuple[float, int]] = defaultdict(lambda: (0.0, 0))
        self._lock = __import__("threading").Lock()

    def _get_client_ip(self, request: Request) -> str:
        """Extract client IP, preferring X-Forwarded-For for reverse proxies."""
        forwarded = request.headers.get("x-forwarded-for")
        if forwarded:
            return forwarded.split(",")[0].strip()
        if request.client:
            return request.client.host
        return "unknown"

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        path = request.url.path

        # -- Exempt public endpoints from rate limiting --
        if path in ("/health", "__status"):
            return await call_next(request)
        if path.startswith(("/docs", "/openapi.json", "/__inject/", "/__status")):
            return await call_next(request)

        client_ip = self._get_client_ip(request)
        now = time.time()

        with self._lock:
            window_start, count = self._windows[client_ip]
            # Reset window if expired
            if now - window_start >= self.window_seconds:
                self._windows[client_ip] = (now, 1)
            else:
                if count >= self.max_requests:
                    logger.warning(
                        "Rate limit exceeded: client_ip=%s path=%s count=%d",
                        client_ip,
                        path,
                        count,
                    )
                    return Response(
                        status_code=429,
                        content='{"detail": "rate limit exceeded"}',
                        media_type="application/json",
                        headers={
                            "Retry-After": str(
                                int(self.window_seconds - (now - window_start))
                            )
                        },
                    )
                self._windows[client_ip] = (window_start, count + 1)

        return await call_next(request)


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
    - ``/__status`` (simulation-only active anomaly status)
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
        if path in ("/health", "/__status"):
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

        token = auth_header[len(_BEARER_PREFIX) :]
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
