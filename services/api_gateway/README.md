# OmniWatch — API Gateway

Central API gateway for the OmniWatch microservices platform. Handles authentication, rate limiting, OpenTelemetry instrumentation, anomaly injection (simulation), and proxy routing to downstream services.

## Purpose

The API gateway is the single entry point for all client traffic in the OmniWatch system. It:

- Validates Bearer token authentication on all protected routes
- Enforces rate limiting (100 requests/minute per client IP)
- Records OpenTelemetry metrics (request count, latency histograms)
- Proxies `/users/*` requests to `user-service` (port 8001)
- Proxies `/orders/*` requests to `order-service` (port 8002)
- Exposes simulation endpoints for anomaly injection and status checking

## Inputs

- **Client HTTP requests** — REST API calls from dashboards, CLI tools, or other services
- **Authorization header** — `Authorization: Bearer omniwatch-token` (required on protected routes)

## Outputs

- **Proxied responses** — forwarded from `user-service` or `order-service`, preserving status code, headers, and body
- **`GET /health`** — health check for container probes
- **`GET /routes`** — list of registered gateway routes
- **`GET /__status`** — active anomaly injection status (simulation only, no auth required)
- **`POST /__inject/anomaly`** — inject an anomaly scenario (simulation only)

## Endpoints

| Endpoint | Methods | Auth Required | Description |
|----------|---------|---------------|-------------|
| `/health` | GET | No | Health check |
| `/routes` | GET | Yes | List registered routes |
| `/__status` | GET | No | Active anomaly status |
| `/users/{path}` | ALL | Yes | Proxy to user-service |
| `/orders/{path}` | ALL | Yes | Proxy to order-service |
| `/__inject/anomaly` | GET, POST, DELETE | No | Anomaly injection (simulation) |
| `/docs` | GET | No | Swagger UI |
| `/openapi.json` | GET | No | OpenAPI schema |

## How to Run

### Local Development

```bash
# From the services/api_gateway directory
cd services/api_gateway
python main.py

# Or via uvicorn directly
uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

### Docker (via docker-compose)

```bash
# From the project root
docker-compose up -d api-gateway
```

The gateway listens on **port 8000**.

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC endpoint for telemetry |
| `OTEL_SERVICE_NAME` | `api-gateway` | Service name in OTel traces |

## Authentication

All protected endpoints require a Bearer token in the `Authorization` header:

```
Authorization: Bearer omniwatch-token
```

Public endpoints (no auth required):
- `GET /health`
- `GET /__status`
- `GET /docs`, `GET /openapi.json`
- `/__inject/*` (simulation endpoints)

## Rate Limiting

- **Limit:** 100 requests per minute per client IP
- **Window:** Fixed 60-second window
- **Response:** HTTP 429 `{"detail": "rate limit exceeded"}` with `Retry-After` header
- **Exemptions:** `/health`, `/__status`, `/docs`, `/openapi.json`, `/__inject/*`

## Proxy Behavior

The gateway proxies requests to downstream services:

- `/users/*` → `http://user-service:8001/{path}`
- `/orders/*` → `http://order-service:8002/{path}`

The proxy preserves:
- HTTP method (GET, POST, PUT, PATCH, DELETE)
- Path suffix and query string
- JSON request body
- `Authorization`, `Content-Type`, `Accept`, `X-Request-ID` headers

On upstream errors:
- **Connection error:** HTTP 503 `{"detail": "upstream unavailable"}`
- **Timeout:** HTTP 504 `{"detail": "upstream timeout"}`
- **Upstream 4xx/5xx:** returns the upstream status code and body directly

## File Structure

```
services/api_gateway/
├── main.py            # FastAPI app, middleware registration, /health, /__status
├── routes.py          # Proxy routes (/users/*, /orders/*) + /routes metadata
├── middleware.py       # AuthMiddleware, RateLimitMiddleware, OTelMiddleware
├── requirements.txt   # Python dependencies
└── README.md          # This file
```
