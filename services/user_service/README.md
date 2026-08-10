# OmniWatch — User Service

## Purpose
User management microservice for the OmniWatch demo stack. Provides CRUD
endpoints for user entities backed by an in-memory store (no database in
Phase 1). Instrumented with OpenTelemetry metrics and supports anomaly
injection for simulation testing.

## Inputs
HTTP requests to `/api/v1/users/*` on port `8001`:

| Method | Path              | Body                          | Description            |
|--------|-------------------|-------------------------------|------------------------|
| POST   | `/api/v1/users/`  | `{"name": str, "email": str}` | Create a user (201)    |
| GET    | `/api/v1/users/`  | —                             | List all users (200)   |
| GET    | `/api/v1/users/{user_id}` | —                     | Get user by ID (200)   |
| PUT    | `/api/v1/users/{user_id}` | `{"name"?: str, "email"?: str}` | Update user (200) |
| DELETE | `/api/v1/users/{user_id}` | —                     | Delete user (204)      |

Anomaly injection routes are registered at startup (see
`services/common/anomaly_injector.py`).

## Outputs
JSON responses using the `User` model:

```json
{
  "id": "uuid",
  "name": "string",
  "email": "string",
  "created_at": "ISO 8601 timestamp"
}
```

Errors: `404 {"detail": "user not found"}` for unknown IDs, `422` for invalid
request bodies (pydantic validation).

## How to Run
```bash
# From the repo root (services/common must be importable)
uvicorn services.user_service.main:app --host 0.0.0.0 --port 8001
```

Health check: `GET /health` → `{"status": "healthy", "service": "user-service"}`

## Environment Variables
| Variable | Default | Purpose |
|----------|---------|---------|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4317` | OTLP gRPC collector endpoint |
| `OTEL_SERVICE_NAME` | `user-service` | OTel service name |

All other configuration is code-level (no secrets required in Phase 1).