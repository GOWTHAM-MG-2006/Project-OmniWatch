# OmniWatch — Order Service

## Purpose
Order management microservice for the OmniWatch Phase 1 demo stack. Exposes
REST endpoints for creating and listing orders, orchestrates order creation
through a saga (local persistence + Kafka `order.created` event), and is
instrumented with OpenTelemetry metrics and anomaly injection.

## Inputs
- HTTP requests to `POST /api/v1/orders`, `GET /api/v1/orders`,
  `GET /api/v1/orders/{order_id}`, `GET /api/v1/orders/users/{user_id}`
- `POST /orders` body: `OrderCreate` JSON (`user_id`, `items[]`)
- Upstream validation call: `GET http://user-service:8001/users/{user_id}`

## Outputs
- JSON responses with `Order` data (`id`, `user_id`, `items`, `total`,
  `status`, `created_at`)
- Kafka event `order.created` published by the saga
- OTLP telemetry (metrics) exported via OpenTelemetry

## How to Run
```bash
# From the repo root (services/ are imported as packages)
uvicorn services.order_service.main:app --host 0.0.0.0 --port 8002
```

## Environment Variables
| Variable | Default | Description |
|----------|---------|-------------|
| `USER_SERVICE_URL` | `http://user-service:8001` | Base URL of the user-service used for user_id validation |

## User Validation on Order Creation
`POST /api/v1/orders` validates the `user_id` against the user-service
(`GET {USER_SERVICE_URL}/users/{user_id}`) **before** creating the order:

- user-service returns 404 → `400 {"detail": "user not found"}` (order NOT created)
- user-service unreachable / timeout → `503 {"detail": "user service unavailable"}`
- user exists → order creation proceeds via the saga as normal