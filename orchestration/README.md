# OmniWatch — Orchestration + Policy Engine (Phase 9)

## Purpose

Consumes prioritized `IncidentRecord` messages from the Kafka topic
`omniwatch.incidents.created` (Phase 8 output), evaluates each proposed
remediation action against OPA Rego policies, routes decisions into
auto-execute, human-approval, or deny paths, executes actions (with retry
and idempotency), and publishes `ActionResult` records to
`omniwatch.remediation.actions` for downstream consumers (dashboard,
learning loop, compliance reporter).

## Architecture

```
Kafka (omniwatch.incidents.created)
  ↓
OrchestrationConsumer  — deserializes IncidentRecord
  ↓
Orchestrator (7-step pipeline)
  ├── Step 1–2: Consume → Enrich (extract entity, severity, confidence)
  ├── Step 3:   OPA Decide (POST /v1/data/omniwatch with incident context)
  ├── Step 4:   Route → auto / approval / deny (fail-closed)
  ├── Step 5a:  Auto-execute with retry (SimulationExecutor or KubernetesExecutor)
  │     or
  │   5b:      Pend → ApprovalRecord in ClickHouse (human-in-the-loop)
  ├── Step 6:   Publish ActionResult to Kafka
  └── Step 7:   Audit archive to MinIO (omniwatch-audit-logs)

FastAPI App (orchestration_engine.py)
  ├── POST /api/v1/pending-approvals  → list undecided requests
  ├── POST /api/v1/approve/{id}       → approve action
  ├── POST /api/v1/deny/{id}          → deny action → publish to learning loop
  ├── GET  /health                    → liveness probe
  └── GET  /stats                     → consumer status + topic info
```

## Components

| File | Responsibility |
|---|---|
| `orchestration_engine.py` | FastAPI app factory (`create_app()`), lifespan (consumer start/stop), health/stats |
| `orchestrator.py` | 7-step pipeline: consume → enrich → OPA decide → route → execute/pend → publish → audit |
| `orchestration_consumer.py` | Kafka consumer for `omniwatch.incidents.created` (thread-based polling loop) |
| `orchestration_producer.py` | Kafka producer for `omniwatch.remediation.actions` (with retry + delivery callback) |
| `decision_client.py` | OPA HTTP client with retry + fail-closed strategy |
| `executor.py` | ActionExecutor ABC, SimulationExecutor (httpx), KubernetesExecutor (lazy import) |
| `action_library.py` | Action registry mapping entity types → ActionDefinition (safe vs human-gated) |
| `models.py` | Pydantic v2 models: OrchestrationDecision, ActionResult, ApprovalRecord |
| `approval_api.py` | FastAPI Router: GET/POST approval endpoints (dependency-injected) |
| `config/settings.py` | Pydantic Settings: Kafka, OPA, K8s, ClickHouse, MinIO, API port |
| `policies/policy.rego` | OPA Rego policy rules |

## Inputs

**Kafka topic:** `omniwatch.incidents.created`

Message format: `IncidentRecord` JSON (Phase 8 contract)
```json
{
  "incident_id": "uuid",
  "created_at": "2026-01-01T00:00:00+00:00",
  "severity": "P1",
  "business_impact_score": 88.5,
  "root_cause": { "root_cause_entity": "postgresql-database", "entity_type": "DATABASE_NODE", "confidence": 92.0 },
  "related_anomalies": [],
  "deduplicated_count": 1,
  "sla_breach_risk": "HIGH",
  "assigned_to": "auto-remediation",
  "status": "OPEN"
}
```

## Outputs

**Kafka topic:** `omniwatch.remediation.actions`

Message format: `ActionResult` JSON
```json
{
  "action_id": "uuid",
  "incident_id": "uuid",
  "action_type": "restart_service",
  "entity_id": "postgresql-database",
  "entity_type": "DATABASE_NODE",
  "success": true,
  "output": "simulated: restart_service on postgresql-database",
  "error": null,
  "execution_time_seconds": 0.001,
  "executed_at": "2026-01-01T00:00:00+00:00Z",
  "triggered_by": "auto",
  "severity": "P1",
  "confidence": 92.0,
  "archived": true
}
```

**ClickHouse table:** `pending_approvals` — ApprovalRecord for human-gated actions.

**MinIO bucket:** `omniwatch-audit-logs` — full ActionResult JSON archived per action_id.

## Running Locally

### Docker Compose

```bash
docker compose up -d orchestration-engine
```

### Standalone (development)

```bash
# From the repo root
uvicorn orchestration.orchestration_engine:app --host 0.0.0.0 --port 8010
# or
python -m orchestration.orchestration_engine
```

### Health Check

```bash
curl http://localhost:8010/health
```

Expected response:
```json
{"status": "ok", "component": "orchestration_engine"}
```

### Stats

```bash
curl http://localhost:8010/stats
```

Expected response:
```json
{
  "running": true,
  "consumer_started": true,
  "topic": "omniwatch.incidents.created",
  "api_port": 8010
}
```

### Approval API

```bash
# List pending approvals
curl http://localhost:8010/api/v1/pending-approvals

# Approve an action
curl -X POST http://localhost:8010/api/v1/approve/{approval_id}

# Deny an action
curl -X POST http://localhost:8010/api/v1/deny/{approval_id}
```

## Environment Variables

All variables have defaults matching `docker-compose.yml`. Override via
environment or `.env` file at repo root.

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_GROUP_ID` | `omniwatch-orchestration-group` | Consumer group id |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset policy |
| `OPA_URL` | `http://localhost:8181` | OPA policy engine endpoint |
| `OPA_CONFIDENCE_THRESHOLD` | `95.0` | Confidence threshold for OPA allow |
| `ENABLE_REAL_K8S` | `false` | Enable real K8s executor (lazy import) |
| `DRY_RUN` | `false` | Dry-run mode (simulates without executing) |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `MINIO_ENDPOINT` | `localhost:9010` | MinIO endpoint |
| `ORCHESTRATION_API_PORT` | `8010` | Service API port |

## Routing Rules

| Condition | Route |
|---|---|
| `block_ip` or `rotate_credentials` | ALWAYS → approval |
| OPA allow AND (confidence > 95 OR severity P1/P2) | → auto-execute |
| OPA needs_approval | → approval |
| Otherwise | → deny (fail-closed) |

## Action Registry

| Entity Type | Safe Actions | Unsafe (Approval Required) |
|---|---|---|
| `API_NODE` | restart_service, scale_deployment, clear_cache, kill_pod, rollback | block_ip |
| `DATABASE_NODE` | restart_service, rollback, clear_cache | rotate_credentials |
| `SERVICE` | restart_service, scale_deployment, kill_pod, rollback | — |
| `K8S_RESOURCE` | kill_pod, scale_deployment | — |
| `INFRASTRUCTURE` | restart_service, clear_cache | — |

## Tests

```bash
python -m pytest tests/phase-9-e2e/ -v
```

Run E2E tests (requires docker-compose + OPA):
```bash
python -m pytest tests/phase-9-e2e/ -v --tb=short
```
