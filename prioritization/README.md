# OmniWatch — Incident Prioritization Engine (Phase 8)

## Purpose

Consumes `RootCauseObject` records from the Kafka topic `omniwatch.incidents.causal`
(Phase 7 output), applies severity classification, business impact scoring, SLA risk
calculation, and alert deduplication (GAP 3), then publishes prioritized
`IncidentRecord` objects to `omniwatch.incidents.created` (consumed by Phase 9
orchestration).

## Architecture

```
Kafka (omniwatch.incidents.causal)
  ↓
PrioritizationConsumer  — deserializes RootCauseObject
  ↓
PrioritizationEngine
  ├── IncidentFactory
  │     ├── SeverityClassifier    → P1/P2/P3/P4 (classification_rules.yaml)
  │     ├── ImpactScorer          → business_impact_score (0..100)
  │     ├── SlaRiskCalculator     → HIGH/MEDIUM/LOW
  │     ├── AssignmentRouter      → auto-remediation / oncall-engineer
  │     └── MinIO archive         → omniwatch-incidents bucket
  │
  ├── DeduplicationEngine  → TTLCache, merges duplicates (GAP 3)
  │
  └── PrioritizationProducer → publishes to omniwatch.incidents.created
```

## Components

| File | Responsibility |
|---|---|
| `models.py` | `RootCauseObject`, `IncidentRecord` Pydantic models, `normalize_confidence()` |
| `severity_classifier.py` | P1–P4 classification from YAML rules (AND/OR logic) |
| `impact_scorer.py` | Business impact score 0–100 from anomaly, impact, confidence, severity |
| `sla_risk_calculator.py` | SLA breach risk (HIGH/MEDIUM/LOW) with impact elevation |
| `deduplication_engine.py` | Alert deduplication (GAP 3) with TTLCache |
| `incident_factory.py` | End-to-end incident assembly: classify → score → assign → archive |
| `prioritization_consumer.py` | Kafka consumer for `omniwatch.incidents.causal` |
| `prioritization_producer.py` | Kafka producer for `omniwatch.incidents.created` |
| `prioritization_engine.py` | FastAPI orchestrator with `/health` and `/stats` endpoints |

## Inputs

**Kafka topic:** `omniwatch.incidents.causal`

Message format: `RootCauseObject` JSON (Phase 7 contract)
```json
{
  "incident_id": "uuid",
  "root_cause_entity": "postgresql-database",
  "entity_type": "DATABASE_NODE",
  "confidence": 0.85,
  "anomaly_score": 0.85,
  "fault_path": ["postgresql-database", "order-service", "api-gateway"],
  "impacted_services": ["order-service", "api-gateway"],
  "impacted_count": 3,
  "evidence": {"log_snippets": ["error: connection refused"]},
  "timestamp": "2026-01-01T00:00:00+00:00"
}
```

## Outputs

**Kafka topic:** `omniwatch.incidents.created`

Message format: `IncidentRecord` JSON (Phase 9 contract)
```json
{
  "incident_id": "uuid",
  "created_at": "2026-01-01T00:00:00+00:00",
  "severity": "P1",
  "business_impact_score": 88.5,
  "root_cause": { ... },
  "related_anomalies": [],
  "deduplicated_count": 1,
  "sla_breach_risk": "HIGH",
  "assigned_to": "auto-remediation",
  "status": "OPEN"
}
```

**MinIO bucket:** `omniwatch-incidents` — full incident JSON archived per incident_id.

## Running Locally

```bash
# From the repo root
PYTHONPATH=/app python -m prioritization.prioritization_engine
# or with the engine module directly:
python -m prioritization.prioritization_engine

# Health check
curl http://localhost:8009/health

# Stats
curl http://localhost:8009/stats
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker endpoint |
| `KAFKA_GROUP_ID` | `omniwatch-prioritization-group` | Consumer group id |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset policy |
| `DEDUP_TTL_SECONDS` | `300` | Deduplication cache TTL (5 min) |
| `DEDUP_ENABLED` | `true` | Enable/disable deduplication |
| `MINIO_ENDPOINT` | `localhost:9000` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO access key |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO secret key |
| `MINIO_INCIDENTS_BUCKET` | `omniwatch-incidents` | Incident archive bucket |
| `PRIORITIZATION_API_PORT` | `8009` | Service API port |

## Classification Rules

Rules are defined in `config/classification_rules.yaml`:

- **P1:** ALL conditions — entity_type contains "DATABASE", confidence ≥ 85, impacted_count ≥ 3
- **P2:** ANY condition — confidence ≥ 70 OR anomaly_score ≥ 0.7
- **P3:** ANY condition — confidence ≥ 40 OR anomaly_score ≥ 0.4
- **P4:** Catch-all (everything else)

## SLA Risk

| Severity | Base SLA Risk |
|---|---|
| P1 | HIGH |
| P2 | MEDIUM |
| P3 | LOW |
| P4 | LOW |

**Impact elevation:** If `business_impact_score ≥ 80` → HIGH; if `≥ 50` → MEDIUM (overrides lower severity-based risk).

## Assignment

| Severity | Confidence (0–100) | Assigned To |
|---|---|---|
| P1 | ≥ 85 | `auto-remediation` |
| all others | — | `oncall-engineer` |

## Deduplication (GAP 3)

Uses a thread-safe in-memory `TTLCache` keyed on `root_cause_entity`. When a duplicate
is detected within the TTL window, the existing incident's `deduplicated_count` is
incremented and the incoming root cause is appended to `related_anomalies`.

**Known limitation:** Single-host only. Horizontal scaling requires migration to Redis-based shared state.

## Known Limitations

1. **In-memory deduplication only** — The deduplication engine uses a thread-safe in-memory `TTLCache`. It does not persist across restarts and cannot share state across multiple replicas. Horizontal scaling requires migration to Redis-based shared state.

2. **Single Kafka partition assumption** — The consumer reads from partition 0 only. In production with multiple partitions, incidents may be processed out of order or missed unless partition assignment is configured.

3. **No retry/backoff on Kafka publish failures** — If the producer fails to publish an `IncidentRecord` to `omniwatch.incidents.created`, the incident is logged and lost. A durable outbox pattern or dead-letter queue is needed for production reliability.

4. **MinIO and ClickHouse archival are best-effort** — Both MinIO archiving and ClickHouse persistence are wrapped in best-effort try/except blocks. Failures are logged but never block incident creation or Kafka publication.

5. **Classification rules are static** — Severity rules are loaded once from `config/classification_rules.yaml` at startup. Runtime changes require a service restart. A hot-reload mechanism or admin API is not yet implemented.
