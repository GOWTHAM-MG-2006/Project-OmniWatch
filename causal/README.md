# Causal Graph Engine

Phase 7 of OmniWatch. Causal root cause analysis with a PyRCA-powered
two-layer causal graph for cloud-native environments.

## What It Does

This service consumes confirmed anomaly signals from Kafka
(`omniwatch.anomalies.detected`, Phase 6 output), builds a two-layer causal
graph (Layer 1: runtime topology from entity resolution; Layer 2: learned
causal edges from PyRCA PC/RandomWalk analysis), traverses the graph from the
symptom entity backwards to the root cause, and publishes structured
`RootCauseObject` incidents to Kafka (`omniwatch.incidents.causal`) for
prioritization, orchestration, and remediation.

## Components

| Component | Purpose |
|-----------|---------|
| CausalEngine | FastAPI service orchestrator (analyze -> resolve -> publish) |
| DagTraversal | Backward BFS over merged two-layer graph to find root cause |
| TwoLayerGraph | Merged Layer-1 (topology) + Layer-2 (causal) graph with node/type registry |
| TemporalCausalModel | Lagged Pearson correlation for candidate edge scoring |
| PyRcaAdapter | GAP-locked wrapper around `sfr-pyrca` PC / RandomWalk / Bayesian analyzers |
| CrossCloudMapper | Raw entity id -> canonical `provider:region:entity_type:name` id |
| DependencyDiscovery | Discovers runtime topology from storage layer (empty offline) |
| RootCauseBuilder | Emits flat RootCauseObject per AGENTS.md contract |
| CausalConsumer | Kafka consumer for `omniwatch.anomalies.detected` |
| CausalProducer | Kafka producer for `omniwatch.incidents.causal` (best-effort) |
| FastAPI Health | `/health` endpoint on port 8008 |

## Inputs

| Source | Topic / Table | Description |
|--------|--------------|-------------|
| Kafka | `omniwatch.anomalies.detected` | Confirmed anomaly signals (performance + security) |
| Storage | topology | Entity topology discovered at graph build (simulation fallback offline) |

## Outputs

| Destination | Topic / Table | Description |
|-------------|--------------|-------------|
| Kafka | `omniwatch.incidents.causal` | Structured RootCauseObject incidents |
| API | `GET /health` | Health + graph readiness + last incident |

RootCauseObject is emitted with FLAT keys per AGENTS.md: `incident_id`,
`root_cause_entity`, `entity_type`, `confidence`, `anomaly_score`,
`fault_path` (root -> symptom), `impacted_services`, `impacted_count`,
`evidence` (`metrics`, `log_snippets`, `anomaly_timeline`), `timestamp`.

## Dependencies

- **PyRCA**: install `sfr-pyrca==1.0.1` (NEVER `pip install pyrca` — that is a
  squatted PyPI name). PyRCA pins `scikit-learn<1.2` which has no cp314 wheels,
  so this service runs on `python:3.10-slim` only.
- `kafka-python-ng` provides the top-level `kafka` namespace. Imports are lazy
  (`from kafka import ...` inside functions) so the service degrades gracefully
  when Kafka is unavailable (simulation-first rule).
- `storage/` module (shared logging, ClickHouse, Neo4j clients) must be
  available at runtime. The Docker build context is set to repo root (`.`) so
  `COPY storage/ ./storage/` works.

## How to Run

### Docker Compose

```bash
docker-compose up -d causal
```

### Health Check

```bash
curl.exe http://localhost:8008/health
```

Expected response:

```json
{
  "status": "degraded",
  "kafka": false,
  "graph_ready": true,
  "last_incident": "none"
}
```

> Note: `kafka` reports `false` (status `degraded`) when no Kafka broker is
> reachable; incidents are still computed and returned locally, satisfying the
> simulation-first rule. `graph_ready` becomes `true` once a topology has been
> loaded. `last_incident` shows the most recent root cause entity processed.

### Standalone (development)

```bash
python -m causal.causal_engine
```

Runs the FastAPI server on `0.0.0.0:8008` and starts the Kafka consumer thread
(no-op when Kafka is unavailable).

## Environment Variables

All variables have defaults matching `docker-compose.yml`. Override via
environment or `.env` file at repo root.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_GROUP_ID` | `omniwatch-causal-group` | Consumer group id |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset policy |
| `CAUSAL_MAX_DEPTH` | `10` | Max fault-path traversal depth |
| `CAUSAL_MIN_CONFIDENCE` | `0.3` | Minimum causal confidence gate |

Algorithm parameters (lag window, partial correlation, canonical id template,
provider defaults) live in `causal/config/causal_rules.yaml`.

## Service Endpoints

Add port 8008 to the AGENTS.md Service Endpoints reference table:

| Service | URL | Credentials |
|---------|-----|-------------|
| Causal Engine Health | http://localhost:8008 | none |

## Tests

```bash
python -m pytest tests/phase-7-e2e/ -v
```

End-to-end test suite covering the full causal pipeline against simulated
anomaly signals. Requires pytest configured via `pyproject.toml`.

## Project Structure

```
causal/
  causal_engine.py           # FastAPI service + pipeline orchestrator (port 8008)
  dag_traversal.py           # Backward BFS root cause traversal
  two_layer_graph.py         # Layer-1 topology + Layer-2 causal merged graph
  temporal_causal_model.py   # Lagged correlation edge scoring
  py_rca_adapter.py          # PyRCA (sfr-pyrca) analyzer wrapper
  cross_cloud_mapper.py      # Raw -> canonical entity id resolution
  dependency_discovery.py    # Runtime topology discovery
  root_cause_builder.py      # Flat RootCauseObject builder
  causal_consumer.py         # Kafka consumer (anomalies.detected)
  causal_producer.py         # Kafka producer (incidents.causal)
  config/
    settings.py              # Pydantic Settings
    causal_rules.yaml        # Algorithm + canonical id rules
  Dockerfile
  requirements.txt
```
