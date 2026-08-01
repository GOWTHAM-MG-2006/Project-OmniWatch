# storage — Unified Storage Layer (Phase 5)

## 1. Overview

Phase 5 implements the **Unified Storage Layer** — the single persistence tier
that every upstream and downstream phase reads and writes through. It is made
of three complementary stores (Dataflow.md Tools 6–8):

| Store | What it holds | Tables / Buckets |
|-------|---------------|------------------|
| **ClickHouse** | Time-series telemetry, anomalies, incidents, approvals, knowledge base | `metrics`, `logs`, `traces`, `anomalies`, `incidents`, `pending_approvals`, `knowledge_base` (database `omniwatch`) |
| **Neo4j** | Causal-dependency topology graph (services, databases, infrastructure, K8s resources) | Labels `:Service`, `:Database`, `:Infrastructure`, `:K8sResource` with `:CALLS`, `:READS_FROM`, `:DEPENDS_ON` relationships |
| **MinIO** | Object archives: telemetry archive, incident records, audit logs, runbooks, ML datasets | `omniwatch-telemetry-archive`, `omniwatch-incidents`, `omniwatch-audit-logs`, `omniwatch-runbooks`, `omniwatch-ml-datasets` |

All three backends are configured from a single `StorageConfig` (env-var driven,
docker-compose defaults), and every client shares the same resilience contract:
structured-JSON logging, `StorageError` exception base, and 3x exponential
backoff retries (`100ms → 500ms → 2s`) via `storage/common.py`.

## 2. Architecture

```
Feature store / ingestion / predictive / prioritization
              │
              ▼
        UNIFIED STORAGE LAYER (storage/)
              │
     ┌────────┼────────────┬────────────────────┐
     ▼        ▼            ▼                    ▼
 ClickHouse  Neo4j        MinIO             (Kafka consumers)
 (schema.sql) (topology   (5 buckets)       topology_loader.py
 metrics/     graph,      upload/download/   reads
 logs/traces  Causal RCA  list/delete        omniwatch.entities.resolved
 anomalies/   traversal   archive objects)   → writes Neo4j graph
 incidents)
```

- **ClickHouse** stores the hot time-series path (Dataflow.md Tool 6):
  telemetry from `ingestion/`, anomalies from `predictive/`, incidents from
  `prioritization/`, and resolved outcomes from the learning loop. Schema is
  applied idempotently by migration `001_initial_schema.py`.
- **Neo4j** stores the topology graph (Tool 7) consumed by the Phase 7 causal
  engine for DAG root-cause traversal. `topology_loader.py` populates it from
  the `omniwatch.entities.resolved` Kafka topic produced by Phase 3.
- **MinIO** stores object payloads (Tool 8): aged telemetry, full incident
  records, compliance/audit logs, generated runbooks, and ML training sets.
- **storage/common.py** is the shared backbone: JSON logging, `StorageError`,
  and the retry helper used by all three clients.

## 3. Configuration

All connection settings come from environment variables with defaults that
match `docker-compose.yml` for local development. Build a config with:

```python
from storage.config import StorageConfig
cfg = StorageConfig.from_env()   # reads env, falls back to compose defaults
cfg.env()                        # dict of effective env-var → value
```

| Env var | Default | Backend |
|---------|---------|---------|
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse (HTTP) |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse (HTTP port, NOT 9000 native) |
| `CLICKHOUSE_DB` | `omniwatch` | ClickHouse |
| `CLICKHOUSE_USER` | `default` | ClickHouse |
| `CLICKHOUSE_PASSWORD` | `""` | ClickHouse |
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j (Bolt) |
| `NEO4J_USER` | `neo4j` | Neo4j |
| `NEO4J_PASSWORD` | `omniwatch` | Neo4j |
| `MINIO_ENDPOINT` | `localhost:9010` | MinIO (API port 9010, NOT 9001 console) |
| `MINIO_ACCESS_KEY` | `minioadmin` | MinIO |
| `MINIO_SECRET_KEY` | `minioadmin` | MinIO |
| `MINIO_SECURE` | `false` | MinIO (accepts `1/true/yes/on`) |
| `MINIO_CONSOLE_PORT` | `9001` | MinIO (console, informational) |

`StorageConfig` is a plain dataclass; any field can also be set directly
(e.g. `StorageConfig(clickhouse_host="clickhouse")`) for test injection.
Numeric/boolean fields are parsed to their typed value — malformed env values
raise `ValueError` (fail-fast).

## 4. Components

### `storage/config.py`
- **Purpose:** Centralized connection configuration for all three storage backends.
- **Inputs:** `CLICKHOUSE_*`, `NEO4J_*`, `MINIO_*` env vars.
- **Outputs:** A `StorageConfig` dataclass instance via `StorageConfig.from_env()`, plus `cfg.env()` for diagnostics.

### `storage/common.py`
- **Purpose:** Shared structured-JSON logging, the `StorageError` exception base, and the 3x exponential-backoff retry helper.
- **Inputs:** Python logging records; callables (client connect/query functions).
- **Outputs:** JSON log lines to stdout; retried function results, or the last exception re-raised after retries are exhausted.
- **Public API:** `create_logger(name, level=logging.INFO)`, `StorageError(Exception)`, `retry_with_backoff(func, retries=3, base_delay=0.1, max_delay=2.0, logger=None, *args, **kwargs)`, `RETRY_MULTIPLIER = 5.0`.

### `storage/clickhouse/client.py`
- **Purpose:** Batched insert + query client for the ClickHouse tables.
- **Inputs:** Telemetry/anomaly/incident row dicts (columns must match `schema.sql`); `StorageConfig`.
- **Outputs:** Inserted row counts (`int`), entity-scoped rows (`list[dict]`), per-table row-count map (`dict[str, int]`), connectivity status (`bool`).
- **Public API:** `ClickHouseClient(config=None, *, connect_timeout=10, send_receive_timeout=30)`, `get_client()`, `close()`, `health_check() -> bool`, `get_table_stats() -> dict[str, int]`, `insert_metrics(rows)`, `insert_logs(rows)`, `insert_anomalies(rows)`, `insert_incidents(rows)`, `select_by_entity(entity_id, table="metrics", limit=100, order_by=None) -> list[dict]`.

### `storage/clickhouse/schema.sql`
- **Purpose:** DDL for the ClickHouse layer — the 7 Phase 5 tables.
- **Inputs:** Telemetry (metrics/logs/traces), anomaly records, incident records, resolved incident outcomes.
- **Outputs:** 7 MergeTree tables in database `omniwatch` with daily partitioning (`toYYYYMMDD`) and TTL retention (metrics 90d, logs 30d, traces 30d, incidents 365d; `pending_approvals` + `knowledge_base` persist).
- Idempotent (`CREATE DATABASE/TABLE IF NOT EXISTS`). `feature_vectors` is **not** created here — Phase 4 owns it.

### `storage/clickhouse/migrations/001_initial_schema.py`
- **Purpose:** Idempotent DDL migration that reads `schema.sql`, splits it into statements (comment/string-aware splitter), and executes each against ClickHouse.
- **Inputs:** `schema.sql` (path relative to this file); `CLICKHOUSE_*` env vars.
- **Outputs:** The `omniwatch` database with all 7 tables created.
- **Run:** `python -m storage.clickhouse.migrations.001_initial_schema` — safe to run repeatedly. Programmatic API: `load_schema_statements()`, `apply_schema()`, `main()`.

### `storage/neo4j/client.py`
- **Purpose:** Bolt-7687 client for the Neo4j causal-dependency graph.
- **Inputs:** `StorageConfig` connection params; node payloads keyed on `id`; relationship payloads matching AGENTS.md types (`:CALLS`, `:READS_FROM`, `:DEPENDS_ON`).
- **Outputs:** Graph writes (node dict / relationship records), connected-node and full-topology query results, health status.
- **Public API:** `Neo4jClient(config=None)`, `connect()`, `close()`, `health_check() -> bool`, `create_node(label, properties) -> Optional[dict]`, `create_relationship(source_id, rel_type, target_id, properties=None) -> Optional[dict]`, `query_by_entity(entity_id) -> List[dict]`, `get_topology() -> dict` (`{nodes, relationships, node_count, relationship_count}`).

### `storage/neo4j/constraints.py`
- **Purpose:** Idempotent bootstrap of Neo4j schema constraints/indexes for the four topology labels.
- **Inputs:** A connected `neo4j.GraphDatabase.driver`.
- **Outputs:** Per label: unique constraint on `id` + composite index on `(entity_id, name, type)`; nothing returned. `NODE_LABELS = ("Service", "Database", "Infrastructure", "K8sResource")`.
- **Run:** `python -m storage.neo4j.constraints` (builds driver from `StorageConfig.from_env()`); programmatic API: `apply_constraints(driver)`.

### `storage/neo4j/topology_loader.py`
- **Purpose:** Kafka consumer that materializes resolved entities into the Neo4j graph — consumes `omniwatch.entities.resolved` (Phase 3 output), maps `entity_type` → Neo4j label (`API_NODE`/`SERVICE` → `Service`, `DATABASE_NODE`/`DATABASE` → `Database`, `INFRASTRUCTURE` → `Infrastructure`, `K8S`/`K8S_RESOURCE` → `K8sResource`; unknown types are skipped with a warning), upserts nodes by `id` (MERGE), and creates `:CALLS` / `:READS_FROM` / `:DEPENDS_ON` relationships carrying their AGENTS.md properties. Relationship targets that don't exist yet are skipped with a warning — the target may arrive in a later message, and the relationship is created when it does (idempotent upsert on re-arrival). Graceful shutdown on SIGINT/SIGTERM. All Neo4j writes go through `Neo4jClient`; no Cypher is built in this module.
- **Inputs:** `UnifiedEntity` records on `omniwatch.entities.resolved` (entity_id, entity_type, name, type, criticality, cloud_provider, status, anomaly_score, last_seen; relationship hints `depends_on` / `calls` / `reads_from`); `StorageConfig`; `KAFKA_BOOTSTRAP_SERVERS` env var (default `localhost:9092`).
- **Outputs:** `:Service` / `:Database` / `:Infrastructure` / `:K8sResource` nodes and `:CALLS` / `:READS_FROM` / `:DEPENDS_ON` relationships in Neo4j.
- **Run:** `python -m storage.neo4j.topology_loader` — verifies Neo4j connectivity before starting the consumer; exits 1 if Neo4j is unreachable.

### `storage/minio/client.py`
- **Purpose:** Unified object-storage client for MinIO (S3-compatible).
- **Inputs:** `StorageConfig` (endpoint `host:port` WITHOUT scheme, keys, secure flag); raw byte payloads; object keys.
- **Outputs:** `ObjectWriteResult` on upload, raw object bytes on download, object name listings, `None` on delete, `health_check()` boolean. SDK exceptions become `StorageError` after retry.
- **Public API:** `MinioClient(config=None, logger=None)`, `upload_object(bucket, object_name, data: bytes, content_type="application/octet-stream") -> ObjectWriteResult`, `download_object(bucket, object_name) -> bytes`, `list_objects(bucket, prefix="") -> List[str]`, `delete_object(bucket, object_name) -> None`, `health_check() -> bool`.

### `storage/minio/bucket_setup.py`
- **Purpose:** Idempotently bootstrap the 5 MinIO buckets and attach their lifecycle (expiry) policies.
- **Inputs:** A connected `minio.Minio` client (or a `StorageConfig` to build one).
- **Outputs:** Ensured buckets — `omniwatch-telemetry-archive` (90d expiry), `omniwatch-incidents` (365d), `omniwatch-audit-logs` (365d); `omniwatch-runbooks` + `omniwatch-ml-datasets` (no lifecycle).
- **Public API:** `setup_buckets(client=None, config=None, buckets=None) -> List[str]` (sorted ensured names). `LIFECYCLE_DAYS` drives expiry rules (Expiration — MinIO single-node rejects Transition storage classes).

### `storage/health.py`
- **Purpose:** Aggregated health summary for the storage layer — runs each backend's `health_check()` and returns a single `{clickhouse, neo4j, minio}` result map (plus `all_healthy`) consumed by the dashboard/API gateway. A down store is reported as `false` and never crashes the check; every client opened is always closed (even on partial failure).
- **Inputs:** `StorageConfig`.
- **Outputs:** `{"clickhouse": bool, "neo4j": bool, "minio": bool, "all_healthy": bool}` health summary dict; pretty JSON to stdout when run as a CLI probe.
- **Public API:** `check_storage_health(config: StorageConfig | None = None) -> dict` (builds fresh clients from `StorageConfig.from_env()` unless a config is passed).
- **Run:** `python -m storage.health` — exits 0 when all stores are healthy, else 1.

## 5. Usage Examples

```python
from storage.config import StorageConfig
from storage.clickhouse.client import ClickHouseClient
from storage.neo4j.client import Neo4jClient
from storage.minio.client import MinioClient
from storage.minio.bucket_setup import setup_buckets
from storage.neo4j.constraints import apply_constraints

cfg = StorageConfig.from_env()

# --- Health checks ------------------------------------------------------
clickhouse = ClickHouseClient(cfg)
assert clickhouse.health_check() is True
print(clickhouse.get_table_stats())          # {'metrics': 0, 'logs': 0, ...}

neo4j = Neo4jClient(cfg)
assert neo4j.health_check() is True

minio = MinioClient(cfg)
assert minio.health_check() is True

# --- ClickHouse insert / select -----------------------------------------
clickhouse.insert_metrics([
    {"entity_id": "postgresql-database", "entity_type": "DATABASE_NODE",
     "metric_name": "cpu_usage", "value": 87.5, "tags": {"region": "us-east"},
     "source_type": "performance", "timestamp": "2026-08-01T00:00:00Z"},
])
rows = clickhouse.select_by_entity("postgresql-database", table="metrics", limit=10)

# --- Neo4j node / relationship / queries ---------------------------------
from neo4j import GraphDatabase  # build a driver for constraints bootstrap
driver = GraphDatabase.driver(cfg.neo4j_uri, auth=(cfg.neo4j_user, cfg.neo4j_password))
apply_constraints(driver)                    # idempotent schema bootstrap
driver.close()

neo4j.create_node("Service", {"id": "order-service", "name": "order-service",
                              "type": "API_NODE", "criticality": "high"})
neo4j.create_node("Database", {"id": "postgresql-database", "name": "postgresql",
                               "type": "DATABASE_NODE", "criticality": "high"})
neo4j.create_relationship("order-service", "READS_FROM", "postgresql-database",
                          {"query_type": "SELECT", "avg_duration_ms": 12.5})
neighbors = neo4j.query_by_entity("order-service")       # connected nodes + edge detail
topology = neo4j.get_topology()                          # {nodes, relationships, ...}

# --- MinIO upload / download / list --------------------------------------
setup_buckets(client=None, config=cfg)                   # ensure 5 buckets (idempotent)
minio.upload_object("omniwatch-incidents", "inc-001.json",
                    b'{"incident_id": "inc-001"}', content_type="application/json")
data = minio.download_object("omniwatch-incidents", "inc-001.json")
names = minio.list_objects("omniwatch-incidents", prefix="inc-")
minio.delete_object("omniwatch-incidents", "inc-001.json")
```

Clients are safe to share process-wide (SDK-managed connection pools) and are
lazy: the first real connection happens on the first operation. Always
`close()` when shutting down.

## 6. Kubernetes Deployment

K8s manifests for the three backends live under `k8s/infra/`. Each service has
a `deployment.yaml` + `service.yaml` + `configmap.yaml`, and each configmap
mirrors the `StorageConfig` env-var contract so storage clients running inside
the cluster resolve the in-cluster endpoints automatically:

| Manifest | Sets | Maps to StorageConfig field |
|----------|------|------------------------------|
| `k8s/infra/clickhouse/configmap.yaml` | `CLICKHOUSE_HOST=clickhouse`, `CLICKHOUSE_PORT=8123`, `CLICKHOUSE_DB=omniwatch`, `CLICKHOUSE_USER=default`, `CLICKHOUSE_PASSWORD=""` | `clickhouse_*` |
| `k8s/infra/neo4j/configmap.yaml` | `NEO4J_URI=bolt://neo4j:7687`, `NEO4J_USER=neo4j`, `NEO4J_PASSWORD=omniwatch` | `neo4j_*` |
| `k8s/infra/minio/configmap.yaml` | `MINIO_ENDPOINT=minio:9010`, `MINIO_ACCESS_KEY=minioadmin`, `MINIO_SECRET_KEY=minioadmin`, `MINIO_SECURE="false"` | `minio_*` |

Deploy with `kubectl apply -f k8s/infra/clickhouse/ -f k8s/infra/neo4j/ -f k8s/infra/minio/`.
Because the configmap keys are exactly the `StorageConfig` env vars, no client
code changes are needed between local Docker and K8s — only the env values
differ (service DNS names replace `localhost`).

## 7. Testing

Run the Phase 5 unit test suite (Task 14):

```powershell
pytest storage/tests/ -v
```

Run the Phase 5 end-to-end tests (Task 15), which boot the full storage layer
against the docker-compose infrastructure:

```powershell
pytest tests/phase-5-e2e/ -v
```

Test dependencies (`pytest`, `pytest-asyncio`) are pinned in the repo-root
`requirements.txt`. Storage clients also follow the Simulation-First rule — all
backends run locally via `docker-compose up -d`, so no cloud credentials are
required.

## 8. Data Contracts

- **AnomalySignal** (from `predictive/`): `entity_id`, `entity_type`,
  `metric_name`, `anomaly_score`, `confidence`, `timestamp`,
  `deviation_from_baseline`, `source_type` → stored in `omniwatch.anomalies`
  (with `anomaly_id`, `status`).
- **Entity record / `UnifiedEntity`** (from Phase 3): normalized cross-cloud
  entity identity published to the **`omniwatch.entities.resolved`** Kafka
  topic. `topology_loader.py` consumes this topic and upserts the matching
  Neo4j nodes/relationships, keeping the graph in sync with resolved entities.
- **IncidentRecord**: stored in `omniwatch.incidents`; full JSON incident
  records are also archived to MinIO `omniwatch-incidents`.
- Column names in the ClickHouse client constants (`METRICS_COLUMNS`, etc.)
  MUST match `schema.sql` — do not change either side without the other.
- Neo4j node labels and relationship types follow the AGENTS.md topology
  contract (`:Service`, `:Database`, `:Infrastructure`, `:K8sResource`;
  `:CALLS`, `:READS_FROM`, `:DEPENDS_ON`) — `topology_loader` and the Phase 7
  causal engine depend on these exact labels.
