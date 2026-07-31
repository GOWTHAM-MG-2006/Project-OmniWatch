# Feature Store (Phase 4 — Windowing Layer)

Phase 4 consumes the `omniwatch.metrics.normalized` topic produced by Entity Resolution (Phase 3) and applies three windowing strategies: tumbling, sliding, and session. The windowed results are aggregated into 15-field feature vectors and persisted to ClickHouse. A FastAPI sidecar on port 8005 serves those vectors to downstream consumers, primarily the anomaly detection pipeline (Phase 6).

```
                         ┌──────────────────────────────────────────────┐
                         │            FeatureStoreJob (Flink)           │
                         │                                              │
omniwatch.metrics  ─────►│  ┌─ Tumbling  1m / 5m / 15m  ─┐             │
  .normalized            │  ├─ Sliding   5m (hop 1m)      ├─► Kafka    │
(Kafka)                  │  └─ Session   30s gap           │  windowed_{│
                         │                                │  1m,5m,15m}│
                         │  ┌─ FeatureVectorBuilder       │            │
                         │  │  (merge window features)    │            │
                         │  └─► FeatureStoreWriter         │            │
                         │      (ClickHouse sink)         │            │
                         └───────────┬──────────────────────┘            │
                                     │                                   │
                                     ▼                                   │
                         ┌───────────────────────┐                      │
                         │  ClickHouse            │                      │
                         │  feature_vectors table │                      │
                         │  (15 columns)          │                      │
                         └───────────┬───────────┘                      │
                                     │                                   │
                                     ▼                                   │
                         ┌───────────────────────┐                      │
                         │  Feature Store API     │◄─────────────────────┘
                         │  FastAPI :8005         │
                         │  GET /features/{id}    │
                         └───────────┬───────────┘
                                     │
                                     ▼
                         Phase 6 — Anomaly Detection
```

---

## Components

| Component | Class / File | Role |
|---|---|---|
| **FeatureStoreJob** | `FeatureStoreJob.java` | Flink entry point. Consumes `omniwatch.metrics.normalized`, keys by entity, branches into tumbling, sliding, and session operators, then feeds `FeatureVectorBuilder` → `FeatureStoreWriter`. Delivery guarantee: `AT_LEAST_ONCE`. Checkpointing: 60 seconds. Pre-flight Kafka check: 5 retries × 5 s. |
| **TumblingWindowAggregator** | `TumblingWindowAggregator.java` | `AggregateFunction` producing `WindowedFeature` for tumbling windows (1 m, 5 m, 15 m). Computes min, max, avg, count, sum per metric. Window bounds stamped by `WindowBoundsStamper`. |
| **SlidingWindowAggregator** | `SlidingWindowAggregator.java` | `ProcessWindowFunction` for a 5-minute window with a 1-minute slide. Computes p50, p95, p99, stddev, and rate from sorted value lists with linear interpolation. |
| **SessionWindowDetector** | `SessionWindowDetector.java` | `ProcessWindowFunction` using a 30-second session gap. Counts `isError()` events per session; sets `burstFlag` when the count exceeds the threshold (default: 3). |
| **FeatureVectorBuilder** | `FeatureVectorBuilder.java` | `KeyedProcessFunction` that merges windowed features into 15-field `FeatureVector` objects. Maintains per-entity state (aggregation accumulators, `featureVersion` counter, TTL). |
| **FeatureStoreWriter** | `FeatureStoreWriter.java` | `SinkFunction` writing `FeatureVector` rows to ClickHouse. Batches 100 rows or flushes every 1 second. Creates the `feature_vectors` table on startup via `CREATE TABLE IF NOT EXISTS`. |
| **Feature Store API** | `api/main.py` | FastAPI application on port 8005. Serves `GET /features/{entity_id}` (with optional `window_size`, `start`, `end` query params) and `GET /health`. |

---

## Input / Output Topics

**Input:**

| Topic | Producer | Consumer |
|---|---|---|
| `omniwatch.metrics.normalized` | Phase 2 ingestion normalizer | FeatureStoreJob |

**Output (Kafka):**

| Topic | Producer | Consumer |
|---|---|---|
| `omniwatch.features.windowed_1m` | TumblingWindowAggregator (1 m) | FeatureVectorBuilder, Phase 6 |
| `omniwatch.features.windowed_5m` | TumblingWindowAggregator (5 m), SlidingWindowAggregator | FeatureVectorBuilder, Phase 6 |
| `omniwatch.features.windowed_15m` | TumblingWindowAggregator (15 m), SessionWindowDetector | FeatureVectorBuilder, Phase 6 |

**Output (ClickHouse):**

| Table | Engine | Partition Key | TTL |
|---|---|---|---|
| `feature_vectors` | MergeTree | `toYYYYMMDD(timestamp)` | 90 days |

`feature_vectors` columns:

```sql
CREATE TABLE IF NOT EXISTS feature_vectors
(
    entity_id       String,
    window_start    DateTime,
    window_end      DateTime,
    window_size     String,
    latency_p50     Float64,
    latency_p95     Float64,
    latency_p99     Float64,
    latency_avg     Float64,
    latency_min     Float64,
    latency_max     Float64,
    error_rate      Float64,
    request_volume  UInt64,
    feature_version UInt32,
    ttl             UInt32,
    timestamp       DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(timestamp)
TTL timestamp + INTERVAL 90 DAY
```

---

## Build

The Flink job compiles to a fat jar via the Shadow Gradle plugin. Java 11, Flink 1.17.2, Scala 2.12.

**Windows:**

```powershell
cd feature-store\flink
gradlew.bat clean shadowJar
```

**Linux / CI:**

```bash
cd feature-store/flink
./gradlew clean shadowJar
```

The jar lands at `build/libs/omniwatch-feature-store-job.jar`.

**Unit tests (75 tests covering window math):**

```bash
gradlew.bat clean test          # Windows
./gradlew clean test            # Linux
```

---

## Run with Docker Compose

Start the infrastructure and both Phase 4 services:

```bash
docker compose up -d \
  zookeeper kafka clickhouse \
  flink-jobmanager flink-taskmanager \
  feature-store-flink feature-store-api
```

Submit the Flink job to the running cluster:

```bash
docker exec omniwatch-feature-store-flink flink run \
  -d -m flink-jobmanager:8081 \
  -c com.omniwatch.features.FeatureStoreJob \
  /opt/flink/jobs/omniwatch-feature-store-job.jar \
  --kafka.brokers kafka:29092 \
  --clickhouse.host clickhouse \
  --clickhouse.port 8123 \
  --clickhouse.db omniwatch
```

**Environment variable equivalents** (used when CLI args are omitted):

| Variable | Default | CLI Override |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` | `--kafka.brokers` |
| `KAFKA_GROUP_ID` | `flink-feature-store` | `--kafka.group.id` |
| `CLICKHOUSE_HOST` | `clickhouse-server` | `--clickhouse.host` |
| `CLICKHOUSE_PORT` | `8123` | `--clickhouse.port` |
| `CLICKHOUSE_DB` | `omniwatch` | `--clickhouse.db` |

**Feature Store API:**

| Endpoint | Description |
|---|---|
| `http://localhost:8005/health` | Health check (`{"status":"healthy","service":"feature-store-api"}`) |
| `http://localhost:8005/features/{entity_id}` | Feature vectors (optional query params: `window_size`, `start`, `end` in ISO 8601) |

---

## Kubernetes

Deploy the Phase 4 resources into the `omniwatch` namespace:

```bash
kubectl apply -k k8s/feature-store/       # kustomize (preferred)
# or, per-component:
kubectl apply -f k8s/feature-store/flink/configmap.yaml
kubectl apply -f k8s/feature-store/flink/deployment.yaml
kubectl apply -f k8s/feature-store/api/deployment.yaml
kubectl apply -f k8s/feature-store/api/service.yaml
```

| Resource | Name | Type | Namespace |
|---|---|---|---|
| `flink/deployment.yaml` | `feature-store-flink` | Deployment (replicas: 1) | `omniwatch` |
| `flink/configmap.yaml` | `feature-store-flink-config` | ConfigMap | `omniwatch` |
| `api/deployment.yaml` | `feature-store-api` | Deployment (replicas: 1) | `omniwatch` |
| `api/service.yaml` | `feature-store-api` | ClusterIP (port 8005) | `omniwatch` |

The API is reachable inside the cluster at `feature-store-api.omniwatch.svc.cluster.local:8005`.

---

## E2E Testing

The E2E test validates the full pipeline: Kafka ingestion → windowing → ClickHouse persistence → API retrieval. It requires the complete stack up and the Flink job running.

```bash
pytest tests/phase-4-e2e/ -v
```

The test confirms:

- The `feature_vectors` table exists with all 15 columns (`DESCRIBE TABLE feature_vectors`).
- After injecting test metrics, at least one row appears in `feature_vectors`.
- `GET /features/{entity_id}` returns a JSON array with matching window sizes.

---

## Failure Semantics

**Kafka unreachable at startup.** `waitForKafka` retries 5 times, 5 seconds apart. If all attempts fail, the job exits with a runtime exception. Mid-stream Kafka outages are handled by Flink's built-in restart strategy and checkpointing.

**ClickHouse write failure.** `FeatureStoreWriter` retries the batch insert 3 times with exponential backoff (100 ms → 500 ms → 2 s). After the third failure the batch is dropped and the `dropped_batches` counter is incremented. The pipeline never blocks.

**Malformed JSON.** Parse failures are logged and skipped. A default empty object replaces the unparseable record, keeping the stream moving.
