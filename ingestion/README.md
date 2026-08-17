# Ingestion — Telemetry Ingestion Layer

**Phase:** 2
**Purpose:** Collect all OTLP telemetry from simulators and real services via the
OpenTelemetry Collector, route it through Kafka, normalize and enrich it with a
Flink 1.17 streaming job, and archive every event to MinIO object storage.

**Inputs:** OTLP gRPC/HTTP metrics, logs, traces, and security event payloads
**Outputs:** Normalized Kafka topics (metrics/logs/traces/security.normalized),
MinIO `omniwatch-telemetry-archive` bucket (JSONL under `dt=` partitions)

---

## Architecture Overview

```
  OTel Demo Services / Simulators
         │
         │  OTLP gRPC :4317 / HTTP :4318
         ▼
 ┌───────────────────┐
 │  OTel Collector   │  otelcol-config.yaml
 │  (contrib v0.157) │  receivers → batch → Kafka exporter
 └───────┬───────────┘
         │  otlp_json encoding
         ▼
 ┌───────────────────┐
 │  Kafka Broker     │  omniwatch.{metrics,logs,traces,security}.{raw,events}
 │  (Confluent 7.5)  │
 └───────┬───────────┘
         │  KafkaSource (Flink connector)
         ▼
 ┌───────────────────────────────────────────────────┐
 │  Flink Ingestion Job  (com.omniwatch.flink)      │
 │                                                   │
 │  Deserializers → Normalizers → K8sContextEnrichment│
 │       │              │               │             │
 │       ▼              ▼               ▼             │
 │  ┌─────────────────────────────────────────────┐   │
 │  │  KafkaSinks (normalized topics)             │   │
 │  │  + SecurityEventRouter (dual topic)         │   │
 │  └─────────────────────────────────────────────┘   │
 │       │                                            │
 │       ▼                                            │
 │  ┌─────────────────────────────────────────────┐   │
 │  │  MinIOSink → omniwatch-telemetry-archive    │   │
 │  │              dt=yyyy-MM-dd/events-*.jsonl   │   │
 │  └─────────────────────────────────────────────┘   │
 └───────────────────────────────────────────────────┘
```

The OTel Collector writes **directly** to Kafka. There is no intermediary
HTTP service. The Flink job is the sole consumer of the raw topics.

---

## Components

### 1. otelcol-config.yaml

OpenTelemetry Collector (contrib) configuration. Version v0.157.0+ with the
franz-go based Kafka client.

| Section | Details |
|---------|---------|
| **Receivers** | OTLP gRPC on `0.0.0.0:4317`, OTLP HTTP on `0.0.0.0:4318` |
| **Processors** | `memory_limiter` (512 MiB), `resourcedetection` (env + system), `resource` (deployment.environment=simulation, service.namespace=omniwatch), `batch` (5s / 1024 msgs) |
| **Exporters** | Kafka to `kafka:29092` with signal-level topic configs; `debug` for stdout |
| **Pipelines** | Three independent pipelines: metrics, traces, logs. Each runs OTLP → processors → Kafka + debug |

Kafka exporter topic mapping:

| Signal | Topic | Encoding |
|--------|-------|----------|
| Metrics | `omniwatch.metrics.raw` | `otlp_json` |
| Traces | `omniwatch.traces.raw` | `otlp_json` |
| Logs | `omniwatch.logs.raw` | `otlp_json` |

### 2. kafka_bus.py

Central message bus utility for topic management and Python-side
produce/consume operations.

**Key classes:**

- `KafkaProducer` — high-level producer with delivery callbacks, Snappy
  compression, `acks=all`, and automatic retry (3 attempts, 500ms backoff).
- `KafkaConsumer` — high-level consumer with auto-commit, configurable
  `auto_offset_reset`, and graceful shutdown.
- `create_topics()` — admin function that idempotently creates all registered
  Kafka topics via `AdminClient`.

**CLI usage:**

```bash
python ingestion/kafka_bus.py create-topics   # create all topics
python ingestion/kafka_bus.py list-topics     # list existing topics
```

Topics are registered in `ALL_TOPICS` and `TOPIC_SPECS` with partition counts
and producer/consumer labels. The bus does **not** run as a persistent service.

### 3. flink/ — Java Ingestion Job

Apache Flink 1.17 streaming job built with Gradle 8.9. The shadow JAR
(`omniwatch-ingestion-job-all.jar`) bundles all dependencies.

**Build:**

```bash
cd ingestion/flink
./gradlew test shadowJar
```

JDK 25 via `JAVA_HOME` is compatible with Gradle 8.9.

#### Class inventory

| Package | Class | Role |
|---------|-------|------|
| (root) | `FlinkJobMain` | Entry point. Assembles sources, normalizers, enrichments, sinks, and executes the pipeline. |
| `config` | `FlinkConfig` | Parses CLI args (`--kafka.brokers`, `--kafka.group.id`, `--minio.endpoint`, `--minio.access-key`, `--minio.secret-key`, `--minio.bucket`, `--auto.offset.reset`) with env-var fallback and an allowlist that rejects unknown keys. |
| `deserializers` | `MetricDeserializer` | OTLP JSON → `MetricEvent` |
| `deserializers` | `LogDeserializer` | OTLP JSON → `LogEvent` |
| `deserializers` | `TraceDeserializer` | OTLP JSON → `TraceEvent` |
| `deserializers` | `SecurityEventDeserializer` | JSON → `SecurityEvent` |
| `normalizers` | `MetricNormalizer` | Unit conversion: bytes→MB, ms→s, µs→s, ns→s. Sets `normalizedValue`. |
| `normalizers` | `LogNormalizer` | Normalizes log severity and body fields |
| `normalizers` | `TraceNormalizer` | Normalizes trace span fields |
| `normalizers` | `EventNormalizer` | Generic normalizer used for security events |
| `enrichment` | `K8sContextEnrichment<T>` | `RichMapFunction` that extracts K8s namespace/pod/container/node from attributes map into standardized `k8s.*` keys |
| `producers` | `NormalizedMetricsProducer` | Factory for `KafkaSink` → `omniwatch.metrics.normalized` |
| `producers` | `NormalizedLogsProducer` | Factory for `KafkaSink` → `omniwatch.logs.normalized` |
| `producers` | `NormalizedTracesProducer` | Factory for `KafkaSink` → `omniwatch.traces.normalized` |
| `producers` | `NormalizedEventsProducer` | Factory for `KafkaSink` → union of all normalized event types |
| `producers` | `SecurityEventRouter` | Dual-sink factory: normalized security events to `omniwatch.security.normalized` **and** legacy `omniwatch.security.events` (backward compat) |
| `sink` | `MinIOSink` | `RichSinkFunction<String>` that buffers JSONL events (batch=100, flush every 5s) and writes to MinIO under `dt=yyyy-MM-dd/events-{timestamp}-{uuid}.jsonl` |
| `models` | `NormalizedEvent` | Abstract base: `entityId`, `entityType`, `timestamp`, `sourceType`, `sourceTopic`, `attributes` |
| `models` | `MetricEvent` | + `metricName`, `value`, `normalizedValue`, `unit` |
| `models` | `LogEvent` | + `severity`, `body`, `serviceName` |
| `models` | `TraceEvent` | + `traceId`, `spanId`, `parentSpanId`, `spanName`, `startTime`, `durationMs`, `status` |
| `models` | `SecurityEvent` | + `eventId`, `attackType`, `confidence`, `sourceIp`, `description` |

#### Pipeline flow inside FlinkJobMain

1. **4 KafkaSources** consume from `omniwatch.metrics.raw`, `omniwatch.logs.raw`, `omniwatch.traces.raw`, `omniwatch.security.events` with `OffsetsInitializer.committedOffsets(EARLIEST)`.
2. **4 Normalizer map operators** run type-specific normalization.
3. **4 K8sContextEnrichment map operators** enrich each stream with K8s metadata.
4. **Jackson serialization** converts each event to a JSON string.
5. **5 KafkaSinks** produce to normalized topics (`metrics.normalized`, `logs.normalized`, `traces.normalized`, `security.normalized`, `events.normalized`). Security events also route to the legacy `security.events` topic.
6. **MinIOSink** archives the union of all JSONL strings to MinIO.

Checkpoint interval: 60 seconds.

### 4. MinIO

Object storage for telemetry archival.

| Setting | Value |
|---------|-------|
| Endpoint (in-cluster) | `http://minio:9010` |
| Console | `http://localhost:9001` |
| Credentials | `minioadmin` / `minioadmin` |
| Bucket | `omniwatch-telemetry-archive` |

**Write path:** The `MinIOSink` buffers events and flushes them as JSONL files:

```
omniwatch-telemetry-archive/
  dt=2026-08-17/
    events-20260817-134022-a1b2c3d4.jsonl
    events-20260817-134027-e5f6g7h8.jsonl
  dt=2026-08-18/
    ...
```

Each file contains up to 100 JSON lines (one per event). Files are flushed
when the buffer is full or every 5 seconds, whichever comes first. The `dt=`
prefix uses Hive-style partitioning for efficient time-range queries.

The bucket is created by the `minio-bucket-init` compose service (profile:
`setup`). The `MinIOSink.open()` method also verifies/creates the bucket as
a fallback.

---

## Kafka Topics

| Topic | Producer | Consumer | Partitions |
|-------|----------|----------|------------|
| `omniwatch.metrics.raw` | otel-collector | flink-ingestion | 3 |
| `omniwatch.logs.raw` | otel-collector | flink-ingestion | 3 |
| `omniwatch.traces.raw` | otel-collector | flink-ingestion | 3 |
| `omniwatch.security.events` | simulation | flink-ingestion | 2 |
| `omniwatch.metrics.normalized` | flink-ingestion | entity-resolution | 3 |
| `omniwatch.logs.normalized` | flink-ingestion | entity-resolution | 3 |
| `omniwatch.traces.normalized` | flink-ingestion | entity-resolution | 3 |
| `omniwatch.security.normalized` | flink-ingestion | entity-resolution | 2 |
| `omniwatch.events.normalized` | flink-ingestion | entity-resolution | 2 |
| `omniwatch.anomalies.detected` | predictive | prioritization | — |

Raw topics carry OTLP JSON directly from the Collector. Normalized topics
carry the enriched, unit-normalized JSON produced by the Flink job.

---

## Data Contracts

All normalized events extend `NormalizedEvent` and are serialized to JSON by
Jackson (camelCase field names).

### NormalizedEvent (base)

```json
{
  "entityId": "string",
  "entityType": "string",
  "timestamp": 1692278422000,
  "sourceType": "string",
  "sourceTopic": "string",
  "attributes": { "k8s.namespace": "omniwatch" }
}
```

### MetricEvent

```json
{
  "entityId": "user-service",
  "entityType": "SERVICE",
  "timestamp": 1692278422000,
  "sourceType": "performance",
  "sourceTopic": "omniwatch.metrics.raw",
  "attributes": {},
  "metricName": "http.requests.total",
  "value": 1048576.0,
  "normalizedValue": 1.0,
  "unit": "By"
}
```

### LogEvent

```json
{
  "entityId": "order-service",
  "entityType": "SERVICE",
  "timestamp": 1692278422000,
  "sourceType": "performance",
  "sourceTopic": "omniwatch.logs.raw",
  "attributes": {},
  "severity": "ERROR",
  "body": "DB connection timeout",
  "serviceName": "order-service"
}
```

### TraceEvent

```json
{
  "entityId": "api-gateway",
  "entityType": "SERVICE",
  "timestamp": 1692278422000,
  "sourceType": "performance",
  "sourceTopic": "omniwatch.traces.raw",
  "attributes": {},
  "traceId": "abc123def456abc123def456abc123de",
  "spanId": "span001hex",
  "parentSpanId": "parent000",
  "spanName": "POST /api/orders",
  "startTime": 1692278422000,
  "durationMs": 50,
  "status": "OK"
}
```

### SecurityEvent

```json
{
  "entityId": "api-gateway",
  "entityType": "SERVICE",
  "timestamp": 1692278422000,
  "sourceType": "security",
  "sourceTopic": "omniwatch.security.events",
  "attributes": {},
  "eventId": "sec-001",
  "attackType": "BRUTE_FORCE",
  "confidence": 0.95,
  "sourceIp": "192.168.1.100",
  "description": "Multiple failed login attempts detected"
}
```

---

## How to Run

### (a) Start infrastructure

```powershell
docker-compose up -d
```

This starts Zookeeper, Kafka, MinIO, the OTel Collector, Flink
JobManager/TaskManager, and all other services.

### (b) Create Kafka topics

```powershell
cd E:\Project-OmniWatch
py ingestion\kafka_bus.py create-topics
```

Creates all registered topics (raw, normalized, downstream) if they don't
already exist.

### (c) Build the Flink job

```powershell
cd E:\Project-OmniWatch\ingestion\flink
.\gradlew.bat test shadowJar
```

Produces `build/libs/omniwatch-ingestion-job-all.jar`. JDK 25 via `JAVA_HOME`
works with Gradle 8.9.

### (d) Submit the Flink job

The shadow JAR has **no** `Main-Class` manifest entry. You must specify the
entry class explicitly when submitting via the Flink REST API.

**Step 1 — Upload the JAR:**

```powershell
curl.exe -X POST http://localhost:8081/jars/upload `
  -F "jarfile=@build\libs\omniwatch-ingestion-job-all.jar"
```

Note the `filename` from the response (e.g., `omniwatch-ingestion-job-all.jar`).

**Step 2 — Run the job:**

```powershell
curl.exe -X POST http://localhost:8081/jars/omniwatch-ingestion-job-all.jar/run `
  -H "Content-Type: application/json" `
  -d "{\"entryClass\": \"com.omniwatch.flink.FlinkJobMain\", \"programArgs\": \"--kafka.brokers kafka:29092 --kafka.group.id flink-ingestion --minio.endpoint http://minio:9010 --minio.access-key minioadmin --minio.secret-key minioadmin --minio.bucket omniwatch-telemetry-archive --auto.offset.reset earliest\"}"
```

`entryClass` is **required**. The `--auto.offset.reset earliest` flag ensures
the job reads from the beginning of each raw topic on first deploy.

### (e) Verify

Run the E2E test suite (see next section).

---

## Verification

E2E tests live in `tests/phase-2-e2e/test_ingestion_pipeline.py` (9 tests).

```powershell
E:\OmniWatch-Test-Files\phase2-e2e-venv\Scripts\python.exe -m pytest tests/phase-2-e2e -v
```

| Test | Validates |
|------|-----------|
| `test_otel_collector_health` | Collector reachable at `:8888/metrics`, reports `otelcol` |
| `test_raw_metric_topic_receives_data` | OTLP metric POST → `omniwatch.metrics.raw` contains `resourceMetrics` |
| `test_raw_log_topic_receives_data` | OTLP log POST → `omniwatch.logs.raw` contains `resourceLogs` |
| `test_flink_job_is_running` | At least one Flink job in `RUNNING` state |
| `test_normalized_metric_topic_has_data` | `omniwatch.metrics.normalized` has data with `entityId`/`metricName` |
| `test_normalized_log_topic_has_data` | `omniwatch.logs.normalized` has data with `severity`/`body` |
| `test_normalized_trace_topic_has_data` | `omniwatch.traces.normalized` has data with `traceId`/`spanId`/`durationMs` |
| `test_minio_bucket_has_data` | `omniwatch-telemetry-archive` has objects under `dt=` prefix, non-empty |
| `test_security_events_are_routed` | Security event → `omniwatch.security.normalized` with `attackType`, NOT in `metrics.normalized` |

---

## Monitoring Note

The Phase 2 close requires a **24-hour monitoring window** with zero Flink job
restarts. Window started **2026-08-17 ~13:40**.

The Flink job has **no** auto-submit in the compose entrypoint. If the
`flink-jobmanager` container restarts, re-submit the job manually using the
REST API steps in section (d) above.

`stream_processor.py` has been soft-deleted as `stream_processor.py.retired`
and is kept (not hard-deleted) per decision to preserve the Phase 1 reference
implementation. It is not used by any compose service or the Flink pipeline.
