# entity-resolution — Entity Resolution Layer (Phase 3)

## Purpose
Cross-cloud entity resolution: normalizes raw entity identifiers from all five
normalized telemetry topics into canonical `UnifiedEntity` records, enriches
them with business tags, deduplicates repeated observations, and derives
service dependency relationships from trace spans.

## Pipeline

```
omniwatch.{metrics,logs,traces,events,security}.normalized
        │  (KafkaSource, JSON → TelemetryEvent)
        ├─► ResourceIdParser ─► CloudProviderMapper ─► EntityEnricher
        │        │                  │                       │
        │        └── raw id → provider/region/type/name     │
        │                  └── canonical UnifiedEntity       │
        │                           └── business tags         │
        │                                      │
        │                              keyBy(entityId)
        │                                      │
        │                              EntityDeduplicator
        │                              (5 min active window)
        │                                      │
        │                              omniwatch.entities.resolved
        │
        └─► filter(isTraceSpan) ─► keyBy(traceId) ─► RelationshipBuilder
                                                      │
                                                      └── omniwatch.entities.relationships
```

## Components

| Class | Role |
|-------|------|
| `EntityResolutionJob` | Flink job entry point; builds both branches |
| `ResourceIdParser` | Stage 1 — regex extraction of provider/region/type/name |
| `CloudProviderMapper` | Stage 2 — canonical `UnifiedEntity` + stable entity_id |
| `EntityEnricher` | Stage 3 — business tags (defaults → type → first name rule) |
| `EntityDeduplicator` | Stage 4 — keyed 5-min dedup/merge, emit only on first sight |
| `RelationshipBuilder` | Stage 5 — CALLS edges from trace parent/child spans |
| `EntityConfig` | Loads `entity_mappings.yaml` + `business_tags.yaml` |

## Inputs / Outputs

- **Inputs (Kafka):** `omniwatch.metrics.normalized`, `omniwatch.logs.normalized`,
  `omniwatch.traces.normalized`, `omniwatch.events.normalized`,
  `omniwatch.security.normalized`
- **Outputs (Kafka):**
  - `omniwatch.entities.resolved` — `UnifiedEntity` JSON
  - `omniwatch.entities.relationships` — `EntityRelationship` JSON (CALLS)
- **Outputs (MinIO):**
  - `omniwatch-telemetry-archive/entity-resolution/dt=YYYY-MM-DD-HH/entities-*.parquet`

## MinIO Output

The `EntityMinIOSink` buffers resolved entity records and periodically flushes
them as Parquet files to the `omniwatch-telemetry-archive` bucket.

| Setting | Value |
|---------|-------|
| Bucket | `omniwatch-telemetry-archive` |
| Prefix | `entity-resolution/` |
| Partitioning | `dt=yyyy-MM-dd-HH` (Hive-style, hour-level) |
| Format | Apache Parquet (Avro schema) |
| Object naming | `entities-{yyyyMMdd-HHmmss}-{uuid8}.parquet` |
| Batch size | 100 records |
| Flush interval | 5 seconds (or batch full, whichever first) |

Parquet schema fields: `entity_id` (string), `entity_type` (string),
`provider` (string), `region` (string), `name` (string),
`business_tags` (string, JSON), `raw_identifiers` (string, JSON),
`first_seen` (long), `last_seen` (long).

## Config

| Key | Env var | Default |
|-----|---------|---------|
| `kafka.brokers` | `KAFKA_BOOTSTRAP_SERVERS` | `kafka:29092` |
| `kafka.group.id` | `KAFKA_GROUP_ID` | `flink-entity-resolution` |
| `minio.endpoint` | `MINIO_ENDPOINT` | `http://minio:9010` |
| `minio.access.key` | `MINIO_ACCESS_KEY` | `minioadmin` |
| `minio.secret.key` | `MINIO_SECRET_KEY` | `minioadmin` |
| `minio.bucket` | `MINIO_BUCKET` | `omniwatch-telemetry-archive` |

Resource extraction patterns: `src/main/resources/entity_mappings.yaml`
Business tag rules: `src/main/resources/business_tags.yaml`

## Build & Test

```powershell
# Build shadow jar
gradlew.bat clean shadowJar --no-daemon

# Unit tests
gradlew.bat test --no-daemon
```

## How to Run (docker-compose)

```powershell
docker-compose up -d --build entity-resolution
```

Then submit the job to the Flink cluster:

```powershell
docker exec -it omniwatch-flink-jobmanager flink run -d /opt/flink/lib/omniwatch-entity-resolution-job.jar
```

Verify in the Flink UI: http://localhost:8081 (job `OmniWatch Entity Resolution`).
