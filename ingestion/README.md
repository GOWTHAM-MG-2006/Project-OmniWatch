# ingestion — Telemetry Ingestion Layer (Phase 2)

## Purpose
Build the official data collection pipeline that reads from simulators
(and eventually real sources) and feeds structured data into the storage layer.

## Components

### 1. otelcol-config.yaml
OpenTelemetry Collector configuration file.
- Receives: OTLP gRPC/HTTP
- Exports: ClickHouse (metrics/logs/traces)
- Processors: batch, memory_limiter, resource detection

### 2. kafka_bus.py
Central message bus for all telemetry.
- Creates all 7 Kafka topics automatically
- Provides KafkaProducer class with send() method
- Provides KafkaConsumer class with subscribe() method
- Handles connection errors and retries

### 3. telemetry_router/
FastAPI service that receives telemetry from OTel Collector and routes to Kafka.

**Endpoints:**
- POST /ingest/metrics → omniwatch.metrics.raw
- POST /ingest/logs → omniwatch.logs.raw
- POST /ingest/traces → omniwatch.traces.raw
- POST /ingest/security → omniwatch.security.events
- GET /health → Health check
- GET /status → Service status

### 4. stream_processor.py
Kafka consumer that reads raw telemetry and converts to anomaly signals.

- **Input:**
  - `omniwatch.metrics.raw` (OTel metrics → performance `AnomalySignal`)
  - `omniwatch.logs.raw`, `omniwatch.traces.raw` (signals same topic)
  - `omniwatch.security.events` → security `SecurityAnomalySignal`
- **Processing:** OTel metric-to-anomaly conversion with configurable scoring
- **Output:** `omniwatch.anomalies.detected` → ClickHouse `anomalies` table
- **Retries:** Configurable Kafka retry with `auto_offset_reset='earliest'`
- **Docker:** Integrated via `docker-compose.yml` (service: `stream-processor`)

### 5. Stream Processing Engine (Flink)
Apache Flink cluster for windowed aggregations and streaming analytics.

- **Services:** Flink JobManager (`localhost:8081`) + TaskManager
- **Usage:** Submit Flink jobs for time-windowed anomaly detection
- **Docker:** Part of `docker-compose.yml` (services: `flink-jobmanager`, `flink-taskmanager`)

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| omniwatch.metrics.raw | ingestion/ | ingestion/ (stream_processor) |
| omniwatch.logs.raw | ingestion/ | ingestion/ (stream_processor) |
| omniwatch.traces.raw | ingestion/ | ingestion/ (stream_processor) |
| omniwatch.security.events | ingestion/ | predictive/security/ |
| omniwatch.anomalies.detected | predictive/ | prioritization/ |
| omniwatch.incidents.created | prioritization/ | causal/ orchestration/ |
| omniwatch.remediation.actions | orchestration/ | learning/ dashboard/ |

## How to Run

### Local (Python)
```powershell
# Create Kafka topics
cd E:\Project-OmniWatch
simulation\.venv\Scripts\Activate.ps1
py ingestion\kafka_bus.py create-topics

# Start Telemetry Router
cd E:\Project-OmniWatch\ingestion\telemetry_router
py -m uvicorn main:app --host 0.0.0.0 --port 8001

# Start Stream Processor (separate terminal)
cd E:\Project-OmniWatch
py ingestion\stream_processor.py
```

### Docker (all services)
```powershell
docker-compose up -d
```

## Testing

```powershell
# Test health
curl.exe http://localhost:8001/health

# Test publish
curl.exe -X POST http://localhost:8001/publish -H "Content-Type: application/json" -d '{"topic": "omniwatch.metrics.raw", "message": {"entity_id": "test", "value": 42}}'

# List topics
curl.exe http://localhost:8001/routes/topics
```

### End-to-End Tests

```
tests/phase-2-e2e/test_otel_kafka_pipeline.py
```

17 tests (9 runnable, 8 skip if Docker services unavailable):

| Category | Tests | What it validates |
|---|---|---|
| Kafka connectivity | 3 | Broker reachable, produce/consume cycle |
| Message serialization | 4 | AnomalySignal, SecurityAnomalySignal, IncidentRecord, ActionResult serde |
| Stream processing | 3 | OTel metric conversion, entity resolution, failure handling |
| Pipeline | 1 | End-to-end OTel → AnomalySignal flow |

```powershell
# Install test dependencies
pip install -r tests/phase-2-e2e/requirements-test.txt

# Run tests
py -m pytest tests/phase-2-e2e/ -v
```
