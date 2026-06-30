# ingestion — Telemetry Ingestion Layer (Phase 2)

## Purpose
Build the official data collection pipeline that reads from simulators
(and eventually real sources) and feeds structured data into the storage layer.

## Components

### 1. otelcol-config.yaml
OpenTelemetry Collector configuration file.
- Receives: OTLP gRPC/HTTP, Prometheus scrape
- Exports: ClickHouse (metrics/logs), Jaeger (traces), Loki (logs)
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

## Kafka Topics

| Topic | Producer | Consumer |
|-------|----------|----------|
| omniwatch.metrics.raw | ingestion/ | abstraction/ |
| omniwatch.logs.raw | ingestion/ | abstraction/ |
| omniwatch.traces.raw | ingestion/ | abstraction/ |
| omniwatch.security.events | ingestion/ | predictive/security/ |
| omniwatch.anomalies.detected | predictive/ | prioritization/ |
| omniwatch.incidents.created | prioritization/ | causal/ orchestration/ |
| omniwatch.remediation.actions | orchestration/ | learning/ dashboard/ |

## How to Run

```powershell
# Create Kafka topics
cd E:\Project-OmniWatch
simulation\.venv\Scripts\Activate.ps1
py ingestion\kafka_bus.py create-topics

# Start Telemetry Router
cd E:\Project-OmniWatch\ingestion\telemetry_router
py -m uvicorn main:app --host 0.0.0.0 --port 8001
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
