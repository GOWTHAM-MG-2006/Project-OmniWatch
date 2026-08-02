# Predictive Intelligence Layer

Phase 6 of OmniWatch. Proactive anomaly detection and security signal
classification for cloud-native environments.

## What It Does

This service consumes windowed feature vectors from ClickHouse (Phase 4 output)
and security events from Kafka, runs multi-algorithm anomaly detection, and
publishes confirmed anomalies to Kafka and ClickHouse for downstream processing
(prioritization, causal analysis, remediation).

## Components

| Component | Purpose |
|-----------|---------|
| AnomalyDetector | IsolationForest + Z-Score + Seasonal Naive detection |
| AdaptiveThresholder | Welford's online algorithm for per-entity baselines |
| NoiseFilter | Transient spike suppression with cascade awareness |
| SignalEnricher | Neo4j entity context enrichment |
| FeatureReader | ClickHouse feature_vectors reader |
| DetectorEngine | Full pipeline orchestrator (detect -> threshold -> filter -> enrich -> publish) |
| SecuritySignalClassifier | GAP 1: brute force, config drift, privilege escalation, data exfiltration |
| AnomalyProducer | Kafka + ClickHouse dual output with circuit-breaker |
| FastAPI Health | `/health` endpoint on port 8007 |

## Inputs

| Source | Topic / Table | Description |
|--------|--------------|-------------|
| ClickHouse | `feature_vectors` | Windowed feature vectors from Phase 4 Feature Store |
| Kafka | `omniwatch.security.events` | Security events for the classifier |

## Outputs

| Destination | Topic / Table | Description |
|-------------|--------------|-------------|
| Kafka | `omniwatch.anomalies.detected` | Confirmed anomaly signals (performance + security) |
| ClickHouse | `anomalies` | Persisted anomaly records with enrichment |

## Dependencies

- `kafka-python-ng` provides the top-level `kafka` namespace. Do NOT install
  the legacy `kafka-python` package. Imports use lazy `from kafka import ...`
  inside functions to avoid module-level failures on Python 3.14.
- `storage/` module (shared ClickHouse, Neo4j, MinIO clients) must be available
  at runtime. The Docker build context is set to repo root (`.`) so
  `COPY storage/ ./storage/` works.

## How to Run

### Docker Compose

```bash
docker-compose up -d predictive
```

### Health Check

```bash
curl.exe http://localhost:8007/health
```

Expected response:

```json
{
  "status": "healthy",
  "kafka": true,
  "clickhouse": true,
  "model_loaded": false,
  "last_anomaly": "none"
}
```

> Note: `model_loaded` reports `false` until a trained `.joblib` model file
> exists. The detector trains online after accumulating cold-start samples
> (default 30).

### Standalone (development)

```bash
python -m predictive.main
```

Runs the FastAPI server on `0.0.0.0:8007`.

## Environment Variables

All variables have defaults matching `docker-compose.yml`. Override via
environment or `.env` file at repo root.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| `CLICKHOUSE_PORT` | `8123` | ClickHouse HTTP port |
| `CLICKHOUSE_DB` | `omniwatch` | ClickHouse database |
| `PREDICTIVE_ANOMALY_SCORE_THRESHOLD` | `0.7` | Minimum anomaly score (0.0-1.0) |
| `PREDICTIVE_CONFIDENCE_THRESHOLD` | `60.0` | Minimum confidence % (0-100) |
| `PREDICTIVE_COLD_START_SAMPLE_COUNT` | `30` | Samples before baseline activates |
| `PREDICTIVE_SEASONALITY_PERIOD` | `24` | Seasonality period in data-point units |
| `PREDICTIVE_SECURITY_ENABLED` | `true` | Enable security signal classifier |

## Service Endpoints

Add port 8007 to the AGENTS.md Service Endpoints reference table:

| Service | URL | Credentials |
|---------|-----|-------------|
| Predictive Health | http://localhost:8007 | none |

## Tests

```bash
python -m pytest tests/phase-6-e2e/ -v
```

32 tests across 14 scenario classes. Requires pytest configured via
`pyproject.toml` (filterwarnings gate, not `-W` CLI flag).

## Project Structure

```
predictive/
  main.py                      # FastAPI health server (port 8007)
  anomaly_detector.py           # IsolationForest + Z-Score + Seasonal Naive
  adaptive_thresholder.py       # Welford's online baselines
  noise_filter.py               # Transient spike suppression
  signal_enricher.py            # Neo4j entity context
  feature_reader.py             # ClickHouse feature_vectors reader
  anomaly_producer.py           # Kafka + ClickHouse output
  detector_engine.py            # Pipeline orchestrator
  config/
    settings.py                 # Pydantic Settings
    detection_rules.yaml        # Per-metric thresholds
    security_rules.yaml         # Security attack thresholds
  security/
    security_signal_classifier.py  # Main GAP 1 classifier
    brute_force_detector.py        # >=10 auth failures / 5min
    config_drift_detector.py       # Unauthorized config changes
    priv_escalation_detector.py    # sudo/su/escalat/role_change
    data_exfil_detector.py         # Outbound traffic spikes
    evidence_aggregator.py         # Ring buffer evidence logs
  Dockerfile
  requirements.txt
```
