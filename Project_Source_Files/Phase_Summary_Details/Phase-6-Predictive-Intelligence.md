# Phase 6 — Predictive Intelligence

**Status:** Completed
**Date:** 2026-08-02
**Tasks:** 1-31 (Waves A through G)

---

## Overview

Phase 6 implements the Predictive Intelligence layer of the OmniWatch AIOps
platform. This layer provides proactive anomaly detection and the GAP 1
Security Signal Classifier. It sits between the Feature Store (Phase 4) and
the Causal Graph Engine (Phase 7) in the data pipeline, consuming windowed
feature vectors from ClickHouse and security events from Kafka, then
publishing confirmed anomalies downstream.

---

## What Was Built

### Anomaly Detection Pipeline

| Component | File | Purpose |
|-----------|------|---------|
| AnomalyDetector | `anomaly_detector.py` | Multi-algorithm detector combining sklearn IsolationForest, scipy Z-Score, and statsmodels Seasonal Naive |
| AdaptiveThresholder | `adaptive_thresholder.py` | Welford's online algorithm for per-entity/metric adaptive baselines |
| NoiseFilter | `noise_filter.py` | Transient spike suppression with cascade awareness (>=3 neighbors = real incident) |
| SignalEnricher | `signal_enricher.py` | Neo4j entity context enrichment (name, type, criticality) with timeout-bounded lookups |
| AnomalyProducer | `anomaly_producer.py` | Dual output: Kafka `omniwatch.anomalies.detected` topic + ClickHouse `anomalies` table, with circuit-breaker for ClickHouse |
| FeatureReader | `feature_reader.py` | Read windowed feature vectors from ClickHouse `feature_vectors` table (Phase 4 output) |
| DetectorEngine | `detector_engine.py` | Pipeline orchestrator: detect -> adaptive threshold -> noise filter -> enrich -> publish. Thread-safe with cold-start training buffer |
| Settings | `config/settings.py` | Pydantic Settings class with all env vars (Kafka, ClickHouse, Neo4j, MinIO, predictive thresholds) |
| Detection Rules | `config/detection_rules.yaml` | Per-metric detection thresholds (YAML-driven) |

### Security Signal Classifier (GAP 1)

| Component | File | Purpose |
|-----------|------|---------|
| SecuritySignalClassifier | `security/security_signal_classifier.py` | Main Kafka consumer/producer: consumes `omniwatch.security.events`, routes to sub-detectors, produces `SecurityAnomalySignal` |
| BruteForceDetector | `security/brute_force_detector.py` | Counts auth failures per (source_ip, 5min window). Threshold: >=10 failures = BRUTE_FORCE_ATTEMPT |
| ConfigDriftDetector | `security/config_drift_detector.py` | Pattern-matches unauthorized config changes. Severity: CRITICAL |
| PrivEscalationDetector | `security/priv_escalation_detector.py` | Grep-based sudo/su/escalat/role_change detection. Skips admin entities |
| DataExfilDetector | `security/data_exfil_detector.py` | Outbound traffic spike detection (>3x rolling average) |
| EvidenceAggregator | `security/evidence_aggregator.py` | Ring buffer of last 5 evidence log lines per (entity, attack_type) |
| Security Rules | `config/security_rules.yaml` | YAML-driven thresholds for all 4 attack types |

### FastAPI Health Service

| Component | File | Purpose |
|-----------|------|---------|
| Health Server | `main.py` | FastAPI app on port 8007 with `/health` endpoint reporting Kafka, ClickHouse, model_loaded, and last_anomaly status |

### Infrastructure

| Component | Location | Purpose |
|-----------|----------|---------|
| Dockerfile | `predictive/Dockerfile` | Repo-root build context with `COPY storage/ ./storage/` for shared module access |
| docker-compose.yml | Root `docker-compose.yml` | `predictive` service with 20 env vars, port 8007, healthcheck |
| K8s ConfigMap | `k8s/predictive/configmap.yaml` | All env vars for K8s deployment |
| K8s Deployment | `k8s/predictive/deployment.yaml` | 2 replicas, resources 100m/256Mi to 500m/512Mi |
| K8s Service | `k8s/predictive/service.yaml` | ClusterIP, port 8007 |
| pytest config | `pyproject.toml` | filterwarnings gate for DeprecationWarning + asyncio |

### Test Suite

| File | Tests | Purpose |
|------|-------|---------|
| `tests/phase-6-e2e/test_predictive.py` | 32 methods across 14 scenario classes | Full E2E: cold start, detect, noise filter, adaptive threshold, enrichment, security classifier, engine pipeline |

---

## Key Decisions

### Merlion Locked Out

Salesforce Merlion (the originally planned anomaly detection library) could not
be installed. Python 3.14 compatibility issues: numpy<2.0 pin means no cp314
wheel exists. The fallback plan uses sklearn IsolationForest + scipy Z-Score +
statsmodels Seasonal Naive, which covers the same detection strategies without
the Merlion dependency.

### kafka-python-ng Namespace

`kafka-python-ng` (the maintained fork) provides the top-level `kafka` namespace
(`from kafka import KafkaConsumer`). This is NOT the legacy `kafka-python`
package. All Kafka imports use lazy import inside functions to avoid
module-level import failures on Python 3.14.

### Feature Vectors from ClickHouse

Feature vectors are read from the ClickHouse `feature_vectors` table (Phase 4
output), NOT consumed from a Kafka topic. The `FeatureReader` component wraps
`ClickHouseClient.select_by_entity` and reverses the result to ascending
chronological order.

### Security Signals Route to Anomalies Topic

Security anomaly signals are published to `omniwatch.anomalies.detected` (the
same topic as performance anomalies), NOT to a phantom
`omniwatch.security.anomalies` topic. This keeps the downstream pipeline
(prioritization, causal engine) unified.

### Port 8007

The predictive health service runs on port 8007, separate from other FastAPI
services (dashboard on 8000, feature API on 8002).

### Docker Build Context

The Docker build context was changed from `./predictive` to repo root `.` with
`COPY storage/ ./storage/` so the health check can import the shared storage
module. Without this, `clickhouse: false` was always reported because
`storage.clickhouse.client` was not in the image.

### pyproject.toml filterwarnings Gate

The `-W error::DeprecationWarning` CLI flag OVERRIDES pyproject.toml filter
entries. The solution uses pyproject.toml filterwarnings exclusively:
```toml
filterwarnings = [
    "error::DeprecationWarning",
    "ignore::DeprecationWarning:asyncio",
]
```
This gates DeprecationWarnings as errors while ignoring the asyncio deprecation
from pytest-asyncio 1.3.0 on Python 3.14.

---

## Test Results

| Suite | Tests | Result | Notes |
|-------|-------|--------|-------|
| `tests/phase-6-e2e/` | 32 | 32 passed, 0 warnings | DeprecationWarning gate enforced via pyproject.toml |
| `storage/tests/` | 31 | 31 passed, 0 warnings | Also passes with -W error::DeprecationWarning (no asyncio dep) |
| `entity-resolution/tests/` | 8 | 8 skipped | Infra-dependent (Kafka, ClickHouse, Neo4j) |
| `tests/phase-0-e2e/` | 16+1 skipped | 16 passed, 1 skipped | docker-compose, terraform, k8s manifest validation |
| `tests/phase-2-e2e/` | 9 | blocked | Live infra (OTel Collector, Kafka, Flink, MinIO) required |

**Total: 0 errors, 0 warnings across all runnable suites.**

---

## Known Limitations

1. **Phase-2 E2E blocked**: Requires live OTel Collector, Kafka, Flink, and
   MinIO pipeline running. This is an infrastructure dependency, not a code
   regression.

2. **model_loaded: false**: The health check reports `model_loaded: false` until
   a trained `.joblib` model file exists in the predictive directory. This is
   expected behavior — the detector trains online after accumulating enough
   cold-start samples.

3. **pytest-asyncio 1.3.0 on Python 3.14**: The `pytest-asyncio` plugin imports
   `asyncio.AbstractEventLoopPolicy` at load time, which is deprecated in
   Python 3.14 (removal scheduled for 3.16). The warning fires with
   `stacklevel=3` from `importlib._bootstrap`, making module-level `-W` filters
   unreliable. Handled via pyproject.toml filterwarnings (upstream issue).

---

## Acceptance Criteria

| # | Criterion | Status |
|---|-----------|--------|
| 1 | AnomalyDetector produces AnomalySignal dicts exceeding threshold | Met |
| 2 | AdaptiveThresholder uses Welford's online algorithm | Met |
| 3 | NoiseFilter suppresses transient spikes, passes cascades | Met |
| 4 | SecuritySignalClassifier detects 4 attack types (brute_force, config_drift, priv_escalation, data_exfil) | Met |
| 5 | AnomalyProducer publishes to Kafka + ClickHouse | Met |
| 6 | FeatureReader reads from ClickHouse feature_vectors | Met |
| 7 | DetectorEngine orchestrates full pipeline with cold-start | Met |
| 8 | FastAPI /health reports Kafka, ClickHouse, model_loaded, last_anomaly | Met |
| 9 | Docker build includes storage module (repo-root context) | Met |
| 10 | K8s manifests (configmap, deployment, service) created | Met |
| 11 | E2E test suite: 32 tests passing, 0 warnings | Met |
| 12 | DeprecationWarning gate in pyproject.toml (not -W flag) | Met |
| 13 | All commits pushed to origin/main, working tree clean | Met |

---

## Git Commit Record

| Commit | Message |
|--------|---------|
| `0ebd751` | phase6: predictive-intelligence — add anomaly detection, security classifier, and health service (37 files) |
| `d9db5ac` | phase6: k8s — add predictive deployment and service manifests (3 files) |
| `8d400a9` | phase6: infra — wire predictive service into compose, env, and pytest config (3 files) |
| `5670dc0` | phase6: storage — add select_metrics_baseline and anomaly schema support (3 files) |
