# Phase 6 — Predictive Redesign

**Status:** Completed
**Date:** 2026-08-04
**Tasks:** T1-T11, F1-F4 (12 implementation tasks + 4 verification wave tasks)

---

## Overview

Phase 6 Predictive Redesign hardens the predictive intelligence layer built in the
original Phase 6. The work adds Bayesian fusion, drift detection (CUSUM + ADWIN),
seasonal decomposition, anomaly session tracking, Kubernetes event integration, and
a critical entity_id provenance fix that unblocked the full detection pipeline.

This redesign sits on top of the existing anomaly detection infrastructure
(IsolationForest, Z-Score, Seasonal Naive) and the Security Signal Classifier
(GAP 1). No new Kafka topics or storage schemas were introduced.

---

## What Was Built

### New Modules

| Component | File | Purpose |
|-----------|------|---------|
| BayesianFusionEngine | `fusion.py` | Platt-scaled logistic regression fusion of per-detector scores into a single calibrated probability. Temperature-scaled logits before sigmoid. Fallback to weighted mean when unfitted. |
| ColdStartAwareFusion | `fusion.py` | Wraps BayesianFusionEngine with `confidence = min(1.0, n_samples / 100)`. Exposes `confidence` property for downstream consumers. |
| CUSUMDetector | `drift.py` | Standard two-sided CUSUM for slow-ramp detection. Detects drift when cumulative sum exceeds `drift_threshold` (default 4.0). Per-metric instance, targets from Z-Score baselines. |
| ADWINDriftDetector | `drift.py` | Bucket-based ADWIN with Hoeffding bound cut test. Fires on concept drift and sets `needs_retrain` flag until reset. Inputs scaled by 0.5 to avoid false positives on single-point outliers. |
| RobustSeasonalDetector | `seasonal.py` | Auto-period detection (12/288/2016 candidates) via lag-p Pearson autocorrelation. Median-based decomposition. Degrades to flat baseline when fewer than 2 full cycles available. |
| AnomalySessionTracker | `session_tracker.py` | Per (entity_id, metric_name) session tracking. Resolves when last 3 observations are all strictly below threshold (0.5). Tracks peak score, score history, duration. |
| K8sEventIntegration | `k8s_integration.py` | Consumes Kubernetes events (node pressure, restart, eviction) to adjust anomaly baselines. Returns 1.5x multiplier when relevant event seen within relevance window; 1.0 otherwise. 5-minute cooldown between API calls. |

### Key Changes to Existing Modules

#### `predictive/anomaly_detector.py`

**entity_id provenance fix (T1):** The scoring loop previously did `value = float(value)` before the feature-column membership check. A string `entity_id` in the feature dict crashed with `ValueError: could not convert string to float: 'postgresql-database'`. Fix: moved the `if metric not in self._feature_cols: continue` guard before the `float()` conversion so metadata keys are skipped entirely. The `entity_id` now reads from `feature.get("entity_id", "unknown")` instead of generating a synthetic `anomaly-{metric}-{timestamp}` ID.

**5 provenance fields added:** `detector_name`, `detector_contributions` (dict of per-component scores), `trend_direction`, `entity_anomaly_count`, `resolution_status`.

**CUSUM + ADWIN drift wiring (T8):** `train()` creates a CUSUMDetector and ADWINDriftDetector per metric. `detect()` feeds each metric's raw value into CUSUM and its Z-score multiplied by 0.5 into ADWIN. `_maybe_retrain()` fires `_retrain_models()` when any ADWIN reports `needs_retrain` or the periodic interval (default 3600s) elapses. `_retrain_models()` refits Z-Score baselines, IsolationForest, scaler, and seasonal history from the recent buffer, rebuilds CUSUM from new baselines, and resets all ADWIN windows.

**`primary_metric` fix:** Changed from `list(feature.keys())[0]` (which returned `entity_id` or `trend_direction`) to the first key present in `self._feature_cols`.

#### `predictive/detector_engine.py` (T7)

Wired fusion, cold-start, and session tracker into `process_message()`. Pipeline order:

1. **detect** (AnomalyDetector)
2. **fusion** (ColdStartAwareFusion.predict on detector_contributions)
3. **adaptive threshold** (AdaptiveThresholder)
4. **noise filter** (NoiseFilter)
5. **enrich** (SignalEnricher via Neo4j)
6. **publish** (AnomalyProducer to Kafka + ClickHouse)

When `signal is None` (no anomaly), calls `_track_resolution()` to check if an active session should be resolved. When an anomaly is confirmed, starts or updates the session and populates `entity_anomaly_count` and `resolution_status` on the signal.

#### `predictive/main.py` (T9)

- JSON structured logging via `JsonLogFormatter` for the `omniwatch.predictive.detection` logger.
- `/health` endpoint expanded: returns per-detector state map (`detectors`), `fusion_calibrated`, `drift_detected`, `k8s_cooldown` flags while preserving the existing contract (`status`, `kafka`, `clickhouse`, `model_loaded`, `last_anomaly`).
- Setter functions for T7/T8 to call: `record_detector_state()`, `record_engine_state()`, `bind_detector_engine()`, `log_detection_event()`.

#### `predictive/security/security_signal_classifier.py` (F2)

`bootstrap_servers` default changed from hardcoded `"localhost:9092"` to `os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")`. Reads the same env var the Pydantic settings class maps to.

### Integration Tests

| Component | File | Purpose |
|-----------|------|---------|
| Flag Combinations | `tests/test_flag_combinations.py` | Parametrized over all 8 permutations of (if_enabled, zscore_enabled, security_enabled) through the full DetectorEngine.process_message() pipeline. 17 tests. |
| E2E conftest fix | `tests/phase-6-e2e/conftest.py` | Engine fixture updated with `_fusion` and `_session_tracker` attributes that process_message() reads. |
| Fusion + Session wiring | `tests/test_detector_engine.py` | 3 tests: baseline pipeline order preserved, fusion + session wiring, session resolution flow. |

---

## Key Decisions

### ADWIN Input Scaling

Feed ADWIN `zscore * 0.5`. At delta=0.002 the Hoeffding bound for a single-point cut
(n1=1) is approximately 2.4. A lone 2.6-sigma outlier would false-fire at full scale.
Scaling by 0.5 keeps stable N(0,1) noise and single outliers up to 4.8-sigma silent,
while a sustained 10-sigma shift fires on the first drift observation.

### CUSUM Slow-Ramp Detection

Ramp of 0.5-sigma increments fires at observation 5 (s_pos accumulates as
0.25*k*(k-1), threshold 4 requires k=5). Faster than the 20-observation
limit assumed in the design spec.

### Session Resolution Rule

3 consecutive observations all strictly below 0.5 threshold. A score exactly at 0.5
is NOT considered normal. This prevents premature resolution when scores hover near
the boundary.

### Loaded-Model Immediate-Retrain Trap

With `_last_retrain_ts=0.0` after `load_model()`, the first `detect()` call would see
elapsed time far exceeding the retrain interval and refit on the single buffered
observation, silently changing anomaly scores. Fixed by resetting the timer inside
`load_model()`.

### Cold-Start Confidence Scaling

`confidence = min(1.0, n_samples / cold_start_window)` with default window of 100.
With only 4 fit samples, confidence reads 0.04. This is correct cold-start behavior,
not a bug.

---

## Test Results

| Suite | Tests | Result | Notes |
|-------|-------|--------|-------|
| `tests/phase-6-e2e/` | 32 | 32 passed, 0 warnings | E2E conftest updated with fusion + session tracker |
| `predictive/tests/` | 367 | 367 passed, 0 warnings | Unit + integration across all new and modified modules |
| **Total** | **399** | **399 passed, 0 errors, 0 warnings** | DeprecationWarning gate enforced via pyproject.toml |

### Verification Highlights

- **CUSUM detects memory_leak slow ramp:** Observed real CUSUM firing at observation 5
  during QA with `anomaly_injector.py --scenario memory_leak`.
- **entity_id propagation:** Real entity_id `"background-worker"` flows through the full
  pipeline from feature dict to anomaly signal to session tracker. No synthetic IDs.
- **Fusion calibrated score:** With 4 labeled fit samples and 3 detector contributions,
  fused score = 0.6355 (vs raw contribution mean ~0.84). Platt scaling + cold-start
  confidence both working as designed.
- **Session resolution:** 3 normal observations after an anomaly correctly resolve the
  session (`status=resolved`, `history=[0.6355, 0.0, 0.0, 0.0]`).

---

## Acceptance Criteria

| # | Criterion | Status |
|---|-----------|--------|
| 1 | entity_id reads from feature dict, not synthetic generation | Met |
| 2 | Metadata keys skipped before float() conversion (no ValueError) | Met |
| 3 | 5 provenance fields populated on every anomaly signal | Met |
| 4 | BayesianFusionEngine produces calibrated probability in [0, 1] | Met |
| 5 | ColdStartAwareFusion confidence scales with sample count | Met |
| 6 | CUSUM detects slow-ramp drift within 20 observations | Met |
| 7 | ADWIN detects concept drift and triggers retrain | Met |
| 8 | RobustSeasonalDetector auto-detects period, degrades gracefully | Met |
| 9 | Session tracker resolves after 3 consecutive normal scores | Met |
| 10 | K8s integration returns 1.5x adjustment on relevant events | Met |
| 11 | Pipeline order: detect -> fusion -> threshold -> filter -> enrich -> publish | Met |
| 12 | /health returns per-detector state + engine flags | Met |
| 13 | Structured JSON logging for detection events | Met |
| 14 | Security classifier reads KAFKA_BOOTSTRAP_SERVERS from env | Met |
| 15 | 399 tests pass, 0 errors, 0 warnings | Met |

---

## Git Commit Record

| Commit | Message |
|--------|---------|
| `2a80ad3` | phase6: predictive redesign — fusion engine, CUSUM, ADWIN, session tracking, entity_id fix |
