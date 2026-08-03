# Phase 7 — Causal Graph Engine

**Status:** Completed
**Date:** 2026-08-03
**Tasks:** Wave 1-3 (10 Python modules, Docker, k8s, 12 E2E tests, PyRCA docker validation)

---

## Overview

Phase 7 implements the Causal Graph Intelligence Engine of the OmniWatch AIOps
platform. This layer converts detected anomalies into causal root-cause
diagnoses: it learns the dependency graph of monitored entities with PyRCA's
PC algorithm, walks the graph with RandomWalk scoring to rank root-cause
candidates, enriches the diagnosis with temporal/entity context, and publishes
structured `RootCauseObject` incidents downstream. It sits between the
Predictive Intelligence layer (Phase 6) and the Incident Prioritization Engine
(Phase 8), consuming `omniwatch.anomalies.detected` and publishing
`omniwatch.incidents.causal`.

---

## What Was Built

### Causal Engine Pipeline

| Component | File | Purpose |
|-----------|------|---------|
| PyRCAAdapter | `py_rca_adapter.py` | PyRCA (sfr-pyrca 1.0.1) integration: PC causal discovery (`discover_graph` -> adjacency), RandomWalk root-cause scoring (`find_root_causes`), correlation fallback, lazy imports |
| CausalEngine | `causal_engine.py` | Pipeline orchestrator: anomaly intake -> graph discovery -> root cause -> incident build -> publish. FastAPI app on port 8008 with `/health` |
| DagTraversal | `dag_traversal.py` | Backward BFS root-cause traversal over the learned causal DAG |
| TwoLayerGraph | `two_layer_graph.py` | Two-layer dependency graph (entity layer + metric layer) construction |
| TemporalCausalModel | `temporal_causal_model.py` | Time-aware causal weighting across anomaly timestamps |
| CrossCloudMapper | `cross_cloud_mapper.py` | Canonical entity ID mapping `{provider}:{region}:{entity_type}:{name}`, default provider gcp |
| DependencyDiscovery | `dependency_discovery.py` | Dependency edge discovery from entity metadata and graph structure |
| RootCauseBuilder | `root_cause_builder.py` | Assembles the flat `RootCauseObject` per AGENTS.md contract |
| CausalConsumer | `causal_consumer.py` | Kafka consumer of `omniwatch.anomalies.detected` (AnomalySignal) |
| CausalProducer | `causal_producer.py` | Kafka producer of `omniwatch.incidents.causal` (RootCauseObject) |

### Configuration

| Component | File | Purpose |
|-----------|------|---------|
| Settings | `config/settings.py` | Pydantic Settings: Kafka, causal thresholds (max_depth 10, min_confidence 0.3) |
| Causal Rules | `config/causal_rules.yaml` | PC algorithm + RandomWalk parameters, cross-cloud canonical ID template |

### Infrastructure

| Component | Location | Purpose |
|-----------|----------|---------|
| Dockerfile | `causal/Dockerfile` | Two-stage `python:3.10-slim` build, repo-root context, javabridge/JDK workaround, port 8008, HEALTHCHECK curl /health |
| K8s ConfigMap | `k8s/causal/configmap.yaml` | All env vars (KAFKA_BOOTSTRAP_SERVERS=kafka:9092, etc.), namespace omniwatch |
| K8s Deployment | `k8s/causal/deployment.yaml` | Causal engine deployment, port 8008 |
| K8s Service | `k8s/causal/service.yaml` | ClusterIP service, port 8008 |

### Test Suite

| File | Tests | Purpose |
|------|-------|---------|
| `tests/phase-7-e2e/conftest.py` | fixture + autouse reset | Engine state reset (`set_graph_ready(False)`, `_last_incident = "none"`), MagicMock producer capture |
| `tests/phase-7-e2e/test_causal_engine.py` | 12 tests | Full E2E: graph discovery, root cause, incident publish contract, dedup, confidence gating |

---

## Key Decisions

### PyRCA Locked to sfr-pyrca (Never `pyrca`)

The `pyrca` name on PyPI is a squatted/typo-squat package. The correct library
is `sfr-pyrca==1.0.1` (Salesforce fork), pinned in `causal/requirements.txt`.
All PyRCA imports are lazy (inside functions) because host Python 3.14 has no
compatible scikit-learn (<1.2 pin = no cp314 wheels) — PyRCA only runs inside
the Python 3.10 Docker image.

### javabridge/JDK Install Trap (Dockerfile Workaround)

`sfr-pyrca==1.0.1`'s wheel METADATA declares `Requires-Dist: javabridge>=1.0.11`
(used only by Java-based analyzers we never import). Without a JDK, pip tries
to build javabridge from source and fails (`Error finding javahome`). The
Dockerfile builder stage installs PyRCA with `--no-deps` first, then installs
the rest of the requirements WITHOUT the sfr-pyrca line:
```
grep -v '^sfr-pyrca' requirements.txt > /tmp/requirements-nopca.txt
pip install --no-cache-dir --prefix=/install --no-deps sfr-pyrca==1.0.1
pip install --no-cache-dir --prefix=/install -r /tmp/requirements-nopca.txt
```
A plain `pip install -r requirements.txt` still re-resolves javabridge (pip 23+
"completes" missing deps of an already-installed package), so the grep filter
is required. PC/RandomWalk/Bayesian analyzers never import javabridge, so the
remaining "requires javabridge, not installed" warning is benign.

### Python 3.10-slim Image

PyRCA pins `scikit-learn<1.2`, which ships no cp314 wheels, so the causal image
MUST be `python:3.10-slim` (NOT 3.14 like predictive). Build context is repo
root with `COPY storage/ ./storage/` for shared-module access.

### Correlation Fallback for RandomWalk NaN

PyRCA's RandomWalk crashes with `ValueError: probabilities contain NaN` when
the anomalous metric is the graph ROOT (no incoming edges: successor weights
and self-weight are all 0 -> `sum(w)=0`). The adapter wraps the scorer in
`_random_walk_scores()` and falls back to `_correlation_fallback()` (max-abs
Pearson correlation ranking over the learned graph) which returns the same
`SimpleNamespace(root_cause_nodes, root_cause_paths)` shape, keeping the public
contract unchanged. Validated in Docker: fallback returns deterministic ranked
candidates.

### PC Orientation Ambiguity

PC cannot deterministically orient a simple chain (Markov equivalence:
`a->b->c` ≡ `c->b->a` — both share one PDAG). It CAN deterministically orient
a collider/v-structure `a->c<-b`. Validation uses collider data so the learned
orientation is checkable.

### pydantic-settings Gap Fixed

Container smoke test failed with `ModuleNotFoundError: pydantic_settings`
(config/settings.py uses `BaseSettings`). Added `pydantic-settings>=2.1.0` to
`causal/requirements.txt`; rebuild + smoke test then passed (container
`healthy`).

### Flat RootCauseObject Contract

Incidents are published with FLAT keys per AGENTS.md:
`incident_id, root_cause_entity, entity_type, confidence, anomaly_score,
fault_path, impacted_services, impacted_count, evidence{metrics,
log_snippets, anomaly_timeline}, timestamp` — consumed by prioritization (P8)
and orchestration (P9).

---

## Test Results

| Suite | Tests | Result | Notes |
|-------|-------|--------|-------|
| `tests/phase-7-e2e/` | 12 | 12 passed in 4.43s, 0 warnings | `-W error::DeprecationWarning` gate (Python 3.14.0, pytest 9.0.2) |
| `ruff check causal/` | — | All checks passed | 0 errors, 0 warnings |
| `py_compile` (10 modules) | — | PY_COMPILE_OK | All causal modules compile |
| Docker build | — | SUCCESS (~56s) | `docker build -f causal/Dockerfile -t omniwatch/causal:test .` |
| Container smoke test | — | healthy | Uvicorn on 0.0.0.0:8008, HEALTHCHECK passes |
| PyRCA docker validation | — | PYRCA_VALIDATION_OK | Collider discovery (metric_a->metric_c, metric_b->metric_c) + RandomWalk roots + correlation fallback |

**Total: 0 errors, 0 warnings across all runnable suites.**

---

## Known Limitations

1. **PyRCA runs only in Docker**: host Python 3.14 cannot install
   scikit-learn<1.2 (no cp314 wheels). All PyRCA imports are lazy; the host
   gate is ruff + py_compile + pytest with the adapter mocked.

2. **javabridge never installed**: `sfr-pyrca` reports a missing optional dep.
   Benign — PC/RandomWalk/Bayesian never import it; Java-based analyzers
   (epitree, etc.) are out of scope.

3. **Chain orientation ambiguity**: PC's orientation of simple chains is
   arbitrary (Markov equivalence). Collider structures orient deterministically.
   In production, temporal causal modeling + cross-cloud context disambiguate.

4. **RandomWalk NaN on root anomaly**: handled via correlation fallback
   (deterministic ranking), not by fixing PyRCA internals (pinned dependency).

---

## Acceptance Criteria

| # | Criterion | Status |
|---|-----------|--------|
| 1 | PyRCAAdapter discovers causal DAG via PC algorithm (adjacency DataFrame) | Met |
| 2 | `find_root_causes` returns `(root_entities, scores, fault_paths)` per contract | Met |
| 3 | Correlation fallback engages when RandomWalk scores fail (NaN) | Met |
| 4 | CausalEngine consumes `omniwatch.anomalies.detected` | Met |
| 5 | RootCauseObject published to `omniwatch.incidents.causal` with flat keys | Met |
| 6 | Docker image builds on python:3.10-slim with javabridge workaround | Met |
| 7 | Container healthcheck reports healthy on port 8008 | Met |
| 8 | K8s manifests (configmap, deployment, service) created | Met |
| 9 | E2E test suite: 12 tests passing, 0 warnings | Met |
| 10 | ruff 0 errors / 0 warnings on causal/ | Met |
| 11 | PyRCA validated in Docker: PYRCA_VALIDATION_OK (collider + fallback) | Met |
| 12 | Commit pushed to origin/main, working tree clean | Met |

---

## Git Commit Record

| Commit | Message |
|--------|---------|
| `e99d1a0` | phase7: causal — add causal graph engine (PyRCA DAG, root cause, k8s, e2e tests, docker validation) (21 files) |

---

## How to Run

```bash
# Docker image
docker build -f causal/Dockerfile -t omniwatch/causal:latest .

# Local dev (host Python 3.10+ with PyRCA installed)
uvicorn causal_engine:app --host 0.0.0.0 --port 8008   # from causal/

# Health check
curl http://localhost:8008/health

# Full test suite (from repo root, host gate)
python -m pytest tests/phase-7-e2e/ -v -W error::DeprecationWarning
```

Requires Kafka (`omniwatch.anomalies.detected`), ClickHouse, and Neo4j
reachable per `config/settings.py` env vars (docker-compose / k8s namespace
`omniwatch` provide them).

---

## Next Phase

**Phase 8 — Incident Prioritization** (`prioritization/`): consumes
`omniwatch.incidents.causal`, computes severity P1-P4, business impact score,
SLA breach risk, and deduplication (GAP 3: alert-storm grouping by root-cause
entity within 5 min), then publishes `omniwatch.incidents.created`.
