"""
OmniWatch — Phase 7 E2E Test Scenarios

18/* — Causal Graph Engine end-to-end scenarios for the CausalEngine
(e.g. normal operation, database cascade root cause, layer-2 causal chain,
canonical id resolution, Kafka producer publish, degraded fallback, health).

No Docker / Kafka / ClickHouse / Neo4j / PyRCA required — all infrastructure is
mocked. The engine builds its graph from explicit topology + adjacency fixtures
passed to `analyze_signal`/`process_signal`, so no storage dependency is hit.

Run:
    python -m pytest tests/phase-7-e2e/ -v -W error::DeprecationWarning
"""

from __future__ import annotations

import asyncio

import pytest

import causal.causal_engine as causal_engine_module
from causal.causal_engine import CausalEngine, health, set_graph_ready

# Topology for the database_cascade scenario: api-gateway CALLS order-service
# READS_FROM postgresql-database (matches AGENTS.md integration scenarios).
DATABASE_CASCADE_TOPOLOGY: dict = {
    "nodes": [
        {"id": "api-gateway", "entity_type": "API_NODE"},
        {"id": "order-service", "entity_type": "API_NODE"},
        {"id": "postgresql-database", "entity_type": "DATABASE_NODE"},
    ],
    "relationships": [
        {
            "source": {"id": "api-gateway"},
            "target": {"id": "order-service"},
            "relationship_type": "CALLS",
            "properties": {},
        },
        {
            "source": {"id": "order-service"},
            "target": {"id": "postgresql-database"},
            "relationship_type": "READS_FROM",
            "properties": {},
        },
    ],
}

# Layer-2 causal chain: a -> b -> c -> d (root at "a", symptom at "d").
LAYER2_ADJACENCY: dict = {"a": ["b"], "b": ["c"], "c": ["d"]}

# Canonical-resolution topology: a lone node whose identity is the canonical id.
CANONICAL_TOPOLOGY: dict = {
    "nodes": [{"id": "gcp:us:API_NODE:svc-x", "entity_type": "API_NODE"}],
    "relationships": [],
}


def make_anomaly_signal(
    entity_id: str,
    *,
    entity_type: str = "API_NODE",
    metric_name: str = "error_rate",
    anomaly_score: float = 0.9,
    confidence: float = 90.0,
    source_type: str = "performance",
) -> dict:
    """Build a full AnomalySignal-shaped dict matching the AGENTS.md contract."""
    return {
        "entity_id": entity_id,
        "entity_type": entity_type,
        "metric_name": metric_name,
        "anomaly_score": anomaly_score,
        "confidence": confidence,
        "timestamp": "2026-01-01T00:00:00+00:00",
        "deviation_from_baseline": 0.5,
        "source_type": source_type,
    }


@pytest.fixture(autouse=True)
def _reset_engine_state() -> None:
    """Reset module-level graph/incident state before each test."""
    set_graph_ready(False)
    causal_engine_module._last_incident = "none"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 1: Normal operation — no topology -> degraded, root = symptom
# ─────────────────────────────────────────────────────────────────────────────
class TestNormalOperation:
    def test_no_topology_is_degraded(self, engine: CausalEngine) -> None:
        signal = make_anomaly_signal("svc-unknown", source_type="performance")

        # topology=None -> discover_and_emit() returns empty topology offline.
        incident = engine.analyze_signal(signal)

        assert incident["root_cause_entity"] == "svc-unknown"
        assert incident["confidence"] == 0.0
        assert incident["fault_path"] == ["svc-unknown"]
        assert incident["impacted_count"] == 1


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 2: Database cascade — root cause = postgresql-database
# ─────────────────────────────────────────────────────────────────────────────
class TestDatabaseCascade:
    def test_db_symptom_roots_at_database(
        self, engine: CausalEngine
    ) -> None:
        signal = make_anomaly_signal(
            "postgresql-database",
            entity_type="DATABASE_NODE",
            metric_name="error_rate",
        )

        incident = engine.analyze_signal(
            signal,
            topology=DATABASE_CASCADE_TOPOLOGY,
            adjacency={},
            metrics={"postgresql-database": 2.0},
            log_snippets=["restarting postgresql-database"],
            related_anomalies=[signal],
        )

        # The DB is both symptom and root (no upstream causal edge).
        assert incident["root_cause_entity"] == "postgresql-database"
        assert incident["entity_type"] == "DATABASE_NODE"
        assert "postgresql-database" in incident["fault_path"]
        assert incident["impacted_count"] >= 1
        assert incident["confidence"] >= 0.0

    def test_api_gateway_symptom_traces_to_db(
        self, engine: CausalEngine
    ) -> None:
        signal = make_anomaly_signal("api-gateway")

        incident = engine.analyze_signal(
            signal, topology=DATABASE_CASCADE_TOPOLOGY, adjacency={}
        )

        # Backward DAG traversal from api-gateway walks upstream toward the DB.
        assert incident["root_cause_entity"] == "postgresql-database"
        assert incident["fault_path"][-1] == "api-gateway"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 3: Layer-2 causal chain — symptom d -> root a
# ─────────────────────────────────────────────────────────────────────────────
class TestLayer2CausalChain:
    def test_symptom_d_roots_to_a(self, engine: CausalEngine) -> None:
        signal = make_anomaly_signal("d")

        incident = engine.analyze_signal(
            signal, topology=DATABASE_CASCADE_TOPOLOGY, adjacency=LAYER2_ADJACENCY
        )

        assert incident["root_cause_entity"] == "a"
        assert incident["fault_path"] == ["a", "b", "c", "d"]
        assert incident["impacted_count"] == 4
        assert incident["confidence"] > 0.0


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 4: Canonical id resolution — raw-42 -> gcp:us:API_NODE:svc-x
# ─────────────────────────────────────────────────────────────────────────────
class TestCanonicalResolution:
    def test_raw_id_maps_to_canonical_node(
        self, engine: CausalEngine
    ) -> None:
        signal = make_anomaly_signal(
            "raw-42",
            entity_type="API_NODE",
            metric_name="cpu",
            anomaly_score=0.8,
            source_type="performance",
        )
        # to_canonical reads name/entity_type/cloud_provider/region from signal.
        signal["name"] = "svc-x"
        signal["cloud_provider"] = "gcp"
        signal["region"] = "us"

        incident = engine.analyze_signal(
            signal, topology=CANONICAL_TOPOLOGY, adjacency={}
        )

        # `raw-42` (not a graph node) -> canonical id matched to the lone node.
        assert incident["root_cause_entity"] == "gcp:us:API_NODE:svc-x"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 5: Kafka producer publishes exactly once
# ─────────────────────────────────────────────────────────────────────────────
class TestProducerPublish:
    def test_process_signal_publishes_once(self, engine: CausalEngine) -> None:
        signal = make_anomaly_signal("d")

        engine.process_signal(
            signal,
            topology=DATABASE_CASCADE_TOPOLOGY,
            adjacency=LAYER2_ADJACENCY,
        )

        assert engine._producer is not None
        engine._producer.publish.assert_called_once()
        published = engine._producer._published
        assert len(published) == 1
        assert published[0]["root_cause_entity"] == "a"

    def test_process_signal_returns_incident(self, engine: CausalEngine) -> None:
        signal = make_anomaly_signal("postgresql-database", entity_type="DATABASE_NODE")

        incident = engine.process_signal(
            signal, topology=DATABASE_CASCADE_TOPOLOGY, adjacency={}
        )

        assert incident is not None
        assert incident["root_cause_entity"] == "postgresql-database"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 6: Producer unavailable -> incident still returned (simulation-first)
# ─────────────────────────────────────────────────────────────────────────────
class TestProducerFallback:
    def test_no_producer_still_returns_incident(self, engine: CausalEngine) -> None:
        engine._producer = None  # force producer-unavailable path

        signal = make_anomaly_signal("d")

        incident = engine.process_signal(
            signal,
            topology=DATABASE_CASCADE_TOPOLOGY,
            adjacency=LAYER2_ADJACENCY,
        )

        assert incident is not None
        assert incident["root_cause_entity"] == "a"


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 7: Health endpoint contract
# ─────────────────────────────────────────────────────────────────────────────
class TestHealth:
    def test_health_contract(self, engine: CausalEngine) -> None:
        body = asyncio.run(health())

        assert set(body.keys()) == {"status", "kafka", "graph_ready", "last_incident"}
        assert body["status"] in {"healthy", "degraded"}
        # Kafka may be reachable (full stack) or not (standalone test)
        assert isinstance(body["kafka"], bool)
        assert body["last_incident"] == "none"  # nothing processed yet

    def test_graph_ready_true_after_build(self, engine: CausalEngine) -> None:
        engine.build_graph(topology=DATABASE_CASCADE_TOPOLOGY, adjacency={})

        body = asyncio.run(health())

        assert body["graph_ready"] is True
        # Status is "healthy" only if BOTH kafka_ok AND graph_ready are True
        expected_status = "healthy" if body["kafka"] else "degraded"
        assert body["status"] == expected_status


# ─────────────────────────────────────────────────────────────────────────────
# Scenario 8: Lifecycle — close() is idempotent, producer torn down
# ─────────────────────────────────────────────────────────────────────────────
class TestLifecycle:
    def test_close_idempotent(self, engine: CausalEngine) -> None:
        engine.close()
        assert engine._producer is None

        # Second close must not raise.
        engine.close()
        assert engine._producer is None

    def test_last_incident_set_on_process(self, engine: CausalEngine) -> None:
        engine.process_signal(
            make_anomaly_signal("d"),
            topology=DATABASE_CASCADE_TOPOLOGY,
            adjacency=LAYER2_ADJACENCY,
        )

        body = asyncio.run(health())
        # "a at <ISO timestamp>" -> starts with the root cause entity.
        assert body["last_incident"].startswith("a at ")