"""
OmniWatch — Predictive Intelligence Layer
Component: Signal Enricher unit tests
Phase: 6
Purpose: Verify enrich() merges Neo4j context and degrades gracefully
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

from typing import Any, Dict, List

import pytest

from predictive.signal_enricher import SignalEnricher


class _FakeNeo4jClient:
    """Minimal stand-in for storage.neo4j.client.Neo4jClient.

    ``nodes`` is the topology node list returned by ``get_topology()``.
    ``raise_on_get_topology`` simulates an unreachable graph.
    """

    def __init__(
        self,
        nodes: List[Dict[str, Any]],
        raise_on_get_topology: bool = False,
    ) -> None:
        self._nodes = nodes
        self._raise = raise_on_get_topology

    def get_topology(self) -> Dict[str, Any]:
        if self._raise:
            raise RuntimeError("Neo4j unreachable")
        return {"nodes": self._nodes, "relationships": [], "node_count": len(self._nodes)}


def _node(
    node_id: str,
    name: str,
    type_: str,
    criticality: str,
    anomaly_score: float,
    last_seen: str,
) -> Dict[str, Any]:
    return {
        "properties": {
            "id": node_id,
            "name": name,
            "type": type_,
            "criticality": criticality,
            "anomaly_score": anomaly_score,
            "last_seen": last_seen,
        }
    }


def _signal(entity_id: str) -> Dict[str, Any]:
    return {
        "entity_id": entity_id,
        "entity_type": "DATABASE_NODE",
        "metric_name": "cpu_usage",
        "anomaly_score": 0.9,
        "confidence": 85.0,
        "timestamp": "2026-08-02T00:00:00Z",
        "deviation_from_baseline": 2.4,
        "source_type": "performance",
    }


class TestSignalEnricherEnrich:
    def test_entity_in_neo4j_is_enriched(self) -> None:
        client = _FakeNeo4jClient(
            nodes=[
                _node(
                    "postgresql-database",
                    "postgresql",
                    "DATABASE_NODE",
                    "high",
                    0.95,
                    "2026-08-02T00:00:00Z",
                )
            ]
        )
        enricher = SignalEnricher(neo4j_client=client, timeout=2.0)

        result = enricher.enrich(_signal("postgresql-database"))

        assert result["enriched"] is True
        assert result["entity_context"] == {
            "name": "postgresql",
            "type": "DATABASE_NODE",
            "criticality": "high",
            "anomaly_score": 0.95,
            "last_seen": "2026-08-02T00:00:00Z",
        }
        # Original signal fields are preserved.
        assert result["metric_name"] == "cpu_usage"
        assert result["anomaly_score"] == 0.9

    def test_missing_entity_is_not_enriched(self) -> None:
        client = _FakeNeo4jClient(
            nodes=[
                _node(
                    "order-service",
                    "order-service",
                    "API_NODE",
                    "medium",
                    0.1,
                    "2026-08-02T00:00:00Z",
                )
            ]
        )
        enricher = SignalEnricher(neo4j_client=client, timeout=2.0)

        result = enricher.enrich(_signal("does-not-exist"))

        assert result["enriched"] is False
        assert "entity_context" not in result
        assert result["entity_id"] == "does-not-exist"

    def test_unreachable_neo4j_is_not_enriched(self) -> None:
        client = _FakeNeo4jClient(nodes=[], raise_on_get_topology=True)
        enricher = SignalEnricher(neo4j_client=client, timeout=2.0)

        result = enricher.enrich(_signal("postgresql-database"))

        assert result["enriched"] is False
        assert "entity_context" not in result

    def test_missing_entity_id_is_not_enriched(self) -> None:
        client = _FakeNeo4jClient(nodes=[])
        enricher = SignalEnricher(neo4j_client=client, timeout=2.0)

        result = enricher.enrich({"metric_name": "cpu_usage"})

        assert result["enriched"] is False
        assert "entity_context" not in result

    def test_input_signal_is_not_mutated(self) -> None:
        client = _FakeNeo4jClient(
            nodes=[
                _node(
                    "postgresql-database",
                    "postgresql",
                    "DATABASE_NODE",
                    "high",
                    0.95,
                    "2026-08-02T00:00:00Z",
                )
            ]
        )
        enricher = SignalEnricher(neo4j_client=client, timeout=2.0)
        original = _signal("postgresql-database")

        enricher.enrich(original)

        assert "enriched" not in original
        assert "entity_context" not in original

    def test_timeout_degrades_gracefully(self) -> None:
        class _SlowClient:
            def get_topology(self) -> Dict[str, Any]:
                import time

                time.sleep(5.0)
                return {"nodes": [], "relationships": [], "node_count": 0}

        enricher = SignalEnricher(neo4j_client=_SlowClient(), timeout=0.1)

        result = enricher.enrich(_signal("postgresql-database"))

        assert result["enriched"] is False
        assert "entity_context" not in result