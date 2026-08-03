"""
OmniWatch — Causal Graph Engine
Component: Dependency Discovery
Phase: 7
Purpose: Auto-discover service dependencies from distributed trace spans
         (ClickHouse ``omniwatch.traces``) and materialize them as Layer-1
         :CALLS edges consumable by TwoLayerGraph / Neo4j.  Parent-span
         service calls child-span service => dependency edge
         ``parent -> child`` (source depends on target), matching the
         two_layer_graph Layer-1 convention.
Inputs: ClickHouse ``omniwatch.traces`` (trace_id, span_id, parent_span_id,
        service_name, operation, duration_ms, timestamp); StorageConfig.
Outputs: DependencyEdge records aggregated per (parent, child) pair, an
         optional topology dict for TwoLayerGraph.load_layer1(), and
         Neo4j :Service nodes + :CALLS relationships via Neo4jClient.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.dependency_discovery")

_DEFAULT_WINDOW_HOURS = 6
_DEFAULT_EDGE_LIMIT = 500


@dataclass
class DependencyEdge:
    """An aggregated parent -> child service call edge."""

    source: str
    target: str
    call_count: int = 0
    latency_avg: float = 0.0
    latency_p50: float = 0.0
    latency_p95: float = 0.0
    latency_p99: float = 0.0
    error_rate: float = 0.0

    def relationship_properties(self) -> dict[str, Any]:
        """Properties carried on the :CALLS relationship (AGENTS.md)."""
        return {
            "latency_p50": round(self.latency_p50, 3),
            "latency_p95": round(self.latency_p95, 3),
            "latency_p99": round(self.latency_p99, 3),
            "error_rate": round(self.error_rate, 4),
            "call_count": self.call_count,
        }


class DependencyDiscovery:
    """Trace-driven service dependency auto-discovery.

    Queries the ClickHouse ``traces`` table joining child spans to their
    parent span within the same trace: ``parent.service_name`` called
    ``child.service_name``.  Edges are aggregated (count + latency
    percentiles) and deduplicated.  All dependencies are optional —
    unavailable ClickHouse yields an empty edge list (simulation-first
    rule), never a crash.
    """

    def __init__(self, clickhouse_client: Any | None = None, neo4j_client: Any | None = None) -> None:
        self._clickhouse = clickhouse_client
        self._neo4j = neo4j_client

    # ------------------------------------------------------------------ #
    # Trace aggregation
    # ------------------------------------------------------------------ #
    def discover_dependencies(
        self,
        window_hours: int = _DEFAULT_WINDOW_HOURS,
        limit: int = _DEFAULT_EDGE_LIMIT,
    ) -> list[DependencyEdge]:
        """Aggregate parent->child service calls from recent trace spans.

        Returns a sorted (by call_count desc) list of DependencyEdge.  When
        no ClickHouse client is configured or the query fails, returns an
        empty list and logs a warning.
        """
        client = self._clickhouse
        if client is None:
            try:
                from clickhouse_driver import Client as ChClient

                from storage.config import StorageConfig

                cfg = StorageConfig.from_env()
                client = ChClient(
                    host=cfg.clickhouse_host,
                    port=cfg.clickhouse_port,
                    user=cfg.clickhouse_user,
                    password=cfg.clickhouse_password,
                    database=cfg.clickhouse_db,
                    settings={"use_numpy": False},
                )
                self._clickhouse = client
            except Exception as exc:  # noqa: BLE001 - simulation-first fallback
                _LOG.warning("clickhouse unavailable, skipping trace discovery: %s", exc)
                return []

        window = max(1, int(window_hours))
        edge_limit = max(1, int(limit))
        sql = (
            "SELECT "
            "  parent.service_name AS source, "
            "  child.service_name AS target, "
            "  count(*) AS call_count, "
            "  avg(child.duration_ms) AS latency_avg, "
            "  quantile(0.50)(child.duration_ms) AS latency_p50, "
            "  quantile(0.95)(child.duration_ms) AS latency_p95, "
            "  quantile(0.99)(child.duration_ms) AS latency_p99 "
            "FROM omniwatch.traces child "
            "INNER JOIN omniwatch.traces parent "
            "  ON parent.trace_id = child.trace_id "
            " AND parent.span_id = child.parent_span_id "
            f"WHERE child.timestamp >= now() - INTERVAL {window} HOUR "
            "  AND child.parent_span_id != '' "
            "  AND parent.service_name != child.service_name "
            "GROUP BY parent.service_name, child.service_name "
            "ORDER BY call_count DESC "
            f"LIMIT {edge_limit}"
        )
        try:
            rows = client.execute(sql, with_column_types=False) or []
        except Exception as exc:  # noqa: BLE001 - surfaced as empty result
            _LOG.warning("trace aggregation failed, returning no edges: %s", exc)
            return []
        edges: list[DependencyEdge] = []
        for row in rows:
            (
                source,
                target,
                call_count,
                latency_avg,
                latency_p50,
                latency_p95,
                latency_p99,
            ) = row
            if not source or not target:
                continue
            edges.append(
                DependencyEdge(
                    source=str(source),
                    target=str(target),
                    call_count=int(call_count or 0),
                    latency_avg=float(latency_avg or 0.0),
                    latency_p50=float(latency_p50 or 0.0),
                    latency_p95=float(latency_p95 or 0.0),
                    latency_p99=float(latency_p99 or 0.0),
                )
            )
        _LOG.info(
            "dependency_discovery complete edges=%d window_hours=%d",
            len(edges),
            window,
        )
        return edges

    # ------------------------------------------------------------------ #
    # Topology emission (for TwoLayerGraph.load_layer1)
    # ------------------------------------------------------------------ #
    def to_topology(self, edges: list[DependencyEdge] | None = None) -> dict[str, Any]:
        """Build a get_topology()-shaped dict from discovered edges.

        Output matches ``Neo4jClient.get_topology()``: ``{"nodes": [...],
        "relationships": [{"source", "relationship_type", "properties",
        "target"}], "node_count", "relationship_count"}`` so it can be fed
        directly to ``TwoLayerGraph.load_layer1``.  Nodes carry the plain
        ``id`` (service name) form accepted by ``TwoLayerGraph._node_id``.
        """
        if edges is None:
            edges = self.discover_dependencies()
        node_ids: set[str] = set()
        relationships: list[dict[str, Any]] = []
        for edge in edges:
            node_ids.add(edge.source)
            node_ids.add(edge.target)
            relationships.append(
                {
                    "source": {"id": edge.source},
                    "relationship_type": "CALLS",
                    "properties": edge.relationship_properties(),
                    "target": {"id": edge.target},
                }
            )
        nodes = [
            {"id": node_id, "entity_type": "API_NODE", "name": node_id}
            for node_id in sorted(node_ids)
        ]
        return {
            "nodes": nodes,
            "relationships": relationships,
            "node_count": len(nodes),
            "relationship_count": len(relationships),
        }

    # ------------------------------------------------------------------ #
    # Neo4j materialization
    # ------------------------------------------------------------------ #
    def write_to_neo4j(self, edges: list[DependencyEdge] | None = None) -> int:
        """Upsert discovered edges into Neo4j as :Service + :CALLS records.

        Requires a configured Neo4jClient (passed to the constructor).
        Returns the number of relationships written; returns 0 with a
        warning when Neo4j is unavailable (simulation-first fallback).
        """
        if self._neo4j is None:
            _LOG.warning("no neo4j client configured; skipping dependency write")
            return 0
        if edges is None:
            edges = self.discover_dependencies()
        written = 0
        try:
            for edge in edges:
                self._neo4j.create_node(
                    "Service",
                    {"id": edge.source, "name": edge.source, "type": "API_NODE"},
                )
                self._neo4j.create_node(
                    "Service",
                    {"id": edge.target, "name": edge.target, "type": "API_NODE"},
                )
                result = self._neo4j.create_relationship(
                    edge.source,
                    "CALLS",
                    edge.target,
                    edge.relationship_properties(),
                )
                if result is not None:
                    written += 1
        except Exception as exc:  # noqa: BLE001 - surfaced as StorageError by client
            _LOG.warning("neo4j dependency write failed after %d edges: %s", written, exc)
            return written
        _LOG.info("dependency_write_neo4j relationships=%d", written)
        return written


def discover_and_emit(
    clickhouse_client: Any | None = None,
    neo4j_client: Any | None = None,
    window_hours: int = _DEFAULT_WINDOW_HOURS,
) -> dict[str, Any]:
    """One-shot: discover trace dependencies and return the topology dict.

    Convenience entry point for causal_engine.py and the E2E test — returns
    the get_topology()-shaped dict (empty when no traces exist) and writes
    to Neo4j when a client is provided.
    """
    discovery = DependencyDiscovery(
        clickhouse_client=clickhouse_client,
        neo4j_client=neo4j_client,
    )
    edges = discovery.discover_dependencies(window_hours=window_hours)
    if neo4j_client is not None:
        discovery.write_to_neo4j(edges)
    return discovery.to_topology(edges)
