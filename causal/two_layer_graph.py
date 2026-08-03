"""
OmniWatch — Causal Graph Engine
Component: Two-Layer Graph Model
Phase: 7
Purpose: Single source of truth for the two-layer dependency/causal graph.
         Layer-1 is the static Neo4j dependency topology (:Service /
         :Database / :Infrastructure nodes with :CALLS / :READS_FROM /
         :DEPENDS_ON edges).  Layer-2 is the learned PyRCA causal DAG.
         This module merges both layers and exposes a deterministic
         fault-path interface consumed by dag_traversal.py.
Inputs: Neo4j topology dicts (get_topology / query_by_entity output) and a
        Layer-2 causal adjacency mapping (from py_rca_adapter.py).
Outputs: A merged TwoLayerGraph with layer-aware node/edge access and
         get_fault_paths() for backward BFS root-cause traversal.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.two_layer_graph")

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent / "config" / "causal_rules.yaml"

# Relationship types that define dependency direction in Layer-1.  An edge
# source -> target means "source depends on target" (traversal moves from a
# service back toward the infrastructure it depends on).
_LAYER1_REL_TYPES = {"CALLS", "READS_FROM", "DEPENDS_ON"}


@dataclass
class LayerEdge:
    """A directed edge in one of the two graph layers.

    ``source -> target``.  ``properties`` carries relationship metadata
    (latency percentiles, error rate, query_type, ...).  ``layer`` tags the
    edge origin ("1" = Neo4j dependency, "2" = PyRCA causal) so traversal
    can prefer Layer-2 evidence and fall back to Layer-1.
    """

    source: str
    target: str
    edge_type: str = "DEPENDS_ON"
    properties: dict[str, Any] = field(default_factory=dict)
    layer: str = "1"


class TwoLayerGraph:
    """Manages the merged Layer-1 + Layer-2 graph used for RCA traversal.

    Layer-1 is loaded from the Neo4j topology (see storage/neo4j/client.py
    ``get_topology``).  Layer-2 is set from the PyRCA causal DAG adjacency
    mapping produced by ``py_rca_adapter``.  ``merge()`` unions the nodes and
    keeps every Layer-2 edge; Layer-1 edges are kept only for node pairs that
    have no Layer-2 edge, so learned causality always wins over static
    dependency structure (plan Decision 8 / 12).
    """

    def __init__(self) -> None:
        self._nodes: dict[str, dict[str, Any]] = {}
        self._out_edges: dict[str, list[LayerEdge]] = {}
        self._in_edges: dict[str, list[LayerEdge]] = {}
        self._entity_types: dict[str, str] = {}
        self._max_depth: int = 10
        self._path_order: str = "root_to_symptom"
        self._load_rules()

    # ------------------------------------------------------------------ #
    # Configuration
    # ------------------------------------------------------------------ #
    def _load_rules(self) -> None:
        """Load traversal tunables from causal_rules.yaml (best effort)."""
        try:
            with open(_DEFAULT_RULES_PATH, "r", encoding="utf-8") as fh:
                rules: dict[str, Any] = yaml.safe_load(fh) or {}
        except OSError:
            _LOG.warning("causal_rules.yaml not found; using default traversal settings")
            return
        traversal = rules.get("dag_traversal", {}) or {}
        self._max_depth = int(traversal.get("max_depth", 10))
        self._path_order = str(traversal.get("path_order", "root_to_symptom"))

    @property
    def max_depth(self) -> int:
        return self._max_depth

    @property
    def path_order(self) -> str:
        return self._path_order

    # ------------------------------------------------------------------ #
    # Node helpers (defensive against driver 6.x Node objects vs dicts)
    # ------------------------------------------------------------------ #
    @staticmethod
    def _node_id(node: Any) -> str:
        """Extract a stable id from a Neo4j node (driver 6.x) or plain dict."""
        if isinstance(node, Mapping):
            candidate = node.get("id") or node.get("name") or node.get("entity_id")
            if candidate is not None:
                return str(candidate)
        # Fall back to element_id for legacy Node objects.
        element_id = getattr(node, "element_id", None) or getattr(node, "id", None)
        if element_id is not None:
            return str(element_id)
        raise StorageError(f"cannot resolve node identity from {node!r}")

    @staticmethod
    def _node_type(node: Any) -> str:
        if isinstance(node, Mapping):
            entity_type = node.get("entity_type") or node.get("type") or node.get("label")
            if entity_type is not None:
                return str(entity_type)
            labels = node.get("labels")
            if labels:
                return str(min(labels))
        labels = getattr(node, "labels", None)
        if labels:
            return str(min(labels))
        return "UNKNOWN"

    # ------------------------------------------------------------------ #
    # Layer-1 loading (Neo4j topology)
    # ------------------------------------------------------------------ #
    def load_layer1(self, topology: dict[str, Any]) -> int:
        """Load the Layer-1 dependency graph from a Neo4j topology dict.

        Accepts the exact shape returned by ``Neo4jClient.get_topology()``:
        ``{"nodes": [...], "relationships": [{"source", "target",
        "relationship_type", "properties"}], ...}``.  Nodes may be driver
        Node objects or plain dicts.  Returns the number of Layer-1 edges
        ingested.
        """
        nodes = topology.get("nodes") or []
        relationships = topology.get("relationships") or []
        for node in nodes:
            node_id = self._node_id(node)
            self._nodes.setdefault(node_id, {})
            if isinstance(node, Mapping):
                props = dict(node)
                props.pop("labels", None)
                self._nodes[node_id].update({k: v for k, v in props.items() if k != "id"})
            self._entity_types.setdefault(node_id, self._node_type(node))
        edge_count = 0
        for rel in relationships:
            source = self._node_id(rel["source"])
            target = self._node_id(rel["target"])
            rel_type = str(rel.get("relationship_type") or "DEPENDS_ON").upper()
            props = dict(rel.get("properties") or {})
            self.add_edge(source, target, edge_type=rel_type, properties=props, layer="1")
            edge_count += 1
        _LOG.info("layer1_loaded nodes=%d edges=%d", len(self._nodes), edge_count)
        return edge_count

    # ------------------------------------------------------------------ #
    # Layer-2 loading (PyRCA causal DAG)
    # ------------------------------------------------------------------ #
    def set_layer2(self, adjacency: Mapping[str, Sequence[str]] | dict[str, list[str]]) -> int:
        """Set the Layer-2 causal DAG from a PyRCA adjacency mapping.

        ``adjacency`` maps a node to its downstream causal children:
        ``{source: [target, ...]}`` — equivalent to the PyRCA graph_df
        convention ``df.loc[i, j] == 1`` meaning edge i -> j.  Every source
        and target is registered as a node; edges are tagged ``layer="2"``.
        Returns the number of Layer-2 edges ingested.
        """
        edge_count = 0
        for source, targets in (adjacency or {}).items():
            src = str(source)
            self._nodes.setdefault(src, {})
            self._entity_types.setdefault(src, "UNKNOWN")
            for target in targets or []:
                tgt = str(target)
                self._nodes.setdefault(tgt, {})
                self._entity_types.setdefault(tgt, "UNKNOWN")
                self.add_edge(src, tgt, edge_type="CAUSAL", layer="2")
                edge_count += 1
        _LOG.info("layer2_loaded edges=%d", edge_count)
        return edge_count

    # ------------------------------------------------------------------ #
    # Edge registry
    # ------------------------------------------------------------------ #
    def add_edge(
        self,
        source: str,
        target: str,
        *,
        edge_type: str = "DEPENDS_ON",
        properties: dict[str, Any] | None = None,
        layer: str = "1",
    ) -> None:
        """Register a directed edge, creating endpoints when unknown."""
        for node in (source, target):
            self._nodes.setdefault(node, {})
            self._entity_types.setdefault(node, "UNKNOWN")
        edge = LayerEdge(
            source=source,
            target=target,
            edge_type=edge_type,
            properties=dict(properties or {}),
            layer=layer,
        )
        self._out_edges.setdefault(source, []).append(edge)
        self._in_edges.setdefault(target, []).append(edge)

    def nodes(self) -> dict[str, dict[str, Any]]:
        return dict(self._nodes)

    def node_count(self) -> int:
        return len(self._nodes)

    def entity_type(self, node_id: str) -> str:
        return self._entity_types.get(node_id, "UNKNOWN")

    def outgoing_edges(self, node_id: str) -> list[LayerEdge]:
        return list(self._out_edges.get(node_id, []))

    def incoming_edges(self, node_id: str) -> list[LayerEdge]:
        return list(self._in_edges.get(node_id, []))

    def has_node(self, node_id: str) -> bool:
        return node_id in self._nodes

    # ------------------------------------------------------------------ #
    # Layer merge
    # ------------------------------------------------------------------ #
    def merge(self) -> TwoLayerGraph:
        """Return a new graph where Layer-2 edges override Layer-1.

        Every Layer-2 edge is preserved; a Layer-1 edge is kept only when
        the same (source, target) pair has no Layer-2 edge.  Layer-1 edges
        whose endpoints do not exist in Layer-2 are retained as fallback
        structure so backward BFS can still emit an incident with lower
        confidence (plan Decision 12).
        """
        merged = TwoLayerGraph()
        merged._max_depth = self._max_depth
        merged._path_order = self._path_order
        merged._nodes = {k: dict(v) for k, v in self._nodes.items()}
        merged._entity_types = dict(self._entity_types)

        causal_pairs = {
            (edge.source, edge.target)
            for edges in self._out_edges.values()
            for edge in edges
            if edge.layer == "2"
        }
        causal_undirected = {frozenset(pair) for pair in causal_pairs}
        for edges in self._out_edges.values():
            for edge in edges:
                if edge.layer == "2":
                    merged.add_edge(
                        edge.source,
                        edge.target,
                        edge_type=edge.edge_type,
                        properties=edge.properties,
                        layer="2",
                    )
                elif frozenset((edge.source, edge.target)) not in causal_undirected:
                    merged.add_edge(
                        edge.source,
                        edge.target,
                        edge_type=edge.edge_type,
                        properties=edge.properties,
                        layer="1",
                    )
        _LOG.info(
            "graph_merged nodes=%d causal_edges=%d",
            merged.node_count(),
            sum(1 for e in merged._all_edges() if e.layer == "2"),
        )
        return merged

    def _all_edges(self) -> list[LayerEdge]:
        seen: set[tuple[str, str, str]] = set()
        result: list[LayerEdge] = []
        for edges in self._out_edges.values():
            for edge in edges:
                key = (edge.source, edge.target, edge.layer)
                if key in seen:
                    continue
                seen.add(key)
                result.append(edge)
        return result

    # ------------------------------------------------------------------ #
    # Fault-path interface (consumed by dag_traversal.py)
    # ------------------------------------------------------------------ #
    def _upstream_neighbors(self, node_id: str) -> list[LayerEdge]:
        """Edges pointing from ``node_id`` toward candidate root causes.

        Layer semantics differ: a Layer-1 Neo4j dependency edge stores
        ``dependent -[:CALLS/:READS_FROM/:DEPENDS_ON]-> dependency``, so the
        fault propagates from the dependency to the dependent — backward
        traversal follows the edge's *outgoing* direction.  A Layer-2 PyRCA
        causal edge stores ``cause -> effect``, so backward traversal follows
        the edge's *incoming* direction.  Normalizing both into
        "edges toward upstream roots" keeps the BFS layer-agnostic.
        """
        upstream: list[LayerEdge] = []
        for edge in self._out_edges.get(node_id, []):
            if edge.layer == "1":
                upstream.append(edge)
        for edge in self._in_edges.get(node_id, []):
            if edge.layer == "2":
                upstream.append(edge)
        return upstream

    def get_fault_paths(self, symptom: str, max_depth: int | None = None) -> list[list[str]]:
        """Deterministically enumerate root -> symptom paths (backward DFS).

        Walks upstream from the symptom (Layer-2 causes + Layer-1
        dependencies) back toward nodes with no upstream edge (candidate
        roots), bounded by ``max_depth``.  Returns paths ordered root-first
        (``path_order: root_to_symptom``); when the config requests
        ``symptom_to_root`` the paths are reversed.  Paths are sorted by
        root id for deterministic output, and cycles are pruned via a
        per-path visited set.
        """
        depth = max_depth if max_depth is not None else self._max_depth
        merged = self.merge()
        paths: list[list[str]] = []

        def _walk(node: str, trail: list[str], visited: set[str]) -> None:
            if len(trail) > depth:
                return
            upstream = merged._upstream_neighbors(node)
            if not upstream:
                # No further upstream edge: this is a candidate root.
                paths.append(list(reversed(trail)))
                return
            for edge in sorted(upstream, key=lambda e: (e.source, e.target, e.layer)):
                neighbor = edge.target if edge.layer == "1" else edge.source
                if neighbor in visited:
                    continue
                _walk(neighbor, trail + [neighbor], visited | {neighbor})

        if symptom in self._nodes:
            _walk(symptom, [symptom], {symptom})
        paths.sort(key=lambda p: (p[0] if p else "", len(p)))
        if self._path_order == "symptom_to_root":
            return [list(reversed(p)) for p in paths]
        return paths

    def to_dict(self) -> dict[str, Any]:
        """Serialize the merged graph for diagnostics / E2E assertions."""
        merged = self.merge()
        return {
            "nodes": [
                {"id": node_id, "entity_type": self._entity_types.get(node_id, "UNKNOWN")}
                for node_id in merged.nodes()
            ],
            "edges": [
                {
                    "source": edge.source,
                    "target": edge.target,
                    "edge_type": edge.edge_type,
                    "layer": edge.layer,
                }
                for edge in merged._all_edges()
            ],
            "path_order": self._path_order,
            "max_depth": self._max_depth,
        }


def load_from_topology(topology: dict[str, Any], adjacency: Mapping[str, Sequence[str]]) -> TwoLayerGraph:
    """Build a merged TwoLayerGraph from Neo4j topology + causal adjacency.

    Convenience factory used by causal_engine.py and the E2E test:
    Layer-1 from ``get_topology()`` output, Layer-2 from the PyRCA adapter's
    adjacency mapping, merged so Layer-2 edges win.
    """
    graph = TwoLayerGraph()
    graph.load_layer1(topology)
    graph.set_layer2(adjacency)
    return graph.merge()
