"""
OmniWatch — Causal Graph Engine
Component: Backward BFS Root-Cause Traversal
Phase: 7
Purpose: Walk the merged Two-Layer graph backward from an anomalous symptom
         toward candidate root causes, score each candidate deterministically
         (Layer-2 learned causality outweighs Layer-1 dependency fallback,
         shorter paths preferred), filter by a confidence threshold, and
         return ordered fault paths (root -> ... -> symptom) with the list of
         impacted services on each path.
Inputs: A merged TwoLayerGraph (from two_layer_graph.py) and a symptom entity
        id; tunables from causal/config/causal_rules.yaml (dag_traversal:
        max_depth, min_confidence, path_order).
Outputs: Ordered RootCandidate list: root_cause_entity, entity_type,
         confidence (0..1), fault_path [root -> ... -> symptom],
         path_edges (layer-aware), impacted_services. Public analyze() dict
         consumed by root_cause_builder.py and asserted by the E2E test.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

from causal.two_layer_graph import LayerEdge, TwoLayerGraph
from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.dag_traversal")

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent / "config" / "causal_rules.yaml"

# Confidence anchors (deterministic, documented for E2E assertions):
# A path backed by learned Layer-2 causality is strong evidence; a path that
# only follows the static Layer-1 dependency topology is still creditable
# (decision 8 / 12 fallback) but weaker.
_LAYER2_CONFIDENCE = 0.85
_LAYER1_CONFIDENCE = 0.55
_SINGLE_NODE_CONFIDENCE = 0.20  # symptom is its own "root": no upstream evidence
_DEPTH_DECAY = 0.12            # per extra hop beyond the first, blended weight


@dataclass
class TraversalEdge:
    """A hop on a candidate fault path (layer-aware, for scoring/evidence)."""

    source: str
    target: str
    edge_type: str
    layer: str


@dataclass
class RootCandidate:
    """One ranked root-cause candidate produced by backward BFS."""

    root_cause: str
    entity_type: str
    confidence: float
    fault_path: list[str] = field(default_factory=list)      # root -> ... -> symptom
    path_edges: list[TraversalEdge] = field(default_factory=list)
    impacted_services: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "root_cause": self.root_cause,
            "entity_type": self.entity_type,
            "confidence": self.confidence,
            "fault_path": list(self.fault_path),
            "path_edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "edge_type": e.edge_type,
                    "layer": e.layer,
                }
                for e in self.path_edges
            ],
            "impacted_services": list(self.impacted_services),
        }


class DagTraversal:
    """Backward BFS root-cause traversal over a merged Two-Layer graph.

    Layer semantics follow two_layer_graph.py: a Layer-1 dependency edge
    ``dependent -[:CALLS/:READS_FROM/:DEPENDS_ON]-> dependency`` propagates
    faults from the dependency to the dependent, so backward traversal follows
    the edge's *outgoing* direction; a Layer-2 causal edge ``cause -> effect``
    propagates faults from cause to effect, so backward traversal follows the
    edge's *incoming* direction.  Both directions are normalized into
    "edges toward upstream roots" so the BFS stays layer-agnostic.
    """

    def __init__(
        self,
        graph: TwoLayerGraph,
        min_confidence: float | None = None,
        max_depth: int | None = None,
    ) -> None:
        if graph is None:
            raise StorageError("DagTraversal requires a TwoLayerGraph")
        # Operate on the authoritative merged view (Layer-2 causal edges
        # override Layer-1 dependency edges for the same node pair) exactly
        # like TwoLayerGraph.get_fault_paths(): a raw unmerged graph would
        # otherwise yield duplicate candidates for the same root.
        self._graph = graph.merge()
        self._max_depth = max_depth if max_depth is not None else self._graph.max_depth
        self._min_confidence = (
            min_confidence if min_confidence is not None else self._load_min_confidence()
        )
        self._path_order = self._graph.path_order

    # ------------------------------------------------------------------ #
    # Configuration
    # ------------------------------------------------------------------ #
    @staticmethod
    def _load_min_confidence() -> float:
        """Read dag_traversal.min_confidence from causal_rules.yaml (best effort)."""
        try:
            with open(_DEFAULT_RULES_PATH, "r", encoding="utf-8") as fh:
                rules: dict[str, Any] = yaml.safe_load(fh) or {}
        except OSError:
            return 0.3
        traversal = rules.get("dag_traversal", {}) or {}
        return float(traversal.get("min_confidence", 0.3))

    @property
    def min_confidence(self) -> float:
        return self._min_confidence

    @property
    def max_depth(self) -> int:
        return self._max_depth

    # ------------------------------------------------------------------ #
    # Upstream expansion (mirrors TwoLayerGraph._upstream_neighbors, public API)
    # ------------------------------------------------------------------ #
    def _upstream_edges(self, node_id: str) -> list[tuple[LayerEdge, str]]:
        """(edge, neighbor) pairs leading from ``node_id`` toward root causes."""
        upstream: list[tuple[LayerEdge, str]] = []
        for edge in self._graph.outgoing_edges(node_id):
            if edge.layer == "1":
                upstream.append((edge, edge.target))
        for edge in self._graph.incoming_edges(node_id):
            if edge.layer == "2":
                upstream.append((edge, edge.source))
        return upstream

    # ------------------------------------------------------------------ #
    # Deterministic confidence scoring
    # ------------------------------------------------------------------ #
    @staticmethod
    def _score(edge_layers: list[str]) -> float:
        """Confidence in [0, 1] from the layer sequence along a fault path.

        - Single-hop evidence (no edges): symptom is its own root -> 0.20.
        - Any Layer-2 causal edge anchors the path at 0.85, else 0.55 for a
          pure Layer-1 dependency fallback (decision 8 / 12).
        - Longer paths are discounted toward the anchor by ``_DEPTH_DECAY``
          per extra hop, blended 60% anchor / 40% depth so depth alone never
          collapses a structurally-valid path below the default 0.3 gate.
        """
        if not edge_layers:
            return _SINGLE_NODE_CONFIDENCE
        anchor = _LAYER2_CONFIDENCE if "2" in edge_layers else _LAYER1_CONFIDENCE
        depth_penalty = max(0.0, 1.0 - _DEPTH_DECAY * (len(edge_layers) - 1))
        confidence = anchor * (0.6 + 0.4 * depth_penalty)
        return round(min(1.0, max(0.0, confidence)), 3)

    # ------------------------------------------------------------------ #
    # Backward BFS
    # ------------------------------------------------------------------ #
    def find_root_causes(self, symptom: str) -> list[RootCandidate]:
        """Enumerate ranked root-cause candidates for ``symptom``.

        Deterministic: upstream edges are sorted by (neighbor, layer,
        edge_type); cycles are pruned with a per-path visited set; depth is
        bounded by ``max_depth``.  Returns candidates whose confidence meets
        ``min_confidence``, ordered by confidence desc then root id asc.
        """
        if not self._graph.has_node(symptom):
            _LOG.warning("symptom '%s' absent from two-layer graph", symptom)
            return []

        raw_paths: list[tuple[list[str], list[TraversalEdge]]] = []

        def _walk(
            node: str,
            path_nodes: list[str],
            path_edges: list[TraversalEdge],
            visited: set[str],
        ) -> None:
            if len(path_nodes) > self._max_depth:
                return
            upstream = sorted(
                self._upstream_edges(node),
                key=lambda t: (t[1], t[0].layer, t[0].edge_type, t[0].source),
            )
            if not upstream:
                raw_paths.append((list(path_nodes), list(path_edges)))
                return
            for edge, neighbor in upstream:
                if neighbor in visited:
                    continue
                _walk(
                    neighbor,
                    path_nodes + [neighbor],
                    path_edges
                    + [
                        TraversalEdge(
                            source=edge.source,
                            target=edge.target,
                            edge_type=edge.edge_type,
                            layer=edge.layer,
                        )
                    ],
                    visited | {neighbor},
                )

        _walk(symptom, [symptom], [], {symptom})
        _LOG.info(
            "traversal symptom=%s raw_paths=%d max_depth=%d",
            symptom,
            len(raw_paths),
            self._max_depth,
        )

        candidates: list[RootCandidate] = []
        for path_nodes, path_edges in raw_paths:
            root = path_nodes[-1]
            layers = [e.layer for e in path_edges]
            confidence = self._score(layers)
            if confidence < self._min_confidence:
                continue
            fault_path = list(reversed(path_nodes))
            if self._path_order == "symptom_to_root":
                fault_path = list(path_nodes)
            candidates.append(
                RootCandidate(
                    root_cause=root,
                    entity_type=self._graph.entity_type(root),
                    confidence=confidence,
                    fault_path=fault_path,
                    path_edges=path_edges,
                    impacted_services=sorted(set(path_nodes)),
                )
            )

        candidates.sort(key=lambda c: (-c.confidence, c.root_cause))
        return candidates

    # ------------------------------------------------------------------ #
    # E2E / consumer-facing surface
    # ------------------------------------------------------------------ #
    def analyze(self, symptom: str) -> dict[str, Any]:
        """Dict payload for root_cause_builder.py and E2E assertions."""
        candidates = self.find_root_causes(symptom)
        return {
            "symptom": symptom,
            "path_order": self._path_order,
            "min_confidence": self._min_confidence,
            "root_cause": candidates[0].root_cause if candidates else None,
            "confidence": candidates[0].confidence if candidates else 0.0,
            "fault_path": candidates[0].fault_path if candidates else [],
            "impacted_services": candidates[0].impacted_services if candidates else [],
            "candidates": [c.to_dict() for c in candidates],
        }
