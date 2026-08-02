"""
OmniWatch — Predictive Intelligence Layer
Component: Signal Enricher
Phase: 6
Purpose: Neo4j entity context enrichment for anomaly signals
Inputs: AnomalySignal dict
Outputs: Enriched signal dict with Neo4j context
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout
from typing import Any, Dict, Optional, Protocol

from storage.neo4j.client import Neo4jClient


class _TopologyProvider(Protocol):
    """Structural interface for the Neo4j client surface the enricher uses.

    Only ``get_topology()`` is required, so tests can inject a lightweight
    fake without constructing a real ``Neo4jClient`` (which would open a
    Bolt driver).
    """

    def get_topology(self) -> Dict[str, Any]: ...

# AGENTS.md Neo4j node properties that enrich an anomaly signal. The node's
# `id` is the join key against AnomalySignal.entity_id; the remaining fields
# are the context we surface to downstream consumers.
_CONTEXT_KEYS = ("name", "type", "criticality", "anomaly_score", "last_seen")

# Default wall-clock budget for a single Neo4j lookup. If the graph is
# unreachable or slow, we degrade gracefully instead of blocking the pipeline.
DEFAULT_TIMEOUT_SECONDS = 2.0


class SignalEnricher:
    """Attach Neo4j entity context to an anomaly signal.

    ``enrich()`` looks up the entity identified by ``anomaly_signal["entity_id"]``
    in the Neo4j topology graph and merges its properties (name, type,
    criticality, anomaly_score, last_seen) into the signal under
    ``entity_context``. The lookup is bounded by a timeout; if Neo4j is
    unreachable, the entity is missing, or the timeout fires, the signal is
    returned unchanged with ``enriched=False`` so the pipeline never blocks on
    the graph.
    """

    def __init__(
        self,
        neo4j_client: Optional[_TopologyProvider] = None,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        """Build an enricher around a Neo4j client.

        ``neo4j_client`` is injectable for tests (a fake/mock); when omitted a
        real ``Neo4jClient`` is constructed from ``StorageConfig.from_env()``.
        ``timeout`` bounds each graph lookup in seconds.
        """
        self._client = neo4j_client or Neo4jClient()
        self._timeout = timeout

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def enrich(self, anomaly_signal: Dict[str, Any]) -> Dict[str, Any]:
        """Return ``anomaly_signal`` merged with Neo4j entity context.

        The input is copied (never mutated). On success the copy gains
        ``entity_context`` (the entity's Neo4j properties) and
        ``enriched=True``. On any failure — missing entity, unreachable graph,
        or timeout — the copy is returned unchanged with ``enriched=False``.
        """
        signal = dict(anomaly_signal)
        entity_id = signal.get("entity_id")
        if not entity_id:
            signal["enriched"] = False
            return signal

        try:
            context = self._lookup_entity(entity_id)
        except Exception:  # noqa: BLE001 - graceful degradation is the contract
            signal["enriched"] = False
            return signal

        if context is None:
            signal["enriched"] = False
            return signal

        signal["entity_context"] = context
        signal["enriched"] = True
        return signal

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    def _lookup_entity(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Fetch the entity's Neo4j properties, bounded by ``self._timeout``.

        Runs the graph query in a worker thread so a hung/unreachable server
        cannot block the caller beyond the timeout. Returns the context dict,
        or ``None`` when the entity does not exist in the graph.
        """
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self._fetch_node, entity_id)
            try:
                return future.result(timeout=self._timeout)
            except FutureTimeout:
                return None

    def _fetch_node(self, entity_id: str) -> Optional[Dict[str, Any]]:
        """Find the node whose ``id`` equals ``entity_id`` and extract context.

        Reuses the public ``Neo4jClient.get_topology()`` API (no storage
        changes) and filters the returned nodes by ``id``. Returns ``None``
        when no node matches.
        """
        topology = self._client.get_topology()
        for node in topology.get("nodes", []):
            props = node.get("properties", node)
            if props.get("id") == entity_id:
                return {key: props.get(key) for key in _CONTEXT_KEYS}
        return None