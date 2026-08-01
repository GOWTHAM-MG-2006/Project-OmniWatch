"""
OmniWatch — Storage Layer
Component: Neo4j Client
Phase: 5
Purpose: Bolt-7687 client for the Neo4j causal-dependency graph: upsert nodes,
         create typed relationships, query an entity's connectivity, dump the
         full topology, and health-check connectivity.
Inputs: StorageConfig.from_env() connection params (NEO4J_URI/USER/PASSWORD);
        node payloads keyed on `id` matching AGENTS.md labels (:Service,
        :Database, :Infrastructure); relationship payloads matching AGENTS.md
        relationship types (:CALLS, :READS_FROM, :DEPENDS_ON)
Outputs: Neo4j graph writes (node dict / relationship records); connected-node
         and full-topology query results; health status boolean
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, cast

from neo4j import Driver, GraphDatabase, Query

from storage.common import StorageError, create_logger, retry_with_backoff
from storage.config import StorageConfig

# Labels/relationship-type tokens must be bare alphanumeric identifiers so they
# can be safely interpolated into Cypher (labels and rel types cannot be bound
# as parameters). Anything else is rejected to prevent query injection.
_LABEL_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")

# AGENTS.md relationship contract for the topology graph. The generic
# `rel_type` parameter accepts these three (with or without a leading colon);
# no additional relationship types are invented by this client.
SUPPORTED_RELATIONSHIP_TYPES = (":CALLS", ":READS_FROM", ":DEPENDS_ON")


class Neo4jClient:
    """Connection-pooled Neo4j client over Bolt 7687.

    A single ``GraphDatabase.driver`` instance (Neo4j's built-in connection
    pool) is created per client; every operation opens a short-lived session
    via ``driver.session()`` and closes it on exit. Constraints/indexes are
    NOT created here — that is the responsibility of
    ``storage/neo4j/constraints.py``; this client assumes ``id`` uniqueness
    already exists.

    Method signatures are stable: ``storage/neo4j/topology_loader.py`` imports
    this client to load the topology graph.
    """

    def __init__(self, config: Optional[StorageConfig] = None) -> None:
        cfg = config or StorageConfig.from_env()
        self._config = cfg
        self._log = create_logger("omniwatch.storage.neo4j")
        # Driver construction is lazy (no network I/O); the first real
        # connection is opened by connect()/health_check().
        self._driver: Driver = GraphDatabase.driver(
            cfg.neo4j_uri,
            auth=(cfg.neo4j_user, cfg.neo4j_password),
        )
        self._log.info("neo4j_client_created uri=%s", cfg.neo4j_uri)

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Close the driver, releasing all pooled connections."""
        self._driver.close()
        self._log.info("neo4j_client_closed")

    def __enter__(self) -> "Neo4jClient":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()

    # ------------------------------------------------------------------ #
    # Internal helpers
    # ------------------------------------------------------------------ #

    @staticmethod
    def _safe_label(label: str) -> str:
        if not isinstance(label, str) or not _LABEL_RE.match(label):
            raise StorageError(f"Invalid Neo4j label: {label!r}")
        return label

    @staticmethod
    def _safe_rel_type(rel_type: str) -> str:
        rel = rel_type[1:] if rel_type.startswith(":") else rel_type
        if not _LABEL_RE.match(rel):
            raise StorageError(f"Invalid Neo4j relationship type: {rel_type!r}")
        return rel

    def _run(self, query: str, **params: Any) -> List[Dict[str, Any]]:
        """Run a write/read Cypher query inside a managed session.

        Returns the full result as a list of dicts (``result.data()``), which
        is empty for writes without a RETURN clause.
        """
        with self._driver.session() as session:
            # Query marks caller-built Cypher as trusted; labels/rel types are
            # already sanitized via _safe_label/_safe_rel_type.
            return session.run(cast(Query, query), **params).data()

    # ------------------------------------------------------------------ #
    # Connectivity
    # ------------------------------------------------------------------ #

    def connect(self) -> None:
        """Verify connectivity to the server, retrying with 3x backoff.

        The first retryable connect attempt is the lazy driver opening its
        initial Bolt handshake; transient failures are retried 100ms -> 500ms
        -> 2s before a ``StorageError`` is raised.
        """
        try:
            retry_with_backoff(
                self._driver.verify_connectivity,
                retries=3,
                logger=self._log,
            )
        except Exception as exc:  # noqa: BLE001 - re-raised as StorageError
            raise StorageError(f"Neo4j connect failed: {exc}") from exc
        self._log.info("neo4j_connected")

    def health_check(self) -> bool:
        """Return True when the server is reachable and authenticated.

        Runs ``verify_connectivity`` (with backoff retry) — the documented
        Neo4j connectivity probe. Raises ``StorageError`` on failure.
        """
        self.connect()
        self._log.info("neo4j_health_check ok")
        return True

    # ------------------------------------------------------------------ #
    # Nodes
    # ------------------------------------------------------------------ #

    def create_node(
        self, label: str, properties: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Upsert a node: MERGE on ``id`` then set the remaining properties.

        Idempotent — re-running with the same ``id`` updates (never duplicates)
        the node. ``properties`` must include an ``id`` value (used as the MERGE
        key); all other keys are written via ``SET n += $props``.

        Returns the stored node dict (element_id, labels, properties).
        """
        safe_label = self._safe_label(label)
        node_id = properties.get("id")
        if not node_id:
            raise StorageError(
                f"create_node({label}) requires an 'id' property, got {list(properties)}"
            )
        props = {key: value for key, value in properties.items() if key != "id"}
        query = (
            f"MERGE (n:`{safe_label}` {{id: $id}}) "
            "SET n += $props "
            "RETURN n"
        )
        try:
            rows = self._run(query, id=node_id, props=props)
        except Exception as exc:  # noqa: BLE001 - surfaced as StorageError
            raise StorageError(
                f"create_node({safe_label}, id={node_id}) failed: {exc}"
            ) from exc
        node = rows[0]["n"] if rows else None
        self._log.info(
            "node_upserted label=%s id=%s existing=%s",
            safe_label,
            node_id,
            not rows,
        )
        return node

    def create_relationship(
        self,
        source_id: str,
        rel_type: str,
        target_id: str,
        properties: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """Upsert a typed directed relationship between two existing nodes.

        MATCHes source/target by ``id`` (nodes must already exist — use
        ``create_node`` first) then MERGEs ``(s)-[:REL]->(t)`` with the given
        properties. Idempotent — re-running with the same (source, type,
        target) updates (never duplicates) the relationship. ``rel_type`` is
        generic; the AGENTS.md contract types are :CALLS (latency_p50/p95/p99,
        error_rate), :READS_FROM (query_type, avg_duration_ms) and :DEPENDS_ON
        (dependency_type, criticality).

        Returns the created/updated relationship record, or ``None`` (not an
        error) if either endpoint node does not exist.
        """
        rel = self._safe_rel_type(rel_type)
        props = properties or {}
        query = (
            "MATCH (s {id: $source_id}), (t {id: $target_id}) "
            f"MERGE (s)-[r:`{rel}`]->(t) "
            "SET r += $props "
            "RETURN r"
        )
        try:
            rows = self._run(
                query,
                source_id=source_id,
                target_id=target_id,
                props=props,
            )
        except Exception as exc:  # noqa: BLE001 - surfaced as StorageError
            raise StorageError(
                f"create_relationship({source_id})-[:{rel}]->({target_id}) "
                f"failed: {exc}"
            ) from exc
        relationship = rows[0]["r"] if rows else None
        self._log.info(
            "relationship_upserted type=%s source=%s target=%s",
            rel,
            source_id,
            target_id,
        )
        return relationship

    # ------------------------------------------------------------------ #
    # Queries
    # ------------------------------------------------------------------ #

    def query_by_entity(self, entity_id: str) -> List[Dict[str, Any]]:
        """Return every node connected to ``entity_id`` with edge detail.

        Each result dict contains the queried entity, the connected node, the
        relationship (type + properties), and whether it is ``outgoing`` or
        ``incoming`` relative to the entity.
        """
        query = (
            "MATCH (n {id: $entity_id})-[r]-(connected) "
            "RETURN n, r, connected"
        )
        try:
            rows = self._run(query, entity_id=entity_id)
        except Exception as exc:  # noqa: BLE001 - surfaced as StorageError
            raise StorageError(
                f"query_by_entity({entity_id}) failed: {exc}"
            ) from exc
        edges: List[Dict[str, Any]] = []
        for row in rows:
            rel = row["r"]
            if isinstance(rel, tuple):
                # Driver 6.x returns relationships as compacted 3-tuples
                # (start_node_props, rel_type, end_node_props) and nodes as
                # plain property dicts; no relationship property dict survives
                # in this representation.
                start_props, rel_type, _end_props = rel
                n_props = (
                    row["n"]._properties
                    if hasattr(row["n"], "_properties")
                    else row["n"]
                )
                is_outgoing = start_props == n_props
                relationship = {"type": rel_type, "properties": {}}
            else:
                # Legacy driver (<6): full Relationship object with metadata.
                is_outgoing = rel.start_node.element_id == row["n"].element_id
                relationship = {"type": rel.type, "properties": dict(rel)}
            edges.append(
                {
                    "entity": row["n"],
                    "connected_node": row["connected"],
                    "relationship": relationship,
                    "direction": "outgoing" if is_outgoing else "incoming",
                }
            )
        self._log.info("entity_queried id=%s connections=%d", entity_id, len(edges))
        return edges

    def get_topology(self) -> Dict[str, Any]:
        """Return the full topology: all nodes plus all typed relationships.

        Output shape: ``{"nodes": [...], "relationships": [...],
        "node_count": int, "relationship_count": int}``. Each relationship
        carries ``source``, ``target``, ``relationship_type`` and
        ``properties``.
        """
        try:
            node_rows = self._run("MATCH (n) RETURN n")
            rel_rows = self._run("MATCH (a)-[r]->(b) RETURN a, r, b")
        except Exception as exc:  # noqa: BLE001 - surfaced as StorageError
            raise StorageError(f"get_topology() failed: {exc}") from exc
        relationships: List[Dict[str, Any]] = []
        for row in rel_rows:
            rel = row["r"]
            if isinstance(rel, tuple):
                # Driver 6.x compacted tuple: (start_props, rel_type, end_props);
                # matched as (a)-[r]->(b), so rel[0] is a's props and rel[2] is
                # b's props. No relationship property dict survives in the tuple.
                _start_props, rel_type, _end_props = rel
                relationships.append(
                    {
                        "source": row["a"],
                        "relationship_type": rel_type,
                        "properties": {},
                        "target": row["b"],
                    }
                )
            else:
                # Legacy driver (<6): full Relationship object with metadata.
                relationships.append(
                    {
                        "source": row["a"],
                        "relationship_type": rel.type,
                        "properties": dict(rel),
                        "target": row["b"],
                    }
                )
        self._log.info(
            "topology_loaded nodes=%d relationships=%d",
            len(node_rows),
            len(relationships),
        )
        return {
            "nodes": [row["n"] for row in node_rows],
            "relationships": relationships,
            "node_count": len(node_rows),
            "relationship_count": len(relationships),
        }
