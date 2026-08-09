"""
OmniWatch — Unified Storage Layer
Component: Topology Loader
Phase: 5
Purpose: Kafka consumer that loads entity topology from the
         omniwatch.entities.resolved topic into Neo4j as typed nodes and
         directed relationships, building the causal dependency graph.
Inputs: Kafka messages from omniwatch.entities.resolved (JSON entity records
        with entity_id, entity_type, name, type, criticality, cloud_provider,
        status, anomaly_score, last_seen, and optional relationship hints
        depends_on/calls/reads_from)
Outputs: Neo4j graph nodes (:Service, :Database, :Infrastructure, :K8sResource)
         and directed relationships (:CALLS, :READS_FROM, :DEPENDS_ON) via
         storage.neo4j.client.Neo4jClient

Design decisions:
  Label mapping (entity_type → Neo4j label):
    API_NODE / SERVICE → Service
    DATABASE_NODE / DATABASE → Database
    INFRASTRUCTURE → Infrastructure
    K8S / K8S_RESOURCE → K8sResource
    Unknown types → SKIP with warning log (safer than fallback to Infrastructure;
    an incorrect label would pollute the graph with misclassified nodes).

  Missing-target behavior: If a relationship's source or target node does not
  yet exist in Neo4j, the relationship is queued in a bounded in-memory
  pending buffer (MAX_PENDING_RELS entries, FIFO with oldest-drop on
  overflow) and logged as relationship_deferred. Rationale: the missing
  endpoint entity may arrive in a subsequent Kafka message; creating stub
  nodes would pollute the graph with incomplete data. When either endpoint
  node is eventually upserted, _flush_pending() retries only the queued
  relationships connected to that node and drops the ones that now succeed.
  Neo4jClient.create_relationship uses MERGE on (source, type, target), so
  every relationship is created exactly once regardless of arrival order or
  record re-arrival (the loader is idempotent).

  Kafka bootstrap: Read from KAFKA_BOOTSTRAP_SERVERS env var (default
  localhost:9092). StorageConfig does not carry Kafka settings; they live
  in the ingestion layer's env vars.

  Simulation-First fallback (--demo): The Kafka topic may be empty or carry
  only unresolved/unknown records (e.g. entity_type=""), which the loader
  correctly skips — leaving the graph empty and the Phase 7 causal engine
  with nothing to traverse. To keep the pipeline reproducible without real
  cloud telemetry, ``python -m storage.neo4j.topology_loader --demo`` loads
  the built-in DEMO_TOPOLOGY (the AGENTS.md :Service/:Database/
  :Infrastructure/:K8sResource model with :CALLS/:READS_FROM/:DEPENDS_ON)
  through the exact same consume_one() code path as Kafka. Idempotent
  (client-side MERGE) — safe to re-run.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import time
from typing import Any, Dict, List, Optional

from storage.common import StorageError, create_logger
from storage.config import StorageConfig
from storage.neo4j.client import Neo4jClient

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TOPIC = "omniwatch.entities.resolved"
GROUP_ID = "neo4j-topology-loader"
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# Poll timeout per consumer.poll() call — keeps the loop responsive and
# prevents infinite hangs when Kafka is down.
POLL_TIMEOUT_SECONDS = 1.0

# Upper bound for the in-memory pending-relationship buffer. Relationships
# whose endpoint node has not arrived yet are queued here and flushed when
# the node is upserted; exceeding this bound drops the OLDEST entry (FIFO)
# to keep memory bounded.
MAX_PENDING_RELS = 1000

# Entity type → Neo4j node label mapping
# Unknown types are skipped (logged + ignored) rather than falling back to
# Infrastructure, which would silently misclassify nodes.
ENTITY_TYPE_LABEL_MAP: Dict[str, str] = {
    "API_NODE": "Service",
    "SERVICE": "Service",
    "DATABASE_NODE": "Database",
    "DATABASE": "Database",
    "INFRASTRUCTURE": "Infrastructure",
    "K8S": "K8sResource",
    "K8S_RESOURCE": "K8sResource",
}

# Node properties that are written to Neo4j per AGENTS.md contract.
# Other fields in the record (e.g. depends_on, calls, reads_from) are
# relationship hints, not node properties.
NODE_PROPERTY_KEYS = (
    "id",
    "name",
    "type",
    "criticality",
    "cloud_provider",
    "status",
    "anomaly_score",
    "last_seen",
)

# ---------------------------------------------------------------------------
# Demo topology (Simulation-First fallback)
# ---------------------------------------------------------------------------
# Built-in demo topology matching the AGENTS.md node/relationship contract.
# Loaded via `python -m storage.neo4j.topology_loader --demo` when the Kafka
# topic is empty or carries only unresolved records, so the causal graph is
# reproducible without real cloud telemetry. Each record uses the same schema
# as a Kafka message on omniwatch.entities.resolved and is fed through
# consume_one() — the identical code path as the live consumer.
#
# Entity types map to labels via ENTITY_TYPE_LABEL_MAP:
#   API_NODE/SERVICE → Service, DATABASE_NODE/DATABASE → Database,
#   INFRASTRUCTURE → Infrastructure, K8S/K8S_RESOURCE → K8sResource
# Relationship hints: calls → :CALLS (latency_p50/p95/p99, error_rate),
# reads_from → :READS_FROM (query_type, avg_duration_ms),
# depends_on → :DEPENDS_ON (dependency_type, criticality).
DEMO_TOPOLOGY: List[Dict[str, Any]] = [
    # --- Services (API_NODE) ---
    {
        "entity_id": "api-gateway",
        "entity_type": "API_NODE",
        "name": "api-gateway",
        "type": "API_NODE",
        "criticality": "high",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
        "calls": ["user-service", "order-service"],
        "latency_p50": 12.5,
        "latency_p95": 45.2,
        "latency_p99": 120.8,
        "error_rate": 0.02,
    },
    {
        "entity_id": "user-service",
        "entity_type": "API_NODE",
        "name": "user-service",
        "type": "API_NODE",
        "criticality": "high",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
        "calls": ["order-service"],
        "latency_p50": 8.2,
        "latency_p95": 30.1,
        "latency_p99": 85.4,
        "error_rate": 0.01,
        "reads_from": ["postgresql-database"],
        "query_type": "SELECT",
        "avg_duration_ms": 12.5,
    },
    {
        "entity_id": "order-service",
        "entity_type": "API_NODE",
        "name": "order-service",
        "type": "API_NODE",
        "criticality": "high",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
        "reads_from": ["postgresql-database", "redis-cache"],
        "query_type": "SELECT",
        "avg_duration_ms": 18.7,
        "depends_on": ["postgresql-database"],
        "dependency_type": "storage",
    },
    {
        "entity_id": "background-worker",
        "entity_type": "SERVICE",
        "name": "background-worker",
        "type": "SERVICE",
        "criticality": "medium",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
        "depends_on": ["postgresql-database"],
        "dependency_type": "storage",
    },
    # --- Databases ---
    {
        "entity_id": "postgresql-database",
        "entity_type": "DATABASE_NODE",
        "name": "postgresql",
        "type": "DATABASE_NODE",
        "criticality": "high",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
    },
    {
        "entity_id": "redis-cache",
        "entity_type": "DATABASE_NODE",
        "name": "redis",
        "type": "DATABASE_NODE",
        "criticality": "medium",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
    },
    # --- Infrastructure ---
    {
        "entity_id": "k8s-cluster",
        "entity_type": "INFRASTRUCTURE",
        "name": "k8s-cluster",
        "type": "INFRASTRUCTURE",
        "criticality": "high",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
    },
    # --- K8sResource ---
    {
        "entity_id": "order-service-deployment",
        "entity_type": "K8S_RESOURCE",
        "name": "order-service-deployment",
        "type": "K8S_RESOURCE",
        "criticality": "medium",
        "cloud_provider": "gcp",
        "status": "healthy",
        "anomaly_score": 0.0,
        "last_seen": "2026-08-08T00:00:00Z",
        "depends_on": ["k8s-cluster"],
        "dependency_type": "orchestration",
    },
]


# ---------------------------------------------------------------------------
# TopologyLoader
# ---------------------------------------------------------------------------

class TopologyLoader:
    """Kafka consumer that materializes entity topology into Neo4j.

    Uses confluent-kafka Consumer directly (replicated from
    ingestion/kafka_bus.py pattern) to avoid cross-layer import coupling.
    Delegates all Neo4j writes to Neo4jClient (create_node,
    create_relationship) — no Cypher is built in this module.

    Relationships whose endpoint node has not arrived yet are deferred in a
    bounded in-memory buffer (MAX_PENDING_RELS) and flushed when either
    endpoint node is upserted; client-side MERGE keeps all writes idempotent.
    """

    def __init__(
        self,
        client: Optional[Neo4jClient] = None,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = TOPIC,
        group_id: str = GROUP_ID,
    ) -> None:
        self._client = client or Neo4jClient()
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._group_id = group_id
        self._log = create_logger("omniwatch.storage.neo4j.topology_loader")
        self._running = False
        self._consumer: Any = None  # confluent_kafka.Consumer (lazy import)
        # Bounded in-memory buffer of relationships whose source/target node
        # has not arrived yet (flushed by _flush_pending on node upsert).
        self._pending: List[Dict[str, Any]] = []

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #

    def start(self) -> None:
        """Initialize the Kafka consumer and subscribe to the topic."""
        from confluent_kafka import Consumer  # lazy: avoid import-time dep

        conf: Dict[str, Any] = {
            "bootstrap.servers": self._bootstrap_servers,
            "group.id": self._group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
            "auto.commit.interval.ms": 5000,
            "max.poll.interval.ms": 300000,
            "session.timeout.ms": 30000,
            "heartbeat.interval.ms": 10000,
        }
        self._consumer = Consumer(conf)
        self._consumer.subscribe([self._topic])
        self._running = True
        self._log.info(
            "topology_loader_started topic=%s group=%s bootstrap=%s",
            self._topic,
            self._group_id,
            self._bootstrap_servers,
        )

    def stop(self) -> None:
        """Close the consumer cleanly."""
        self._running = False
        if self._consumer is not None:
            self._consumer.close()
            self._consumer = None
        self._log.info("topology_loader_stopped")

    def run(self) -> None:
        """Main consumer loop with graceful shutdown on SIGINT/SIGTERM.

        Bounded poll timeout (1.0s) ensures the loop never hangs forever,
        even when Kafka is unreachable. On Kafka connection failure the
        consumer.poll() returns None (no messages) — no crash, just idle.
        If the broker is truly unreachable, librdkafka logs error messages
        and poll returns None repeatedly until the broker comes back.
        """
        # Register signal handlers for graceful shutdown
        def _shutdown_handler(signum: int, _frame: Any) -> None:
            sig_name = signal.Signals(signum).name
            self._log.info("shutdown_signal_received signal=%s", sig_name)
            self._running = False

        signal.signal(signal.SIGINT, _shutdown_handler)
        signal.signal(signal.SIGTERM, _shutdown_handler)

        self.start()

        self._log.info("consumer_loop_started")

        while self._running:
            try:
                msg = self._consumer.poll(POLL_TIMEOUT_SECONDS)
            except Exception as exc:
                # Catch-all: log and continue (bounded poll prevents tight loop)
                self._log.error(
                    "kafka_poll_error error=%s", exc, exc_info=True
                )
                time.sleep(POLL_TIMEOUT_SECONDS)
                continue

            if msg is None:
                # No message within timeout — normal idle
                continue

            if msg.error():
                self._log.error(
                    "kafka_message_error code=%s reason=%s",
                    msg.error().code(),
                    msg.error().str(),
                )
                continue

            # Decode and process the message
            try:
                value_bytes = msg.value()
                if value_bytes is None:
                    self._log.warning("empty_message topic=%s offset=%s", msg.topic(), msg.offset())
                    continue
                record: Dict[str, Any] = json.loads(value_bytes.decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                self._log.warning(
                    "message_decode_error topic=%s offset=%s error=%s",
                    msg.topic(),
                    msg.offset(),
                    exc,
                )
                continue

            try:
                self.consume_one(record)
            except Exception as exc:
                self._log.error(
                    "consume_one_error entity_id=%s error=%s",
                    record.get("entity_id", "<missing>"),
                    exc,
                    exc_info=True,
                )
                # Do not re-raise — continue processing next message

        self.stop()

    # ------------------------------------------------------------------ #
    # Record processing (testable without Kafka)
    # ------------------------------------------------------------------ #

    def consume_one(self, record: Dict[str, Any]) -> None:
        """Process a single entity record: upsert node + create relationships.

        This method is the core testable unit. It accepts a parsed JSON dict
        (the Kafka message value) and delegates to Neo4jClient for all writes.

        Raises:
            StorageError: If required fields are missing or Neo4j writes fail.
        """
        entity_id = record.get("entity_id")
        entity_type = record.get("entity_type")

        if not entity_id:
            self._log.warning("skipping_record_no_entity_id keys=%s", list(record.keys()))
            return

        if not entity_type:
            self._log.warning(
                "skipping_record_no_entity_type entity_id=%s", entity_id
            )
            return

        # Resolve Neo4j label from entity_type
        label = ENTITY_TYPE_LABEL_MAP.get(entity_type.upper())
        if label is None:
            self._log.warning(
                "unknown_entity_type entity_id=%s entity_type=%s — skipping node creation",
                entity_id,
                entity_type,
            )
            # Still try to create relationships if the node already exists
            # from a prior message (or will exist). If not, relationships
            # are skipped with a warning in _create_relationships.
            self._create_relationships(entity_id, record)
            return

        # Build node properties — only known keys, all values coerced to
        # strings/floats/ints for Neo4j compatibility.
        node_props: Dict[str, Any] = {"id": entity_id}
        for key in NODE_PROPERTY_KEYS:
            if key == "id":
                continue  # Already set above
            value = record.get(key)
            if value is not None:
                node_props[key] = value

        # Upsert the node via Neo4jClient (MERGE on id)
        self._client.create_node(label, node_props)
        self._log.info(
            "node_upserted entity_id=%s label=%s", entity_id, label
        )

        # Flush deferred relationships connected to this just-upserted node
        # (either as source or target). Removes flushed entries, keeps the rest.
        self._flush_pending(entity_id)

        # Create relationships
        self._create_relationships(entity_id, record)

    def _create_relationships(
        self, source_id: str, record: Dict[str, Any]
    ) -> None:
        """Create directed relationships from this entity to its targets.

        Relationship hints in the record:
          - depends_on: list[str] → :DEPENDS_ON (properties: dependency_type, criticality)
          - calls: list[str] → :CALLS (properties: latency_p50, latency_p95, latency_p99, error_rate)
          - reads_from: list[str] → :READS_FROM (properties: query_type, avg_duration_ms)

        If an endpoint node does not exist in Neo4j, the relationship is
        queued in the bounded in-memory pending buffer (relationship_deferred)
        and flushed when either endpoint node is upserted. The loader is
        idempotent: client-side MERGE on (source, type, target) guarantees a
        single relationship per record pair regardless of arrival order or
        record re-arrival.
        """
        # :DEPENDS_ON
        depends_on = record.get("depends_on", [])
        if isinstance(depends_on, list):
            for target_id in depends_on:
                if not isinstance(target_id, str) or not target_id:
                    continue
                props: Dict[str, Any] = {}
                dep_type = record.get("dependency_type")
                if dep_type is not None:
                    props["dependency_type"] = dep_type
                crit = record.get("criticality")
                if crit is not None:
                    props["criticality"] = crit
                self._safe_create_rel(source_id, "DEPENDS_ON", target_id, props)

        # :CALLS
        calls = record.get("calls", [])
        if isinstance(calls, list):
            for target_id in calls:
                if not isinstance(target_id, str) or not target_id:
                    continue
                props = {}
                for rel_key in ("latency_p50", "latency_p95", "latency_p99", "error_rate"):
                    val = record.get(rel_key)
                    if val is not None:
                        props[rel_key] = val
                self._safe_create_rel(source_id, "CALLS", target_id, props)

        # :READS_FROM
        reads_from = record.get("reads_from", [])
        if isinstance(reads_from, list):
            for target_id in reads_from:
                if not isinstance(target_id, str) or not target_id:
                    continue
                props = {}
                for rel_key in ("query_type", "avg_duration_ms"):
                    val = record.get(rel_key)
                    if val is not None:
                        props[rel_key] = val
                self._safe_create_rel(source_id, "READS_FROM", target_id, props)

    def _safe_create_rel(
        self,
        source_id: str,
        rel_type: str,
        target_id: str,
        props: Dict[str, Any],
        defer: bool = True,
    ) -> bool:
        """Attempt to create a relationship; defer to pending buffer on failure.

        Neo4jClient.create_relationship requires both endpoint nodes to exist
        (MATCH by id). If either endpoint is missing, the MATCH returns empty
        and no relationship is created — we then queue the relationship in the
        bounded pending buffer (log relationship_deferred) so it can be
        retried once the missing node is upserted.

        ``defer=False`` is used by _flush_pending to avoid re-queueing entries
        that are already in the buffer while retrying them.

        Returns True when the relationship was created (client MERGE makes
        re-arrivals idempotent), False when it is still deferred.
        """
        try:
            result = self._client.create_relationship(
                source_id, rel_type, target_id, props
            )
            if result is None:
                if defer:
                    self._defer_relationship(source_id, rel_type, target_id, props)
                else:
                    self._log.warning(
                        "relationship_target_missing source=%s rel=%s target=%s — skipping",
                        source_id,
                        rel_type,
                        target_id,
                    )
                return False
            self._log.info(
                "relationship_created source=%s rel=%s target=%s",
                source_id,
                rel_type,
                target_id,
            )
            return True
        except StorageError as exc:
            # Endpoint missing or transient failure — defer and retry when a
            # connected node arrives; the pending buffer stays bounded.
            if defer:
                self._defer_relationship(source_id, rel_type, target_id, props)
            else:
                self._log.warning(
                    "relationship_skipped source=%s rel=%s target=%s reason=%s",
                    source_id,
                    rel_type,
                    target_id,
                    exc,
                )
            return False

    def _defer_relationship(
        self,
        source_id: str,
        rel_type: str,
        target_id: str,
        props: Dict[str, Any],
    ) -> None:
        """Queue a relationship whose endpoint node has not arrived yet.

        Appends to the bounded pending buffer (no duplicate entries). On
        overflow past MAX_PENDING_RELS the OLDEST entry is dropped with a
        WARNING, keeping memory bounded.
        """
        entry: Dict[str, Any] = {
            "source": source_id,
            "type": rel_type,
            "target": target_id,
            "props": props,
        }
        if entry in self._pending:
            return
        self._pending.append(entry)
        if len(self._pending) > MAX_PENDING_RELS:
            dropped = self._pending.pop(0)
            self._log.warning(
                "pending_rels_overflow dropped=%s pending=%d",
                dropped,
                len(self._pending),
            )
        self._log.info(
            "relationship_deferred source=%s rel=%s target=%s pending=%d",
            source_id,
            rel_type,
            target_id,
            len(self._pending),
        )

    def _flush_pending(self, entity_id: str) -> None:
        """Retry queued relationships connected to a just-upserted node.

        Called after every successful node upsert. Only entries whose source
        OR target equals ``entity_id`` are retried (O(1)-ish per record);
        entries that now succeed are removed, entries still missing their
        other endpoint stay queued. Relationships not connected to this node
        are left untouched.
        """
        if not self._pending:
            return
        still_pending: List[Dict[str, Any]] = []
        flushed = 0
        for entry in self._pending:
            if entry["source"] != entity_id and entry["target"] != entity_id:
                still_pending.append(entry)
                continue
            if self._safe_create_rel(
                entry["source"],
                entry["type"],
                entry["target"],
                entry["props"],
                defer=False,
            ):
                flushed += 1
            else:
                still_pending.append(entry)
        self._pending = still_pending
        if flushed:
            self._log.info(
                "pending_rels_flushed entity_id=%s flushed=%d pending=%d",
                entity_id,
                flushed,
                len(self._pending),
            )

    # ------------------------------------------------------------------ #
    # Cleanup (for testing / QA)
    # ------------------------------------------------------------------ #

    def cleanup_test_nodes(self, prefix: str = "topo-") -> int:
        """Delete all nodes whose id starts with ``prefix`` (DETACH DELETE).

        Used by QA scripts to remove test data. Returns the count of
        deleted nodes.
        """
        query = "MATCH (n) WHERE n.id STARTS WITH $prefix DETACH DELETE n RETURN count(n) AS deleted"
        rows = self._client._run(query, prefix=prefix)
        count = rows[0]["deleted"] if rows else 0
        self._log.info("cleanup_test_nodes prefix=%s deleted=%d", prefix, count)
        return count

    # ------------------------------------------------------------------ #
    # Simulation-First fallback (--demo)
    # ------------------------------------------------------------------ #

    def load_demo_topology(self) -> Dict[str, int]:
        """Load the built-in demo topology into Neo4j (Simulation-First).

        Feeds every record in :data:`DEMO_TOPOLOGY` through
        :meth:`consume_one` — the exact same code path the Kafka consumer
        uses — so the demo graph is a faithful reproduction of what a live
        ``omniwatch.entities.resolved`` stream would produce. Idempotent:
        client-side MERGE means re-running never duplicates nodes or
        relationships.

        Returns ``{"nodes": int, "relationships": int}`` — the post-load
        counts read back from Neo4j via ``get_topology()``.
        """
        for record in DEMO_TOPOLOGY:
            self.consume_one(record)
        topology = self._client.get_topology()
        self._log.info(
            "demo_topology_loaded nodes=%d relationships=%d",
            topology["node_count"],
            topology["relationship_count"],
        )
        return {
            "nodes": topology["node_count"],
            "relationships": topology["relationship_count"],
        }


# ---------------------------------------------------------------------------
# CLI entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="OmniWatch Neo4j topology loader — consumes "
        "omniwatch.entities.resolved, or loads the built-in demo topology "
        "with --demo (Simulation-First fallback when the Kafka topic is "
        "empty/unresolved).",
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Load the built-in demo topology into Neo4j and exit (no Kafka "
        "required). Idempotent — safe to re-run.",
    )
    args = parser.parse_args()

    create_logger("omniwatch.storage.neo4j.topology_loader")
    cfg = StorageConfig.from_env()
    client = Neo4jClient(cfg)

    # Verify Neo4j connectivity before starting consumer
    try:
        client.connect()
    except StorageError as exc:
        print(f"FATAL: Neo4j connection failed: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.demo:
        loader = TopologyLoader(client=client)
        counts = loader.load_demo_topology()
        print(
            f"Demo topology loaded: {counts['nodes']} nodes, "
            f"{counts['relationships']} relationships"
        )
        client.close()
        sys.exit(0)

    loader = TopologyLoader(client=client)
    loader.run()
