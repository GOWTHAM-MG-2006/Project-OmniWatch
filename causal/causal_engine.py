"""
OmniWatch — Causal Graph Engine
Component: Causal Engine
Phase: 7
Purpose: Orchestrate the root-cause pipeline: consume AnomalySignals from
         omniwatch.anomalies.detected, assemble the Two-Layer graph
         (Layer-1 dependency topology + Layer-2 causal adjacency),
         canonicalize entity ids across clouds, traverse backward to the
         root cause, build a RootCauseObject and publish it to
         omniwatch.incidents.causal; expose a FastAPI /health endpoint.
Inputs: AnomalySignal dicts (Kafka omniwatch.anomalies.detected), optional
        topology/adjacency overrides, causal_rules.yaml, .env.
Outputs: RootCauseObject dicts (Kafka omniwatch.incidents.causal), /health JSON.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI

from causal.config.settings import Settings
from causal.cross_cloud_mapper import CrossCloudMapper
from causal.dag_traversal import DagTraversal
from causal.dependency_discovery import discover_and_emit
from causal.root_cause_builder import RootCauseBuilder
from causal.two_layer_graph import TwoLayerGraph, load_from_topology
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.causal_engine")


# --------------------------------------------------------------------------- #
# FastAPI app + module-level health state
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="OmniWatch Causal Graph Engine",
    description="Phase 7 — root cause analysis + health endpoint",
    version="0.1.0",
)

_last_incident: str = "none"
_graph_ready: bool = False


class CausalEngine:
    """End-to-end root cause analysis orchestrator.

    Analysis is simulation-first and fully injectable: ``analyze_signal`` /
    ``process_signal`` accept optional ``topology`` and ``adjacency`` so tests
    and demos can supply Layer-1 / Layer-2 structure without ClickHouse,
    PyRCA or Kafka.  When omitted, Layer-1 is discovered best-effort from
    ClickHouse trace spans (empty when unavailable) and Layer-2 defaults to
    an empty DAG (plan Decision 10 — degradation is graceful).
    """

    def __init__(self, settings: Settings | None = None) -> None:
        self._settings = settings or Settings.from_env()
        self._mapper = CrossCloudMapper.from_config()
        self._builder = RootCauseBuilder()
        self._graph: TwoLayerGraph | None = None
        self._producer: Any = _PRODUCER_UNSET
        self._consumer: Any = None
        self._lock = threading.Lock()

    # ------------------------------------------------------------------ #
    # Graph assembly
    # ------------------------------------------------------------------ #
    def build_graph(
        self,
        topology: dict[str, Any] | None = None,
        adjacency: dict[str, list[str]] | None = None,
    ) -> TwoLayerGraph:
        """Assemble and cache the merged Two-Layer graph.

        ``topology`` is the get_topology()-shaped Layer-1 dict (defaults to
        best-effort ClickHouse trace discovery via ``discover_and_emit``).
        ``adjacency`` is the Layer-2 causal mapping ``{source: [targets]}``.
        """
        if topology is None:
            topology = discover_and_emit()
        if adjacency is None:
            adjacency = {}
        graph = load_from_topology(topology, adjacency)
        with self._lock:
            self._graph = graph
        set_graph_ready(graph.node_count() > 0)
        return graph

    @property
    def graph(self) -> TwoLayerGraph | None:
        return self._graph

    # ------------------------------------------------------------------ #
    # Symptom resolution (cross-cloud)
    # ------------------------------------------------------------------ #
    def _resolve_symptom(self, graph: TwoLayerGraph, signal: dict[str, Any]) -> str:
        """Map the signal's entity id onto a graph node id.

        Prefers the raw entity_id; when absent from the graph, falls back to
        the cross-cloud canonical id (``{provider}:{region}:{type}:{name}``)
        so provider-qualified topologies still resolve (CrossCloudMapper).
        """
        entity_id = str(signal.get("entity_id") or "")
        if graph.has_node(entity_id):
            return entity_id
        canonical = self._mapper.to_canonical(signal)
        if canonical and graph.has_node(canonical):
            _LOG.info("symptom resolved via canonical id: %s -> %s", entity_id, canonical)
            return canonical
        return entity_id

    # ------------------------------------------------------------------ #
    # Analysis pipeline
    # ------------------------------------------------------------------ #
    def analyze_signal(
        self,
        anomaly_signal: dict[str, Any],
        *,
        topology: dict[str, Any] | None = None,
        adjacency: dict[str, list[str]] | None = None,
        metrics: dict[str, Any] | None = None,
        log_snippets: list[str] | None = None,
        related_anomalies: list[dict[str, Any]] | None = None,
        incident_id: str | None = None,
    ) -> dict[str, Any]:
        """Consume one AnomalySignal and produce a RootCauseObject.

        Pipeline: build graph -> resolve symptom -> backward DAG traversal
        (DagTraversal, thresholds from settings) -> RootCauseBuilder.  Never
        raises for malformed signals — the builder degrades gracefully when
        no candidate root is found.
        """
        graph = self.build_graph(topology=topology, adjacency=adjacency)
        symptom = self._resolve_symptom(graph, anomaly_signal)
        traversal = DagTraversal(
            graph,
            min_confidence=self._settings.causal_min_confidence,
            max_depth=self._settings.causal_max_depth,
        ).analyze(symptom)
        _LOG.info(
            "causal_analysis symptom=%s root=%s confidence=%s",
            symptom,
            traversal.get("root_cause"),
            traversal.get("confidence"),
        )
        return self._builder.build(
            anomaly_signal,
            traversal,
            metrics=metrics,
            log_snippets=log_snippets,
            related_anomalies=related_anomalies,
            incident_id=incident_id,
        )

    def process_signal(
        self,
        anomaly_signal: dict[str, Any],
        *,
        topology: dict[str, Any] | None = None,
        adjacency: dict[str, list[str]] | None = None,
    ) -> dict[str, Any] | None:
        """Analyze a signal and publish the RootCauseObject to Kafka.

        Publishing is best-effort: when the Kafka producer cannot be built
        (no kafka-python-ng / no broker) the incident is still returned so
        callers and tests keep working (simulation-first rule).
        """
        incident = self.analyze_signal(
            anomaly_signal,
            topology=topology,
            adjacency=adjacency,
        )
        producer = self._get_producer()
        if producer is not None:
            try:
                producer.publish(incident)
            except Exception as exc:  # noqa: BLE001 - publish must never crash the loop
                _LOG.warning("incident publish failed: %s", exc)
        set_last_incident(
            str(incident.get("root_cause_entity") or incident.get("incident_id") or "unknown")
        )
        return incident

    # ------------------------------------------------------------------ #
    # Kafka wiring
    # ------------------------------------------------------------------ #
    def _get_producer(self) -> Any:
        """Lazily build the CausalProducer (once); None when unavailable."""
        if self._producer is _PRODUCER_UNSET:
            try:
                from causal.causal_producer import CausalProducer

                self._producer = CausalProducer(settings=self._settings)
            except Exception as exc:  # noqa: BLE001 - simulation-first fallback
                _LOG.warning("causal producer unavailable; incidents not published: %s", exc)
                self._producer = None
        return self._producer

    def start_consumer(self, *, run_in_thread: bool = False) -> Any:
        """Wire the Kafka consumer loop to ``process_signal``.

        Runs the blocking consume loop when ``run_in_thread`` is False,
        otherwise starts a daemon thread and returns it.  Requires
        kafka-python-ng at call time; raises StorageError otherwise.
        """
        from causal.causal_consumer import CausalConsumer

        consumer = CausalConsumer(handler=self.process_signal, settings=self._settings)
        self._consumer = consumer
        if run_in_thread:
            thread = threading.Thread(
                target=consumer.run,
                name="omniwatch-causal-consumer",
                daemon=True,
            )
            thread.start()
            _LOG.info("causal consumer thread started")
            return thread
        consumer.run()
        return consumer

    def close(self) -> None:
        """Close the consumer (if running) and flush the producer."""
        if self._consumer is not None:
            try:
                self._consumer.close()
            except Exception:  # noqa: BLE001 - best-effort shutdown
                _LOG.debug("consumer close failed", exc_info=True)
            self._consumer = None
        producer = self._producer if self._producer is not _PRODUCER_UNSET else None
        if producer is not None:
            try:
                producer.close()
            except Exception:  # noqa: BLE001 - best-effort shutdown
                _LOG.debug("producer close failed", exc_info=True)
            self._producer = None


_PRODUCER_UNSET: Any = object()


# --------------------------------------------------------------------------- #
# Component health checks (all wrapped in try/except → never crash)
# --------------------------------------------------------------------------- #

def _check_kafka() -> bool:
    """Lightweight Kafka reachability check.  Returns False on any failure."""
    try:
        from kafka import KafkaProducer

        settings = Settings.from_env()
        producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            request_timeout_ms=2_000,
            max_block_ms=2_000,
        )
        producer.flush(timeout=2.0)
        producer.close(timeout=2.0)
        return True
    except Exception:  # noqa: BLE001 - health probe must never crash
        _LOG.debug("kafka health check failed", exc_info=True)
        return False


# --------------------------------------------------------------------------- #
# Health endpoint
# --------------------------------------------------------------------------- #

@app.get("/health")
async def health() -> dict[str, Any]:
    """Return component health status.

    Response shape::

        {
            "status": "healthy" | "degraded",
            "kafka": true | false,
            "graph_ready": true | false,
            "last_incident": "<root_cause_entity> at <ISO timestamp>" | "none"
        }
    """
    kafka_ok = _check_kafka()
    all_ok = kafka_ok and _graph_ready
    return {
        "status": "healthy" if all_ok else "degraded",
        "kafka": kafka_ok,
        "graph_ready": _graph_ready,
        "last_incident": _last_incident,
    }


# --------------------------------------------------------------------------- #
# Public setters for engine state → health endpoint
# --------------------------------------------------------------------------- #

def set_graph_ready(ready: bool) -> None:
    """Record whether the Two-Layer graph currently has any nodes."""
    global _graph_ready
    _graph_ready = bool(ready)


def set_last_incident(root_cause_entity: str) -> None:
    """Update the module-level last-incident record for /health."""
    global _last_incident
    _last_incident = f"{root_cause_entity} at {datetime.now(timezone.utc).isoformat()}"


# --------------------------------------------------------------------------- #
# Entry-point: consumer thread + uvicorn on 0.0.0.0:8008
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    engine = CausalEngine()
    try:
        engine.start_consumer(run_in_thread=True)
    except Exception as exc:  # noqa: BLE001 - keep serving health without Kafka
        _LOG.warning("kafka consumer unavailable; serving health only: %s", exc)
    uvicorn.run(
        "causal.causal_engine:app",
        host="0.0.0.0",
        port=8008,
        reload=False,
    )
