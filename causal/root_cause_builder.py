"""
OmniWatch — Causal Graph Engine
Component: Root Cause Object Builder
Phase: 7
Purpose: Assemble the full RootCauseObject data contract from an AnomalySignal
         (Kafka omniwatch.anomalies.detected), the DagTraversal.analyze() result,
         and optional extra evidence (metrics dict, log snippets, related
         anomaly timeline).  Degrades gracefully: when traversal finds no
         candidate root cause (no graph evidence above min_confidence), the
         symptom entity itself is reported as the root with confidence 0.0 so
         the pipeline still produces an incident for prioritization.
Inputs: anomaly_signal dict (entity_id, entity_type, metric_name,
        anomaly_score 0..1, confidence 0..100, timestamp ISO,
        deviation_from_baseline, source_type); traversal_result dict from
        dag_traversal.DagTraversal.analyze(); optional metrics/log_snippets/
        related_anomalies; optional pre-generated incident_id (UUID).
Outputs: RootCauseObject dict matching the AGENTS.md data contract:
         incident_id (UUID), root_cause_entity, entity_type, confidence,
         anomaly_score, fault_path [root -> ... -> symptom],
         impacted_services, impacted_count, evidence {metrics, log_snippets,
         anomaly_timeline}, timestamp ISO.  Consumed by causal_engine.py and
         asserted by the E2E test.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any

from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.root_cause_builder")


class RootCauseBuilder:
    """Package a RootCauseObject from anomaly + traversal evidence."""

    def build(
        self,
        anomaly_signal: dict[str, Any],
        traversal_result: dict[str, Any],
        *,
        metrics: dict[str, Any] | None = None,
        log_snippets: list[str] | None = None,
        related_anomalies: list[dict[str, Any]] | None = None,
        incident_id: str | None = None,
    ) -> dict[str, Any]:
        """Assemble the RootCauseObject contract (all required fields).

        ``traversal_result`` is the dict produced by
        ``DagTraversal.analyze()``: symptom, path_order, min_confidence,
        root_cause (str | None), confidence (0..1), fault_path,
        impacted_services, candidates.  When no candidate passed the
        confidence gate, the symptom entity degrades into the root cause so
        the downstream prioritization layer always receives an incident.
        """
        if not isinstance(anomaly_signal, dict) or not anomaly_signal.get("entity_id"):
            raise StorageError("RootCauseBuilder.build requires anomaly_signal with entity_id")

        traversal = traversal_result if isinstance(traversal_result, dict) else {}

        # Prefer the traversal's (already canonicalized) symptom so a degraded
        # incident keeps the cross-cloud id the engine resolved, rather than the
        # raw signal entity_id. Falls back to the raw id when unavailable.
        symptom = str(traversal.get("symptom") or anomaly_signal["entity_id"])

        root_cause = traversal.get("root_cause")
        candidates = traversal.get("candidates") or []
        candidate = candidates[0] if candidates else {}

        degraded = not root_cause
        if degraded:
            root_cause = symptom
            _LOG.warning(
                "no causal candidate for symptom '%s'; degrading to symptom root (confidence 0.0)",
                symptom,
            )

        # The graph's entity_type() returns the truthy "UNKNOWN" sentinel for
        # nodes only present in Layer-2 (learned causality). Treat that sentinel
        # as absent so the caller-provided signal entity_type is preferred
        # before degrading to "UNKNOWN" (design intent of the fallback chain).
        candidate_type = candidate.get("entity_type")
        if not candidate_type or candidate_type == "UNKNOWN":
            candidate_type = anomaly_signal.get("entity_type")
        entity_type = str(candidate_type or "UNKNOWN")
        confidence = float(traversal.get("confidence") or 0.0)
        anomaly_score = float(anomaly_signal.get("anomaly_score") or 0.0)
        fault_path = list(traversal.get("fault_path") or []) or [root_cause]
        impacted_services = list(traversal.get("impacted_services") or []) or [symptom]
        if degraded:
            fault_path = [root_cause]
            impacted_services = [symptom]

        return {
            "incident_id": incident_id or str(uuid.uuid4()),
            "root_cause_entity": root_cause,
            "entity_type": entity_type,
            "confidence": confidence,
            "anomaly_score": anomaly_score,
            "fault_path": fault_path,
            "impacted_services": impacted_services,
            "impacted_count": len(impacted_services),
            "evidence": {
                "metrics": self._build_metrics(anomaly_signal, metrics),
                "log_snippets": list(log_snippets) if log_snippets else [],
                "anomaly_timeline": self._build_timeline(anomaly_signal, related_anomalies),
            },
            "timestamp": self._timestamp(anomaly_signal),
        }

    # ------------------------------------------------------------------ #
    # Evidence assembly
    # ------------------------------------------------------------------ #
    @staticmethod
    def _build_metrics(
        anomaly_signal: dict[str, Any], extra: dict[str, Any] | None
    ) -> dict[str, Any]:
        """Metric evidence: the offending metric plus any caller-supplied set."""
        metrics: dict[str, Any] = {}
        metric_name = anomaly_signal.get("metric_name")
        if metric_name:
            metrics[str(metric_name)] = {
                "anomaly_score": anomaly_signal.get("anomaly_score"),
                "deviation_from_baseline": anomaly_signal.get("deviation_from_baseline"),
            }
        if extra:
            metrics.update(extra)
        return metrics

    @staticmethod
    def _build_timeline(
        anomaly_signal: dict[str, Any], related: list[dict[str, Any]] | None
    ) -> list[dict[str, Any]]:
        """Chronological anomaly timeline; falls back to the triggering signal."""
        entries = related if related is not None else []
        if not entries:
            entries = [
                {
                    "timestamp": anomaly_signal.get("timestamp"),
                    "entity_id": anomaly_signal.get("entity_id"),
                    "anomaly_score": anomaly_signal.get("anomaly_score"),
                    "source_type": anomaly_signal.get("source_type"),
                }
            ]
        return sorted(
            entries,
            key=lambda e: str(e.get("timestamp") or ""),
        )

    @staticmethod
    def _timestamp(anomaly_signal: dict[str, Any]) -> str:
        ts = anomaly_signal.get("timestamp")
        if ts:
            return str(ts)
        return datetime.now(timezone.utc).isoformat()
