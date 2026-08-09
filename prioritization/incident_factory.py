"""
OmniWatch — Incident Prioritization
Component: Incident Factory
Phase: 8
Purpose: Assemble IncidentRecord objects from RootCauseObject, applying
         severity classification, impact scoring, SLA risk, and
         assignment routing. Archives full incident JSON to MinIO.
Inputs: RootCauseObject (dict), SeverityClassifier, ImpactScorer,
        SlaRiskCalculator, MinioClient
Outputs: IncidentRecord (pydantic model) published to Kafka;
         full JSON archived to MinIO omniwatch-incidents bucket
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from prioritization.config.settings import Settings
from prioritization.impact_scorer import ImpactScorer
from prioritization.models import IncidentRecord, RootCauseObject, normalize_confidence
from prioritization.severity_classifier import SeverityClassifier
from prioritization.sla_risk_calculator import SlaRiskCalculator
from storage.common import StorageError, create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.incident_factory")

# Assignment rule: P1 + confidence ≥ 0.85 (0..1 scale = 85.0 on 0..100) → auto-remediation
_AUTO_REMEDIATION_SEVERITY = "P1"
_AUTO_REMEDIATION_CONFIDENCE_THRESHOLD = 85.0  # 0..100 scale
_FALLBACK_ASSIGNEE = "oncall-engineer"
_MINIO_INCIDENTS_BUCKET = "omniwatch-incidents"


class IncidentFactory:
    """Builds prioritized IncidentRecord objects from RootCauseObjects.

    The factory orchestrates the classification pipeline:
    1. Severity classification (P1–P4)
    2. Business impact scoring (0–100)
    3. SLA risk calculation (HIGH/MEDIUM/LOW)
    4. Assignment routing (auto-remediation vs oncall-engineer)
    5. Full incident JSON archival to MinIO

    Args:
        severity_classifier: Configured SeverityClassifier instance.
        impact_scorer: Configured ImpactScorer instance.
        sla_calculator: Configured SlaRiskCalculator instance.
        minio_client: Optional MinIO client for evidence archival.
        settings: Optional Settings (for bucket name overrides).
    """

    def __init__(
        self,
        severity_classifier: Optional[SeverityClassifier] = None,
        impact_scorer: Optional[ImpactScorer] = None,
        sla_calculator: Optional[SlaRiskCalculator] = None,
        minio_client: Any = None,
        settings: Optional[Settings] = None,
    ) -> None:
        self._classifier = severity_classifier or SeverityClassifier()
        self._impact_scorer = impact_scorer or ImpactScorer()
        self._sla_calculator = sla_calculator or SlaRiskCalculator()
        self._minio = minio_client
        self._settings = settings or Settings.from_env()
        self._incidents_bucket = (
            getattr(self._settings, "minio_incidents_bucket", None)
            or os.environ.get("MINIO_INCIDENTS_BUCKET", _MINIO_INCIDENTS_BUCKET)
        )

    def create(self, root_cause: RootCauseObject | dict[str, Any]) -> IncidentRecord:
        """Build a complete IncidentRecord from a RootCauseObject.

        Args:
            root_cause: Phase 7 RootCauseObject dict or Pydantic model.

        Returns:
            A fully populated IncidentRecord with status="OPEN".
        """
        # Normalize to RootCauseObject Pydantic model
        if isinstance(root_cause, dict):
            rc = RootCauseObject(**root_cause)
        elif isinstance(root_cause, RootCauseObject):
            rc = root_cause
        else:
            raise StorageError(
                f"IncidentFactory.create expected RootCauseObject or dict, got {type(root_cause).__name__}"
            )

        # 1. Severity classification
        severity = self._classifier.classify(rc)

        # 2. Business impact score
        business_impact = self._impact_scorer.score(rc, severity)

        # 3. SLA breach risk
        sla_risk = self._sla_calculator.calculate(severity, business_impact)

        # 4. Assignment routing
        confidence_normalized = normalize_confidence(rc.confidence)
        assigned_to = self._assign(severity, confidence_normalized)

        # 5. Build the IncidentRecord
        timestamp = rc.timestamp or datetime.now(timezone.utc).isoformat()
        incident = IncidentRecord(
            incident_id=str(uuid4()),
            created_at=timestamp,
            severity=severity,
            business_impact_score=round(business_impact, 2),
            root_cause=rc,
            related_anomalies=[],
            deduplicated_count=1,
            sla_breach_risk=sla_risk,
            assigned_to=assigned_to,
            status="OPEN",
        )

        _LOG.info(
            "incident created: id=%s severity=%s impact=%.1f sla=%s assigned_to=%s entity=%s",
            incident.incident_id,
            severity,
            business_impact,
            sla_risk,
            assigned_to,
            rc.root_cause_entity,
        )

        # 6. Archive full incident JSON to MinIO (best-effort)
        self._archive_to_minio(incident)

        # 7. Persist to ClickHouse (best-effort)
        self._persist_to_clickhouse(incident)

        return incident

    def _assign(self, severity: str, confidence_normalized: float) -> str:
        """Determine assignment target per classification_rules.yaml.

        P1 with confidence ≥ 85.0 (0..100) → "auto-remediation";
        otherwise → "oncall-engineer".
        """
        if (
            severity == _AUTO_REMEDIATION_SEVERITY
            and confidence_normalized >= _AUTO_REMEDIATION_CONFIDENCE_THRESHOLD
        ):
            return "auto-remediation"
        return _FALLBACK_ASSIGNEE

    def _archive_to_minio(self, incident: IncidentRecord) -> None:
        """Archive the full IncidentRecord JSON to MinIO (best-effort).

        Stores to ``{bucket}/{incident_id}.json``.  Failures are logged
        but never block incident creation — the incident is still published
        to Kafka.  This is the evidence-preservation contract (S2/MEDIUM).
        """
        if self._minio is None:
            _LOG.debug(
                "minio client not configured, skipping archive for incident %s",
                incident.incident_id,
            )
            return

        try:
            data = json.dumps(incident.model_dump(), default=str, indent=2).encode("utf-8")
            object_name = f"{incident.incident_id}.json"
            self._minio.upload_object(
                bucket=self._incidents_bucket,
                object_name=object_name,
                data=data,
                content_type="application/json",
            )
            _LOG.info(
                "archived incident %s to MinIO %s/%s",
                incident.incident_id,
                self._incidents_bucket,
                object_name,
            )
        except Exception as exc:  # noqa: BLE001 - best-effort archival
            _LOG.warning(
                "failed to archive incident %s to MinIO: %s",
                incident.incident_id,
                exc,
            )

    def _persist_to_clickhouse(self, incident: IncidentRecord) -> None:
        """Persist the incident record to ClickHouse (best-effort).

        Uses ``flatten_for_clickhouse`` from ``prioritization.models`` to
        produce the flat row schema expected by ``omniwatch.incidents``.
        Failures are logged but never block incident creation — the incident
        is still published to Kafka and archived to MinIO.
        """
        try:
            from storage.clickhouse.client import ClickHouseClient
            from storage.config import StorageConfig
            from prioritization.models import flatten_for_clickhouse

            cfg = StorageConfig.from_env()
            client = ClickHouseClient(config=cfg)
            try:
                row = flatten_for_clickhouse(incident)
                client.insert_incidents([row])
                _LOG.info(
                    "persisted incident %s to ClickHouse",
                    incident.incident_id,
                )
            finally:
                client.close()
        except Exception as exc:  # noqa: BLE001 - best-effort persistence
            _LOG.warning(
                "failed to persist incident %s to ClickHouse: %s",
                incident.incident_id,
                exc,
            )
