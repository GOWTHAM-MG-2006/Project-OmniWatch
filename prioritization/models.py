"""
OmniWatch — Incident Prioritization
Component: Data Models
Phase: 8
Purpose: Pydantic v2 models for RootCauseObject input contract and IncidentRecord output contract, plus confidence normalization utility.
Inputs: RootCauseObject dicts from Kafka topic omniwatch.incidents.causal (Phase 7 output)
Outputs: IncidentRecord for Kafka topic omniwatch.incidents.created; flat dict for ClickHouse storage
"""

from __future__ import annotations

import json
import logging
from typing import Any

from pydantic import BaseModel, ConfigDict

from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.prioritization.models")


def normalize_confidence(value: float) -> float:
    """Normalize confidence to 0..100 scale.

    Phase 7 outputs confidence on a 0..1 scale; the prioritization layer
    expects 0..100.  This function accepts *both* scales and always
    returns a value clamped to ``[0.0, 100.0]``:

    * ``value <= 1.0``  – treated as 0..1 fraction → ``value * 100.0``
    * ``value >= 100.0`` – clamped to 100.0
    * ``1.0 < value < 2.0`` – treated as 0..1 overshoot (e.g. 1.5
      represents 150 % which is impossible for a probability) → 100.0
    * ``2.0 <= value < 100.0`` – treated as an already-normalised 0..100
      percentage → returned unchanged
    """
    if value <= 1.0:
        return max(0.0, min(100.0, value * 100.0))
    # value > 1.0 — possibly 0..100 scale, but could be fraction overshoot
    if value < 2.0:
        return 100.0
    return max(0.0, min(100.0, float(value)))


class RootCauseObject(BaseModel):
    """Phase 7 output: flat root cause object matching AGENTS.md contract.

    Consumed by the prioritization layer (Phase 8) to compute severity
    and generate incident records.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    incident_id: str
    root_cause_entity: str
    entity_type: str
    confidence: float
    anomaly_score: float
    fault_path: list[str]
    impacted_services: list[str]
    impacted_count: int
    evidence: dict[str, Any]
    timestamp: str


class IncidentRecord(BaseModel):
    """Phase 8 output: prioritized incident record for Kafka + ClickHouse.

    Published to Kafka topic omniwatch.incidents.created and stored in
    ClickHouse incidents table. Contains nested RootCauseObject per
    AGENTS.md contract.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    incident_id: str
    created_at: str
    severity: str
    business_impact_score: float
    root_cause: RootCauseObject
    related_anomalies: list[Any]
    deduplicated_count: int
    sla_breach_risk: str
    assigned_to: str
    status: str


def flatten_for_clickhouse(incident: IncidentRecord) -> dict[str, Any]:
    """Flatten IncidentRecord to match ClickHouse INCIDENTS_COLUMNS schema.

    The ClickHouse incidents table expects a flat row with root_cause fields
    inlined. JSON-serializes fault_path and impacted_services per
    INCIDENTS_JSON_COLUMNS contract.

    Returns:
        dict with keys matching INCIDENTS_COLUMNS, ready for ClickHouse insert.
    """
    rc = incident.root_cause
    confidence = normalize_confidence(rc.confidence)

    row: dict[str, Any] = {
        "incident_id": incident.incident_id,
        "severity": incident.severity,
        "business_impact_score": incident.business_impact_score,
        "root_cause_entity": rc.root_cause_entity,
        "entity_type": rc.entity_type,
        "confidence": confidence,
        "fault_path": json.dumps(rc.fault_path),
        "impacted_services": json.dumps(rc.impacted_services),
        "status": incident.status,
        "deduplicated_count": incident.deduplicated_count,
        "sla_breach_risk": incident.sla_breach_risk,
        "assigned_to": incident.assigned_to,
        "created_at": incident.created_at,
    }
    return row
