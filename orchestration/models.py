"""
OmniWatch — Orchestration + Policy
Component: Data Models
Phase: 9
Purpose: Pydantic v2 models for orchestration decisions, action results, and
         human-in-the-loop approval records — the wire format for Kafka and
         the contract consumed by later stages (approval API, dashboard, learning).
Inputs: IncidentRecord from prioritization.models (consumed, not re-defined)
Outputs: ActionResult published to omniwatch.remediation.actions; ApprovalRecord
         stored in ClickHouse pending_approvals table
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class OrchestrationDecision(BaseModel):
    """OPA policy evaluation result for a proposed remediation action.

    Returned by the OPA decision client after evaluating the action against
    the loaded Rego policies and the incident context.
    """

    model_config = ConfigDict(extra="forbid")

    allow: bool = Field(default=False, description="Whether the action is permitted")
    needs_approval: bool = Field(
        default=False,
        description="Whether human approval is required before execution",
    )
    reason: str = Field(
        default="",
        description="Human-readable explanation for the decision",
    )


class ActionResult(BaseModel):
    """Record of a single remediation action execution.

    Published to Kafka topic ``omniwatch.remediation.actions`` and stored in
    ClickHouse.  This is the primary output of the orchestration layer —
    consumed by the dashboard (Phase 11), the learning loop (Phase 11), and
    the compliance reporter (Phase 10, GAP 2).

    Fields are ordered to match the AGENTS.md ActionResult contract.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    action_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique action identifier (UUID string for Kafka JSON)",
    )
    incident_id: str = Field(
        description="Incident this action addresses",
    )
    action_type: str = Field(
        description="Action category (e.g. restart, rollback, scale, block_ip)",
    )
    entity_id: str = Field(
        description="Target entity for the action",
    )
    entity_type: str = Field(
        description="Entity type (SERVICE_NODE, DATABASE_NODE, etc.)",
    )
    success: bool = Field(
        description="Whether the action executed successfully",
    )
    output: str = Field(
        description="Stdout / result text from the action executor",
    )
    error: str | None = Field(
        default=None,
        description="Error message if the action failed",
    )
    execution_time_seconds: float = Field(
        description="Wall-clock time of the action execution",
    )
    executed_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp of execution",
    )
    triggered_by: str = Field(
        description='Who triggered the action: "auto" or "human:{name}"',
    )
    dry_run: bool = Field(
        default=False,
        description="True if the action was a simulation only",
    )
    needs_approval: bool = Field(
        default=False,
        description="Whether this action required human approval",
    )
    approval_id: str | None = Field(
        default=None,
        description="Approval record ID if human approval was used",
    )
    severity: str = Field(
        description="Incident severity (P1–P4)",
    )
    confidence: float = Field(
        description="Root cause confidence (0..100 scale)",
    )
    archived: bool = Field(
        default=False,
        description="True if the action record has been archived to MinIO",
    )


class ApprovalRecord(BaseModel):
    """Human-in-the-loop approval request for high-severity or risky actions.

    Stored in ClickHouse ``pending_approvals`` table and exposed via the
    approval API (later stages).  The orchestrator creates this record when
    ``OrchestrationDecision.needs_approval`` is True and waits for a human
    to approve/reject before executing the action.
    """

    model_config = ConfigDict(extra="forbid", populate_by_name=True)

    approval_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique approval request identifier",
    )
    incident_id: str = Field(
        description="Incident that triggered this approval request",
    )
    action_type: str = Field(
        description="Proposed action type",
    )
    entity_id: str = Field(
        description="Target entity for the proposed action",
    )
    proposed_by: str = Field(
        default="auto-remediation",
        description="Who proposed the action (auto-remediation or engineer name)",
    )
    status: str = Field(
        default="pending",
        description="Approval status: pending, approved, or rejected",
    )
    created_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp when the approval was created",
    )
    decided_at: str | None = Field(
        default=None,
        description="ISO 8601 timestamp when the approval was decided",
    )
    incident_severity: str = Field(
        description="Severity of the associated incident (P1–P4)",
    )
    incident_entity: str = Field(
        description="Entity that caused the incident",
    )
    opa_reason: str = Field(
        default="",
        description="OPA policy reason requiring human approval",
    )
