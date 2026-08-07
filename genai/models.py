"""
OmniWatch — Generative AI Layer
Component: Data Models
Phase: 10
Purpose: Pydantic v2 models for grounded generation input/output, validation
         results, generated artifacts, and compliance report metadata — the
         wire format for the GenAI layer.
Inputs: RootCauseObject from causal engine (consumed, not re-defined)
Outputs: GroundedAnalysis, Runbook, PostMortem, GeneratedArtifact,
         ValidationReport, ComplianceReportMeta
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field


class RootCauseObject(BaseModel):
    """Root cause analysis output from the causal engine (Phase 7).

    This is the input to grounded generation — the LLM must cite ONLY
    entities present in this object.  Field names match the AGENTS.md
    RootCauseObject contract exactly.
    """

    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(description="UUID for this incident")
    root_cause_entity: str = Field(description="Entity identified as root cause")
    entity_type: str = Field(description="Type of the root cause entity")
    confidence: float = Field(description="Confidence score (0..100)")
    anomaly_score: float = Field(description="Anomaly score (0..1)")
    fault_path: list[str] = Field(description="Causal path from root to symptom")
    impacted_services: list[str] = Field(description="Services impacted by this incident")
    impacted_count: int = Field(description="Number of impacted entities")
    evidence: dict = Field(
        default_factory=dict,
        description="Evidence bundle: metrics, log_snippets, anomaly_timeline",
    )
    timestamp: str = Field(description="ISO 8601 timestamp")


class GroundedAnalysis(BaseModel):
    """Validated output from grounded LLM generation.

    The output_validator ensures every entity referenced in this analysis
    exists in the input RootCauseObject.
    """

    model_config = ConfigDict(extra="forbid")

    summary: str = Field(description="One-paragraph root cause summary")
    root_cause_entity: str = Field(description="Entity identified as root cause")
    confidence: float = Field(description="LLM confidence (0..100)")
    recommended_actions: list[str] = Field(description="Recommended remediation actions")
    impacted_entities: list[str] = Field(description="Entities impacted")
    reasoning: str = Field(description="Step-by-step reasoning")


class ValidationReport(BaseModel):
    """Result of post-generation entity validation.

    The output_validator produces this to indicate whether the LLM output
    is grounded (all entities exist in the input RootCauseObject) or contains
    hallucinated references.
    """

    model_config = ConfigDict(extra="forbid")

    valid: bool = Field(description="True if all referenced entities are grounded")
    hallucinated_entities: list[str] = Field(
        default_factory=list,
        description="Entity names/ids referenced in output but NOT in RootCauseObject",
    )
    grounded_entities: list[str] = Field(
        default_factory=list,
        description="Entity names/ids that ARE present in RootCauseObject",
    )
    attempt: int = Field(default=1, description="Which generation attempt this validation covers")


class ComplianceReportMeta(BaseModel):
    """Metadata for a generated compliance report.

    Written to MinIO omniwatch-audit-logs alongside the Markdown content.
    """

    model_config = ConfigDict(extra="forbid")

    report_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique report identifier",
    )
    incident_id: str = Field(description="Incident this report covers")
    report_type: str = Field(description="Report type name")
    framework: str = Field(description="Compliance framework (SOC2, ISO27001, HIPAA, PCI-DSS)")
    generated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 generation timestamp",
    )
    bucket: str = Field(
        default="omniwatch-audit-logs",
        description="MinIO bucket where the report is stored",
    )
    object_key: str = Field(description="MinIO object key for the Markdown report")


class GroundedArtifact(BaseModel):
    """Base model for any LLM-generated artifact persisted to MinIO."""

    model_config = ConfigDict(extra="forbid")

    artifact_id: str = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique artifact identifier",
    )
    incident_id: str = Field(description="Incident this artifact relates to")
    artifact_type: str = Field(description="summary | runbook | report | postmortem")
    content: str = Field(description="Generated markdown/text content")
    generated_at: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 generation timestamp",
    )
    model_used: str = Field(default="qwen3:8b", description="LLM model used")
    grounded: bool = Field(default=True, description="Whether output passed validation")


class Runbook(GroundedArtifact):
    """Step-by-step remediation runbook generated for an incident."""

    model_config = ConfigDict(extra="forbid")

    artifact_type: str = Field(default="runbook", description="Always 'runbook'")
    steps: list[str] = Field(description="Ordered remediation steps")
    estimated_duration_minutes: int = Field(
        default=15, description="Estimated time to complete all steps"
    )
    severity: str = Field(default="P2", description="Incident severity (P1-P4)")


class PostMortem(GroundedArtifact):
    """Post-incident analysis / post-mortem report."""

    model_config = ConfigDict(extra="forbid")

    artifact_type: str = Field(default="postmortem", description="Always 'postmortem'")
    timeline: list[str] = Field(description="Event timeline")
    root_cause_summary: str = Field(description="One-paragraph root cause")
    lessons_learned: list[str] = Field(description="Key takeaways")
    action_items: list[str] = Field(description="Follow-up actions")


class GeneratedReport(GroundedArtifact):
    """Executive or compliance report generated for stakeholders."""

    model_config = ConfigDict(extra="forbid")

    artifact_type: str = Field(default="report", description="Always 'report'")
    report_type: str = Field(
        description="executive_summary | compliance | incident_response"
    )
    audience: str = Field(default="executives", description="Target audience")
    sections: list[dict[str, str]] = Field(
        default_factory=list,
        description="Report sections as [{title, content}]",
    )
