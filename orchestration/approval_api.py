"""
OmniWatch — Orchestration + Policy
Component: Approval API Router
Phase: 9
Purpose: FastAPI Router exposing human-in-the-loop approval endpoints for
         remediation actions routed to the approval path by the orchestrator.
         GET /pending-approvals lists undecided requests; POST /approve/{id}
         and POST /deny/{id} record decisions idempotently.
Inputs: Pending approval rows from ClickHouse omniwatch.pending_approvals
Outputs: JSON responses conforming to the ApprovalRecord shape; decision
         writes back to ClickHouse; denials also publish to the learning loop
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.orchestration.approval_api")

router = APIRouter(prefix="/api/v1", tags=["approval"])

# ---------------------------------------------------------------------------
# Dependency-injected callables (set via configure() at startup)
# ---------------------------------------------------------------------------

# Callable that returns all pending approval rows from ClickHouse.
# Signature: () -> list[dict[str, Any]]
_select_pending_fn: Callable[[], list[dict[str, Any]]] | None = None

# Callable that updates a single approval row's decision.
# Signature: (approval_id: str, decision: str, decided_at: str) -> bool
_update_decision_fn: Callable[[str, str, str], bool] | None = None

# Callable that publishes a denial to the learning loop (Kafka topic).
# Signature: (record: dict[str, Any]) -> None
_learning_producer_fn: Callable[[dict[str, Any]], None] | None = None


# ---------------------------------------------------------------------------
# Request/response schemas
# ---------------------------------------------------------------------------

class ApprovalDecisionResponse(BaseModel):
    """Response for approve/deny endpoints."""

    approval_id: str
    status: str
    decided_at: str
    message: str


# ---------------------------------------------------------------------------
# Configuration — inject real dependencies at startup
# ---------------------------------------------------------------------------

def configure(
    select_pending: Callable[[], list[dict[str, Any]]] | None = None,
    update_decision: Callable[[str, str, str], bool] | None = None,
    learning_producer: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    """Inject dependency callables for ClickHouse and learning producer.

    Called once at startup by the orchestration engine. When ``None``,
    the corresponding functionality is disabled gracefully.
    """
    global _select_pending_fn, _update_decision_fn, _learning_producer_fn  # noqa: PLW0603
    _select_pending_fn = select_pending
    _update_decision_fn = update_decision
    _learning_producer_fn = learning_producer
    _LOG.info(
        "approval_api configured: select_pending=%s update_decision=%s learning_producer=%s",
        _select_pending_fn is not None,
        _update_decision_fn is not None,
        _learning_producer_fn is not None,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    """Return the current UTC time as an ISO 8601 string."""
    return datetime.now(timezone.utc).isoformat()


def _pending_rows() -> list[dict[str, Any]]:
    """Fetch pending approval rows — fail-soft, returns empty on error."""
    if _select_pending_fn is None:
        _LOG.warning("select_pending_fn not configured — returning empty list")
        return []
    try:
        return _select_pending_fn()
    except Exception as exc:  # noqa: BLE001
        _LOG.error("failed to fetch pending approvals: %s", exc)
        return []


def _update_approval(
    approval_id: str, decision: str, decided_at: str
) -> bool:
    """Write decision back to ClickHouse — fail-soft, returns False on error."""
    if _update_decision_fn is None:
        _LOG.warning("update_decision_fn not configured — cannot persist decision")
        return False
    try:
        return _update_decision_fn(approval_id, decision, decided_at)
    except Exception as exc:  # noqa: BLE001
        _LOG.error(
            "failed to update approval %s decision=%s: %s",
            approval_id,
            decision,
            exc,
        )
        return False


def _publish_denial(record: dict[str, Any]) -> None:
    """Publish denial to the learning loop — fail-soft, never raises."""
    if _learning_producer_fn is None:
        _LOG.debug("learning_producer not configured — skipping denial publish")
        return
    try:
        _learning_producer_fn(record)
    except Exception as exc:  # noqa: BLE001
        _LOG.warning(
            "failed to publish denial for approval_id=%s: %s",
            record.get("approval_id"),
            exc,
        )


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@router.get("/pending-approvals")
def list_pending_approvals() -> list[dict[str, Any]]:
    """Return all pending (undecided) approval requests.

    Each row contains: approval_id, incident_id, action_type, entity_id,
    proposed_by, status, created_at, decided_at (None while pending).
    """
    rows = _pending_rows()
    # Filter to only truly pending rows (防御 in depth)
    pending = [r for r in rows if r.get("status") == "pending"]
    _LOG.info(
        "GET /pending-approvals: returning %d pending of %d total",
        len(pending),
        len(rows),
    )
    return pending


@router.post("/approve/{approval_id}", response_model=ApprovalDecisionResponse)
def approve_action(approval_id: str) -> ApprovalDecisionResponse:
    """Approve a pending remediation action.

    Idempotent: if already decided, returns the current state.
    Sets ``status=approved`` and ``decided_at`` in ClickHouse.
    """
    rows = _pending_rows()
    existing = next((r for r in rows if r.get("approval_id") == approval_id), None)

    if existing is None:
        raise HTTPException(
            status_code=404,
            detail=f"approval {approval_id} not found",
        )

    current_status = existing.get("status", "pending")
    if current_status != "pending":
        decided_at = existing.get("decided_at", _now_iso())
        _LOG.info(
            "approve/%s: already decided (%s) — returning current",
            approval_id,
            current_status,
        )
        return ApprovalDecisionResponse(
            approval_id=approval_id,
            status=current_status,
            decided_at=decided_at,
            message=f"already {current_status}",
        )

    decided_at = _now_iso()
    _update_approval(approval_id, "APPROVED", decided_at)

    _LOG.info("approve/%s: APPROVED at %s", approval_id, decided_at)
    return ApprovalDecisionResponse(
        approval_id=approval_id,
        status="APPROVED",
        decided_at=decided_at,
        message="action approved",
    )


@router.post("/deny/{approval_id}", response_model=ApprovalDecisionResponse)
def deny_action(approval_id: str) -> ApprovalDecisionResponse:
    """Deny a pending remediation action.

    Idempotent: if already decided, returns the current state.
    Sets ``status=denied`` and ``decided_at`` in ClickHouse,
    then publishes the denial to the learning loop.
    """
    rows = _pending_rows()
    existing = next((r for r in rows if r.get("approval_id") == approval_id), None)

    if existing is None:
        raise HTTPException(
            status_code=404,
            detail=f"approval {approval_id} not found",
        )

    current_status = existing.get("status", "pending")
    if current_status != "pending":
        decided_at = existing.get("decided_at", _now_iso())
        _LOG.info(
            "deny/%s: already decided (%s) — returning current",
            approval_id,
            current_status,
        )
        return ApprovalDecisionResponse(
            approval_id=approval_id,
            status=current_status,
            decided_at=decided_at,
            message=f"already {current_status}",
        )

    decided_at = _now_iso()
    _update_approval(approval_id, "DENIED", decided_at)

    # Publish denial to learning loop
    denial_record = {
        "approval_id": approval_id,
        "incident_id": existing.get("incident_id"),
        "action_type": existing.get("action_type"),
        "entity_id": existing.get("entity_id"),
        "decision": "DENIED",
        "decided_at": decided_at,
    }
    _publish_denial(denial_record)

    _LOG.info("deny/%s: DENIED at %s", approval_id, decided_at)
    return ApprovalDecisionResponse(
        approval_id=approval_id,
        status="DENIED",
        decided_at=decided_at,
        message="action denied",
    )
