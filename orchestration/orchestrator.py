"""
OmniWatch — Orchestration + Policy
Component: Orchestrator Engine
Phase: 9
Purpose: Core auto-remediation orchestrator implementing the 7-step pipeline:
         consume → enrich → OPA decide → route → execute/pend → publish → audit.
         Supports retry with exponential backoff, human-in-the-loop approval,
         fail-closed deny, and audit archiving to MinIO.
Inputs: IncidentRecord dicts from the orchestration consumer
Outputs: ActionResult (published to Kafka), ApprovalRecord (stored in ClickHouse)
"""

from __future__ import annotations

import json
import logging
import time
from typing import Any, Callable

from orchestration.action_library import ActionDefinition, _REGISTRY
from orchestration.models import ActionResult, ApprovalRecord, OrchestrationDecision
from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.orchestration.orchestrator")

# ---------------------------------------------------------------------------
# Routing constants
# ---------------------------------------------------------------------------

# Actions that ALWAYS require human approval regardless of OPA decision
_ALWAYS_APPROVAL: frozenset[str] = frozenset({"block_ip", "rotate_credentials"})

# Retry constants: delay = min(BASE_DELAY * MULTIPLIER^attempt, MAX_DELAY)
# 3 total attempts (1 initial + 2 retries)
_MAX_RETRIES: int = 2
_RETRY_BASE_DELAY: float = 1.0
_RETRY_MULTIPLIER: float = 5.0
_RETRY_MAX_DELAY: float = 25.0


# ---------------------------------------------------------------------------
# Orchestrator — 7-step pipeline engine
# ---------------------------------------------------------------------------

class Orchestrator:
    """Core auto-remediation orchestrator — the 7-step pipeline engine.

    Steps:
      1. Consume: incident dict passed via ``handle_message()``
      2. Enrich: extract entity_id, entity_type, severity, confidence
      3. OPA decide: evaluate each available action against policy
      4. Route: classify as auto / approval / deny
      5. Execute (auto) with retry, or pend (approval) with ClickHouse store
      6. Publish: ActionResult to Kafka via producer
      7. Audit: archive ActionResult to MinIO

    Routing rules (D1-D13):
      - ``block_ip`` / ``rotate_credentials`` → ALWAYS approval path
      - OPA allow AND (confidence > 95 OR severity P1/P2) → auto-execute
      - OPA needs_approval → approval path
      - Otherwise → deny (fail-closed)

    Args:
        opa: OPA decision client with ``.decide(incident, action_type)`` method.
        executor: Action executor with ``.execute(action_def, incident)`` method.
        producer: Kafka producer with ``.publish_action_result(dict)`` method.
        clickhouse_fn: Optional callable to store approval records
            (e.g. ``storage.clickhouse.client.insert_pending_approvals``).
            If ``None``, approval records are logged but not persisted.
        archiver: Optional MinIO client with
            ``.upload_object(bucket, name, data, content_type)``.
            If ``None``, audit archiving is skipped silently.
    """

    def __init__(
        self,
        opa: Any,
        executor: Any,
        producer: Any,
        clickhouse_fn: Callable[[dict], int] | None = None,
        archiver: Any = None,
    ) -> None:
        self._opa = opa
        self._executor = executor
        self._producer = producer
        self._clickhouse_fn = clickhouse_fn
        self._archiver = archiver

    # ------------------------------------------------------------------
    # Step 1: handle_message — entry point (called by consumer callback)
    # ------------------------------------------------------------------

    def handle_message(
        self, incident: dict[str, Any]
    ) -> ActionResult | ApprovalRecord | None:
        """Process a single incident through the 7-step pipeline.

        Returns:
            ``ActionResult`` if an action was auto-executed (success or failure).
            ``ApprovalRecord`` if the action requires human approval.
            ``None`` if no action was taken (denied or no actions available).
        """
        incident_id = incident.get("incident_id", "unknown")
        entity_id = incident.get("root_cause", {}).get("root_cause_entity", "unknown")
        entity_type = incident.get("root_cause", {}).get("entity_type", "unknown")
        severity = incident.get("severity", "UNKNOWN")
        confidence = incident.get("root_cause", {}).get("confidence", 0.0)

        _LOG.info(
            "orchestrator: processing incident=%s entity=%s severity=%s confidence=%.1f",
            incident_id,
            entity_id,
            severity,
            confidence,
        )

        # Step 2: Enrich — get ALL available actions (safe + unsafe)
        all_actions = self._get_all_actions(entity_type)
        if not all_actions:
            _LOG.warning(
                "orchestrator: no actions registered for entity_type=%s",
                entity_type,
            )
            return None

        # Steps 3 & 4: OPA decide + route for each action
        auto_candidate: tuple[ActionDefinition, OrchestrationDecision] | None = None
        approval_candidate: tuple[ActionDefinition, OrchestrationDecision] | None = None

        for action_def in all_actions:
            raw_decision = self._opa.decide(incident, action_def.action_type)
            decision = self._normalize_decision(raw_decision)
            route = self._route(
                decision, severity, confidence, action_def.action_type
            )

            _LOG.debug(
                "orchestrator: action=%s route=%s allow=%s needs_approval=%s",
                action_def.action_type,
                route,
                decision.allow,
                decision.needs_approval,
            )

            if route == "auto" and auto_candidate is None:
                auto_candidate = (action_def, decision)
            elif route == "approval" and approval_candidate is None:
                approval_candidate = (action_def, decision)

        # Step 5: Execute or pend
        if auto_candidate is not None:
            action_def, decision = auto_candidate
            result = self._handle_auto(incident, action_def, decision)
            # Step 6: Publish
            if result is not None:
                self._publish(result)
                # Step 7: Audit
                self._audit(result, incident)
            return result

        if approval_candidate is not None:
            action_def, decision = approval_candidate
            record = self._handle_approval(incident, action_def, decision)
            return record

        # Deny — fail-closed (D7)
        _LOG.warning(
            "orchestrator: DENY — no auto or approval action for "
            "incident=%s entity_type=%s",
            incident_id,
            entity_type,
        )
        return None

    # ------------------------------------------------------------------
    # Step 3 helpers: normalize + route
    # ------------------------------------------------------------------

    @staticmethod
    def _normalize_decision(raw: Any) -> OrchestrationDecision:
        """Normalize OPA result to ``OrchestrationDecision``.

        Handles:
          - ``OrchestrationDecision`` object (from real ``OPADecisionClient``)
          - ``dict`` with ``"result"`` key (from ``FakeOPAClient``)
          - ``dict`` with direct ``allow``/``needs_approval``/``reason`` keys
        """
        if isinstance(raw, OrchestrationDecision):
            return raw

        if isinstance(raw, dict):
            # FakeOPAClient wraps in {"result": {...}}
            data = raw.get("result", raw)
            return OrchestrationDecision(
                allow=bool(data.get("allow", False)),
                needs_approval=bool(data.get("needs_approval", False)),
                reason=str(data.get("reason", "")),
            )

        # Fallback — deny
        return OrchestrationDecision(
            allow=False,
            needs_approval=False,
            reason=f"unexpected decision type: {type(raw).__name__}",
        )

    @staticmethod
    def _route(
        decision: OrchestrationDecision,
        severity: str,
        confidence: float,
        action_type: str,
    ) -> str:
        """Classify action routing: ``"auto"``, ``"approval"``, or ``"deny"``.

        Rules (non-negotiable, D1-D13):
          1. ``block_ip`` / ``rotate_credentials`` → ALWAYS ``"approval"``
          2. OPA allow AND (confidence > 95 OR severity P1/P2) → ``"auto"``
          3. OPA needs_approval → ``"approval"``
          4. Otherwise → ``"deny"`` (fail-closed)
        """
        if action_type in _ALWAYS_APPROVAL:
            return "approval"

        if decision.allow and (confidence > 95.0 or severity in ("P1", "P2")):
            return "auto"

        if decision.needs_approval:
            return "approval"

        return "deny"

    # ------------------------------------------------------------------
    # Step 5: execute (auto) / pend (approval)
    # ------------------------------------------------------------------

    def _handle_auto(
        self,
        incident: dict[str, Any],
        action_def: ActionDefinition,
        decision: OrchestrationDecision,
    ) -> ActionResult | None:
        """Execute an action with retry on failure.

        Retries up to ``_MAX_RETRIES`` times with exponential backoff:
        ``delay = min(_RETRY_BASE_DELAY * _RETRY_MULTIPLIER^attempt, _RETRY_MAX_DELAY)``

        Returns the ``ActionResult`` from the final attempt (success or failure).
        """
        incident_id = incident["incident_id"]
        entity_id = incident["root_cause"]["root_cause_entity"]
        entity_type = incident["root_cause"]["entity_type"]
        severity = incident["severity"]
        confidence = incident["root_cause"]["confidence"]

        action_dict = {
            "action_type": action_def.action_type,
            "entity_type": action_def.entity_type,
            "safe": action_def.safe,
            "description": action_def.description,
        }

        last_result: ActionResult | None = None
        total_attempts = _MAX_RETRIES + 1  # 1 initial + 2 retries = 3

        for attempt in range(total_attempts):
            start = time.monotonic()
            exec_result = self._executor.execute(action_dict, incident)
            elapsed = time.monotonic() - start

            result = ActionResult(
                incident_id=incident_id,
                action_type=action_def.action_type,
                entity_id=entity_id,
                entity_type=entity_type,
                success=exec_result["success"],
                output=exec_result["output"],
                error=exec_result.get("error"),
                execution_time_seconds=round(elapsed, 3),
                triggered_by="auto",
                severity=severity,
                confidence=confidence,
            )
            last_result = result

            if result.success:
                _LOG.info(
                    "orchestrator: auto-executed action=%s incident=%s attempt=%d",
                    action_def.action_type,
                    incident_id,
                    attempt + 1,
                )
                return result

            # Retry with exponential backoff
            if attempt < _MAX_RETRIES:
                delay = min(
                    _RETRY_BASE_DELAY * (_RETRY_MULTIPLIER ** attempt),
                    _RETRY_MAX_DELAY,
                )
                _LOG.warning(
                    "orchestrator: attempt %d/%d failed for action=%s; "
                    "retrying in %.1fs",
                    attempt + 1,
                    total_attempts,
                    action_def.action_type,
                    delay,
                )
                time.sleep(delay)

        # All retries exhausted — return last failed result
        _LOG.error(
            "orchestrator: action=%s failed after %d attempts for incident=%s",
            action_def.action_type,
            total_attempts,
            incident_id,
        )
        return last_result

    def _handle_approval(
        self,
        incident: dict[str, Any],
        action_def: ActionDefinition,
        decision: OrchestrationDecision,
    ) -> ApprovalRecord:
        """Create an ``ApprovalRecord`` and optionally store in ClickHouse."""
        incident_id = incident["incident_id"]
        entity_id = incident["root_cause"]["root_cause_entity"]

        record = ApprovalRecord(
            incident_id=incident_id,
            action_type=action_def.action_type,
            entity_id=entity_id,
            incident_severity=incident["severity"],
            incident_entity=entity_id,
            opa_reason=decision.reason,
        )

        _LOG.info(
            "orchestrator: approval required action=%s incident=%s approval_id=%s",
            action_def.action_type,
            incident_id,
            record.approval_id,
        )

        # Store in ClickHouse if function is available
        if self._clickhouse_fn is not None:
            try:
                self._clickhouse_fn(record.model_dump())
            except Exception as exc:
                _LOG.error(
                    "orchestrator: failed to store approval record "
                    "approval_id=%s: %s",
                    record.approval_id,
                    exc,
                )

        return record

    # ------------------------------------------------------------------
    # Step 6: publish
    # ------------------------------------------------------------------

    def _publish(self, action_result: ActionResult) -> None:
        """Publish ``ActionResult`` to Kafka via the producer.

        Fail-soft: logs on error, never raises.
        """
        try:
            self._producer.publish_action_result(action_result.model_dump())
            _LOG.debug(
                "orchestrator: published action_id=%s", action_result.action_id
            )
        except Exception as exc:
            _LOG.error(
                "orchestrator: publish failed for action_id=%s: %s",
                action_result.action_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Step 7: audit
    # ------------------------------------------------------------------

    def _audit(
        self, action_result: ActionResult, incident: dict[str, Any]
    ) -> None:
        """Archive ``ActionResult`` to MinIO for compliance auditing.

        Fail-soft: if archiver is ``None`` or upload fails, logs and continues.
        """
        if self._archiver is None:
            return

        try:
            bucket = "omniwatch-audit-logs"
            object_name = f"audit/{action_result.action_id}.json"
            payload = json.dumps(
                {
                    "action_result": action_result.model_dump(),
                    "incident_summary": {
                        "incident_id": incident.get("incident_id"),
                        "severity": incident.get("severity"),
                        "entity_id": incident.get("root_cause", {}).get(
                            "root_cause_entity"
                        ),
                    },
                },
                default=str,
            ).encode("utf-8")

            self._archiver.upload_object(
                bucket, object_name, payload, content_type="application/json"
            )
            action_result.archived = True
            _LOG.debug(
                "orchestrator: archived action_id=%s to %s",
                action_result.action_id,
                bucket,
            )
        except Exception as exc:
            _LOG.warning(
                "orchestrator: audit archive failed for action_id=%s: %s",
                action_result.action_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _get_all_actions(entity_type: str) -> list[ActionDefinition]:
        """Return ALL ``ActionDefinition``s (safe + unsafe) for *entity_type*.

        Accesses ``_REGISTRY`` directly — not ``get_actions()`` which filters
        to safe-only — so the orchestrator can evaluate unsafe actions like
        ``block_ip`` for the approval path.
        """
        return list(_REGISTRY.get(entity_type, []))
