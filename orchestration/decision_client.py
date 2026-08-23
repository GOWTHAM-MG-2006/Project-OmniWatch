"""
OmniWatch — Orchestration + Policy
Component: OPA Decision Client
Phase: 9
Purpose: HTTP client that evaluates proposed remediation actions against the OPA
         Rego policy engine, returning an OrchestrationDecision (allow/needs_approval/reason).
         Uses fail-closed strategy (D7): if OPA is unreachable after retries, the
         action is denied — never auto-execute when the policy engine is down.
         Query path: POST {opa_url}/v1/data/omniwatch (returns package document
         with all complete rules: allow, needs_approval, reason).
Inputs: Incident dict (severity, root_cause.confidence) + action_type string
Outputs: OrchestrationDecision (allow, needs_approval, reason)
"""

from __future__ import annotations

import time
from typing import Any

import httpx

from orchestration.config.settings import Settings
from orchestration.models import OrchestrationDecision

# Safe actions that qualify for auto-remediation on P1/P2 (decision D5).
# Must match the list passed to OPA via data.config.safe_actions.
SAFE_ACTIONS: list[str] = [
    "restart_service",
    "scale_deployment",
    "clear_cache",
    "kill_pod",
    "rollback",
]

# Retry constants — match storage/common.py backoff formula:
# delay(attempt) = base_delay * RETRY_MULTIPLIER ** attempt, capped at max_delay
# Produces: 100ms → 500ms → 2.0s
_RETRY_MULTIPLIER: float = 5.0
_RETRY_BASE_DELAY: float = 0.1
_RETRY_MAX_DELAY: float = 2.0
_RETRY_ATTEMPTS: int = 3  # retries after initial attempt (4 total calls)


class OPADecisionClient:
    """Synchronous HTTP client for OPA policy evaluation.

    Sends POST /v1/data/omniwatch with the incident context, then maps the
    OPA result (package document with allow/needs_approval/reason rules) to
    an OrchestrationDecision.  Retries on connection errors and 5xx responses
    with exponential backoff.
    """

    def __init__(
        self,
        opa_url: str | None = None,
        confidence_threshold: float | None = None,
    ) -> None:
        settings = Settings(_env_file=None)  # type: ignore[call-arg]
        self.opa_url: str = opa_url or settings.opa_url
        self.confidence_threshold: float = (
            confidence_threshold
            if confidence_threshold is not None
            else settings.opa_confidence_threshold
        )
        self._client = httpx.Client(timeout=10.0)
        self._load_config()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def decide(
        self, incident: dict[str, Any], action_type: str
    ) -> OrchestrationDecision:
        """Evaluate a proposed action against OPA and return the decision.

        Parameters
        ----------
        incident:
            Incident dict with at least ``severity`` (str) and
            ``root_cause`` → ``confidence`` (float, 0..100).
        action_type:
            The proposed remediation action (e.g. ``restart_service``).

        Returns
        -------
        OrchestrationDecision
            On success: the OPA evaluation result.
            On failure: fail-closed decision (allow=False, needs_approval=False).
        """
        input_data: dict[str, Any] = {
            "severity": incident["severity"],
            "confidence": incident["root_cause"]["confidence"],
            "action_type": action_type,
        }

        try:
            result = self._call_opa_with_retry(input_data)
            decision = result.get("result", {})
            return OrchestrationDecision(
                allow=bool(decision.get("allow", False)),
                needs_approval=bool(decision.get("needs_approval", False)),
                reason=str(decision.get("reason", "")),
            )
        except Exception as exc:
            # Fail-closed (D7): deny the action when OPA is unavailable
            return OrchestrationDecision(
                allow=False,
                needs_approval=False,
                reason=f"OPA unavailable: {exc}",
            )

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_config(self) -> None:
        """Best-effort PUT of the config bundle into OPA's data store.

        Ensures data.config.confidence_threshold and data.config.safe_actions
        resolve at query time.  If OPA is down at init, log a warning and
        continue — decide() will fail-closed anyway.
        """
        url = f"{self.opa_url}/v1/data/config"
        config_payload = {
            "confidence_threshold": self.confidence_threshold,
            "safe_actions": SAFE_ACTIONS,
        }
        try:
            resp = self._client.put(url, json=config_payload)
            resp.raise_for_status()
        except Exception as exc:  # noqa: BLE001 — best-effort at init
            print(f"WARNING: could not load config into OPA: {exc}")

    def _call_opa_with_retry(
        self,
        input_data: dict[str, Any],
    ) -> dict[str, Any]:
        """POST to OPA with 3x exponential backoff on transient failures.

        Query path: POST /v1/data/omniwatch (returns the full package document
        with allow, needs_approval, reason rules).  Retries on: connection
        errors, timeouts, 5xx responses.  Raises the last exception after
        retries are exhausted.
        """
        url = f"{self.opa_url}/v1/data/omniwatch"
        payload = {"input": input_data}
        last_error: Exception | None = None
        total_attempts = _RETRY_ATTEMPTS + 1

        for attempt in range(total_attempts):
            try:
                response = self._client.post(url, json=payload)
                # Treat 5xx as transient
                if response.status_code >= 500:
                    response.raise_for_status()
                response.raise_for_status()
                return response.json()
            except (httpx.ConnectError, httpx.HTTPStatusError, httpx.TimeoutException) as exc:
                last_error = exc
                if attempt < _RETRY_ATTEMPTS:
                    wait = min(
                        _RETRY_BASE_DELAY * (_RETRY_MULTIPLIER ** attempt),
                        _RETRY_MAX_DELAY,
                    )
                    time.sleep(wait)

        assert last_error is not None  # always set after loop
        raise last_error


# ---------------------------------------------------------------------------
# Backward-compatible alias — plan checkbox imports DecisionClient
# ---------------------------------------------------------------------------

DecisionClient = OPADecisionClient
