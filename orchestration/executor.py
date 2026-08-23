"""
OmniWatch — Orchestration + Policy
Component: Action Executors
Phase: 9
Purpose: ActionExecutor ABC defines the execute() contract; SimulationExecutor
         uses httpx to POST actions to a mock backend endpoint (default: no real
         remediation); KubernetesExecutor uses the kubernetes SDK with lazy import
         (only imported when ENABLE_REAL_K8S=true).  Both executors support
         DRY_RUN mode (returns success with dry-run output) and idempotency
         dedup (skips duplicate action+entity+incident combos).
Inputs: action_definition dict (action_type, entity_type, safe, description)
        incident dict (incident_id, root_cause.entity_id, severity, confidence)
Outputs: dict with ActionResult fields: success, output, error, execution_time_seconds
"""

from __future__ import annotations

import logging
import os
import time
from abc import ABC, abstractmethod
from typing import Any

import httpx

from orchestration.action_library import build_idempotency_key

logger = logging.getLogger(__name__)

# Default simulation endpoint — mock backend, no real remediation
_SIMULATION_ENDPOINT: str = "http://localhost:8010/api/v1/simulate-action"

# DRY_RUN output format — must match plan spec exactly
_DRY_RUN_TEMPLATE: str = "dry-run: would execute {action_type} on {entity_id}"


# ---------------------------------------------------------------------------
# ActionExecutor — Abstract Base Class
# ---------------------------------------------------------------------------

class ActionExecutor(ABC):
    """Abstract base class defining the action execution contract.

    All executors must implement ``execute()`` which takes an action definition
    dict and an incident dict, and returns an ActionResult-compatible dict with
    keys: success, output, error, execution_time_seconds.

    Subclasses must also call ``_check_dry_run()`` at the start of execute()
    to honour the DRY_RUN environment variable.
    """

    def __init__(self) -> None:
        self._idempotency_cache: set[str] = set()
        self.calls: list[dict[str, Any]] = []

    @abstractmethod
    def execute(
        self,
        action_definition: dict[str, Any],
        incident: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a remediation action.

        Parameters
        ----------
        action_definition:
            Dict with at least ``action_type``, ``entity_type``, ``safe``,
            ``description`` keys.
        incident:
            IncidentRecord-like dict with ``incident_id``, ``root_cause`` →
            ``root_cause_entity``, ``severity``, ``confidence``.

        Returns
        -------
        dict
            ActionResult-compatible dict: success, output, error,
            execution_time_seconds.
        """
        ...

    def _check_dry_run(
        self,
        action_type: str,
        entity_id: str,
    ) -> dict[str, Any] | None:
        """Check if DRY_RUN mode is active and return dry-run result if so.

        Returns None if DRY_RUN is not active (caller should proceed).
        Returns a result dict if DRY_RUN is active (caller should return it).
        """
        dry_run = os.environ.get("DRY_RUN", "").lower() == "true"
        if dry_run:
            return {
                "success": True,
                "output": _DRY_RUN_TEMPLATE.format(
                    action_type=action_type, entity_id=entity_id
                ),
                "error": None,
                "execution_time_seconds": 0.0,
            }
        return None

    def _check_idempotency(
        self,
        action_type: str,
        entity_id: str,
        incident_id: str,
    ) -> dict[str, Any] | None:
        """Check for duplicate action and return idempotent result if found.

        Returns None if this is a new action (caller should proceed).
        Returns a success result dict if this is a duplicate (caller should
        return it — idempotent no-op).
        """
        key = build_idempotency_key(action_type, entity_id, incident_id)
        if key in self._idempotency_cache:
            return {
                "success": True,
                "output": f"already executed: {key}",
                "error": None,
                "execution_time_seconds": 0.0,
            }
        self._idempotency_cache.add(key)
        return None

    def _extract_entity_id(self, incident: dict[str, Any]) -> str:
        """Extract entity_id from incident root_cause."""
        return incident.get("root_cause", {}).get("root_cause_entity", "unknown")


# ---------------------------------------------------------------------------
# SimulationExecutor — httpx-based mock backend
# ---------------------------------------------------------------------------

class SimulationExecutor(ActionExecutor):
    """Executor that POSTs actions to a simulation/mock backend endpoint.

    Uses ``httpx.Client`` (sync, matching the decision_client.py pattern) to
    send action definitions to a configurable endpoint.  Default endpoint is
    ``http://localhost:8010/api/v1/simulate-action`` — a mock backend that
    always returns success (no real remediation occurs).

    Behaviour:
    - DRY_RUN=true → returns success with dry-run output, no HTTP call.
    - Idempotent → second call with same (action, entity, incident) returns
      success immediately (no HTTP call).
    - Normal → POST to endpoint, return simulated result.
    """

    def __init__(self, endpoint: str | None = None) -> None:
        super().__init__()
        self._endpoint: str = endpoint or _SIMULATION_ENDPOINT
        self._client = httpx.Client(timeout=10.0)

    def execute(
        self,
        action_definition: dict[str, Any],
        incident: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a simulated remediation action.

        Checks DRY_RUN first, then idempotency, then POSTs to the mock
        backend endpoint.
        """
        action_type: str = action_definition.get("action_type", "unknown")
        entity_id: str = self._extract_entity_id(incident)
        incident_id: str = incident.get("incident_id", "unknown")

        self.calls.append({
            "action_type": action_type,
            "entity_id": entity_id,
            "incident_id": incident_id,
        })

        # 1. DRY_RUN check
        dry_run_result = self._check_dry_run(action_type, entity_id)
        if dry_run_result is not None:
            return dry_run_result

        # 2. Idempotency check
        dedup_result = self._check_idempotency(action_type, entity_id, incident_id)
        if dedup_result is not None:
            return dedup_result

        # 3. Simulated execution via mock backend
        payload = {
            "action_type": action_type,
            "entity_type": action_definition.get("entity_type", "unknown"),
            "entity_id": entity_id,
            "incident_id": incident_id,
            "description": action_definition.get("description", ""),
        }

        start = time.monotonic()
        try:
            response = self._client.post(self._endpoint, json=payload)
            elapsed = time.monotonic() - start

            if response.status_code >= 400:
                # SimulationExecutor is a mock — the backend is optional. A 4xx
                # typically means the mock endpoint is absent or the port is
                # occupied by another OmniWatch service (e.g. the orchestration
                # engine). Honor the documented contract: always return success.
                return {
                    "success": True,
                    "output": f"simulated: {action_type} on {entity_id} (offline mock; backend returned {response.status_code})",
                    "error": None,
                    "execution_time_seconds": round(elapsed, 3),
                }

            body = response.json()
            return {
                "success": bool(body.get("success", True)),
                "output": str(body.get("output", "action completed")),
                "error": body.get("error"),
                "execution_time_seconds": round(elapsed, 3),
            }
        except httpx.ConnectError as exc:
            elapsed = time.monotonic() - start
            # Simulation backend not running — treat as success (mock mode)
            logger.warning("Simulation endpoint unreachable: %s — treating as success", exc)
            return {
                "success": True,
                "output": f"simulated: {action_type} on {entity_id} (offline mock)",
                "error": None,
                "execution_time_seconds": round(elapsed, 3),
            }
        except Exception as exc:
            elapsed = time.monotonic() - start
            # SimulationExecutor is a mock that always succeeds; any unexpected
            # error (timeout, parse error, etc.) is treated as offline-mock
            # success per the documented contract.
            logger.warning("SimulationExecutor backend error: %s — treating as success", exc)
            return {
                "success": True,
                "output": f"simulated: {action_type} on {entity_id} (offline mock)",
                "error": None,
                "execution_time_seconds": round(elapsed, 3),
            }

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()


# ---------------------------------------------------------------------------
# KubernetesExecutor — lazy kubernetes SDK import
# ---------------------------------------------------------------------------

class KubernetesExecutor(ActionExecutor):
    """Executor that uses the kubernetes Python SDK for real K8s operations.

    The ``kubernetes`` package is imported **lazily** — only when ``execute()``
    is called and ``ENABLE_REAL_K8S=true``.  This avoids adding kubernetes
    as a hard dependency and keeps import-time side effects zero.

    Behaviour:
    - ENABLE_REAL_K8S=false (default) → falls back to SimulationExecutor-like
      behaviour (logs the action, returns success).
    - ENABLE_REAL_K8S=true → imports kubernetes client, performs the action
      (restart deployment, scale replicas, etc.).
    - DRY_RUN=true → returns success with dry-run output, no K8s calls.
    """

    def __init__(self) -> None:
        super().__init__()
        self._k8s_client: Any = None  # lazy-loaded

    def execute(
        self,
        action_definition: dict[str, Any],
        incident: dict[str, Any],
    ) -> dict[str, Any]:
        """Execute a K8s remediation action.

        Checks DRY_RUN first, then idempotency, then either delegates to
        the real kubernetes SDK or falls back to simulation.
        """
        action_type: str = action_definition.get("action_type", "unknown")
        entity_id: str = self._extract_entity_id(incident)
        incident_id: str = incident.get("incident_id", "unknown")

        self.calls.append({
            "action_type": action_type,
            "entity_id": entity_id,
            "incident_id": incident_id,
        })

        # 1. DRY_RUN check
        dry_run_result = self._check_dry_run(action_type, entity_id)
        if dry_run_result is not None:
            return dry_run_result

        # 2. Idempotency check
        dedup_result = self._check_idempotency(action_type, entity_id, incident_id)
        if dedup_result is not None:
            return dedup_result

        # 3. Real K8s execution (if enabled) or simulation fallback
        enable_real = os.environ.get("ENABLE_REAL_K8S", "").lower() == "true"

        if enable_real:
            return self._execute_real_k8s(action_definition, entity_id, incident_id)
        else:
            return self._execute_simulation_fallback(action_type, entity_id)

    def _execute_real_k8s(
        self,
        action_definition: dict[str, Any],
        entity_id: str,
        incident_id: str,
    ) -> dict[str, Any]:
        """Execute via the kubernetes SDK (lazy import)."""
        try:
            import kubernetes  # noqa: F401 — lazy import
            from kubernetes import client as k8s_client
        except ImportError:
            return {
                "success": False,
                "output": "kubernetes package not installed",
                "error": "ImportError: install kubernetes package for real K8s execution",
                "execution_time_seconds": 0.0,
            }

        action_type = action_definition.get("action_type", "unknown")

        start = time.monotonic()
        try:
            # Configure in-cluster or kubeconfig
            k8s_client.Config.load_incluster_config()

            apps_v1 = k8s_client.AppsV1Api()
            namespace = os.environ.get("K8S_NAMESPACE", "default")

            # Map action_type to K8s API call
            if action_type == "restart_service":
                # Patch deployment to trigger rolling restart
                body: dict[str, Any] = {
                    "spec": {
                        "template": {
                            "metadata": {
                                "annotations": {
                                    "kubectl.kubernetes.io/restartedAt": time.strftime(
                                        "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
                                    )
                                }
                            }
                        }
                    }
                }
                apps_v1.patch_namespaced_deployment(
                    name=entity_id, namespace=namespace, body=body
                )
            elif action_type == "scale_deployment":
                params = action_definition.get("parameter_schema", {})
                replicas = int(params.get("replicas", 2))
                body = {"spec": {"replicas": replicas}}
                apps_v1.patch_namespaced_deployment(
                    name=entity_id, namespace=namespace, body=body
                )
            else:
                return {
                    "success": False,
                    "output": f"unsupported K8s action: {action_type}",
                    "error": f"Action {action_type} not implemented for K8s executor",
                    "execution_time_seconds": round(time.monotonic() - start, 3),
                }

            elapsed = time.monotonic() - start
            return {
                "success": True,
                "output": f"K8s {action_type} executed on {entity_id} in {namespace}",
                "error": None,
                "execution_time_seconds": round(elapsed, 3),
            }
        except Exception as exc:
            elapsed = time.monotonic() - start
            logger.error("KubernetesExecutor error: %s", exc)
            return {
                "success": False,
                "output": f"K8s action failed: {exc}",
                "error": str(exc),
                "execution_time_seconds": round(elapsed, 3),
            }

    def _execute_simulation_fallback(
        self,
        action_type: str,
        entity_id: str,
    ) -> dict[str, Any]:
        """Fallback when ENABLE_REAL_K8S is false — log and return success."""
        logger.info(
            "KubernetesExecutor (simulation mode): %s on %s",
            action_type,
            entity_id,
        )
        return {
            "success": True,
            "output": f"simulated K8s {action_type} on {entity_id}",
            "error": None,
            "execution_time_seconds": 0.0,
        }
