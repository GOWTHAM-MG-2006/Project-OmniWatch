"""
OmniWatch — Orchestration + Policy
Component: Action Library
Phase: 9
Purpose: Action definition registry mapping entity types to available remediation
         actions, with safety classification (safe vs human-gated), parameter schemas,
         and idempotency key generation for deduplication.
Inputs: entity_type, action_type
Outputs: ActionDefinition (safe/human-gated, parameter schema)
"""

from __future__ import annotations

from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# ActionDefinition — single remediation action metadata
# ---------------------------------------------------------------------------

@dataclass(frozen=True, slots=True)
class ActionDefinition:
    """Definition of a single remediation action.

    Attributes
    ----------
    action_type:
        Canonical action name (e.g. ``restart_service``).
    entity_type:
        The Neo4j entity type this action targets.
    safe:
        If True the action can auto-execute on P1/P2 with high confidence.
        If False the action is human-gated (requires OPA approval).
    description:
        Human-readable summary of what the action does.
    parameter_schema:
        Dictionary describing expected parameters (name → description).
    """

    action_type: str
    entity_type: str
    safe: bool
    description: str
    parameter_schema: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Idempotency helpers
# ---------------------------------------------------------------------------

def build_idempotency_key(
    action_type: str,
    entity_id: str,
    incident_id: str,
) -> str:
    """Build a deterministic dedup key for action execution.

    Format: ``"{action_type}:{entity_id}:{incident_id}"``

    SimulationExecutor skips execution on duplicate keys; KubernetesExecutor
    checks resource status before acting (idempotent by design).
    """
    return f"{action_type}:{entity_id}:{incident_id}"


# ---------------------------------------------------------------------------
# Registry — entity type → list[ActionDefinition]
# ---------------------------------------------------------------------------

# Common parameter schemas reused across actions
_PARAMS_RESTART: dict[str, str] = {
    "service_name": "Name of the service to restart",
}

_PARAMS_SCALE: dict[str, str] = {
    "replicas": "Target replica count",
}

_PARAMS_ROLLBACK: dict[str, str] = {
    "revision": "Deployment revision to roll back to (optional)",
}

_PARAMS_CLEAR_CACHE: dict[str, str] = {
    "cache_type": "Type of cache to clear (e.g. redis, local)",
}

_PARAMS_KILL_POD: dict[str, str] = {
    "pod_name": "Name of the pod to terminate",
}

_PARAMS_BLOCK_IP: dict[str, str] = {
    "ip_address": "IP address to block",
    "duration_seconds": "Block duration in seconds (0 = permanent)",
}

_PARAMS_ROTATE_CREDENTIALS: dict[str, str] = {
    "credential_type": "Type of credential to rotate (e.g. db_password, api_key)",
}

# Sensible action mappings per entity type.
# Safe actions follow SAFE_ACTIONS from decision_client.py; sensitive actions
# (block_ip, rotate_credentials) are human-gated per policy.rego.
_REGISTRY: dict[str, list[ActionDefinition]] = {
    "API_NODE": [
        ActionDefinition(
            action_type="restart_service",
            entity_type="API_NODE",
            safe=True,
            description="Restart the API node service process",
            parameter_schema=_PARAMS_RESTART,
        ),
        ActionDefinition(
            action_type="scale_deployment",
            entity_type="API_NODE",
            safe=True,
            description="Scale the API node deployment replicas",
            parameter_schema=_PARAMS_SCALE,
        ),
        ActionDefinition(
            action_type="clear_cache",
            entity_type="API_NODE",
            safe=True,
            description="Clear cached responses on the API node",
            parameter_schema=_PARAMS_CLEAR_CACHE,
        ),
        ActionDefinition(
            action_type="kill_pod",
            entity_type="API_NODE",
            safe=True,
            description="Terminate an unresponsive API pod",
            parameter_schema=_PARAMS_KILL_POD,
        ),
        ActionDefinition(
            action_type="rollback",
            entity_type="API_NODE",
            safe=True,
            description="Roll back the API node to a previous version",
            parameter_schema=_PARAMS_ROLLBACK,
        ),
        ActionDefinition(
            action_type="block_ip",
            entity_type="API_NODE",
            safe=False,
            description="Block a source IP at the API gateway",
            parameter_schema=_PARAMS_BLOCK_IP,
        ),
    ],
    "DATABASE_NODE": [
        ActionDefinition(
            action_type="restart_service",
            entity_type="DATABASE_NODE",
            safe=True,
            description="Restart the database service process",
            parameter_schema=_PARAMS_RESTART,
        ),
        ActionDefinition(
            action_type="rollback",
            entity_type="DATABASE_NODE",
            safe=True,
            description="Roll back the database to a previous backup or version",
            parameter_schema=_PARAMS_ROLLBACK,
        ),
        ActionDefinition(
            action_type="clear_cache",
            entity_type="DATABASE_NODE",
            safe=True,
            description="Clear the database query cache / buffer pool",
            parameter_schema=_PARAMS_CLEAR_CACHE,
        ),
        ActionDefinition(
            action_type="rotate_credentials",
            entity_type="DATABASE_NODE",
            safe=False,
            description="Rotate database credentials (requires human approval)",
            parameter_schema=_PARAMS_ROTATE_CREDENTIALS,
        ),
    ],
    "SERVICE": [
        ActionDefinition(
            action_type="restart_service",
            entity_type="SERVICE",
            safe=True,
            description="Restart the microservice process",
            parameter_schema=_PARAMS_RESTART,
        ),
        ActionDefinition(
            action_type="scale_deployment",
            entity_type="SERVICE",
            safe=True,
            description="Scale the service deployment replicas",
            parameter_schema=_PARAMS_SCALE,
        ),
        ActionDefinition(
            action_type="kill_pod",
            entity_type="SERVICE",
            safe=True,
            description="Terminate an unresponsive service pod",
            parameter_schema=_PARAMS_KILL_POD,
        ),
        ActionDefinition(
            action_type="rollback",
            entity_type="SERVICE",
            safe=True,
            description="Roll back the service to a previous version",
            parameter_schema=_PARAMS_ROLLBACK,
        ),
    ],
    "K8S_RESOURCE": [
        ActionDefinition(
            action_type="kill_pod",
            entity_type="K8S_RESOURCE",
            safe=True,
            description="Terminate the Kubernetes pod",
            parameter_schema=_PARAMS_KILL_POD,
        ),
        ActionDefinition(
            action_type="scale_deployment",
            entity_type="K8S_RESOURCE",
            safe=True,
            description="Scale the Kubernetes deployment replicas",
            parameter_schema=_PARAMS_SCALE,
        ),
    ],
    "INFRASTRUCTURE": [
        ActionDefinition(
            action_type="restart_service",
            entity_type="INFRASTRUCTURE",
            safe=True,
            description="Restart the infrastructure service",
            parameter_schema=_PARAMS_RESTART,
        ),
        ActionDefinition(
            action_type="clear_cache",
            entity_type="INFRASTRUCTURE",
            safe=True,
            description="Clear the infrastructure-level cache",
            parameter_schema=_PARAMS_CLEAR_CACHE,
        ),
    ],
}


# ---------------------------------------------------------------------------
# ActionLibrary — class API
# ---------------------------------------------------------------------------

class ActionLibrary:
    """Registry of remediation actions grouped by entity type.

    Usage::

        al = ActionLibrary()
        actions = al.get_actions("DATABASE_NODE")     # ['clear_cache', 'restart_service', 'rollback']
        definition = al.get_definition("DATABASE_NODE", "restart_service")
    """

    def __init__(self) -> None:
        self._registry: dict[str, list[ActionDefinition]] = _REGISTRY

    def get_actions(self, entity_type: str) -> list[str]:
        """Return sorted list of action_type strings available for *entity_type*.

        Parameters
        ----------
        entity_type:
            Neo4j entity type (e.g. ``DATABASE_NODE``).

        Returns
        -------
        list[str]
            Sorted action type names, or empty list for unknown entity types.
        """
        defs = self._registry.get(entity_type, [])
        return sorted(d.action_type for d in defs if d.safe)

    def get_definition(
        self, entity_type: str, action_type: str
    ) -> ActionDefinition | None:
        """Return the full ActionDefinition for a specific entity+action pair.

        Parameters
        ----------
        entity_type:
            Neo4j entity type.
        action_type:
            Action category (e.g. ``restart_service``).

        Returns
        -------
        ActionDefinition or None
        """
        for d in self._registry.get(entity_type, []):
            if d.action_type == action_type:
                return d
        return None


# ---------------------------------------------------------------------------
# Module-level convenience function (used by verify command / task 8 checkbox)
# ---------------------------------------------------------------------------

def get_actions(entity_type: str) -> list[str]:
    """Return sorted action types available for *entity_type*.

    Thin wrapper around ``ActionLibrary().get_actions()`` so that both
    ``from orchestration.action_library import get_actions`` and
    ``from orchestration.action_library import ActionLibrary`` work
    identically.
    """
    return ActionLibrary().get_actions(entity_type)
