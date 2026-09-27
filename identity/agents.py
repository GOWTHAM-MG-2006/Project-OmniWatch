"""
OmniWatch — Entry-Point / Agent Binding Layer
Component: Agent pair/list/unbind routers (mounted under /agents)
Phase: omni-agent-page
Purpose: Bind a pod-deployed agent to one of the caller's workspaces via
         endpoint + token; the binding is the contract the ingestion layer
         uses to route that agent's telemetry exclusively to the workspace.
Inputs: JWT via resolve_caller; endpoint URL + API token + workspace_id
Outputs: Binding JSON + live probe status; 403 (never 404-leak), 409, 422
"""

from __future__ import annotations

import logging
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field

from identity.workspaces import get_registry, resolve_caller

logger = logging.getLogger("omniwatch.identity.agents")

router = APIRouter(prefix="/agents", tags=["agents"])

_PROBE_TIMEOUT_S = 10


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class AgentBinding:
    binding_id: str
    user_id: str
    workspace_id: str
    workspace_slug: str
    agent_endpoint: str
    token_hash: str
    status: str = "connected"
    created_at: datetime = field(default_factory=utcnow)


class AgentRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_id: dict[str, AgentBinding] = {}

    def reset(self) -> None:
        with self._lock:
            self._by_id.clear()

    def restore(self, record: AgentBinding) -> None:
        """Load one deduplicated row (boot reload from ClickHouse mirror)."""
        with self._lock:
            self._by_id[record.binding_id] = record

    def pair(
        self,
        user_id: str,
        workspace_id: str,
        workspace_slug: str,
        agent_endpoint: str,
        token_hash: str,
    ) -> AgentBinding:
        cleaned = agent_endpoint.strip().rstrip("/")
        with self._lock:
            for record in self._by_id.values():
                if record.user_id == user_id and record.agent_endpoint == cleaned:
                    raise DuplicateAgentError(cleaned)
            binding = AgentBinding(
                binding_id=str(uuid.uuid4()),
                user_id=user_id,
                workspace_id=workspace_id,
                workspace_slug=workspace_slug,
                agent_endpoint=cleaned,
                token_hash=token_hash,
            )
            self._by_id[binding.binding_id] = binding
            return binding

    def list_mine(self, user_id: str) -> list[AgentBinding]:
        with self._lock:
            return [r for r in self._by_id.values() if r.user_id == user_id]

    def get_mine(self, user_id: str, binding_id: str) -> Optional[AgentBinding]:
        with self._lock:
            record = self._by_id.get(binding_id)
            if record is None or record.user_id != user_id:
                return None
            return record

    def unbind(self, user_id: str, binding_id: str) -> bool:
        with self._lock:
            record = self._by_id.get(binding_id)
            if record is None or record.user_id != user_id:
                return False
            del self._by_id[binding_id]
            return True


class DuplicateAgentError(Exception):
    pass


_REGISTRY = AgentRegistry()


def get_agent_registry() -> AgentRegistry:
    return _REGISTRY


def reset_agent_registry() -> None:
    _REGISTRY.reset()


class AgentPair(BaseModel):
    endpoint: str = Field(min_length=1, max_length=500)
    token: str = Field(min_length=1, max_length=500)
    workspace_id: str = Field(min_length=1, max_length=100)


class AgentBindingResponse(BaseModel):
    binding_id: str
    workspace_id: str
    workspace_slug: str
    agent_endpoint: str
    status: str
    created_at: str


def _forbidden() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="agent binding not found or access denied",
    )


def _validate_endpoint(endpoint: str) -> str:
    cleaned = endpoint.strip().rstrip("/")
    try:
        parts = urllib.parse.urlparse(cleaned)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail="endpoint is not a valid URL") from exc
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise HTTPException(
            status_code=422, detail="endpoint must be an http(s) URL with a host")
    return cleaned


def probe_agent(endpoint: str, timeout_s: int = _PROBE_TIMEOUT_S) -> dict[str, Any]:
    result: dict[str, Any] = {"reachable": False, "health": None, "ready": None}
    for path, key in (("/health", "health"), ("/ready", "ready")):
        try:
            req = urllib.request.Request(endpoint + path, method="GET")
            with urllib.request.urlopen(req, timeout=timeout_s) as resp:
                result[key] = resp.status
        except urllib.error.HTTPError as exc:
            result[key] = exc.code
        except Exception as exc:  # noqa: BLE001 - probe must never raise
            logger.debug("agent probe failed endpoint=%s path=%s: %s", endpoint, path, exc)
            return result
    result["reachable"] = result["health"] == 200
    return result


def _to_response(record: AgentBinding, live_status: Optional[str] = None) -> AgentBindingResponse:
    return AgentBindingResponse(
        binding_id=record.binding_id,
        workspace_id=record.workspace_id,
        workspace_slug=record.workspace_slug,
        agent_endpoint=record.agent_endpoint,
        status=live_status or record.status,
        created_at=record.created_at.isoformat(),
    )


@router.post("/pair", response_model=AgentBindingResponse, status_code=status.HTTP_201_CREATED)
def pair_agent(body: AgentPair, ctx: Any = Depends(resolve_caller)) -> AgentBindingResponse:
    from identity import auth as auth_module

    endpoint = _validate_endpoint(body.endpoint)
    workspace = next(
        (w for w in get_registry().list_mine(ctx.user_id) if w.workspace_id == body.workspace_id),
        None,
    )
    if workspace is None:
        raise _forbidden()
    probe = probe_agent(endpoint)
    if not probe["reachable"]:
        raise HTTPException(status_code=422, detail="agent endpoint unreachable or unhealthy")
    try:
        record = get_agent_registry().pair(
            ctx.user_id,
            workspace.workspace_id,
            workspace.slug,
            endpoint,
            auth_module.hash_password(body.token),
        )
    except DuplicateAgentError as exc:
        raise HTTPException(status_code=409, detail="agent already paired") from exc
    try:
        from identity import persistence as persistence_module

        persistence_module.mirror_agent(record)
    except Exception:  # noqa: BLE001 - mirror must never fail pairing
        logger.warning("agent mirror degraded", exc_info=True)
    return _to_response(record)


@router.get("", response_model=list[AgentBindingResponse])
def list_agents(ctx: Any = Depends(resolve_caller)) -> list[AgentBindingResponse]:
    records = get_agent_registry().list_mine(ctx.user_id)
    out: list[AgentBindingResponse] = []
    for record in records:
        probe = probe_agent(record.agent_endpoint)
        live = "connected" if probe["reachable"] else "unreachable"
        out.append(_to_response(record, live_status=live))
    return out


@router.delete("/{binding_id}")
def unbind_agent(binding_id: str, ctx: Any = Depends(resolve_caller)) -> dict[str, str]:
    record = get_agent_registry().get_mine(ctx.user_id, binding_id)
    if record is None:
        raise _forbidden()
    get_agent_registry().unbind(ctx.user_id, binding_id)
    try:
        from identity import persistence as persistence_module

        persistence_module.mirror_agent(record, deleted=True)
    except Exception:  # noqa: BLE001 - mirror must never fail unbind
        logger.warning("agent mirror degraded", exc_info=True)
    return {"binding_id": binding_id, "status": "unbound"}
