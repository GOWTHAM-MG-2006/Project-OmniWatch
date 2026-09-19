"""
OmniWatch — Dashboard API
Component: Workspace scope enforcement (ENTRY-5)
Phase: entry-point (Wave 3, todo 5)
Purpose: JWT decode + membership enforcement + tenant_allow gate + per-request
         workspace scope (ClickHouse DB, Kafka prefix, MinIO prefix, Neo4j slug)
         consumed by dashboard/api/main.py middleware and query helpers.
Inputs: Authorization: Bearer <JWT> header, optional ?workspace_id= override,
        identity service (membership), OPA (best-effort tenant_allow confirm)
Outputs: Scope object per request; ScopeError (401/403/422) on denial
"""

from __future__ import annotations

import contextvars
import logging
import os
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

_LOG: logging.Logger = logging.getLogger("omniwatch.dashboard.scope")

# ---------------------------------------------------------------------------
# Isolation naming (docs/workspace-isolation.md section 1 — code matches doc).
# storage.config is the source of truth, loaded via importlib (the same lazy
# pattern as identity/provision.py naming_helpers): the identity image ships
# without storage/, and static absolute imports break local-dev LSP roots, so
# no `from storage...` / `from identity...` statement may appear in this file.
# Doc-verbatim fallbacks keep the module importable when storage/ is absent.
# ---------------------------------------------------------------------------

import importlib as _importlib


def _storage_config() -> Any | None:
    """Return the storage.config module, or None when not importable."""
    try:
        return _importlib.import_module("storage.config")
    except ImportError:
        return None


def _fallback_database(slug: str, base_db: str = "omniwatch") -> str:
    return base_db if slug == "default" else f"{base_db}_ws_{slug}"


def _fallback_kafka_prefix(slug: str) -> str:
    return "" if slug == "default" else f"ws_{slug}."


def _fallback_minio_prefix(slug: str) -> str:
    return "" if slug == "default" else f"workspaces/{slug}/"


def workspace_database(slug: str, base_db: str = "omniwatch") -> str:
    """ClickHouse database for a workspace slug (default: bare `omniwatch`)."""
    mod = _storage_config()
    if mod is not None:
        return str(mod.workspace_database(slug, base_db))
    return _fallback_database(slug, base_db)


def workspace_kafka_prefix(slug: str) -> str:
    """Kafka topic prefix for a workspace slug (default: bare names)."""
    mod = _storage_config()
    if mod is not None:
        return str(mod.workspace_kafka_prefix(slug))
    return _fallback_kafka_prefix(slug)


def workspace_minio_prefix(slug: str) -> str:
    """MinIO key prefix for a workspace slug (default: bare keys)."""
    mod = _storage_config()
    if mod is not None:
        return str(mod.workspace_prefix(slug))
    return _fallback_minio_prefix(slug)


def workspace_topics(slug: str) -> dict[str, str]:
    """Full Kafka topic registry scoped to a workspace slug."""
    mod = _storage_config()
    if mod is not None:
        return dict(mod.workspace_topics(slug))
    prefix = _fallback_kafka_prefix(slug)
    return {key: f"{prefix}{base}" for key, base in _BARE_KAFKA_TOPICS.items()}


#: Bare topic names (docs/workspace-isolation.md section 1); used only when
#: storage.config is not importable. Must stay in sync with KAFKA_TOPICS.
_BARE_KAFKA_TOPICS: dict[str, str] = {
    "anomalies": "omniwatch.anomalies.detected",
    "causal": "omniwatch.incidents.causal",
    "created": "omniwatch.incidents.created",
    "actions": "omniwatch.remediation.actions",
    "security": "omniwatch.security.events",
    "features": "omniwatch.features.windowed",
    "entities": "omniwatch.entities.resolved",
    "summaries": "omniwatch.generated.summaries",
    "runbooks": "omniwatch.generated.runbooks",
    "reports": "omniwatch.generated.reports",
}


def _identity_module(name: str) -> Any:
    """Import identity.<name> via importlib (None when not importable)."""
    try:
        return _importlib.import_module(f"identity.{name}")
    except ImportError:
        return None

# Explicit workspace_id override (?workspace_id=): opaque IDs only — reject
# path traversal / control chars early (422). Well-formed-but-unknown IDs
# fall through to the membership check (403, never 404-leak).
_WORKSPACE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-_]{0,127}$")

IDENTITY_URL: str = os.getenv(
    "OMNIWATCH_IDENTITY_URL",
    os.getenv("IDENTITY_URL", "http://localhost:8012"),
).rstrip("/")

OPA_URL: str = os.getenv(
    "OMNIWATCH_OPA_URL", os.getenv("OPA_URL", "http://localhost:8181")
).rstrip("/")

# Negative cache for the best-effort live OPA confirm: when OPA is down we
# must not pay a connect timeout on every request.
_OPA_DOWN_UNTIL: float = 0.0
_OPA_DOWN_TTL_S: float = 60.0


class ScopeError(Exception):
    """Denial with an HTTP status (raised by resolve_request_scope)."""

    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


class IdentityUnreachableError(Exception):
    """Membership resolver could not reach the identity service."""


@dataclass(frozen=True)
class Scope:
    """Per-request workspace scope (all keys derived from the slug)."""

    user_id: str
    workspace_id: str
    slug: str
    ch_database: str
    kafka_prefix: str
    minio_prefix: str
    is_default: bool

    def kafka_topics(self) -> dict[str, str]:
        """Full Kafka topic registry scoped to this workspace."""
        return workspace_topics(self.slug)

    def to_dict(self) -> dict[str, Any]:
        return {
            "user_id": self.user_id,
            "workspace_id": self.workspace_id,
            "slug": self.slug,
            "ch_database": self.ch_database,
            "kafka_prefix": self.kafka_prefix,
            "kafka_topics": self.kafka_topics(),
            "minio_prefix": self.minio_prefix,
            "neo4j_workspace": self.slug,
            "is_default": self.is_default,
        }


def default_scope() -> Scope:
    """Legacy single-tenant scope: bare topic/DB/key names (zero regression)."""
    return Scope(
        user_id="local-dev",
        workspace_id="default",
        slug="default",
        ch_database=workspace_database("default"),
        kafka_prefix=workspace_kafka_prefix("default"),
        minio_prefix=workspace_minio_prefix("default"),
        is_default=True,
    )


def scope_for(slug: str, user_id: str, workspace_id: str) -> Scope:
    """Build a non-default scope (slug-keyed isolation mappings)."""
    return Scope(
        user_id=user_id,
        workspace_id=workspace_id,
        slug=slug,
        ch_database=workspace_database(slug),
        kafka_prefix=workspace_kafka_prefix(slug),
        minio_prefix=workspace_minio_prefix(slug),
        is_default=(slug == "default"),
    )


# ---------------------------------------------------------------------------
# Per-request scope carrier (set by the middleware, read by query helpers).
# ---------------------------------------------------------------------------

_current_scope: contextvars.ContextVar[Optional[Scope]] = contextvars.ContextVar(
    "omniwatch_workspace_scope", default=None
)


def get_current_scope() -> Scope:
    """Return the active request scope (default scope outside requests)."""
    scope = _current_scope.get()
    return scope if scope is not None else default_scope()


def set_current_scope(scope: Scope) -> contextvars.Token:
    """Pin a scope for the current request (tests + middleware)."""
    return _current_scope.set(scope)


def reset_current_scope(token: contextvars.Token) -> None:
    """Release a pinned scope."""
    _current_scope.reset(token)


# ---------------------------------------------------------------------------
# tenant_allow — local mirror of orchestration/policies/tenant_allow.rego.
# Evaluated BEFORE any data access (deny short-circuits to 403). The .rego
# files themselves are never touched by this lane (byte-identical).
# ---------------------------------------------------------------------------


def tenant_allow(
    jwt_sub: str,
    jwt_ws_slug: str,
    resource_ws_slug: str,
    resource_owner: str,
) -> bool:
    """Mirror of tenant_allow.rego (absent jwt ws reads as "default").

    Rule 1: legacy/unscoped resource + default-scoped caller -> allow.
    Rule 2: exact workspace match owned by the caller -> allow.
    Anything else -> deny.
    """
    jwt_ws = jwt_ws_slug or "default"
    if resource_ws_slug == "default" and jwt_ws == "default":
        return True
    return resource_ws_slug == jwt_ws and jwt_sub == resource_owner


def query_opa_tenant_allow(
    jwt_sub: str,
    jwt_ws_slug: str,
    resource_ws_slug: str,
    resource_owner: str,
    timeout_s: float = 0.5,
) -> Optional[bool]:
    """Best-effort live OPA confirm (None when OPA is unreachable).

    Never raises: unreachable OPA degrades to the local mirror verdict.
    """
    global _OPA_DOWN_UNTIL
    if time.time() < _OPA_DOWN_UNTIL:
        return None
    try:
        import httpx  # noqa: PLC0415

        resp = httpx.post(
            f"{OPA_URL}/v1/data/omniwatch/tenant_allow",
            json={
                "input": {
                    "jwt_sub": jwt_sub,
                    "jwt_ws": jwt_ws_slug or "default",
                    "resource_ws": resource_ws_slug,
                    "resource_owner": resource_owner,
                }
            },
            timeout=timeout_s,
        )
        resp.raise_for_status()
        result = resp.json().get("result")
        return bool(result) if isinstance(result, bool) else None
    except Exception as exc:  # noqa: BLE001 - best-effort only
        _LOG.debug("OPA tenant_allow confirm degraded: %s", exc)
        _OPA_DOWN_UNTIL = time.time() + _OPA_DOWN_TTL_S
        return None


# ---------------------------------------------------------------------------
# Membership resolution (injectable for tests; HTTP default for prod).
# ---------------------------------------------------------------------------

#: (user_id, workspace_id, auth_header) -> slug | None.
#: None covers missing / other-user / tombstoned (caller maps ALL to 403).
MembershipResolver = Callable[[str, str, Optional[str]], Optional[str]]

_resolver: Optional[MembershipResolver] = None


def set_membership_resolver(resolver: Optional[MembershipResolver]) -> None:
    """Inject a membership resolver (tests); None restores the HTTP default."""
    global _resolver
    _resolver = resolver


def _http_membership_resolver(
    user_id: str, workspace_id: str, auth_header: Optional[str]
) -> Optional[str]:
    """Validate membership against the identity service (owner-only 200).

    GET /workspaces/{id} with the caller's bearer token: identity returns
    200 + workspace JSON iff owned-and-live, 403 otherwise (never 404-leak).
    Raises IdentityUnreachableError when the service cannot be reached.
    """
    import httpx  # noqa: PLC0415

    headers: dict[str, str] = {}
    if auth_header:
        headers["Authorization"] = auth_header
    try:
        resp = httpx.get(
            f"{IDENTITY_URL}/workspaces/{workspace_id}",
            headers=headers,
            timeout=2.0,
        )
    except Exception as exc:
        raise IdentityUnreachableError(str(exc)) from exc
    if resp.status_code == 200:
        try:
            return str(resp.json().get("slug"))
        except Exception:
            return None
    return None


def _resolve_membership(
    user_id: str, workspace_id: str, auth_header: Optional[str]
) -> Optional[str]:
    resolver = _resolver or _http_membership_resolver
    return resolver(user_id, workspace_id, auth_header)


# ---------------------------------------------------------------------------
# JWT decode (same secret + algorithm as identity; present-but-bad -> 401).
# ---------------------------------------------------------------------------


def _jwt_secret() -> str:
    settings_mod = _identity_module("settings")
    if settings_mod is None:  # pragma: no cover - identity always ships here
        raise ScopeError(401, "invalid token")
    return settings_mod.resolve_jwt_secret(settings_mod.IdentitySettings.from_env())


def _decode_caller(auth_header: Optional[str]) -> Optional[dict[str, Any]]:
    """Decode the bearer JWT (None when no header; 401 on bad token)."""
    if not auth_header:
        return None
    scheme, _, token = auth_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise ScopeError(401, "invalid Authorization header (want Bearer <JWT>)")
    tokens_mod = _identity_module("tokens")
    if tokens_mod is None:  # pragma: no cover - identity always ships here
        raise ScopeError(401, "invalid token")

    try:
        claims = tokens_mod.decode_token(token.strip(), _jwt_secret())
    except tokens_mod.ExpiredTokenError as exc:
        raise ScopeError(401, "token expired") from exc
    except tokens_mod.InvalidTokenError as exc:
        raise ScopeError(401, "invalid token") from exc
    if claims.get("type", "access") != "access":
        raise ScopeError(401, "invalid token")
    if not claims.get("sub"):
        raise ScopeError(401, "invalid token")
    return claims


def _check_override(value: str) -> str:
    """Validate an explicit ?workspace_id= (malformed -> 422)."""
    if not _WORKSPACE_ID_RE.match(value):
        raise ScopeError(422, "malformed workspace_id")
    return value


# ---------------------------------------------------------------------------
# Request entry point (called by the middleware for every /api/* route).
# ---------------------------------------------------------------------------


def resolve_request_scope(
    auth_header: Optional[str],
    workspace_id_override: Optional[str] = None,
) -> Scope:
    """Resolve the request scope from JWT + optional explicit workspace.

    - No token -> default scope (legacy boot, byte-identical single-tenant).
    - Legacy token (no ws claim) -> default scope.
    - Token with ws -> membership check (non-member/unknown -> 403, never
      404-leak); explicit ?workspace_id= override is membership-checked too
      (non-member -> 403, malformed -> 422).
    - tenant_allow (mirror + best-effort live OPA) is evaluated BEFORE any
      data access; deny short-circuits to 403.
    """
    claims = _decode_caller(auth_header)
    if claims is None:
        if workspace_id_override is not None:
            # Unauthenticated callers cannot prove membership of anything.
            raise ScopeError(403, "workspace not found or access denied")
        return default_scope()

    user_id = str(claims.get("sub"))
    ws_claim = claims.get("ws")

    target_id = (
        _check_override(workspace_id_override)
        if workspace_id_override is not None
        else None
    )
    scope_id = target_id or (str(ws_claim) if ws_claim else None)
    if scope_id is None:
        return default_scope()  # legacy token without ws claim

    try:
        slug = _resolve_membership(user_id, scope_id, auth_header)
    except IdentityUnreachableError as exc:
        raise ScopeError(403, "workspace not found or access denied") from exc
    if not slug:
        # Missing OR other-user OR tombstoned: uniform 403 (no existence oracle).
        raise ScopeError(403, "workspace not found or access denied")

    scope = scope_for(slug, user_id, scope_id)

    # tenant_allow gate (local mirror first, live OPA confirm when reachable).
    if not tenant_allow(user_id, slug, slug, user_id):
        raise ScopeError(403, "workspace not found or access denied")
    live = query_opa_tenant_allow(user_id, slug, slug, user_id)
    if live is False:
        raise ScopeError(403, "workspace not found or access denied")
    return scope
