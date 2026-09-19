"""
OmniWatch — Entry-Point / Workspace Layer
Component: Workspace CRUD + switch routers (mounted under /workspaces)
Phase: entry-point (Wave 2, todo 2)
Purpose: Multi-workspace model per user (create/list/rename/switch/delete as
         tombstone); switch re-issues the JWT with the `ws` claim; the
         `default` bootstrap keeps unauthenticated single-tenant behavior
         byte-identical.
Inputs: JWT via require_user (or no token -> default-bootstrap identity);
        workspace names/slugs in request bodies
Outputs: Workspace JSON + connection bundle; 403 (never 404-leak), 409, 422
"""

from __future__ import annotations

import logging
import re
import threading
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel, Field

logger = logging.getLogger("omniwatch.identity.workspaces")

router = APIRouter(prefix="/workspaces", tags=["workspaces"])

_optional_bearer = HTTPBearer(auto_error=False)

# Slug contract (docs/workspace-isolation.md §2 — code matches doc verbatim):
# lowercase alphanumeric + hyphen/underscore, 1..63 chars.
SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9\-_]{0,62}$")
SLUG_MAX_LENGTH = 63

BOOTSTRAP_USER_EMAIL = "local-dev"
BOOTSTRAP_WORKSPACE_NAME = "default"
BOOTSTRAP_WORKSPACE_SLUG = "default"


class DuplicateWorkspaceNameError(Exception):
    """Per-user workspace name collision (maps to 409)."""


def utcnow() -> datetime:
    """Timezone-aware UTC now (ClickHouse DateTime compatible)."""
    return datetime.now(timezone.utc)


def auto_slug(name: str) -> str:
    """Derive a contract-valid slug from a display name."""
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-_")
    slug = re.sub(r"-{2,}", "-", slug)[:SLUG_MAX_LENGTH].strip("-_")
    return slug or "workspace"


def check_slug(slug: str) -> str:
    """Validate an explicit slug against the contract (malformed -> ValueError)."""
    if not SLUG_RE.match(slug):
        raise ValueError(
            "slug must match ^[a-z0-9][a-z0-9-_]{0,62}$ "
            "(lowercase alphanumeric plus hyphen/underscore)"
        )
    return slug


@dataclass
class WorkspaceRecord:
    """One row of the `workspaces` table (migration 003)."""

    workspace_id: str
    user_id: str
    name: str
    slug: str
    app_type: str = ""
    cloud_provider: str = "unknown"
    endpoints: list[str] = field(default_factory=list)
    expected_volume: str = "<100"
    retention_days: int = 30
    created_at: datetime = field(default_factory=utcnow)
    deleted: bool = False


class WorkspaceRegistry:
    """Thread-safe workspace store (dev/tests memory backend).

    Production ClickHouse mirroring lives in migration 003 + the
    best-effort mirror in provision.py; the registry stays memory-backed so
    the identity image needs no storage/ copy (see identity/Dockerfile).
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_id: dict[str, WorkspaceRecord] = {}
        self._slugs: set[str] = set()

    def reset(self) -> None:
        """Clear all state (tests only)."""
        with self._lock:
            self._by_id.clear()
            self._slugs.clear()

    def _unique_slug(self, desired: str) -> str:
        slug, counter = desired, 2
        while slug in self._slugs:
            suffix = f"-{counter}"
            slug = desired[: SLUG_MAX_LENGTH - len(suffix)] + suffix
            counter += 1
        return slug

    def create(
        self,
        user_id: str,
        name: str,
        slug: Optional[str] = None,
        app_type: str = "",
        cloud_provider: str = "unknown",
        endpoints: Optional[list[str]] = None,
        expected_volume: str = "<100",
        retention_days: int = 30,
    ) -> WorkspaceRecord:
        """Create a workspace (per-user name dup -> 409, slug collision -> suffix)."""
        cleaned = name.strip()
        with self._lock:
            for record in self._by_id.values():
                if (
                    record.user_id == user_id
                    and not record.deleted
                    and record.name == cleaned
                ):
                    raise DuplicateWorkspaceNameError(cleaned)
            desired = check_slug(slug) if slug else auto_slug(cleaned)
            final_slug = self._unique_slug(desired)
            record = WorkspaceRecord(
                workspace_id=str(uuid.uuid4()),
                user_id=user_id,
                name=cleaned,
                slug=final_slug,
                app_type=app_type,
                cloud_provider=cloud_provider,
                endpoints=list(endpoints or []),
                expected_volume=expected_volume,
                retention_days=retention_days,
            )
            self._by_id[record.workspace_id] = record
            self._slugs.add(final_slug)
            return record

    def list_mine(self, user_id: str) -> list[WorkspaceRecord]:
        """Live workspaces owned by a user (tombstones excluded)."""
        with self._lock:
            return [
                record
                for record in self._by_id.values()
                if record.user_id == user_id and not record.deleted
            ]

    def get_owned(self, workspace_id: str, user_id: str) -> Optional[WorkspaceRecord]:
        """Live workspace iff owned by user (None covers missing/other/deleted)."""
        with self._lock:
            record = self._by_id.get(workspace_id)
            if record is None or record.deleted or record.user_id != user_id:
                return None
            return record

    def rename(self, workspace_id: str, user_id: str, name: str) -> Optional[WorkspaceRecord]:
        """Rename an owned workspace (slug immutable; name dup -> 409)."""
        cleaned = name.strip()
        with self._lock:
            record = self._by_id.get(workspace_id)
            if record is None or record.deleted or record.user_id != user_id:
                return None
            for other in self._by_id.values():
                if (
                    other.workspace_id != workspace_id
                    and other.user_id == user_id
                    and not other.deleted
                    and other.name == cleaned
                ):
                    raise DuplicateWorkspaceNameError(cleaned)
            record.name = cleaned
            return record

    def tombstone(self, workspace_id: str, user_id: str) -> Optional[WorkspaceRecord]:
        """Soft-delete (deleted=1; data retained 7d, never purged here)."""
        with self._lock:
            record = self._by_id.get(workspace_id)
            if record is None or record.deleted or record.user_id != user_id:
                return None
            record.deleted = True
            return record


_REGISTRY = WorkspaceRegistry()


def get_registry() -> WorkspaceRegistry:
    """Return the module-level workspace registry."""
    return _REGISTRY


def reset_registry() -> None:
    """Clear workspace state (tests only)."""
    _REGISTRY.reset()


# ---------------------------------------------------------------------------
# Request/response models
# ---------------------------------------------------------------------------


class WorkspaceCreate(BaseModel):
    """POST /workspaces body (slug optional -> auto-slug; malformed -> 422)."""

    name: str = Field(min_length=1, max_length=200)
    slug: Optional[str] = Field(default=None, max_length=SLUG_MAX_LENGTH)
    app_type: str = Field(default="", max_length=50)
    cloud_provider: str = Field(default="unknown", max_length=50)
    endpoints: list[str] = Field(default_factory=list)
    expected_volume: str = Field(default="<100", max_length=20)
    retention_days: int = Field(default=30, ge=1, le=3650)


class WorkspaceRename(BaseModel):
    """PATCH /workspaces/{id} body (name only — slug immutable)."""

    name: str = Field(min_length=1, max_length=200)


class WorkspaceResponse(BaseModel):
    """Workspace JSON (never leaks other users' rows; deleted never listed)."""

    workspace_id: str
    name: str
    slug: str
    app_type: str
    cloud_provider: str
    endpoints: list[str]
    expected_volume: str
    retention_days: int
    created_at: str
    kafka_topic_prefix: str
    clickhouse_database: str
    minio_prefix: str
    neo4j_workspace: str
    k8s_namespace: str


class ConnectionBundle(BaseModel):
    """Provision return values: endpoint notes + config values, NOT secrets."""

    kafka_topic_prefix: str
    clickhouse_database: str
    minio_prefix: str
    neo4j_workspace: str
    k8s_namespace: str
    otlp_note: str
    agent_config: dict[str, str]


class WorkspaceCreateResponse(BaseModel):
    """201 response: workspace + provision statuses + connection bundle."""

    workspace: WorkspaceResponse
    provisioning: dict[str, str]
    connection_bundle: ConnectionBundle


class SwitchResponse(BaseModel):
    """POST /workspaces/{id}/switch response (fresh access JWT with ws)."""

    access_token: str
    token_type: str = "bearer"
    expires_in: int
    workspace_id: str


# ---------------------------------------------------------------------------
# Caller resolution: JWT when present, default bootstrap otherwise
# ---------------------------------------------------------------------------


def _naming() -> Any:
    """storage.config helpers (lazy: identity image ships without storage/)."""
    from identity.provision import naming_helpers

    return naming_helpers()


def to_response(record: WorkspaceRecord) -> WorkspaceResponse:
    """Render a workspace row with its isolation mapping (doc §1 verbatim)."""
    naming = _naming()
    created = record.created_at
    created_at = created.isoformat() if hasattr(created, "isoformat") else str(created)
    return WorkspaceResponse(
        workspace_id=record.workspace_id,
        name=record.name,
        slug=record.slug,
        app_type=record.app_type,
        cloud_provider=record.cloud_provider,
        endpoints=list(record.endpoints),
        expected_volume=record.expected_volume,
        retention_days=record.retention_days,
        created_at=created_at,
        kafka_topic_prefix=naming.kafka_prefix(record.slug),
        clickhouse_database=naming.clickhouse_db(record.slug),
        minio_prefix=naming.minio_prefix(record.slug),
        neo4j_workspace=record.slug,
        k8s_namespace=naming.k8s_namespace(record.slug),
    )


async def resolve_caller(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_optional_bearer),
) -> Any:
    """Caller identity: Bearer JWT -> require_user; absent -> bootstrap identity.

    The bootstrap path auto-provisions user `local-dev` + workspace `default`
    (slug `default`, bare-name mappings) so unauthenticated compose/tests/E2E
    keep working byte-identical. Authenticated callers scope strictly to
    their own workspaces.
    """
    from identity import auth as auth_module

    if credentials is not None and credentials.credentials:
        return await auth_module.require_user(credentials)
    store = auth_module.get_store()
    user = store.get_user_by_email(BOOTSTRAP_USER_EMAIL)
    if user is None:
        user = store.create_user(
            BOOTSTRAP_USER_EMAIL,
            auth_module.hash_password(f"bootstrap-{uuid.uuid4()}"),
        )
    registry = get_registry()
    existing = [w for w in registry.list_mine(user.user_id) if w.slug == BOOTSTRAP_WORKSPACE_SLUG]
    workspace = existing[0] if existing else None
    if workspace is None:
        workspace = registry.create(
            user.user_id,
            BOOTSTRAP_WORKSPACE_NAME,
            slug=BOOTSTRAP_WORKSPACE_SLUG,
        )
        try:
            from identity import provision as provision_module

            provision_module.provision_workspace(workspace)
        except Exception:  # noqa: BLE001 - bootstrap must never fail boot
            logger.warning("default workspace provision degraded", exc_info=True)
    return auth_module.AuthContext(
        user_id=user.user_id,
        email=user.email,
        workspace_id=workspace.workspace_id,
        claims={"sub": user.user_id, "ws": workspace.workspace_id},
    )


def _forbidden() -> HTTPException:
    """403 for missing/other-user/tombstoned ids (never 404-leak, never 200)."""
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="workspace not found or access denied",
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.post("", response_model=WorkspaceCreateResponse, status_code=status.HTTP_201_CREATED)
def create_workspace(body: WorkspaceCreate, ctx: Any = Depends(resolve_caller)) -> WorkspaceCreateResponse:
    """Validate -> row -> CH DB+tables -> MinIO prefixes -> Neo4j node -> bundle."""
    from identity import provision as provision_module

    if body.slug is not None:
        try:
            check_slug(body.slug)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
    try:
        record = get_registry().create(
            ctx.user_id,
            body.name,
            slug=body.slug,
            app_type=body.app_type,
            cloud_provider=body.cloud_provider,
            endpoints=body.endpoints,
            expected_volume=body.expected_volume,
            retention_days=body.retention_days,
        )
    except DuplicateWorkspaceNameError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="workspace name already exists",
        ) from exc
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    result = provision_module.provision_workspace(record)
    logger.info("workspace created slug=%s user_id=%s", record.slug, ctx.user_id)
    return WorkspaceCreateResponse(
        workspace=to_response(record),
        provisioning=result.statuses,
        connection_bundle=result.bundle,
    )


@router.get("", response_model=list[WorkspaceResponse])
def list_workspaces(ctx: Any = Depends(resolve_caller)) -> list[WorkspaceResponse]:
    """List my live workspaces (unauthenticated -> [default] via bootstrap)."""
    return [to_response(record) for record in get_registry().list_mine(ctx.user_id)]


@router.get("/{workspace_id}", response_model=WorkspaceResponse)
def get_workspace(workspace_id: str, ctx: Any = Depends(resolve_caller)) -> WorkspaceResponse:
    """Single workspace iff owned (else 403 — never 404-leak)."""
    record = get_registry().get_owned(workspace_id, ctx.user_id)
    if record is None:
        raise _forbidden()
    return to_response(record)


@router.patch("/{workspace_id}", response_model=WorkspaceResponse)
def rename_workspace(
    workspace_id: str, body: WorkspaceRename, ctx: Any = Depends(resolve_caller)
) -> WorkspaceResponse:
    """Rename an owned workspace (slug immutable; dup name -> 409; else 403)."""
    try:
        record = get_registry().rename(workspace_id, ctx.user_id, body.name)
    except DuplicateWorkspaceNameError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="workspace name already exists",
        ) from exc
    if record is None:
        raise _forbidden()
    return to_response(record)


@router.delete("/{workspace_id}")
def delete_workspace(workspace_id: str, ctx: Any = Depends(resolve_caller)) -> dict[str, Any]:
    """Tombstone (deleted=1; data retained 7d, never purged here)."""
    record = get_registry().tombstone(workspace_id, ctx.user_id)
    if record is None:
        raise _forbidden()
    logger.info("workspace tombstoned slug=%s user_id=%s", record.slug, ctx.user_id)
    return {"workspace_id": workspace_id, "deleted": True}


@router.post("/{workspace_id}/switch", response_model=SwitchResponse)
def switch_workspace(workspace_id: str, ctx: Any = Depends(resolve_caller)) -> SwitchResponse:
    """Switch active workspace: re-issue access JWT with the `ws` claim."""
    from identity import auth as auth_module
    from identity.tokens import create_access_token

    record = get_registry().get_owned(workspace_id, ctx.user_id)
    if record is None:
        raise _forbidden()
    settings = auth_module.get_settings()
    secret = auth_module.get_jwt_secret()
    token = create_access_token(ctx.user_id, secret, settings.jwt_ttl_s, workspace_id)
    logger.info("workspace switch user_id=%s ws=%s", ctx.user_id, workspace_id)
    return SwitchResponse(
        access_token=token,
        expires_in=settings.jwt_ttl_s,
        workspace_id=workspace_id,
    )
