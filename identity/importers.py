"""
OmniWatch — Entry-Point / Importer Layer
Component: Per-workspace importer credentials (mounted under /importers)
Phase: exporter-importer-split
Purpose: One importer identity per workspace: auto-generated endpoint URL +
         API token issued once at workspace creation. Exporters present the
         token as Bearer to the importer service, which resolves the
         workspace and writes only to that workspace's stores.
Inputs: JWT via resolve_caller; workspace ownership via workspace registry
Outputs: Endpoint URL + raw token (creation/rotation only, never stored);
         403 (never 404-leak)
"""

from __future__ import annotations

import logging
import os
import secrets
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, status
from pydantic import BaseModel

from identity.workspaces import get_registry, resolve_caller

logger = logging.getLogger("omniwatch.identity.importers")

router = APIRouter(prefix="/importers", tags=["importers"])


# Live tunnel base pushed by tunnel-registrar (container cloudflare method).
# When set, it wins over IMPORTER_PUBLIC_BASE_URL so both existing rows
# (via update_all_endpoints) and newly created workspaces pick up the fresh
# quick-tunnel URL without a container rebuild. Cleared on registry reset
# (test isolation); registrar re-pushes on every compose start.
_LAST_PUSHED_BASE: Optional[str] = None
_BASE_LOCK = threading.Lock()


def _base_url() -> str:
    with _BASE_LOCK:
        live = _LAST_PUSHED_BASE
    base = (live or os.getenv(
        "IMPORTER_PUBLIC_BASE_URL", "http://localhost:4320")).strip().rstrip("/")
    return base


def set_live_base_url(base_url: str) -> str:
    """Remember a registrar-pushed tunnel base (normalized, no /ingest)."""
    base = base_url.strip().rstrip("/")
    with _BASE_LOCK:
        global _LAST_PUSHED_BASE
        _LAST_PUSHED_BASE = base
    return base


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class ImporterRecord:
    workspace_id: str
    user_id: str
    slug: str
    endpoint_url: str
    token_hash: str
    created_at: datetime = field(default_factory=utcnow)


class ImporterRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_workspace: dict[str, ImporterRecord] = {}

    def reset(self) -> None:
        with self._lock:
            self._by_workspace.clear()

    def restore(self, record: ImporterRecord) -> None:
        """Load one row (boot reload from ClickHouse mirror)."""
        with self._lock:
            self._by_workspace[record.workspace_id] = record

    def ensure_for_workspace(
        self, user_id: str, workspace_id: str, slug: str
    ) -> tuple[ImporterRecord, str]:
        with self._lock:
            existing = self._by_workspace.get(workspace_id)
            if existing is not None and existing.user_id == user_id:
                raise DuplicateImporterError(workspace_id)
            from identity import auth as auth_module

            raw_token = secrets.token_urlsafe(32)
            record = ImporterRecord(
                workspace_id=workspace_id,
                user_id=user_id,
                slug=slug,
                endpoint_url=f"{_base_url()}/ingest",
                token_hash=auth_module.hash_password(raw_token),
            )
            self._by_workspace[workspace_id] = record
            try:
                from identity import persistence as persistence_module

                persistence_module.mirror_importer(record)
            except Exception:  # noqa: BLE001 - mirror must never fail issuance
                logger.warning("importer mirror degraded", exc_info=True)
            return record, raw_token

    def get_owned(self, user_id: str, workspace_id: str) -> Optional[ImporterRecord]:
        with self._lock:
            record = self._by_workspace.get(workspace_id)
            if record is None or record.user_id != user_id:
                return None
            return record

    def rotate(
        self, user_id: str, workspace_id: str
    ) -> Optional[tuple[ImporterRecord, str]]:
        with self._lock:
            record = self._by_workspace.get(workspace_id)
            if record is None or record.user_id != user_id:
                return None
            from identity import auth as auth_module

            raw_token = secrets.token_urlsafe(32)
            record.token_hash = auth_module.hash_password(raw_token)
            record.created_at = utcnow()
            try:
                from identity import persistence as persistence_module

                persistence_module.mirror_importer(record)
            except Exception:  # noqa: BLE001 - mirror must never fail rotation
                logger.warning("importer mirror degraded", exc_info=True)
            return record, raw_token

    def verify(self, raw_token: str) -> Optional[ImporterRecord]:
        """Resolve a raw importer token to its record (None when unknown)."""
        with self._lock:
            records = list(self._by_workspace.values())
        from identity import auth as auth_module

        for record in records:
            try:
                if auth_module.verify_password(raw_token, record.token_hash):
                    return record
            except Exception:  # noqa: BLE001 - one bad hash must not break verify
                continue
        return None

    def update_all_endpoints(self, base_url: str) -> int:
        """Point every importer row at a fresh tunnel base (de-freeze).

        Used by the tunnel-registrar push: quick-tunnel URLs rotate on every
        (re)start, so the stored endpoint_url must follow. Mirrors are
        degraded-safe — a ClickHouse outage never fails the push.
        Returns the number of rows updated.
        """
        endpoint = f"{base_url.strip().rstrip('/')}/ingest"
        with self._lock:
            records = list(self._by_workspace.values())
            for record in records:
                record.endpoint_url = endpoint
        try:
            from identity import persistence as persistence_module

            for record in records:
                try:
                    persistence_module.mirror_importer(record)
                except Exception:  # noqa: BLE001 - one bad mirror skips
                    logger.warning("endpoint mirror degraded", exc_info=True)
        except Exception:  # noqa: BLE001 - mirror must never fail the push
            logger.warning("endpoint mirror degraded", exc_info=True)
        return len(records)


class DuplicateImporterError(Exception):
    pass


_REGISTRY = ImporterRegistry()


def get_importer_registry() -> ImporterRegistry:
    return _REGISTRY


def reset_importer_registry() -> None:
    _REGISTRY.reset()
    with _BASE_LOCK:
        global _LAST_PUSHED_BASE
        _LAST_PUSHED_BASE = None


class ImporterInfo(BaseModel):
    workspace_id: str
    workspace_slug: str
    endpoint_url: str
    created_at: str
    # Raw token, present ONLY on first auto-provision (old workspaces minted
    # on first read). Stored hashed — never returned again afterwards.
    api_token: Optional[str] = None


class ImporterIssued(BaseModel):
    workspace_id: str
    workspace_slug: str
    endpoint_url: str
    api_token: str
    created_at: str


def _forbidden() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_403_FORBIDDEN,
        detail="importer not found or access denied",
    )


def _to_info(record: ImporterRecord, api_token: Optional[str] = None) -> ImporterInfo:
    return ImporterInfo(
        workspace_id=record.workspace_id,
        workspace_slug=record.slug,
        endpoint_url=record.endpoint_url,
        created_at=record.created_at.isoformat(),
        api_token=api_token,
    )


@router.get("/by-workspace/{workspace_id}", response_model=ImporterInfo)
def get_importer(workspace_id: str, ctx: Any = Depends(resolve_caller)) -> ImporterInfo:
    registry = get_importer_registry()
    record = registry.get_owned(ctx.user_id, workspace_id)
    if record is None:
        # Backfill: default/bootstrap + pre-split workspaces have no importer
        # yet. Auto-provision on first read iff the caller owns the workspace
        # (else 403 — never 404-leak). The raw token is revealed exactly
        # once, on this first read; later reads return metadata only.
        owned = get_registry().get_owned(workspace_id, ctx.user_id)
        if owned is None:
            raise _forbidden()
        try:
            record, raw_token = registry.ensure_for_workspace(
                ctx.user_id, workspace_id, owned.slug
            )
            return _to_info(record, api_token=raw_token)
        except DuplicateImporterError:
            record = registry.get_owned(ctx.user_id, workspace_id)
            if record is None:
                raise _forbidden()
    return _to_info(record)


@router.post("/{workspace_id}/rotate", response_model=ImporterIssued)
def rotate_importer(workspace_id: str, ctx: Any = Depends(resolve_caller)) -> ImporterIssued:
    rotated = get_importer_registry().rotate(ctx.user_id, workspace_id)
    if rotated is None:
        raise _forbidden()
    record, raw_token = rotated
    return ImporterIssued(
        workspace_id=record.workspace_id,
        workspace_slug=record.slug,
        endpoint_url=record.endpoint_url,
        api_token=raw_token,
        created_at=record.created_at.isoformat(),
    )


class VerifyRequest(BaseModel):
    token: str


class VerifyResponse(BaseModel):
    workspace_id: str
    workspace_slug: str


def _internal_secret() -> str:
    secret = os.getenv("IMPORTER_INTERNAL_SECRET", "")
    if not secret:
        logger.warning(
            "IMPORTER_INTERNAL_SECRET unset — using dev-only default; "
            "set it in production"
        )
        return "dev-only-internal-secret"
    return secret


@router.post("/verify", response_model=VerifyResponse)
def verify_importer_token(
    body: VerifyRequest,
    x_internal_secret: str = Header(default="", alias="X-Internal-Secret"),
) -> VerifyResponse:
    """Internal token->workspace resolution for the importer service.

    Guarded by the shared X-Internal-Secret — unknown token and bad secret
    both yield 403 so the endpoint is not a token oracle.
    """
    import hmac

    if not hmac.compare_digest(x_internal_secret, _internal_secret()):
        raise _forbidden()
    record = get_importer_registry().verify(body.token)
    if record is None:
        raise _forbidden()
    return VerifyResponse(workspace_id=record.workspace_id, workspace_slug=record.slug)


class EndpointPushRequest(BaseModel):
    # Fresh tunnel base WITHOUT /ingest, e.g.
    # https://discussed-shanghai-automobiles-judge.trycloudflare.com
    base_url: str


class EndpointPushResponse(BaseModel):
    updated: int
    endpoint_url: str


@router.post("/internal/endpoint", response_model=EndpointPushResponse)
def push_live_endpoint(
    body: EndpointPushRequest,
    x_internal_secret: str = Header(default="", alias="X-Internal-Secret"),
) -> EndpointPushResponse:
    """Tunnel-registrar push: adopt a fresh quick-tunnel base (de-freeze).

    Guarded by the shared X-Internal-Secret (same pattern as /verify) —
    bad secret yields 403, non-http(s) base yields 400. Updates every
    importer row (in-memory + ClickHouse mirror) and arms _base_url() so
    newly created workspaces also get the live URL. Idempotent: pushing
    the same URL twice is a no-op in effect.
    """
    import hmac

    if not hmac.compare_digest(x_internal_secret, _internal_secret()):
        raise _forbidden()
    base = body.base_url.strip().rstrip("/")
    if not (base.startswith("http://") or base.startswith("https://")):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="base_url must be an http(s) URL",
        )
    live = set_live_base_url(base)
    updated = get_importer_registry().update_all_endpoints(live)
    logger.info("live importer endpoint adopted: %s/ingest (%d rows)",
                live, updated)
    return EndpointPushResponse(updated=updated, endpoint_url=f"{live}/ingest")
