"""
OmniWatch — Entry-Point / Identity Layer
Component: Registry persistence (mirror + reload)
Phase: entry-point (exporter-importer-split)
Purpose: Survive identity restarts. Every registry mutation is mirrored
         best-effort into ClickHouse (never fails the request); on boot
         reload_all() rebuilds all in-memory registries from those tables.
         Active ONLY when OMNIWATCH_IDENTITY_STORE=clickhouse (same flag as
         provision.py's row mirror); memory mode is a strict no-op so unit
         tests stay hermetic.
Inputs: Registry records (workspaces, importers, exporters, agent bindings)
Outputs: CH rows (mirror_*) / restored registry counts (reload_all);
         "disabled" / "degraded ..." status strings, never exceptions
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional

logger = logging.getLogger("omniwatch.identity.persistence")


def _enabled() -> bool:
    return os.getenv(
        "OMNIWATCH_IDENTITY_STORE",
        os.getenv("IDENTITY_STORE", "memory"),
    ).strip().lower() == "clickhouse"


def _connect() -> Any:
    """Open a ClickHouse client from env (same names as provision.py)."""
    import clickhouse_connect

    host = os.getenv("OMNIWATCH_CLICKHOUSE_HOST",
                     os.getenv("CLICKHOUSE_HOST", "localhost"))
    port = int(os.getenv("OMNIWATCH_CLICKHOUSE_HTTP_PORT",
                         os.getenv("CLICKHOUSE_PORT", "8123")))
    database = os.getenv("OMNIWATCH_CLICKHOUSE_DB",
                         os.getenv("CLICKHOUSE_DB", "omniwatch"))
    user = os.getenv("OMNIWATCH_CLICKHOUSE_USER",
                     os.getenv("CLICKHOUSE_USER", "default"))
    password = os.getenv("OMNIWATCH_CLICKHOUSE_PASSWORD",
                         os.getenv("CLICKHOUSE_PASSWORD", ""))
    return clickhouse_connect.get_client(
        host=host, port=port, database=database, username=user,
        password=password, connect_timeout=2, send_receive_timeout=5,
    )


def _naive(dt: datetime) -> datetime:
    return dt.replace(tzinfo=None) if dt.tzinfo is not None else dt


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _aware(dt: Any) -> datetime:
    if isinstance(dt, datetime):
        return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Mirrors (append-only; reload takes latest row per key, skips deleted)
# ---------------------------------------------------------------------------

def mirror_workspace(record: Any) -> str:
    """Mirror a workspace row (create/rename/tombstone all append)."""
    if not _enabled():
        return "disabled"
    try:
        client = _connect()
        try:
            client.insert(
                "workspaces",
                [[
                    str(record.workspace_id), str(record.user_id),
                    record.name, record.slug, record.app_type,
                    record.cloud_provider, list(record.endpoints),
                    record.expected_volume, int(record.retention_days),
                    _naive(record.created_at),
                    1 if record.deleted else 0,
                    _naive(_utcnow()),
                ]],
                column_names=[
                    "workspace_id", "user_id", "name", "slug", "app_type",
                    "cloud_provider", "endpoints", "expected_volume",
                    "retention_days", "created_at", "deleted", "mirrored_at",
                ],
            )
        finally:
            client.close()
        return "ok"
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("workspace mirror degraded slug=%s: %s",
                       getattr(record, "slug", "?"), exc)
        return f"degraded: {exc}"


def mirror_importer(record: Any) -> str:
    """Mirror an importer row (issuance + rotation both append; no deletes)."""
    if not _enabled():
        return "disabled"
    try:
        client = _connect()
        try:
            client.insert(
                "workspace_importers",
                [[
                    str(record.workspace_id), str(record.user_id),
                    record.slug, record.endpoint_url, record.token_hash,
                    _naive(record.created_at),
                    _naive(_utcnow()),
                ]],
                column_names=[
                    "workspace_id", "user_id", "slug", "endpoint_url",
                    "token_hash", "created_at", "mirrored_at",
                ],
            )
        finally:
            client.close()
        return "ok"
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("importer mirror degraded ws=%s: %s",
                       getattr(record, "workspace_id", "?"), exc)
        return f"degraded: {exc}"


def mirror_exporter(record: Any, deleted: bool = False) -> str:
    """Mirror an exporter row (register appends; delete appends tombstone)."""
    if not _enabled():
        return "disabled"
    try:
        client = _connect()
        try:
            client.insert(
                "workspace_exporters",
                [[
                    str(record.workspace_id), str(record.user_id),
                    int(record.number), record.name,
                    _naive(record.created_at), 1 if deleted else 0,
                    _naive(_utcnow()),
                ]],
                column_names=[
                    "workspace_id", "user_id", "number", "name",
                    "created_at", "deleted", "mirrored_at",
                ],
            )
        finally:
            client.close()
        return "ok"
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("exporter mirror degraded ws=%s: %s",
                       getattr(record, "workspace_id", "?"), exc)
        return f"degraded: {exc}"


def mirror_agent(record: Any, deleted: bool = False) -> str:
    """Mirror an agent binding (pair appends; unbind appends tombstone)."""
    if not _enabled():
        return "disabled"
    try:
        client = _connect()
        try:
            client.insert(
                "agent_bindings",
                [[
                    str(record.binding_id), str(record.user_id),
                    str(record.workspace_id), record.workspace_slug,
                    record.agent_endpoint, record.token_hash,
                    record.status, _naive(record.created_at),
                    1 if deleted else 0,
                    _naive(_utcnow()),
                ]],
                column_names=[
                    "binding_id", "user_id", "workspace_id",
                    "workspace_slug", "agent_endpoint", "token_hash",
                    "status", "created_at", "deleted", "mirrored_at",
                ],
            )
        finally:
            client.close()
        return "ok"
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("agent mirror degraded binding=%s: %s",
                       getattr(record, "binding_id", "?"), exc)
        return f"degraded: {exc}"


# ---------------------------------------------------------------------------
# Reload (boot; latest row per key wins, deleted rows skipped)
# ---------------------------------------------------------------------------

def _latest_rows(client: Any, table: str, columns: str, key_cols: str) -> list:
    """Newest row per key (explicit columns: live tables may carry extra
    columns from later migrations, e.g. onboarding_* on workspaces).
    Ordering is by mirrored_at (write time), NOT created_at: renames and
    tombstones reuse the record's original created_at, so created_at ties
    would resurrect stale state."""
    return client.query(
        f"SELECT {columns} FROM {table} ORDER BY mirrored_at DESC "
        f"LIMIT 1 BY {key_cols}"
    ).result_rows


def reload_all() -> dict[str, Any]:
    """Rebuild every registry from ClickHouse; per-table degraded, never fatal."""
    counts: dict[str, Any] = {
        "workspaces": 0, "importers": 0, "exporters": 0, "agents": 0}
    if not _enabled():
        counts["mode"] = "disabled (memory store)"
        return counts
    try:
        from identity.workspaces import WorkspaceRecord, get_registry
        from identity.importers import ImporterRecord, get_importer_registry
        from identity.exporters import ExporterRecord, get_exporter_registry
        from identity.agents import AgentBinding, get_agent_registry

        client = _connect()
        try:
            # Server-side LIMIT 1 BY already picks the newest row per key;
            # the seen-sets below are a second net (duplicates must never
            # resurrect stale names, dead rows, or rotated-out hashes).
            seen_ws: set[str] = set()
            for row in _latest_rows(
                client, "workspaces",
                "workspace_id, user_id, name, slug, app_type,"
                " cloud_provider, endpoints, expected_volume,"
                " retention_days, created_at, deleted",
                "workspace_id",
            ):
                (ws_id, user_id, name, slug, app_type, cloud,
                 endpoints, volume, retention, created, deleted) = row
                if deleted or str(ws_id) in seen_ws:
                    continue
                seen_ws.add(str(ws_id))
                get_registry().restore(WorkspaceRecord(
                    workspace_id=str(ws_id), user_id=str(user_id),
                    name=name, slug=slug, app_type=app_type or "",
                    cloud_provider=cloud or "unknown",
                    endpoints=list(endpoints or []),
                    expected_volume=volume or "<100",
                    retention_days=int(retention or 30),
                    created_at=_aware(created),
                ))
                counts["workspaces"] += 1
            seen_imp: set[str] = set()
            for row in _latest_rows(
                client, "workspace_importers",
                "workspace_id, user_id, slug, endpoint_url,"
                " token_hash, created_at",
                "workspace_id",
            ):
                (ws_id, user_id, slug, endpoint,
                 token_hash, created) = row
                if str(ws_id) in seen_imp:
                    continue
                seen_imp.add(str(ws_id))
                get_importer_registry().restore(ImporterRecord(
                    workspace_id=str(ws_id), user_id=str(user_id),
                    slug=slug, endpoint_url=endpoint,
                    token_hash=token_hash, created_at=_aware(created),
                ))
                counts["importers"] += 1
            seen_exp: set[tuple[str, int]] = set()
            for row in _latest_rows(
                client, "workspace_exporters",
                "workspace_id, user_id, number, name,"
                " created_at, deleted",
                "workspace_id, number",
            ):
                (ws_id, user_id, number, name, created, deleted) = row
                key = (str(ws_id), int(number))
                if deleted or key in seen_exp:
                    continue
                seen_exp.add(key)
                get_exporter_registry().restore(ExporterRecord(
                    workspace_id=str(ws_id), user_id=str(user_id),
                    number=int(number), name=name,
                    created_at=_aware(created),
                ))
                counts["exporters"] += 1
            seen_bindings: set[str] = set()
            for row in _latest_rows(
                client, "agent_bindings",
                "binding_id, user_id, workspace_id, workspace_slug,"
                " agent_endpoint, token_hash, status, created_at, deleted",
                "binding_id",
            ):
                (bid, user_id, ws_id, slug, endpoint, token_hash,
                 st, created, deleted) = row
                if deleted or str(bid) in seen_bindings:
                    continue
                seen_bindings.add(str(bid))
                get_agent_registry().restore(AgentBinding(
                    binding_id=str(bid), user_id=str(user_id),
                    workspace_id=str(ws_id), workspace_slug=slug,
                    agent_endpoint=endpoint, token_hash=token_hash,
                    status=st or "connected", created_at=_aware(created),
                ))
                counts["agents"] += 1
        finally:
            client.close()
    except Exception as exc:  # noqa: BLE001 - degraded boot, never fatal
        logger.warning("identity reload degraded: %s", exc)
        counts["degraded"] = str(exc)
    return counts
