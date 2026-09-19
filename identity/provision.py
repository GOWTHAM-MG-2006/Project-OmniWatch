"""
OmniWatch — Entry-Point / Workspace Layer
Component: Provisioning chain (CH DB+tables, MinIO prefixes, Neo4j node)
Phase: entry-point (Wave 2, todo 2)
Purpose: Execute POST /workspaces provisioning: validate -> row -> ClickHouse
         database+tables -> MinIO prefixes -> Neo4j Workspace node ->
         connection bundle (endpoint notes + config values, NOT secrets).
         Every backend step is best-effort (ok|skipped|degraded) so the 201
         never fails on infra availability.
Inputs: WorkspaceRecord (identity/workspaces.py)
Outputs: ProvisionResult(statuses per backend + ConnectionBundle)
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable

if TYPE_CHECKING:  # type-only; runtime stays decoupled (no storage/ import)
    from identity.workspaces import WorkspaceRecord

logger = logging.getLogger("omniwatch.identity.provision")


@dataclass
class Naming:
    """Isolation naming (docs/workspace-isolation.md §1 — verbatim)."""

    kafka_prefix: Callable[[str], str]
    clickhouse_db: Callable[[str], str]
    minio_prefix: Callable[[str], str]
    k8s_namespace: Callable[[str], str]


def naming_helpers() -> Naming:
    """storage.config helpers when importable, else doc-verbatim fallbacks.

    The identity image ships without storage/ (see identity/Dockerfile), so
    the import MUST stay lazy with identical fallback formulas.
    """
    try:
        from storage import config as storage_config

        return Naming(
            kafka_prefix=storage_config.workspace_kafka_prefix,
            clickhouse_db=storage_config.workspace_database,
            minio_prefix=storage_config.workspace_prefix,
            k8s_namespace=storage_config.workspace_k8s_namespace,
        )
    except Exception:  # noqa: BLE001 - fallback keeps the service bootable
        return Naming(
            kafka_prefix=lambda slug: "" if slug == "default" else f"ws_{slug}.",
            clickhouse_db=lambda slug: "omniwatch" if slug == "default" else f"omniwatch_ws_{slug}",
            minio_prefix=lambda slug: "" if slug == "default" else f"workspaces/{slug}/",
            k8s_namespace=lambda slug: "omniwatch" if slug == "default" else f"omniwatch-ws-{slug}",
        )


@dataclass
class ProvisionResult:
    """Per-backend statuses + connection bundle for POST /workspaces."""

    statuses: dict[str, str]
    bundle: Any  # ConnectionBundle (imported lazily to avoid a cycle)


def connection_bundle(record: WorkspaceRecord) -> Any:
    """Build the connection bundle (values + notes, never secrets)."""
    from identity.workspaces import BOOTSTRAP_WORKSPACE_SLUG

    naming = naming_helpers()
    slug = record.slug
    prefix = naming.kafka_prefix(slug)
    return {
        "kafka_topic_prefix": prefix,
        "clickhouse_database": naming.clickhouse_db(slug),
        "minio_prefix": naming.minio_prefix(slug),
        "neo4j_workspace": slug,
        "k8s_namespace": naming.k8s_namespace(slug),
        "otlp_note": (
            "send OTLP with resource attribute omniwatch.workspace="
            f"{slug}; collector routes to {prefix or '(bare topics)'}"
        ),
        "agent_config": {
            "omniwatch_workspace": slug,
            "kafka_topic_prefix": prefix or "(bare names — default workspace)",
            "clickhouse_database": naming.clickhouse_db(slug),
            "minio_prefix": naming.minio_prefix(slug)
            or "(bare keys — default workspace)",
        },
    }


def _provision_clickhouse(record: WorkspaceRecord, naming: Naming) -> str:
    """CREATE DATABASE + clone schema.sql tables (default: skip, uses omniwatch)."""
    if record.slug == "default":
        return "skipped (default workspace uses omniwatch)"
    import clickhouse_connect

    host = os.getenv("OMNIWATCH_CLICKHOUSE_HOST", os.getenv("CLICKHOUSE_HOST", "localhost"))
    port = int(os.getenv("OMNIWATCH_CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123")))
    user = os.getenv("OMNIWATCH_CLICKHOUSE_USER", os.getenv("CLICKHOUSE_USER", "default"))
    password = os.getenv("OMNIWATCH_CLICKHOUSE_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    db = naming.clickhouse_db(record.slug)
    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password,
        connect_timeout=2, send_receive_timeout=5,
    )
    try:
        client.command(f"CREATE DATABASE IF NOT EXISTS `{db}`")
        import importlib

        schema_migration = importlib.import_module(
            "storage.clickhouse.migrations.001_initial_schema"
        )
        for statement in schema_migration.load_schema_statements():
            if "CREATE DATABASE" in statement:
                continue  # database already created above
            # Backtick-quote: slugs allow hyphens (entry02-proof) which are
            # illegal in bare ClickHouse identifiers.
            scoped = statement.replace("omniwatch.", f"`{db}`.")
            client.command(scoped)
    finally:
        client.close()
    return "ok"


def _tcp_open(host: str, port: int, timeout: float = 2.0) -> None:
    """Fail fast when a backend endpoint is unreachable (no retry storms).

    Raises:
        ConnectionError: TCP connect refused/timed-out within `timeout`.
    """
    import socket

    try:
        with socket.create_connection((host, int(port)), timeout=timeout):
            return
    except (OSError, ValueError) as exc:
        raise ConnectionError(f"{host}:{port} unreachable: {exc}") from exc


def _split_endpoint(endpoint: str) -> tuple[str, int]:
    """Split a `host:port` endpoint (no scheme) into parts."""
    host, _, port = endpoint.rpartition(":")
    return host or "localhost", int(port)


def _provision_minio(record: WorkspaceRecord, naming: Naming) -> str:
    """Ensure workspaces/<slug>/ marker prefixes in existing buckets."""
    if record.slug == "default":
        return "skipped (default workspace uses bare keys)"
    from minio import Minio

    endpoint = os.getenv("OMNIWATCH_MINIO_ENDPOINT", os.getenv("MINIO_ENDPOINT", "localhost:9010"))
    _tcp_open(*_split_endpoint(endpoint))

    access = os.getenv("OMNIWATCH_MINIO_ACCESS_KEY", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    secret = os.getenv("OMNIWATCH_MINIO_SECRET_KEY", os.getenv("MINIO_SECRET_KEY", "minioadmin"))
    secure = os.getenv("OMNIWATCH_MINIO_SECURE", os.getenv("MINIO_SECURE", "false")).lower() in (
        "1", "true", "yes", "on",
    )
    try:
        from storage.config import MINIO_BUCKETS
        buckets = list(MINIO_BUCKETS.values())
    except Exception:  # noqa: BLE001 - image without storage/
        buckets = [
            "omniwatch-telemetry-archive",
            "omniwatch-incidents",
            "omniwatch-audit-logs",
            "omniwatch-runbooks",
            "omniwatch-ml-datasets",
            "omniwatch-dashboards",
        ]
    import io

    client = Minio(endpoint, access_key=access, secret_key=secret, secure=secure)
    prefix = naming.minio_prefix(record.slug)
    marker = f"{prefix}.omniwatch-workspace".lstrip("/")
    for bucket in buckets:
        if not client.bucket_exists(bucket):
            continue
        client.put_object(bucket, marker, io.BytesIO(b'{"workspace": "%s"}' % record.slug.encode()),
                           length=len(record.slug) + 17, content_type="application/json")
    return "ok"


def _provision_neo4j(record: WorkspaceRecord) -> str:
    """MERGE (:Workspace{slug}) scoping node (idempotent)."""
    from neo4j import GraphDatabase

    uri = os.getenv("OMNIWATCH_NEO4J_URI", os.getenv("NEO4J_URI", "bolt://localhost:7687"))
    user = os.getenv("OMNIWATCH_NEO4J_USER", os.getenv("NEO4J_USER", "neo4j"))
    password = os.getenv("OMNIWATCH_NEO4J_PASSWORD", os.getenv("NEO4J_PASSWORD", "omniwatch"))
    hostport = uri.split("://", 1)[-1]
    _tcp_open(*_split_endpoint(hostport))
    driver = GraphDatabase.driver(uri, auth=(user, password), connection_timeout=2)
    try:
        with driver.session() as session:
            session.run(
                "MERGE (w:Workspace {slug: $slug}) "
                "ON CREATE SET w.created_at = datetime() RETURN w",
                slug=record.slug,
            )
    finally:
        driver.close()
    return "ok"


def _mirror_row(record: WorkspaceRecord) -> str:
    """Best-effort mirror of the workspace row into CH `workspaces` (migration 003)."""
    if os.getenv("OMNIWATCH_IDENTITY_STORE", os.getenv("IDENTITY_STORE", "memory")).strip().lower() != "clickhouse":
        return "skipped (memory store)"
    import clickhouse_connect

    host = os.getenv("OMNIWATCH_CLICKHOUSE_HOST", os.getenv("CLICKHOUSE_HOST", "localhost"))
    port = int(os.getenv("OMNIWATCH_CLICKHOUSE_HTTP_PORT", os.getenv("CLICKHOUSE_PORT", "8123")))
    database = os.getenv("OMNIWATCH_CLICKHOUSE_DB", os.getenv("CLICKHOUSE_DB", "omniwatch"))
    user = os.getenv("OMNIWATCH_CLICKHOUSE_USER", os.getenv("CLICKHOUSE_USER", "default"))
    password = os.getenv("OMNIWATCH_CLICKHOUSE_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", ""))
    client = clickhouse_connect.get_client(
        host=host, port=port, database=database, username=user,
        password=password, connect_timeout=2, send_receive_timeout=5,
    )
    try:
        client.insert(
            "workspaces",
            [[
                record.workspace_id, record.user_id, record.name, record.slug,
                record.app_type, record.cloud_provider, list(record.endpoints),
                record.expected_volume, record.retention_days,
                record.created_at.replace(tzinfo=None), 1 if record.deleted else 0,
            ]],
            column_names=[
                "workspace_id", "user_id", "name", "slug", "app_type",
                "cloud_provider", "endpoints", "expected_volume",
                "retention_days", "created_at", "deleted",
            ],
        )
    finally:
        client.close()
    return "ok"


def provision_workspace(record: WorkspaceRecord) -> ProvisionResult:
    """Run the §5 chain; each backend degrades independently (201 never fails)."""
    from identity.workspaces import ConnectionBundle

    naming = naming_helpers()
    statuses: dict[str, str] = {}
    try:
        statuses["row"] = _mirror_row(record)
    except Exception as exc:  # noqa: BLE001 - degraded, never fatal
        logger.warning("workspace row mirror degraded slug=%s: %s", record.slug, exc)
        statuses["row"] = f"degraded: {exc}"
    for backend, step in (
        ("clickhouse", lambda: _provision_clickhouse(record, naming)),
        ("minio", lambda: _provision_minio(record, naming)),
        ("neo4j", lambda: _provision_neo4j(record)),
    ):
        try:
            statuses[backend] = step()
        except Exception as exc:  # noqa: BLE001 - degraded, never fatal
            logger.warning("workspace provision %s degraded slug=%s: %s", backend, record.slug, exc)
            statuses[backend] = f"degraded: {type(exc).__name__}"
    bundle = ConnectionBundle(**connection_bundle(record))
    return ProvisionResult(statuses=statuses, bundle=bundle)
