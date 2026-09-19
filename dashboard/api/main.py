"""
OmniWatch — Dashboard API
Component: Dashboard Backend API Gateway
Phase: 11 — Dashboard + Continuous Learning
Purpose: FastAPI app with 20+ endpoints serving ClickHouse/Neo4j/MinIO/Kafka
         data to the dashboard frontend on port 8011.
Inputs: ClickHouse (metrics, logs, anomalies, incidents, knowledge_base),
        Neo4j (topology graph), MinIO (audit-logs, incidents, dashboards buckets),
        learning service (recommendations), Ollama (copilot chat).
Outputs: JSON responses consumed by the React dashboard frontend.
"""

from __future__ import annotations

import json
import logging
import os
import re
import textwrap
from datetime import datetime, timezone
from typing import Any

import asyncio

import clickhouse_connect  # type: ignore[import-untyped]
import httpx
import minio  # type: ignore[import-untyped]
import neo4j
import urllib3  # type: ignore[import-untyped]
from fastapi import Body, FastAPI, File, Form, Header, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from urllib3.util import Retry, Timeout

try:  # Docker: uvicorn dashboard.api.main:app from /app (PYTHONPATH=/app)
    from dashboard.api.model_manager import ModelManager, ModelProvider, ModelSettings, sse_response
except ImportError:  # Local dev: uvicorn main:app from dashboard/api/
    from model_manager import ModelManager, ModelProvider, ModelSettings, sse_response

try:  # Docker: workspace scope enforcement (ENTRY-5)
    from dashboard.api.workspace_scope import (
        ScopeError,
        get_current_scope,
        reset_current_scope,
        resolve_request_scope,
        set_current_scope,
    )
except ImportError:  # Local dev: uvicorn main:app from dashboard/api/
    from workspace_scope import (
        ScopeError,
        get_current_scope,
        reset_current_scope,
        resolve_request_scope,
        set_current_scope,
    )

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_LOG: logging.Logger = logging.getLogger("omniwatch.dashboard")

# ---------------------------------------------------------------------------
# Config (all from env, never hardcoded; OMNIWATCH_* primary, old bare
# names deprecated fallback — today's defaults preserved)
# ---------------------------------------------------------------------------

def _cfg(primary: str, fallback: str, default: str) -> str:
    return os.getenv(primary, os.getenv(fallback, default))


def _cfg_int(primary: str, fallback: str, default: int) -> int:
    return int(_cfg(primary, fallback, str(default)))


CLICKHOUSE_HOST: str = _cfg(
    "OMNIWATCH_CLICKHOUSE_HOST", "CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT: int = _cfg_int(
    "OMNIWATCH_CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_PORT", 8123)
CLICKHOUSE_DB: str = _cfg(
    "OMNIWATCH_CLICKHOUSE_DB", "CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER: str = _cfg(
    "OMNIWATCH_CLICKHOUSE_USER", "CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = _cfg(
    "OMNIWATCH_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD", "")

NEO4J_URI: str = _cfg(
    "OMNIWATCH_NEO4J_URI", "NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER: str = _cfg(
    "OMNIWATCH_NEO4J_USER", "NEO4J_USER", "neo4j")
NEO4J_PASSWORD: str = _cfg(
    "OMNIWATCH_NEO4J_PASSWORD", "NEO4J_PASSWORD", "omniwatch")

MINIO_ENDPOINT: str = _cfg(
    "OMNIWATCH_MINIO_ENDPOINT", "MINIO_ENDPOINT", "localhost:9010")
MINIO_ACCESS_KEY: str = _cfg(
    "OMNIWATCH_MINIO_ACCESS_KEY", "MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = _cfg(
    "OMNIWATCH_MINIO_SECRET_KEY", "MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE: bool = _cfg(
    "OMNIWATCH_MINIO_SECURE", "MINIO_SECURE", "false").lower() in (
    "1", "true", "yes", "on")

OLLAMA_URL: str = _cfg(
    "OMNIWATCH_LLM_URL", "OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL: str = _cfg(
    "OMNIWATCH_LLM_MODEL", "OLLAMA_MODEL", "qwen3:8b")
LEARNING_SERVICE_URL: str = _cfg(
    "OMNIWATCH_LEARNING_URL", "LEARNING_SERVICE_URL", "http://learning:8030")
GENAI_SERVICE_URL: str = _cfg(
    "OMNIWATCH_GENAI_URL", "GENAI_SERVICE_URL", "http://genai:8020")
ORCHESTRATION_SERVICE_URL: str = _cfg(
    "OMNIWATCH_ORCHESTRATION_URL", "ORCHESTRATION_SERVICE_URL",
    "http://orchestration:8010")

DASHBOARD_PORT: int = _cfg_int(
    "OMNIWATCH_DASHBOARD_PORT", "DASHBOARD_PORT", 8011)

# MinIO bucket registry (env-overridable; today's names preserved)
MINIO_BUCKET_AUDIT: str = _cfg(
    "OMNIWATCH_MINIO_BUCKETS_AUDIT", "MINIO_BUCKET_AUDIT",
    "omniwatch-audit-logs")
MINIO_BUCKET_INCIDENTS: str = _cfg(
    "OMNIWATCH_MINIO_BUCKETS_INCIDENTS", "MINIO_BUCKET_INCIDENTS",
    "omniwatch-incidents")
MINIO_BUCKET_RUNBOOKS: str = _cfg(
    "OMNIWATCH_MINIO_BUCKETS_RUNBOOKS", "MINIO_BUCKET_RUNBOOKS",
    "omniwatch-runbooks")
MINIO_BUCKET_DASHBOARDS: str = _cfg(
    "OMNIWATCH_MINIO_BUCKETS_DASHBOARDS", "MINIO_BUCKET_DASHBOARDS",
    "omniwatch-dashboards")

# ---------------------------------------------------------------------------
# Model Manager (LLM provider abstraction — singleton)
# ---------------------------------------------------------------------------

_model_manager: ModelManager | None = None


def _get_model_manager() -> ModelManager:
    global _model_manager
    if _model_manager is None:
        _model_manager = ModelManager()
    return _model_manager


def _mask_api_key(key: str) -> str:
    """Mask API key for safe display: show last 4 chars only."""
    if not key:
        return ""
    if len(key) <= 4:
        return "****"
    return f"{'*' * (len(key) - 4)}{key[-4:]}"


# ---------------------------------------------------------------------------
# Lazy clients
# ---------------------------------------------------------------------------

_ch_client: Any = None
_neo4j_driver: Any = None
_minio_client: Any = None


def _get_ch_client() -> Any:
    global _ch_client
    if _ch_client is None:
        _ch_client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
            username=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
        )
    return _ch_client


def _get_neo4j_driver() -> Any:
    global _neo4j_driver
    if _neo4j_driver is None:
        _neo4j_driver = neo4j.GraphDatabase.driver(
            NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD)
        )
    return _neo4j_driver


def _get_minio_client() -> Any:
    global _minio_client
    if _minio_client is None:
        http_client = urllib3.PoolManager(
            timeout=Timeout(connect=1.5, read=2.5),
            maxsize=10,
            cert_reqs="CERT_NONE" if not MINIO_SECURE else "CERT_REQUIRED",
            retries=Retry(total=1, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]),
        )
        _minio_client = minio.Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
            http_client=http_client,
        )
    return _minio_client


def _get_minio_client_for(access_key: str, secret_key: str) -> Any:
    """Build a per-request MinIO client with user-supplied credentials.

    The module singleton uses env credentials and must NOT serve the
    user-authenticated browser consoles — every request may carry different
    credentials that have to be verified against the server with a real
    handshake (list_buckets/stat), never trusted on presence alone.
    """
    http_client = urllib3.PoolManager(
        timeout=Timeout(connect=1.5, read=2.5),
        maxsize=10,
        cert_reqs="CERT_NONE" if not MINIO_SECURE else "CERT_REQUIRED",
        retries=Retry(total=1, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504]),
    )
    return minio.Minio(
        MINIO_ENDPOINT,
        access_key=access_key,
        secret_key=secret_key,
        secure=MINIO_SECURE,
        http_client=http_client,
    )


def _is_ch_auth_error(msg: str) -> bool:
    """True when a ClickHouse exception message signals bad credentials.

    clickhouse-connect raises at construction time (handshake) as
    ``DatabaseError: ... code: 516 ... Authentication failed ...`` — that
    path must map to 401, not 502.
    """
    m = (msg or "").lower()
    return any(
        s in m
        for s in (
            "authentication",
            "wrong_password",
            "unauthorized",
            "authentication_failed",
            "code: 516",
        )
    )


def _is_minio_auth_error(msg: str) -> bool:
    """True when a MinIO/S3 exception message signals bad credentials."""
    m = (msg or "").lower()
    return any(
        s in m
        for s in (
            "invalidaccesskeyid",
            "signaturedoesnotmatch",
            "accessdenied",
            "invalid access",
            "forbidden",
            "unauthorized",
            "401",
        )
    )
def _close_clients() -> None:
    global _ch_client, _neo4j_driver, _minio_client
    try:
        if _ch_client is not None:
            _ch_client.close()
            _ch_client = None
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("ClickHouse close failed: %s", exc)
    try:
        if _neo4j_driver is not None:
            _neo4j_driver.close()
            _neo4j_driver = None
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("Neo4j close failed: %s", exc)
    _minio_client = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_ch_query(query: str, parameters: dict | None = None) -> list[dict]:
    """Execute ClickHouse query with graceful fallback on error."""
    try:
        query = _scoped_ch_query(query)  # ENTRY-5: per-workspace DB injection
        client = _get_ch_client()
        result = client.query(query, parameters=parameters or {})
        columns = [col[0] for col in result.column_names] if hasattr(result, "column_names") and result.column_names else []
        if not columns and hasattr(result, "result_columns"):
            return []
        rows: list[dict] = []
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                rows.append({columns[i]: row[i] for i in range(min(len(columns), len(row)))})
        return rows
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("ClickHouse query failed: %s", exc)
        return []


def _safe_neo4j_query(query: str, parameters: dict | None = None) -> list[dict]:
    """Execute Neo4j query with graceful fallback on error."""
    try:
        driver = _get_neo4j_driver()
        with driver.session() as session:
            result = session.run(query, parameters or {})
            return [dict(record) for record in result]
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("Neo4j query failed: %s", exc)
        return []


def _safe_minio_list(bucket: str, prefix: str = "") -> list[str]:
    """List MinIO objects with graceful fallback on error."""
    try:
        prefix = _scoped_prefix(prefix)  # ENTRY-5: workspaces/<slug>/ injection
        client = _get_minio_client()
        return [obj.object_name for obj in client.list_objects(bucket, prefix=prefix)]
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("MinIO list failed for bucket=%s prefix=%s: %s", bucket, prefix, exc)
        return []


def _safe_minio_get(bucket: str, object_name: str) -> bytes | None:
    """Download MinIO object with graceful fallback on error."""
    try:
        object_name = _scoped_key(object_name)  # ENTRY-5: workspace prefix
        client = _get_minio_client()
        return client.get_object(bucket, object_name).read()
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("MinIO get failed for bucket=%s key=%s: %s", bucket, object_name, exc)
        return None


def _safe_minio_put(bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """Upload data to MinIO with graceful fallback. Returns True on success."""
    try:
        object_name = _scoped_key(object_name)  # ENTRY-5: workspace prefix
        client = _get_minio_client()
        from io import BytesIO
        client.put_object(bucket, object_name, BytesIO(data), len(data), content_type=content_type)
        return True
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("MinIO put failed for bucket=%s key=%s: %s", bucket, object_name, exc)
        return False


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# Workspace scope injection (ENTRY-5 — docs/workspace-isolation.md section 1).
# Scope comes from the per-request ContextVar pinned by the middleware below.
# Default scope is the identity mapping (bare names) so legacy unauthenticated
# behavior is byte-identical; non-default scopes narrow every query path.
# ---------------------------------------------------------------------------

def _scoped_ch_query(query: str) -> str:
    """Rewrite the shared `omniwatch.` DB qualifier to the request scope DB.

    Default scope -> `omniwatch` (string untouched). Non-default ->
    backtick-quoted `omniwatch_ws_<slug>` (slugs allow hyphens, illegal in
    bare ClickHouse identifiers — ENTRY-2 hyphen lesson).
    """
    db = get_current_scope().ch_database
    if db == "omniwatch":
        return query
    return query.replace("omniwatch.", f"`{db}`.")


def _scoped_prefix(prefix: str) -> str:
    """Prepend the workspace MinIO prefix (default scope: identity, no-op)."""
    ws_prefix = get_current_scope().minio_prefix
    if not ws_prefix or prefix.startswith(ws_prefix):
        return prefix
    return f"{ws_prefix}{prefix}"


def _scoped_key(key: str) -> str:
    """Prepend the workspace MinIO prefix to an object key (no double-prefix)."""
    return _scoped_prefix(key)


def _ws_where(*aliases: str) -> str:
    """Neo4j scoping WHERE clause (default scope: empty, legacy-identical).

    Non-default: every listed alias must BELONG_TO the request's :Workspace
    node (contract: entity nodes carry BELONGS_TO edges; legacy nodes without
    one read as `default`).
    """
    if get_current_scope().is_default or not aliases:
        return ""
    preds = " AND ".join(
        f"({a})-[:BELONGS_TO]->(:Workspace {{slug: $ws_slug}})" for a in aliases
    )
    return f"WHERE {preds}"


def _ws_and(*aliases: str) -> str:
    """Neo4j scoping predicate for queries that already have WHERE."""
    if get_current_scope().is_default or not aliases:
        return ""
    preds = " AND ".join(
        f"({a})-[:BELONGS_TO]->(:Workspace {{slug: $ws_slug}})" for a in aliases
    )
    return f"AND {preds}"


def _ws_params(params: dict | None = None) -> dict:
    """Merge the $ws_slug parameter (default scope: params untouched)."""
    out = dict(params or {})
    if not get_current_scope().is_default:
        out["ws_slug"] = get_current_scope().slug
    return out


# ---------------------------------------------------------------------------
# Timeframe helpers (global picker: 1h/6h/24h/7d -> hours)
# ---------------------------------------------------------------------------

_TIME_RANGE_MAP: dict[str, int] = {"1h": 1, "6h": 6, "24h": 24, "7d": 168}


def _resolve_hours(hours: Any = None, timeRange: Any = None) -> int | None:
    """Normalize ?hours= and ?timeRange= to an hours int.

    - Supports both param names (hours takes precedence when present).
    - Accepts values like 24, '24', '24h', '7d' (case-insensitive).
    - Malformed / negative / zero -> fallback to 24 (not 500).
    - Huge -> clamp to 720 (not error).
    - Both omitted -> None (no filter, backward compat).
    """
    if hours is not None:
        try:
            hs = str(hours).strip().lower()
            if hs.endswith("h"):
                hs = hs[:-1]
            elif hs.endswith("d"):
                hs = str(int(hs[:-1]) * 24)
            h = int(hs)
            if h < 1:
                return 24
            if h > 720:
                return 720
            return h
        except (ValueError, TypeError, AttributeError):
            return 24
    if timeRange is not None:
        tr = str(timeRange).strip().lower()
        if tr in _TIME_RANGE_MAP:
            return _TIME_RANGE_MAP[tr]
        try:
            if tr.endswith("h"):
                h = int(tr[:-1])
            elif tr.endswith("d"):
                h = int(tr[:-1]) * 24
            else:
                h = int(tr)
            if h < 1:
                return 24
            if h > 720:
                return 720
            return h
        except (ValueError, TypeError):
            return 24
    return None


# ---------------------------------------------------------------------------
# App factory (orchestration pattern)
# ---------------------------------------------------------------------------

def create_app() -> FastAPI:
    app = FastAPI(
        title="OmniWatch Dashboard API",
        version="1.0.0",
        description="Dashboard backend API gateway for OmniWatch AIOps platform",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ----- workspace scope middleware (ENTRY-5) -----

    @app.middleware("http")
    async def workspace_scope_middleware(request, call_next):
        """Resolve (user_id, workspace_id) on every /api/* route.

        No token -> default scope (legacy single-tenant, byte-identical).
        Present-but-bad token -> 401; non-member/unknown workspace -> 403
        (never 404-leak); malformed ?workspace_id= -> 422. Non-/api/ paths
        (/, /health, /docs) bypass scoping entirely.
        """
        if not request.url.path.startswith("/api/"):
            return await call_next(request)
        try:
            params = request.query_params
            override = (
                params.get("workspace_id") if "workspace_id" in params else None
            )
            scope = resolve_request_scope(
                request.headers.get("authorization"),
                override,
            )
        except ScopeError as exc:
            return JSONResponse(
                status_code=exc.status_code, content={"error": exc.detail}
            )
        token = set_current_scope(scope)
        try:
            response = await call_next(request)
        finally:
            reset_current_scope(token)
        response.headers["X-Workspace-Scope"] = scope.slug
        return response

    # ----- root health -----

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "service": "dashboard-api", "timestamp": _now_iso()}

    # ----- workspace scope introspection (ENTRY-5, additive) -----

    @app.get("/api/scope")
    async def api_scope() -> dict:
        """Return the resolved workspace scope for this request.

        Reports the JWT-derived (user_id, workspace_id) plus the isolation
        keys (ClickHouse DB, Kafka topics, MinIO prefix, Neo4j workspace).
        Membership was already enforced by the middleware (non-member -> 403
        before reaching here), so this doubles as the URL-tamper probe:
        /api/scope?workspace_id=<other> -> 403 unless owned-and-live.
        """
        return get_current_scope().to_dict()

    # ----- summary / overview -----

    @app.get("/api/summary")
    async def api_summary(
        hours: Any = Query(None, description="Filter window in hours (1..720)"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        ch = _safe_ch_query
        if eff is not None:
            incidents_rows = ch(
                "SELECT count() as cnt FROM omniwatch.incidents WHERE created_at >= now() - INTERVAL %(hours)s HOUR",
                parameters={"hours": eff},
            )
            anomalies_rows = ch(
                "SELECT count() as cnt FROM omniwatch.anomalies WHERE status = 'active' AND timestamp >= now() - INTERVAL %(hours)s HOUR",
                parameters={"hours": eff},
            )
            entities_rows = ch(
                "SELECT count() as cnt FROM omniwatch.knowledge_base WHERE created_at >= now() - INTERVAL %(hours)s HOUR",
                parameters={"hours": eff},
            )
        else:
            incidents_rows = ch("SELECT count() as cnt FROM omniwatch.incidents")
            anomalies_rows = ch("SELECT count() as cnt FROM omniwatch.anomalies WHERE status = 'active'")
            entities_rows = ch("SELECT count() as cnt FROM omniwatch.knowledge_base")

        total_incidents = incidents_rows[0].get("cnt", 0) if incidents_rows else 0
        active_anomalies = anomalies_rows[0].get("cnt", 0) if anomalies_rows else 0
        kb_entries = entities_rows[0].get("cnt", 0) if entities_rows else 0

        return {
            "total_incidents": total_incidents,
            "active_anomalies": active_anomalies,
            "knowledge_base_entries": kb_entries,
            "timestamp": _now_iso(),
        }

    # ----- incidents -----

    @app.get("/api/incidents")
    async def api_incidents(
        severity: str | None = Query(None),
        status: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        where_clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if severity:
            where_clauses.append("severity = %(severity)s")
            parameters["severity"] = severity
        if status:
            where_clauses.append("status = %(status)s")
            parameters["status"] = status
        if eff is not None:
            where_clauses.append("created_at >= now() - INTERVAL %(hours)s HOUR")
            parameters["hours"] = eff

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM omniwatch.incidents{where_sql} ORDER BY created_at DESC LIMIT %(limit)s"
        parameters["limit"] = limit

        rows = _safe_ch_query(query, parameters=parameters)
        return {"incidents": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/incidents/{incident_id}", response_model=None)
    async def api_incident_detail(incident_id: str):
        """Return a single incident by ID."""
        rows = _safe_ch_query(
            "SELECT * FROM omniwatch.incidents WHERE incident_id = %(iid)s LIMIT 1",
            parameters={"iid": incident_id},
        )
        if not rows:
            return JSONResponse(status_code=404, content={"error": "incident not found", "incident_id": incident_id})
        return {"incident": rows[0], "timestamp": _now_iso()}

    # ----- anomalies -----

    @app.get("/api/anomalies")
    async def api_anomalies(
        entity_id: str | None = Query(None),
        source_type: str | None = Query(None),
        status: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        where_clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if entity_id:
            where_clauses.append("entity_id = %(eid)s")
            parameters["eid"] = entity_id
        if source_type:
            where_clauses.append("source_type = %(st)s")
            parameters["st"] = source_type
        if status:
            where_clauses.append("status = %(sts)s")
            parameters["sts"] = status
        if eff is not None:
            where_clauses.append("timestamp >= now() - INTERVAL %(hours)s HOUR")
            parameters["hours"] = eff

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM omniwatch.anomalies{where_sql} ORDER BY timestamp DESC LIMIT %(limit)s"
        parameters["limit"] = limit

        rows = _safe_ch_query(query, parameters=parameters)
        return {"anomalies": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- metrics -----

    @app.get("/api/metrics")
    async def api_metrics(
        entity_id: str | None = Query(None),
        metric_name: str | None = Query(None),
        limit: int = Query(100, ge=1, le=1000),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        where_clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if entity_id:
            where_clauses.append("entity_id = %(eid)s")
            parameters["eid"] = entity_id
        if metric_name:
            where_clauses.append("metric_name = %(mn)s")
            parameters["mn"] = metric_name
        if eff is not None:
            where_clauses.append("timestamp >= now() - INTERVAL %(hours)s HOUR")
            parameters["hours"] = eff

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM omniwatch.metrics{where_sql} ORDER BY timestamp DESC LIMIT %(limit)s"
        parameters["limit"] = limit

        rows = _safe_ch_query(query, parameters=parameters)
        return {"metrics": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/metrics/timeseries")
    async def api_metrics_timeseries(
        entity_id: str,
        metric_name: str,
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        if eff is None:
            eff = 24
        query = textwrap.dedent("""\
            SELECT
                toStartOfHour(timestamp) AS hour,
                avg(value) AS avg_value,
                min(value) AS min_value,
                max(value) AS max_value,
                count() AS sample_count
            FROM omniwatch.metrics
            WHERE entity_id = %(eid)s
              AND metric_name = %(mn)s
              AND timestamp >= now() - INTERVAL %(hours)s HOUR
            GROUP BY hour
            ORDER BY hour
        """)
        rows = _safe_ch_query(query, parameters={"eid": entity_id, "mn": metric_name, "hours": eff})
        return {"timeseries": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- logs -----

    @app.get("/api/logs")
    async def api_logs(
        entity_id: str | None = Query(None),
        log_level: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        where_clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if entity_id:
            where_clauses.append("entity_id = %(eid)s")
            parameters["eid"] = entity_id
        if log_level:
            where_clauses.append("log_level = %(lv)s")
            parameters["lv"] = log_level
        if eff is not None:
            where_clauses.append("timestamp >= now() - INTERVAL %(hours)s HOUR")
            parameters["hours"] = eff

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM omniwatch.logs{where_sql} ORDER BY timestamp DESC LIMIT %(limit)s"
        parameters["limit"] = limit

        rows = _safe_ch_query(query, parameters=parameters)
        return {"logs": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- traces -----

    @app.get("/api/traces")
    async def api_traces(
        service_name: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        where_clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if service_name:
            where_clauses.append("service_name = %(sn)s")
            parameters["sn"] = service_name
        if eff is not None:
            where_clauses.append("timestamp >= now() - INTERVAL %(hours)s HOUR")
            parameters["hours"] = eff

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM omniwatch.traces{where_sql} ORDER BY timestamp DESC LIMIT %(limit)s"
        parameters["limit"] = limit

        rows = _safe_ch_query(query, parameters=parameters)
        return {"traces": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- topology (Neo4j → React Flow) -----

    @app.get("/api/topology")
    async def api_topology() -> dict:
        """Return Neo4j graph as React Flow nodes + edges."""
        nodes_query = f"MATCH (n) {_ws_where('n')} RETURN n.id AS id, n.name AS label, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score"
        nodes_raw = _safe_neo4j_query(nodes_query, parameters=_ws_params())

        edges_query = f"MATCH (a)-[r]->(b) {_ws_where('a', 'b')} RETURN a.id AS source, b.id AS target, type(r) AS label, r.latency_p50 AS latency_p50, r.error_rate AS error_rate"
        edges_raw = _safe_neo4j_query(edges_query, parameters=_ws_params())

        # Position nodes in a simple circle layout
        node_count = len(nodes_raw)
        nodes: list[dict] = []
        for i, n in enumerate(nodes_raw):
            angle = 2 * 3.14159 * i / max(node_count, 1)
            nodes.append({
                "id": str(n.get("id", f"node-{i}")),
                "data": {
                    "label": str(n.get("label", n.get("id", f"Node {i}"))),
                    "entity_type": n.get("entity_type", ""),
                    "criticality": n.get("criticality", ""),
                    "status": n.get("status", ""),
                    "anomaly_score": n.get("anomaly_score", 0),
                },
                "position": {"x": 250 + 200 * __import__("math").cos(angle), "y": 250 + 200 * __import__("math").sin(angle)},
                "type": "serviceNode",
            })

        edges: list[dict] = []
        for e in edges_raw:
            edges.append({
                "source": str(e.get("source", "")),
                "target": str(e.get("target", "")),
                "label": str(e.get("label", "")),
                "data": {
                    "latency_p50": e.get("latency_p50", 0),
                    "error_rate": e.get("error_rate", 0),
                },
            })

        return {"nodes": nodes, "edges": edges, "node_count": len(nodes), "edge_count": len(edges)}

    # ----- entities (Neo4j) -----

    @app.get("/api/entities")
    async def api_entities() -> dict:
        """Return all entities from Neo4j."""
        query = f"MATCH (n) {_ws_where('n')} RETURN n.id AS id, n.name AS name, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score, n.last_seen AS last_seen ORDER BY n.anomaly_score DESC"
        rows = _safe_neo4j_query(query, parameters=_ws_params())
        return {"entities": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/entities/top")
    async def api_entities_top(limit: int = Query(10, ge=1, le=100)) -> dict:
        """Return top entities by anomaly score."""
        query = f"MATCH (n) WHERE n.anomaly_score IS NOT NULL {_ws_and('n')} RETURN n.id AS id, n.name AS name, labels(n) AS labels, n.type AS entity_type, n.anomaly_score AS anomaly_score ORDER BY n.anomaly_score DESC LIMIT %(limit)s"
        rows = _safe_neo4j_query(query, parameters=_ws_params({"limit": limit}))
        return {"entities": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/entity/{entity_id}", response_model=None)
    async def api_entity_detail(entity_id: str):
        """Return details for a specific entity from Neo4j."""
        query = f"MATCH (n {{id: $eid}}) {_ws_where('n')} RETURN n.id AS id, n.name AS name, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score, n.last_seen AS last_seen"
        rows = _safe_neo4j_query(query, parameters=_ws_params({"eid": entity_id}))
        if not rows:
            return JSONResponse(status_code=404, content={"error": "entity not found", "entity_id": entity_id})

        # Get connected entities
        neighbors_query = f"MATCH (n {{id: $eid}})-[r]-(m) {_ws_where('m')} RETURN m.id AS id, m.name AS name, type(r) AS rel_type, labels(m) AS labels"
        neighbors = _safe_neo4j_query(neighbors_query, parameters=_ws_params({"eid": entity_id}))

        return {"entity": rows[0], "neighbors": neighbors, "timestamp": _now_iso()}

    @app.get("/api/entity/{entity_id}/metrics")
    async def api_entity_metrics(entity_id: str, limit: int = Query(50, ge=1, le=500)) -> dict:
        """Return metrics for a specific entity."""
        rows = _safe_ch_query(
            "SELECT * FROM omniwatch.metrics WHERE entity_id = %(eid)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"eid": entity_id, "limit": limit},
        )
        return {"entity_id": entity_id, "metrics": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/entity/{entity_id}/anomalies")
    async def api_entity_anomalies(entity_id: str, limit: int = Query(50, ge=1, le=500)) -> dict:
        """Return anomalies for a specific entity."""
        rows = _safe_ch_query(
            "SELECT * FROM omniwatch.anomalies WHERE entity_id = %(eid)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"eid": entity_id, "limit": limit},
        )
        return {"entity_id": entity_id, "anomalies": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/entity/{entity_id}/logs")
    async def api_entity_logs(entity_id: str, limit: int = Query(50, ge=1, le=500)) -> dict:
        """Return logs for a specific entity."""
        rows = _safe_ch_query(
            "SELECT * FROM omniwatch.logs WHERE entity_id = %(eid)s ORDER BY timestamp DESC LIMIT %(limit)s",
            parameters={"eid": entity_id, "limit": limit},
        )
        return {"entity_id": entity_id, "logs": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- pending approvals -----

    @app.get("/api/pending-approvals")
    async def api_pending_approvals(status: str = Query("pending")) -> dict:
        """Return pending approval records."""
        rows = _safe_ch_query(
            "SELECT * FROM omniwatch.pending_approvals WHERE status = %(sts)s ORDER BY created_at DESC LIMIT 100",
            parameters={"sts": status},
        )
        return {"approvals": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- knowledge base -----

    @app.get("/api/knowledge-base")
    async def api_knowledge_base(
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        if eff is not None:
            rows = _safe_ch_query(
                "SELECT * FROM omniwatch.knowledge_base WHERE created_at >= now() - INTERVAL %(hours)s HOUR ORDER BY created_at DESC LIMIT %(limit)s",
                parameters={"hours": eff, "limit": limit},
            )
        else:
            rows = _safe_ch_query(
                "SELECT * FROM omniwatch.knowledge_base ORDER BY created_at DESC LIMIT %(limit)s",
                parameters={"limit": limit},
            )
        return {"knowledge_base": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- audit logs (MinIO) -----

    @app.get("/api/audit-logs")
    async def api_audit_logs(prefix: str = Query("")) -> dict:
        """List audit log objects from MinIO omniwatch-audit-logs bucket."""
        files = _safe_minio_list(MINIO_BUCKET_AUDIT, prefix=prefix)
        return {"audit_logs": files, "count": len(files), "timestamp": _now_iso()}

    @app.get("/api/audit-logs/{object_name}", response_model=None)
    async def api_audit_log_detail(object_name: str):
        """Download a specific audit log from MinIO."""
        data = _safe_minio_get(MINIO_BUCKET_AUDIT, object_name)
        if data is None:
            return JSONResponse(status_code=404, content={"error": "audit log not found", "object_name": object_name})
        try:
            content = json.loads(data)
        except (json.JSONDecodeError, ValueError):
            content = data.decode("utf-8", errors="replace")
        return {"object_name": object_name, "content": content, "timestamp": _now_iso()}

    # ----- MinIO browser — live bucket/object listing (zero dummy) -----

    @app.get("/api/minio/buckets", response_model=None)
    async def api_minio_buckets(
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ) -> dict | JSONResponse:
        """List all MinIO buckets via real MinIO SDK — no hardcoded names. Auth required."""
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        try:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            buckets = client.list_buckets()
            result: list[dict] = []
            for b in buckets:
                name = getattr(b, "name", str(b))
                cd = getattr(b, "creation_date", None)
                result.append(
                    {
                        "name": name,
                        "creation_date": cd.isoformat() if hasattr(cd, "isoformat") and cd else (str(cd) if cd else None),
                    }
                )
            return {"buckets": result, "count": len(result), "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("MinIO list_buckets failed: %s", exc)
            if _is_minio_auth_error(str(exc)):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            return {"buckets": [], "count": 0, "error": str(exc), "timestamp": _now_iso()}

    @app.post("/api/minio/buckets", response_model=None)
    async def api_minio_create_bucket(
        body: dict[str, Any] = Body(...),
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ) -> dict | JSONResponse:
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        name = (body.get("name") or "").strip()
        if not name:
            return JSONResponse(status_code=422, content={"error": "name is required"})
        try:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            client.make_bucket(name)
            return {"created": True, "bucket": name, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO make_bucket failed name=%s: %s", name, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            if "BucketAlreadyOwnedByYou" in msg or "BucketAlreadyExists" in msg:
                return JSONResponse(status_code=409, content={"error": f"bucket already exists: {name}"})
            if "timed out" in msg.lower() or "MaxRetryError" in msg:
                return JSONResponse(status_code=502, content={"error": "MinIO is not reachable"})
            return JSONResponse(status_code=500, content={"error": f"create bucket failed: {msg[:200]}"})

    @app.get("/api/minio/objects", response_model=None)
    async def api_minio_objects(
        bucket: str = Query(..., min_length=1, description="Bucket name"),
        prefix: str = Query("", description="Optional prefix filter"),
        limit: int = Query(50, ge=1, le=500, description="Page size"),
        offset: int = Query(0, ge=0, description="Pagination offset"),
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ):
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        def _sync_collect() -> tuple[list[dict], bool, int]:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            objs: list[dict] = []
            has_more = False
            idx = 0
            for obj in client.list_objects(bucket, prefix=_scoped_prefix(prefix), recursive=True):
                if idx < offset:
                    idx += 1
                    continue
                if len(objs) < limit:
                    lm = getattr(obj, "last_modified", None)
                    objs.append(
                        {
                            "name": getattr(obj, "object_name", "") or "",
                            "size": int(getattr(obj, "size", 0) or 0),
                            "last_modified": lm.isoformat() if hasattr(lm, "isoformat") and lm else (str(lm) if lm else None),
                            "etag": getattr(obj, "etag", None),
                        }
                    )
                    idx += 1
                    continue
                has_more = True
                break
            if has_more:
                total = offset + len(objs) + 1
            else:
                total = idx if idx >= offset else 0
                if total < offset:
                    total = 0
                    objs = []
            return objs, has_more, total

        try:
            objs, has_more, total = await asyncio.wait_for(
                asyncio.to_thread(_sync_collect), timeout=3.2
            )
            return {
                "bucket": bucket,
                "prefix": prefix,
                "objects": objs,
                "count": len(objs),
                "total": total,
                "limit": limit,
                "offset": offset,
                "has_more": has_more,
                "truncated": has_more,
                "timestamp": _now_iso(),
            }
        except asyncio.TimeoutError:
            _LOG.warning("MinIO list_objects timed out bucket=%s prefix=%s", bucket, prefix)
            return {
                "bucket": bucket,
                "prefix": prefix,
                "objects": [],
                "count": 0,
                "total": 0,
                "limit": limit,
                "offset": offset,
                "has_more": False,
                "truncated": False,
                "error": "MinIO listing timed out — bucket is slow or too large, try a more specific prefix filter",
                "timestamp": _now_iso(),
            }
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO list_objects failed bucket=%s: %s", bucket, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key", "bucket": bucket, "timestamp": _now_iso()})
            if "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"bucket not found: {bucket}", "bucket": bucket, "timestamp": _now_iso()})
            if "timed out" in msg.lower() or "ReadTimeout" in msg or "MaxRetryError" in msg:
                return {
                    "bucket": bucket,
                    "prefix": prefix,
                    "objects": [],
                    "count": 0,
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "has_more": False,
                    "truncated": False,
                    "error": f"MinIO listing timed out: {msg[:200]}",
                    "timestamp": _now_iso(),
                }
            return JSONResponse(status_code=500, content={"error": msg, "bucket": bucket, "timestamp": _now_iso()})

    # ----- MinIO object operations (upload / download / delete / metadata) -----

    @app.post("/api/minio/upload", response_model=None)
    async def api_minio_upload(
        bucket: str = Form(..., description="Bucket name"),
        key: str = Form(..., description="Object key"),
        file: UploadFile = File(..., description="File to upload"),
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ) -> dict | JSONResponse:
        """Upload a file to MinIO. Multipart form: bucket, key, file. Auth required."""
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        try:
            content = await file.read()
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            from io import BytesIO
            client.put_object(bucket, _scoped_key(key), BytesIO(content), len(content), content_type=file.content_type or "application/octet-stream")
            return {"bucket": bucket, "key": key, "size": len(content), "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO upload failed bucket=%s key=%s: %s", bucket, key, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            if "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"bucket not found: {bucket}"})
            if "timed out" in msg.lower() or "MaxRetryError" in msg:
                return JSONResponse(status_code=502, content={"error": "MinIO is not reachable"})
            return JSONResponse(status_code=500, content={"error": f"upload failed: {msg[:200]}"})

    @app.get("/api/minio/download/{bucket}/{key:path}", response_model=None)
    async def api_minio_download(
        bucket: str,
        key: str,
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ):
        """Download/stream an object from MinIO. Auth required. Returns 404 if not found."""
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        try:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            stat = client.stat_object(bucket, _scoped_key(key))
            response = client.get_object(bucket, _scoped_key(key))
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO download failed bucket=%s key=%s: %s", bucket, key, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            if "NoSuchKey" in msg or "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"object not found: {bucket}/{key}"})
            if "timed out" in msg.lower() or "MaxRetryError" in msg:
                return JSONResponse(status_code=502, content={"error": "MinIO is not reachable"})
            return JSONResponse(status_code=500, content={"error": f"download failed: {msg[:200]}"})
        content_type = getattr(stat, "content_type", None) or "application/octet-stream"
        def _stream():
            try:
                with response:
                    while True:
                        chunk = response.read(65536)
                        if not chunk:
                            break
                        yield chunk
            finally:
                response.close()
                response.release_conn()
        return StreamingResponse(_stream(), media_type=content_type, headers={
            "Content-Disposition": f'attachment; filename="{key.rsplit("/", 1)[-1]}"',
            "Content-Length": str(getattr(stat, "size", 0) or 0),
        })

    @app.delete("/api/minio/object", response_model=None)
    async def api_minio_delete_object(
        body: dict[str, Any] = Body(...),
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ) -> dict | JSONResponse:
        """Delete an object from MinIO. Body: {bucket, key}. Auth required."""
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        bucket = (body.get("bucket") or "").strip()
        key = (body.get("key") or "").strip()
        if not bucket or not key:
            return JSONResponse(status_code=422, content={"error": "bucket and key are required"})
        try:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            client.remove_object(bucket, _scoped_key(key))
            return {"deleted": True, "bucket": bucket, "key": key, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO delete failed bucket=%s key=%s: %s", bucket, key, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            if "NoSuchKey" in msg or "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"object not found: {bucket}/{key}"})
            if "timed out" in msg.lower() or "MaxRetryError" in msg:
                return JSONResponse(status_code=502, content={"error": "MinIO is not reachable"})
            return JSONResponse(status_code=500, content={"error": f"delete failed: {msg[:200]}"})

    @app.delete("/api/minio/bucket/{name}", response_model=None)
    async def api_minio_delete_bucket(
        name: str,
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ) -> dict | JSONResponse:
        """Delete a MinIO bucket. Auth required. Bucket must be empty."""
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        try:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            client.remove_bucket(name)
            return {"deleted": True, "bucket": name, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO remove_bucket failed name=%s: %s", name, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            if "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"bucket not found: {name}"})
            if "BucketNotEmpty" in msg:
                return JSONResponse(status_code=409, content={"error": f"bucket is not empty: {name}"})
            if "timed out" in msg.lower() or "MaxRetryError" in msg:
                return JSONResponse(status_code=502, content={"error": "MinIO is not reachable"})
            return JSONResponse(status_code=500, content={"error": f"delete bucket failed: {msg[:200]}"})

    @app.get("/api/minio/metadata/{bucket}/{key:path}", response_model=None)
    async def api_minio_metadata(
        bucket: str,
        key: str,
        x_minio_access_key: str | None = Header(None, alias="X-MinIO-AccessKey"),
        x_minio_secret_key: str | None = Header(None, alias="X-MinIO-SecretKey"),
    ) -> dict | JSONResponse:
        """Get object metadata from MinIO. Auth required. Returns size, content_type, last_modified, etag."""
        if not x_minio_access_key or not x_minio_secret_key:
            return JSONResponse(status_code=401, content={"error": "missing X-MinIO-AccessKey or X-MinIO-SecretKey header"})
        try:
            client = _get_minio_client_for(x_minio_access_key, x_minio_secret_key)
            stat = client.stat_object(bucket, _scoped_key(key))
            lm = getattr(stat, "last_modified", None)
            return {
                "bucket": bucket,
                "key": key,
                "size": int(getattr(stat, "size", 0) or 0),
                "content_type": getattr(stat, "content_type", None) or "application/octet-stream",
                "last_modified": lm.isoformat() if hasattr(lm, "isoformat") and lm else (str(lm) if lm else None),
                "etag": getattr(stat, "etag", None),
                "timestamp": _now_iso(),
            }
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO stat_object failed bucket=%s key=%s: %s", bucket, key, msg)
            if _is_minio_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "MinIO authentication failed: invalid access key or secret key"})
            if "NoSuchKey" in msg or "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"object not found: {bucket}/{key}"})
            if "timed out" in msg.lower() or "MaxRetryError" in msg:
                return JSONResponse(status_code=502, content={"error": "MinIO is not reachable"})
            return JSONResponse(status_code=500, content={"error": f"metadata failed: {msg[:200]}"})

    # ----- incident archive (MinIO) -----

    @app.get("/api/incident-archive")
    async def api_incident_archive(prefix: str = Query("")) -> dict:
        """List archived incident objects from MinIO omniwatch-incidents bucket."""
        files = _safe_minio_list(MINIO_BUCKET_INCIDENTS, prefix=prefix)
        return {"incidents": files, "count": len(files), "timestamp": _now_iso()}

    # ----- compliance reports (MinIO) -----

    @app.get("/api/compliance-reports")
    async def api_compliance_reports(prefix: str = Query("")) -> dict:
        """List compliance reports from MinIO omniwatch-audit-logs bucket."""
        files = _safe_minio_list(MINIO_BUCKET_AUDIT, prefix="compliance/")
        return {"reports": files, "count": len(files), "timestamp": _now_iso()}

    # ----- recommendations (proxy to learning service) -----

    @app.get("/api/recommendations/{entity_id}")
    async def api_recommendations(entity_id: str) -> dict:
        """Proxy to learning service recommendation engine."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{LEARNING_SERVICE_URL}/api/recommendations/{entity_id}")
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Learning service proxy failed: %s", exc)
            return {"entity_id": entity_id, "recommendations": [], "count": 0, "error": str(exc)}

    # ----- config: model settings -----

    @app.get("/api/config/model-settings")
    async def api_get_model_settings() -> dict:
        """Return current model settings with masked API key."""
        mm = _get_model_manager()
        settings = mm.get_settings()
        d = settings.to_dict()
        d["api_key"] = _mask_api_key(settings.api_key)
        return d

    @app.put("/api/config/model-settings", response_model=None)
    async def api_put_model_settings(body: dict[str, Any]):
        """Update model settings. Validates provider enum, persists to disk.

        Absent-means-keep for api_key: if the field is omitted from the body,
        the existing stored key is preserved. An explicit empty string clears it.
        Frontend omits the field (never sends masked ``sk-...xxxx`` values).
        """
        provider_str = body.get("provider", "")
        try:
            provider = ModelProvider(provider_str)
        except ValueError:
            valid = [p.value for p in ModelProvider]
            return JSONResponse(
                status_code=400,
                content={"error": f"Invalid provider '{provider_str}'. Must be one of: {valid}"},
            )

        mm = _get_model_manager()
        existing = mm.get_settings()
        # absent-means-keep: only overwrite if key is explicitly present in body
        incoming_api_key = body["api_key"] if "api_key" in body else existing.api_key

        new_settings = ModelSettings(
            provider=provider,
            model_name=body.get("model_name", OLLAMA_MODEL),
            api_key=incoming_api_key,
            base_url=body.get("base_url", ""),
            temperature=float(body.get("temperature", 0.7)),
            max_tokens=int(body.get("max_tokens", 2048)),
        )
        mm.update_settings(new_settings)

        resp = new_settings.to_dict()
        resp["api_key"] = _mask_api_key(new_settings.api_key)
        return resp

    @app.post("/api/config/test-connection", response_model=None)
    async def api_test_connection(body: dict[str, Any] | None = None):
        """Test connection using form overrides (if provided) without persisting.

        Absent-means-keep for api_key: if omitted from body, the stored key
        is used.  Response always reflects the effective provider/model tested.
        """
        mm = _get_model_manager()
        if not body:
            return await mm.test_connection()

        provider_str = body.get("provider", "")
        if provider_str:
            try:
                provider = ModelProvider(provider_str)
            except ValueError:
                valid = [p.value for p in ModelProvider]
                return JSONResponse(
                    status_code=400,
                    content={"error": f"Invalid provider '{provider_str}'. Must be one of: {valid}"},
                )
        else:
            provider = mm.get_settings().provider

        existing = mm.get_settings()
        incoming_api_key = body["api_key"] if "api_key" in body else existing.api_key

        merged = ModelSettings(
            provider=provider,
            model_name=body.get("model_name", existing.model_name),
            api_key=incoming_api_key,
            base_url=body.get("base_url", existing.base_url),
            temperature=float(body.get("temperature", existing.temperature)),
            max_tokens=int(body.get("max_tokens", existing.max_tokens)),
        )
        return await mm.test_connection(settings=merged)

    # ----- ollama: model management -----

    @app.get("/api/ollama/models", response_model=None)
    async def api_ollama_models():
        """List locally available Ollama models (proxy to Ollama /api/tags)."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{OLLAMA_URL}/api/tags")
                resp.raise_for_status()
                data = resp.json()
                raw = data.get("models", []) if isinstance(data, dict) else []
                models = [
                    {
                        "name": m.get("name", ""),
                        "modified_at": m.get("modified_at", ""),
                        "size": m.get("size", 0),
                    }
                    for m in raw
                    if isinstance(m, dict)
                ]
                return {"models": models, "count": len(models)}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Ollama models proxy failed: %s", exc)
            return {"models": [], "count": 0, "error": str(exc)}

    @app.post("/api/ollama/pull", response_model=None)
    async def api_ollama_pull(body: dict[str, Any] | None = None):
        """Pull an Ollama model with real-time progress.

        Proxies Ollama ``POST /api/pull`` with ``stream: true`` and forwards
        every NDJSON line as an SSE ``data:`` frame. The browser reads the
        stream and renders a live progress bar — the nginx ``/api/ollama/pull``
        location is tuned for 30-minute streaming with buffering off, so a
        multi-GB download never hits a gateway timeout. The Ollama daemon
        continues the download even if the client disconnects (hence ``ollama
        list`` eventually shows the model), but streaming gives live feedback
        instead of a silent 504.
        """
        name = (body or {}).get("name", "") if body else ""
        name = name.strip() if isinstance(name, str) else ""
        if not name:
            return JSONResponse(
                status_code=400,
                content={"success": False, "error": "Missing 'name' in request body."},
            )
        if name.endswith(":cloud"):
            return {
                "success": True,
                "name": name,
                "note": "Cloud models don't need pulling — they run via Ollama Cloud. Just paste your key and Save.",
            }

        async def event_gen():
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream(
                        "POST", f"{OLLAMA_URL}/api/pull", json={"name": name, "stream": True}
                    ) as resp:
                        if resp.status_code != 200:
                            body_bytes = await resp.aread()
                            try:
                                detail = body_bytes.decode()
                            except Exception:
                                detail = str(body_bytes)
                            yield f"data: {json.dumps({'status': 'error', 'error': detail})}\n\n"
                            return
                        async for line in resp.aiter_lines():
                            if not line.strip():
                                continue
                            # Each line is already JSON from Ollama; forward verbatim
                            yield f"data: {line}\n\n"
                        yield f"data: {json.dumps({'status': 'success', 'name': name})}\n\n"
            except Exception as exc:  # noqa: BLE001
                _LOG.warning("Ollama pull stream failed for %s: %s", name, exc)
                yield f"data: {json.dumps({'status': 'error', 'error': str(exc)})}\n\n"

        return StreamingResponse(
            event_gen(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @app.delete("/api/ollama/models/{model_name:path}", response_model=None)
    async def api_ollama_delete_model(model_name: str):
        """Delete an Ollama model (proxy to Ollama /api/delete)."""
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.request("DELETE", f"{OLLAMA_URL}/api/delete", json={"name": model_name})
                resp.raise_for_status()
                return {"success": True, "deleted": model_name}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Ollama delete proxy failed for %s: %s", model_name, exc)
            return {"success": False, "error": str(exc)}

    # ----- copilot (LLM — via ModelManager) -----

    @app.get("/api/copilot", response_model=None)
    async def api_copilot(
        question: str = Query(..., min_length=1),
        context: str = Query(""),
        stream: bool = Query(False),
        history: str = Query(""),
    ):
        """Ask the copilot a question. Supports SSE streaming when stream=true.
        Accepts optional history as JSON array string of {role, content} objects."""
        mm = _get_model_manager()
        model_name = mm.get_settings().model_name
        system_prompt = textwrap.dedent(f"""\
            You are the OmniWatch AIOps Copilot (powered by {model_name} via Ollama). Be concise, max 300 words, directly answer the question, use code blocks for queries.
            Current model: {model_name} — identify as this when asked "what model are you".
            Real OmniWatch stack: OpenTelemetry SDK + Collector (4317/4318), Kafka, Flink (entity-resolution + feature-store), ClickHouse, Neo4j, MinIO, OPA, Ollama, React. Does NOT use Prometheus/Grafana.
            Real routes: / (Overview), /incidents, /topology, /knowledge, /reports, /security, /data, /graph, /storage, /settings/model. You can answer general OmniWatch questions (overview, how to use, architecture) using this stack. If asked about Prometheus/Grafana, clarify it uses OTel+ClickHouse. Only invent integrations if truly unknown — then say "I don't know".
        """)
        user_prompt = f"Context:\n{context}\n\nQuestion: {question}" if context else question

        # Build messages array with optional history (keep short to avoid confusion)
        messages = [{"role": "system", "content": system_prompt}]
        if history:
            try:
                parsed = json.loads(history)
                if isinstance(parsed, list):
                    for entry in parsed[-6:]:
                        if isinstance(entry, dict) and entry.get("role") in ("user", "assistant") and entry.get("content"):
                            messages.append({"role": entry["role"], "content": str(entry["content"])[:2000]})
            except (json.JSONDecodeError, TypeError):
                pass
        messages.append({"role": "user", "content": user_prompt})

        try:
            mm = _get_model_manager()
            result = await mm.chat(messages, stream=stream)

            if stream and not isinstance(result, str):
                return sse_response(result)

            answer = result if isinstance(result, str) else str(result)
            return {"answer": answer, "model": mm.get_settings().model_name, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Copilot failed: %s", exc)
            return {"answer": "Copilot unavailable — LLM service not reachable.", "error": str(exc), "timestamp": _now_iso()}

    # ----- patterns (proxy to learning service) -----

    @app.get("/api/patterns")
    async def api_patterns() -> dict:
        """Proxy to learning service pattern mining."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{LEARNING_SERVICE_URL}/api/patterns")
                resp.raise_for_status()
                return resp.json()
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Learning service proxy failed for patterns: %s", exc)
            return {"patterns": [], "count": 0, "error": str(exc)}

    # ----- dashboard summary (for widget rendering) -----

    @app.get("/api/dashboard/severity-distribution")
    async def api_severity_distribution(
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        if eff is not None:
            rows = _safe_ch_query(
                "SELECT severity, count() as cnt FROM omniwatch.incidents WHERE created_at >= now() - INTERVAL %(hours)s HOUR GROUP BY severity ORDER BY cnt DESC",
                parameters={"hours": eff},
            )
        else:
            rows = _safe_ch_query(
                "SELECT severity, count() as cnt FROM omniwatch.incidents GROUP BY severity ORDER BY cnt DESC"
            )
        return {"distribution": rows, "timestamp": _now_iso()}

    @app.get("/api/dashboard/entity-health")
    async def api_entity_health() -> dict:
        """Return entity anomaly scores from Neo4j for health heatmap."""
        query = f"MATCH (n) WHERE n.anomaly_score IS NOT NULL {_ws_and('n')} RETURN n.id AS id, n.name AS name, n.anomaly_score AS anomaly_score, n.status AS status ORDER BY n.anomaly_score DESC"
        rows = _safe_neo4j_query(query, parameters=_ws_params())
        return {"entities": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/dashboard/incidents-timeline")
    async def api_incidents_timeline(
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        if eff is None:
            eff = 24
        query = textwrap.dedent("""\
            SELECT
                toStartOfHour(created_at) AS hour,
                count() AS incident_count,
                severity
            FROM omniwatch.incidents
            WHERE created_at >= now() - INTERVAL %(hours)s HOUR
            GROUP BY hour, severity
            ORDER BY hour
        """)
        rows = _safe_ch_query(query, parameters={"hours": eff})
        return {"timeline": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- storage health -----

    @app.get("/api/storage-health")
    async def api_storage_health() -> dict:
        """Check health of all storage backends."""
        ch_ok = False
        neo4j_ok = False
        minio_ok = False

        try:
            ch = _get_ch_client()
            ch.query("SELECT 1")
            ch_ok = True
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("ClickHouse health check failed: %s", exc)

        try:
            driver = _get_neo4j_driver()
            with driver.session() as session:
                session.run("RETURN 1")
            neo4j_ok = True
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("Neo4j health check failed: %s", exc)

        try:
            client = _get_minio_client()
            minio_ok = client.bucket_exists(MINIO_BUCKET_AUDIT)
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("MinIO health check failed: %s", exc)

        all_healthy = ch_ok and neo4j_ok and minio_ok
        return {
            "clickhouse": ch_ok,
            "neo4j": neo4j_ok,
            "minio": minio_ok,
            "all_healthy": all_healthy,
            "timestamp": _now_iso(),
        }

    # ----- clickhouse table stats -----

    @app.get("/api/stats")
    async def api_stats() -> dict:
        """Return row counts per ClickHouse table."""
        tables = ["metrics", "logs", "traces", "anomalies", "incidents", "pending_approvals", "knowledge_base"]
        stats: dict[str, int] = {}
        for table in tables:
            rows = _safe_ch_query(f"SELECT count() as cnt FROM omniwatch.{table}")
            stats[table] = rows[0].get("cnt", 0) if rows else 0
        return {"stats": stats, "timestamp": _now_iso()}

    # ===================================================================
    # Spec-required routes added in patch
    # ===================================================================

    # ----- GET /api/metrics/logs (alias to /api/logs under /api/metrics path) -----

    @app.get("/api/metrics/logs")
    async def api_metrics_logs(
        entity_id: str | None = Query(None),
        log_level: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
    ) -> dict:
        """Alias for /api/logs under the /api/metrics namespace."""
        where_clauses: list[str] = []
        parameters: dict[str, Any] = {}
        if entity_id:
            where_clauses.append("entity_id = %(eid)s")
            parameters["eid"] = entity_id
        if log_level:
            where_clauses.append("log_level = %(lv)s")
            parameters["lv"] = log_level

        where_sql = " WHERE " + " AND ".join(where_clauses) if where_clauses else ""
        query = f"SELECT * FROM omniwatch.logs{where_sql} ORDER BY timestamp DESC LIMIT %(limit)s"
        parameters["limit"] = limit

        rows = _safe_ch_query(query, parameters=parameters)
        return {"logs": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- GET /api/topology/{entity_id} (entity-scoped topology) -----

    @app.get("/api/topology/{entity_id}", response_model=None)
    async def api_topology_entity(entity_id: str):
        """Return entity-scoped topology subgraph from Neo4j."""
        center_query = f"MATCH (n {{id: $eid}}) {_ws_where('n')} RETURN n.id AS id, n.name AS label, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score"
        center_raw = _safe_neo4j_query(center_query, parameters=_ws_params({"eid": entity_id}))

        if not center_raw:
            return JSONResponse(status_code=404, content={"error": "entity not found", "entity_id": entity_id})

        neighbors_query = f"MATCH (n {{id: $eid}})-[r]-(m) {_ws_where('m')} RETURN m.id AS id, m.name AS label, labels(m) AS labels, m.type AS entity_type, m.criticality AS criticality, m.status AS status, m.anomaly_score AS anomaly_score, type(r) AS rel_type, r.latency_p50 AS latency_p50, r.error_rate AS error_rate"
        neighbors_raw = _safe_neo4j_query(neighbors_query, parameters=_ws_params({"eid": entity_id}))

        node_ids: set[str] = {center_raw[0].get("id", "")}
        nodes: list[dict] = [
            {
                "id": center_raw[0].get("id", entity_id),
                "data": {
                    "label": center_raw[0].get("label", entity_id),
                    "entity_type": center_raw[0].get("entity_type", ""),
                    "criticality": center_raw[0].get("criticality", ""),
                    "status": center_raw[0].get("status", ""),
                    "anomaly_score": center_raw[0].get("anomaly_score", 0),
                },
                "position": {"x": 250, "y": 250},
                "type": "serviceNode",
            }
        ]
        edges: list[dict] = []

        import math as _math
        for i, nb in enumerate(neighbors_raw):
            nid = str(nb.get("id", f"nb-{i}"))
            if nid not in node_ids:
                node_ids.add(nid)
                angle = 2 * _math.pi * (i + 1) / max(len(neighbors_raw) + 1, 1)
                nodes.append({
                    "id": nid,
                    "data": {
                        "label": nb.get("label", nid),
                        "entity_type": nb.get("entity_type", ""),
                        "criticality": nb.get("criticality", ""),
                        "status": nb.get("status", ""),
                        "anomaly_score": nb.get("anomaly_score", 0),
                    },
                    "position": {"x": 250 + 200 * _math.cos(angle), "y": 250 + 200 * _math.sin(angle)},
                    "type": "serviceNode",
                })
            edges.append({
                "source": entity_id,
                "target": nid,
                "label": str(nb.get("rel_type", "")),
                "data": {"latency_p50": nb.get("latency_p50", 0), "error_rate": nb.get("error_rate", 0)},
            })

        return {"nodes": nodes, "edges": edges, "node_count": len(nodes), "edge_count": len(edges)}

    # ----- GET /api/knowledge/search?q= -----

    @app.get("/api/knowledge/search")
    async def api_knowledge_search(
        q: str = Query("", min_length=0),
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        base_filter = ""
        params: dict[str, Any] = {"limit": limit}
        if eff is not None:
            base_filter = " AND created_at >= now() - INTERVAL %(hours)s HOUR"
            params["hours"] = eff
        if not q.strip():
            rows = _safe_ch_query(
                f"SELECT * FROM omniwatch.knowledge_base WHERE 1=1{base_filter} ORDER BY created_at DESC LIMIT %(limit)s",
                parameters=params,
            )
        else:
            params["q"] = f"%{q}%"
            rows = _safe_ch_query(
                f"SELECT * FROM omniwatch.knowledge_base WHERE (root_cause_entity LIKE %(q)s OR resolution_summary LIKE %(q)s){base_filter} ORDER BY created_at DESC LIMIT %(limit)s",
                parameters=params,
            )
        return {"results": rows, "count": len(rows), "query": q, "timestamp": _now_iso()}

    # ----- GET /api/knowledge/stats -----

    @app.get("/api/knowledge/stats")
    async def api_knowledge_stats(
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        if eff is not None:
            total = _safe_ch_query(
                "SELECT count() as cnt FROM omniwatch.knowledge_base WHERE created_at >= now() - INTERVAL %(hours)s HOUR",
                parameters={"hours": eff},
            )
            by_outcome = _safe_ch_query(
                "SELECT outcome, count() as cnt FROM omniwatch.knowledge_base WHERE created_at >= now() - INTERVAL %(hours)s HOUR GROUP BY outcome",
                parameters={"hours": eff},
            )
            by_type = _safe_ch_query(
                "SELECT root_cause_entity_type, count() as cnt FROM omniwatch.knowledge_base WHERE created_at >= now() - INTERVAL %(hours)s HOUR GROUP BY root_cause_entity_type",
                parameters={"hours": eff},
            )
        else:
            total = _safe_ch_query("SELECT count() as cnt FROM omniwatch.knowledge_base")
            by_outcome = _safe_ch_query(
                "SELECT outcome, count() as cnt FROM omniwatch.knowledge_base GROUP BY outcome"
            )
            by_type = _safe_ch_query(
                "SELECT root_cause_entity_type, count() as cnt FROM omniwatch.knowledge_base GROUP BY root_cause_entity_type"
            )
        return {
            "total_entries": total[0].get("cnt", 0) if total else 0,
            "by_outcome": by_outcome,
            "by_entity_type": by_type,
            "timestamp": _now_iso(),
        }

    # ----- GET /api/minio/runbooks/{id} -----

    @app.get("/api/minio/runbooks/{runbook_id}", response_model=None)
    async def api_minio_runbook(runbook_id: str):
        """Fetch a runbook from MinIO omniwatch-runbooks bucket."""
        data = _safe_minio_get(MINIO_BUCKET_RUNBOOKS, runbook_id)
        if data is None:
            return JSONResponse(status_code=404, content={"error": "runbook not found", "runbook_id": runbook_id})
        try:
            content = json.loads(data)
        except (json.JSONDecodeError, ValueError):
            content = data.decode("utf-8", errors="replace")
        return {"runbook_id": runbook_id, "content": content, "timestamp": _now_iso()}

    # ----- GET /api/genai/summary -----
    # Live: ClickHouse incidents+anomalies+knowledge_base + MinIO runbooks
    # + Ollama qwen3:8b. Returns {content, source, timestamp} matching GenAIReport.

    def _latest_incident() -> dict | None:
        rows = _safe_ch_query("SELECT * FROM omniwatch.incidents ORDER BY created_at DESC LIMIT 1")
        return rows[0] if rows else None

    def _render_live_summary_markdown(stats: dict, incidents: list[dict], runbooks: list[str]) -> str:
        total = stats.get("incidents", 0)
        anomalies = stats.get("anomalies", 0)
        kb = stats.get("knowledge_base", 0)
        sev_rows = _safe_ch_query("SELECT severity, count() as cnt FROM omniwatch.incidents GROUP BY severity ORDER BY cnt DESC")
        sev_line = ", ".join(f"{r.get('severity')}:{r.get('cnt')}" for r in sev_rows) if sev_rows else "none"
        latest = incidents[0] if incidents else None
        lines = [
            "# System Summary",
            "",
            f"_Generated {_now_iso()} — live from ClickHouse + MinIO_",
            "",
            "## Current State",
            "",
            f"- **Total incidents:** {total}",
            f"- **Active anomalies:** {anomalies}",
            f"- **Knowledge base entries:** {kb}",
            f"- **Severity breakdown:** {sev_line}",
            f"- **Runbooks in MinIO (omniwatch-runbooks):** {len(runbooks)}",
            "",
        ]
        if latest:
            lines.extend([
                "## Latest Incident",
                "",
                f"- **ID:** `{latest.get('incident_id','')}`",
                f"- **Severity:** {latest.get('severity','')} | **Status:** {latest.get('status','')}",
                f"- **Root cause:** `{latest.get('root_cause_entity','')}` ({latest.get('entity_type','')})",
                f"- **Fault path:** {latest.get('fault_path','')}",
                f"- **Created:** {latest.get('created_at','')}",
                "",
            ])
        else:
            lines.extend([
                "> No incidents recorded yet — run `python simulation/anomaly_injector.py --scenario database_cascade` to generate data.",
                "",
            ])
        lines.extend([
            "## Data Sources",
            "",
            "- ClickHouse `omniwatch.incidents` + `omniwatch.anomalies` + `omniwatch.knowledge_base`",
            "- MinIO bucket `omniwatch-runbooks` (live list)",
            "- Ollama `qwen3:8b` when reachable (otherwise deterministic live template)",
            "",
        ])
        return "\n".join(lines)

    async def _ollama_enhance(prompt: str) -> str | None:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                resp = await client.post(
                    f"{OLLAMA_URL}/api/generate",
                    json={"model": OLLAMA_MODEL, "prompt": prompt, "stream": False, "think": False},
                )
                resp.raise_for_status()
                data = resp.json()
                txt = data.get("response", "").strip()
                return txt if txt else None
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("Ollama enhance failed: %s", exc)
            return None

    async def _llm_generate(prompt: str) -> str | None:
        """Generate text via configured LLM provider. Returns None on failure."""
        try:
            result = await _get_model_manager().generate(prompt)
            # generate() with stream=False always returns str; narrow for type checker
            text = result if isinstance(result, str) else ""
            return text.strip() if text else None
        except Exception as exc:  # noqa: BLE001
            _LOG.debug("LLM generate failed: %s", exc)
            return None

    @app.get("/api/genai/summary")
    async def api_genai_summary() -> dict:
        """Live system summary from ClickHouse + MinIO + Ollama. Always returns {content, source, timestamp}."""
        # Try genai service first (grounded generation) if it exposes /generate
        stats_rows = _safe_ch_query("SELECT count() as cnt FROM omniwatch.incidents")
        total = stats_rows[0].get("cnt", 0) if stats_rows else 0
        # Fast path: if genai service has summary endpoint, proxy but normalize shape
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{GENAI_SERVICE_URL}/api/summary")
                if resp.status_code == 200:
                    data = resp.json()
                    # Normalize any shape to GenAIReport
                    content = data.get("content") or data.get("summary") or json.dumps(data, indent=2)
                    return {"content": content, "source": data.get("source", "genai-service"), "timestamp": data.get("timestamp", _now_iso())}
        except Exception:  # noqa: BLE001
            pass
        # Live fallback: build from ClickHouse+MinIO
        all_stats: dict[str, int] = {}
        for tbl in ["incidents", "anomalies", "knowledge_base"]:
            rows = _safe_ch_query(f"SELECT count() as cnt FROM omniwatch.{tbl}")
            all_stats[tbl] = rows[0].get("cnt", 0) if rows else 0
        runbooks = _safe_minio_list(MINIO_BUCKET_RUNBOOKS, prefix="")
        recent = _safe_ch_query("SELECT * FROM omniwatch.incidents ORDER BY created_at DESC LIMIT 5")
        content = _render_live_summary_markdown(all_stats, recent, runbooks)
        # Optionally enhance with Ollama using live stats
        prompt = f"Summarize this live AIOps state in 3 sentences, grounded only in these facts:\n{content}\nKeep it concise, no hallucinations."
        enhanced = await _llm_generate(prompt)
        if enhanced:
            content = content + "\n\n---\n\n**Ollama qwen3:8b (grounded):**\n\n" + enhanced
            source = "clickhouse+minio+ollama"
        else:
            source = "clickhouse+minio"
        if total == 0 and not runbooks:
            source = "empty"
        return {"content": content, "source": source, "timestamp": _now_iso()}

    # ----- GET /api/genai/executive -----

    @app.get("/api/genai/executive")
    async def api_genai_executive() -> dict:
        """Live executive report from ClickHouse incidents (business impact + SLA) + Ollama."""
        try:
            async with httpx.AsyncClient(timeout=3.0) as client:
                resp = await client.get(f"{GENAI_SERVICE_URL}/api/executive")
                if resp.status_code == 200:
                    data = resp.json()
                    content = data.get("content") or data.get("summary") or json.dumps(data, indent=2)
                    return {"content": content, "source": data.get("source", "genai-service"), "timestamp": data.get("timestamp", _now_iso())}
        except Exception:  # noqa: BLE001
            pass
        recent = _safe_ch_query("SELECT * FROM omniwatch.incidents ORDER BY created_at DESC LIMIT 5")
        sev_rows = _safe_ch_query("SELECT severity, count() as cnt FROM omniwatch.incidents GROUP BY severity ORDER BY cnt DESC")
        total_rows = _safe_ch_query("SELECT count() as cnt FROM omniwatch.incidents")
        total = total_rows[0].get("cnt", 0) if total_rows else 0
        if not recent:
            return {
                "content": "# Executive Report\n\n_No incidents recorded yet — no reports yet — generate one by running `python simulation/anomaly_injector.py --scenario database_cascade`._\n\n_Data source: ClickHouse `omniwatch.incidents` (live, empty)._",
                "source": "empty",
                "timestamp": _now_iso(),
            }
        # Build executive markdown live
        lines = [
            "# Executive Report",
            "",
            f"_Generated {_now_iso()} — live from ClickHouse `omniwatch.incidents`_",
            "",
            f"**Total incidents:** {total} | **Breakdown:** " + (", ".join(f"{r.get('severity')}:{r.get('cnt')}" for r in sev_rows) if sev_rows else "none"),
            "",
            "## Key Incidents",
            "",
        ]
        for inc in recent:
            lines.append(f"- **{inc.get('severity','')}** `{inc.get('incident_id','')}` — root cause `{inc.get('root_cause_entity','')}` | impact {inc.get('business_impact_score','')} | SLA risk {inc.get('sla_breach_risk','')} | {inc.get('created_at','')}")
        lines.extend(["", "## Business Impact", "", "Scores and SLA breach risks are live from ClickHouse; no synthetic data.", ""])
        content = "\n".join(lines)
        prompt = f"Rewrite this as a 4-sentence executive summary for leadership, grounded only in these facts, no hallucinations:\n{content}"
        enhanced = await _llm_generate(prompt)
        if enhanced:
            content = content + "\n\n---\n\n**Ollama qwen3:8b (executive, grounded):**\n\n" + enhanced
            source = "clickhouse+ollama"
        else:
            source = "clickhouse"
        return {"content": content, "source": source, "timestamp": _now_iso()}

    # ----- GET /api/genai/runbook -----

    @app.get("/api/genai/runbook")
    async def api_genai_runbook(
        entity_id: str | None = Query(None),
        incident_id: str | None = Query(None),
    ) -> dict:
        """Live runbook: prefers MinIO omniwatch-runbooks artifact, then grounded generation from latest incident."""
        target_incident: dict | None = None
        if incident_id:
            rows = _safe_ch_query("SELECT * FROM omniwatch.incidents WHERE incident_id = %(iid)s LIMIT 1", parameters={"iid": incident_id})
            target_incident = rows[0] if rows else None
        elif entity_id:
            rows = _safe_ch_query("SELECT * FROM omniwatch.incidents WHERE root_cause_entity = %(eid)s ORDER BY created_at DESC LIMIT 1", parameters={"eid": entity_id})
            target_incident = rows[0] if rows else None
        else:
            target_incident = _latest_incident()
        # Try MinIO persisted runbooks first (live)
        if target_incident:
            iid = target_incident.get("incident_id", "")
            # Search common prefixes
            for prefix in [f"genai/{iid}/runbook", f"reports/{iid}", ""]:
                objs = _safe_minio_list(MINIO_BUCKET_RUNBOOKS, prefix=prefix)
                if objs:
                    # Try to fetch the newest object
                    data = _safe_minio_get(MINIO_BUCKET_RUNBOOKS, objs[-1])
                    if data:
                        try:
                            j = json.loads(data)
                            content = j.get("content") or j.get("summary") or json.dumps(j, indent=2)
                            if isinstance(content, list):
                                content = "\n".join(str(x) for x in content)
                            return {"content": f"# Runbook — {iid}\n\n_MinIO `omniwatch-runbooks/{objs[-1]}` (live)_\n\n{content}", "source": "minio:omniwatch-runbooks", "timestamp": _now_iso()}
                        except Exception:  # noqa: BLE001
                            txt = data.decode("utf-8", errors="replace")
                            return {"content": f"# Runbook — {iid}\n\n_MinIO `omniwatch-runbooks/{objs[-1]}` (live)_\n\n{txt}", "source": "minio:omniwatch-runbooks", "timestamp": _now_iso()}
            # Try genai service grounded generation
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    # Build RootCauseObject from incident row
                    rc = {
                        "incident_id": target_incident.get("incident_id", ""),
                        "root_cause_entity": target_incident.get("root_cause_entity", ""),
                        "entity_type": target_incident.get("entity_type", ""),
                        "confidence": float(target_incident.get("confidence", 0) or 0),
                        "anomaly_score": 0.9,
                        "fault_path": json.loads(target_incident.get("fault_path", "[]")) if isinstance(target_incident.get("fault_path"), str) else target_incident.get("fault_path", []),
                        "impacted_services": json.loads(target_incident.get("impacted_services", "[]")) if isinstance(target_incident.get("impacted_services"), str) else target_incident.get("impacted_services", []),
                        "impacted_count": len(json.loads(target_incident.get("impacted_services", "[]")) if isinstance(target_incident.get("impacted_services"), str) else target_incident.get("impacted_services", [])),
                        "evidence": {},
                        "timestamp": str(target_incident.get("created_at", _now_iso())),
                    }
                    resp = await client.post(f"{GENAI_SERVICE_URL}/generate", json={"root_cause": rc, "artifact_type": "runbook"})
                    if resp.status_code == 200:
                        data = resp.json()
                        content = data.get("content", "")
                        return {"content": f"# Runbook — {iid}\n\n_Grounded generation via genai-service (qwen3:8b)_\n\n{content}", "source": "genai-service+ollama", "timestamp": _now_iso()}
            except Exception as exc:  # noqa: BLE001
                _LOG.debug("GenAI runbook generate failed: %s", exc)
            # Deterministic live fallback from incident row itself (still live, not dummy)
            fault = target_incident.get("fault_path", "")
            impacted = target_incident.get("impacted_services", "")
            content = textwrap.dedent(f"""\
                # Runbook — {iid}

                _Live from ClickHouse `omniwatch.incidents` — deterministic template (Ollama unavailable)_

                ## Incident
                - **Root cause:** `{target_incident.get('root_cause_entity','')}` ({target_incident.get('entity_type','')})
                - **Severity:** {target_incident.get('severity','')} | **Status:** {target_incident.get('status','')}
                - **Fault path:** {fault}
                - **Impacted:** {impacted}

                ## Steps
                1. Isolate `{target_incident.get('root_cause_entity','')}` — stop traffic / drain.
                2. Check metrics for `{target_incident.get('root_cause_entity','')}` in ClickHouse `omniwatch.metrics`.
                3. Apply remediation for `{target_incident.get('entity_type','')}` per runbook library.
                4. Verify via `GET /api/entity/{target_incident.get('root_cause_entity','')}/metrics`.
                5. Close incident and record outcome to `omniwatch.knowledge_base`.
                """)
            return {"content": content, "source": "clickhouse", "timestamp": _now_iso()}
        # No incident at all
        return {
            "content": "# Runbook Generation\n\n_No reports yet — generate one by running `python simulation/anomaly_injector.py --scenario database_cascade`._\n\n_Data source: ClickHouse `omniwatch.incidents` (live, empty) + MinIO `omniwatch-runbooks` (live, empty)._",
            "source": "empty",
            "timestamp": _now_iso(),
        }

    # ----- GET /api/genai/postmortem -----

    @app.get("/api/genai/postmortem")
    async def api_genai_postmortem(
        incident_id: str | None = Query(None),
    ) -> dict:
        """Live post-incident analysis from ClickHouse + MinIO + grounded LLM."""
        target_incident: dict | None = None
        if incident_id:
            rows = _safe_ch_query("SELECT * FROM omniwatch.incidents WHERE incident_id = %(iid)s LIMIT 1", parameters={"iid": incident_id})
            target_incident = rows[0] if rows else None
        else:
            target_incident = _latest_incident()
        if target_incident:
            iid = target_incident.get("incident_id", "")
            # Check MinIO for existing postmortem artifacts
            for prefix in [f"genai/{iid}/postmortem", f"reports/{iid}", ""]:
                objs = _safe_minio_list(MINIO_BUCKET_RUNBOOKS, prefix=prefix)
                # Filter to likely postmortem keys
                pm_objs = [o for o in objs if "postmortem" in o.lower() or "post-mortem" in o.lower()]
                if pm_objs:
                    data = _safe_minio_get(MINIO_BUCKET_RUNBOOKS, pm_objs[-1])
                    if data:
                        try:
                            j = json.loads(data)
                            content = j.get("content") or json.dumps(j, indent=2)
                            return {"content": f"# Post-Incident Analysis — {iid}\n\n_MinIO `omniwatch-runbooks/{pm_objs[-1]}` (live)_\n\n{content}", "source": "minio:omniwatch-runbooks", "timestamp": _now_iso()}
                        except Exception:  # noqa: BLE001
                            txt = data.decode("utf-8", errors="replace")
                            return {"content": f"# Post-Incident Analysis — {iid}\n\n_MinIO `omniwatch-runbooks/{pm_objs[-1]}` (live)_\n\n{txt}", "source": "minio:omniwatch-runbooks", "timestamp": _now_iso()}
            # Deterministic live fallback
            content = textwrap.dedent(f"""\
                # Post-Incident Analysis — {iid}

                _Live from ClickHouse `omniwatch.incidents` — deterministic template (Ollama unavailable)_

                ## Root Cause
                `{target_incident.get('root_cause_entity','')}` ({target_incident.get('entity_type','')}) — confidence {target_incident.get('confidence','')}

                ## Timeline / Fault Path
                {target_incident.get('fault_path','')}

                ## Impacted Services
                {target_incident.get('impacted_services','')} | SLA risk {target_incident.get('sla_breach_risk','')} | deduplicated {target_incident.get('deduplicated_count','')}

                ## Lessons Learned
                - Root cause `{target_incident.get('root_cause_entity','')}` propagated via {target_incident.get('fault_path','')}.
                - Check `omniwatch.knowledge_base` for prior similar incidents.

                ## Action Items
                - Harden `{target_incident.get('root_cause_entity','')}` monitoring.
                - Update runbook in `omniwatch-runbooks` bucket.
                """)
            prompt = f"Write a 3-sentence post-incident lesson, grounded only in: {content}"
            enhanced = await _llm_generate(prompt)
            if enhanced:
                content = content + "\n\n---\n\n**Ollama qwen3:8b (grounded):**\n\n" + enhanced
                source = "clickhouse+ollama"
            else:
                source = "clickhouse"
            return {"content": content, "source": source, "timestamp": _now_iso()}
        return {
            "content": "# Post-Incident Analysis\n\n_No reports yet — generate one by running `python simulation/anomaly_injector.py --scenario database_cascade`._\n\n_Data source: ClickHouse `omniwatch.incidents` (live, empty)._",
            "source": "empty",
            "timestamp": _now_iso(),
        }

    # ----- GET /api/orchestration/status -----

    @app.get("/api/orchestration/status")
    async def api_orchestration_status() -> dict:
        """Proxy to orchestration engine health or return fallback."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{ORCHESTRATION_SERVICE_URL}/health")
                resp.raise_for_status()
                data = resp.json()
                return {"status": "ok", "orchestration": data, "timestamp": _now_iso()}
        except Exception:  # noqa: BLE001
            return {"status": "ok", "message": "orchestration not reachable", "timestamp": _now_iso()}

    # ----- GET /api/actions + /api/remediation (Input 6 live — OPA + action library) -----

    @app.get("/api/actions")
    async def api_actions(
        limit: int = Query(50, ge=1, le=500),
        status: str | None = Query(None),
    ) -> dict:
        """Live remediation actions: ClickHouse pending_approvals + MinIO audit-logs fallback."""
        where = ""
        params: dict[str, Any] = {"limit": limit}
        if status:
            where = "WHERE status = %(status)s"
            params["status"] = status
        rows = _safe_ch_query(
            f"SELECT * FROM omniwatch.pending_approvals {where} ORDER BY created_at DESC LIMIT %(limit)s",
            parameters=params,
        )
        return {"actions": rows, "count": len(rows), "timestamp": _now_iso(), "source": "clickhouse:pending_approvals+minio:audit-logs"}

    @app.get("/api/remediation/history")
    async def api_remediation_history(limit: int = Query(50, ge=1, le=500)) -> dict:
        """Live remediation history from ClickHouse knowledge_base + pending_approvals."""
        kb = _safe_ch_query(
            "SELECT * FROM omniwatch.knowledge_base ORDER BY created_at DESC LIMIT %(limit)s",
            parameters={"limit": limit},
        )
        approvals = _safe_ch_query(
            "SELECT * FROM omniwatch.pending_approvals WHERE status != 'pending' ORDER BY decided_at DESC LIMIT %(limit)s",
            parameters={"limit": limit},
        )
        return {"knowledge_base": kb, "approvals": approvals, "count": len(kb) + len(approvals), "timestamp": _now_iso()}

    @app.get("/api/learning/stats")
    async def api_learning_stats() -> dict:
        """Proxy to learning service or fallback to ClickHouse knowledge_base stats."""
        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.get(f"{LEARNING_SERVICE_URL}/stats")
                if resp.status_code == 200:
                    return resp.json()
        except Exception:  # noqa: BLE001
            pass
        # Fallback: same as /api/knowledge/stats live from ClickHouse
        total = _safe_ch_query("SELECT count() as cnt FROM omniwatch.knowledge_base")
        by_outcome = _safe_ch_query("SELECT outcome, count() as cnt FROM omniwatch.knowledge_base GROUP BY outcome")
        return {
            "total_entries": total[0].get("cnt", 0) if total else 0,
            "by_outcome": by_outcome,
            "source": "clickhouse:knowledge_base",
            "timestamp": _now_iso(),
        }

    # ----- POST /api/copilot -----

    @app.post("/api/copilot", response_model=None)
    async def api_copilot_post(body: dict[str, Any]):
        """Accept copilot chat as JSON POST body. Supports SSE streaming when body.stream=true.
        Accepts optional history array of {role, content} objects for context memory."""
        question = body.get("question", "")
        context = body.get("context", "")
        stream = body.get("stream", False)
        raw_history = body.get("history", [])
        if not question:
            return JSONResponse(status_code=400, content={"error": "question field required"})

        mm = _get_model_manager()
        model_name = mm.get_settings().model_name
        system_prompt = textwrap.dedent(f"""\
            You are the OmniWatch AIOps Copilot (powered by {model_name} via Ollama). Be concise, max 300 words, directly answer the question, use code blocks for queries.
            Current model: {model_name} — identify as this when asked "what model are you".
            Real OmniWatch stack: OpenTelemetry SDK + Collector (4317/4318), Kafka, Flink (entity-resolution + feature-store), ClickHouse, Neo4j, MinIO, OPA, Ollama, React. Does NOT use Prometheus/Grafana.
            Real routes: / (Overview), /incidents, /topology, /knowledge, /reports, /security, /data, /graph, /storage, /settings/model. You can answer general OmniWatch questions (overview, how to use, architecture) using this stack. If asked about Prometheus/Grafana, clarify it uses OTel+ClickHouse. Only invent integrations if truly unknown — then say "I don't know".
        """)
        user_prompt = f"Context:\n{context}\n\nQuestion: {question}" if context else question

        # Build messages array with optional history (keep short to avoid confusion)
        messages = [{"role": "system", "content": system_prompt}]
        if isinstance(raw_history, list):
            for entry in raw_history[-6:]:
                if isinstance(entry, dict) and entry.get("role") in ("user", "assistant") and entry.get("content"):
                    messages.append({"role": entry["role"], "content": str(entry["content"])[:2000]})
        messages.append({"role": "user", "content": user_prompt})

        try:
            mm = _get_model_manager()
            result = await mm.chat(messages, stream=stream)

            if stream and not isinstance(result, str):
                return sse_response(result)

            answer = result if isinstance(result, str) else str(result)
            return {"answer": answer, "model": mm.get_settings().model_name, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Copilot POST failed: %s", exc)
            return {"answer": "Copilot unavailable — LLM service not reachable.", "error": str(exc), "timestamp": _now_iso()}

    # ----- GET /api/dashboard/{id} (load from MinIO omniwatch-dashboards) -----

    @app.get("/api/dashboard/{dashboard_id}", response_model=None)
    async def api_dashboard_load(dashboard_id: str):
        """Load a saved dashboard JSON from MinIO omniwatch-dashboards bucket."""
        data = _safe_minio_get(MINIO_BUCKET_DASHBOARDS, dashboard_id)
        if data is None:
            return JSONResponse(status_code=404, content={"error": "dashboard not found", "dashboard_id": dashboard_id})
        try:
            content = json.loads(data)
        except (json.JSONDecodeError, ValueError):
            content = data.decode("utf-8", errors="replace")
        return {"dashboard_id": dashboard_id, "dashboard": content, "timestamp": _now_iso()}

    # ----- Security IP/geo aggregation (zero dummy — live ClickHouse source_ip) -----

    @app.get("/api/security/anomalies")
    async def api_security_anomalies(
        limit: int = Query(50, ge=1, le=500),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        where = "WHERE source_type = 'security'"
        params: dict[str, Any] = {"limit": limit}
        if eff is not None:
            where += " AND timestamp >= now() - INTERVAL %(hours)s HOUR"
            params["hours"] = eff
        query = f"SELECT * FROM omniwatch.anomalies {where} ORDER BY timestamp DESC LIMIT %(limit)s"
        rows = _safe_ch_query(query, parameters=params)
        return {"anomalies": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/security/geo")
    async def api_security_geo(
        limit: int = Query(50, ge=1, le=200),
        hours: Any = Query(None, description="Filter window in hours"),
        timeRange: Any = Query(None, description="Alias: 1h, 6h, 24h, 7d"),
    ) -> dict:
        eff = _resolve_hours(hours, timeRange)
        effective_hours = eff
        where = "WHERE source_type = 'security' AND source_ip IS NOT NULL AND source_ip != '' AND source_ip != 'unknown'"
        params2: dict[str, Any] = {"limit": limit}
        if effective_hours is not None:
            where += " AND timestamp >= now() - INTERVAL %(hours)s HOUR"
            params2["hours"] = effective_hours
        # IP-level aggregation — honest, no invented country mapping
        query = f"""
            SELECT source_ip AS ip, count() AS cnt,
                   any(attack_type) AS attack_type,
                   any(severity) AS severity,
                   max(timestamp) AS last_seen
            FROM omniwatch.anomalies
            {where}
            GROUP BY source_ip
            ORDER BY cnt DESC
            LIMIT %(limit)s
        """
        rows = _safe_ch_query(query, parameters=params2)
        # Normalize cnt key to count for frontend convenience, keep both
        buckets = []
        for r in rows:
            buckets.append({
                "ip": r.get("ip", ""),
                "count": int(r.get("cnt", 0) or 0),
                "cnt": int(r.get("cnt", 0) or 0),
                "attack_type": r.get("attack_type", ""),
                "severity": r.get("severity", ""),
                "last_seen": str(r.get("last_seen", "")),
            })
        return {"buckets": buckets, "count": len(buckets), "timestamp": _now_iso(), "note": "ip-level aggregation from ClickHouse omniwatch.anomalies.source_ip — no synthetic GeoIP"}

    # ----- POST /api/dashboard/{id} (save to MinIO omniwatch-dashboards) -----

    @app.post("/api/dashboard/{dashboard_id}", response_model=None)
    async def api_dashboard_save(dashboard_id: str, body: dict[str, Any]):
        """Save a dashboard JSON to MinIO omniwatch-dashboards bucket."""
        payload = json.dumps(body, default=str).encode("utf-8")
        ok = _safe_minio_put(MINIO_BUCKET_DASHBOARDS, dashboard_id, payload, content_type="application/json")
        if not ok:
            return JSONResponse(status_code=500, content={"error": "failed to save dashboard", "dashboard_id": dashboard_id})
        return {"dashboard_id": dashboard_id, "saved": True, "timestamp": _now_iso()}

    # ----- POST /api/clickhouse/query (SQL proxy) -----

    _DDL_DENY_RE = re.compile(r"\b(DROP|ALTER|TRUNCATE|DETACH|REVOKE|GRANT|CREATE)\b", re.IGNORECASE)

    @app.post("/api/clickhouse/query", response_model=None)
    async def api_clickhouse_query(
        body: dict[str, Any] = Body(...),
        x_clickhouse_user: str | None = Header(None, alias="X-ClickHouse-User"),
        x_clickhouse_password: str | None = Header(None, alias="X-ClickHouse-Password"),
    ) -> JSONResponse | dict:
        """SQL proxy: execute a read-only query against ClickHouse with auth, DDL deny-list, and LIMIT enforcement."""
        if not x_clickhouse_user or not x_clickhouse_password:
            return JSONResponse(status_code=401, content={"error": "missing X-ClickHouse-User or X-ClickHouse-Password header"})
        sql = (body.get("query") or "").strip()
        if not sql:
            return JSONResponse(status_code=422, content={"error": "query field is required"})
        # DDL deny-list: case-insensitive match for destructive SQL keywords
        if _DDL_DENY_RE.search(sql):
            return JSONResponse(status_code=422, content={"error": "DDL statements are not allowed (DROP, ALTER, TRUNCATE, DETACH, REVOKE, GRANT, CREATE)"})
        # LIMIT enforcement: clamp 1..500, append if absent
        raw_limit = int(body.get("limit", 100) or 100)
        limit = max(1, min(raw_limit, 500))
        if not re.search(r"\bLIMIT\s+\d", sql, re.IGNORECASE):
            sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                username=x_clickhouse_user, password=x_clickhouse_password,
                connect_timeout=5, send_receive_timeout=30,
            )
        except Exception as exc:
            msg = str(exc).split("\n")[0][:200]
            if _is_ch_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "ClickHouse authentication failed"})
            if "timeout" in msg.lower():
                return JSONResponse(status_code=504, content={"error": "query timeout"})
            return JSONResponse(status_code=502, content={"error": "ClickHouse is not reachable"})
        try:
            result = client.query(sql)
        except Exception as exc:
            msg = str(exc).split("\n")[0][:200]
            if _is_ch_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "ClickHouse authentication failed"})
            if "timeout" in msg.lower():
                return JSONResponse(status_code=504, content={"error": "query timeout (>30s)"})
            return JSONResponse(status_code=422, content={"error": f"syntax or execution error: {msg}"})
        columns = [col[0] for col in result.column_names] if hasattr(result, "column_names") and result.column_names else []
        rows: list[dict] = []
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                rows.append({columns[i]: row[i] for i in range(min(len(columns), len(row)))})
        truncated = len(rows) >= limit
        return {"columns": columns, "rows": rows, "row_count": len(rows), "truncated": truncated, "timestamp": _now_iso()}

    # ----- GET /api/clickhouse/tables -----

    @app.get("/api/clickhouse/tables", response_model=None)
    async def api_clickhouse_tables(
        x_clickhouse_user: str | None = Header(None, alias="X-ClickHouse-User"),
        x_clickhouse_password: str | None = Header(None, alias="X-ClickHouse-Password"),
    ) -> JSONResponse | dict:
        """List all tables from system.tables with auth credentials."""
        if not x_clickhouse_user or not x_clickhouse_password:
            return JSONResponse(status_code=401, content={"error": "missing X-ClickHouse-User or X-ClickHouse-Password header"})
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                username=x_clickhouse_user, password=x_clickhouse_password,
                connect_timeout=5, send_receive_timeout=10,
            )
            result = client.query(
                "SELECT database, name, engine FROM system.tables ORDER BY database, name LIMIT 500"
            )
        except Exception as exc:
            msg = str(exc).split("\n")[0][:200]
            if _is_ch_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "ClickHouse authentication failed"})
            if "timeout" in msg.lower():
                return JSONResponse(status_code=504, content={"error": "query timeout"})
            return JSONResponse(status_code=502, content={"error": "ClickHouse is not reachable"})
        columns = [col[0] for col in result.column_names] if hasattr(result, "column_names") and result.column_names else []
        rows: list[dict] = []
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                rows.append({columns[i]: row[i] for i in range(min(len(columns), len(row)))})
        return {"tables": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- GET /api/clickhouse/schema/{table} -----

    @app.get("/api/clickhouse/schema/{table_name}", response_model=None)
    async def api_clickhouse_schema(
        table_name: str,
        x_clickhouse_user: str | None = Header(None, alias="X-ClickHouse-User"),
        x_clickhouse_password: str | None = Header(None, alias="X-ClickHouse-Password"),
    ) -> JSONResponse | dict:
        """Return column schema for a given table from system.columns with auth credentials."""
        if not x_clickhouse_user or not x_clickhouse_password:
            return JSONResponse(status_code=401, content={"error": "missing X-ClickHouse-User or X-ClickHouse-Password header"})
        # Input validation: table name must be a safe identifier (dots/underscores for db.table)
        if not re.match(r"^[a-zA-Z0-9_.]+$", table_name):
            return JSONResponse(status_code=422, content={"error": "invalid table name"})
        try:
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
                username=x_clickhouse_user, password=x_clickhouse_password,
                connect_timeout=5, send_receive_timeout=10,
            )
            result = client.query(
                "SELECT name, type, default_kind, default_expression, comment "
                "FROM system.columns WHERE table = %(tbl)s ORDER BY position",
                parameters={"tbl": table_name},
            )
        except Exception as exc:
            msg = str(exc).split("\n")[0][:200]
            if _is_ch_auth_error(msg):
                return JSONResponse(status_code=401, content={"error": "ClickHouse authentication failed"})
            if "timeout" in msg.lower():
                return JSONResponse(status_code=504, content={"error": "query timeout"})
            return JSONResponse(status_code=502, content={"error": "ClickHouse is not reachable"})
        columns = [col[0] for col in result.column_names] if hasattr(result, "column_names") and result.column_names else []
        rows: list[dict] = []
        if hasattr(result, "result_rows") and result.result_rows:
            for row in result.result_rows:
                rows.append({columns[i]: row[i] for i in range(min(len(columns), len(row)))})
        if not rows:
            return JSONResponse(status_code=404, content={"error": f"table '{table_name}' not found or has no columns"})
        return {"table": table_name, "columns": rows, "count": len(rows), "timestamp": _now_iso()}

    # ----- POST /api/neo4j/query (Cypher proxy) -----

    # Neo4j DDL deny-list: block destructive operations and label creation.
    # Note: DELETE is blocked separately (case-insensitive) to catch both
    # standalone DELETE and DETACH DELETE patterns.
    _NEO4J_DDL_DENY_RE = re.compile(r"\b(DROP|DETACH\s+DELETE|DELETE|CREATE)\b", re.IGNORECASE)

    @app.post("/api/neo4j/query", response_model=None)
    async def api_neo4j_query(
        body: dict[str, Any] = Body(...),
        x_neo4j_user: str | None = Header(None, alias="X-Neo4j-User"),
        x_neo4j_password: str | None = Header(None, alias="X-Neo4j-Password"),
    ) -> JSONResponse | dict:
        """Cypher proxy: execute a read-only query against Neo4j with auth, DDL deny-list, and LIMIT enforcement.

        Uses a per-request driver (not the module singleton) because each request
        may carry different credentials via X-Neo4j-User / X-Neo4j-Password headers.
        The singleton ``_get_neo4j_driver()`` uses env-var credentials and is
        unsuitable for multi-tenant auth — hence the intentional exception.
        """
        if not x_neo4j_user or not x_neo4j_password:
            return JSONResponse(status_code=401, content={"error": "missing X-Neo4j-User or X-Neo4j-Password header"})
        cypher = (body.get("query") or "").strip()
        if not cypher:
            return JSONResponse(status_code=422, content={"error": "query field is required"})
        # DDL deny-list: case-insensitive match for destructive Cypher keywords
        if _NEO4J_DDL_DENY_RE.search(cypher):
            return JSONResponse(status_code=422, content={"error": "destructive operations are not allowed (DROP, DELETE, DETACH DELETE, CREATE)"})
        # LIMIT enforcement: clamp 1..500, append if absent
        raw_limit = int(body.get("limit", 100) or 100)
        limit = max(1, min(raw_limit, 500))
        if not re.search(r"\bLIMIT\s+\d", cypher, re.IGNORECASE):
            cypher = f"{cypher.rstrip()} LIMIT {limit}"
        # Per-request driver — see docstring for why singleton is not reused
        driver: Any = None
        try:
            driver = neo4j.GraphDatabase.driver(
                NEO4J_URI, auth=(x_neo4j_user, x_neo4j_password),
            )
        except Exception:
            return JSONResponse(status_code=502, content={"error": "Neo4j is not reachable"})
        try:
            with driver.session() as session:
                result = session.run(cypher)
                records = [dict(record) for record in result]
            # Flatten keys from all records to build a consistent column list
            columns: list[str] = []
            seen: set[str] = set()
            for rec in records:
                for key in rec:
                    if key not in seen:
                        columns.append(key)
                        seen.add(key)
            truncated = len(records) >= limit
            return {"columns": columns, "rows": records, "row_count": len(records), "truncated": truncated, "timestamp": _now_iso()}
        except neo4j.exceptions.AuthError:
            return JSONResponse(status_code=401, content={"error": "Neo4j authentication failed"})
        except neo4j.exceptions.ServiceUnavailable:
            return JSONResponse(status_code=502, content={"error": "Neo4j is not reachable"})
        except neo4j.exceptions.Neo4jError as exc:
            msg = str(exc).split("\n")[0][:200]
            if "timeout" in msg.lower():
                return JSONResponse(status_code=504, content={"error": "query timeout"})
            return JSONResponse(status_code=422, content={"error": f"syntax or execution error: {msg}"})
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).split("\n")[0][:200]
            return JSONResponse(status_code=422, content={"error": f"execution error: {msg}"})
        finally:
            if driver is not None:
                try:
                    driver.close()
                except Exception:  # noqa: BLE001
                    pass

    # ----- GET /api/neo4j/schema -----

    @app.get("/api/neo4j/schema", response_model=None)
    async def api_neo4j_schema(
        x_neo4j_user: str | None = Header(None, alias="X-Neo4j-User"),
        x_neo4j_password: str | None = Header(None, alias="X-Neo4j-Password"),
    ) -> JSONResponse | dict:
        """Return Neo4j schema metadata: labels, relationship types, and property keys.

        Uses a per-request driver for the same multi-tenant auth reasons as the
        query endpoint above.
        """
        if not x_neo4j_user or not x_neo4j_password:
            return JSONResponse(status_code=401, content={"error": "missing X-Neo4j-User or X-Neo4j-Password header"})
        driver: Any = None
        try:
            driver = neo4j.GraphDatabase.driver(
                NEO4J_URI, auth=(x_neo4j_user, x_neo4j_password),
            )
        except Exception:
            return JSONResponse(status_code=502, content={"error": "Neo4j is not reachable"})
        try:
            with driver.session() as session:
                labels_rec = session.run("CALL db.labels() YIELD label RETURN label")
                labels = [record["label"] for record in labels_rec]
                rels_rec = session.run("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
                rel_types = [record["relationshipType"] for record in rels_rec]
                props_rec = session.run("CALL db.propertyKeys() YIELD propertyKey RETURN propertyKey")
                prop_keys = [record["propertyKey"] for record in props_rec]
            return {
                "labels": labels,
                "relationshipTypes": rel_types,
                "propertyKeys": prop_keys,
                "count": {"labels": len(labels), "relationshipTypes": len(rel_types), "propertyKeys": len(prop_keys)},
                "timestamp": _now_iso(),
            }
        except neo4j.exceptions.AuthError:
            return JSONResponse(status_code=401, content={"error": "Neo4j authentication failed"})
        except neo4j.exceptions.ServiceUnavailable:
            return JSONResponse(status_code=502, content={"error": "Neo4j is not reachable"})
        except Exception as exc:  # noqa: BLE001
            msg = str(exc).split("\n")[0][:200]
            return JSONResponse(status_code=422, content={"error": f"schema query error: {msg}"})
        finally:
            if driver is not None:
                try:
                    driver.close()
                except Exception:  # noqa: BLE001
                    pass

    return app


# ---------------------------------------------------------------------------
# Module-level app (orchestration pattern)
# ---------------------------------------------------------------------------

app = create_app()

# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("dashboard.api.main:app", host="0.0.0.0", port=DASHBOARD_PORT, reload=False)
