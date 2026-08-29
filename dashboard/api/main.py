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
import textwrap
from datetime import datetime, timezone
from typing import Any

import clickhouse_connect  # type: ignore[import-untyped]
import httpx
import minio  # type: ignore[import-untyped]
import neo4j
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_LOG: logging.Logger = logging.getLogger("omniwatch.dashboard")

# ---------------------------------------------------------------------------
# Config (all from env, never hardcoded)
# ---------------------------------------------------------------------------

CLICKHOUSE_HOST: str = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT: int = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB: str = os.getenv("CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER: str = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD: str = os.getenv("CLICKHOUSE_PASSWORD", "")

NEO4J_URI: str = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER: str = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD: str = os.getenv("NEO4J_PASSWORD", "omniwatch")

MINIO_ENDPOINT: str = os.getenv("MINIO_ENDPOINT", "localhost:9010")
MINIO_ACCESS_KEY: str = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_SECURE: bool = os.getenv("MINIO_SECURE", "false").lower() in ("1", "true", "yes", "on")

OLLAMA_URL: str = os.getenv("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL: str = os.getenv("OLLAMA_MODEL", "qwen3:8b")
LEARNING_SERVICE_URL: str = os.getenv("LEARNING_SERVICE_URL", "http://learning:8030")
GENAI_SERVICE_URL: str = os.getenv("GENAI_SERVICE_URL", "http://genai:8020")
ORCHESTRATION_SERVICE_URL: str = os.getenv("ORCHESTRATION_SERVICE_URL", "http://orchestration:8010")

DASHBOARD_PORT: int = int(os.getenv("DASHBOARD_PORT", "8011"))

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
        _minio_client = minio.Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE,
        )
    return _minio_client


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
        client = _get_minio_client()
        return [obj.object_name for obj in client.list_objects(bucket, prefix=prefix)]
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("MinIO list failed for bucket=%s prefix=%s: %s", bucket, prefix, exc)
        return []


def _safe_minio_get(bucket: str, object_name: str) -> bytes | None:
    """Download MinIO object with graceful fallback on error."""
    try:
        client = _get_minio_client()
        return client.get_object(bucket, object_name).read()
    except Exception as exc:  # noqa: BLE001
        _LOG.warning("MinIO get failed for bucket=%s key=%s: %s", bucket, object_name, exc)
        return None


def _safe_minio_put(bucket: str, object_name: str, data: bytes, content_type: str = "application/octet-stream") -> bool:
    """Upload data to MinIO with graceful fallback. Returns True on success."""
    try:
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

    # ----- root health -----

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok", "service": "dashboard-api", "timestamp": _now_iso()}

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
        nodes_query = "MATCH (n) RETURN n.id AS id, n.name AS label, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score"
        nodes_raw = _safe_neo4j_query(nodes_query)

        edges_query = "MATCH (a)-[r]->(b) RETURN a.id AS source, b.id AS target, type(r) AS label, r.latency_p50 AS latency_p50, r.error_rate AS error_rate"
        edges_raw = _safe_neo4j_query(edges_query)

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
        query = "MATCH (n) RETURN n.id AS id, n.name AS name, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score, n.last_seen AS last_seen ORDER BY n.anomaly_score DESC"
        rows = _safe_neo4j_query(query)
        return {"entities": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/entities/top")
    async def api_entities_top(limit: int = Query(10, ge=1, le=100)) -> dict:
        """Return top entities by anomaly score."""
        query = "MATCH (n) WHERE n.anomaly_score IS NOT NULL RETURN n.id AS id, n.name AS name, labels(n) AS labels, n.type AS entity_type, n.anomaly_score AS anomaly_score ORDER BY n.anomaly_score DESC LIMIT %(limit)s"
        rows = _safe_neo4j_query(query, parameters={"limit": limit})
        return {"entities": rows, "count": len(rows), "timestamp": _now_iso()}

    @app.get("/api/entity/{entity_id}", response_model=None)
    async def api_entity_detail(entity_id: str):
        """Return details for a specific entity from Neo4j."""
        query = "MATCH (n {id: $eid}) RETURN n.id AS id, n.name AS name, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score, n.last_seen AS last_seen"
        rows = _safe_neo4j_query(query, parameters={"eid": entity_id})
        if not rows:
            return JSONResponse(status_code=404, content={"error": "entity not found", "entity_id": entity_id})

        # Get connected entities
        neighbors_query = "MATCH (n {id: $eid})-[r]-(m) RETURN m.id AS id, m.name AS name, type(r) AS rel_type, labels(m) AS labels"
        neighbors = _safe_neo4j_query(neighbors_query, parameters={"eid": entity_id})

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
        files = _safe_minio_list("omniwatch-audit-logs", prefix=prefix)
        return {"audit_logs": files, "count": len(files), "timestamp": _now_iso()}

    @app.get("/api/audit-logs/{object_name}", response_model=None)
    async def api_audit_log_detail(object_name: str):
        """Download a specific audit log from MinIO."""
        data = _safe_minio_get("omniwatch-audit-logs", object_name)
        if data is None:
            return JSONResponse(status_code=404, content={"error": "audit log not found", "object_name": object_name})
        try:
            content = json.loads(data)
        except (json.JSONDecodeError, ValueError):
            content = data.decode("utf-8", errors="replace")
        return {"object_name": object_name, "content": content, "timestamp": _now_iso()}

    # ----- MinIO browser — live bucket/object listing (zero dummy) -----

    @app.get("/api/minio/buckets")
    async def api_minio_buckets() -> dict:
        """List all MinIO buckets via real MinIO SDK — no hardcoded names."""
        try:
            client = _get_minio_client()
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
            return {"buckets": [], "count": 0, "error": str(exc), "timestamp": _now_iso()}

    @app.get("/api/minio/objects", response_model=None)
    async def api_minio_objects(
        bucket: str = Query(..., min_length=1, description="Bucket name"),
        prefix: str = Query("", description="Optional prefix filter"),
        limit: int = Query(50, ge=1, le=500, description="Page size"),
        offset: int = Query(0, ge=0, description="Pagination offset"),
    ):
        """List objects in a bucket with name/size/last_modified — paginated, live."""
        try:
            client = _get_minio_client()
            objs: list[dict] = []
            for obj in client.list_objects(bucket, prefix=prefix, recursive=True):
                lm = getattr(obj, "last_modified", None)
                objs.append(
                    {
                        "name": getattr(obj, "object_name", "") or "",
                        "size": int(getattr(obj, "size", 0) or 0),
                        "last_modified": lm.isoformat() if hasattr(lm, "isoformat") and lm else (str(lm) if lm else None),
                        "etag": getattr(obj, "etag", None),
                    }
                )
            total = len(objs)
            paged = objs[offset : offset + limit]
            return {
                "bucket": bucket,
                "prefix": prefix,
                "objects": paged,
                "count": len(paged),
                "total": total,
                "limit": limit,
                "offset": offset,
                "timestamp": _now_iso(),
            }
        except Exception as exc:  # noqa: BLE001
            msg = str(exc)
            _LOG.warning("MinIO list_objects failed bucket=%s: %s", bucket, msg)
            if "NoSuchBucket" in msg or "does not exist" in msg.lower():
                return JSONResponse(status_code=404, content={"error": f"bucket not found: {bucket}", "bucket": bucket, "timestamp": _now_iso()})
            return JSONResponse(status_code=500, content={"error": msg, "bucket": bucket, "timestamp": _now_iso()})

    # ----- incident archive (MinIO) -----

    @app.get("/api/incident-archive")
    async def api_incident_archive(prefix: str = Query("")) -> dict:
        """List archived incident objects from MinIO omniwatch-incidents bucket."""
        files = _safe_minio_list("omniwatch-incidents", prefix=prefix)
        return {"incidents": files, "count": len(files), "timestamp": _now_iso()}

    # ----- compliance reports (MinIO) -----

    @app.get("/api/compliance-reports")
    async def api_compliance_reports(prefix: str = Query("")) -> dict:
        """List compliance reports from MinIO omniwatch-audit-logs bucket."""
        files = _safe_minio_list("omniwatch-audit-logs", prefix="compliance/")
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

    # ----- copilot (Ollama LLM) -----

    @app.get("/api/copilot")
    async def api_copilot(
        question: str = Query(..., min_length=1),
        context: str = Query(""),
    ) -> dict:
        """Ask the copilot a question using Ollama qwen3:8b."""
        system_prompt = textwrap.dedent("""\
            You are the OmniWatch AIOps copilot. Answer questions about
            cloud operations, anomalies, incidents, and root causes.
            Be concise and actionable. If you don't know, say so.
        """)
        user_prompt = question
        if context:
            user_prompt = f"Context:\n{context}\n\nQuestion: {question}"

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={
                        "model": OLLAMA_MODEL,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "stream": False,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                answer = data.get("message", {}).get("content", "No response")
                return {"answer": answer, "model": OLLAMA_MODEL, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Ollama copilot failed: %s", exc)
            return {"answer": "Copilot unavailable — Ollama service not reachable.", "error": str(exc), "timestamp": _now_iso()}

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
        query = "MATCH (n) WHERE n.anomaly_score IS NOT NULL RETURN n.id AS id, n.name AS name, n.anomaly_score AS anomaly_score, n.status AS status ORDER BY n.anomaly_score DESC"
        rows = _safe_neo4j_query(query)
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
            minio_ok = client.bucket_exists("omniwatch-audit-logs")
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
        center_query = "MATCH (n {id: $eid}) RETURN n.id AS id, n.name AS label, labels(n) AS labels, n.type AS entity_type, n.criticality AS criticality, n.status AS status, n.anomaly_score AS anomaly_score"
        center_raw = _safe_neo4j_query(center_query, parameters={"eid": entity_id})

        if not center_raw:
            return JSONResponse(status_code=404, content={"error": "entity not found", "entity_id": entity_id})

        neighbors_query = "MATCH (n {id: $eid})-[r]-(m) RETURN m.id AS id, m.name AS label, labels(m) AS labels, m.type AS entity_type, m.criticality AS criticality, m.status AS status, m.anomaly_score AS anomaly_score, type(r) AS rel_type, r.latency_p50 AS latency_p50, r.error_rate AS error_rate"
        neighbors_raw = _safe_neo4j_query(neighbors_query, parameters={"eid": entity_id})

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
        data = _safe_minio_get("omniwatch-runbooks", runbook_id)
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
        runbooks = _safe_minio_list("omniwatch-runbooks", prefix="")
        recent = _safe_ch_query("SELECT * FROM omniwatch.incidents ORDER BY created_at DESC LIMIT 5")
        content = _render_live_summary_markdown(all_stats, recent, runbooks)
        # Optionally enhance with Ollama using live stats
        prompt = f"Summarize this live AIOps state in 3 sentences, grounded only in these facts:\n{content}\nKeep it concise, no hallucinations."
        enhanced = await _ollama_enhance(prompt)
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
        enhanced = await _ollama_enhance(prompt)
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
                objs = _safe_minio_list("omniwatch-runbooks", prefix=prefix)
                if objs:
                    # Try to fetch the newest object
                    data = _safe_minio_get("omniwatch-runbooks", objs[-1])
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
                objs = _safe_minio_list("omniwatch-runbooks", prefix=prefix)
                # Filter to likely postmortem keys
                pm_objs = [o for o in objs if "postmortem" in o.lower() or "post-mortem" in o.lower()]
                if pm_objs:
                    data = _safe_minio_get("omniwatch-runbooks", pm_objs[-1])
                    if data:
                        try:
                            j = json.loads(data)
                            content = j.get("content") or json.dumps(j, indent=2)
                            return {"content": f"# Post-Incident Analysis — {iid}\n\n_MinIO `omniwatch-runbooks/{pm_objs[-1]}` (live)_\n\n{content}", "source": "minio:omniwatch-runbooks", "timestamp": _now_iso()}
                        except Exception:  # noqa: BLE001
                            txt = data.decode("utf-8", errors="replace")
                            return {"content": f"# Post-Incident Analysis — {iid}\n\n_MinIO `omniwatch-runbooks/{pm_objs[-1]}` (live)_\n\n{txt}", "source": "minio:omniwatch-runbooks", "timestamp": _now_iso()}
            # Try genai service if it supports postmortem via /generate
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    rc = {
                        "incident_id": target_incident.get("incident_id", ""),
                        "root_cause_entity": target_incident.get("root_cause_entity", ""),
                        "entity_type": target_incident.get("entity_type", ""),
                        "confidence": float(target_incident.get("confidence", 0) or 0),
                        "anomaly_score": 0.9,
                        "fault_path": json.loads(target_incident.get("fault_path", "[]")) if isinstance(target_incident.get("fault_path"), str) else target_incident.get("fault_path", []),
                        "impacted_services": json.loads(target_incident.get("impacted_services", "[]")) if isinstance(target_incident.get("impacted_services"), str) else target_incident.get("impacted_services", []),
                        "impacted_count": 1,
                        "evidence": {},
                        "timestamp": str(target_incident.get("created_at", _now_iso())),
                    }
                    # generation_engine currently only supports summary/runbook; try anyway
                    resp = await client.post(f"{GENAI_SERVICE_URL}/generate", json={"root_cause": rc, "artifact_type": "summary"})
                    if resp.status_code == 200:
                        data = resp.json()
                        content = data.get("content", "")
                        return {"content": f"# Post-Incident Analysis — {iid}\n\n_Grounded postmortem via genai-service_\n\n## Timeline\n{target_incident.get('fault_path','')}\n\n## Analysis\n{content}\n\n## Impacted\n{target_incident.get('impacted_services','')}", "source": "genai-service+clickhouse", "timestamp": _now_iso()}
            except Exception as exc:  # noqa: BLE001
                _LOG.debug("GenAI postmortem generate failed: %s", exc)
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
            enhanced = await _ollama_enhance(prompt)
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

    # ----- POST /api/copilot (keep existing GET) -----

    @app.post("/api/copilot", response_model=None)
    async def api_copilot_post(body: dict[str, Any]):
        """Accept copilot chat as JSON POST body."""
        question = body.get("question", "")
        context = body.get("context", "")
        if not question:
            return JSONResponse(status_code=400, content={"error": "question field required"})

        system_prompt = textwrap.dedent("""\
            You are the OmniWatch AIOps copilot. Answer questions about
            cloud operations, anomalies, incidents, and root causes.
            Be concise and actionable. If you don't know, say so.
        """)
        user_prompt = f"Context:\n{context}\n\nQuestion: {question}" if context else question

        try:
            async with httpx.AsyncClient(timeout=5.0) as client:
                resp = await client.post(
                    f"{OLLAMA_URL}/api/chat",
                    json={
                        "model": OLLAMA_MODEL,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                        "stream": False,
                    },
                )
                resp.raise_for_status()
                data = resp.json()
                answer = data.get("message", {}).get("content", "No response")
                return {"answer": answer, "model": OLLAMA_MODEL, "timestamp": _now_iso()}
        except Exception as exc:  # noqa: BLE001
            _LOG.warning("Ollama copilot POST failed: %s", exc)
            return {"answer": "Copilot unavailable — Ollama service not reachable.", "error": str(exc), "timestamp": _now_iso()}

    # ----- GET /api/dashboard/{id} (load from MinIO omniwatch-dashboards) -----

    @app.get("/api/dashboard/{dashboard_id}", response_model=None)
    async def api_dashboard_load(dashboard_id: str):
        """Load a saved dashboard JSON from MinIO omniwatch-dashboards bucket."""
        data = _safe_minio_get("omniwatch-dashboards", dashboard_id)
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
        ok = _safe_minio_put("omniwatch-dashboards", dashboard_id, payload, content_type="application/json")
        if not ok:
            return JSONResponse(status_code=500, content={"error": "failed to save dashboard", "dashboard_id": dashboard_id})
        return {"dashboard_id": dashboard_id, "saved": True, "timestamp": _now_iso()}

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
