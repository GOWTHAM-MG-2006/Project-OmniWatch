"""
OmniWatch — Importer Layer
Component: Per-workspace OTLP/HTTP ingest service
Phase: exporter-importer-split
Purpose: Authenticate exporter Bearer tokens via identity /importers/verify,
         resolve the workspace, and write telemetry ONLY to that workspace's
         ClickHouse database (omniwatch_ws_<slug>, default -> omniwatch).
Inputs: POST /ingest (Bearer token, 10 telemetry types); identity verify
Outputs: 202 {accepted, inserted, workspace_slug}; 401/403/422; GET /health
"""

from __future__ import annotations

import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from fastapi import FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from collections import deque

logger = logging.getLogger("omniwatch.importer")


def _L(primary: str, fallback: str, default: str) -> str:
    return os.environ.get(primary, os.environ.get(fallback, default))


IMPORTER_PORT = int(_L("OMNIWATCH_IMPORTER_PORT", "IMPORTER_PORT", "4320"))
IDENTITY_URL = _L(
    "OMNIWATCH_IDENTITY_URL", "IDENTITY_URL", "http://localhost:8012"
).rstrip("/")
INTERNAL_SECRET = os.getenv("IMPORTER_INTERNAL_SECRET") or "dev-only-internal-secret"

CH_HOST = _L("OMNIWATCH_CLICKHOUSE_HOST", "CLICKHOUSE_HOST", "localhost")
CH_PORT = int(_L("OMNIWATCH_CLICKHOUSE_HTTP_PORT", "CLICKHOUSE_PORT", "8123"))
CH_USER = _L("OMNIWATCH_CLICKHOUSE_USER", "CLICKHOUSE_USER", "default")
CH_PASSWORD = _L("OMNIWATCH_CLICKHOUSE_PASSWORD", "CLICKHOUSE_PASSWORD", "")

KAFKA_BOOTSTRAP_SERVERS = _L(
    "OMNIWATCH_KAFKA_BOOTSTRAP_SERVERS", "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:29092")

# Raw topics per Dataflow.md (Flink entity-resolution + feature-store read
# these; predictive reads omniwatch.security.events).
RAW_TOPIC_METRICS = "omniwatch.metrics.raw"
RAW_TOPIC_LOGS = "omniwatch.logs.raw"
RAW_TOPIC_TRACES = "omniwatch.traces.raw"
RAW_TOPIC_EVENTS = "omniwatch.events.raw"
RAW_TOPIC_SECURITY = "omniwatch.security.raw"
TOPIC_SECURITY_EVENTS = "omniwatch.security.events"

# Live-log ring: per-workspace newest-150 (deque evicts oldest). In-memory
# by design — no ClickHouse, survives nothing, needs nothing.
LIVE_LOG_MAX = 150

# The 10 exporter telemetry types (spec §B).
METRIC_TYPES = {"metrics"}
LOG_TYPES = {"logs", "audit_logs", "auth_logs", "siem", "security_alerts"}
TRACE_TYPES = {"traces", "profiling"}
META_TYPES = {"metadata", "state"}
ALL_TYPES = METRIC_TYPES | LOG_TYPES | TRACE_TYPES | META_TYPES

_VERIFY_CACHE: dict[str, tuple[float, dict]] = {}
_VERIFY_TTL_S = 300.0

# Live exporter activity: (workspace_slug, entity_id, telemetry_type) ->
# last accepted insert (UTC). In-memory by design: a restart clears it and
# exporters re-announce on their next heartbeat (<= collection interval).
# Status is DERIVED from this (never stored, never faked).
_SEEN: dict[tuple[str, str, str], datetime] = {}
_SEEN_LOCK = threading.Lock()
_SEEN_MAX_ENTRIES = 10000


class MetricPoint(BaseModel):
    metric_name: str
    value: float
    tags: dict[str, str] = Field(default_factory=dict)
    source_type: str = "performance"
    timestamp: Optional[str] = None


class LogPoint(BaseModel):
    log_level: str = "INFO"
    message: str
    service_name: Optional[str] = None
    trace_id: str = ""
    timestamp: Optional[str] = None


class SpanPoint(BaseModel):
    trace_id: str
    span_id: str
    parent_span_id: str = ""
    service_name: Optional[str] = None
    operation: str = ""
    duration_ms: float = 0.0
    timestamp: Optional[str] = None


class IngestBody(BaseModel):
    telemetry_type: str
    entity_id: str
    entity_type: str = "API_NODE"
    metrics: list[MetricPoint] = Field(default_factory=list)
    logs: list[LogPoint] = Field(default_factory=list)
    traces: list[SpanPoint] = Field(default_factory=list)


class IngestResponse(BaseModel):
    accepted: bool = True
    workspace_slug: str
    inserted: dict[str, int]


class ActivityPoint(BaseModel):
    entity_id: str
    telemetry_type: str
    last_seen: str


class ActivityResponse(BaseModel):
    workspace_slug: str
    activity: list[ActivityPoint]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(raw: Optional[str]) -> datetime:
    if not raw:
        return utcnow()
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return utcnow()


def _workspace_db(slug: str) -> str:
    return "omniwatch" if slug == "default" else f"omniwatch_ws_{slug}"


def verify_token(token: str) -> dict:
    """Resolve a Bearer token via identity; cached for _VERIFY_TTL_S."""
    now = time.monotonic()
    hit = _VERIFY_CACHE.get(token)
    if hit is not None and now - hit[0] < _VERIFY_TTL_S:
        return hit[1]
    import httpx

    try:
        resp = httpx.post(
            f"{IDENTITY_URL}/importers/verify",
            json={"token": token},
            headers={"X-Internal-Secret": INTERNAL_SECRET},
            timeout=5.0,
        )
    except Exception as exc:  # noqa: BLE001 - network failure -> 503, not crash
        logger.warning("identity verify unreachable: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="identity unavailable",
        ) from exc
    if resp.status_code != 200:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="invalid importer token",
        )
    body = resp.json()
    _VERIFY_CACHE[token] = (now, body)
    return body


def _ch_client(db: str) -> Any:
    import clickhouse_connect

    return clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, database=db, username=CH_USER,
        password=CH_PASSWORD, connect_timeout=5, send_receive_timeout=15,
    )


def _record_seen(slug: str, entity_id: str, telemetry_type: str) -> None:
    """Mark one accepted batch (bounded map, oldest-evicted on overflow)."""
    now = utcnow()
    with _SEEN_LOCK:
        _SEEN[(slug, entity_id, telemetry_type)] = now
        if len(_SEEN) > _SEEN_MAX_ENTRIES:
            oldest = min(_SEEN.items(), key=lambda kv: kv[1])[0]
            del _SEEN[oldest]


def _get_activity(slug: str) -> list[ActivityPoint]:
    with _SEEN_LOCK:
        rows = [
            ActivityPoint(
                entity_id=entity_id, telemetry_type=ttype,
                last_seen=seen.isoformat(),
            )
            for (ws, entity_id, ttype), seen in _SEEN.items()
            if ws == slug
        ]
    rows.sort(key=lambda r: r.last_seen, reverse=True)
    return rows


def _check_workspace_owner(
    workspace_id: str, auth_header: str
) -> Optional[str]:
    """Owner check via identity using the CALLER's own bearer token.

    Returns the workspace slug iff identity answers 200 (owned-and-live);
    None otherwise (caller maps ALL of it to 403 — never 404-leak).
    """
    import httpx

    if not auth_header.startswith("Bearer "):
        return None
    try:
        resp = httpx.get(
            f"{IDENTITY_URL}/workspaces/{workspace_id}",
            headers={"Authorization": auth_header.strip()},
            timeout=5.0,
        )
    except Exception:  # noqa: BLE001 - unreachable identity -> deny
        return None
    if resp.status_code != 200:
        return None
    try:
        return str(resp.json().get("slug"))
    except Exception:  # noqa: BLE001 - malformed body -> deny
        return None


def _raw_topics(telemetry_type: str) -> list[str]:
    """Dataflow.md raw topics for one ingested batch (all 10 types covered)."""
    if telemetry_type in METRIC_TYPES:
        return [RAW_TOPIC_METRICS]
    if telemetry_type in TRACE_TYPES:
        return [RAW_TOPIC_TRACES]
    if telemetry_type in META_TYPES:
        return [RAW_TOPIC_EVENTS]
    if telemetry_type == "logs":
        return [RAW_TOPIC_LOGS]
    return [RAW_TOPIC_SECURITY, TOPIC_SECURITY_EVENTS]


_PRODUCER_LOCK = threading.Lock()
_PRODUCER: Any = None


def _kafka_producer() -> Any:
    """Lazy Kafka singleton (None when unreachable — caller degrades)."""
    global _PRODUCER
    with _PRODUCER_LOCK:
        if _PRODUCER is not None:
            return _PRODUCER
        try:
            from kafka import KafkaProducer

            import json as _json

            _PRODUCER = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: _json.dumps(v).encode("utf-8"),
                request_timeout_ms=3000,
                retries=0,
            )
            return _PRODUCER
        except Exception as exc:  # noqa: BLE001 - down Kafka -> direct-only
            logger.warning("kafka producer unavailable: %s", exc)
            return None


def _publish_raw(topic: str, envelope: dict) -> None:
    """Best-effort single publish (never raises into the ingest path)."""
    try:
        producer = _kafka_producer()
        if producer is None:
            return
        producer.send(topic, value=envelope)
        try:
            producer.flush(5)
        except Exception:  # noqa: BLE001 - flush best-effort too
            pass
    except Exception as exc:  # noqa: BLE001 - fan-out is best-effort
        logger.warning("kafka publish degraded topic=%s: %s", topic, exc)


def _attr(key: str, value: str) -> dict:
    """OTLP attribute entry the Flink parsers understand."""
    return {"key": key, "value": {"stringValue": str(value)}}


def _resource_attributes(slug: str, entity_id: str,
                         extra: dict | None = None) -> list[dict]:
    """Resource attributes shared by every OTLP envelope we emit.

    service.name drives entityId in BOTH Flink jobs
    (EntityResolutionJob + FeatureStoreJob); omniwatch.workspace preserves
    the workspace routing that the raw OTLP path would otherwise lose.
    """
    attrs = [
        _attr("service.name", entity_id),
        _attr("service.instance.id", entity_id),
        _attr("omniwatch.workspace", slug),
    ]
    for key, value in (extra or {}).items():
        attrs.append(_attr(key, value))
    return attrs


def _fan_out(slug: str, body: Any) -> None:
    """Rejoin Dataflow.md: every accepted point also hits the raw topics.

    OTLP-shaped JSON per point (NOT flat normalized dicts): both Flink jobs
    parse raw OTLP (resourceMetrics/resourceLogs/resourceSpans with
    service.name resource attributes, gauge dataPoints with asDouble,
    timeUnixNano as int nanos). Flat dicts go ONLY to
    omniwatch.security.events, whose Python classifier reads
    attack_type/event_type/entity_id/source_ip/message keys.
    """
    now_ms = int(time.time() * 1000)
    now_nanos = now_ms * 1_000_000

    def nanos(raw: Optional[str]) -> int:
        try:
            return _epoch_ms(raw, now_ms) * 1_000_000
        except Exception:  # noqa: BLE001 - fall back to ingest time
            return now_nanos

    for m in body.metrics:
        ts_nanos = nanos(m.timestamp)
        tags = dict(m.tags or {})
        _publish_raw(RAW_TOPIC_METRICS, {
            "resourceMetrics": [{
                "resource": {"attributes": _resource_attributes(
                    slug, body.entity_id, tags)},
                "scopeMetrics": [{
                    "metrics": [{
                        "name": m.metric_name,
                        "gauge": {"dataPoints": [{
                            "timeUnixNano": ts_nanos,
                            "asDouble": float(m.value),
                            "attributes": [
                                _attr("metric.source_type",
                                      m.source_type or "performance"),
                            ],
                        }]},
                    }],
                }],
            }],
        })
    for entry in body.logs:
        ts_nanos = nanos(entry.timestamp)
        level = entry.log_level or "INFO"
        service = entry.service_name or body.entity_id
        for topic in _raw_topics(body.telemetry_type):
            if topic == TOPIC_SECURITY_EVENTS:
                # Python classifier contract: flat dict, never OTLP.
                _publish_raw(topic, {
                    "entity_id": body.entity_id,
                    "entity_type": body.entity_type,
                    "event_type": body.telemetry_type,
                    "attack_type": body.telemetry_type,
                    "message": entry.message,
                    "severity": level,
                    "service_name": service,
                    "trace_id": entry.trace_id or "",
                    "source_ip": "",
                    "timestamp": ts_nanos // 1_000_000,
                    "workspace_slug": slug,
                })
                continue
            _publish_raw(topic, {
                "resourceLogs": [{
                    "resource": {"attributes": _resource_attributes(
                        slug, body.entity_id)},
                    "scopeLogs": [{
                        "logRecords": [{
                            "timeUnixNano": ts_nanos,
                            "severityText": level,
                            "body": {"stringValue": entry.message},
                            "attributes": [
                                _attr("service.name", service),
                                _attr("trace_id",
                                      entry.trace_id or ""),
                                _attr("omniwatch.telemetry_type",
                                      body.telemetry_type),
                            ],
                        }],
                    }],
                }],
            })
    for s in body.traces:
        ts_nanos = nanos(s.timestamp)
        try:
            duration_nanos = int(float(s.duration_ms or 0.0) * 1_000_000)
        except Exception:  # noqa: BLE001 - bad duration -> zero span
            duration_nanos = 0
        service = s.service_name or body.entity_id
        for topic in _raw_topics(body.telemetry_type):
            _publish_raw(topic, {
                "resourceSpans": [{
                    "resource": {"attributes": _resource_attributes(
                        slug, body.entity_id)},
                    "scopeSpans": [{
                        "spans": [{
                            "traceId": s.trace_id,
                            "spanId": s.span_id,
                            "parentSpanId": s.parent_span_id or "",
                            "name": s.operation or "",
                            "startTimeUnixNano": ts_nanos,
                            "endTimeUnixNano": ts_nanos + duration_nanos,
                            "status": {"code": "STATUS_CODE_OK"},
                            "attributes": [
                                _attr("service.name", service),
                                _attr("omniwatch.telemetry_type",
                                      body.telemetry_type),
                            ],
                        }],
                    }],
                }],
            })
    if body.telemetry_type in META_TYPES and not body.logs and not body.traces:
        _publish_raw(RAW_TOPIC_EVENTS, {
            "resourceLogs": [{
                "resource": {"attributes": _resource_attributes(
                    slug, body.entity_id)},
                "scopeLogs": [{
                    "logRecords": [{
                        "timeUnixNano": now_nanos,
                        "severityText": "INFO",
                        "body": {"stringValue":
                                 f"{body.telemetry_type} envelope"},
                        "attributes": [
                            _attr("omniwatch.telemetry_type",
                                  body.telemetry_type),
                            _attr("entity.type", body.entity_type),
                        ],
                    }],
                }],
            }],
        })


def _epoch_ms(raw: Optional[str], default_ms: int) -> int:
    try:
        dt = _parse_ts(raw)
        return int(dt.timestamp() * 1000)
    except Exception:  # noqa: BLE001 - fall back to ingest time
        return default_ms


_LOGS_LOCK = threading.Lock()
_LOGS: dict[str, Any] = {}


def _record_live_log(
    slug: str, entity_id: str, level: str, message: str,
    timestamp: Optional[str] = None,
) -> None:
    """Append to the workspace's newest-150 ring (no ClickHouse involved)."""
    with _LOGS_LOCK:
        ring = _LOGS.get(slug)
        if ring is None:
            ring = _LOGS[slug] = deque(maxlen=LIVE_LOG_MAX)
        ring.append({
            "timestamp": timestamp or utcnow().isoformat(),
            "level": level or "INFO",
            "entity": entity_id,
            "message": message,
        })


def _recent_logs(slug: str) -> list[dict]:
    with _LOGS_LOCK:
        return list(_LOGS.get(slug, []))


# Token-bucket limits: (route group, window_s, max_hits).
RATE_INGEST = ("ingest", 60.0, 60)
RATE_ACTIVITY = ("activity", 60.0, 120)
_RATE_BUCKETS: dict[tuple[str, str], list] = {}
_RATE_LOCK = threading.Lock()


def _rate_limit(group: str, window_s: float, max_hits: int,
                client_ip: str) -> Optional[int]:
    """None when allowed; retry-after seconds when over the limit."""
    now = time.monotonic()
    key = (group, client_ip)
    with _RATE_LOCK:
        bucket = _RATE_BUCKETS.get(key)
        if bucket is None or now - bucket[0] >= window_s:
            _RATE_BUCKETS[key] = [now, 1]
            return None
        bucket[1] += 1
        if bucket[1] <= max_hits:
            return None
        return int(window_s - (now - bucket[0])) + 1


def _client_ip(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def create_app() -> FastAPI:
    application = FastAPI(title="OmniWatch Importer Service", version="1.0.0")

    @application.middleware("http")
    async def rate_limit_middleware(request: Request, call_next: Any) -> Any:
        path = request.url.path
        if path.startswith("/ingest"):
            group, window_s, max_hits = RATE_INGEST
        elif path.startswith("/activity"):
            group, window_s, max_hits = RATE_ACTIVITY
        else:
            return await call_next(request)
        retry_after = _rate_limit(
            group, window_s, max_hits, _client_ip(request))
        if retry_after is not None:
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "rate limit exceeded"},
                headers={"Retry-After": str(retry_after)},
            )
        return await call_next(request)

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok", "service": "importer"}

    @application.get("/activity/recent-logs/by-workspace/{workspace_id}")
    def recent_logs(
        workspace_id: str,
        authorization: str = Header(default=""),
    ) -> dict:
        """Newest-150 log entries, straight from memory (no ClickHouse)."""
        if not authorization:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing bearer token",
            )
        slug = _check_workspace_owner(workspace_id, authorization)
        if slug is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="workspace not found or access denied",
            )
        return {"workspace_slug": slug, "logs": _recent_logs(slug)}

    @application.get(
        "/activity/by-workspace/{workspace_id}",
        response_model=ActivityResponse,
    )
    def activity(
        workspace_id: str,
        authorization: str = Header(default=""),
    ) -> ActivityResponse:
        if not authorization:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing bearer token",
            )
        slug = _check_workspace_owner(workspace_id, authorization)
        if slug is None:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="workspace not found or access denied",
            )
        return ActivityResponse(
            workspace_slug=slug, activity=_get_activity(slug))

    @application.post("/ingest", response_model=IngestResponse,
                       status_code=status.HTTP_202_ACCEPTED)
    def ingest(
        body: IngestBody,
        authorization: str = Header(default=""),
    ) -> IngestResponse:
        if not authorization.startswith("Bearer "):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="missing bearer token",
            )
        if body.telemetry_type not in ALL_TYPES:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                detail=f"unknown telemetry_type (one of {sorted(ALL_TYPES)})",
            )
        resolved = verify_token(authorization[len("Bearer "):].strip())
        slug: str = resolved["workspace_slug"]
        db = _workspace_db(slug)

        metric_rows: list[list] = []
        log_rows: list[list] = []
        trace_rows: list[list] = []
        ttype = body.telemetry_type

        if ttype in METRIC_TYPES:
            for m in body.metrics:
                metric_rows.append([
                    body.entity_id, body.entity_type, m.metric_name,
                    m.value, m.tags, m.source_type, _parse_ts(m.timestamp),
                ])
        elif ttype in TRACE_TYPES:
            for s in body.traces:
                trace_rows.append([
                    s.trace_id, s.span_id, s.parent_span_id,
                    s.service_name or body.entity_id, s.operation,
                    s.duration_ms, _parse_ts(s.timestamp),
                ])
        else:  # logs + log-ish security types + metadata/state envelope
            level_default = "ERROR" if ttype in {
                "siem", "security_alerts"} else "INFO"
            for entry in body.logs:
                log_rows.append([
                    body.entity_id, entry.log_level or level_default,
                    entry.message, entry.service_name or body.entity_id,
                    entry.trace_id, _parse_ts(entry.timestamp),
                ])
            if ttype in META_TYPES and not body.logs:
                import json

                log_rows.append([
                    body.entity_id, "INFO",
                    json.dumps({"telemetry_type": ttype,
                                "entity_type": body.entity_type}),
                    body.entity_id, "", utcnow(),
                ])

        try:
            client = _ch_client(db)
            if metric_rows:
                client.insert("metrics",
                              [tuple(r) for r in metric_rows],
                              column_names=["entity_id", "entity_type",
                                            "metric_name", "value", "tags",
                                            "source_type", "timestamp"])
            if log_rows:
                client.insert("logs",
                              [tuple(r) for r in log_rows],
                              column_names=["entity_id", "log_level",
                                            "message", "service_name",
                                            "trace_id", "timestamp"])
            if trace_rows:
                client.insert("traces",
                              [tuple(r) for r in trace_rows],
                              column_names=["trace_id", "span_id",
                                            "parent_span_id", "service_name",
                                            "operation", "duration_ms",
                                            "timestamp"])
        except Exception as exc:  # noqa: BLE001 - storage failure -> 503
            logger.warning("clickhouse insert failed db=%s: %s", db, exc)
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="storage unavailable",
            ) from exc

        logger.info("ingest type=%s ws=%s m=%d l=%d t=%d",
                    ttype, slug, len(metric_rows), len(log_rows),
                    len(trace_rows))
        if metric_rows or log_rows or trace_rows:
            _record_seen(slug, body.entity_id, ttype)
        for row in log_rows:
            _record_live_log(slug, row[0], row[1], row[2],
                             row[5].isoformat()
                             if hasattr(row[5], "isoformat") else None)
        _fan_out(slug, body)
        return IngestResponse(
            workspace_slug=slug,
            inserted={"metrics": len(metric_rows), "logs": len(log_rows),
                      "traces": len(trace_rows)},
        )

    return application


app = create_app()


def main() -> None:
    """Run the importer service via uvicorn."""
    import uvicorn

    if INTERNAL_SECRET == "dev-only-internal-secret":
        logger.warning(
            "IMPORTER_INTERNAL_SECRET unset — using dev-only default")
    uvicorn.run("importer.main:app", host="0.0.0.0",
                port=IMPORTER_PORT, reload=False)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    main()
