"""
OmniWatch — Telemetry Ingestion Layer
Component: Telemetry Router Service
Phase: 2
Purpose: FastAPI service that receives telemetry from OTel Collector and routes to Kafka topics
Inputs: HTTP requests from OTel Collector (metrics, logs, traces, security)
Outputs: Structured messages to Kafka topics
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Add parent directory to path for kafka_bus imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka_bus import (
    ALL_TOPICS,
    KafkaProducer,
    create_topics,
)
from routes import router as routes_router

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------
app = FastAPI(
    title="OmniWatch Telemetry Router",
    description="Routes telemetry from OTel Collector to Kafka topics",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routes from routes.py
app.include_router(routes_router, prefix="/routes")

# Global producer instance
producer = KafkaProducer(KAFKA_BOOTSTRAP_SERVERS)


# ---------------------------------------------------------------------------
# Startup event
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup():
    """Create Kafka topics on startup."""
    print("[telemetry_router] Creating Kafka topics...")
    create_topics(KAFKA_BOOTSTRAP_SERVERS)
    print("[telemetry_router] Startup complete")


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------
@app.get("/health")
async def health():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "telemetry-router",
        "kafka_servers": KAFKA_BOOTSTRAP_SERVERS,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ---------------------------------------------------------------------------
# Ingest endpoints
# ---------------------------------------------------------------------------
@app.post("/ingest/metrics")
async def ingest_metrics(payload: dict):
    """
    Receive metrics and publish to omniwatch.metrics.raw topic.

    Expected payload: OTLP ExportMetricsServiceRequest or flat metric dict
    """
    try:
        # Add ingestion metadata
        enriched = {
            "source": "otelcol",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_type": "metrics",
            **payload,
        }

        # Extract entity_id for partitioning
        entity_id = payload.get("resource", {}).get("attributes", {}).get(
            "service.name", payload.get("entity_id", "unknown")
        )
        enriched["entity_id"] = entity_id

        success = producer.send("omniwatch.metrics.raw", enriched, key=entity_id)
        producer.flush(timeout=5)

        return {
            "status": "accepted" if success else "failed",
            "topic": "omniwatch.metrics.raw",
            "entity_id": entity_id,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/ingest/logs")
async def ingest_logs(payload: dict):
    """
    Receive logs and publish to omniwatch.logs.raw topic.

    Expected payload: OTLP ExportLogsServiceRequest or flat log dict
    """
    try:
        enriched = {
            "source": "otelcol",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_type": "logs",
            **payload,
        }

        entity_id = payload.get("resource", {}).get("attributes", {}).get(
            "service.name", payload.get("entity_id", "unknown")
        )
        enriched["entity_id"] = entity_id

        success = producer.send("omniwatch.logs.raw", enriched, key=entity_id)
        producer.flush(timeout=5)

        return {
            "status": "accepted" if success else "failed",
            "topic": "omniwatch.logs.raw",
            "entity_id": entity_id,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/ingest/traces")
async def ingest_traces(payload: dict):
    """
    Receive traces and publish to omniwatch.traces.raw topic.

    Expected payload: OTLP ExportTraceServiceRequest or flat trace dict
    """
    try:
        enriched = {
            "source": "otelcol",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_type": "traces",
            **payload,
        }

        entity_id = payload.get("resource", {}).get("attributes", {}).get(
            "service.name", payload.get("entity_id", "unknown")
        )
        enriched["entity_id"] = entity_id

        success = producer.send("omniwatch.traces.raw", enriched, key=entity_id)
        producer.flush(timeout=5)

        return {
            "status": "accepted" if success else "failed",
            "topic": "omniwatch.traces.raw",
            "entity_id": entity_id,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


@app.post("/ingest/security")
async def ingest_security(payload: dict):
    """
    Receive security events and publish to omniwatch.security.events topic.

    Expected payload: Security event dict with event_type, entity_id, etc.
    """
    try:
        enriched = {
            "source": "otelcol",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_type": "security",
            **payload,
        }

        entity_id = payload.get("entity_id", "unknown")
        enriched["entity_id"] = entity_id

        success = producer.send(
            "omniwatch.security.events", enriched, key=entity_id
        )
        producer.flush(timeout=5)

        return {
            "status": "accepted" if success else "failed",
            "topic": "omniwatch.security.events",
            "entity_id": entity_id,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ---------------------------------------------------------------------------
# Generic ingest endpoint
# ---------------------------------------------------------------------------
@app.post("/ingest/{topic}")
async def ingest_generic(topic: str, payload: dict):
    """
    Generic ingest endpoint — publishes to any valid Kafka topic.

    Validates topic name against known topics.
    """
    if topic not in ALL_TOPICS:
        return {
            "status": "error",
            "error": f"Unknown topic: {topic}. Valid topics: {ALL_TOPICS}",
        }

    try:
        enriched = {
            "source": "otelcol",
            "ingested_at": datetime.now(timezone.utc).isoformat(),
            "data_type": topic.split(".")[-1],
            **payload,
        }

        entity_id = payload.get("entity_id", "unknown")
        enriched["entity_id"] = entity_id

        success = producer.send(topic, enriched, key=entity_id)
        producer.flush(timeout=5)

        return {
            "status": "accepted" if success else "failed",
            "topic": topic,
            "entity_id": entity_id,
        }
    except Exception as e:
        return {"status": "error", "error": str(e)}


# ---------------------------------------------------------------------------
# Status endpoint
# ---------------------------------------------------------------------------
@app.get("/status")
async def status():
    """Return current ingestion status and topic information."""
    from kafka_bus import list_topics

    try:
        topics = list_topics(KAFKA_BOOTSTRAP_SERVERS)
    except Exception:
        topics = []

    return {
        "service": "telemetry-router",
        "kafka_bootstrap": KAFKA_BOOTSTRAP_SERVERS,
        "expected_topics": ALL_TOPICS,
        "active_topics": topics,
        "endpoints": [
            "POST /ingest/metrics",
            "POST /ingest/logs",
            "POST /ingest/traces",
            "POST /ingest/security",
            "GET /health",
            "GET /status",
        ],
    }
