"""
OmniWatch — Telemetry Ingestion Layer
Component: Telemetry Router Routes
Phase: 2
Purpose: Additional utility endpoints for the Telemetry Router service
Inputs: HTTP requests
Outputs: JSON responses
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

# Add parent directory to path for kafka_bus imports
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from kafka_bus import KafkaConsumer, KafkaProducer, list_topics

# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------
router = APIRouter()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")


# ---------------------------------------------------------------------------
# Request/Response models
# ---------------------------------------------------------------------------
class PublishRequest(BaseModel):
    topic: str
    message: dict
    key: Optional[str] = None


class PublishResponse(BaseModel):
    status: str
    topic: str
    entity_id: Optional[str] = None


class TopicInfo(BaseModel):
    name: str
    is_active: bool


class TopicListResponse(BaseModel):
    topics: list[TopicInfo]
    total: int


# ---------------------------------------------------------------------------
# Publish endpoint
# ---------------------------------------------------------------------------
@router.post("/publish", response_model=PublishResponse)
async def publish_message(request: PublishRequest):
    """
    Publish a message to any valid Kafka topic.

    Useful for testing and manual event injection.
    """
    valid_topics = [
        "omniwatch.metrics.raw",
        "omniwatch.logs.raw",
        "omniwatch.traces.raw",
        "omniwatch.security.events",
        "omniwatch.anomalies.detected",
        "omniwatch.incidents.created",
        "omniwatch.remediation.actions",
    ]

    if request.topic not in valid_topics:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid topic: {request.topic}. Valid topics: {valid_topics}",
        )

    producer = KafkaProducer(KAFKA_BOOTSTRAP_SERVERS)

    # Add metadata
    enriched = {
        "published_at": datetime.now(timezone.utc).isoformat(),
        "source": "manual",
        **request.message,
    }

    entity_id = request.key or request.message.get("entity_id", "unknown")
    success = producer.send(request.topic, enriched, key=entity_id)
    producer.flush(timeout=5)

    if not success:
        raise HTTPException(status_code=500, detail="Failed to publish message")

    return PublishResponse(
        status="published",
        topic=request.topic,
        entity_id=entity_id,
    )


# ---------------------------------------------------------------------------
# Topics endpoint
# ---------------------------------------------------------------------------
@router.get("/topics", response_model=TopicListResponse)
async def get_topics():
    """List all active Kafka topics."""
    try:
        topics = list_topics(KAFKA_BOOTSTRAP_SERVERS)
        topic_info = [
            TopicInfo(name=t, is_active=True) for t in topics
        ]
        return TopicListResponse(topics=topic_info, total=len(topics))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------------------------------------------------------------
# Sample data endpoints (for testing)
# ---------------------------------------------------------------------------
@router.post("/sample/metrics")
async def send_sample_metric():
    """Send a sample metric message for testing."""
    producer = KafkaProducer(KAFKA_BOOTSTRAP_SERVERS)
    message = {
        "entity_id": "api-gateway",
        "entity_type": "API_NODE",
        "metric_name": "cpu_usage_percent",
        "metric_value": 45.2,
        "cloud_provider": "simulated-aws",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
    }
    success = producer.send("omniwatch.metrics.raw", message, key="api-gateway")
    producer.flush(timeout=5)
    return {"status": "sent" if success else "failed", "message": message}


@router.post("/sample/logs")
async def send_sample_log():
    """Send a sample log message for testing."""
    producer = KafkaProducer(KAFKA_BOOTSTRAP_SERVERS)
    message = {
        "entity_id": "postgresql-database",
        "entity_type": "DATABASE_NODE",
        "log_level": "info",
        "message": "Query executed SELECT products duration=23ms rows=150",
        "cloud_provider": "simulated-aws",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
    }
    success = producer.send("omniwatch.logs.raw", message, key="postgresql-database")
    producer.flush(timeout=5)
    return {"status": "sent" if success else "failed", "message": message}


@router.post("/sample/security")
async def send_sample_security():
    """Send a sample security event for testing."""
    producer = KafkaProducer(KAFKA_BOOTSTRAP_SERVERS)
    message = {
        "event_type": "BRUTE_FORCE_ATTEMPT",
        "entity_id": "auth-service",
        "entity_type": "AUTH_NODE",
        "source_ip": "10.0.0.1",
        "severity": "HIGH",
        "source_type": "security",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "test",
    }
    success = producer.send(
        "omniwatch.security.events", message, key="auth-service"
    )
    producer.flush(timeout=5)
    return {"status": "sent" if success else "failed", "message": message}
