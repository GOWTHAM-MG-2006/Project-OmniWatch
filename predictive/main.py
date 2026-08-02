"""
OmniWatch — Predictive Intelligence Layer
Component: FastAPI Health Server
Phase: 6
Purpose: Expose a /health endpoint reporting component status (Kafka, ClickHouse, model, last anomaly)
Inputs: Health check requests
Outputs: JSON health status with component-level reachability
"""

from __future__ import annotations

import glob
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI

logger = logging.getLogger("omniwatch.predictive.main")

# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #

app = FastAPI(
    title="OmniWatch Predictive Intelligence",
    description="Phase 6 — Anomaly detection + security signal classifier health endpoint",
    version="0.1.0",
)

# Module-level state — last anomaly published by the detector engine
_last_anomaly: str = "none"
_last_anomaly_time: str = ""


# --------------------------------------------------------------------------- #
# Component health checks (all wrapped in try/except → never crash)
# --------------------------------------------------------------------------- #

def _check_kafka() -> bool:
    """Lightweight Kafka reachability check.  Returns False on any failure."""
    try:
        from predictive.config.settings import Settings

        settings = Settings.from_env()
        # Lazy import — kafka-python-ng fails on Python 3.14 at module level
        from kafka import KafkaProducer  # noqa: F811

        producer = KafkaProducer(
            bootstrap_servers=settings.kafka_bootstrap_servers,
            request_timeout_ms=2_000,
            max_block_ms=2_000,
        )
        # Flush with a short timeout — raises if broker is unreachable
        producer.flush(timeout=2.0)
        producer.close(timeout=2.0)
        return True
    except Exception:
        logger.debug("kafka health check failed", exc_info=True)
        return False


def _check_clickhouse() -> bool:
    """ClickHouse reachability via the storage client health check."""
    try:
        from storage.clickhouse.client import ClickHouseClient
        from storage.config import StorageConfig

        cfg = StorageConfig.from_env()
        client = ClickHouseClient(config=cfg)
        healthy = client.health_check()
        client.close()
        return healthy
    except Exception:
        logger.debug("clickhouse health check failed", exc_info=True)
        return False


def _check_model_loaded() -> bool:
    """Check if a trained model file exists or the detector has been trained."""
    # Look for any persisted model files (joblib/pkl) in the predictive directory
    predictive_root = os.path.dirname(os.path.abspath(__file__))
    model_patterns = [
        os.path.join(predictive_root, "**", "*.joblib"),
        os.path.join(predictive_root, "**", "*.pkl"),
    ]
    for pattern in model_patterns:
        if glob.glob(pattern, recursive=True):
            return True

    # Also check environment variable hint (useful when model is loaded in-memory)
    if os.environ.get("PREDICTIVE_MODEL_LOADED", "").lower() in ("1", "true", "yes"):
        return True

    return False


# --------------------------------------------------------------------------- #
# Health endpoint
# --------------------------------------------------------------------------- #

@app.get("/health")
async def health() -> dict[str, Any]:
    """Return component health status.

    Response shape::

        {
            "status": "healthy" | "degraded",
            "kafka": true | false,
            "clickhouse": true | false,
            "model_loaded": true | false,
            "last_anomaly": "<entity_id> at <ISO timestamp>" | "none"
        }
    """
    kafka_ok = _check_kafka()
    clickhouse_ok = _check_clickhouse()
    model_ok = _check_model_loaded()

    all_ok = kafka_ok and clickhouse_ok and model_ok

    return {
        "status": "healthy" if all_ok else "degraded",
        "kafka": kafka_ok,
        "clickhouse": clickhouse_ok,
        "model_loaded": model_ok,
        "last_anomaly": _last_anomaly or "none",
    }


# --------------------------------------------------------------------------- #
# Public setter for detector engine to update last anomaly
# --------------------------------------------------------------------------- #

def set_last_anomaly(entity_id: str) -> None:
    """Update the module-level last anomaly record.

    Called by ``DetectorEngine`` after a successful publish so the health
    endpoint reflects the most recent detection.
    """
    global _last_anomaly, _last_anomaly_time  # noqa: PLW0603
    _last_anomaly_time = datetime.now(timezone.utc).isoformat()
    _last_anomaly = f"{entity_id} at {_last_anomaly_time}"


# --------------------------------------------------------------------------- #
# Entry-point for standalone uvicorn run
# --------------------------------------------------------------------------- #

if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )
    uvicorn.run(
        "predictive.main:app",
        host="0.0.0.0",
        port=8007,
        reload=False,
    )
