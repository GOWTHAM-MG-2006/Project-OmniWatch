"""
OmniWatch — Continuous Learning Layer
Component: Learning Service
Phase: 11
Purpose: FastAPI application orchestrating the continuous learning pipeline:
         FeedbackLoopProcessor (Kafka consumer thread), PatternMiner (15-min
         scheduler thread), and RecommendationEngine (lazy per-request).
         Exposes health, recommendation, and pattern query endpoints.
Inputs: ActionResult Kafka messages (via FeedbackLoopProcessor),
        ClickHouse knowledge_base/incidents tables, Neo4j pattern graph
Outputs: GET /health, GET /api/recommendations/{entity_id}, GET /api/patterns
"""

from __future__ import annotations

import logging
import os
import sys
import threading
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException

from learning.feedback_loop import FeedbackLoopProcessor
from learning.pattern_mining import PatternMiner
from learning.recommendation_engine import RecommendationEngine

logger = logging.getLogger("omniwatch.learning.service")

# ---------------------------------------------------------------------------
# Environment-driven configuration
# ---------------------------------------------------------------------------

CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "omniwatch")

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID = os.environ.get("KAFKA_LEARNING_GROUP_ID", "omniwatch-learning-group")
KAFKA_AUTO_OFFSET_RESET = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "earliest")

PATTERN_MINING_INTERVAL = int(os.environ.get("PATTERN_MINING_INTERVAL", "900"))
LEARNING_API_PORT = int(os.environ.get("LEARNING_API_PORT", "8030"))

# ---------------------------------------------------------------------------
# Module-level state — managed by lifespan
# ---------------------------------------------------------------------------

_feedback_processor: FeedbackLoopProcessor | None = None
_pattern_miner: PatternMiner | None = None
_recommendation_engine: RecommendationEngine | None = None
_feedback_thread: threading.Thread | None = None
_pattern_thread: threading.Thread | None = None


def _get_recommendation_engine() -> RecommendationEngine:
    """Return the RecommendationEngine, creating it lazily on first call."""
    global _recommendation_engine
    if _recommendation_engine is None:
        _recommendation_engine = RecommendationEngine(
            clickhouse_config={
                "host": CLICKHOUSE_HOST,
                "port": CLICKHOUSE_PORT,
                "database": CLICKHOUSE_DB,
                "username": CLICKHOUSE_USER,
                "password": CLICKHOUSE_PASSWORD,
            }
        )
    return _recommendation_engine


# ---------------------------------------------------------------------------
# FastAPI lifespan — start background processors, yield, stop on shutdown
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Lifespan context manager: start feedback loop + pattern miner threads,
    yield for request serving, then stop both processors on shutdown.
    """
    global _feedback_processor, _pattern_miner, _recommendation_engine
    global _feedback_thread, _pattern_thread

    # --- Startup ---
    # 1. FeedbackLoopProcessor — Kafka consumer in background thread
    _feedback_processor = FeedbackLoopProcessor(
        clickhouse_config={
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "database": CLICKHOUSE_DB,
            "username": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        },
        kafka_config={
            "bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "group_id": KAFKA_GROUP_ID,
            "auto_offset_reset": KAFKA_AUTO_OFFSET_RESET,
        },
    )
    _feedback_thread = threading.Thread(
        target=_feedback_processor.start,
        kwargs={"poll_interval": 1.0},
        daemon=True,
        name="feedback-loop-thread",
    )
    _feedback_thread.start()
    logger.info("feedback loop thread started")

    # 2. PatternMiner — scheduled mining in background thread
    _pattern_miner = PatternMiner(
        clickhouse_config={
            "host": CLICKHOUSE_HOST,
            "port": CLICKHOUSE_PORT,
            "database": CLICKHOUSE_DB,
            "username": CLICKHOUSE_USER,
            "password": CLICKHOUSE_PASSWORD,
        },
        neo4j_config={
            "uri": NEO4J_URI,
            "user": NEO4J_USER,
            "password": NEO4J_PASSWORD,
        },
    )
    _pattern_thread = threading.Thread(
        target=_pattern_miner.start,
        kwargs={"interval": PATTERN_MINING_INTERVAL},
        daemon=True,
        name="pattern-miner-thread",
    )
    _pattern_thread.start()
    logger.info(
        "pattern miner thread started interval=%ds",
        PATTERN_MINING_INTERVAL,
    )

    # 3. RecommendationEngine — lazy init, no thread needed

    logger.info("learning service started port=%d", LEARNING_API_PORT)

    yield

    # --- Shutdown ---
    if _feedback_processor is not None:
        try:
            _feedback_processor.stop()
        except Exception as exc:  # noqa: BLE001
            logger.error("error stopping feedback processor: %s", exc)
        _feedback_processor = None

    if _pattern_miner is not None:
        try:
            _pattern_miner.stop()
        except Exception as exc:  # noqa: BLE001
            logger.error("error stopping pattern miner: %s", exc)
        _pattern_miner = None

    if _recommendation_engine is not None:
        try:
            _recommendation_engine.close()
        except Exception as exc:  # noqa: BLE001
            logger.error("error closing recommendation engine: %s", exc)
        _recommendation_engine = None

    logger.info("learning service stopped")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Build the learning service FastAPI application."""
    application = FastAPI(
        title="OmniWatch Learning Service",
        version="1.0.0",
        lifespan=lifespan,
    )

    @application.get("/health")
    def health() -> dict[str, str]:
        return {"status": "ok"}

    @application.get("/api/recommendations/{entity_id}")
    def get_recommendations(entity_id: str) -> dict[str, Any]:
        """Return top-3 historically successful remediation actions for entity."""
        engine = _get_recommendation_engine()
        recommendations = engine.get_recommendations(entity_id)
        return {
            "entity_id": entity_id,
            "recommendations": recommendations,
            "count": len(recommendations),
        }

    @application.get("/api/patterns")
    def get_patterns() -> dict[str, Any]:
        """Return current mined patterns from Neo4j (one-shot query)."""
        if _pattern_miner is None:
            raise HTTPException(
                status_code=503,
                detail="pattern miner not initialized",
            )
        try:
            patterns = _pattern_miner.mine_patterns()
        except Exception as exc:
            logger.error("pattern query failed: %s", exc)
            raise HTTPException(
                status_code=500,
                detail="pattern query failed",
            ) from exc
        return {
            "patterns": patterns,
            "count": len(patterns),
        }

    return application


# ---------------------------------------------------------------------------
# Default app instance (for uvicorn invocation)
# ---------------------------------------------------------------------------

app = create_app()


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the learning service via uvicorn."""
    import uvicorn

    uvicorn.run(
        "learning.learning_service:app",
        host="0.0.0.0",
        port=LEARNING_API_PORT,
        reload=False,
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    main()
