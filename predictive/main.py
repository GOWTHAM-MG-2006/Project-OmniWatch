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
import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI

logger = logging.getLogger("omniwatch.predictive.main")

# Configure logging at module level so messages are visible when uvicorn
# imports this module (not just when run via `python -m predictive.main`).
# Without this, INFO/ERROR logs are silently dropped by Python's lastResort.
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)

# Module-level stop event for the detection daemon thread
_stop_event: threading.Event = threading.Event()

# --------------------------------------------------------------------------- #
# Detection daemon — polls ClickHouse feature_vectors and runs DetectorEngine
# --------------------------------------------------------------------------- #

def _detection_worker(
    engine: Any | None = None,
    feature_reader: Any | None = None,
    poll_interval: float = 5.0,
    stop_event: threading.Event | None = None,
) -> None:
    """Background daemon that polls ClickHouse for fresh feature vectors and
    runs them through the DetectorEngine.

    Discovers entities via ``FeatureReader.list_entities()``, reads the most
    recent feature vectors for each entity, and passes each one to
    ``DetectorEngine.process_message()``.  Confirmed anomalies are logged via
    ``log_detection_event()`` (which updates last-anomaly state on /health).

    The engine's ``AnomalyProducer.publish()`` handles Kafka + ClickHouse
    writes internally — the worker only orchestrates.

    **Incremental processing:**  The worker tracks the last-processed
    ``window_start`` timestamp per entity so each poll only processes vectors
    that have arrived *since the previous poll*.  On the very first poll for
    an entity, it reads a larger initial batch (for cold-start training)
    then remembers the latest timestamp so subsequent polls only see new data.
    """
    from predictive.config.settings import Settings
    from predictive.detector_engine import DetectorEngine
    from predictive.feature_reader import FeatureReader

    logger.info("predictive detection loop started")

    settings = Settings.from_env()
    if engine is None:
        engine = DetectorEngine(settings)
    else:
        engine = engine
    if feature_reader is None:
        feature_reader = FeatureReader(settings)
    if stop_event is None:
        stop_event = _stop_event

    # Bind engine for live introspection on /health
    bind_detector_engine(engine)

    # Track last-processed window_start per entity so we only score NEW vectors.
    # On the first poll for an entity, we read a larger batch to give the
    # cold-start buffer enough samples to train on (30+).
    #
    # Score ONLY 5-minute windows: the anomaly signal (latency P50/P95 spiking in
    # the simulation scenarios) is carried by the "5m" aggregation rows.  The
    # 1m rows are emitted at a higher cadence with earlier window_start values
    # (and near-zero baseline latency), so a cursor that advances to the newest
    # 1m row's window_start would permanently skip the older 5m anomaly rows.
    # Filtering at the source keeps the cursor on the signal-bearing rows.
    _FEATURE_WINDOW_SIZE = "5m"
    _INITIAL_BATCH_SIZE = 500
    _POLL_BATCH_SIZE = 500
    _last_processed: dict[str, str] = {}

    while not stop_event.is_set():
        try:
            entities = feature_reader.list_entities()
            if not entities:
                logger.debug("no entities found in feature_vectors — idle")
            else:
                logger.debug("discovered %d entities: %s", len(entities), entities[:5])

            for entity_id in entities:
                if stop_event.is_set():
                    break
                try:
                    # Read only vectors newer than the last-processed timestamp.
                    # First poll for an entity: last_processed is absent → read
                    # a large initial batch for cold-start training.
                    since = _last_processed.get(entity_id)
                    batch_size = _INITIAL_BATCH_SIZE if since is None else _POLL_BATCH_SIZE
                    features = feature_reader.read_features(
                        entity_id,
                        limit=batch_size,
                        start=since,
                        window_size=_FEATURE_WINDOW_SIZE,
                    )
                    if not features:
                        continue

                    for fv in features:
                        if stop_event.is_set():
                            break
                        signal = engine.process_message(fv)
                        if signal is not None:
                            log_detection_event(
                                detector_name=signal.get("detector_name", "unknown"),
                                entity_id=signal.get("entity_id", "unknown"),
                                metric_name=signal.get("metric_name", "unknown"),
                                score=signal.get("anomaly_score", 0.0),
                            )

                    # Update last-processed timestamp to the newest vector we saw
                    last_ts = features[-1].get("window_start", "")
                    if last_ts:
                        _last_processed[entity_id] = str(last_ts)

                except Exception as exc:
                    logger.error(
                        "error processing entity %s: %s",
                        entity_id,
                        exc,
                        exc_info=True,
                    )
        except Exception:
            logger.error("detection loop error — retrying", exc_info=True)

        stop_event.wait(timeout=poll_interval)

    # Cleanup
    try:
        engine.close()
    except Exception:
        logger.debug("error closing detector engine", exc_info=True)
    try:
        feature_reader.close()
    except Exception:
        logger.debug("error closing feature reader", exc_info=True)
    logger.info("predictive detection loop stopped")


# --------------------------------------------------------------------------- #
# App
# --------------------------------------------------------------------------- #

@asynccontextmanager
async def lifespan(app: FastAPI):
    """FastAPI lifespan — starts the detection daemon on bootstrap."""
    logger.info("Starting OmniWatch Predictive Intelligence Layer")
    _stop_event.clear()
    _detection_thread = threading.Thread(
        target=_detection_worker,
        name="omniwatch-detection-loop",
        daemon=True,
    )
    _detection_thread.start()
    yield
    _stop_event.set()
    _detection_thread.join(timeout=10.0)
    logger.info("OmniWatch Predictive Intelligence Layer stopped")


app = FastAPI(
    title="OmniWatch Predictive Intelligence",
    description="Phase 6 — Anomaly detection + security signal classifier health endpoint",
    version="0.1.0",
    lifespan=lifespan,
)

# Module-level state — last anomaly published by the detector engine
_last_anomaly: str = "none"
_last_anomaly_time: str = ""

# Per-detector state registry — keyed by detector_name.  The DetectorEngine
# (T7) populates this via the public setters below so the /health endpoint can
# surface per-detector training / scoring state.  Missing fields degrade to
# ``None`` (null in JSON) rather than crashing.
_detectors: dict[str, dict[str, Any]] = {}

# Engine-level flags surfaced on /health (None = not yet observed).
_fusion_calibrated: bool | None = None
_drift_detected: bool | None = None
_k8s_cooldown: bool | None = None

# Optional bound DetectorEngine for live state introspection.
_engine: Any = None


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
# Structured JSON logging — detection events carry detector provenance
# --------------------------------------------------------------------------- #

class JsonLogFormatter(logging.Formatter):
    """Format a log record as a single JSON line.

    Standard record fields are emitted as ``timestamp`` / ``logger`` /
    ``level`` / ``message``; any custom ``extra`` kwargs (e.g. detector
    provenance) are merged verbatim so log aggregators can index on
    ``detector_name``, ``entity_id``, ``metric_name`` and ``score``.
    """

    _RESERVED = frozenset(
        {
            "name",
            "msg",
            "args",
            "levelname",
            "levelno",
            "pathname",
            "filename",
            "module",
            "exc_info",
            "exc_text",
            "stack_info",
            "lineno",
            "funcName",
            "created",
            "msecs",
            "relativeCreated",
            "thread",
            "threadName",
            "processName",
            "process",
            "taskName",
            "message",
            "asctime",
            # keys we normalise ourselves:
            "logger",
            "level",
            "timestamp",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        """Serialise the record (including extra fields) to one JSON line."""
        payload: dict[str, Any] = {
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S%z"),
            "logger": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in self._RESERVED:
                continue
            payload[key] = value
        return json.dumps(payload, default=str, ensure_ascii=False)


def _install_json_handler(logger_: logging.Logger) -> None:
    """Attach a JSON ``StreamHandler`` to *logger_* (idempotent)."""
    for handler in logger_.handlers:
        if getattr(handler, "_omniwatch_json", False):
            return
    handler = logging.StreamHandler()
    handler.setFormatter(JsonLogFormatter())
    handler._omniwatch_json = True  # type: ignore[attr-defined]
    logger_.addHandler(handler)
    logger_.setLevel(logging.INFO)
    logger_.propagate = True


detection_logger = logging.getLogger("omniwatch.predictive.detection")
_install_json_handler(detection_logger)


# --------------------------------------------------------------------------- #
# Detector-state helpers (recorded + live-introspected)
# --------------------------------------------------------------------------- #

def _introspect_engine() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Best-effort live read of a bound DetectorEngine.

    Returns ``(detectors, flags)`` built from whatever state the engine
    exposes.  T7 / T8 may not expose every field yet — values that are not
    present are omitted so the caller keeps its defensive defaults.  Never
    raises.
    """
    detectors: dict[str, dict[str, Any]] = {}
    flags: dict[str, Any] = {}
    if _engine is None:
        return detectors, flags
    try:
        detector = getattr(_engine, "_detector", None)
        if detector is not None:
            name = getattr(detector, "__class__", type(None)).__name__ or "detector"
            entry: dict[str, Any] = {}
            train_count = getattr(detector, "_train_count", None)
            if train_count is not None:
                entry["n_samples"] = int(train_count)
            is_trained = getattr(_engine, "_is_trained", None)
            if is_trained is not None:
                entry["trained"] = bool(is_trained)
            elif train_count is not None:
                entry["trained"] = int(train_count) > 0
            if entry:
                detectors[name] = entry

        # Fusion calibration state (T2's ColdStartAwareFusion exposes `.fitted`)
        fusion = getattr(_engine, "_fusion", None)
        if fusion is not None:
            fitted = getattr(fusion, "fitted", None)
            if fitted is not None:
                flags["fusion_calibrated"] = bool(fitted)
        elif getattr(_engine, "_fusion_calibrated", None) is not None:
            flags["fusion_calibrated"] = bool(_engine._fusion_calibrated)

        # Drift state (T3's ADWINDriftDetector exposes `.needs_retrain`)
        drift = getattr(_engine, "_drift", None)
        if drift is not None:
            needs_retrain = getattr(drift, "needs_retrain", None)
            if needs_retrain is not None:
                flags["drift_detected"] = bool(needs_retrain)
        elif getattr(_engine, "_drift_detected", None) is not None:
            flags["drift_detected"] = bool(_engine._drift_detected)

        # K8s cooldown flag
        k8s = getattr(_engine, "_k8s_cooldown", None)
        if k8s is not None:
            flags["k8s_cooldown"] = bool(k8s)
    except Exception:
        logger.debug("engine state introspection failed", exc_info=True)
    return detectors, flags


def _collect_state() -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    """Merge recorded + live-introspected state for the /health response.

    Recorded values win; introspection only fills fields that have not been
    observed yet.  Every detector entry always carries the full field set.
    """
    merged: dict[str, dict[str, Any]] = {
        name: {
            "trained": entry.get("trained"),
            "n_samples": entry.get("n_samples"),
            "last_score": entry.get("last_score"),
        }
        for name, entry in _detectors.items()
    }
    flags: dict[str, Any] = {
        "fusion_calibrated": _fusion_calibrated,
        "drift_detected": _drift_detected,
        "k8s_cooldown": _k8s_cooldown,
    }
    introspected_detectors, introspected_flags = _introspect_engine()
    for name, entry in introspected_detectors.items():
        target = merged.setdefault(
            name, {"trained": None, "n_samples": None, "last_score": None}
        )
        for key in ("trained", "n_samples"):
            if target.get(key) is None and entry.get(key) is not None:
                target[key] = entry[key]
    for key, value in introspected_flags.items():
        if flags.get(key) is None and value is not None:
            flags[key] = value
    return merged, flags


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
            "last_anomaly": "<entity_id> at <ISO timestamp>" | "none",
            "detectors": {
                "<detector_name>": {
                    "trained": true | false | null,
                    "n_samples": int | null,
                    "last_score": float | null,
                },
                ...
            },
            "fusion_calibrated": true | false | null,
            "drift_detected": true | false | null,
            "k8s_cooldown": true | false | null
        }

    Fields the detector engine has not yet populated are surfaced as ``null``
    rather than crashing.
    """
    kafka_ok = _check_kafka()
    clickhouse_ok = _check_clickhouse()
    model_ok = _check_model_loaded()

    all_ok = kafka_ok and clickhouse_ok and model_ok

    detector_state, flags = _collect_state()

    return {
        "status": "healthy" if all_ok else "degraded",
        "kafka": kafka_ok,
        "clickhouse": clickhouse_ok,
        "model_loaded": model_ok,
        "last_anomaly": _last_anomaly or "none",
        "detectors": detector_state,
        **flags,
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


def record_detector_state(
    detector_name: str,
    *,
    trained: bool | None = None,
    n_samples: int | None = None,
    last_score: float | None = None,
) -> None:
    """Record observed state for a single detector.

    Each call updates only the fields provided; unspecified fields keep their
    previous value (``None`` until first observed).  This is the public hook
    ``DetectorEngine`` (T7) uses to surface per-detector training / scoring
    state on the health endpoint.
    """
    entry = _detectors.setdefault(detector_name, {})
    if trained is not None:
        entry["trained"] = bool(trained)
    if n_samples is not None:
        entry["n_samples"] = int(n_samples)
    if last_score is not None:
        entry["last_score"] = float(last_score)


def record_engine_state(
    *,
    fusion_calibrated: bool | None = None,
    drift_detected: bool | None = None,
    k8s_cooldown: bool | None = None,
) -> None:
    """Record engine-level flags surfaced by the health endpoint."""
    global _fusion_calibrated, _drift_detected, _k8s_cooldown  # noqa: PLW0603
    if fusion_calibrated is not None:
        _fusion_calibrated = bool(fusion_calibrated)
    if drift_detected is not None:
        _drift_detected = bool(drift_detected)
    if k8s_cooldown is not None:
        _k8s_cooldown = bool(k8s_cooldown)


def bind_detector_engine(engine: Any) -> None:
    """Bind a DetectorEngine for live state introspection on /health.

    The endpoint defensively reads whatever state the engine exposes; fields
    it does not (yet) expose keep their registry / default values.
    """
    global _engine  # noqa: PLW0603
    _engine = engine


def log_detection_event(
    *,
    detector_name: str,
    entity_id: str,
    metric_name: str,
    score: float,
) -> None:
    """Record + log a detection event with structured JSON provenance.

    Updates the last-anomaly record and the per-detector ``last_score``
    surfaced by ``/health``, then emits a single JSON log line carrying
    detector provenance.  Called by ``DetectorEngine`` after a confirmed
    anomaly is published.
    """
    set_last_anomaly(entity_id)
    entry = _detectors.setdefault(detector_name, {})
    entry["last_score"] = float(score)
    detection_logger.info(
        "detection_event",
        extra={
            "detector_name": detector_name,
            "entity_id": entity_id,
            "metric_name": metric_name,
            "score": float(score),
        },
    )


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
