"""
OmniWatch — Predictive Intelligence Layer
Component: Detector Engine
Phase: 6
Purpose: Orchestrate the full detection pipeline — consume feature vectors,
         run anomaly detection, apply adaptive thresholds, filter noise,
         enrich signals, and publish confirmed anomalies.
Inputs: Feature vectors (dict) from FeatureReader (ClickHouse) or Kafka
Outputs: AnomalySignal dicts to Kafka omniwatch.anomalies.detected + ClickHouse
"""

from __future__ import annotations

import logging
import threading
from typing import Any, Dict, List, Optional

import pandas as pd

from .adaptive_thresholder import AdaptiveThresholder
from .anomaly_detector import AnomalyDetector
from .anomaly_producer import AnomalyProducer
from .config.settings import Settings
from .noise_filter import NoiseFilter
from .signal_enricher import SignalEnricher

logger = logging.getLogger("omniwatch.predictive.detector_engine")


class DetectorEngine:
    """Orchestrate the full detection pipeline.

    Pipeline per feature vector::

        feature_vector → AnomalyDetector.detect()
                        → AdaptiveThresholder gate
                        → NoiseFilter.suppress()
                        → SignalEnricher.enrich()
                        → AnomalyProducer.publish()

    **Data source note (per C1 / Decision 10):**  Feature vectors are
    primarily read from ClickHouse via ``FeatureReader`` (Phase 4 / Task 10),
    NOT consumed from a Kafka topic.  The ``process()`` method therefore
    accepts a feature-vector dict directly — callers pass in whatever they
    fetched from the feature store.  The optional Kafka consumer in ``run()``
    exists for secondary / streaming mode only.

    **Cold start:**  If the detector has fewer than
    ``settings.predictive_cold_start_sample_count`` training samples, the
    engine accumulates incoming feature vectors into an internal buffer and
    calls ``detector.train()`` once enough samples exist.  Detection is
    skipped (returns ``None``) until training completes.

    **Thread safety:**  A ``threading.Lock`` guards all model access (train /
    detect) so the consumer loop and concurrent callers do not corrupt model
    state.
    """

    def __init__(
        self,
        settings: Optional[Settings] = None,
        *,
        detector: Optional[AnomalyDetector] = None,
        thresholder: Optional[AdaptiveThresholder] = None,
        noise_filter: Optional[NoiseFilter] = None,
        enricher: Optional[SignalEnricher] = None,
        producer: Optional[AnomalyProducer] = None,
    ) -> None:
        """Wire together the detection pipeline.

        Parameters
        ----------
        settings : Settings | None
            Shared settings.  ``None`` → ``Settings.from_env()``.
        detector : AnomalyDetector | None
            Injected for testability.  ``None`` → constructed from *settings*.
        thresholder : AdaptiveThresholder | None
            Injected for testability.  ``None`` → constructed.
        noise_filter : NoiseFilter | None
            Injected for testability.  ``None`` → constructed.
        enricher : SignalEnricher | None
            Injected for testability.  ``None`` → constructed.
        producer : AnomalyProducer | None
            Injected for testability.  ``None`` → constructed from *settings*.
        """
        self._settings = settings or Settings.from_env()
        self._detector = detector or AnomalyDetector(self._settings)
        self._thresholder = thresholder or AdaptiveThresholder()
        self._noise_filter = noise_filter or NoiseFilter()
        self._enricher = enricher or SignalEnricher()
        self._producer = producer or AnomalyProducer(self._settings)

        # Cold-start bookkeeping
        self._cold_start_count = self._settings.predictive_cold_start_sample_count
        self._training_buffer: List[Dict[str, float]] = []
        self._is_trained = False

        # Thread-safety lock for model access (train / detect)
        self._model_lock = threading.Lock()

        logger.info(
            "DetectorEngine initialised — cold_start_threshold=%d",
            self._cold_start_count,
        )

    # ------------------------------------------------------------------ #
    # Public API
    # ------------------------------------------------------------------ #

    def process_message(self, message: dict) -> Optional[Dict[str, Any]]:
        """Run the full detection pipeline on a single feature vector.

        Parameters
        ----------
        message : dict
            A feature vector dict ``{metric_name: value, ...}``.
            Must contain at least ``entity_id`` and numeric metric values.

        Returns
        -------
        dict | None
            The enriched ``AnomalySignal`` if an anomaly was detected,
            confirmed by thresholds and noise filtering, and published.
            ``None`` when the signal was suppressed, absent, or the engine
            is still in cold start.
        """
        entity_id = message.get("entity_id", "unknown")

        # ── Cold-start gate ──────────────────────────────────────────── #
        if not self._is_trained:
            self._training_buffer.append(message)
            if len(self._training_buffer) < self._cold_start_count:
                logger.debug(
                    "Cold start: buffering feature (%d/%d) — entity=%s",
                    len(self._training_buffer),
                    self._cold_start_count,
                    entity_id,
                )
                return None

            # Enough samples — train the detector
            with self._model_lock:
                df = pd.DataFrame(self._training_buffer)
                # Drop non-numeric / metadata columns
                numeric_cols = df.select_dtypes(include="number").columns.tolist()
                if not numeric_cols:
                    logger.warning(
                        "Cold-start training buffer has no numeric columns — "
                        "skipping train"
                    )
                    self._training_buffer.clear()
                    return None
                self._detector.train(df.loc[:, numeric_cols])
                self._is_trained = True
                self._training_buffer.clear()
                logger.info(
                    "Cold-start training complete — %d samples, %d numeric cols",
                    len(df),
                    len(numeric_cols),
                )

        # ── Detect ───────────────────────────────────────────────────── #
        with self._model_lock:
            signal = self._detector.detect(message)

        if signal is None:
            return None

        # ── Adaptive threshold gate ──────────────────────────────────── #
        metric_name = signal.get("metric_name", "")
        adaptive_threshold = self._thresholder.get_threshold(entity_id, metric_name)
        if adaptive_threshold is not None:
            if signal["anomaly_score"] < adaptive_threshold:
                logger.debug(
                    "Suppressed by adaptive threshold — entity=%s metric=%s "
                    "score=%.4f < threshold=%.4f",
                    entity_id,
                    metric_name,
                    signal["anomaly_score"],
                    adaptive_threshold,
                )
                return None

        # ── Update adaptive baseline (online) ────────────────────────── #
        # Feed the primary metric value back so the threshold adapts
        primary_value = message.get(metric_name)
        if primary_value is not None:
            try:
                self._thresholder.update(entity_id, metric_name, float(primary_value))
            except (TypeError, ValueError):
                pass

        # ── Noise filter ─────────────────────────────────────────────── #
        from datetime import datetime, timezone

        ts_str = signal.get("timestamp", "")
        try:
            ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            ts = datetime.now(timezone.utc)

        should_suppress = self._noise_filter.should_suppress(
            entity_id=entity_id,
            metric=metric_name,
            timestamp=ts,
            affected_neighbors=message.get("affected_neighbors", 0),
            source_type=signal.get("source_type", "performance"),
            anomaly_score=signal.get("anomaly_score", 0.0),
        )
        if should_suppress:
            logger.debug(
                "Suppressed by noise filter — entity=%s metric=%s",
                entity_id,
                metric_name,
            )
            return None

        # ── Enrich ───────────────────────────────────────────────────── #
        enriched_signal = self._enricher.enrich(signal)

        # ── Publish ──────────────────────────────────────────────────── #
        self._producer.publish(enriched_signal)

        logger.info(
            "Anomaly published — entity=%s score=%.4f enriched=%s",
            entity_id,
            enriched_signal.get("anomaly_score", 0.0),
            enriched_signal.get("enriched", False),
        )

        return enriched_signal

    def run(self, consumer=None) -> None:
        """Main consume-loop entry point (long-running).

        Parameters
        ----------
        consumer : KafkaConsumer | None
            An optional ``kafka.KafkaConsumer`` instance (kafka-python-ng).
            When ``None`` the engine creates one from *settings*.  The
            consumer reads from feature-vector topics; each message value is
            passed to ``process_message()``.

        This method blocks until ``KeyboardInterrupt`` or ``close()`` is
        called.  Tests should NOT call ``run()`` directly — test
        ``process_message()`` instead.
        """
        # Lazy import of Kafka consumer to avoid import-time failures
        if consumer is None:
            from kafka import KafkaConsumer as _KafkaConsumer

            consumer = _KafkaConsumer(
                "omniwatch.features.windowed",  # Phase 4 output topic
                bootstrap_servers=self._settings.kafka_bootstrap_servers,
                group_id=getattr(
                    self._settings, "kafka_group_id", "omniwatch-detector-group"
                ),
                auto_offset_reset="earliest",
                value_deserializer=lambda v: __import__("json").loads(
                    v.decode("utf-8")
                ),
            )

        logger.info("DetectorEngine.run() — consuming from Kafka")

        try:
            for msg in consumer:
                value = msg.value if isinstance(msg.value, dict) else None
                if value is None:
                    continue
                self.process_message(value)
        except KeyboardInterrupt:
            logger.info("DetectorEngine — interrupted, shutting down")
        finally:
            self.close()

    def close(self) -> None:
        """Close all owned resources (producer, enricher, etc.)."""
        try:
            self._producer.close()
        except Exception:
            logger.warning("Error closing producer", exc_info=True)
        logger.info("DetectorEngine closed")
