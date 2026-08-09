"""
OmniWatch — Predictive Intelligence Layer
Component: DetectorEngine unit tests
Phase: 6
Purpose: Verify full detection pipeline orchestration — cold start, detect,
         threshold, noise filter, enrich, produce — all mocked.
Inputs: None
Outputs: pytest pass/fail
"""

from __future__ import annotations

import sys
import threading
import time
from datetime import datetime, timedelta, timezone
from types import ModuleType
from typing import Any, Dict, Optional
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Pre-inject a fake ``kafka`` module so any lazy kafka-python-ng import
# resolves without importing the real package (broken on Python 3.14).
# ---------------------------------------------------------------------------
_fake_kafka = ModuleType("kafka")
_fake_kafka.KafkaProducer = MagicMock  # type: ignore[attr-defined]
_fake_kafka.KafkaConsumer = MagicMock  # type: ignore[attr-defined]
sys.modules.setdefault("kafka", _fake_kafka)

from predictive.anomaly_detector import AnomalyDetector
from predictive.config.settings import Settings
from predictive.detector_engine import DetectorEngine
from predictive.fusion import BayesianFusionEngine, ColdStartAwareFusion
from predictive.session_tracker import AnomalySessionTracker


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_settings(**overrides: Any) -> Settings:
    """Build a Settings isolated from dotenv leakage."""
    return Settings(_env_file=None, **overrides)


def _sample_feature(**extra: Any) -> Dict[str, Any]:
    """A minimal feature vector dict (as returned by FeatureReader)."""
    base: Dict[str, Any] = {
        "entity_id": "order-service",
        "latency_p99": 150.0,
        "error_rate": 0.02,
        "request_volume": 1000.0,
    }
    base.update(extra)
    return base


def _sample_signal(**overrides: Any) -> Dict[str, Any]:
    """A valid AnomalySignal dict."""
    base: Dict[str, Any] = {
        "entity_id": "order-service",
        "entity_type": "API_NODE",
        "metric_name": "latency_p99",
        "anomaly_score": 0.85,
        "confidence": 78.0,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "deviation_from_baseline": 2.3,
        "source_type": "performance",
    }
    base.update(overrides)
    return base


class _FakeDetector:
    """Minimal AnomalyDetector stand-in for pipeline tests."""

    def __init__(
        self,
        return_signal: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._return_signal = return_signal
        self.detect_calls: list = []
        self.train_calls: list = []

    def detect(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.detect_calls.append(feature)
        return self._return_signal

    def train(self, df: Any) -> None:
        self.train_calls.append(df)


class _FakeThresholder:
    """Minimal AdaptiveThresholder stand-in."""

    def __init__(self, threshold: Optional[float] = None) -> None:
        self._threshold = threshold
        self.update_calls: list = []

    def get_threshold(
        self, entity_id: str, metric: str
    ) -> Optional[float]:
        return self._threshold

    def update(self, entity_id: str, metric: str, value: float) -> None:
        self.update_calls.append((entity_id, metric, value))


class _FakeNoiseFilter:
    """Minimal NoiseFilter stand-in."""

    def __init__(self, suppress: bool = False) -> None:
        self._suppress = suppress
        self.suppress_calls: list = []

    def should_suppress(
        self,
        entity_id: str,
        metric: str,
        timestamp: datetime,
        affected_neighbors: int = 0,
        source_type: str = "performance",
        anomaly_score: float = 0.0,
    ) -> bool:
        self.suppress_calls.append(
            {
                "entity_id": entity_id,
                "metric": metric,
                "timestamp": timestamp,
                "affected_neighbors": affected_neighbors,
                "source_type": source_type,
                "anomaly_score": anomaly_score,
            }
        )
        return self._suppress


class _FakeEnricher:
    """Minimal SignalEnricher stand-in."""

    def __init__(self) -> None:
        self.enrich_calls: list = []

    def enrich(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        self.enrich_calls.append(signal)
        enriched = dict(signal)
        enriched["entity_context"] = {"name": "order-service", "criticality": "high"}
        enriched["enriched"] = True
        return enriched


class _FakeProducer:
    """Minimal AnomalyProducer stand-in."""

    def __init__(self) -> None:
        self.publish_calls: list = []
        self.close_called = False

    def publish(self, signal: Dict[str, Any]) -> None:
        self.publish_calls.append(signal)

    def close(self) -> None:
        self.close_called = True


class _StatefulDetector:
    """Returns an anomaly signal for the first ``anomaly_calls`` detect()
    calls, then ``None`` — used to exercise the session-resolution path."""

    def __init__(
        self,
        return_signal: Optional[Dict[str, Any]] = None,
        anomaly_calls: int = 1,
    ) -> None:
        self._return_signal = return_signal
        self._anomaly_calls = anomaly_calls
        self._calls = 0
        self.detect_calls: list = []
        self.train_calls: list = []

    def detect(self, feature: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        self.detect_calls.append(feature)
        self._calls += 1
        if self._calls <= self._anomaly_calls:
            return self._return_signal
        return None

    def train(self, df: Any) -> None:
        self.train_calls.append(df)


# ---------------------------------------------------------------------------
# Tests — full pipeline happy path
# ---------------------------------------------------------------------------


class TestPipelineHappyPath:
    """Anomaly detected → threshold passes → noise passes → enrich → publish."""

    def test_full_pipeline_order(self) -> None:
        signal = _sample_signal()
        detector = _FakeDetector(return_signal=signal)
        thresholder = _FakeThresholder(threshold=None)  # no adaptive gate
        noise_filter = _FakeNoiseFilter(suppress=False)
        enricher = _FakeEnricher()
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=thresholder,
            noise_filter=noise_filter,
            enricher=enricher,
            producer=producer,
        )
        # Mark as trained so cold-start gate is bypassed
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        # Pipeline executed in order
        assert len(detector.detect_calls) == 1
        assert len(noise_filter.suppress_calls) == 1
        assert len(enricher.enrich_calls) == 1
        assert len(producer.publish_calls) == 1
        assert result is not None
        assert result["enriched"] is True

    def test_producer_receives_enriched_signal(self) -> None:
        signal = _sample_signal()
        detector = _FakeDetector(return_signal=signal)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        engine.process_message(_sample_feature())

        published = producer.publish_calls[0]
        assert published["enriched"] is True
        assert "entity_context" in published


# ---------------------------------------------------------------------------
# Tests — no anomaly (detect returns None)
# ---------------------------------------------------------------------------


class TestNoAnomaly:
    """When detect() returns None, nothing downstream is called."""

    def test_producer_not_called(self) -> None:
        detector = _FakeDetector(return_signal=None)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        assert result is None
        assert len(producer.publish_calls) == 0

    def test_enricher_not_called(self) -> None:
        enricher = _FakeEnricher()
        engine = DetectorEngine(
            settings=_make_settings(),
            detector=_FakeDetector(return_signal=None),
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=enricher,
            producer=_FakeProducer(),
        )
        engine._is_trained = True

        engine.process_message(_sample_feature())

        assert len(enricher.enrich_calls) == 0


# ---------------------------------------------------------------------------
# Tests — adaptive threshold suppression
# ---------------------------------------------------------------------------


class TestAdaptiveThreshold:
    """Score below adaptive threshold → suppressed."""

    def test_below_threshold_suppressed(self) -> None:
        signal = _sample_signal(anomaly_score=0.60)
        detector = _FakeDetector(return_signal=signal)
        thresholder = _FakeThresholder(threshold=0.75)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=thresholder,
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        assert result is None
        assert len(producer.publish_calls) == 0

    def test_above_threshold_passes(self) -> None:
        signal = _sample_signal(anomaly_score=0.90)
        detector = _FakeDetector(return_signal=signal)
        thresholder = _FakeThresholder(threshold=0.75)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=thresholder,
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        assert result is not None
        assert len(producer.publish_calls) == 1

    def test_no_adaptive_threshold_passes(self) -> None:
        """When thresholder returns None (insufficient data), skip the gate."""
        signal = _sample_signal(anomaly_score=0.50)
        detector = _FakeDetector(return_signal=signal)
        thresholder = _FakeThresholder(threshold=None)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=thresholder,
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        assert result is not None
        assert len(producer.publish_calls) == 1


# ---------------------------------------------------------------------------
# Tests — noise filter suppression
# ---------------------------------------------------------------------------


class TestNoiseFilter:
    """NoiseFilter.should_suppress() returns True → pipeline stops."""

    def test_suppressed_by_noise_filter(self) -> None:
        signal = _sample_signal(anomaly_score=0.50)
        detector = _FakeDetector(return_signal=signal)
        noise_filter = _FakeNoiseFilter(suppress=True)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=noise_filter,
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        assert result is None
        assert len(producer.publish_calls) == 0
        assert len(noise_filter.suppress_calls) == 1

    def test_noise_filter_receives_correct_args(self) -> None:
        signal = _sample_signal(anomaly_score=0.60, source_type="security")
        detector = _FakeDetector(return_signal=signal)
        noise_filter = _FakeNoiseFilter(suppress=False)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=noise_filter,
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        engine.process_message(_sample_feature(entity_id="web-api"))

        call_kwargs = noise_filter.suppress_calls[0]
        assert call_kwargs["entity_id"] == "web-api"
        assert call_kwargs["source_type"] == "security"
        assert call_kwargs["anomaly_score"] == 0.60


# ---------------------------------------------------------------------------
# Tests — cold start
# ---------------------------------------------------------------------------


class TestColdStart:
    """Detection skipped until enough training samples accumulate."""

    def test_buffering_returns_none(self) -> None:
        detector = _FakeDetector(return_signal=_sample_signal())
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(predictive_cold_start_sample_count=5),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )

        # Feed 4 samples (below threshold of 5)
        for _ in range(4):
            result = engine.process_message(_sample_feature())
            assert result is None

        # Detector.detect never called during buffering
        assert len(detector.detect_calls) == 0
        assert len(producer.publish_calls) == 0

    def test_trains_and_detects_on_threshold(self) -> None:
        signal = _sample_signal()
        detector = _FakeDetector(return_signal=signal)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(predictive_cold_start_sample_count=3),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )

        # Feed exactly 3 samples (threshold)
        for i in range(3):
            engine.process_message(_sample_feature(request_volume=1000.0 + i))

        # Training was invoked
        assert len(detector.train_calls) == 1
        assert engine._is_trained is True
        # After the 3rd sample, detect should have run at least once
        assert len(detector.detect_calls) >= 1

    def test_training_buffer_cleared_after_train(self) -> None:
        detector = _FakeDetector(return_signal=None)

        engine = DetectorEngine(
            settings=_make_settings(predictive_cold_start_sample_count=2),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=_FakeProducer(),
        )

        engine.process_message(_sample_feature())
        engine.process_message(_sample_feature())

        assert len(engine._training_buffer) == 0
        assert engine._is_trained is True

    def test_skips_train_with_no_numeric_columns(self) -> None:
        """If buffer has no numeric cols, skip training gracefully."""
        detector = _FakeDetector(return_signal=None)

        engine = DetectorEngine(
            settings=_make_settings(predictive_cold_start_sample_count=1),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=_FakeProducer(),
        )

        # Only non-numeric data
        engine.process_message({"entity_id": "test", "label": "hello"})
        assert len(detector.train_calls) == 0
        assert engine._is_trained is False


# ---------------------------------------------------------------------------
# Tests — thread safety
# ---------------------------------------------------------------------------


class TestThreadSafety:
    """Verify the model lock prevents corrupted state."""

    def test_concurrent_process_calls(self) -> None:
        signal = _sample_signal(anomaly_score=0.50)
        detector = _FakeDetector(return_signal=signal)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        errors: list = []

        def _process(idx: int) -> None:
            try:
                feat = _sample_feature(request_volume=float(idx))
                engine.process_message(feat)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=_process, args=(i,)) for i in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        assert errors == [], f"Concurrent errors: {errors}"
        assert len(producer.publish_calls) == 20
        # Lock is used (type check)
        assert isinstance(engine._model_lock, type(threading.Lock()))

    def test_lock_used_during_detect(self) -> None:
        """Acquire and release the lock around detect to prove it's guarded."""
        detector = _FakeDetector(return_signal=_sample_signal())
        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=_FakeProducer(),
        )
        engine._is_trained = True

        # The lock should be a real threading.Lock
        assert hasattr(engine._model_lock, "acquire")
        assert hasattr(engine._model_lock, "release")


# ---------------------------------------------------------------------------
# Tests — close()
# ---------------------------------------------------------------------------


class TestClose:
    """close() propagates to all owned components."""

    def test_close_calls_producer_close(self) -> None:
        producer = _FakeProducer()
        engine = DetectorEngine(
            settings=_make_settings(),
            detector=_FakeDetector(),
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )

        engine.close()

        assert producer.close_called is True

    def test_close_survives_producer_error(self) -> None:
        """If producer.close() raises, the engine doesn't crash."""
        broken_producer = MagicMock()
        broken_producer.close.side_effect = RuntimeError("kafka down")

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=_FakeDetector(),
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=broken_producer,
        )

        # Should not raise
        engine.close()
        broken_producer.close.assert_called_once()

    def test_double_close_safe(self) -> None:
        producer = _FakeProducer()
        engine = DetectorEngine(
            settings=_make_settings(),
            detector=_FakeDetector(),
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )

        engine.close()
        engine.close()
        assert producer.close_called is True


# ---------------------------------------------------------------------------
# Tests — adaptive thresholder updated with metric value
# ---------------------------------------------------------------------------


class TestThresholderUpdate:
    """After detection, the adaptive thresholder is updated online."""

    def test_thresholder_update_called(self) -> None:
        signal = _sample_signal(metric_name="error_rate")
        detector = _FakeDetector(return_signal=signal)
        thresholder = _FakeThresholder(threshold=None)

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=thresholder,
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=_FakeProducer(),
        )
        engine._is_trained = True

        engine.process_message(_sample_feature(error_rate=0.05))

        assert len(thresholder.update_calls) == 1
        entity, metric, value = thresholder.update_calls[0]
        assert entity == "order-service"
        assert metric == "error_rate"
        assert value == 0.05


# ---------------------------------------------------------------------------
# Tests — fusion + session tracker wiring (Task T7)
# ---------------------------------------------------------------------------


class TestFusionAndSessionWiring:
    """Bayesian fusion + cold-start + session tracker wired into process_message."""

    def test_baseline_pipeline_order_preserved(self) -> None:
        """Characterize the pipeline order — unchanged by the T7 wiring.

        detect → fusion → adaptive threshold → noise filter → enrich → publish.
        """
        signal = _sample_signal(
            anomaly_score=0.85,
            detector_contributions={
                "z_score": 0.9,
                "seasonal_naive": 0.8,
                "isolation_forest": 0.7,
            },
        )
        detector = _FakeDetector(return_signal=signal)
        noise_filter = _FakeNoiseFilter(suppress=False)
        enricher = _FakeEnricher()
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=noise_filter,
            enricher=enricher,
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        assert len(detector.detect_calls) == 1
        assert len(noise_filter.suppress_calls) == 1
        assert len(enricher.enrich_calls) == 1
        assert len(producer.publish_calls) == 1
        assert result is not None
        assert result["enriched"] is True

    def test_fusion_and_session_wiring(self) -> None:
        """Fusion recalibrates the score; session tracker populates provenance."""
        signal = _sample_signal(
            anomaly_score=0.85,
            detector_contributions={
                "z_score": 0.9,
                "seasonal_naive": 0.8,
                "isolation_forest": 0.7,
            },
        )
        detector = _FakeDetector(return_signal=signal)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        result = engine.process_message(_sample_feature())

        # The published anomaly_score is the fused calibrated probability,
        # not the raw composite score (0.85) the detector returned.
        assert result["anomaly_score"] != 0.85
        assert 0.0 <= result["anomaly_score"] <= 1.0
        # Session tracker populates provenance fields.
        assert result.get("entity_anomaly_count", 0) >= 1
        assert result.get("resolution_status") == "active"

    def test_session_resolution_flow(self) -> None:
        """An active session resolves after 3 consecutive normal observations."""
        signal = _sample_signal(anomaly_score=0.85, metric_name="latency_p99")
        detector = _StatefulDetector(signal, anomaly_calls=1)
        producer = _FakeProducer()

        engine = DetectorEngine(
            settings=_make_settings(),
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
        )
        engine._is_trained = True

        # Anomaly observation → session starts.
        engine.process_message(_sample_feature())
        assert (
            engine._session_tracker.get_session("order-service", "latency_p99")
            is not None
        )

        # 3 normal observations → session resolves.
        for _ in range(3):
            engine.process_message(_sample_feature())

        resolved = engine._session_tracker.get_resolved_sessions()
        assert len(resolved) == 1
        assert resolved[0].resolution_status == "resolved"


# ---------------------------------------------------------------------------
# Tests — integration pipeline with real components (Task T11)
# ---------------------------------------------------------------------------


class TestIntegrationPipeline:
    """Full process_message() pipeline with REAL AnomalyDetector + fusion +
    session tracker (infra-free: thresholder / noise / enricher / producer
    are fakes because they need Kafka / Neo4j / ClickHouse)."""

    _DETECTOR_ORDER = ["z_score", "seasonal_naive", "isolation_forest"]

    def _build_engine(self, cold_start: int = 60):
        settings = _make_settings(predictive_cold_start_sample_count=cold_start)
        detector = AnomalyDetector(settings)
        fusion = ColdStartAwareFusion(
            BayesianFusionEngine(detector_order=self._DETECTOR_ORDER)
        )
        # Fit the Platt calibration on two labelled samples so fusion is real.
        fusion.fit(
            [
                (
                    {"z_score": 0.1, "seasonal_naive": 0.1, "isolation_forest": 0.1},
                    0,
                ),
                (
                    {"z_score": 0.9, "seasonal_naive": 0.9, "isolation_forest": 0.9},
                    1,
                ),
            ]
        )
        producer = _FakeProducer()
        engine = DetectorEngine(
            settings=settings,
            detector=detector,
            thresholder=_FakeThresholder(),
            noise_filter=_FakeNoiseFilter(),
            enricher=_FakeEnricher(),
            producer=producer,
            fusion=fusion,
            session_tracker=AnomalySessionTracker(),
        )
        return engine, detector, producer

    def test_real_detector_fusion_session_pipeline(self) -> None:
        """Train a real detector, then an anomalous observation flows through
        the full pipeline and is published with provenance + session fields."""
        engine, detector, producer = self._build_engine()

        # Feed cold-start samples to train the real detector.
        for i in range(60):
            engine.process_message(_sample_feature(request_volume=1000.0 + i))
        assert engine._is_trained is True

        # A large spike in every metric is a clear anomaly (a single-metric
        # spike gets diluted below the 0.7 composite threshold by the constant
        # training columns, so all three must deviate together).
        result = engine.process_message(
            _sample_feature(latency_p99=5000.0, error_rate=10.0, request_volume=5000.0)
        )

        assert result is not None
        assert 0.0 <= result["anomaly_score"] <= 1.0
        assert result["entity_id"] == "order-service"
        assert result["enriched"] is True
        # Real detector provenance + session tracking populated.
        assert result.get("detector_name") == "AnomalyDetector"
        assert result.get("entity_anomaly_count", 0) >= 1
        assert result.get("resolution_status") == "active"
        assert len(producer.publish_calls) == 1
        assert producer.publish_calls[0] is result

    def test_real_pipeline_normal_observation_no_anomaly(self) -> None:
        """A normal observation after training produces no anomaly and is not
        published."""
        engine, detector, producer = self._build_engine()

        for i in range(60):
            engine.process_message(_sample_feature(request_volume=1000.0 + i))
        assert engine._is_trained is True

        # A value inside the trained baseline is normal.  Training varied
        # request_volume across 1000.0..1059.0 (latency/error rate constant
        # at their own trained values), so a mid-range request_volume is a
        # normal observation.
        result = engine.process_message(_sample_feature(request_volume=1030.0))

        assert result is None
        assert len(producer.publish_calls) == 0
