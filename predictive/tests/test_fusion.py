"""
OmniWatch — Predictive Intelligence Layer
Component: Bayesian Fusion Engine Tests
Phase: 6
Purpose: Verify Platt-calibrated Bayesian fusion, detector-order handling,
         temperature scaling, cold-start fallback, and confidence scaling
Inputs: N/A (test file)
Outputs: Test results
"""

import pytest

from predictive.fusion import BayesianFusionEngine, ColdStartAwareFusion


# ------------------------------------------------------------------
# BayesianFusionEngine — calibration behaviour
# ------------------------------------------------------------------

class TestBayesianFusionEngine:
    def test_calibrated_probability_in_range(self):
        """A fitted engine must return a probability in [0, 1]."""
        engine = BayesianFusionEngine(detector_order=["iso", "zscore"])
        samples = [
            ({"iso": 0.9, "zscore": 0.8}, 1) for _ in range(30)
        ] + [
            ({"iso": 0.1, "zscore": 0.2}, 0) for _ in range(30)
        ]
        engine.fit(samples)
        assert engine._fitted is True
        for scores in (
            {"iso": 0.9, "zscore": 0.8},
            {"iso": 0.1, "zscore": 0.2},
            {"iso": 0.5, "zscore": 0.5},
        ):
            p = engine.predict(scores)
            assert 0.0 <= p <= 1.0

    def test_detector_order_respected(self):
        """Feature vectors must place scores in ``_detector_order`` order."""
        engine = BayesianFusionEngine(
            detector_order=["iso", "zscore", "seasonal"]
        )
        vec = engine._feature_vector(
            {"zscore": 0.5, "iso": 0.9, "seasonal": 0.2}
        )
        assert vec == [0.9, 0.5, 0.2]

    def test_platt_temperature_scales_logits(self):
        """Higher temperature pulls probabilities toward 0.5."""
        samples = [
            ({"a": 0.9, "b": 0.1}, 1) for _ in range(30)
        ] + [
            ({"a": 0.1, "b": 0.9}, 0) for _ in range(30)
        ]
        cold = BayesianFusionEngine(
            detector_order=["a", "b"], platt_temperature=1.0
        )
        hot = BayesianFusionEngine(
            detector_order=["a", "b"], platt_temperature=5.0
        )
        cold.fit(samples)
        hot.fit(samples)
        p_cold = cold.predict({"a": 0.9, "b": 0.1})
        p_hot = hot.predict({"a": 0.9, "b": 0.1})
        # T=5.0 scales logits down → closer to 0.5 than T=1.0
        assert abs(p_cold - 0.5) > abs(p_hot - 0.5)

    def test_fallback_when_unfitted(self):
        """Unfitted engine falls back to a weighted mean of scores."""
        engine = BayesianFusionEngine(detector_order=["a", "b"])
        assert engine._fitted is False
        p = engine.predict({"a": 0.8, "b": 0.4})
        assert abs(p - 0.6) < 1e-9  # mean(0.8, 0.4)

    def test_fallback_when_insufficient_samples(self):
        """Fewer than the minimum samples → fallback, not a crash."""
        engine = BayesianFusionEngine(detector_order=["a", "b"])
        engine.fit([({"a": 0.9, "b": 0.1}, 1)])  # single sample
        assert engine._fitted is False
        p = engine.predict({"a": 0.8, "b": 0.4})
        assert abs(p - 0.6) < 1e-9

    def test_fallback_when_single_class(self):
        """All labels identical → cannot fit → fallback."""
        engine = BayesianFusionEngine(detector_order=["a", "b"])
        engine.fit([({"a": 0.9, "b": 0.1}, 1) for _ in range(10)])
        assert engine._fitted is False
        p = engine.predict({"a": 0.8, "b": 0.4})
        assert abs(p - 0.6) < 1e-9

    def test_fallback_missing_detector_treated_as_zero(self):
        """A detector absent from the scores dict contributes 0.0."""
        engine = BayesianFusionEngine(detector_order=["a", "b"])
        p = engine.predict({"a": 0.8})  # b missing → 0.0
        assert abs(p - 0.4) < 1e-9  # mean(0.8, 0.0)


# ------------------------------------------------------------------
# ColdStartAwareFusion
# ------------------------------------------------------------------

class TestColdStartAwareFusion:
    def test_confidence_scaling(self):
        """confidence = min(1.0, n_samples / 100)."""
        engine = BayesianFusionEngine(detector_order=["a"])
        fusion = ColdStartAwareFusion(engine)
        assert fusion.n_samples == 0
        assert fusion.confidence == 0.0

        fusion.fit([({"a": 0.5}, 1) for _ in range(50)])
        assert fusion.n_samples == 50
        assert abs(fusion.confidence - 0.5) < 1e-9

        fusion.fit([({"a": 0.5}, 1) for _ in range(100)])
        assert fusion.n_samples == 100
        assert fusion.confidence == 1.0

    def test_predict_delegates_to_engine(self):
        """ColdStartAwareFusion.predict returns the engine's probability."""
        engine = BayesianFusionEngine(detector_order=["a", "b"])
        fusion = ColdStartAwareFusion(engine)
        samples = [
            ({"a": 0.9, "b": 0.1}, 1) for _ in range(30)
        ] + [
            ({"a": 0.1, "b": 0.9}, 0) for _ in range(30)
        ]
        fusion.fit(samples)
        p = fusion.predict({"a": 0.9, "b": 0.1})
        assert 0.0 <= p <= 1.0
        assert p > 0.5  # high a-score → anomaly