"""
OmniWatch — Predictive Intelligence Layer
Component: Drift Detection Tests (CUSUM + ADWIN)
Phase: 6
Purpose: Verify CUSUM and ADWIN drift detectors on real synthetic series
Inputs: N/A (test file)
Outputs: Test results
"""

import math
import random

import pytest

from predictive.drift import ADWINDriftDetector, CUSUMDetector


# ------------------------------------------------------------------
# CUSUMDetector tests
# ------------------------------------------------------------------

class TestCUSUMDetector:
    def test_detects_sustained_mean_shift_within_20_obs(self):
        """A ramp starting after 10 stable points must trip within ~20 obs."""
        d = CUSUMDetector(target_mean=0.0, target_std=1.0)
        for _ in range(10):
            assert d.update(0.0) is False

        detected_at = None
        for i in range(1, 11):
            if d.update(0.5 * i):
                detected_at = 10 + i
                break

        assert detected_at is not None
        assert detected_at <= 20

    def test_no_false_positive_on_stable_data(self):
        """Constant and low-noise stable series must never trip."""
        d = CUSUMDetector(target_mean=10.0, target_std=2.0)
        for _ in range(500):
            assert d.update(10.0) is False

        d2 = CUSUMDetector(target_mean=0.0, target_std=1.0)
        rng = random.Random(7)
        for _ in range(300):
            assert d2.update(rng.gauss(0.0, 0.5)) is False

    def test_bidirectional_detection(self):
        """s_neg must trip on a downward shift, s_pos on an upward shift."""
        d = CUSUMDetector(target_mean=0.0, target_std=1.0)
        for _ in range(10):
            d.update(0.0)
        fired_down = False
        for i in range(1, 11):
            if d.update(-0.5 * i):
                fired_down = True
                break
        assert fired_down
        assert d.s_neg > d.drift_threshold

        d2 = CUSUMDetector(target_mean=0.0, target_std=1.0)
        for _ in range(10):
            d2.update(0.0)
        fired_up = False
        for i in range(1, 11):
            if d2.update(0.5 * i):
                fired_up = True
                break
        assert fired_up
        assert d2.s_pos > d.drift_threshold

    def test_reset_clears_state(self):
        """reset() must zero both cumulative sums and stop false alarms."""
        d = CUSUMDetector(target_mean=0.0, target_std=1.0)
        for _ in range(10):
            d.update(0.0)
        fired = False
        for i in range(1, 11):
            if d.update(0.5 * i):
                fired = True
                break
        assert fired

        d.reset()
        assert d.s_pos == 0.0
        assert d.s_neg == 0.0
        for _ in range(100):
            assert d.update(0.0) is False

    def test_constructor_exposes_parameters(self):
        """Defaults and overrides must be stored on the instance."""
        d = CUSUMDetector(target_mean=5.0, target_std=2.0)
        assert d.drift_threshold == 4.0
        assert d.slack == 0.5
        assert d.target_mean == 5.0
        assert d.target_std == 2.0

        d2 = CUSUMDetector(target_mean=0.0, target_std=1.0, drift_threshold=8.0, slack=1.0)
        assert d2.drift_threshold == 8.0
        assert d2.slack == 1.0


# ------------------------------------------------------------------
# ADWINDriftDetector tests
# ------------------------------------------------------------------

class TestADWINDriftDetector:
    def test_detects_concept_drift_on_distribution_shift(self):
        """A Gaussian series that shifts in mean must trigger drift."""
        d = ADWINDriftDetector(delta=0.002)
        rng = random.Random(42)
        # Stable phase: tight noise (sigma 0.5) so no single outlier trips the
        # Hoeffding bound before the real shift arrives.
        for _ in range(40):
            assert d.update(rng.gauss(0.0, 0.5)) is False
        assert d.needs_retrain is False

        detected_at = None
        for i in range(1, 21):
            if d.update(rng.gauss(5.0, 0.5)):
                detected_at = i
                break

        assert detected_at is not None
        assert detected_at <= 20
        assert d.needs_retrain is True

    def test_no_drift_before_min_window(self):
        """A huge jump must be ignored while the window is below 30 obs."""
        d = ADWINDriftDetector(delta=0.002)
        for i in range(15):
            v = 0.0 if i < 10 else 1000.0
            assert d.update(v) is False
        assert d.needs_retrain is False

    def test_drift_respects_min_window_boundary(self):
        """The earliest possible detection is exactly the 30th observation."""
        d = ADWINDriftDetector(delta=0.002)
        for i in range(40):
            v = 0.0 if i < 20 else 100.0
            fired = d.update(v)
            if fired:
                assert i + 1 >= 30  # never before the min window
        assert d.needs_retrain is True

    def test_needs_retrain_toggles(self):
        """needs_retrain flips True on drift and back to False on reset."""
        d = ADWINDriftDetector(delta=0.002)
        assert d.needs_retrain is False
        for _ in range(40):
            d.update(0.0)
        assert d.needs_retrain is False

        fired = False
        for _ in range(10):
            if d.update(5.0):
                fired = True
                break
        assert fired
        assert d.needs_retrain is True

        d.reset()
        assert d.needs_retrain is False
        for _ in range(40):
            assert d.update(0.0) is False
        assert d.needs_retrain is False

    def test_constructor_defaults(self):
        """Default delta and min window size must match the spec."""
        d = ADWINDriftDetector()
        assert d.delta == 0.002
        assert d.min_window_size == 30

    def test_window_grows_with_data(self):
        """window_size must track the number of buffered observations."""
        d = ADWINDriftDetector(delta=0.002)
        for i in range(1, 31):
            d.update(0.0)
            assert d.window_size == i
