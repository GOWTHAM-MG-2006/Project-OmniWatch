"""
OmniWatch — Predictive Intelligence Layer
Component: Adaptive Thresholder Tests
Phase: 6
Purpose: Verify Welford's online algorithm, threshold computation, and state persistence
Inputs: N/A (test file)
Outputs: Test results
"""

import json
import math
import os
import tempfile

import pytest

from predictive.adaptive_thresholder import AdaptiveThresholder, _WelfordStats


# ------------------------------------------------------------------
# _WelfordStats unit tests
# ------------------------------------------------------------------

class TestWelfordStats:
    def test_empty_stats(self):
        s = _WelfordStats()
        assert s.count == 0
        assert s.mean == 0.0
        assert s.m2 == 0.0
        assert s.variance == 0.0
        assert s.stddev == 0.0

    def test_single_value(self):
        s = _WelfordStats()
        s.update(5.0)
        assert s.count == 1
        assert s.mean == 5.0
        assert s.variance == 0.0  # population var undefined for n=1

    def test_known_values(self):
        """Match a hand-calculated variance for [2, 4, 6, 8, 10]."""
        s = _WelfordStats()
        for v in [2, 4, 6, 8, 10]:
            s.update(float(v))
        assert s.count == 5
        assert s.mean == 6.0
        # population variance = 8.0
        assert abs(s.variance - 8.0) < 1e-12
        assert abs(s.stddev - math.sqrt(8.0)) < 1e-12

    def test_large_stream_stability(self):
        """Feed 10 000 identical values — stddev should be zero."""
        s = _WelfordStats()
        for _ in range(10_000):
            s.update(42.0)
        assert s.count == 10_000
        assert abs(s.mean - 42.0) < 1e-12
        assert abs(s.variance) < 1e-12

    def test_roundtrip_dict(self):
        s = _WelfordStats(count=5, mean=6.0, m2=40.0)
        d = s.to_dict()
        s2 = _WelfordStats.from_dict(d)
        assert s2.count == s.count
        assert s2.mean == s.mean
        assert s2.m2 == s.m2


# ------------------------------------------------------------------
# AdaptiveThresholder tests
# ------------------------------------------------------------------

class TestAdaptiveThresholder:
    def test_no_data_returns_none(self):
        t = AdaptiveThresholder()
        assert t.get_threshold("svc", "cpu") is None

    def test_single_value_returns_none(self):
        t = AdaptiveThresholder()
        t.update("svc", "cpu", 50.0)
        assert t.get_threshold("svc", "cpu") is None  # need >= 2 samples

    def test_threshold_formula(self):
        """threshold = mean + k * stddev."""
        t = AdaptiveThresholder(k=2.0)
        for v in [10, 20, 30, 40, 50]:
            t.update("svc", "cpu", float(v))
        threshold = t.get_threshold("svc", "cpu")
        stats = t.get_stats("svc", "cpu")
        assert threshold is not None
        assert stats is not None
        assert abs(threshold - (stats["mean"] + 2.0 * stats["stddev"])) < 1e-12

    def test_100_values_threshold_moves(self):
        """Feed 100 values with increasing mean; threshold should rise."""
        t = AdaptiveThresholder(k=3.0)
        thresholds = []
        for i in range(1, 101):
            t.update("svc", "mem", float(i))
            if i >= 2:
                thresholds.append(t.get_threshold("svc", "mem"))

        first_th = thresholds[0]
        last_th = thresholds[-1]
        assert first_th is not None
        assert last_th is not None
        # First threshold (i=2) should be much lower than last (i=100)
        assert last_th > first_th
        # Threshold should always be above the mean
        stats = t.get_stats("svc", "mem")
        assert stats is not None
        assert last_th > stats["mean"]

    def test_multiple_entities_independent(self):
        t = AdaptiveThresholder()
        for v in range(1, 51):
            t.update("svc_a", "cpu", float(v))
            t.update("svc_b", "cpu", float(v * 10))
        th_a = t.get_threshold("svc_a", "cpu")
        th_b = t.get_threshold("svc_b", "cpu")
        assert th_a is not None
        assert th_b is not None
        assert th_b > th_a  # svc_b values are 10x larger

    def test_multiple_metrics_independent(self):
        t = AdaptiveThresholder()
        for v in range(1, 51):
            t.update("svc", "cpu", float(v))
            t.update("svc", "mem", float(v * 100))
        th_cpu = t.get_threshold("svc", "cpu")
        th_mem = t.get_threshold("svc", "mem")
        assert th_cpu is not None
        assert th_mem is not None
        assert th_mem > th_cpu


# ------------------------------------------------------------------
# State persistence tests
# ------------------------------------------------------------------

class TestPersistence:
    def test_state_survives_restart(self):
        """Feed data, save, reload from a new instance, verify identical state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "thresh.json")

            # First instance: ingest data
            t1 = AdaptiveThresholder(state_path=state_file, k=3.0)
            for v in range(1, 51):
                t1.update("svc", "cpu", float(v))
            th1 = t1.get_threshold("svc", "cpu")
            stats1 = t1.get_stats("svc", "cpu")

            # Second instance: reload from disk
            t2 = AdaptiveThresholder(state_path=state_file, k=3.0)
            th2 = t2.get_threshold("svc", "cpu")
            stats2 = t2.get_stats("svc", "cpu")

            assert th1 is not None
            assert th2 is not None
            assert abs(th1 - th2) < 1e-12
            assert stats1 is not None
            assert stats2 is not None
            assert stats2["count"] == stats1["count"]
            assert abs(stats2["mean"] - stats1["mean"]) < 1e-12
            assert abs(stats2["m2"] - stats1["m2"]) < 1e-12

    def test_atomic_write_no_corruption(self):
        """Verify the JSON file is valid after every update."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "thresh.json")
            t = AdaptiveThresholder(state_path=state_file, k=3.0)
            for i in range(1, 21):
                t.update("svc", "cpu", float(i))
                # File must be valid JSON after every update
                with open(state_file, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                assert "stats" in data
                key = "svc::cpu"
                assert key in data["stats"]
                assert data["stats"][key]["count"] == i

    def test_tmp_file_removed(self):
        """The .tmp file should not remain after a successful save."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "thresh.json")
            t = AdaptiveThresholder(state_path=state_file, k=3.0)
            t.update("svc", "cpu", 1.0)
            tmp_path = state_file + ".tmp"
            assert not os.path.exists(tmp_path)
            assert os.path.exists(state_file)

    def test_k_persisted(self):
        """Custom k value must be saved and reloaded."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_file = os.path.join(tmpdir, "thresh.json")
            t1 = AdaptiveThresholder(state_path=state_file, k=5.0)
            for v in range(1, 11):
                t1.update("svc", "cpu", float(v))
            th1 = t1.get_threshold("svc", "cpu")

            t2 = AdaptiveThresholder(state_path=state_file)
            th2 = t2.get_threshold("svc", "cpu")

            # k=5 was persisted; default k=3.0 would give different threshold
            assert th1 is not None
            assert th2 is not None
            assert abs(th1 - th2) < 1e-12
