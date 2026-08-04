"""
OmniWatch — Predictive Intelligence Layer
Component: Anomaly Session Tracker Tests
Phase: 6
Purpose: Verify session start/update, peak tracking, duration, and 3-consecutive-normal resolution
Inputs: N/A (test file)
Outputs: Test results
"""

from datetime import datetime, timedelta

import pytest

from predictive.session_tracker import AnomalySession, AnomalySessionTracker

# Reference timestamp (UTC) used as the session start point.
_T0 = "2026-08-04T10:00:00+00:00"


def _ts(seconds_from_t0: int) -> str:
    """ISO-8601 timestamp ``seconds_from_t0`` seconds after _T0."""
    base = datetime.fromisoformat(_T0)
    return (base + timedelta(seconds=seconds_from_t0)).isoformat()


# ------------------------------------------------------------------
# AnomalySession dataclass tests
# ------------------------------------------------------------------

class TestAnomalySession:
    def test_defaults(self):
        s = AnomalySession(
            entity_id="svc-a",
            metric_name="cpu_usage",
            start_time=_T0,
            last_update=_T0,
            peak_score=0.1,
            score_history=[0.1],
        )
        assert s.resolution_status == "active"
        assert s.duration_seconds == 0.0

    def test_to_dict_json_serializable(self):
        import json

        s = AnomalySession(
            entity_id="svc-a",
            metric_name="cpu_usage",
            start_time=_T0,
            last_update=_ts(90),
            peak_score=0.93,
            score_history=[0.3, 0.93],
            resolution_status="active",
            duration_seconds=90.0,
        )
        d = s.to_dict()
        # Must not raise — every value must be JSON-serialisable.
        json.dumps(d)
        assert d["entity_id"] == "svc-a"
        assert d["metric_name"] == "cpu_usage"
        assert d["start_time"] == _T0
        assert d["last_update"] == _ts(90)
        assert d["peak_score"] == 0.93
        assert d["score_history"] == [0.3, 0.93]
        assert d["resolution_status"] == "active"
        assert d["duration_seconds"] == 90.0

    def test_to_dict_returns_copy_of_score_history(self):
        s = AnomalySession(
            entity_id="svc-a",
            metric_name="cpu_usage",
            start_time=_T0,
            last_update=_T0,
            peak_score=0.5,
            score_history=[0.5],
        )
        d = s.to_dict()
        d["score_history"].append(0.99)
        # Mutating the dict must not affect the session's history.
        assert s.score_history == [0.5]


# ------------------------------------------------------------------
# AnomalySessionTracker.start tests
# ------------------------------------------------------------------

class TestStart:
    def test_start_creates_session(self):
        t = AnomalySessionTracker()
        s = t.start("svc-a", "cpu_usage", 0.85, _T0)
        assert isinstance(s, AnomalySession)
        assert s.entity_id == "svc-a"
        assert s.metric_name == "cpu_usage"
        assert s.start_time == _T0
        assert s.last_update == _T0
        assert s.peak_score == 0.85
        assert s.score_history == [0.85]
        assert s.resolution_status == "active"
        assert s.duration_seconds == 0.0

    def test_start_registers_active_session(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.85, _T0)
        active = t.get_active_sessions()
        assert ("svc-a", "cpu_usage") in active
        assert len(active) == 1

    def test_start_duplicate_updates_existing(self):
        t = AnomalySessionTracker()
        s1 = t.start("svc-a", "cpu_usage", 0.85, _T0)
        s2 = t.start("svc-a", "cpu_usage", 0.95, _ts(30))
        assert s1 is s2
        assert len(t.get_active_sessions()) == 1
        assert s2.peak_score == 0.95
        assert s2.score_history == [0.85, 0.95]

    def test_multiple_entities_independent(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.8, _T0)
        t.start("svc-b", "mem_usage", 0.9, _T0)
        assert len(t.get_active_sessions()) == 2
        assert ("svc-a", "cpu_usage") in t.get_active_sessions()
        assert ("svc-b", "mem_usage") in t.get_active_sessions()


# ------------------------------------------------------------------
# AnomalySessionTracker.update tests
# ------------------------------------------------------------------

class TestUpdate:
    def test_update_tracks_peak_score(self):
        t = AnomalySessionTracker()
        s = t.start("svc-a", "cpu_usage", 0.4, _T0)
        t.update("svc-a", "cpu_usage", 0.6, _ts(10))
        t.update("svc-a", "cpu_usage", 0.95, _ts(20))
        assert s.peak_score == 0.95
        assert s.score_history == [0.4, 0.6, 0.95]

    def test_update_does_not_lower_peak(self):
        t = AnomalySessionTracker()
        s = t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.update("svc-a", "cpu_usage", 0.2, _ts(10))
        assert s.peak_score == 0.9

    def test_update_tracks_duration(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.8, _T0)
        t.update("svc-a", "cpu_usage", 0.85, _ts(120))
        s = t.get_session("svc-a", "cpu_usage")
        assert s is not None
        assert s.duration_seconds == pytest.approx(120.0)
        assert s.last_update == _ts(120)

    def test_update_unknown_session_raises(self):
        t = AnomalySessionTracker()
        with pytest.raises(KeyError):
            t.update("svc-a", "cpu_usage", 0.8, _T0)


# ------------------------------------------------------------------
# Resolution tests (3-consecutive-normal rule)
# ------------------------------------------------------------------

class TestResolution:
    def test_not_resolved_after_one_normal_score(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        resolved = t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(10))
        assert resolved is False
        s = t.get_session("svc-a", "cpu_usage")
        assert s is not None
        assert s.resolution_status == "active"

    def test_not_resolved_after_two_normal_scores(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(10))
        resolved = t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(20))
        assert resolved is False
        s = t.get_session("svc-a", "cpu_usage")
        assert s is not None
        assert s.resolution_status == "active"

    def test_resolved_after_exactly_three_normal_scores(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(10))
        t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(20))
        resolved = t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(30))
        assert resolved is True
        # Resolved session is moved out of active into resolved.
        assert t.get_session("svc-a", "cpu_usage") is None
        resolved_list = t.get_resolved_sessions()
        assert len(resolved_list) == 1
        assert resolved_list[0].resolution_status == "resolved"
        assert resolved_list[0].duration_seconds == pytest.approx(30.0)

    def test_anomaly_interrupts_normal_streak(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(10))
        t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(20))
        # An anomalous score in the middle resets the streak.
        t.update("svc-a", "cpu_usage", 0.8, _ts(30))
        resolved = t.check_resolution("svc-a", "cpu_usage", 0.2, _ts(40))
        assert resolved is False
        s = t.get_session("svc-a", "cpu_usage")
        assert s is not None
        assert s.resolution_status == "active"

    def test_score_at_threshold_is_not_normal(self):
        """0.5 is NOT below the default threshold 0.5 → never resolves."""
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.check_resolution("svc-a", "cpu_usage", 0.5, _ts(10))
        t.check_resolution("svc-a", "cpu_usage", 0.5, _ts(20))
        resolved = t.check_resolution("svc-a", "cpu_usage", 0.5, _ts(30))
        assert resolved is False
        assert t.get_session("svc-a", "cpu_usage") is not None

    def test_custom_resolution_window(self):
        t = AnomalySessionTracker(resolution_window=5)
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        for i in range(1, 5):
            assert t.check_resolution("svc-a", "cpu_usage", 0.1, _ts(i * 10)) is False
        assert t.check_resolution("svc-a", "cpu_usage", 0.1, _ts(50)) is True

    def test_new_session_after_resolution_starts_fresh(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.check_resolution("svc-a", "cpu_usage", 0.1, _ts(10))
        t.check_resolution("svc-a", "cpu_usage", 0.1, _ts(20))
        t.check_resolution("svc-a", "cpu_usage", 0.1, _ts(30))
        assert len(t.get_resolved_sessions()) == 1
        assert len(t.get_active_sessions()) == 0
        s = t.start("svc-a", "cpu_usage", 0.9, _ts(60))
        assert s.score_history == [0.9]
        assert s.duration_seconds == 0.0
        assert len(t.get_active_sessions()) == 1


# ------------------------------------------------------------------
# Accessor tests
# ------------------------------------------------------------------

class TestAccessors:
    def test_resolved_sessions_are_listed(self):
        t = AnomalySessionTracker()
        t.start("svc-a", "cpu_usage", 0.9, _T0)
        t.start("svc-b", "mem_usage", 0.8, _T0)
        for i in range(1, 4):
            t.check_resolution("svc-a", "cpu_usage", 0.1, _ts(i * 10))
        assert len(t.get_active_sessions()) == 1
        assert ("svc-b", "mem_usage") in t.get_active_sessions()
        resolved = t.get_resolved_sessions()
        assert len(resolved) == 1
        assert resolved[0].entity_id == "svc-a"
        assert resolved[0].metric_name == "cpu_usage"

    def test_get_session_returns_none_for_unknown(self):
        t = AnomalySessionTracker()
        assert t.get_session("svc-a", "cpu_usage") is None
