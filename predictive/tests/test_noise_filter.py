"""Tests for the NoiseFilter transient spike suppression."""

from datetime import datetime, timedelta, timezone

import pytest

from predictive.noise_filter import (
    CASCADE_NEIGHBOR_THRESHOLD,
    CRITICAL_SCORE_THRESHOLD,
    SPIKE_DURATION_THRESHOLD_SECONDS,
    NoiseFilter,
)


def _ts(seconds_from_epoch: int) -> datetime:
    return datetime.fromtimestamp(seconds_from_epoch, tz=timezone.utc)


def test_isolated_30s_spike_is_suppressed():
    """A short-lived, isolated spike should be suppressed as noise."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)
    t1 = t0 + timedelta(seconds=30)

    assert nf.should_suppress("svc-a", "cpu", t0, affected_neighbors=0) is True
    assert nf.should_suppress("svc-a", "cpu", t1, affected_neighbors=0) is True


def test_long_duration_spike_passes_through():
    """A spike lasting longer than 3 minutes is not transient noise."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)
    t1 = t0 + timedelta(seconds=SPIKE_DURATION_THRESHOLD_SECONDS + 1)

    assert nf.should_suppress("entity-a", "cpu", t0, affected_neighbors=0) is True
    assert nf.should_suppress("entity-a", "cpu", t1, affected_neighbors=0) is False


def test_5min_cascade_passes_through():
    """A cascade with >= 3 affected neighbors passes through immediately."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)
    t1 = t0 + timedelta(minutes=5)

    assert (
        nf.should_suppress(
            "entity-a", "cpu", t0, affected_neighbors=CASCADE_NEIGHBOR_THRESHOLD
        )
        is False
    )
    assert (
        nf.should_suppress(
            "entity-a", "cpu", t1, affected_neighbors=CASCADE_NEIGHBOR_THRESHOLD
        )
        is False
    )


def test_security_source_passes_through():
    """Security signals are never suppressed."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)

    assert (
        nf.should_suppress(
            "entity-a", "cpu", t0, affected_neighbors=0, source_type="security"
        )
        is False
    )


def test_critical_score_bypasses_suppression():
    """Anomalies with score >= 0.85 are never suppressed."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)

    assert (
        nf.should_suppress(
            "entity-a",
            "cpu",
            t0,
            affected_neighbors=0,
            anomaly_score=CRITICAL_SCORE_THRESHOLD,
        )
        is False
    )
    assert (
        nf.should_suppress(
            "entity-a", "cpu", t0, affected_neighbors=0, anomaly_score=0.95
        )
        is False
    )


def test_tracks_first_seen_per_entity_metric():
    """First-seen timestamps are tracked independently per (entity, metric)."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)

    # First observation of entity-a/cpu starts its timer.
    assert nf.should_suppress("entity-a", "cpu", t0, affected_neighbors=0) is True

    # A different metric on the same entity starts its own timer.
    assert nf.should_suppress("entity-a", "memory", t0, affected_neighbors=0) is True

    # entity-a/cpu now exceeds the threshold -> passes through.
    t1 = t0 + timedelta(seconds=SPIKE_DURATION_THRESHOLD_SECONDS + 1)
    assert nf.should_suppress("entity-a", "cpu", t1, affected_neighbors=0) is False

    # entity-a/memory observed shortly after its own first-seen -> suppressed.
    t_short = t0 + timedelta(seconds=30)
    assert nf.should_suppress("entity-a", "memory", t_short, affected_neighbors=0) is True


def test_reset_clears_tracking():
    """reset() clears all first-seen timestamps."""
    nf = NoiseFilter()
    t0 = _ts(1_000_000)

    assert nf.should_suppress("entity-a", "cpu", t0, affected_neighbors=0) is True
    nf.reset()
    assert nf.should_suppress("entity-a", "cpu", t0, affected_neighbors=0) is True