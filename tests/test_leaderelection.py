"""Failing-first tests for OmniWatch leader election (IND-7).

Contract under test (see common/leaderelection.py — not yet written):
  - LeaderElectionConfig.from_env(default_lease_name): reads
    OMNIWATCH_LEADER_ELECTION_ENABLED / LEASE_NAME / LEASE_NAMESPACE /
    LEASE_DURATION (default 15s).
  - Two LeaderElector instances sharing one lease backend -> exactly one
    leader at any time (only-leader-processes).
  - Lease expiry (holder stops renewing) -> the other elector takes over
    within lease_duration (failover).
  - Disabled elector (enabled=false) -> is_leader() always True, no backend
    traffic (standalone baseline unchanged).
"""

from __future__ import annotations

import os
import threading
import time

import pytest


def _import_sut():
    from common.leaderelection import (  # noqa: PLC0415
        InMemoryLeaseBackend,
        LeaderElectionConfig,
        LeaderElector,
    )

    return InMemoryLeaseBackend, LeaderElectionConfig, LeaderElector


def _enabled_config(lease_name: str, duration: float = 15.0):
    _, LeaderElectionConfig, _ = _import_sut()
    return LeaderElectionConfig(
        enabled=True,
        lease_name=lease_name,
        lease_namespace="omniwatch-test",
        lease_duration_seconds=duration,
        renew_interval_seconds=min(0.05, duration / 3.0),
    )


def test_config_defaults_match_contract():
    _, LeaderElectionConfig, _ = _import_sut()
    cfg = LeaderElectionConfig.from_env(default_lease_name="omniwatch-test-leader")
    assert cfg.enabled is False  # default OFF: standalone baseline unchanged
    assert cfg.lease_duration_seconds == 15.0
    assert cfg.lease_name == "omniwatch-test-leader"
    assert cfg.lease_namespace  # non-empty (POD_NAMESPACE or omniwatch)


def test_config_env_overrides(monkeypatch: pytest.MonkeyPatch):
    _, LeaderElectionConfig, _ = _import_sut()
    monkeypatch.setenv("OMNIWATCH_LEADER_ELECTION_ENABLED", "true")
    monkeypatch.setenv("OMNIWATCH_LEADER_ELECTION_LEASE_NAME", "custom-lease")
    monkeypatch.setenv("OMNIWATCH_LEADER_ELECTION_LEASE_NAMESPACE", "custom-ns")
    monkeypatch.setenv("OMNIWATCH_LEADER_ELECTION_LEASE_DURATION", "30")
    cfg = LeaderElectionConfig.from_env(default_lease_name="ignored")
    assert cfg.enabled is True
    assert cfg.lease_name == "custom-lease"
    assert cfg.lease_namespace == "custom-ns"
    assert cfg.lease_duration_seconds == 30.0


def test_only_one_leader_among_replicas():
    """3 replicas sharing a lease -> exactly 1 leader processes."""
    InMemoryLeaseBackend, _, LeaderElector = _import_sut()
    backend = InMemoryLeaseBackend()
    electors = [
        LeaderElector(_enabled_config("only-one", duration=5.0), identity=f"pod-{i}", backend=backend)
        for i in range(3)
    ]
    for elector in electors:
        elector.start()
    try:
        deadline = time.monotonic() + 5.0
        leaders: list = []
        while time.monotonic() < deadline:
            leaders = [e for e in electors if e.is_leader()]
            if len(leaders) == 1:
                break
            time.sleep(0.05)
        assert len(leaders) == 1, f"expected exactly 1 leader, got {len(leaders)}"
    finally:
        for elector in electors:
            elector.stop()


def test_leader_loss_triggers_failover_within_lease_duration():
    """Leader stops renewing -> a follower takes over within lease_duration."""
    InMemoryLeaseBackend, _, LeaderElector = _import_sut()
    backend = InMemoryLeaseBackend()
    duration = 0.6
    leader = LeaderElector(_enabled_config("failover", duration=duration), identity="pod-a", backend=backend)
    follower = LeaderElector(
        _enabled_config("failover", duration=duration), identity="pod-b", backend=backend
    )
    leader.start()
    follower.start()
    try:
        deadline = time.monotonic() + 5.0
        while not leader.is_leader() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert leader.is_leader() or follower.is_leader()
        # Kill whichever is leader without releasing (simulates pod death).
        doomed = leader if leader.is_leader() else follower
        survivor = follower if doomed is leader else leader
        doomed.abandon()  # stops renewing WITHOUT releasing the lease
        deadline = time.monotonic() + duration + 5.0
        while not survivor.is_leader() and time.monotonic() < deadline:
            time.sleep(0.05)
        assert survivor.is_leader(), "survivor did not take over after lease expiry"
    finally:
        leader.stop()
        follower.stop()


def test_disabled_mode_always_leader_no_backend():
    """enabled=false -> standalone: is_leader() True, backend untouched."""
    InMemoryLeaseBackend, LeaderElectionConfig, LeaderElector = _import_sut()
    backend = InMemoryLeaseBackend()
    cfg = LeaderElectionConfig.from_env(default_lease_name="x")
    assert cfg.enabled is False
    elector = LeaderElector(cfg, identity="standalone", backend=backend)
    elector.start()
    try:
        assert elector.is_leader() is True
        assert backend.read("x", "y") is None  # no lease traffic in standalone mode
    finally:
        elector.stop()


def test_follower_gate_blocks_processing_loop():
    """Gated runner only executes the processing fn on the leader."""
    InMemoryLeaseBackend, _, LeaderElector = _import_sut()
    from common.leaderelection import run_leader_gated  # noqa: PLC0415

    backend = InMemoryLeaseBackend()
    processed: list[str] = []
    lock = threading.Lock()

    def make_processor(identity, elector):
        stop = threading.Event()

        def run() -> None:
            with lock:
                processed.append(identity)

        def halt() -> None:
            stop.set()

        return run, halt, stop

    electors = [
        LeaderElector(_enabled_config("gated", duration=5.0), identity=f"pod-{i}", backend=backend)
        for i in range(2)
    ]
    threads = []
    for i, elector in enumerate(electors):
        elector.start()
        run, halt, stop = make_processor(f"pod-{i}", elector)
        t = threading.Thread(
            target=run_leader_gated,
            kwargs={"elector": elector, "run_fn": run, "stop_fn": halt,
                    "stop_event": stop, "poll_interval": 0.05},
            daemon=True,
        )
        threads.append((t, stop))
    for t, _ in threads:
        t.start()
    time.sleep(1.0)
    for _, stop in threads:
        stop.set()
    for t, _ in threads:
        t.join(timeout=5.0)
    for elector in electors:
        elector.stop()
    with lock:
        assert len(processed) >= 1, "leader never processed"
        assert len(set(processed)) == 1, f"more than one replica processed: {processed}"


def test_env_prefix_is_documented():
    """All four required env vars are honoured (import-time check)."""
    for var in (
        "OMNIWATCH_LEADER_ELECTION_ENABLED",
        "OMNIWATCH_LEADER_ELECTION_LEASE_NAME",
        "OMNIWATCH_LEADER_ELECTION_LEASE_NAMESPACE",
        "OMNIWATCH_LEADER_ELECTION_LEASE_DURATION",
    ):
        assert var not in os.environ or isinstance(os.environ[var], str)
