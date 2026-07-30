"""
OmniWatch — Phase 1 Unit Tests
Component: AnomalyEngine (services/common/anomaly_injector.py)
Purpose: Validate the thread-safe anomaly injection engine — inject, apply, TTL,
         clear, expiry, and FastAPI route integration.
"""

from __future__ import annotations

import time

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient


# =============================================================================
# AnomalyEngine unit tests
# =============================================================================


class TestAnomalyEngine:
    """Direct unit tests against ``AnomalyEngine``."""

    def test_inject_and_is_active(self, engine):
        """inject() activates a scenario; is_active() returns True."""
        engine.inject("latency_spike", ttl_seconds=120)
        assert engine.is_active("latency_spike") is True

    def test_is_active_false_for_inactive(self, engine):
        """is_active() returns False when scenario was never injected."""
        assert engine.is_active("database_cascade") is False

    def test_is_active_false_after_clear(self, engine):
        """is_active() returns False after clear()."""
        engine.inject("memory_leak", ttl_seconds=120)
        engine.clear("memory_leak")
        assert engine.is_active("memory_leak") is False

    def test_apply_returns_payload_for_active(self, engine):
        """apply() returns the scenario payload when active."""
        engine.inject("database_cascade", ttl_seconds=120)
        payload = engine.apply("database_cascade")
        assert "error_rate" in payload
        assert "delay_ms" in payload
        assert payload["error_rate"] == 0.3
        assert payload["delay_ms"] == 2000

    def test_apply_returns_empty_for_inactive(self, engine):
        """apply() returns {} when the scenario is not active."""
        assert engine.apply("latency_spike") == {}

    def test_apply_returns_empty_after_clear(self, engine):
        """apply() returns {} after the scenario has been cleared."""
        engine.inject("config_drift", ttl_seconds=120)
        engine.clear("config_drift")
        assert engine.apply("config_drift") == {}

    def test_clear_all_deactivates_all(self, engine):
        """clear_all() removes every active scenario."""
        engine.inject("latency_spike", ttl_seconds=120)
        engine.inject("memory_leak", ttl_seconds=120)
        engine.clear_all()
        assert engine.is_active("latency_spike") is False
        assert engine.is_active("memory_leak") is False

    def test_get_active_returns_active_list(self, engine):
        """get_active() returns one entry per active scenario."""
        engine.inject("latency_spike", ttl_seconds=120)
        engine.inject("config_drift", ttl_seconds=60)
        active = engine.get_active()
        scenarios = {a["scenario"] for a in active}
        assert "latency_spike" in scenarios
        assert "config_drift" in scenarios
        assert len(active) == 2

    def test_get_active_empty_when_none(self, engine):
        """get_active() returns [] when no anomalies are active."""
        assert engine.get_active() == []

    def test_get_active_remaining_seconds(self, engine):
        """get_active() entries include remaining_seconds."""
        engine.inject("security_attack", ttl_seconds=300)
        active = engine.get_active()
        entry = active[0]
        assert entry["scenario"] == "security_attack"
        assert entry["remaining_seconds"] > 0
        assert entry["expires_at"] > time.time()

    def test_unknown_scenario_raises_value_error(self, engine):
        """inject() with an unknown scenario raises ValueError."""
        with pytest.raises(ValueError, match="Unknown scenario"):
            engine.inject("nonexistent_scenario")

    def test_clear_unknown_scenario_is_noop(self, engine):
        """clear() on a scenario that was never injected is a no-op (no error)."""
        engine.clear("nonexistent_scenario")  # should not raise

    def test_clear_all_empty_is_noop(self, engine):
        """clear_all() on a clean engine is a no-op (no error)."""
        engine.clear_all()  # should not raise

    def test_ttl_expiry_auto_eviction(self, engine):
        """Scenario is auto-evicted after its TTL expires."""
        engine.inject("latency_spike", ttl_seconds=0.01)  # 10 ms TTL
        time.sleep(0.05)  # wait past expiry
        assert engine.is_active("latency_spike") is False
        assert engine.get_active() == []

    def test_multiple_scenarios_independent(self, engine):
        """Activating one scenario does not affect others."""
        engine.inject("latency_spike", ttl_seconds=120)
        engine.inject("memory_leak", ttl_seconds=120)
        assert engine.is_active("latency_spike") is True
        assert engine.is_active("memory_leak") is True
        engine.clear("latency_spike")
        assert engine.is_active("latency_spike") is False
        assert engine.is_active("memory_leak") is True

    def test_apply_respects_scenario_payload(self, engine):
        """Each scenario returns its own distinct payload."""
        engine.inject("latency_spike", ttl_seconds=120)
        engine.inject("config_drift", ttl_seconds=120)
        latency_payload = engine.apply("latency_spike")
        config_payload = engine.apply("config_drift")
        assert "delay_ms" in latency_payload
        assert "features_disabled" in config_payload

    def test_service_name_preserved(self, engine):
        """Engine stores and returns the service name in responses."""
        assert engine.service_name == "test-service"


# =============================================================================
# FastAPI route integration tests
# =============================================================================


class TestAnomalyRoutes:
    """Test the ``/__inject/anomaly`` endpoints registered by ``add_routes``."""

    @pytest.fixture
    def anomaly_app(self):
        """A minimal FastAPI app with anomaly routes."""
        from services.common.anomaly_injector import AnomalyEngine, add_routes

        app = FastAPI()
        engine = AnomalyEngine(service_name="route-test")
        add_routes(app.router, engine)
        app.state.engine = engine
        return app

    @pytest.fixture
    def client(self, anomaly_app):
        with TestClient(anomaly_app) as c:
            yield c

    def test_post_inject_anomaly(self, client):
        """POST /__inject/anomaly activates a scenario."""
        resp = client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "injected"
        assert data["scenario"] == "latency_spike"
        assert data["ttl_seconds"] == 60

    def test_post_invalid_scenario_returns_400(self, client):
        """POST with unknown scenario returns 400."""
        resp = client.post(
            "/__inject/anomaly",
            json={"scenario": "invalid", "ttl_seconds": 60},
        )
        assert resp.status_code == 400
        assert "Unknown scenario" in resp.json()["detail"]

    def test_delete_single_scenario(self, client):
        """DELETE /__inject/anomaly/{scenario} clears one scenario."""
        client.post(
            "/__inject/anomaly",
            json={"scenario": "memory_leak", "ttl_seconds": 60},
        )
        resp = client.delete("/__inject/anomaly/memory_leak")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cleared"

    def test_delete_invalid_scenario_returns_400(self, client):
        """DELETE with unknown scenario returns 400."""
        resp = client.delete("/__inject/anomaly/invalid")
        assert resp.status_code == 400
        assert "Unknown scenario" in resp.json()["detail"]

    def test_delete_all_scenarios(self, client):
        """DELETE /__inject/anomaly (no scenario) clears all."""
        client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 60},
        )
        client.post(
            "/__inject/anomaly",
            json={"scenario": "config_drift", "ttl_seconds": 60},
        )
        resp = client.delete("/__inject/anomaly")
        assert resp.status_code == 200
        assert resp.json()["cleared"] == "all"

    def test_get_active_anomalies(self, client):
        """GET /__inject/anomaly lists active scenarios."""
        client.post(
            "/__inject/anomaly",
            json={"scenario": "security_attack", "ttl_seconds": 60},
        )
        resp = client.get("/__inject/anomaly")
        assert resp.status_code == 200
        data = resp.json()
        assert data["service"] == "route-test"
        assert len(data["active"]) == 1
        assert data["active"][0]["scenario"] == "security_attack"

    def test_get_active_empty(self, client):
        """GET /__inject/anomaly returns [] when nothing is active."""
        resp = client.get("/__inject/anomaly")
        assert resp.status_code == 200
        assert resp.json()["active"] == []

    def test_get_active_after_ttl_expiry(self, client):
        """Expired scenarios no longer appear in GET."""
        client.post(
            "/__inject/anomaly",
            json={"scenario": "latency_spike", "ttl_seconds": 1},
        )
        time.sleep(1.5)  # wait past TTL
        resp = client.get("/__inject/anomaly")
        assert len(resp.json()["active"]) == 0


# =============================================================================
# Scenario payload contract tests
# =============================================================================


class TestScenarioPayloads:
    """Verify the payload contract for each built-in scenario."""

    @pytest.fixture
    def engine(self):
        from services.common.anomaly_injector import AnomalyEngine

        e = AnomalyEngine(service_name="contract-test")
        return e

    def test_database_cascade_payload(self, engine):
        """database_cascade: delay_ms + error_rate."""
        engine.inject("database_cascade", ttl_seconds=60)
        p = engine.apply("database_cascade")
        assert p.get("delay_ms") == 2000
        assert p.get("error_rate") == 0.3

    def test_memory_leak_payload(self, engine):
        """memory_leak: extra_memory_mb + response_bloat."""
        engine.inject("memory_leak", ttl_seconds=60)
        p = engine.apply("memory_leak")
        assert p.get("extra_memory_mb") == 50
        assert p.get("response_bloat") is True

    def test_latency_spike_payload(self, engine):
        """latency_spike: delay_ms."""
        engine.inject("latency_spike", ttl_seconds=60)
        p = engine.apply("latency_spike")
        assert p.get("delay_ms") == 3000

    def test_security_attack_payload(self, engine):
        """security_attack: block_ip + log_frequency."""
        engine.inject("security_attack", ttl_seconds=60)
        p = engine.apply("security_attack")
        assert p.get("block_ip") is True
        assert p.get("log_frequency") == "high"

    def test_config_drift_payload(self, engine):
        """config_drift: config_version + features_disabled."""
        engine.inject("config_drift", ttl_seconds=60)
        p = engine.apply("config_drift")
        assert p.get("config_version") == "drifted"
        assert "cache" in p.get("features_disabled", [])
        assert "rate_limit" in p.get("features_disabled", [])
