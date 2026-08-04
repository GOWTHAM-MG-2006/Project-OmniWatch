"""
OmniWatch — Predictive Intelligence Layer
Component: Health endpoint tests
Phase: 6
Purpose: Pin the /health response contract (baseline characterization) and
         verify per-detector state reporting + structured JSON detection
         logging with detector provenance.
Inputs: FastAPI TestClient requests to /health, module-level setters
Outputs: Assertions on response JSON and formatted log records
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient

import predictive.main as main_module


@pytest.fixture(autouse=True)
def _reset_module_state() -> Iterator[None]:
    """Reset module-level health state between tests (isolation).

    ``predictive.main`` keeps a module-level registry of detector state and
    engine flags; without a reset, a test that records state would leak into
    the next test.
    """
    main_module._detectors.clear()
    main_module._fusion_calibrated = None
    main_module._drift_detected = None
    main_module._k8s_cooldown = None
    main_module._engine = None
    main_module._last_anomaly = "none"
    main_module._last_anomaly_time = ""
    yield


def _client() -> TestClient:
    """Return a fresh TestClient bound to the health app."""
    return TestClient(main_module.app)


def _stub_infra_checks(monkeypatch) -> None:
    """Stub the slow/network component checks so tests are fast + deterministic.

    The real ``_check_clickhouse()`` performs 4 exponential-backoff retries
    (~140 s) against a down broker, and ``_check_kafka()`` opens a real
    producer connection — neither belongs in a unit test.  These tests pin the
    response *shape*; infra reachability is out of scope.
    """
    monkeypatch.setattr(main_module, "_check_kafka", lambda: True)
    monkeypatch.setattr(main_module, "_check_clickhouse", lambda: True)
    monkeypatch.setattr(main_module, "_check_model_loaded", lambda: True)


# --------------------------------------------------------------------------- #
# Baseline characterization — pins the ORIGINAL /health contract so the
# enhanced endpoint can never silently drop an existing field.
# --------------------------------------------------------------------------- #


def test_baseline_health_contract(monkeypatch) -> None:
    """Baseline: original /health shape is preserved (status + components)."""
    _stub_infra_checks(monkeypatch)
    with _client() as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "healthy"
    for key in ("kafka", "clickhouse", "model_loaded", "last_anomaly"):
        assert key in body, f"baseline field {key!r} missing from /health"


def test_baseline_last_anomaly_updated_by_setter(monkeypatch) -> None:
    """Baseline: set_last_anomaly() feeds the /health last_anomaly field."""
    _stub_infra_checks(monkeypatch)
    main_module.set_last_anomaly("postgresql-database")
    with _client() as client:
        body = client.get("/health").json()
    assert body["last_anomaly"].startswith("postgresql-database at ")


# --------------------------------------------------------------------------- #
# New contract — per-detector state + engine flags surfaced on /health.
# These FAIL on the pre-change code (fields absent) and pass after T9.
# --------------------------------------------------------------------------- #


def test_health_reports_per_detector_state(monkeypatch) -> None:
    """New: /health surfaces trained/n_samples/last_score per detector."""
    _stub_infra_checks(monkeypatch)
    main_module.record_detector_state(
        detector_name="AnomalyDetector",
        trained=True,
        n_samples=120,
        last_score=0.83,
    )
    main_module.record_detector_state(
        detector_name="SecuritySignalClassifier",
        trained=False,
        n_samples=0,
    )
    with _client() as client:
        body = client.get("/health").json()

    detectors = body["detectors"]
    assert "AnomalyDetector" in detectors
    assert detectors["AnomalyDetector"] == {
        "trained": True,
        "n_samples": 120,
        "last_score": 0.83,
    }
    # A detector with no last_score yet degrades to None (never crashes).
    assert detectors["SecuritySignalClassifier"] == {
        "trained": False,
        "n_samples": 0,
        "last_score": None,
    }


def test_health_reports_engine_flags(monkeypatch) -> None:
    """New: fusion_calibrated / drift_detected / k8s_cooldown flags."""
    _stub_infra_checks(monkeypatch)
    main_module.record_engine_state(
        fusion_calibrated=True,
        drift_detected=False,
        k8s_cooldown=True,
    )
    with _client() as client:
        body = client.get("/health").json()
    assert body["fusion_calibrated"] is True
    assert body["drift_detected"] is False
    assert body["k8s_cooldown"] is True


def test_health_engine_flags_default_to_none(monkeypatch) -> None:
    """New: unobserved engine flags degrade to null (defensive, no crash)."""
    _stub_infra_checks(monkeypatch)
    with _client() as client:
        body = client.get("/health").json()
    assert body["fusion_calibrated"] is None
    assert body["drift_detected"] is None
    assert body["k8s_cooldown"] is None


def test_detection_event_logs_structured_json_with_provenance() -> None:
    """New: detection events emit a JSON log line carrying detector provenance."""
    formatter = main_module.JsonLogFormatter()
    record = logging.LogRecord(
        name="omniwatch.predictive.detection",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="detection_event",
        args=(),
        exc_info=None,
    )
    record.detector_name = "AnomalyDetector"
    record.entity_id = "postgresql-database"
    record.metric_name = "cpu_usage"
    record.score = 0.91
    payload = json.loads(formatter.format(record))
    assert payload["detector_name"] == "AnomalyDetector"
    assert payload["entity_id"] == "postgresql-database"
    assert payload["metric_name"] == "cpu_usage"
    assert payload["score"] == 0.91
    assert payload["level"] == "INFO"
    assert payload["message"] == "detection_event"


def test_log_detection_event_updates_health_state(monkeypatch, caplog) -> None:
    """New: log_detection_event() records last_score + last_anomaly + JSON log."""
    _stub_infra_checks(monkeypatch)
    with caplog.at_level(logging.INFO, logger="omniwatch.predictive.detection"):
        main_module.log_detection_event(
            detector_name="AnomalyDetector",
            entity_id="postgresql-database",
            metric_name="cpu_usage",
            score=0.91,
        )
    # Health state updated
    with _client() as client:
        body = client.get("/health").json()
    assert body["detectors"]["AnomalyDetector"]["last_score"] == 0.91
    assert body["last_anomaly"].startswith("postgresql-database at ")
    # Structured JSON log emitted with provenance
    record = caplog.records[-1]
    assert record.detector_name == "AnomalyDetector"
    assert record.entity_id == "postgresql-database"
    assert record.metric_name == "cpu_usage"
    assert record.score == 0.91
