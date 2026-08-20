"""
OmniWatch — Phase 8 E2E Test Fixtures

Mock-based fixtures for the Incident Prioritization Engine (Phase 8) end-to-end
tests.  No Docker / Kafka / MinIO required — the Kafka producer is mocked and
captured, and all internal components (classifier, scorer, dedup, factory) use
real implementations with no external dependencies.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from prioritization.config.settings import Settings
from prioritization.deduplication_engine import DeduplicationEngine
from prioritization.models import IncidentRecord
from prioritization.prioritization_engine import PrioritizationEngine


def _make_mock_producer() -> MagicMock:
    """Create a MagicMock producer that captures published incidents."""
    producer = MagicMock()
    producer._published = []

    def _capture_publish(incident: Any, key: str | None = None) -> str:
        producer._published.append(incident)
        return incident.incident_id

    producer.publish_incident.side_effect = _capture_publish
    producer.start.return_value = None
    producer.stop.return_value = None
    producer.flush.return_value = 0
    return producer


def _noop_persist(incident: IncidentRecord) -> None:
    """No-op ClickHouse persist — prevents connection retries in tests."""


@pytest.fixture()
def settings() -> Settings:
    """Isolated settings that ignore any host .env / env variables."""
    return Settings(_env_file=None)


@pytest.fixture()
def engine(settings: Settings) -> PrioritizationEngine:
    """Real PrioritizationEngine with the Kafka producer swapped for a capturing mock.

    ``engine._producer`` is a MagicMock whose ``publish_incident`` side effect
    records every published IncidentRecord into ``producer._published`` for
    assertions.  The dedup engine is real (in-memory TTLCache) with default
    settings so dedup tests work out of the box.
    """
    eng = PrioritizationEngine(settings=settings, persist_fn=_noop_persist)
    eng._producer = _make_mock_producer()
    return eng


@pytest.fixture()
def engine_with_short_ttl(settings: Settings) -> PrioritizationEngine:
    """Engine with a dedup TTL of 1 second for TTL-expiry tests."""
    dedup = DeduplicationEngine(ttl_seconds=1, enabled=True)
    eng = PrioritizationEngine(
        settings=settings, dedup_engine=dedup, persist_fn=_noop_persist
    )
    eng._producer = _make_mock_producer()
    return eng


@pytest.fixture()
def engine_dedup_disabled(settings: Settings) -> PrioritizationEngine:
    """Engine with deduplication disabled for pass-through tests."""
    dedup = DeduplicationEngine(ttl_seconds=300, enabled=False)
    eng = PrioritizationEngine(
        settings=settings, dedup_engine=dedup, persist_fn=_noop_persist
    )
    eng._producer = _make_mock_producer()
    return eng


@pytest.fixture()
def make_root_cause():
    """Factory for creating RootCauseObject dicts with test data.

    All values use the Phase 7 0..1 scale for confidence and anomaly_score.
    Override any field via kwargs.
    """

    def _make(**overrides: Any) -> dict[str, Any]:
        defaults: dict[str, Any] = {
            "incident_id": "rc-test-001",
            "root_cause_entity": "postgresql-database",
            "entity_type": "DATABASE_NODE",
            "confidence": 0.85,  # 0..1 scale
            "anomaly_score": 0.85,  # 0..1 scale
            "fault_path": ["postgresql-database", "order-service", "api-gateway"],
            "impacted_services": ["order-service", "api-gateway", "payment-service"],
            "impacted_count": 3,
            "evidence": {
                "log_snippets": [
                    "FATAL: connection refused",
                    "timeout waiting for response",
                    "restarting postgresql",
                ],
            },
            "timestamp": "2026-01-01T00:00:00+00:00",
        }
        defaults.update(overrides)
        return defaults

    return _make
