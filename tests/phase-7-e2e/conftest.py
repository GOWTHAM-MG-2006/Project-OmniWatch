"""
OmniWatch — Phase 7 E2E Test Fixtures

Mock-based fixtures for the Causal Graph Engine (Phase 7) end-to-end tests.
No Docker / Kafka / ClickHouse / Neo4j / PyRCA required — the producer is
mocked and the engine builds its graph from explicit in-test topology and
adjacency fixtures passed directly to CausalEngine.analyze_signal, so no
storage layer or external dependency is touched.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from causal.causal_engine import CausalEngine
from causal.config.settings import Settings


@pytest.fixture()
def settings() -> Settings:
    """Isolated settings that ignore any host .env / KAFKA_* variables."""
    return Settings(_env_file=None)


@pytest.fixture()
def engine(settings: Settings) -> CausalEngine:
    """Real CausalEngine with the Kafka producer swapped for a capturing mock.

    engine._producer is a MagicMock whose ``send`` side effect records every
    published record into ``producer._published`` for assertions.
    """
    eng = CausalEngine(settings=settings)

    producer = MagicMock()
    producer._published = []

    def _capture_publish(incident: dict, **kwargs) -> MagicMock:
        producer._published.append(incident)
        return MagicMock()

    producer.publish.side_effect = _capture_publish
    producer.close.return_value = None
    eng._producer = producer
    return eng