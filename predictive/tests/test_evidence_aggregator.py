"""Tests for the EvidenceAggregator security evidence collection."""

from typing import Any, Dict, List

import pytest

from predictive.security.evidence_aggregator import (
    EVIDENCE_BUFFER_MAX,
    EvidenceAggregator,
)


def _make_event(
    entity_id: str = "svc-a",
    attack_type: str = "BRUTE_FORCE",
    log: str | None = "failed login attempt",
    message: str | None = None,
    description: str | None = None,
    **extra: Any,
) -> Dict[str, Any]:
    """Build a minimal security event dict for testing."""
    event: Dict[str, Any] = {"entity_id": entity_id, "attack_type": attack_type}
    if log is not None:
        event["log"] = log
    if message is not None:
        event["message"] = message
    if description is not None:
        event["description"] = description
    event.update(extra)
    return event


# ── ring buffer cap tests ─────────────────────────────────────────────── #


def test_collect_returns_single_line():
    """First event produces a single-element evidence list."""
    agg = EvidenceAggregator()
    result = agg.collect(_make_event(log="line-1"))
    assert result == ["line-1"]


def test_collect_returns_up_to_5():
    """After 5 events the buffer holds exactly 5 lines."""
    agg = EvidenceAggregator()
    result: List[str] = []
    for i in range(5):
        result = agg.collect(_make_event(log=f"line-{i}"))
    assert result == ["line-0", "line-1", "line-2", "line-3", "line-4"]
    assert len(result) == 5


def test_collect_returns_max_5_after_10_events():
    """Feeding 10 events yields only the last 5 evidence lines."""
    agg = EvidenceAggregator()
    result: List[str] = []
    for i in range(10):
        result = agg.collect(_make_event(log=f"line-{i}"))
    # After 10 events, only the last 5 survive
    assert result == ["line-5", "line-6", "line-7", "line-8", "line-9"]
    assert len(result) == 5


def test_ring_buffer_evicts_oldest():
    """The oldest entry is evicted when the buffer is full."""
    agg = EvidenceAggregator()
    for i in range(7):
        agg.collect(_make_event(log=f"line-{i}"))
    evidence = agg.collect(_make_event(log="line-7"))
    # After 8 events (7 + 1 in this call), buffer = [3,4,5,6,7]
    assert evidence == ["line-3", "line-4", "line-5", "line-6", "line-7"]


def test_custom_max_per_key():
    """Custom max_per_key limits the buffer correctly."""
    agg = EvidenceAggregator(max_per_key=3)
    result: List[str] = []
    for i in range(5):
        result = agg.collect(_make_event(log=f"line-{i}"))
    assert result == ["line-2", "line-3", "line-4"]
    assert len(result) == 3


# ── per-key isolation tests ───────────────────────────────────────────── #


def test_different_entities_are_isolated():
    """Events for different entity_ids maintain separate buffers."""
    agg = EvidenceAggregator()
    for i in range(3):
        agg.collect(_make_event(entity_id="svc-a", log=f"a-{i}"))
        agg.collect(_make_event(entity_id="svc-b", log=f"b-{i}"))

    ev_a = agg.get_evidence("svc-a", "BRUTE_FORCE")
    ev_b = agg.get_evidence("svc-b", "BRUTE_FORCE")
    assert ev_a == ["a-0", "a-1", "a-2"]
    assert ev_b == ["b-0", "b-1", "b-2"]


def test_different_attack_types_are_isolated():
    """Events for different attack_types maintain separate buffers."""
    agg = EvidenceAggregator()
    agg.collect(_make_event(attack_type="BRUTE_FORCE", log="bf-1"))
    agg.collect(_make_event(attack_type="BRUTE_FORCE", log="bf-2"))
    agg.collect(_make_event(attack_type="PRIVILEGE_ESCALATION", log="pe-1"))

    ev_bf = agg.get_evidence("svc-a", "BRUTE_FORCE")
    ev_pe = agg.get_evidence("svc-a", "PRIVILEGE_ESCALATION")
    assert ev_bf == ["bf-1", "bf-2"]
    assert ev_pe == ["pe-1"]


def test_full_isolation_entity_and_attack_type():
    """Combination of entity_id and attack_type forms the isolation key."""
    agg = EvidenceAggregator()
    agg.collect(_make_event(entity_id="x", attack_type="A", log="xa-1"))
    agg.collect(_make_event(entity_id="x", attack_type="B", log="xb-1"))
    agg.collect(_make_event(entity_id="y", attack_type="A", log="ya-1"))

    assert agg.get_evidence("x", "A") == ["xa-1"]
    assert agg.get_evidence("x", "B") == ["xb-1"]
    assert agg.get_evidence("y", "A") == ["ya-1"]


# ── empty / missing fields ────────────────────────────────────────────── #


def test_empty_buffer_returns_empty_list():
    """get_evidence on unknown key returns empty list."""
    agg = EvidenceAggregator()
    assert agg.get_evidence("unknown", "UNKNOWN") == []


def test_missing_log_message_description_uses_empty_string():
    """Event without log/message/description yields empty string entry."""
    agg = EvidenceAggregator()
    event = {"entity_id": "svc-a", "attack_type": "BRUTE_FORCE"}
    result = agg.collect(event)
    assert result == [""]


def test_message_field_fallback():
    """When 'log' is absent, 'message' field is used."""
    agg = EvidenceAggregator()
    event = {"entity_id": "svc-a", "attack_type": "X", "message": "msg-text"}
    result = agg.collect(event)
    assert result == ["msg-text"]


def test_description_field_fallback():
    """When 'log' and 'message' are absent, 'description' is used."""
    agg = EvidenceAggregator()
    event = {"entity_id": "svc-a", "attack_type": "X", "description": "desc-text"}
    result = agg.collect(event)
    assert result == ["desc-text"]


def test_missing_entity_id_and_attack_type_default_to_empty():
    """Missing entity_id/attack_type default to empty strings as key."""
    agg = EvidenceAggregator()
    event: Dict[str, Any] = {"log": "orphan-log"}
    result = agg.collect(event)
    assert result == ["orphan-log"]
    # Should be retrievable with empty-string key
    assert agg.get_evidence("", "") == ["orphan-log"]


# ── clear tests ────────────────────────────────────────────────────────── #


def test_clear_all():
    """clear() with no args resets all buffers."""
    agg = EvidenceAggregator()
    agg.collect(_make_event(entity_id="a", log="a-1"))
    agg.collect(_make_event(entity_id="b", log="b-1"))
    agg.clear()
    assert agg.get_evidence("a", "BRUTE_FORCE") == []
    assert agg.get_evidence("b", "BRUTE_FORCE") == []


def test_clear_by_entity():
    """clear(entity_id=...) removes only that entity's buffers."""
    agg = EvidenceAggregator()
    agg.collect(_make_event(entity_id="a", log="a-1"))
    agg.collect(_make_event(entity_id="b", log="b-1"))
    agg.clear(entity_id="a")
    assert agg.get_evidence("a", "BRUTE_FORCE") == []
    assert agg.get_evidence("b", "BRUTE_FORCE") == ["b-1"]


def test_clear_by_attack_type():
    """clear(attack_type=...) removes only buffers with that attack type."""
    agg = EvidenceAggregator()
    agg.collect(_make_event(attack_type="A", log="a-1"))
    agg.collect(_make_event(attack_type="B", log="b-1"))
    agg.clear(attack_type="A")
    assert agg.get_evidence("svc-a", "A") == []
    assert agg.get_evidence("svc-a", "B") == ["b-1"]


# ── log line extraction priority ───────────────────────────────────────── #


def test_log_field_takes_priority():
    """'log' field is preferred over 'message' and 'description'."""
    agg = EvidenceAggregator()
    event = {
        "entity_id": "x",
        "attack_type": "A",
        "log": "from-log",
        "message": "from-message",
        "description": "from-description",
    }
    result = agg.collect(event)
    assert result == ["from-log"]


def test_message_over_description():
    """'message' is preferred when 'log' is absent."""
    agg = EvidenceAggregator()
    event = {
        "entity_id": "x",
        "attack_type": "A",
        "message": "from-message",
        "description": "from-description",
    }
    result = agg.collect(event)
    assert result == ["from-message"]


# ── custom max boundary tests ──────────────────────────────────────────── #


def test_max_per_key_of_1():
    """With max=1, only the most recent event is kept."""
    agg = EvidenceAggregator(max_per_key=1)
    agg.collect(_make_event(log="first"))
    result = agg.collect(_make_event(log="second"))
    assert result == ["second"]


def test_max_per_key_of_0():
    """With max=0 the deque never stores anything (deque maxlen=0)."""
    agg = EvidenceAggregator(max_per_key=0)
    result = agg.collect(_make_event(log="anything"))
    assert result == []
