"""Tests for the K8sEventIntegration Kubernetes event baseline adjustment."""

import subprocess
import sys
import textwrap

import pytest

from predictive.k8s_integration import (
    COOLDOWN_SECONDS,
    RELEVANCE_WINDOW_SECONDS,
    RELEVANT_EVENT_ADJUSTMENT,
    K8sEventIntegration,
)


# ---------------------------------------------------------------------------
# Test doubles (fake k8s client + controllable clock). These fake only the
# external API boundary; the cooldown, relevance, and adjustment logic under
# test is the real module code.
# ---------------------------------------------------------------------------


class _FakeEvent:
    """Minimal stand-in for a kubernetes V1Event."""

    def __init__(self, reason: str = "", type_: str = "Normal", message: str = "") -> None:
        self.reason = reason
        self.type = type_
        self.message = message


class _FakeEventList:
    def __init__(self, events) -> None:
        self.items = events


class _FakeClient:
    """Fake CoreV1Api that records how many times the API is hit."""

    def __init__(self, events) -> None:
        self._events = events
        self.call_count = 0

    def list_namespaced_event(self, namespace=None, limit=None, **kwargs):
        self.call_count += 1
        return _FakeEventList(self._events)


class _FakeClock:
    def __init__(self, start: float = 1000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now


def _relevant_event() -> _FakeEvent:
    return _FakeEvent(reason="Evicted", type_="Warning", message="pod evicted")


def _irrelevant_event() -> _FakeEvent:
    return _FakeEvent(reason="Pulled", type_="Normal", message="container image pulled")


# ---------------------------------------------------------------------------
# Import-without-kubernetes (real behavior, subprocess with a blocked import)
# ---------------------------------------------------------------------------


def test_module_imports_cleanly_without_kubernetes():
    """The module must import and degrade to disabled when kubernetes is absent."""
    script = textwrap.dedent(
        """
        import sys

        class _Blocker:
            def find_spec(self, name, path=None, target=None):
                if name == "kubernetes" or name.startswith("kubernetes."):
                    raise ModuleNotFoundError(
                        "No module named %r (blocked for test)" % name
                    )
                return None

        sys.meta_path.insert(0, _Blocker())

        import predictive.k8s_integration as m

        k = m.K8sEventIntegration()
        assert k.enabled is False, "should be disabled without kubernetes"
        assert k.get_baseline_adjustment() == 1.0
        print("IMPORT_OK")
        """
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=sys.path[0] or ".",
    )
    assert result.returncode == 0, result.stderr
    assert "IMPORT_OK" in result.stdout


# ---------------------------------------------------------------------------
# Disabled mode
# ---------------------------------------------------------------------------


def test_disabled_mode_returns_1_0():
    """A disabled integration always returns 1.0 and never touches the API."""
    k = K8sEventIntegration(client=None)
    assert k.enabled is False
    assert k.get_baseline_adjustment() == 1.0
    assert k.get_baseline_adjustment() == 1.0


# ---------------------------------------------------------------------------
# Enabled mode: adjustment values
# ---------------------------------------------------------------------------


def test_relevant_event_returns_1_5():
    """A recently observed relevant event yields the 1.5 adjustment."""
    client = _FakeClient([_relevant_event()])
    clock = _FakeClock()
    k = K8sEventIntegration(client=client, clock=clock)

    assert k.enabled is True
    assert k.get_baseline_adjustment() == RELEVANT_EVENT_ADJUSTMENT
    assert RELEVANT_EVENT_ADJUSTMENT == 1.5


def test_no_relevant_event_returns_1_0():
    """Irrelevant events do not trigger an adjustment."""
    client = _FakeClient([_irrelevant_event()])
    clock = _FakeClock()
    k = K8sEventIntegration(client=client, clock=clock)

    assert k.get_baseline_adjustment() == 1.0


def test_relevant_event_expires_after_window():
    """Once the relevance window passes, adjustment returns to 1.0."""
    client = _FakeClient([_relevant_event()])
    clock = _FakeClock()
    k = K8sEventIntegration(client=client, clock=clock)

    assert k.get_baseline_adjustment() == 1.5

    # Advance past the relevance window; the next fetch sees no relevant event.
    clock.now += RELEVANCE_WINDOW_SECONDS + COOLDOWN_SECONDS + 1
    client._events = [_irrelevant_event()]
    assert k.get_baseline_adjustment() == 1.0


# ---------------------------------------------------------------------------
# Cooldown: no repeated API calls within 5 minutes
# ---------------------------------------------------------------------------


def test_cooldown_prevents_repeated_api_calls():
    """Within the cooldown window the API is not hit again."""
    client = _FakeClient([_relevant_event()])
    clock = _FakeClock()
    k = K8sEventIntegration(client=client, clock=clock)

    assert k.get_baseline_adjustment() == 1.5
    assert client.call_count == 1

    # Advance 299s (still inside the 300s cooldown) -> no new API call.
    clock.now += COOLDOWN_SECONDS - 1
    assert k.get_baseline_adjustment() == 1.5
    assert client.call_count == 1

    # Advance past the cooldown -> a new API call is made.
    clock.now += 2
    assert k.get_baseline_adjustment() == 1.5
    assert client.call_count == 2


# ---------------------------------------------------------------------------
# reset()
# ---------------------------------------------------------------------------


def test_reset_clears_relevant_event_state():
    """reset() clears cached event state so adjustment returns to 1.0."""
    client = _FakeClient([_relevant_event()])
    clock = _FakeClock()
    k = K8sEventIntegration(client=client, clock=clock)

    assert k.get_baseline_adjustment() == 1.5
    k.reset()
    assert k.get_baseline_adjustment() == 1.0


# ---------------------------------------------------------------------------
# enabled property
# ---------------------------------------------------------------------------


def test_enabled_property_reflects_mode():
    """enabled is True with a client, False without one."""
    assert K8sEventIntegration(client=_FakeClient([])).enabled is True
    assert K8sEventIntegration(client=None).enabled is False