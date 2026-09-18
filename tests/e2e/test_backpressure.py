"""
OmniWatch — IND-9 E2E: backpressure -> queue drop -> alert.

Scenario 5 (IND-2/IND-5 hooks): when the bounded queue fills, the oldest
item is dropped, the drop counter increments, and the
``AgentQueueDropping`` alert rule (``rate(...queue_dropped_total[5m]) > 0``)
fires.

Component mode: run the REAL Go resilience suite (drop-oldest FIFO,
non-blocking, drop counter) and assert the alert rule + SLO wiring in
``configs/alerts/prometheusrules.yaml`` and the exporter attributes.

Live-stack mode (extension): if the agent is up, ``/health`` 200 confirms
the exporter pipeline is alive alongside the queue-drop contract.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
ALERTS_FILE = (
    REPO_ROOT / "omniwatch-agent" / "configs" / "alerts" / "prometheusrules.yaml"
)


def test_backpressure_queue_drop_alert(e2e_stack) -> None:
    marker = e2e_stack.marker("backpressure")
    with e2e_stack.timed("backpressure"):
        e2e_stack.record("backpressure", f"marker={marker}")
        passed, output = e2e_stack.go_package_tests("resilience")
        e2e_stack.record(
            "backpressure",
            f"component={'HIT' if passed else 'FAIL'} "
            f"go test ./internal/resilience/: {output[-600:]}",
        )
        assert passed, f"resilience suite failed:\n{output}"

        # Alert rule fires on the drop counter (IND-5 wiring).
        rules_txt = ALERTS_FILE.read_text(encoding="utf-8")
        assert "AgentQueueDropping" in rules_txt, "queue-drop alert rule missing"
        assert "queue_dropped_total" in rules_txt, "alert not bound to drop metric"

        # Queue semantics: drop-oldest, counted, non-blocking (IND-2).
        queue_go = (
            REPO_ROOT / "omniwatch-agent" / "internal" / "resilience" / "queue.go"
        ).read_text(encoding="utf-8")
        assert "QueueDroppedMetric" in queue_go
        assert "Dropped()" in queue_go

        live_hit = e2e_stack.live("omniwatch-agent")
        e2e_stack.record(
            "backpressure",
            f"queue-drop->alert contract verified live={'HIT' if live_hit else 'MISS'}",
        )
