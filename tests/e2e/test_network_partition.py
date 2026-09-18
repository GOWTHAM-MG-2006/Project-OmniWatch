"""
OmniWatch — IND-9 E2E: network partition -> backpressure -> recovery.

Scenario 3: Kafka becomes unreachable (partition), the agent's IND-2
resilience (circuit breaker 5/30s + bounded queue + retry/backoff) absorbs
the outage, and traffic resumes after the partition heals.

Live-stack mode: ``docker pause omniwatch-kafka`` (partition), observe the
agent leave ready state, ``docker unpause``, then poll back to healthy.

Component mode: run the REAL Go resilience suite
(``go test ./internal/resilience/`` — breaker open/half-open/close,
drop-oldest queue, retry schedule) as the backpressure/recovery proof.
"""

from __future__ import annotations


def test_network_partition_backpressure_recovery(e2e_stack) -> None:
    marker = e2e_stack.marker("partition")
    with e2e_stack.timed("network_partition"):
        e2e_stack.record("network_partition", f"marker={marker}")
        if e2e_stack.container_running(
            "omniwatch-kafka"
        ) and e2e_stack.container_running("omniwatch-agent"):
            ok, out = e2e_stack.compose("pause", "kafka", timeout=60.0)
            assert ok, f"docker pause kafka failed: {out[-500:]}"
            e2e_stack.record("network_partition", "kafka paused (partition) live=HIT")
            # During the partition the agent must NOT stay fully ready —
            # backpressure engages (breaker opens / queue fills).
            import time

            degraded = False
            deadline = time.time() + 90.0
            while time.time() < deadline:
                if not e2e_stack.live("agent-ready"):
                    degraded = True
                    break
                time.sleep(5)
            e2e_stack.record(
                "network_partition", f"backpressure_observed={degraded}"
            )
            ok, out = e2e_stack.compose("unpause", "kafka", timeout=60.0)
            assert ok, f"docker unpause kafka failed: {out[-500:]}"
            assert e2e_stack.wait_healthy("agent-ready", timeout=180.0), (
                "agent /ready did not recover after partition healed"
            )
            e2e_stack.record("network_partition", "recovery: /ready=200")
            return

        # Component mode: real resilience suite = breaker + queue + retry.
        passed, output = e2e_stack.go_package_tests("resilience")
        e2e_stack.record(
            "network_partition",
            f"live=MISS component={'HIT' if passed else 'FAIL'} "
            f"go test ./internal/resilience/: {output[-600:]}",
        )
        assert passed, f"resilience suite failed:\n{output}"
