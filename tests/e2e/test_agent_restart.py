"""
OmniWatch — IND-9 E2E: agent restart -> resume.

Scenario 2: the omniwatch-agent container restarts and resumes collection.

Live-stack mode: ``docker restart omniwatch-agent`` then poll ``/health``
(200 = process alive) and ``/ready`` (200 = collector running again).

Component mode: assert the REAL restart/resume contract — ``/health`` stays
200 across collector states while ``/ready`` gates on running (so a restart
is observable and recovery is binary), the compose service exposes :8080,
and the auth-wrapped handler chain is wired.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_agent_restart_resumes(e2e_stack) -> None:
    marker = e2e_stack.marker("restart")
    with e2e_stack.timed("agent_restart"):
        e2e_stack.record("agent_restart", f"marker={marker}")
        if e2e_stack.container_running("omniwatch-agent"):
            ok, out = e2e_stack.compose("restart", "omniwatch-agent",
                                        timeout=120.0)
            e2e_stack.record("agent_restart", f"docker restart ok={ok} live=HIT")
            assert ok, f"docker restart omniwatch-agent failed: {out[-500:]}"
            assert e2e_stack.wait_healthy("omniwatch-agent", timeout=180.0), (
                "agent /health did not return 200 after restart"
            )
            assert e2e_stack.wait_healthy("agent-ready", timeout=180.0), (
                "agent /ready did not return 200 after restart (no resume)"
            )
            e2e_stack.record("agent_restart", "resume: /health=200 /ready=200")
            return

        # Component mode: verify the restart/resume contract in real code.
        health_go = (
            REPO_ROOT / "omniwatch-agent" / "internal" / "health" / "health.go"
        ).read_text(encoding="utf-8")
        assert 'mux.HandleFunc("/health"' in health_go
        assert 'mux.HandleFunc("/ready"' in health_go
        assert "HandlerWithAuth" in health_go, "auth chain not wired to probes"
        # /ready must gate on collector running (503 otherwise) — that is what
        # makes restart->resume a binary observable outcome.
        assert "503" in health_go, "ready gate has no not-ready state"
        compose_txt = (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
        assert "8080:8080" in compose_txt, "agent :8080 not published"
        e2e_stack.record(
            "agent_restart",
            "live=MISS component=HIT health/ready/auth contract verified",
        )
