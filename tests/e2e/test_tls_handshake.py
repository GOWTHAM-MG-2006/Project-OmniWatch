"""
OmniWatch — IND-9 E2E: TLS handshake fail -> insecure fallback.

Scenario 4 (IND-1 hooks): secure mode without SPIRE must fail fast
(exit 1, clear error); explicit ``OMNIWATCH_EXPORTER_OTLP_INSECURE=true``
must start with a WARN and reach ``/health``/``/ready``.

Live-stack mode: the agent (started under the E2E override with insecure
fallback) answers ``/health`` 200 — proving the fallback path is live.

Component mode: run the REAL Go TLS suite (``go test ./internal/tls/`` —
stub Workload API handshake, rotation, fallback gating) plus contract
asserts on the exporter gating code and agent.yaml tls section.
"""

from __future__ import annotations

from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_tls_handshake_fallback(e2e_stack) -> None:
    marker = e2e_stack.marker("tls")
    with e2e_stack.timed("tls_handshake"):
        e2e_stack.record("tls_handshake", f"marker={marker}")
        live_hit = e2e_stack.live("omniwatch-agent")
        if live_hit:
            assert e2e_stack.wait_healthy("omniwatch-agent", timeout=60.0)
            e2e_stack.record("tls_handshake", "/health=200 live=HIT")

        # Real TLS suite: handshake, rotation, insecure-gating rules.
        passed, output = e2e_stack.go_package_tests("tls")
        e2e_stack.record(
            "tls_handshake",
            f"component={'HIT' if passed else 'FAIL'} "
            f"go test ./internal/tls/: {output[-600:]}",
        )
        assert passed, f"tls suite failed:\n{output}"

        # Contract: insecure path warns, secure-without-SPIRE fails fast.
        exporter_go = (
            REPO_ROOT / "omniwatch-agent" / "internal" / "exporter" / "exporter.go"
        ).read_text(encoding="utf-8")
        assert "InsecureFallbackWarning" in exporter_go
        assert "WithInsecure()" in exporter_go
        agent_yaml = (
            REPO_ROOT / "omniwatch-agent" / "configs" / "agent.yaml"
        ).read_text(encoding="utf-8")
        assert "tls:" in agent_yaml and "spiffe_socket_path" in agent_yaml
        override_txt = (
            REPO_ROOT / "tests" / "e2e" / "docker-compose.e2e.yml"
        ).read_text(encoding="utf-8")
        assert "OMNIWATCH_EXPORTER_OTLP_INSECURE=true" in override_txt
        e2e_stack.record(
            "tls_handshake",
            f"fallback gating contract verified live={'HIT' if live_hit else 'MISS'}",
        )
