"""
OmniWatch — IND-9 E2E shared fixture (session-scoped compose harness).

Modes
-----
* **live** — full compose stack reachable: scenarios assert against the real
  services (dashboard-api :8011, agent :8080, kafka, ...).
* **component** — no stack (or partial stack): scenarios drive the REAL
  pipeline code (predictive / causal / prioritization) and the REAL Go
  resilience/TLS test suites. Nothing is faked; the mode is recorded in the
  per-scenario evidence files.

The fixture NEVER modifies the committed ``docker-compose.yml`` — all E2E
knobs live in ``tests/e2e/docker-compose.e2e.yml`` (override file only) and
are validated with ``docker compose config``.

Teardown: nothing is torn down that this fixture did not start (it starts
nothing by default — full-stack bring-up is an explicit operator step, so a
local dev stack is never wiped). If the Docker daemon is absent the suite
runs in component mode with a clear recorded message.
"""

from __future__ import annotations

import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any
import uuid

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
E2E_DIR = Path(__file__).resolve().parent
OVERRIDE_FILE = E2E_DIR / "docker-compose.e2e.yml"
EVIDENCE_DIR = REPO_ROOT / ".omo" / "evidence"

# localhost health endpoints for the compose stack (AGENTS.md ports).
SERVICE_HEALTH: dict[str, str] = {
    "omniwatch-agent": "http://localhost:8080/health",
    "agent-ready": "http://localhost:8080/ready",
    "predictive": "http://localhost:8007/health",
    "causal": "http://localhost:8008/health",
    "prioritization": "http://localhost:8009/health",
    "orchestration": "http://localhost:8010/health",
    "dashboard-api": "http://localhost:8011/health",
    "genai": "http://localhost:8020/health",
    "learning": "http://localhost:8030/health",
    "clickhouse": "http://localhost:8123/ping",
    "neo4j": "http://localhost:7474/",
    "minio": "http://localhost:9001/minio/health/live",
}

GO_BIN = r"E:\Go\bin\go.exe"
GO_ENV = {
    "GOCACHE": r"E:\OmniWatch-Test-Files\.gocache",
    "GOTMPDIR": r"E:\OmniWatch-Test-Files\.gocache",
}


class E2EStack:
    """Session harness: liveness probes, compose runner, evidence log."""

    def __init__(self) -> None:
        self.started_at = time.time()
        self.notes: dict[str, list[str]] = {}
        self.timings: dict[str, float] = {}
        self.docker_available = self._docker_available()
        self.override_valid, self.override_msg = self._validate_override()
        self._live_cache: dict[str, bool] = {}
        self._go_cache: dict[str, tuple[bool, str]] = {}
        self.poll_timeout = 120.0
        self.record(
            "stack",
            f"docker_available={self.docker_available} "
            f"override_valid={self.override_valid} ({self.override_msg})",
        )

    # -- helpers ------------------------------------------------------
    def _run(
        self, *args: str, timeout: float = 60.0, cwd: Path | None = None
    ) -> tuple[bool, str]:
        try:
            proc = subprocess.run(
                list(args),
                capture_output=True,
                text=True,
                timeout=timeout,
                cwd=str(cwd or REPO_ROOT),
            )
            out = (proc.stdout + proc.stderr)[-4000:]
            return proc.returncode == 0, out.strip()
        except (OSError, subprocess.TimeoutExpired) as exc:
            return False, f"{type(exc).__name__}: {exc}"

    def _docker_available(self) -> bool:
        ok, _ = self._run("docker", "info", "--format", "{{.ServerVersion}}",
                          timeout=20.0)
        return ok

    def _validate_override(self) -> tuple[bool, str]:
        if not self.docker_available:
            return False, "docker daemon absent — override check deferred"
        if not OVERRIDE_FILE.exists():
            return False, "override file missing"
        ok, out = self._run(
            "docker", "compose", "-f", "docker-compose.yml",
            "-f", str(OVERRIDE_FILE), "config", "--quiet", timeout=60.0,
        )
        return ok, "compose config ok" if ok else f"compose config FAILED: {out[-500:]}"

    def live(self, name: str, timeout: float = 4.0) -> bool:
        """True when a stack service answers its health endpoint."""
        if name in self._live_cache:
            return self._live_cache[name]
        url = SERVICE_HEALTH.get(name)
        if url is None:
            return False
        try:
            with urllib.request.urlopen(url, timeout=timeout) as resp:
                ok = 200 <= resp.status < 500
        except Exception:
            ok = False
        self._live_cache[name] = ok
        return ok

    def wait_healthy(
        self, name: str, timeout: float = 120.0, want: int = 200
    ) -> bool:
        """Poll a health endpoint until HTTP `want` or timeout."""
        url = SERVICE_HEALTH[name]
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=5) as resp:
                    if resp.status == want:
                        self._live_cache[name] = True
                        return True
            except Exception:
                pass
            time.sleep(5)
        return False

    def compose(self, *args: str, timeout: float = 120.0) -> tuple[bool, str]:
        """Run `docker compose -f base -f override <args>`."""
        return self._run(
            "docker", "compose", "-f", "docker-compose.yml",
            "-f", str(OVERRIDE_FILE), *args, timeout=timeout,
        )

    def container_running(self, name: str) -> bool:
        if not self.docker_available:
            return False
        ok, out = self._run(
            "docker", "ps", "--filter", f"name={name}",
            "--format", "{{.Names}}", timeout=20.0,
        )
        return ok and name in out

    def go_package_tests(self, package: str) -> tuple[bool, str]:
        """Run the REAL Go test suite for one agent package (cached)."""
        if package in self._go_cache:
            return self._go_cache[package]
        import os

        env = dict(os.environ)
        env.update(GO_ENV)
        Path(GO_ENV["GOCACHE"]).mkdir(parents=True, exist_ok=True)
        try:
            proc = subprocess.run(
                [GO_BIN, "test", f"./internal/{package}/", "-count=1"],
                capture_output=True,
                text=True,
                timeout=300.0,
                cwd=str(REPO_ROOT / "omniwatch-agent"),
                env=env,
            )
            result = (
                proc.returncode == 0,
                (proc.stdout + proc.stderr)[-4000:].strip(),
            )
        except (OSError, subprocess.TimeoutExpired) as exc:
            result = (False, f"{type(exc).__name__}: {exc}")
        self._go_cache[package] = result
        return result

    def marker(self, scenario: str) -> str:
        """Unique incident marker for log correlation (order-independent)."""
        return f"e2e-{scenario}-{uuid.uuid4().hex[:8]}"

    def record(self, scenario: str, message: str) -> None:
        elapsed = time.time() - self.started_at
        self.notes.setdefault(scenario, []).append(f"[+{elapsed:7.1f}s] {message}")

    def timed(self, scenario: str) -> Any:
        """Context manager recording per-scenario wall time."""
        stack = self

        class _Timer:
            def __enter__(self) -> "_Timer":
                self.t0 = time.time()
                return self

            def __exit__(self, *exc: Any) -> None:
                stack.timings[scenario] = time.time() - self.t0
                stack.record(scenario, f"duration={stack.timings[scenario]:.1f}s")

        return _Timer()


@pytest.fixture(scope="session")
def e2e_stack() -> Any:
    """Session-scoped stack harness with evidence flush on teardown."""
    stack = E2EStack()
    yield stack
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    for scenario, lines in stack.notes.items():
        if scenario == "stack":
            continue
        path = EVIDENCE_DIR / f"industry-09-e2e-{scenario}.txt"
        header = (
            f"IND-9 E2E scenario: {scenario}\n"
            f"mode: {'live' if any('live=HIT' in ln for ln in lines) else 'component'}\n"
        )
        path.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
    stack_path = EVIDENCE_DIR / "industry-09-e2e-stack.txt"
    stack_path.write_text(
        "IND-9 E2E stack fixture log\n" + "\n".join(stack.notes.get("stack", [])) + "\n",
        encoding="utf-8",
    )
