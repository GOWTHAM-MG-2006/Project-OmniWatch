"""
OmniWatch — Security Event Generator (CLI)
Component: security_event_generator.py
Phase: 1
Purpose: Continuously generates security events (brute force, privilege escalation,
         config drift, data exfiltration) and pushes them to the anomaly injection
         endpoints for E2E testing of the security pipeline.
Inputs: CLI arguments (--scenario, --interval, --count, --target)
Outputs: Security events injected into target services via /__inject/anomaly

Usage:
    python simulation/security_event_generator.py --scenario security_attack --interval 5
    python simulation/security_event_generator.py --scenario config_drift --count 20
    python simulation/security_event_generator.py --all --interval 3
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Default targets for each security scenario
SECURITY_TARGETS: dict[str, list[dict[str, Any]]] = {
    "security_attack": [
        {"host": "localhost:8001", "name": "user-service"},
        {"host": "localhost:8000", "name": "api-gateway"},
    ],
    "config_drift": [
        {"host": "localhost:8000", "name": "api-gateway"},
        {"host": "localhost:8002", "name": "order-service"},
    ],
}

VALID_SCENARIOS = list(SECURITY_TARGETS.keys())
ANOMALY_PATH = "/__inject/anomaly"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _request(method: str, url: str, body: dict[str, Any] | None = None) -> dict[str, Any]:
    """Make an HTTP request and return parsed JSON."""
    data = json.dumps(body).encode("utf-8") if body else None
    req = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"} if body else {},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        body_bytes = e.read()
        detail = body_bytes.decode("utf-8") if body_bytes else str(e)
        return {"error": True, "status": e.code, "detail": detail}
    except urllib.error.URLError as e:
        return {"error": True, "detail": f"Connection failed: {e.reason}"}
    except Exception as e:
        return {"error": True, "detail": str(e)}


# ---------------------------------------------------------------------------
# Security event generators
# ---------------------------------------------------------------------------


def inject_security_event(
    scenario: str,
    ttl: int,
    targets: list[dict[str, Any]],
    event_number: int,
) -> dict[str, Any]:
    """Inject a single security event and return the result."""
    results: dict[str, Any] = {}
    for tgt in targets:
        url = f"http://{tgt['host']}{ANOMALY_PATH}"
        print(f"  [{event_number}] {tgt['name']} <- {scenario} (TTL={ttl}s)")
        result = _request("POST", url, {"scenario": scenario, "ttl_seconds": ttl})
        results[tgt["name"]] = result
        if result.get("error"):
            print(f"    [FAIL] {result['detail']}")
        else:
            expires = result.get("expires_at", "?")
            print(f"    [OK] expires at {expires}")
    return results


def generate_brute_force_events(interval: float, count: int, ttl: int) -> None:
    """Simulate repeated brute force login attempts."""
    print(f"\n[BRUTE] Generating {count} brute force security events every {interval}s...\n")
    targets = SECURITY_TARGETS["security_attack"]
    for i in range(count):
        print(f"\n--- Event {i + 1}/{count} ---")
        inject_security_event("security_attack", ttl, targets, i + 1)
        if i < count - 1:
            time.sleep(interval)
    print("\n[OK] Brute force simulation complete.")


def generate_config_drift_events(interval: float, count: int, ttl: int) -> None:
    """Simulate configuration drift detection events."""
    print(f"\n[DRIFT] Generating {count} config drift events every {interval}s...\n")
    targets = SECURITY_TARGETS["config_drift"]
    for i in range(count):
        print(f"\n--- Event {i + 1}/{count} ---")
        inject_security_event("config_drift", ttl, targets, i + 1)
        if i < count - 1:
            time.sleep(interval)
    print("\n[OK] Config drift simulation complete.")


def generate_all_events(interval: float, count: int, ttl: int) -> None:
    """Interleave all security scenarios."""
    print(f"\n[MIX] Generating interleaved security events every {interval}s...\n")
    scenarios = list(SECURITY_TARGETS.items())  # [(name, targets), ...]
    for i in range(count):
        scenario_name, targets = scenarios[i % len(scenarios)]
        print(f"\n--- Event {i + 1}/{count}: {scenario_name} ---")
        inject_security_event(scenario_name, ttl, targets, i + 1)
        if i < count - 1:
            time.sleep(interval)
    print("\n[OK] Interleaved security simulation complete.")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OmniWatch security event generator — simulates security incidents for E2E testing.",
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=VALID_SCENARIOS,
        help=f"Security scenario to inject. Valid: {VALID_SCENARIOS}",
    )
    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="Interleave all security scenarios.",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Seconds between events (default: 5.0)",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=10,
        help="Number of events to generate (default: 10)",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=30,
        help="Anomaly TTL in seconds (default: 30)",
    )

    args = parser.parse_args()

    if args.all:
        generate_all_events(args.interval, args.count, args.ttl)
    elif args.scenario == "security_attack":
        generate_brute_force_events(args.interval, args.count, args.ttl)
    elif args.scenario == "config_drift":
        generate_config_drift_events(args.interval, args.count, args.ttl)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
