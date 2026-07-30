"""
OmniWatch — Simulation Anomaly Injector (CLI)
Component: anomaly_injector.py
Phase: 1
Purpose: CLI tool to inject anomaly scenarios into running microservices for E2E testing.
         Calls the /__inject/anomaly endpoints on each microservice.
Inputs: CLI arguments (--scenario, --ttl, --service, --list, --clear)
Outputs: JSON responses from the target service's anomaly injection endpoint

Usage:
    python simulation/anomaly_injector.py --scenario database_cascade
    python simulation/anomaly_injector.py --scenario memory_leak --ttl 120
    python simulation/anomaly_injector.py --scenario security_attack --service custom-service
    python simulation/anomaly_injector.py --list
    python simulation/anomaly_injector.py --clear
    python simulation/anomaly_injector.py --clear-scenario database_cascade
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.request
import urllib.error
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Default target services for each scenario.
# The CLI injects into ALL listed services for a scenario unless --service is given.
SCENARIO_TARGETS: dict[str, list[dict[str, Any]]] = {
    "database_cascade": [
        {"host": "localhost:8001", "name": "user-service"},
        {"host": "localhost:8002", "name": "order-service"},
    ],
    "memory_leak": [
        {"host": "localhost:8001", "name": "user-service"},
        {"host": "localhost:8002", "name": "order-service"},
    ],
    "latency_spike": [
        {"host": "localhost:8001", "name": "user-service"},
        {"host": "localhost:8002", "name": "order-service"},
    ],
    "security_attack": [
        {"host": "localhost:8001", "name": "user-service"},
        {"host": "localhost:8000", "name": "api-gateway"},
    ],
    "config_drift": [
        {"host": "localhost:8000", "name": "api-gateway"},
        {"host": "localhost:8002", "name": "order-service"},
    ],
}

VALID_SCENARIOS = list(SCENARIO_TARGETS.keys())

ANOMALY_PATH = "/__inject/anomaly"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _request(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
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
# Actions
# ---------------------------------------------------------------------------


def inject(scenario: str, ttl: int, targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """POST the anomaly to each target service."""
    results: list[dict[str, Any]] = []
    for tgt in targets:
        url = f"http://{tgt['host']}{ANOMALY_PATH}"
        print(f"  -> {tgt['name']} ({url})")
        result = _request("POST", url, {"scenario": scenario, "ttl_seconds": ttl})
        results.append({"service": tgt["name"], "host": tgt["host"], "response": result})
        if result.get("error"):
            print(f"    [FAIL] {result['detail']}")
        else:
            print(f"    [OK] injected — expires at {result.get('expires_at', '?')}")
    return results


def list_active(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """GET active anomalies from each target service."""
    results: list[dict[str, Any]] = []
    for tgt in targets:
        url = f"http://{tgt['host']}{ANOMALY_PATH}"
        print(f"  -> {tgt['name']} ({url})")
        result = _request("GET", url)
        results.append({"service": tgt["name"], "host": tgt["host"], "response": result})
        if result.get("error"):
            print(f"    [FAIL] {result['detail']}")
        else:
            active = result.get("active", [])
            if active:
                for a in active:
                    print(f"    ! {a['scenario']} ({a['remaining_seconds']:.0f}s remaining)")
            else:
                print(f"    [OK] no active anomalies")
    return results


def clear_all(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """DELETE all anomalies from each target service."""
    results: list[dict[str, Any]] = []
    for tgt in targets:
        url = f"http://{tgt['host']}{ANOMALY_PATH}"
        print(f"  -> {tgt['name']} ({url})")
        result = _request("DELETE", url)
        results.append({"service": tgt["name"], "host": tgt["host"], "response": result})
        if result.get("error"):
            print(f"    [FAIL] {result['detail']}")
        else:
            print(f"    [OK] cleared")
    return results


def clear_scenario(scenario: str, targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """DELETE a specific scenario from each target service."""
    results: list[dict[str, Any]] = []
    for tgt in targets:
        url = f"http://{tgt['host']}{ANOMALY_PATH}/{scenario}"
        print(f"  -> {tgt['name']} ({url})")
        result = _request("DELETE", url)
        results.append({"service": tgt["name"], "host": tgt["host"], "response": result})
        if result.get("error"):
            print(f"    [FAIL] {result['detail']}")
        else:
            print(f"    [OK] cleared")
    return results


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _all_services() -> list[dict[str, Any]]:
    """Return all known service targets."""
    seen: set[tuple[str, str]] = set()
    result: list[dict[str, Any]] = []
    for targets in SCENARIO_TARGETS.values():
        for tgt in targets:
            key = (tgt["host"], tgt["name"])
            if key not in seen:
                seen.add(key)
                result.append(tgt)
    return sorted(result, key=lambda x: x["host"])


def _resolve_targets(scenario: str | None, service: str | None) -> list[dict[str, Any]]:
    """Resolve the list of target services to hit."""
    if service:
        # User specified an explicit service host:port
        return [{"host": service, "name": service}]

    if scenario:
        targets = SCENARIO_TARGETS.get(scenario)
        if not targets:
            print(f"Unknown scenario '{scenario}'. Valid: {VALID_SCENARIOS}")
            sys.exit(1)
        return targets

    return _all_services()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="OmniWatch anomaly injection CLI — inject scenarios into microservices.",
    )
    parser.add_argument(
        "--scenario", "-s",
        choices=VALID_SCENARIOS,
        help=f"Anomaly scenario to inject. Valid: {VALID_SCENARIOS}",
    )
    parser.add_argument(
        "--ttl",
        type=int,
        default=60,
        help="Time-to-live in seconds (default: 60)",
    )
    parser.add_argument(
        "--service",
        help="Target a specific service (host:port, e.g. localhost:8001). Overrides scenario defaults.",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="List active anomalies on all services.",
    )
    parser.add_argument(
        "--clear", "-c",
        action="store_true",
        help="Clear all anomalies on all services.",
    )
    parser.add_argument(
        "--clear-scenario",
        help="Clear a specific scenario on all services.",
    )

    args = parser.parse_args()

    # -- Determine action --
    if args.list:
        targets = _resolve_targets(None, args.service) if args.service else _all_services()
        print(f"Listing active anomalies on {len(targets)} service(s)...")
        list_active(targets)

    elif args.clear:
        targets = _resolve_targets(None, args.service) if args.service else _all_services()
        print(f"Clearing all anomalies on {len(targets)} service(s)...")
        clear_all(targets)

    elif args.clear_scenario:
        if args.clear_scenario not in VALID_SCENARIOS:
            print(f"Unknown scenario '{args.clear_scenario}'. Valid: {VALID_SCENARIOS}")
            sys.exit(1)
        targets = _resolve_targets(args.clear_scenario, args.service)
        print(f"Clearing '{args.clear_scenario}' on {len(targets)} service(s)...")
        clear_scenario(args.clear_scenario, targets)

    elif args.scenario:
        targets = _resolve_targets(args.scenario, args.service)
        print(f"Injecting '{args.scenario}' (TTL={args.ttl}s) into {len(targets)} service(s)...")
        inject(args.scenario, args.ttl, targets)

    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
