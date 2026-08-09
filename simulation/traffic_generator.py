"""
OmniWatch — Simulation Traffic Generator
Component: traffic_generator.py
Phase: 1
Purpose: Drive real business requests to order-service and user-service so OTel
         metric instruments in route handlers fire during anomaly-injection runs.
Inputs: CLI arguments (--rps, --duration, --scenario-active, --order-host, --user-host)
Outputs: Structured JSON logs to stdout; HTTP requests to /api/v1/* endpoints

Usage:
    # Fixed-duration mode — send 30 RPS for 90 seconds
    python simulation/traffic_generator.py --rps 30 --duration 90

    # Scenario-aware mode — auto-start/stop based on active anomalies
    python simulation/traffic_generator.py --rps 30 --scenario-active

    # Custom hosts (useful outside Docker)
    python simulation/traffic_generator.py --rps 10 --duration 30 \\
        --order-host localhost:8002 --user-host localhost:8001
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from typing import Any

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# Service hosts (localhost ports from docker-compose.yml)
DEFAULT_ORDER_HOST = "localhost:8002"
DEFAULT_USER_HOST = "localhost:8001"
ANOMALY_PATH = "/__inject/anomaly"

# Product catalogue for order creation
PRODUCTS = [
    {"product_id": "SKU-001", "name": "Cloud Widget"},
    {"product_id": "SKU-002", "name": "Monitoring Agent"},
    {"product_id": "SKU-003", "name": "Log Aggregator"},
    {"product_id": "SKU-004", "name": "Alert Router"},
    {"product_id": "SKU-005", "name": "Metrics Dashboard"},
]

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logger = logging.getLogger("omniwatch.traffic_generator")


def _json_log(level: str, msg: str, **fields: Any) -> None:
    """Emit a structured JSON log line."""
    record = {"level": level, "msg": msg, "ts": time.time(), **fields}
    print(json.dumps(record), flush=True)


# ---------------------------------------------------------------------------
# HTTP helpers (stdlib only — no requests/httpx dependency)
# ---------------------------------------------------------------------------


def _get(url: str, timeout: float = 5.0) -> tuple[int, Any]:
    """GET a URL; return (status_code, parsed_json_or_None)."""
    req = urllib.request.Request(url, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            data = json.loads(body) if body else None
            return resp.status, data
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except Exception:
        return 0, None


def _post(url: str, payload: dict[str, Any], timeout: float = 5.0) -> tuple[int, Any]:
    """POST JSON to a URL; return (status_code, parsed_json_or_None)."""
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except Exception:
        return 0, None


def _delete(url: str, timeout: float = 5.0) -> tuple[int, Any]:
    """DELETE a URL; return (status_code, parsed_json_or_None)."""
    req = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, json.loads(body) if body else None
    except urllib.error.HTTPError as exc:
        return exc.code, None
    except Exception:
        return 0, None


# ---------------------------------------------------------------------------
# Anomaly-aware check
# ---------------------------------------------------------------------------


def _anomaly_active(host: str) -> bool:
    """Return True if any anomaly is currently active on the given host."""
    url = f"http://{host}{ANOMALY_PATH}"
    status, data = _get(url, timeout=3.0)
    if status != 200 or data is None:
        return False
    active = data.get("active", [])
    return len(active) > 0


def _any_anomaly_active(order_host: str, user_host: str) -> bool:
    """Check both services for active anomalies."""
    return _anomaly_active(order_host) or _anomaly_active(user_host)


# ---------------------------------------------------------------------------
# Request generators
# ---------------------------------------------------------------------------


class TrafficGenerator:
    """Stateful traffic generator that maintains known user/order IDs."""

    def __init__(self, order_host: str, user_host: str) -> None:
        self.order_host = order_host
        self.user_host = user_host
        self._user_ids: list[str] = []
        self._order_ids: list[str] = []
        self._lock = threading.Lock()
        self._user_counter = 0
        self._order_counter = 0

        # Stats
        self.total_requests = 0
        self.total_errors = 0
        self.total_2xx = 0

    # -- seed data ----------------------------------------------------------

    def seed_users(self, count: int = 3) -> None:
        """Create initial users so subsequent order creates have valid user_ids."""
        for _ in range(count):
            user_id = self._create_user()
            if user_id:
                with self._lock:
                    self._user_ids.append(user_id)

    # -- individual request types -------------------------------------------

    def _create_user(self) -> str | None:
        """POST /api/v1/users/ — create a user, return its ID."""
        self._user_counter += 1
        payload = {
            "name": f"TrafficUser-{self._user_counter}",
            "email": f"traffic-{self._user_counter}-{uuid.uuid4().hex[:8]}@test.local",
        }
        status, data = _post(
            f"http://{self.user_host}/api/v1/users/", payload, timeout=5.0
        )
        self._bump_stats(status)
        if status == 201 and data:
            return data.get("id")
        return None

    def _create_order(self) -> str | None:
        """POST /api/v1/orders — create an order, return its ID."""
        with self._lock:
            user_ids = list(self._user_ids)
        if not user_ids:
            return None

        self._order_counter += 1
        product = random.choice(PRODUCTS)
        payload = {
            "user_id": random.choice(user_ids),
            "items": [
                {
                    "product_id": product["product_id"],
                    "name": product["name"],
                    "quantity": random.randint(1, 5),
                    "price": round(random.uniform(5.0, 150.0), 2),
                }
            ],
        }
        status, data = _post(
            f"http://{self.order_host}/api/v1/orders", payload, timeout=5.0
        )
        self._bump_stats(status)
        if status == 200 and data:
            order_id = data.get("id")
            if order_id:
                with self._lock:
                    self._order_ids.append(order_id)
            return order_id
        return None

    def _list_orders(self) -> int:
        """GET /api/v1/orders — list all orders."""
        status, data = _get(f"http://{self.order_host}/api/v1/orders", timeout=5.0)
        self._bump_stats(status)
        if status == 200 and isinstance(data, list):
            return len(data)
        return 0

    def _get_order(self) -> None:
        """GET /api/v1/orders/{id} — fetch a single order by ID."""
        with self._lock:
            order_ids = list(self._order_ids)
        if order_ids:
            oid = random.choice(order_ids)
        else:
            oid = str(uuid.uuid4())  # will 404 but still fires OTel instruments
        status, _ = _get(
            f"http://{self.order_host}/api/v1/orders/{oid}", timeout=5.0
        )
        self._bump_stats(status)

    def _list_user_orders(self) -> None:
        """GET /api/v1/orders/users/{user_id} — orders for a specific user."""
        with self._lock:
            user_ids = list(self._user_ids)
        if not user_ids:
            return
        uid = random.choice(user_ids)
        status, _ = _get(
            f"http://{self.order_host}/api/v1/orders/users/{uid}", timeout=5.0
        )
        self._bump_stats(status)

    def _list_users(self) -> int:
        """GET /api/v1/users/ — list all users."""
        status, data = _get(f"http://{self.user_host}/api/v1/users/", timeout=5.0)
        self._bump_stats(status)
        if status == 200 and isinstance(data, list):
            return len(data)
        return 0

    def _get_user(self) -> None:
        """GET /api/v1/users/{user_id} — fetch a single user by ID."""
        with self._lock:
            user_ids = list(self._user_ids)
        if user_ids:
            uid = random.choice(user_ids)
        else:
            uid = str(uuid.uuid4())  # will 404
        status, _ = _get(
            f"http://{self.user_host}/api/v1/users/{uid}", timeout=5.0
        )
        self._bump_stats(status)

    # -- stats --------------------------------------------------------------

    def _bump_stats(self, status: int) -> None:
        self.total_requests += 1
        if status == 0:
            self.total_errors += 1
        elif 200 <= status < 300:
            self.total_2xx += 1

    # -- mixed request dispatch ---------------------------------------------

    def fire_one_request(self) -> None:
        """Fire a single weighted-random request across both services.

        Request mix (approximate):
          35% GET /api/v1/orders           (list orders)
          15% POST /api/v1/orders          (create order)
          10% GET /api/v1/orders/{id}      (get order)
          10% GET /api/v1/orders/users/uid (user orders)
          15% GET /api/v1/users/           (list users)
           5% POST /api/v1/users/          (create user)
          10% GET /api/v1/users/{uid}      (get user)
        """
        roll = random.random()

        if roll < 0.35:
            self._list_orders()
        elif roll < 0.50:
            self._create_order()
        elif roll < 0.60:
            self._get_order()
        elif roll < 0.70:
            self._list_user_orders()
        elif roll < 0.85:
            self._list_users()
        elif roll < 0.90:
            uid = self._create_user()
            if uid:
                with self._lock:
                    self._user_ids.append(uid)
        else:
            self._get_user()


# ---------------------------------------------------------------------------
# Traffic loop
# ---------------------------------------------------------------------------


def _run_traffic_loop(
    gen: TrafficGenerator,
    rps: int,
    duration: float,
    scenario_active: bool,
    order_host: str,
    user_host: str,
) -> None:
    """Main traffic loop — fires requests at the target RPS for duration seconds.

    If scenario_active is True, the loop polls /__inject/anomaly every 5s
    and pauses/resumes traffic based on whether any scenario is active.
    """
    interval = 1.0 / max(rps, 1)
    deadline = time.time() + duration
    paused = False

    _json_log(
        "INFO",
        "traffic_loop_start",
        rps=rps,
        duration=duration,
        scenario_active=scenario_active,
        order_host=order_host,
        user_host=user_host,
    )

    while time.time() < deadline:
        loop_start = time.time()

        # Scenario-aware pause/resume
        if scenario_active:
            active = _any_anomaly_active(order_host, user_host)
            if active and paused:
                paused = False
                _json_log("INFO", "traffic_resumed", reason="anomaly_active")
            elif not active and not paused:
                paused = True
                _json_log("INFO", "traffic_paused", reason="no_active_anomaly")

            if paused:
                # Poll every 5 seconds while paused
                time.sleep(min(5.0, deadline - time.time()))
                continue

        gen.fire_one_request()

        # Sleep for remainder of interval
        elapsed = time.time() - loop_start
        sleep_time = interval - elapsed
        if sleep_time > 0:
            time.sleep(sleep_time)

    _json_log(
        "INFO",
        "traffic_loop_end",
        total_requests=gen.total_requests,
        total_2xx=gen.total_2xx,
        total_errors=gen.total_errors,
    )


# ---------------------------------------------------------------------------
# Health check
# ---------------------------------------------------------------------------


def _wait_for_services(
    order_host: str, user_host: str, timeout: float = 30.0
) -> bool:
    """Wait for both services to become healthy. Returns True if ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        order_ok = _get(f"http://{order_host}/health", timeout=3.0)[0] == 200
        user_ok = _get(f"http://{user_host}/health", timeout=3.0)[0] == 200
        if order_ok and user_ok:
            _json_log("INFO", "services_ready", order_host=order_host, user_host=user_host)
            return True
        time.sleep(1.0)

    _json_log("ERROR", "services_not_ready", order_host=order_host, user_host=user_host)
    return False


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "OmniWatch traffic generator — drives real business requests to "
            "order-service and user-service so OTel instruments fire."
        ),
    )
    parser.add_argument(
        "--rps",
        type=int,
        default=30,
        help="Target requests per second (default: 30)",
    )
    parser.add_argument(
        "--duration",
        type=float,
        default=90.0,
        help="Duration in seconds (default: 90)",
    )
    parser.add_argument(
        "--scenario-active",
        action="store_true",
        help=(
            "Scenario-aware mode: poll /__inject/anomaly on both services; "
            "auto-start traffic when a scenario is active, auto-stop when none."
        ),
    )
    parser.add_argument(
        "--order-host",
        default=DEFAULT_ORDER_HOST,
        help=f"order-service host:port (default: {DEFAULT_ORDER_HOST})",
    )
    parser.add_argument(
        "--user-host",
        default=DEFAULT_USER_HOST,
        help=f"user-service host:port (default: {DEFAULT_USER_HOST})",
    )
    parser.add_argument(
        "--seed-users",
        type=int,
        default=3,
        help="Number of users to create before starting traffic (default: 3)",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help="Skip user seeding (use existing users)",
    )

    args = parser.parse_args()

    # Configure logging
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    _json_log(
        "INFO",
        "traffic_generator_starting",
        rps=args.rps,
        duration=args.duration,
        scenario_active=args.scenario_active,
        order_host=args.order_host,
        user_host=args.user_host,
    )

    # Wait for services
    if not _wait_for_services(args.order_host, args.user_host):
        _json_log("ERROR", "exiting_services_unavailable")
        sys.exit(1)

    # Build generator
    gen = TrafficGenerator(
        order_host=args.order_host,
        user_host=args.user_host,
    )

    # Seed users
    if not args.no_seed:
        _json_log("INFO", "seeding_users", count=args.seed_users)
        gen.seed_users(args.seed_users)
        with gen._lock:
            seeded = len(gen._user_ids)
        _json_log("INFO", "seeding_complete", users_seeded=seeded)

    # Run traffic
    _run_traffic_loop(
        gen=gen,
        rps=args.rps,
        duration=args.duration,
        scenario_active=args.scenario_active,
        order_host=args.order_host,
        user_host=args.user_host,
    )

    # Final summary
    _json_log(
        "INFO",
        "traffic_generator_done",
        total_requests=gen.total_requests,
        total_2xx=gen.total_2xx,
        total_errors=gen.total_errors,
    )


if __name__ == "__main__":
    main()
