"""
OmniWatch — Simulation Layer
Component: Anomaly Injector
Phase: 1
Purpose: CLI tool to inject specific failure scenarios into the running simulation
Inputs: CLI argument --scenario name, reads .env for service URLs
Outputs: Writes anomaly_state.json, pushes logs to Loki, publishes events to Kafka
"""

import argparse
import json
import os
import random
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).parent.parent / ".env")

LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
ANOMALY_STATE_FILE = Path(__file__).parent / "anomaly_state.json"
KAFKA_TOPIC = "omniwatch.security.events"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def write_anomaly_state(state_dict: dict):
    """Write anomaly state to the shared JSON file atomically."""
    tmp = ANOMALY_STATE_FILE.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(state_dict, f, indent=2)
    tmp.replace(ANOMALY_STATE_FILE)
    print(
        f"[anomaly_injector] State written — scenario: {state_dict['active_scenario']}"
    )


def reset_anomaly_state():
    """Delete anomaly_state.json to return simulation to normal."""
    if ANOMALY_STATE_FILE.exists():
        ANOMALY_STATE_FILE.unlink()
    print("[anomaly_injector] Anomaly state cleared — simulation returning to normal")


def push_logs_to_loki(log_entries: list):
    """Push a batch of log entries to Loki via HTTP POST."""
    if not log_entries:
        return
    streams = []
    for entry in log_entries:
        streams.append(
            {
                "stream": {
                    "job": "omniwatch-simulation",
                    "service": entry["service"],
                    "level": entry.get("level", "info"),
                    "node_type": entry.get("node_type", "UNKNOWN"),
                    "cloud_provider": entry.get("cloud_provider", "simulated-aws"),
                },
                "values": [[str(time.time_ns()), entry["message"]]],
            }
        )
    payload = {"streams": streams}
    url = f"{LOKI_URL.rstrip('/')}/loki/api/v1/push"
    for attempt in range(2):
        try:
            resp = requests.post(url, json=payload, timeout=5)
            if resp.status_code < 300:
                return
            print(f"[anomaly_injector] WARNING: Loki returned HTTP {resp.status_code}")
            return
        except requests.ConnectionError:
            if attempt == 0:
                time.sleep(2)
            else:
                print(
                    "[anomaly_injector] WARNING: Loki connection failed — logs not pushed"
                )
        except Exception as exc:
            print(f"[anomaly_injector] WARNING: Loki push error: {exc}")
            return


def publish_to_kafka(message_dict: dict):
    """Publish a single JSON message to the security events Kafka topic."""
    try:
        conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
        producer = Producer(conf)
        producer.produce(KAFKA_TOPIC, json.dumps(message_dict).encode("utf-8"))
        producer.flush(timeout=5)
        print(
            f"[anomaly_injector] Kafka event published — type: {message_dict['event_type']}"
        )
    except Exception as exc:
        print(f"[anomaly_injector] WARNING: Kafka publish failed: {exc}")


# ---------------------------------------------------------------------------
# Scenario listing
# ---------------------------------------------------------------------------


def list_scenarios():
    """Print formatted table of available anomaly scenarios."""
    print("OmniWatch Anomaly Injector — Available Scenarios")
    print("=" * 72)
    print()
    print(f"{'Scenario':<20}{'Root Cause':<24}{'Duration':<12}{'What It Tests'}")
    print("-" * 72)
    print(
        f"{'database_cascade':<20}{'postgresql-database':<24}{'120 sec':<12}{'Causal graph traversal'}"
    )
    print(
        f"{'memory_leak':<20}{'background-worker':<24}{'150 sec':<12}{'Trend-based detection'}"
    )
    print(
        f"{'security_attack':<20}{'auth-service':<24}{'90 sec':<12}{'Security signal classifier'}"
    )
    print(
        f"{'config_drift':<20}{'postgresql-database':<24}{'90 sec':<12}{'Config change detection'}"
    )
    print()
    print("Usage:")
    print("  py simulation\\anomaly_injector.py --scenario database_cascade")
    print("  py simulation\\anomaly_injector.py --scenario memory_leak")
    print("  py simulation\\anomaly_injector.py --scenario security_attack")
    print("  py simulation\\anomaly_injector.py --scenario config_drift")
    print("  py simulation\\anomaly_injector.py --reset")


# ---------------------------------------------------------------------------
# Scenario A: database_cascade
# ---------------------------------------------------------------------------


def scenario_database_cascade():
    """Simulate PostgreSQL failure cascading through inventory → product → api-gateway."""
    print("[anomaly_injector] ══════════════════════════════════════════")
    print("[anomaly_injector] INJECTING: database_cascade")
    print("[anomaly_injector] Root cause: postgresql-database")
    print("[anomaly_injector] Expected fault path: db → inventory → product → api")
    print("[anomaly_injector] ══════════════════════════════════════════")

    db_query_ms = round(random.uniform(6000, 9000), 0)
    db_er = round(random.uniform(35, 50), 1)

    # --- Phase 1: immediate database degradation ---
    state = {
        "active_scenario": "database_cascade",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "phase": 1,
        "overrides": {
            "postgresql-database": {
                "cpu": round(random.uniform(85, 95), 1),
                "memory": round(random.uniform(80, 90), 1),
                "error_rate": db_er,
                "db_query_duration_ms": db_query_ms,
            }
        },
    }
    write_anomaly_state(state)
    push_logs_to_loki(
        [
            {
                "service": "postgresql-database",
                "node_type": "DATABASE_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": f"ERROR: Query timeout after {int(db_query_ms)}ms — connection pool exhausted",
            },
            {
                "service": "postgresql-database",
                "node_type": "DATABASE_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "CRITICAL: Max connections reached (50/50) — new connections refused",
            },
            {
                "service": "postgresql-database",
                "node_type": "DATABASE_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: Deadlock detected on table=orders — rolling back transaction",
            },
        ]
    )
    print(
        f"[anomaly_injector] Phase 1 — postgresql-database CRITICAL (query_time={int(db_query_ms)}ms error_rate={db_er}%)"
    )
    time.sleep(15)

    # --- Phase 2: cascade to inventory-service ---
    inv_er = round(random.uniform(25, 40), 1)
    inv_p99 = round(random.uniform(3000, 5000), 0)
    state["phase"] = 2
    state["overrides"]["inventory-service"] = {
        "error_rate": inv_er,
        "latency_p99_ms": inv_p99,
    }
    write_anomaly_state(state)
    push_logs_to_loki(
        [
            {
                "service": "inventory-service",
                "node_type": "API_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: Database connection failed after 3 retries — upstream=postgresql-database",
            },
            {
                "service": "inventory-service",
                "node_type": "API_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: Request timeout waiting for postgresql-database response=5000ms",
            },
            {
                "service": "inventory-service",
                "node_type": "API_NODE",
                "cloud_provider": "simulated-aws",
                "level": "warning",
                "message": "WARNING: Circuit breaker OPEN for postgresql-database — failing fast",
            },
        ]
    )
    print("[anomaly_injector] Phase 2 — inventory-service DEGRADED cascading from db")
    time.sleep(15)

    # --- Phase 3: cascade to api-gateway ---
    prod_er = round(random.uniform(20, 35), 1)
    gw_er = round(random.uniform(15, 25), 1)
    gw_p99 = round(random.uniform(2000, 4000), 0)
    state["phase"] = 3
    state["overrides"]["product-service"] = {"error_rate": prod_er}
    state["overrides"]["api-gateway"] = {"error_rate": gw_er, "latency_p99_ms": gw_p99}
    write_anomaly_state(state)
    push_logs_to_loki(
        [
            {
                "service": "product-service",
                "node_type": "API_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: inventory-service returned 503 — product availability check failed",
            },
            {
                "service": "api-gateway",
                "node_type": "API_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: Upstream product-service unavailable — returning 503 to client",
            },
            {
                "service": "api-gateway",
                "node_type": "API_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: 503 Service Unavailable — downstream cascade detected affecting 3 services",
            },
        ]
    )
    print("[anomaly_injector] Phase 3 — CASCADE COMPLETE api-gateway returning 503")

    # --- Hold for remaining duration ---
    remaining = 90
    while remaining > 0:
        print(
            f"[anomaly_injector] Scenario holding — {remaining}s remaining before auto-reset"
        )
        wait = min(30, remaining)
        time.sleep(wait)
        remaining -= wait

    # --- Auto-reset ---
    reset_anomaly_state()
    print(
        "[anomaly_injector] database_cascade complete — simulation restored to normal"
    )


# ---------------------------------------------------------------------------
# Scenario B: memory_leak
# ---------------------------------------------------------------------------


def scenario_memory_leak():
    """Simulate background worker memory leak leading to OOM."""
    print("[anomaly_injector] ══════════════════════════════════════════")
    print("[anomaly_injector] INJECTING: memory_leak")
    print("[anomaly_injector] Root cause: background-worker")
    print("[anomaly_injector] Simulating gradual memory growth then OOM")
    print("[anomaly_injector] ══════════════════════════════════════════")

    # --- Phase 1: memory starts climbing ---
    state = {
        "active_scenario": "memory_leak",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "phase": 1,
        "overrides": {
            "background-worker": {
                "memory": 72.0,
                "job_queue_depth": 25,
            }
        },
    }
    write_anomaly_state(state)
    push_logs_to_loki(
        [
            {
                "service": "background-worker",
                "node_type": "WORKER_NODE",
                "cloud_provider": "simulated-gcp",
                "level": "warning",
                "message": "WARNING: Memory usage climbing — current=72% baseline=45% delta=+27%",
            },
        ]
    )
    print("[anomaly_injector] Phase 1 — background-worker memory=72% queue_depth=25")
    time.sleep(30)

    # --- Phase 2: memory grows further ---
    state["phase"] = 2
    state["overrides"]["background-worker"]["memory"] = 81.0
    state["overrides"]["background-worker"]["job_queue_depth"] = 75
    write_anomaly_state(state)
    push_logs_to_loki(
        [
            {
                "service": "background-worker",
                "node_type": "WORKER_NODE",
                "cloud_provider": "simulated-gcp",
                "level": "warning",
                "message": "WARNING: Memory leak suspected — growth_rate=3%/min current=81%",
            },
            {
                "service": "background-worker",
                "node_type": "WORKER_NODE",
                "cloud_provider": "simulated-gcp",
                "level": "warning",
                "message": "WARNING: Job queue growing abnormally — depth=75 normal_max=20",
            },
        ]
    )
    print("[anomaly_injector] Phase 2 — memory=81% queue_depth=75 — leak confirmed")
    time.sleep(30)

    # --- Phase 3: OOM imminent ---
    state["phase"] = 3
    state["overrides"]["background-worker"]["memory"] = 91.0
    state["overrides"]["background-worker"]["job_queue_depth"] = 180
    state["overrides"]["background-worker"]["cpu"] = 85.0
    state["overrides"]["background-worker"]["error_rate"] = 20.0
    write_anomaly_state(state)
    push_logs_to_loki(
        [
            {
                "service": "background-worker",
                "node_type": "WORKER_NODE",
                "cloud_provider": "simulated-gcp",
                "level": "error",
                "message": "ERROR: OOM kill imminent — memory=91% job_queue=180 dropping new jobs",
            },
            {
                "service": "background-worker",
                "node_type": "WORKER_NODE",
                "cloud_provider": "simulated-gcp",
                "level": "error",
                "message": "ERROR: Job processing failed — type=report_generation reason=out_of_memory",
            },
        ]
    )
    print("[anomaly_injector] Phase 3 — OOM IMMINENT memory=91% cpu=85% error_rate=20%")

    # --- Hold ---
    remaining = 60
    while remaining > 0:
        print(
            f"[anomaly_injector] Scenario holding — {remaining}s remaining before auto-reset"
        )
        wait = min(30, remaining)
        time.sleep(wait)
        remaining -= wait

    reset_anomaly_state()
    print("[anomaly_injector] memory_leak complete — simulation restored to normal")


# ---------------------------------------------------------------------------
# Scenario C: security_attack
# ---------------------------------------------------------------------------


def scenario_security_attack():
    """Simulate brute force login attack against auth-service."""
    print("[anomaly_injector] ══════════════════════════════════════════")
    print("[anomaly_injector] INJECTING: security_attack")
    print("[anomaly_injector] Root cause: auth-service (external brute force)")
    print("[anomaly_injector] Simulating 20 failed logins from 45.33.32.156")
    print("[anomaly_injector] ══════════════════════════════════════════")

    # --- Phase 1: auth-service degraded + rapid failed logins ---
    state = {
        "active_scenario": "security_attack",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "phase": 1,
        "overrides": {
            "auth-service": {
                "cpu": round(random.uniform(70, 85), 1),
                "error_rate": round(random.uniform(40, 60), 1),
                "latency_p99_ms": round(random.uniform(500, 800), 0),
            }
        },
    }
    write_anomaly_state(state)

    # Push 10 rapid failed login logs
    print("[anomaly_injector] Injecting rapid failed login attempts...")
    for attempt in range(1, 11):
        push_logs_to_loki(
            [
                {
                    "service": "auth-service",
                    "node_type": "AUTH_NODE",
                    "cloud_provider": "simulated-azure",
                    "level": "warning",
                    "message": f"SECURITY: Failed login attempt — user=admin ip=45.33.32.156 attempt={attempt}/20",
                },
            ]
        )
        print(f"[anomaly_injector] Attempt {attempt}/20 — ip=45.33.32.156")
        time.sleep(1)

    # Push final alert log
    push_logs_to_loki(
        [
            {
                "service": "auth-service",
                "node_type": "AUTH_NODE",
                "cloud_provider": "simulated-azure",
                "level": "error",
                "message": "SECURITY: BRUTE FORCE DETECTED — ip=45.33.32.156 attempts=20 in 30s — BLOCKING",
            },
        ]
    )
    print("[anomaly_injector] SECURITY ATTACK — 20 failed logins from 45.33.32.156")

    # Publish Kafka event
    publish_to_kafka(
        {
            "event_type": "BRUTE_FORCE_ATTEMPT",
            "source_ip": "45.33.32.156",
            "target_service": "auth-service",
            "entity_id": "auth-service",
            "entity_type": "AUTH_NODE",
            "attempt_count": 20,
            "time_window_seconds": 30,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": "HIGH",
            "source_type": "security",
            "recommended_action": "block_ip_address",
        }
    )

    # --- Hold ---
    remaining = 60
    while remaining > 0:
        print(
            f"[anomaly_injector] Scenario holding — {remaining}s remaining before auto-reset"
        )
        wait = min(30, remaining)
        time.sleep(wait)
        remaining -= wait

    reset_anomaly_state()
    print("[anomaly_injector] security_attack complete — simulation restored to normal")


# ---------------------------------------------------------------------------
# Scenario D: config_drift
# ---------------------------------------------------------------------------


def scenario_config_drift():
    """Simulate unauthorized configuration change on postgresql-database."""
    print("[anomaly_injector] ══════════════════════════════════════════")
    print("[anomaly_injector] INJECTING: config_drift")
    print(
        "[anomaly_injector] Root cause: postgresql-database (unauthorized config change)"
    )
    print("[anomaly_injector] max_connections changed from 100 to 5")
    print("[anomaly_injector] ══════════════════════════════════════════")

    # --- Phase 1: config changed, connections failing ---
    db_query_ms = round(random.uniform(2000, 4000), 0)
    db_er = round(random.uniform(10, 20), 1)
    state = {
        "active_scenario": "config_drift",
        "start_time": datetime.now(timezone.utc).isoformat(),
        "phase": 1,
        "overrides": {
            "postgresql-database": {
                "error_rate": db_er,
                "db_query_duration_ms": db_query_ms,
            }
        },
    }
    write_anomaly_state(state)

    push_logs_to_loki(
        [
            {
                "service": "postgresql-database",
                "node_type": "DATABASE_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "CONFIG CHANGE DETECTED: max_connections changed from 100 to 5 by=unknown",
            },
            {
                "service": "postgresql-database",
                "node_type": "DATABASE_NODE",
                "cloud_provider": "simulated-aws",
                "level": "warning",
                "message": "WARNING: Connection limit nearly reached (4/5 active connections)",
            },
            {
                "service": "postgresql-database",
                "node_type": "DATABASE_NODE",
                "cloud_provider": "simulated-aws",
                "level": "error",
                "message": "ERROR: Connection refused — max_connections=5 limit reached rejecting new connections",
            },
        ]
    )
    print("[anomaly_injector] CONFIG DRIFT — max_connections changed 100→5")

    # Publish Kafka event
    publish_to_kafka(
        {
            "event_type": "UNAUTHORIZED_CONFIG_CHANGE",
            "entity_id": "postgresql-database",
            "entity_type": "DATABASE_NODE",
            "config_key": "max_connections",
            "old_value": "100",
            "new_value": "5",
            "changed_by": "unknown",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "severity": "HIGH",
            "source_type": "security",
            "recommended_action": "configuration_rollback",
        }
    )

    # --- Hold ---
    remaining = 60
    while remaining > 0:
        print(
            f"[anomaly_injector] Scenario holding — {remaining}s remaining before auto-reset"
        )
        wait = min(30, remaining)
        time.sleep(wait)
        remaining -= wait

    reset_anomaly_state()
    print("[anomaly_injector] config_drift complete — simulation restored to normal")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(
        description="OmniWatch Anomaly Injector — Inject failure scenarios into simulation",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--scenario",
        choices=["database_cascade", "memory_leak", "security_attack", "config_drift"],
        help="Name of scenario to inject",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List all available scenarios",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Reset simulation to normal state immediately",
    )

    args = parser.parse_args()

    if args.list:
        list_scenarios()
    elif args.reset:
        reset_anomaly_state()
    elif args.scenario:
        scenarios = {
            "database_cascade": scenario_database_cascade,
            "memory_leak": scenario_memory_leak,
            "security_attack": scenario_security_attack,
            "config_drift": scenario_config_drift,
        }
        scenarios[args.scenario]()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
