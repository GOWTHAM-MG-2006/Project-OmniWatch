"""
OmniWatch — Simulation Layer
Component: Security Event Generator
Phase: 1
Purpose: Continuously generates background security events at random intervals to simulate real enterprise security monitoring data
Inputs: Reads .env for service URLs, uses Faker for realistic random data
Outputs: Logs pushed to Loki via HTTP POST, security events published to Kafka omniwatch.security.events topic
"""

import json
import os
import random
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from faker import Faker

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).parent.parent / ".env")

LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = "omniwatch.security.events"

fake = Faker()
_session = requests.Session()


# ---------------------------------------------------------------------------
# Loki push
# ---------------------------------------------------------------------------

def push_logs_to_loki(log_entries: list):
    """Push a batch of log entries to Loki via HTTP POST."""
    if not log_entries:
        return
    streams = []
    for entry in log_entries:
        streams.append({
            "stream": {
                "job": "omniwatch-simulation",
                "service": entry["service"],
                "level": entry.get("level", "info"),
                "node_type": entry.get("node_type", "UNKNOWN"),
                "cloud_provider": entry.get("cloud_provider", "simulated-aws"),
            },
            "values": [[str(time.time_ns()), entry["message"]]],
        })
    payload = {"streams": streams}
    url = f"{LOKI_URL.rstrip('/')}/loki/api/v1/push"
    for attempt in range(2):
        try:
            resp = _session.post(url, json=payload, timeout=5)
            if resp.status_code < 300:
                return
            print(f"[security_generator] WARNING: Loki returned HTTP {resp.status_code}")
            return
        except requests.ConnectionError:
            if attempt == 0:
                time.sleep(2)
            else:
                print("[security_generator] WARNING: Loki connection failed")
        except Exception as exc:
            print(f"[security_generator] WARNING: Loki push error: {exc}")
            return


# ---------------------------------------------------------------------------
# Kafka publish
# ---------------------------------------------------------------------------

def publish_to_kafka(message_dict: dict):
    """Publish a single JSON message to the security events Kafka topic."""
    try:
        from confluent_kafka import Producer
        conf = {"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS}
        producer = Producer(conf)
        producer.produce(KAFKA_TOPIC, json.dumps(message_dict).encode("utf-8"))
        producer.flush(timeout=5)
        print(f"[security_generator] Published: {message_dict['event_type']} event to Loki and Kafka")
    except Exception as exc:
        print(f"[security_generator] WARNING: Kafka publish failed: {exc}")


# ---------------------------------------------------------------------------
# Event generators
# ---------------------------------------------------------------------------

def generate_failed_login():
    """Normal failed login attempt — background noise."""
    username = fake.user_name()
    source_ip = fake.ipv4()
    now_str = datetime.now(timezone.utc).isoformat()

    push_logs_to_loki([{
        "service": "auth-service",
        "node_type": "AUTH_NODE",
        "cloud_provider": "simulated-azure",
        "level": "warning",
        "message": f"Failed login attempt: user={username} ip={source_ip} reason=invalid_password",
    }])

    publish_to_kafka({
        "event_type": "FAILED_LOGIN",
        "username": username,
        "source_ip": source_ip,
        "target_service": "auth-service",
        "entity_id": "auth-service",
        "entity_type": "AUTH_NODE",
        "severity": "LOW",
        "source_type": "security",
        "timestamp": now_str,
    })


def generate_admin_access():
    """Unusual admin endpoint access attempt."""
    source_ip = fake.ipv4()
    user_id = fake.random_int(min=1000, max=9999)
    now_str = datetime.now(timezone.utc).isoformat()

    push_logs_to_loki([{
        "service": "api-gateway",
        "node_type": "API_NODE",
        "cloud_provider": "simulated-aws",
        "level": "warning",
        "message": f"Unusual access: GET /admin/users ip={source_ip} user_id={user_id} status=403",
    }])

    publish_to_kafka({
        "event_type": "UNAUTHORIZED_ACCESS_ATTEMPT",
        "endpoint": "/admin/users",
        "source_ip": source_ip,
        "target_service": "api-gateway",
        "entity_id": "api-gateway",
        "entity_type": "API_NODE",
        "severity": "MEDIUM",
        "source_type": "security",
        "timestamp": now_str,
    })


def generate_config_read():
    """Routine config read — normal activity, no Kafka."""
    now_str = datetime.now(timezone.utc).isoformat()

    push_logs_to_loki([{
        "service": "postgresql-database",
        "node_type": "DATABASE_NODE",
        "cloud_provider": "simulated-aws",
        "level": "info",
        "message": "Configuration read: parameter=max_connections value=100 by=monitoring-agent",
    }])


def generate_data_export():
    """Large data export detected."""
    export_size = fake.random_int(min=100, max=500)
    user_id = fake.random_int(min=1000, max=9999)
    now_str = datetime.now(timezone.utc).isoformat()

    push_logs_to_loki([{
        "service": "api-gateway",
        "node_type": "API_NODE",
        "cloud_provider": "simulated-aws",
        "level": "warning",
        "message": f"Large export detected: endpoint=/api/export size={export_size}MB user_id={user_id}",
    }])

    publish_to_kafka({
        "event_type": "LARGE_DATA_EXPORT",
        "export_size_mb": export_size,
        "user_id": user_id,
        "endpoint": "/api/export",
        "target_service": "api-gateway",
        "entity_id": "api-gateway",
        "entity_type": "API_NODE",
        "severity": "MEDIUM",
        "source_type": "security",
        "timestamp": now_str,
    })


def generate_privilege_op():
    """Successful privilege operation — audit trail, no Kafka."""
    admin_uid = fake.random_int(min=1000, max=9999)
    target_user = fake.user_name()

    push_logs_to_loki([{
        "service": "auth-service",
        "node_type": "AUTH_NODE",
        "cloud_provider": "simulated-azure",
        "level": "info",
        "message": f"Privilege operation: user_id={admin_uid} action=role_assignment target_user={target_user}",
    }])


# ---------------------------------------------------------------------------
# Event dispatch
# ---------------------------------------------------------------------------

EVENT_DISPATCH = {
    "failed_login": generate_failed_login,
    "admin_access": generate_admin_access,
    "config_read": generate_config_read,
    "data_export": generate_data_export,
    "privilege_op": generate_privilege_op,
}


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    print("[security_generator] Started — publishing to Loki and Kafka topic omniwatch.security.events")

    while True:
        try:
            event_type = random.choice(list(EVENT_DISPATCH.keys()))
            EVENT_DISPATCH[event_type]()
            sleep_time = random.uniform(10, 30)
            time.sleep(sleep_time)
        except KeyboardInterrupt:
            print("\n[security_generator] Shutting down")
            break
        except Exception:
            traceback.print_exc()
            time.sleep(10)


if __name__ == "__main__":
    main()
