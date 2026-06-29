"""
OmniWatch — Simulation Layer
Component: Normal Telemetry Generator
Phase: 1
Purpose: Continuously generates realistic Prometheus metrics and pushes structured logs to Loki for all simulated services
Inputs: topology.json (service definitions), anomaly_state.json (optional anomaly overrides)
Outputs: Prometheus metrics on :8000/metrics, JSON logs to Loki via HTTP POST
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
from prometheus_client import Counter, Gauge, start_http_server

# ---------------------------------------------------------------------------
# Section 3: Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

LOKI_URL = os.getenv("LOKI_URL", "http://localhost:3100")
PROMETHEUS_PORT = 8000
ANOMALY_STATE_PATH = Path(__file__).resolve().parent / "anomaly_state.json"
TOPOLOGY_PATH = Path(__file__).resolve().parent / "topology.json"
CYCLE_INTERVAL = 5  # seconds

# ---------------------------------------------------------------------------
# Section 4: Service registry — load topology.json
# ---------------------------------------------------------------------------
if not TOPOLOGY_PATH.exists():
    print("[normal_telemetry] ERROR: topology.json not found at", TOPOLOGY_PATH)
    sys.exit(1)

with open(TOPOLOGY_PATH) as f:
    TOPOLOGY = json.load(f)

SERVICE_LIST = [svc["id"] for svc in TOPOLOGY["services"]]
SERVICE_META = {svc["id"]: svc for svc in TOPOLOGY["services"]}

# ---------------------------------------------------------------------------
# Section 5: Prometheus metric definitions
# ---------------------------------------------------------------------------
cpu_usage = Gauge(
    "omniwatch_cpu_usage_percent",
    "CPU usage percentage",
    ["service", "cloud_provider", "node_type"],
)
memory_usage = Gauge(
    "omniwatch_memory_usage_percent",
    "Memory usage percentage",
    ["service", "cloud_provider", "node_type"],
)
http_latency_p50 = Gauge(
    "omniwatch_http_latency_p50_ms",
    "HTTP latency p50 in milliseconds",
    ["service", "endpoint"],
)
http_latency_p95 = Gauge(
    "omniwatch_http_latency_p95_ms",
    "HTTP latency p95 in milliseconds",
    ["service", "endpoint"],
)
http_latency_p99 = Gauge(
    "omniwatch_http_latency_p99_ms",
    "HTTP latency p99 in milliseconds",
    ["service", "endpoint"],
)
error_rate_gauge = Gauge(
    "omniwatch_error_rate_percent",
    "Error rate percentage",
    ["service"],
)
db_query_duration = Gauge(
    "omniwatch_db_query_duration_ms",
    "Database query duration in milliseconds",
    ["service", "query_type"],
)
cache_hit_rate = Gauge(
    "omniwatch_cache_hit_rate_percent",
    "Cache hit rate percentage",
    ["service"],
)
job_queue_depth = Gauge(
    "omniwatch_job_queue_depth",
    "Background job queue depth",
    ["service"],
)
http_requests = Counter(
    "omniwatch_http_requests_total",
    "Total HTTP requests",
    ["service", "method", "status_code", "endpoint"],
)

# ---------------------------------------------------------------------------
# Section 6: State management — per-service metric values with drift
# ---------------------------------------------------------------------------

# Normal ranges per service type
NORMAL_RANGES = {
    "load-balancer": {"cpu": (15, 40), "memory": (30, 55), "error_rate": (0, 1.5), "latency_base": (5, 15)},
    "api-gateway": {"cpu": (20, 55), "memory": (35, 65), "error_rate": (0, 2), "latency_base": (20, 80)},
    "auth-service": {"cpu": (15, 45), "memory": (30, 60), "error_rate": (0, 1.5), "latency_base": (10, 40)},
    "product-service": {"cpu": (18, 50), "memory": (35, 65), "error_rate": (0, 2), "latency_base": (15, 60)},
    "inventory-service": {"cpu": (15, 45), "memory": (30, 55), "error_rate": (0, 2), "latency_base": (10, 50)},
    "background-worker": {"cpu": (10, 35), "memory": (40, 70), "error_rate": (0, 1.5), "latency_base": (50, 200)},
    "postgresql-database": {"cpu": (20, 60), "memory": (40, 75), "error_rate": (0, 1), "latency_base": (5, 30)},
    "redis-cache": {"cpu": (10, 30), "memory": (50, 75), "error_rate": (0, 0.5), "latency_base": (1, 5)},
    "user-database": {"cpu": (15, 45), "memory": (35, 65), "error_rate": (0, 1), "latency_base": (5, 25)},
    "minio-storage": {"cpu": (8, 25), "memory": (30, 50), "error_rate": (0, 0.5), "latency_base": (3, 15)},
}


class ServiceState:
    """Tracks current metric values for a service with slow Gaussian drift."""

    def __init__(self, service_id: str):
        meta = SERVICE_META[service_id]
        self.service_id = service_id
        self.cloud_provider = meta.get("cloud_provider", "simulated-aws")
        self.node_type = meta.get("type", "API_NODE")
        ranges = NORMAL_RANGES.get(service_id, NORMAL_RANGES["api-gateway"])

        mid_cpu = (ranges["cpu"][0] + ranges["cpu"][1]) / 2
        mid_mem = (ranges["memory"][0] + ranges["memory"][1]) / 2
        mid_lat = (ranges["latency_base"][0] + ranges["latency_base"][1]) / 2

        self.cpu = mid_cpu
        self.memory = mid_mem
        self.latency_base = mid_lat
        self.error_rate = 0.5
        self.cpu_range = ranges["cpu"]
        self.memory_range = ranges["memory"]
        self.error_range = ranges["error_rate"]
        self.latency_range = ranges["latency_base"]

    def drift(self):
        """Apply slow Gaussian drift to each metric."""
        self.cpu += random.gauss(0, 1.5)
        self.memory += random.gauss(0, 1.0)
        self.latency_base += random.gauss(0, 2.0)
        self.error_rate += random.gauss(0, 0.15)

        # Correlate: high CPU → higher latency
        if self.cpu > 60:
            self.latency_base += (self.cpu - 60) * 0.3

        self.cpu = max(self.cpu_range[0], min(self.cpu_range[1], self.cpu))
        self.memory = max(self.memory_range[0], min(self.memory_range[1], self.memory))
        self.latency_base = max(self.latency_range[0], min(self.latency_range[1], self.latency_base))
        self.error_rate = max(self.error_range[0], min(self.error_range[1], self.error_rate))

    def apply_overrides(self, overrides: dict):
        """Apply anomaly overrides from anomaly_state.json."""
        for key, value in overrides.items():
            if key == "cpu":
                self.cpu = value
            elif key == "memory":
                self.memory = value
            elif key == "error_rate":
                self.error_rate = value
            elif key == "db_query_duration_ms":
                pass  # handled at metric-push time
            elif key == "latency_p99_ms":
                pass  # handled at metric-push time


# Initialise state for every service
service_states = {sid: ServiceState(sid) for sid in SERVICE_LIST}

# ---------------------------------------------------------------------------
# Section 7: Loki push function
# ---------------------------------------------------------------------------
_session = requests.Session()


def push_logs_to_loki(log_entries: list):
    """Push a batch of log entries to Loki via HTTP POST."""
    if not log_entries:
        return

    streams = []
    for entry in log_entries:
        stream = {
            "stream": {
                "job": "omniwatch-simulation",
                "service": entry["service"],
                "level": entry.get("level", "info"),
                "node_type": entry.get("node_type", "UNKNOWN"),
                "cloud_provider": entry.get("cloud_provider", "simulated-aws"),
            },
            "values": [[str(time.time_ns()), entry["message"]]],
        }
        streams.append(stream)

    payload = {"streams": streams}
    url = f"{LOKI_URL.rstrip('/')}/loki/api/v1/push"

    for attempt in range(2):
        try:
            resp = _session.post(url, json=payload, timeout=5)
            if resp.status_code < 300:
                return
            print(f"[normal_telemetry] WARNING: Loki returned HTTP {resp.status_code}")
            return
        except requests.ConnectionError:
            if attempt == 0:
                time.sleep(2)
            else:
                print("[normal_telemetry] WARNING: Loki connection failed — logs not pushed")
        except Exception as exc:
            print(f"[normal_telemetry] WARNING: Loki push error: {exc}")
            return


# ---------------------------------------------------------------------------
# Section 8: Metric update function
# ---------------------------------------------------------------------------
def update_metrics(anomaly_overrides: dict):
    """Update all Gauge values with drift + anomaly overrides."""
    for sid, state in service_states.items():
        # Apply drift first
        state.drift()

        # Apply overrides if present for this service
        if sid in anomaly_overrides:
            state.apply_overrides(anomaly_overrides[sid])

        labels = {"service": sid, "cloud_provider": state.cloud_provider, "node_type": state.node_type}
        cpu_usage.labels(**labels).set(round(state.cpu, 1))
        memory_usage.labels(**labels).set(round(state.memory, 1))
        error_rate_gauge.labels(service=sid).set(round(state.error_rate, 2))

        # Latency percentiles
        overrides = anomaly_overrides.get(sid, {})
        base = state.latency_base
        p99_override = overrides.get("latency_p99_ms")
        p99 = p99_override if p99_override else round(base * 3.5, 1)
        p95 = round(base * 2.0, 1) if not p99_override else round(p99_override * 0.65, 1)
        p50 = round(base, 1)

        endpoint = "/api/default"
        http_latency_p50.labels(service=sid, endpoint=endpoint).set(p50)
        http_latency_p95.labels(service=sid, endpoint=endpoint).set(p95)
        http_latency_p99.labels(service=sid, endpoint=endpoint).set(p99)

        # Service-specific metrics
        if sid in ("postgresql-database", "user-database"):
            qdur = overrides.get("db_query_duration_ms", round(base * 2, 1))
            db_query_duration.labels(service=sid, query_type="SELECT").set(qdur)
            db_query_duration.labels(service=sid, query_type="INSERT").set(round(qdur * 0.8, 1))

        if sid == "redis-cache":
            cache_hit_rate.labels(service=sid).set(round(random.uniform(75, 95), 1))

        if sid == "background-worker":
            qd = overrides.get("job_queue_depth", random.randint(2, 15))
            job_queue_depth.labels(service=sid).set(qd)

        # Increment HTTP request counters
        methods = ["GET", "POST"]
        codes = ["200", "200", "200", "200", "201", "404"]
        endpoints = ["/api/products", "/api/orders", "/api/users"]
        for _ in range(random.randint(1, 5)):
            http_requests.labels(
                service=sid,
                method=random.choice(methods),
                status_code=random.choice(codes),
                endpoint=random.choice(endpoints),
            ).inc()


# ---------------------------------------------------------------------------
# Section 9: Log generation function
# ---------------------------------------------------------------------------
LOG_TEMPLATES = {
    "load-balancer": [
        ("info", "Request routed to api-gateway latency=2ms connections={conn}"),
        ("info", "Health check passed all backends healthy response_time=1ms"),
        ("warning", "Connection count elevated active={conn} limit=500"),
    ],
    "api-gateway": [
        ("info", "GET /api/products 200 {lat}ms user_id={uid}"),
        ("info", "POST /api/orders 201 {lat}ms user_id={uid}"),
        ("info", "GET /api/users 200 {lat}ms user_id={uid}"),
        ("warning", "Slow response detected endpoint=/api/search latency=350ms"),
    ],
    "auth-service": [
        ("info", "User login successful user_id={uid} ip=192.168.1.{ip_octet}"),
        ("info", "Token validated user_id={uid} duration=12ms"),
        ("info", "Session refreshed user_id={uid}"),
    ],
    "postgresql-database": [
        ("info", "Query executed SELECT products duration=23ms rows=150"),
        ("info", "Query executed INSERT orders duration=45ms affected=1"),
        ("info", "Connection pool active=8 idle=12 max=100"),
    ],
    "redis-cache": [
        ("info", "Cache HIT key=product:{uid} ttl=3540s"),
        ("info", "Cache HIT key=session:{uid} ttl=1800s"),
        ("info", "Cache MISS key=product:{uid} fetching from db"),
    ],
    "background-worker": [
        ("info", "Job processed type=email_notification duration=234ms"),
        ("info", "Job processed type=report_generation duration=1234ms"),
        ("info", "Queue depth={qd} jobs pending"),
    ],
    "product-service": [
        ("info", "Product catalog fetched items=45 cache=hit duration=12ms"),
        ("info", "Inventory check completed product_id={uid} in_stock=true"),
    ],
    "inventory-service": [
        ("info", "Inventory updated product_id={uid} quantity=150"),
        ("info", "Stock level check completed warehouse=A items=1200"),
    ],
    "user-database": [
        ("info", "Query executed SELECT users duration=18ms rows=50"),
        ("info", "Connection pool active=5 idle=10 max=50"),
    ],
    "minio-storage": [
        ("info", "Object stored bucket=omniwatch-incidents key=inc-{uid} size=2KB"),
        ("info", "Object retrieved bucket=omniwatch-audit-logs key=audit-{uid}"),
    ],
}


def generate_logs(anomaly_overrides: dict) -> list:
    """Generate 1-2 log messages per service each cycle."""
    entries = []
    for sid in SERVICE_LIST:
        meta = SERVICE_META[sid]
        templates = LOG_TEMPLATES.get(sid, [])
        if not templates:
            continue

        level, tmpl = random.choice(templates)

        # Check for anomaly log overrides
        overrides = anomaly_overrides.get(sid, {})
        if "log_level" in overrides:
            level = overrides["log_level"]
        if "log_message" in overrides:
            msg = overrides["log_message"]
        else:
            msg = tmpl.format(
                uid=random.randint(1000, 9999),
                ip_octet=random.randint(1, 254),
                conn=random.randint(200, 400),
                lat=random.randint(20, 120),
                qd=random.randint(2, 10),
            )

        entries.append({
            "service": sid,
            "node_type": meta.get("type", "UNKNOWN"),
            "cloud_provider": meta.get("cloud_provider", "simulated-aws"),
            "level": level,
            "message": msg,
        })

    return entries


# ---------------------------------------------------------------------------
# Section 10: Main loop
# ---------------------------------------------------------------------------
def read_anomaly_state() -> dict:
    """Read anomaly_state.json if it exists, return overrides dict."""
    if not ANOMALY_STATE_PATH.exists():
        return {}
    try:
        with open(ANOMALY_STATE_PATH) as f:
            state = json.load(f)
        return state.get("overrides", {})
    except Exception:
        return {}


def main():
    start_http_server(PROMETHEUS_PORT)
    print(f"[normal_telemetry] Started — exposing metrics on :{PROMETHEUS_PORT}, pushing logs to Loki")

    cycle_count = 0
    while True:
        try:
            cycle_count += 1
            overrides = read_anomaly_state()
            update_metrics(overrides)
            logs = generate_logs(overrides)
            push_logs_to_loki(logs)

            if cycle_count % 6 == 0:
                ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
                print(f"[normal_telemetry] Cycle {cycle_count} — {ts} — metrics updated for {len(SERVICE_LIST)} services — logs pushed to Loki")

            time.sleep(CYCLE_INTERVAL)
        except KeyboardInterrupt:
            print("\n[normal_telemetry] Shutting down")
            break
        except Exception:
            traceback.print_exc()
            time.sleep(CYCLE_INTERVAL)


if __name__ == "__main__":
    main()
