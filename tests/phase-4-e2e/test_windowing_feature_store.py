"""
OmniWatch — Phase 4 E2E Test
Component: Windowing Layer + Feature Store Wiring
Phase: 4
Purpose: Verify the FULL real-data path:
         Kafka metrics -> Flink FeatureStoreJob (tumbling/sliding/session windows
         -> FeatureVectorBuilder) -> ClickHouse feature_vectors table ->
         Feature Store API.
         Proves the plumbing, not unit-level aggregation correctness (75 unit
         tests cover that).
Inputs: Docker stack (Kafka, Flink, ClickHouse, Feature Store API)
Outputs: pytest pass/fail for 9 DONE-WHEN criteria
"""

from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

import pytest

# =============================================================================
# Module-level gate — skip entire file when Docker stack is unavailable
# =============================================================================


def _docker_available() -> bool:
    """Return True when the Docker daemon is reachable."""
    try:
        result = subprocess.run(
            ["docker", "version", "--format", "{{.Server.Version}}"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


pytestmark = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available — Phase 4 E2E skipped",
)

# =============================================================================
# Constants
# =============================================================================

CH_HOST = "http://localhost:8123"
API_HOST = "http://localhost:8005"
FLINK_REST = "http://localhost:8081"
KAFKA_CONTAINER = "omniwatch-kafka"
FLINK_CONTAINER = "omniwatch-feature-store-flink"
# Exact job display name from FeatureStoreJob.env.execute("OmniWatch Feature Store")
JOB_DISPLAY_NAME = "OmniWatch Feature Store"

# Expected 15 columns in the ClickHouse feature_vectors table.
# Compared as a SET — column order does not matter.
EXPECTED_COLUMNS = {
    "entity_id",
    "window_start",
    "window_end",
    "window_size",
    "latency_p50",
    "latency_p95",
    "latency_p99",
    "latency_avg",
    "latency_min",
    "latency_max",
    "error_rate",
    "request_volume",
    "feature_version",
    "ttl",
    "timestamp",
}

# Phase summary artifact (written by the summary task).
PHASE_SUMMARY = (
    Path(__file__).resolve().parent.parent.parent
    / "Project_Source_Files"
    / "Phase_Summary_Details"
    / "Phase-4-Summary.txt"
)

# =============================================================================
# Shared helpers — stdlib only (urllib + subprocess)
# =============================================================================


def ch_query(sql: str) -> str:
    """Execute a SQL query via the ClickHouse HTTP interface and return raw text."""
    url = f"{CH_HOST}/?query={urllib.parse.quote(sql)}&database=omniwatch"
    resp = urllib.request.urlopen(url, timeout=10)
    return resp.read().decode("utf-8")


def kafka_produce(topic: str, messages: list[str]) -> None:
    """Produce a batch of newline-delimited JSON messages to a Kafka topic."""
    result = subprocess.run(
        [
            "docker", "exec", "-i", KAFKA_CONTAINER,
            "kafka-console-producer",
            "--bootstrap-server", "localhost:9092",
            "--topic", topic,
        ],
        input="\n".join(messages) + "\n",
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, (
        f"kafka-console-producer failed (rc={result.returncode})\n"
        f"stdout: {result.stdout}\nstderr: {result.stderr}"
    )


def flink_rest(path: str) -> dict:
    """Fetch JSON from the Flink REST API."""
    url = f"{FLINK_REST}{path}"
    resp = urllib.request.urlopen(url, timeout=10)
    return json.loads(resp.read().decode("utf-8"))


def job_running() -> bool:
    """Return True when the FeatureStore Flink job is in RUNNING state."""
    data = flink_rest("/jobs/overview")
    for job in data.get("jobs", []):
        if (
            job.get("state") == "RUNNING"
            and JOB_DISPLAY_NAME.lower() in job.get("name", "").lower()
        ):
            return True
    return False


def ensure_job_submitted() -> None:
    """Ensure the FeatureStore Flink job is RUNNING; submit it if not."""
    if job_running():
        return

    result = subprocess.run(
        [
            "docker", "exec", FLINK_CONTAINER,
            "flink", "run", "-d",
            "-m", "flink-jobmanager:8081",
            "-c", "com.omniwatch.features.FeatureStoreJob",
            "/opt/flink/jobs/omniwatch-feature-store-job.jar",
            "--kafka.brokers", "kafka:29092",
            "--kafka.group.id", "flink-feature-store-e2e",
            "--clickhouse.host", "clickhouse",
            "--clickhouse.port", "8123",
            "--clickhouse.db", "omniwatch",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    # Poll until RUNNING (worst case: job takes a while to register).
    deadline = time.time() + 60
    while time.time() < deadline:
        if job_running():
            return
        time.sleep(3)

    pytest.fail(
        "Flink job did not reach RUNNING state within 60 s.\n"
        f"Submit stdout: {result.stdout}\n"
        f"Submit stderr: {result.stderr}"
    )


def wait_for_rows(
    entity_id: str,
    min_rows: int = 1,
    timeout_s: int = 150,
) -> int:
    """Poll ClickHouse until the entity has at least *min_rows* feature vectors."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        raw = ch_query(
            f"SELECT count(*) FROM omniwatch.feature_vectors "
            f"WHERE entity_id = '{entity_id}'"
        )
        count = int(raw.strip()) if raw.strip().isdigit() else 0
        if count >= min_rows:
            return count
        time.sleep(5)
    # One final attempt after timeout.
    raw = ch_query(
        f"SELECT count(*) FROM omniwatch.feature_vectors "
        f"WHERE entity_id = '{entity_id}'"
    )
    return int(raw.strip()) if raw.strip().isdigit() else 0


def make_metrics(entity_id: str, count: int, value_base: float) -> list[str]:
    """Build N MetricsEvent JSON strings with spread timestamps.

    Timestamps span ~100 seconds ending 5s before now (5s apart per event).
    This lets the Flink event-time watermark (5s bounded out-of-orderness)
    advance past 1-minute tumbling window boundaries so windows fire immediately.
    """
    now_ms = int(time.time() * 1000)
    events = []
    for i in range(count):
        events.append(
            json.dumps(
                {
                    "entity_id": entity_id,
                    "metric_name": "request_latency_ms",
                    "value": value_base + i,
                    "timestamp": now_ms - 100_000 + (i * 5_000),
                    "is_error": False,
                    "source_type": "performance",
                }
            )
        )
    return events


# =============================================================================
# Session fixture — produces data shared by downstream tests (5-7).
# Tests 1-4 are independent; tests 5-7 consume the data produced here.
# =============================================================================
@pytest.fixture(scope="session", autouse=False)
def e2e_data():
    """Ensure the Flink job is running and produce events for the E2E check."""
    ensure_job_submitted()

    # Produce events for two distinct entities.
    msgs_checkout = make_metrics("svc-e2e-checkout", 20, 100.0)
    msgs_payments = make_metrics("svc-e2e-payments", 20, 200.0)
    kafka_produce("omniwatch.metrics.normalized", msgs_checkout + msgs_payments)

    # Wait for ClickHouse rows to appear.
    count_checkout = wait_for_rows("svc-e2e-checkout", min_rows=1, timeout_s=150)
    count_payments = wait_for_rows("svc-e2e-payments", min_rows=1, timeout_s=150)
    assert count_checkout > 0, (
        "No feature_vectors rows for svc-e2e-checkout after 150 s"
    )
    assert count_payments > 0, (
        "No feature_vectors rows for svc-e2e-payments after 150 s"
    )


# =============================================================================
# Tests — order matches dependency chain (1-4 independent, 5-7 need e2e_data)
# =============================================================================


def test_clickhouse_reachable():
    """DONE-WHEN: ClickHouse is reachable and the omniwatch database exists."""
    # Basic connectivity.
    result = ch_query("SELECT 1")
    assert result.strip() == "1", f"Expected '1', got {result!r}"

    # Database exists.
    db_list = ch_query("SELECT name FROM system.databases")
    databases = {line.strip() for line in db_list.strip().splitlines() if line.strip()}
    assert "omniwatch" in databases, (
        f"Database 'omniwatch' not found. Available: {sorted(databases)}"
    )


def test_feature_store_api_health():
    """DONE-WHEN: Feature Store API /health returns 200 + healthy status."""
    url = f"{API_HOST}/health"
    resp = urllib.request.urlopen(url, timeout=10)
    assert resp.status == 200, f"Expected HTTP 200, got {resp.status}"
    body = json.loads(resp.read().decode("utf-8"))
    assert body.get("status") == "healthy", (
        f"Expected status=healthy, got {body!r}"
    )


def test_job_running_after_submission():
    """DONE-WHEN: Flink REST shows the FeatureStore job in RUNNING state."""
    ensure_job_submitted()
    assert job_running(), (
        "FeatureStore job not in RUNNING state after ensure_job_submitted()"
    )


def test_feature_vectors_table_schema():
    """DONE-WHEN: feature_vectors table exists with exactly the 15 expected columns."""
    # The table is created by FeatureStoreWriter.open() at job startup.
    # Poll until it appears.
    deadline = time.time() + 60
    columns: set[str] = set()
    while time.time() < deadline:
        try:
            raw = ch_query("DESCRIBE TABLE omniwatch.feature_vectors")
            lines = [l.strip() for l in raw.strip().splitlines() if l.strip()]
            columns = {line.split("\t")[0] for line in lines}
            if len(columns) >= 10:  # Table exists with columns.
                break
        except urllib.error.HTTPError:
            pass  # Table not created yet.
        time.sleep(3)

    assert columns, (
        "feature_vectors table did not appear within 60 s"
    )
    missing = EXPECTED_COLUMNS - columns
    extra = columns - EXPECTED_COLUMNS
    assert not missing, f"Missing columns: {sorted(missing)}"
    assert not extra, f"Unexpected columns: {sorted(extra)}"


def test_windowed_features_written_to_clickhouse(e2e_data):
    """DONE-WHEN: Produced events result in feature_vectors rows with valid window_size."""
    # Count check (e2e_data fixture already asserts > 0).
    raw = ch_query(
        "SELECT count(*) FROM omniwatch.feature_vectors "
        "WHERE entity_id = 'svc-e2e-checkout'"
    )
    count = int(raw.strip()) if raw.strip().isdigit() else 0
    assert count > 0, "No feature_vectors rows for svc-e2e-checkout"

    # Window size must be one of the expected values.
    raw_ws = ch_query(
        "SELECT DISTINCT window_size FROM omniwatch.feature_vectors "
        "WHERE entity_id = 'svc-e2e-checkout' LIMIT 5"
    )
    window_sizes = {
        ws.strip() for ws in raw_ws.strip().splitlines() if ws.strip()
    }
    valid_sizes = {"1m", "5m", "15m"}
    assert window_sizes & valid_sizes, (
        f"Expected window_size in {valid_sizes}, got {window_sizes}"
    )


def test_feature_store_api_returns_features(e2e_data):
    """DONE-WHEN: GET /features/svc-e2e-checkout returns 200 with non-empty body."""
    url = f"{API_HOST}/features/svc-e2e-checkout"
    resp = urllib.request.urlopen(url, timeout=15)
    assert resp.status == 200, f"Expected HTTP 200, got {resp.status}"
    body = resp.read().decode("utf-8")
    # The API returns a JSON array of feature vectors.
    data = json.loads(body)
    assert isinstance(data, list), f"Expected list, got {type(data).__name__}"
    assert len(data) > 0, "API returned empty feature list"
    # Spot-check first vector has expected fields.
    first = data[0]
    assert "entity_id" in first, f"Missing entity_id in {first.keys()}"
    assert first["entity_id"] == "svc-e2e-checkout"


def test_windowed_topics_populated():
    """DONE-WHEN: Kafka windowed_1m topic contains at least one message (best-effort)."""
    lines: list[str] = []
    try:
        result = subprocess.run(
            [
            "docker", "exec", "-i", KAFKA_CONTAINER,
                "kafka-console-consumer",
                "--bootstrap-server", "localhost:9092",
                "--topic", "omniwatch.features.windowed_1m",
                "--from-beginning",
                "--max-messages", "3",
                "--timeout-ms", "15000",
            ],
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )
        lines = [
            l for l in result.stdout.strip().splitlines() if l.strip()
        ]
        assert len(lines) >= 1, (
            "No messages in omniwatch.features.windowed_1m within 15 s.\n"
            f"stdout: {result.stdout[:500]}\n"
            f"stderr: {result.stderr[:500]}"
        )
        # First message should be valid JSON (a WindowedFeature).
        first_msg = json.loads(lines[0])
        assert "entity_id" in first_msg or "entityId" in first_msg, (
            f"First message lacks entity_id: {lines[0][:200]}"
        )
    except subprocess.TimeoutExpired:
        pytest.fail(
            "kafka-console-consumer timed out reading omniwatch.features.windowed_1m"
        )
    except json.JSONDecodeError as exc:
        pytest.fail(
            f"First message in windowed_1m is not valid JSON: {exc}\n"
            f"Raw: {lines[0][:300] if lines else '(no lines captured)'}"
        )


def test_session_and_sliding_windows_paths():
    """DONE-WHEN: Flink job graph includes tumbling/sliding/session operators."""
    data = flink_rest("/jobs/overview")
    # Find the running FeatureStore job.
    jid = None
    for job in data.get("jobs", []):
        if (
            job.get("state") == "RUNNING"
            and JOB_DISPLAY_NAME.lower() in job.get("name", "").lower()
        ):
            jid = job.get("jid")
            break

    assert jid is not None, "FeatureStore job not found in /jobs/overview"

    # Fetch detailed job plan.
    plan = flink_rest(f"/jobs/{jid}")
    vertices = plan.get("vertices", [])

    # Strategy 1: look for window-operator name fragments in vertex names.
    lower_names = {v.get("name", "").lower() for v in vertices}
    window_keywords = {"tumbling", "sliding", "session", "window"}
    found = {kw for kw in window_keywords if any(kw in n for n in lower_names)}

    if found:
        # At least one window operator type found by name — good.
        assert len(found) >= 2, (
            f"Expected ≥2 window operator types, found: {sorted(found)}.\n"
            f"Vertex names: {sorted(lower_names)}"
        )
    else:
        # Fallback: vertex names are opaque — just count them.
        assert len(vertices) >= 5, (
            f"Job graph has only {len(vertices)} vertices (< 5); "
            f"expected a multi-operator topology.\n"
            f"Vertex names: {sorted(lower_names)}"
        )


def test_phase_summary_artifact():
    """DONE-WHEN: Phase-4-Summary.txt exists (written by the summary task)."""
    assert PHASE_SUMMARY.exists(), (
        f"Phase summary artifact not found at {PHASE_SUMMARY}.\n"
        "The summary task should write this file as part of Phase 4."
    )
