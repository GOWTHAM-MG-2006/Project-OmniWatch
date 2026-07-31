"""
OmniWatch — Phase 3 E2E Tests

Verifies the entity-resolution Flink job end-to-end:
  - GCP / AWS / K8s entities are parsed and published to omniwatch.entities.resolved
  - Business tags are attached via business_tags.yaml rules
  - Duplicate entity_id within the 5-minute dedup window produces a single record
  - Trace spans produce CALLS relationship edges in omniwatch.entities.relationships
  - Malformed JSON does not crash the job
  - Job stays RUNNING after processing real events
"""
import time
import uuid

import pytest

# Input topics (Phase 2 normalised telemetry)
TOPIC_METRICS = "omniwatch.metrics.normalized"
TOPIC_TRACES = "omniwatch.traces.normalized"

# Output topics
TOPIC_RESOLVED = "omniwatch.entities.resolved"
TOPIC_RELATIONSHIPS = "omniwatch.entities.relationships"

# Unique suffix per test session to avoid cross-test dedup interference
_SESSION = uuid.uuid4().hex[:8]


@pytest.fixture
def unique_id():
    return f"{_SESSION}-{uuid.uuid4().hex[:8]}"


# --------------------------------------------------------------------------- #
# Test 1 — GCP entity parsed and published to resolved topic
# --------------------------------------------------------------------------- #
def test_gcp_entity_resolved(kafka_producer, test_group_id, unique_id):
    """A GCP compute instance entity_id is parsed and published with canonical id."""
    raw = f"projects/my-project/zones/us-central1-a/instances/web-{unique_id}"
    canonical = f"gcp:API_NODE/web-{unique_id}"

    event = {
        "entity_id": raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "performance",
        "source_topic": TOPIC_METRICS,
    }
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.flush(timeout=5)

    from conftest import wait_for_output
    result = wait_for_output(TOPIC_RESOLVED, test_group_id, canonical, timeout=30)

    assert result is not None, "No resolved entity found in output topic"
    assert result["entity_id"] == canonical
    assert result["entity_type"] == "API_NODE"
    assert result["provider"] == "gcp"
    assert result["region"] == "us-central1"
    assert result["name"] == f"web-{unique_id}"
    assert raw in result["raw_identifiers"]


# --------------------------------------------------------------------------- #
# Test 2 — AWS entity parsed and published
# --------------------------------------------------------------------------- #
def test_aws_entity_resolved(kafka_producer, test_group_id, unique_id):
    """An AWS EC2 instance ARN is parsed and published with canonical id."""
    raw = f"arn:aws:ec2:us-east-1:123456789012:instance/i-{unique_id}"
    canonical = f"aws:API_NODE/i-{unique_id}"

    event = {
        "entity_id": raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "performance",
        "source_topic": TOPIC_METRICS,
    }
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.flush(timeout=5)

    from conftest import wait_for_output
    result = wait_for_output(TOPIC_RESOLVED, test_group_id, canonical, timeout=30)

    assert result is not None, "No resolved entity found in output topic"
    assert result["entity_id"] == canonical
    assert result["provider"] == "aws"
    assert result["region"] == "us-east-1"
    assert result["raw_identifiers"][0] == raw


# --------------------------------------------------------------------------- #
# Test 3 — K8s entity parsed and published
# --------------------------------------------------------------------------- #
def test_k8s_entity_resolved(kafka_producer, test_group_id, unique_id):
    """A K8s pod identifier (namespace/name-hash) is parsed and published."""
    pod_name = f"web-{unique_id[:8]}"
    raw = f"default/{pod_name}-a1b2c"
    canonical = f"k8s:API_NODE/{pod_name}"

    event = {
        "entity_id": raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "performance",
        "source_topic": TOPIC_METRICS,
    }
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.flush(timeout=5)

    from conftest import wait_for_output
    result = wait_for_output(TOPIC_RESOLVED, test_group_id, canonical, timeout=30)

    assert result is not None, "No resolved entity found in output topic"
    assert result["entity_id"] == canonical
    assert result["provider"] == "k8s"
    assert result["name"] == pod_name
    assert raw in result["raw_identifiers"]


# --------------------------------------------------------------------------- #
# Test 4 — Business tags attached correctly via name rules
# --------------------------------------------------------------------------- #
def test_business_tags_attached(kafka_producer, test_group_id, unique_id):
    """An entity whose name matches a tag rule gets the rule's tags (not defaults)."""
    raw = f"projects/my-project/zones/us-central1-a/instances/payment-svc-{unique_id}"
    canonical = f"gcp:API_NODE/payment-svc-{unique_id}"

    event = {
        "entity_id": raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "performance",
        "source_topic": TOPIC_METRICS,
    }
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.flush(timeout=5)

    from conftest import wait_for_output
    result = wait_for_output(TOPIC_RESOLVED, test_group_id, canonical, timeout=30)

    assert result is not None, "No resolved entity found in output topic"
    tags = result["business_tags"]
    # The name_regex ^(payment|checkout) rule for API_NODE should override defaults
    assert tags["service_name"] == "payments"
    assert tags["owner_team"] == "payments-team"
    assert tags["criticality"] == "CRITICAL"
    assert tags["sla_tier"] == "24x7"


# --------------------------------------------------------------------------- #
# Test 5 — Deduplication: same entity_id sent twice → one resolved record
# --------------------------------------------------------------------------- #
def test_deduplication_single_output(kafka_producer, test_group_id, unique_id):
    """Sending the same entity_id twice within the dedup window yields one record."""
    raw = f"projects/my-project/zones/us-central1-a/instances/dedup-svc-{unique_id}"
    canonical = f"gcp:API_NODE/dedup-svc-{unique_id}"

    event = {
        "entity_id": raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "performance",
        "source_topic": TOPIC_METRICS,
    }
    # Send twice rapidly
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.flush(timeout=5)

    time.sleep(3)

    from conftest import consume_all_matching
    matching = consume_all_matching(
        TOPIC_RESOLVED, test_group_id,
        lambda v: v is not None and v.get("entity_id") == canonical,
        timeout=15,
    )

    # Exactly one record should be emitted (first observation; second is deduped)
    assert len(matching) == 1, f"Expected 1 deduped record, got {len(matching)}"


# --------------------------------------------------------------------------- #
# Test 6 — Relationship edges published from trace spans
# --------------------------------------------------------------------------- #
def test_relationship_edges_from_spans(kafka_producer, test_group_id, unique_id):
    """Two spans sharing a trace_id produce a CALLS edge (parent → child)."""
    trace_id = f"trace-{unique_id}"

    # Parent span: GCP entity
    parent_raw = f"projects/p1/zones/us-central1-a/instances/api-gw-{unique_id}"
    parent_span_id = f"span-p-{unique_id[:8]}"

    # Child span: AWS entity, parent_span_id references parent
    child_raw = f"arn:aws:ec2:us-east-1:123456789012:instance/i-{unique_id}"
    child_span_id = f"span-c-{unique_id[:8]}"

    parent_event = {
        "entity_id": parent_raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "trace",
        "source_topic": TOPIC_TRACES,
        "trace_id": trace_id,
        "span_id": parent_span_id,
        "span_name": "parent-call",
        "duration_ms": 50,
        "status": "OK",
    }

    child_event = {
        "entity_id": child_raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "trace",
        "source_topic": TOPIC_TRACES,
        "trace_id": trace_id,
        "span_id": child_span_id,
        "parent_span_id": parent_span_id,
        "span_name": "child-call",
        "duration_ms": 30,
        "status": "OK",
    }

    # Send parent first, then child (child references parent's span_id)
    kafka_producer.send(TOPIC_TRACES, parent_event)
    kafka_producer.send(TOPIC_TRACES, child_event)
    kafka_producer.flush(timeout=5)

    expected_source = f"gcp:API_NODE/api-gw-{unique_id}"
    from conftest import consume_filtered
    result = consume_filtered(
        TOPIC_RELATIONSHIPS, test_group_id,
        lambda v: v is not None
        and v.get("relationship_type") == "CALLS"
        and v.get("source_entity_id") == expected_source,
        timeout=30,
    )

    assert result is not None, "No CALLS relationship edge found for this test's parent entity"
    assert result["source_entity_id"] == expected_source
    assert result["target_entity_id"] == f"aws:API_NODE/i-{unique_id}"
    assert "trace_id" in result.get("properties", {})


# --------------------------------------------------------------------------- #
# Test 7 — Malformed JSON does not crash the job
# --------------------------------------------------------------------------- #
def test_malformed_json_does_not_crash(kafka_producer, test_group_id, unique_id):
    """Malformed JSON on an input topic should be skipped, not crash the job."""
    # Send malformed JSON
    kafka_producer.send(TOPIC_METRICS, "{not valid json")
    kafka_producer.flush(timeout=5)

    # Give Flink time to process (and not crash)
    time.sleep(5)

    # Verify job is still RUNNING
    import requests
    r = requests.get("http://localhost:8081/jobs", timeout=5)
    assert r.status_code == 200
    jobs = r.json().get("jobs", [])
    running = [j for j in jobs if j["status"] == "RUNNING"]
    assert len(running) >= 1, "Flink job should still be RUNNING after malformed JSON"


# --------------------------------------------------------------------------- #
# Test 8 — Job stays RUNNING after processing valid events
# --------------------------------------------------------------------------- #
def test_job_running_after_processing(kafka_producer, test_group_id, unique_id):
    """After processing valid events, the entity-resolution job remains RUNNING."""
    raw = f"projects/my-project/zones/us-central1-a/instances/health-check-{unique_id}"
    canonical = f"gcp:API_NODE/health-check-{unique_id}"

    event = {
        "entity_id": raw,
        "entity_type": "API_NODE",
        "timestamp": int(time.time() * 1000),
        "source_type": "performance",
        "source_topic": TOPIC_METRICS,
    }
    kafka_producer.send(TOPIC_METRICS, event)
    kafka_producer.flush(timeout=5)

    # Wait for processing
    time.sleep(5)

    import requests
    r = requests.get("http://localhost:8081/jobs", timeout=5)
    assert r.status_code == 200
    jobs = r.json().get("jobs", [])
    running = [j for j in jobs if j["status"] == "RUNNING"]
    assert len(running) >= 1, "Flink job should be RUNNING"

    # Also verify the entity was actually resolved (proves the job is alive and working)
    from conftest import wait_for_output
    result = wait_for_output(TOPIC_RESOLVED, test_group_id, canonical, timeout=20)
    assert result is not None, "Entity should be resolved in output topic"
