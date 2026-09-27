"""OmniWatch Importer service tests — auth/routing/inserts (verify + CH mocked)."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import importer.main as imp


class FakeCH:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list, list[str]]] = []

    def insert(self, table: str, rows: list, column_names: list[str]) -> None:
        self.inserts.append((table, list(rows), list(column_names)))


@pytest.fixture()
def client(monkeypatch) -> TestClient:
    monkeypatch.setattr(
        imp, "verify_token",
        lambda token: {"workspace_id": "ws-1", "workspace_slug": "box-shop"},
    )
    fake = FakeCH()
    monkeypatch.setattr(imp, "_ch_client", lambda db: fake)
    imp._SEEN.clear()
    app = imp.create_app()
    transport = TestClient(app)
    transport.fake_ch = fake  # type: ignore[attr-defined]
    return transport


def _bearer() -> dict:
    return {"Authorization": "Bearer good-token"}


def test_ingest_metrics_routes_to_metrics_table(client: TestClient) -> None:
    resp = client.post("/ingest", json={
        "telemetry_type": "metrics",
        "entity_id": "aws-box-agent",
        "metrics": [{"metric_name": "cpu_usage", "value": 41.2}],
    }, headers=_bearer())
    assert resp.status_code == 202, resp.text
    body = resp.json()
    assert body["workspace_slug"] == "box-shop"
    assert body["inserted"] == {"metrics": 1, "logs": 0, "traces": 0}
    tables = [t for t, _, _ in client.fake_ch.inserts]  # type: ignore[attr-defined]
    assert tables == ["metrics"]


def test_ingest_security_type_routes_to_logs(client: TestClient) -> None:
    resp = client.post("/ingest", json={
        "telemetry_type": "security_alerts",
        "entity_id": "aws-box-agent",
        "logs": [{"message": "brute force suspected"}],
    }, headers=_bearer())
    assert resp.status_code == 202, resp.text
    assert resp.json()["inserted"]["logs"] == 1
    tables = [t for t, _, _ in client.fake_ch.inserts]  # type: ignore[attr-defined]
    assert tables == ["logs"]


def test_ingest_profiling_routes_to_traces(client: TestClient) -> None:
    resp = client.post("/ingest", json={
        "telemetry_type": "profiling",
        "entity_id": "aws-box-agent",
        "traces": [{"trace_id": "t1", "span_id": "s1"}],
    }, headers=_bearer())
    assert resp.status_code == 202, resp.text
    assert resp.json()["inserted"]["traces"] == 1


def test_ingest_missing_token_401(client: TestClient) -> None:
    resp = client.post("/ingest", json={
        "telemetry_type": "metrics", "entity_id": "x", "metrics": []})
    assert resp.status_code == 401, resp.text


def test_ingest_unknown_type_422(client: TestClient) -> None:
    resp = client.post("/ingest", json={
        "telemetry_type": "telepathy", "entity_id": "x"}, headers=_bearer())
    assert resp.status_code == 422, resp.text


def test_ingest_bad_token_403(client: TestClient, monkeypatch) -> None:
    from fastapi import HTTPException

    def _deny(token: str) -> dict:
        raise HTTPException(status_code=403, detail="invalid importer token")

    monkeypatch.setattr(imp, "verify_token", _deny)
    resp = client.post("/ingest", json={
        "telemetry_type": "metrics", "entity_id": "x",
        "metrics": [{"metric_name": "m", "value": 1.0}]},
        headers={"Authorization": "Bearer bad"})
    assert resp.status_code == 403, resp.text


def test_raw_topic_routing_covers_all_types() -> None:
    assert imp._raw_topics("metrics") == ["omniwatch.metrics.raw"]
    assert imp._raw_topics("logs") == ["omniwatch.logs.raw"]
    assert imp._raw_topics("traces") == ["omniwatch.traces.raw"]
    assert imp._raw_topics("profiling") == ["omniwatch.traces.raw"]
    assert imp._raw_topics("metadata") == ["omniwatch.events.raw"]
    assert imp._raw_topics("state") == ["omniwatch.events.raw"]
    assert imp._raw_topics("audit_logs") == [
        "omniwatch.security.raw", "omniwatch.security.events"]
    assert imp._raw_topics("security_alerts") == [
        "omniwatch.security.raw", "omniwatch.security.events"]


def test_fanout_publishes_per_point_without_token(client: TestClient) -> None:
    sent: list[tuple] = []

    class FakeProducer:
        def send(self, topic: str, value: dict) -> None:
            sent.append((topic, value))

        def flush(self, timeout: float | None = None) -> None:
            return None

    import importer.main as imp_module

    imp_module._PRODUCER = FakeProducer()
    try:
        resp = client.post("/ingest", json={
            "telemetry_type": "security_alerts",
            "entity_id": "box-1",
            "logs": [{"message": "intrusion?"}],
        }, headers=_bearer())
        assert resp.status_code == 202, resp.text
    finally:
        imp_module._PRODUCER = None
    topics = [t for t, _ in sent]
    assert "omniwatch.security.raw" in topics
    assert "omniwatch.security.events" in topics
    raw_msg = next(v for t, v in sent if t == "omniwatch.security.raw")
    assert "resourceLogs" in raw_msg
    attrs = raw_msg["resourceLogs"][0]["resource"]["attributes"]
    by_key = {a["key"]: a["value"]["stringValue"] for a in attrs}
    assert by_key["service.name"] == "box-1"
    assert by_key["omniwatch.workspace"] == "box-shop"
    flat_msg = next(v for t, v in sent if t == "omniwatch.security.events")
    assert flat_msg["entity_id"] == "box-1"
    assert flat_msg["event_type"] == "security_alerts"
    assert "token" not in str(sent).lower()


def test_kafka_down_still_202s(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(imp, "_kafka_producer",
                        lambda: (_ for _ in ()).throw(ConnectionError("down")))
    resp = client.post("/ingest", json={
        "telemetry_type": "metrics", "entity_id": "x",
        "metrics": [{"metric_name": "m", "value": 1.0}]}, headers=_bearer())
    assert resp.status_code == 202, resp.text


def test_ring_caps_at_150_and_recent_logs_auth(client: TestClient) -> None:
    for i in range(200):
        imp._record_live_log("box-shop", "e", "INFO", f"line-{i}")
    assert len(imp._LOGS["box-shop"]) == 150
    assert imp._LOGS["box-shop"][0]["message"] == "line-50"
    assert imp._LOGS["box-shop"][-1]["message"] == "line-199"


def test_recent_logs_endpoint(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(imp, "_check_workspace_owner",
                        lambda ws, auth: "box-shop")
    imp._record_live_log("box-shop", "e1", "ERROR", "boom",
                         "2026-01-01T00:00:00+00:00")
    resp = client.get("/activity/recent-logs/by-workspace/ws-1",
                      headers={"Authorization": "Bearer user-jwt"})
    assert resp.status_code == 200, resp.text
    logs = resp.json()["logs"]
    assert logs[-1]["message"] == "boom"
    assert logs[-1]["entity"] == "e1"

    assert client.get("/activity/recent-logs/by-workspace/ws-1").status_code == 401

    monkeypatch.setattr(imp, "_check_workspace_owner",
                        lambda ws, auth: None)
    denied = client.get("/activity/recent-logs/by-workspace/ws-1",
                        headers={"Authorization": "Bearer stranger-jwt"})
    assert denied.status_code == 403, denied.text


def test_rate_limit_429(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(imp, "RATE_ACTIVITY", ("activity", 60.0, 2))
    imp._RATE_BUCKETS.clear()
    headers = {"Authorization": "Bearer user-jwt"}
    monkeypatch.setattr(imp, "_check_workspace_owner",
                        lambda ws, auth: "box-shop")
    assert client.get(
        "/activity/by-workspace/ws-1", headers=headers).status_code == 200
    assert client.get(
        "/activity/by-workspace/ws-1", headers=headers).status_code == 200
    limited = client.get("/activity/by-workspace/ws-1", headers=headers)
    assert limited.status_code == 429, limited.text
    assert "Retry-After" in limited.headers


def test_activity_empty_initially(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(imp, "_check_workspace_owner", lambda ws, auth: "box-shop")
    resp = client.get("/activity/by-workspace/ws-1",
                      headers={"Authorization": "Bearer user-jwt"})
    assert resp.status_code == 200, resp.text
    assert resp.json() == {"workspace_slug": "box-shop", "activity": []}


def test_activity_records_accepted_ingest(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(imp, "_check_workspace_owner", lambda ws, auth: "box-shop")
    posted = client.post("/ingest", json={
        "telemetry_type": "metrics", "entity_id": "exporter-1-box-shop",
        "metrics": [{"metric_name": "cpu_load_1m", "value": 0.3}]},
        headers=_bearer())
    assert posted.status_code == 202, posted.text
    resp = client.get("/activity/by-workspace/ws-1",
                      headers={"Authorization": "Bearer user-jwt"})
    assert resp.status_code == 200, resp.text
    (point,) = resp.json()["activity"]
    assert point["entity_id"] == "exporter-1-box-shop"
    assert point["telemetry_type"] == "metrics"
    assert point["last_seen"]


def test_activity_requires_bearer(client: TestClient) -> None:
    resp = client.get("/activity/by-workspace/ws-1")
    assert resp.status_code == 401, resp.text


def test_activity_denies_non_owner(client: TestClient, monkeypatch) -> None:
    monkeypatch.setattr(imp, "_check_workspace_owner", lambda ws, auth: None)
    resp = client.get("/activity/by-workspace/ws-1",
                      headers={"Authorization": "Bearer stranger-jwt"})
    assert resp.status_code == 403, resp.text
