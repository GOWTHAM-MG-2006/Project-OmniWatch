"""OmniWatch Onboarding wizard tests — submit/reread/422/403/determinism."""

from __future__ import annotations

import json

from fastapi.testclient import TestClient


def _register(client: TestClient, email: str,
              password: str = "correct-horse-2024!") -> dict:
    resp = client.post("/auth/register",
                       json={"email": email, "password": password})
    assert resp.status_code == 201, resp.text
    return resp.json()


def _login(client: TestClient, email: str,
           password: str = "correct-horse-2024!") -> dict:
    resp = client.post("/auth/login",
                       json={"email": email, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _payload(**overrides) -> dict:
    body = {
        "app_name": "Acme API",
        "app_type": "api",
        "cloud_provider": "aws",
        "service_endpoints": ["https://api.acme.example/health"],
        "expected_eps": "1k-10k",
        "log_volume": "100-1k",
        "retention_days": 30,
        "alert_contact": "ops@acme.example",
    }
    body.update(overrides)
    return body


def _workspace(client: TestClient, token: str, name: str = "Acme App") -> dict:
    resp = client.post("/workspaces", json={"name": name},
                       headers=_auth(token))
    assert resp.status_code == 201, resp.text
    return resp.json()["workspace"]


# ---------------------------------------------------------------------------
# Happy path: full submit -> config.json present -> re-read identical
# ---------------------------------------------------------------------------


def test_full_submit_config_present_reread_identical(
        client: TestClient) -> None:
    from identity.onboarding import get_cached_config_json

    _register(client, "wiz@example.com")
    token = _login(client, "wiz@example.com")["access_token"]
    ws = _workspace(client, token)

    submitted = client.post(f"/workspaces/{ws['workspace_id']}/onboarding",
                            json=_payload(), headers=_auth(token))
    assert submitted.status_code == 200, submitted.text
    post_body = submitted.json()
    assert post_body["workspace_id"] == ws["workspace_id"]
    assert post_body["slug"] == ws["slug"]
    assert post_body["answers"]["app_name"] == "Acme API"
    assert set(post_body["suggested_config"]) == {
        "queue_depth", "batch_size", "poll_interval_s", "scrape_interval_s"}
    assert post_body["config_path"].endswith(
        f"workspaces/{ws['slug']}/config.json")
    assert post_body["updated_at"]

    # config.json present (write-through cache mirrors the MinIO body).
    cached = get_cached_config_json()
    assert post_body["config_path"] in cached, sorted(cached)
    stored = json.loads(cached[post_body["config_path"]])
    assert stored["answers"] == post_body["answers"]
    assert stored["suggested_config"] == post_body["suggested_config"]

    # Idempotent re-read: GET returns the same payload as POST.
    reread = client.get(f"/workspaces/{ws['workspace_id']}/onboarding",
                        headers=_auth(token))
    assert reread.status_code == 200, reread.text
    assert reread.json() == post_body


def test_ml_app_type_sane_suggestions(client: TestClient) -> None:
    _register(client, "ml@example.com")
    token = _login(client, "ml@example.com")["access_token"]
    ws = _workspace(client, token, "ML Predictor")
    resp = client.post(
        f"/workspaces/{ws['workspace_id']}/onboarding",
        json=_payload(app_type="ml", cloud_provider="gcp",
                      expected_eps=">10k", log_volume=">10k"),
        headers=_auth(token))
    assert resp.status_code == 200, resp.text
    suggested = resp.json()["suggested_config"]
    assert suggested["queue_depth"] >= 20000
    assert suggested["batch_size"] >= 1000


def test_resubmit_updates_not_duplicates(client: TestClient) -> None:
    _register(client, "re@example.com")
    token = _login(client, "re@example.com")["access_token"]
    ws = _workspace(client, token)
    url = f"/workspaces/{ws['workspace_id']}/onboarding"
    first = client.post(url, json=_payload(), headers=_auth(token))
    assert first.status_code == 200, first.text
    second = client.post(url, json=_payload(app_name="Acme API v2"),
                         headers=_auth(token))
    assert second.status_code == 200, second.text
    reread = client.get(url, headers=_auth(token))
    assert reread.status_code == 200, reread.text
    assert reread.json()["answers"]["app_name"] == "Acme API v2"
    assert reread.json() == second.json()


def test_read_before_submit_404(client: TestClient) -> None:
    _register(client, "fresh@example.com")
    token = _login(client, "fresh@example.com")["access_token"]
    ws = _workspace(client, token)
    resp = client.get(f"/workspaces/{ws['workspace_id']}/onboarding",
                      headers=_auth(token))
    assert resp.status_code == 404, resp.text


# ---------------------------------------------------------------------------
# Failure matrix: out-of-range 422, script-injection 422, non-owner 403
# ---------------------------------------------------------------------------


def test_out_of_range_422(client: TestClient) -> None:
    _register(client, "range@example.com")
    token = _login(client, "range@example.com")["access_token"]
    ws = _workspace(client, token)
    url = f"/workspaces/{ws['workspace_id']}/onboarding"
    bad_bodies = [
        _payload(expected_eps="999999999999"),
        _payload(log_volume="unbounded"),
        _payload(app_type="mainframe"),
        _payload(cloud_provider="mars"),
        _payload(retention_days=0),
        _payload(retention_days=99999),
        _payload(service_endpoints=[""]),
    ]
    for bad in bad_bodies:
        resp = client.post(url, json=bad, headers=_auth(token))
        assert resp.status_code == 422, (bad, resp.text)


def test_script_injection_422(client: TestClient) -> None:
    _register(client, "xss@example.com")
    token = _login(client, "xss@example.com")["access_token"]
    ws = _workspace(client, token)
    url = f"/workspaces/{ws['workspace_id']}/onboarding"
    injections = [
        _payload(app_name="<script>alert(1)</script>"),
        _payload(app_name="api; rm -rf /"),
        _payload(app_name="${{ secrets.AWS_KEY }}"),
        _payload(alert_contact="ops@example.com$(whoami)"),
        _payload(alert_contact="javascript:alert(1)"),
        _payload(service_endpoints=["https://ok.example/ && evil"]),
        _payload(service_endpoints=["https://ok.example/`id`"]),
    ]
    for bad in injections:
        resp = client.post(url, json=bad, headers=_auth(token))
        assert resp.status_code == 422, (bad, resp.text)


def test_non_owner_403_and_unauthenticated_401(
        client: TestClient) -> None:
    _register(client, "owner3@example.com")
    _register(client, "intruder3@example.com")
    owner_t = _login(client, "owner3@example.com")["access_token"]
    evil_t = _login(client, "intruder3@example.com")["access_token"]
    ws = _workspace(client, owner_t, "Private App")
    url = f"/workspaces/{ws['workspace_id']}/onboarding"
    assert client.post(
        url, json=_payload(), headers=_auth(evil_t)).status_code == 403
    assert client.get(url, headers=_auth(evil_t)).status_code == 403
    assert client.post(url, json=_payload()).status_code == 401
    assert client.get(url).status_code == 401
    # Unknown id: same 403 (no existence oracle).
    unknown = "00000000-0000-0000-0000-000000000000"
    assert client.post(f"/workspaces/{unknown}/onboarding", json=_payload(),
                       headers=_auth(evil_t)).status_code == 403
    # Owner flow still works after the rejected attempts.
    assert client.post(
        url, json=_payload(), headers=_auth(owner_t)).status_code == 200


# ---------------------------------------------------------------------------
# Determinism: identical bands -> identical suggestions; bands order sane
# ---------------------------------------------------------------------------


def test_suggestions_deterministic_and_band_ordered(
        client: TestClient) -> None:
    from identity.onboarding import suggest_config

    _register(client, "det@example.com")
    token = _login(client, "det@example.com")["access_token"]
    ws = _workspace(client, token)
    url = f"/workspaces/{ws['workspace_id']}/onboarding"

    low = client.post(
        url, json=_payload(expected_eps="<100", log_volume="<100"),
        headers=_auth(token)).json()["suggested_config"]
    high = client.post(
        url, json=_payload(expected_eps=">10k", log_volume=">10k"),
        headers=_auth(token)).json()["suggested_config"]
    repeat = client.post(
        url, json=_payload(expected_eps=">10k", log_volume=">10k"),
        headers=_auth(token)).json()["suggested_config"]
    assert repeat == high  # repeatable for identical inputs
    assert high["queue_depth"] > low["queue_depth"]
    assert high["batch_size"] > low["batch_size"]
    assert high["poll_interval_s"] < low["poll_interval_s"]

    # Pure function: same bands -> same suggestions without any HTTP I/O.
    assert (suggest_config(">10k", ">10k").model_dump()
            == suggest_config(">10k", ">10k").model_dump())
    assert (suggest_config("<100", ">10k").model_dump()
            == suggest_config(">10k", "<100").model_dump())
