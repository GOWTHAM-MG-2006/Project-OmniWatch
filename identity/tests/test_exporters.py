"""OmniWatch Exporter registry + bundle tests (exporter-importer-split, A3)."""

from __future__ import annotations

import io
import zipfile

from fastapi.testclient import TestClient

from identity.store import MemoryStore

ADMIN_EMAIL = "admin"
ADMIN_PASSWORD = "Admin123"


def _admin_login(client: TestClient) -> dict:
    resp = client.post(
        "/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _login(client: TestClient, email: str, password: str) -> dict:
    resp = client.post("/auth/login", json={"email": email, "password": password})
    assert resp.status_code == 200, resp.text
    return resp.json()


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


def _create(client: TestClient, token: str, name: str) -> dict:
    resp = client.post("/workspaces", json={"name": name}, headers=_auth(token))
    assert resp.status_code == 201, resp.text
    return resp.json()


def test_register_numbers_exporters_and_lists(client: TestClient) -> None:
    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Bundle App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])

    first = client.post(
        "/exporters", json={"workspace_id": ws_id, "name": "web-01"}, headers=headers)
    assert first.status_code == 201, first.text
    assert first.json()["number"] == 1
    assert first.json()["name"] == "web-01"
    assert first.json()["endpoint_url"].endswith("/ingest")

    second = client.post(
        "/exporters", json={"workspace_id": ws_id, "name": "db-01"}, headers=headers)
    assert second.status_code == 201, second.text
    assert second.json()["number"] == 2

    listed = client.get(f"/exporters/by-workspace/{ws_id}", headers=headers)
    assert listed.status_code == 200, listed.text
    assert [(e["number"], e["name"]) for e in listed.json()] == [
        (1, "web-01"), (2, "db-01")]
    assert "api_token" not in listed.text


def test_bundle_zip_prefilled_and_403_for_stranger(
    client: TestClient, store: MemoryStore
) -> None:
    from identity.auth import hash_password

    owner = _admin_login(client)
    created = _create(client, owner["access_token"], "Zip App")
    ws_id = created["workspace"]["workspace_id"]
    slug = created["workspace"]["slug"]
    headers = _auth(owner["access_token"])

    client.post("/exporters", json={"workspace_id": ws_id, "name": "edge-01"},
                headers=headers)

    bundle = client.get(f"/exporters/bundle/{ws_id}?number=1", headers=headers)
    assert bundle.status_code == 200, bundle.text
    assert bundle.headers["content-type"] == "application/zip"
    zf = zipfile.ZipFile(io.BytesIO(bundle.content))
    assert set(zf.namelist()) == {
        "exporter-config.yaml", "install.sh", "update-endpoint.sh",
        "delete_agent.sh", "README.txt"}
    config = zf.read("exporter-config.yaml").decode()
    assert created["importer_endpoint"] in config
    assert slug in config
    assert "PASTE_IMPORTER_TOKEN_HERE" in config

    stranger_pw = "bundle-proof-77!"
    store.create_user("bundle-stranger@example.com", hash_password(stranger_pw))
    stranger = _login(client, "bundle-stranger@example.com", stranger_pw)
    denied = client.get(f"/exporters/bundle/{ws_id}?number=1",
                        headers=_auth(stranger["access_token"]))
    assert denied.status_code == 403, denied.text
    denied_list = client.get(f"/exporters/by-workspace/{ws_id}",
                             headers=_auth(stranger["access_token"]))
    assert denied_list.status_code == 403, denied_list.text


def test_bundle_includes_binary_when_packaged(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    fake_bin = tmp_path / "omniwatch-agent"
    fake_bin.write_bytes(b"\x7fELF-fake-binary")
    monkeypatch.setenv("OMNIWATCH_AGENT_BIN", str(fake_bin))

    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Bin App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])
    client.post("/exporters", json={"workspace_id": ws_id, "name": "edge-01"},
                headers=headers)

    bundle = client.get(f"/exporters/bundle/{ws_id}?number=1", headers=headers)
    assert bundle.status_code == 200, bundle.text
    zf = zipfile.ZipFile(io.BytesIO(bundle.content))
    assert set(zf.namelist()) == {
        "exporter-config.yaml", "install.sh", "update-endpoint.sh",
        "delete_agent.sh", "README.txt", "omniwatch-agent"}
    assert zf.read("omniwatch-agent") == b"\x7fELF-fake-binary"
    mode = (zf.getinfo("omniwatch-agent").external_attr >> 16) & 0o777
    assert mode & 0o111, f"binary must stay executable, mode={oct(mode)}"
    assert "INCLUDES the Linux exporter binary" in zf.read("README.txt").decode()


def test_bundle_config_only_without_binary(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("OMNIWATCH_AGENT_BIN", str(tmp_path / "missing"))

    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Nobin App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])
    client.post("/exporters", json={"workspace_id": ws_id, "name": "edge-01"},
                headers=headers)

    bundle = client.get(f"/exporters/bundle/{ws_id}?number=1", headers=headers)
    assert bundle.status_code == 200, bundle.text
    zf = zipfile.ZipFile(io.BytesIO(bundle.content))
    assert set(zf.namelist()) == {
        "exporter-config.yaml", "install.sh", "update-endpoint.sh",
        "delete_agent.sh", "README.txt"}
    assert "does NOT include the exporter binary" in zf.read("README.txt").decode()


def test_update_endpoint_script_reuses_token_and_restarts(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("OMNIWATCH_AGENT_BIN", str(tmp_path / "missing"))

    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Updater App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])
    client.post("/exporters", json={"workspace_id": ws_id, "name": "edge-01"},
                headers=headers)

    bundle = client.get(f"/exporters/bundle/{ws_id}?number=1", headers=headers)
    assert bundle.status_code == 200, bundle.text
    zf = zipfile.ZipFile(io.BytesIO(bundle.content))
    assert "update-endpoint.sh" in zf.namelist()
    mode = (zf.getinfo("update-endpoint.sh").external_attr >> 16) & 0o777
    assert mode & 0o111, f"updater must stay executable, mode={oct(mode)}"
    script = zf.read("update-endpoint.sh").decode()
    # Asks for the URL only — never prompts for the token.
    assert "endpoint" in script.lower() and "install.sh" in script
    assert "OMNIWATCH_IMPORTER_API_TOKEN" in script
    assert script.count("api_token") >= 1  # reads saved token back from yaml
    assert "Paste importer API token" not in script  # no token prompt path
    assert "README" not in script  # sanity: we read the right file
    assert "TUNNEL ROTATED" in zf.read("README.txt").decode()


def test_install_preflight_warns_without_docker_and_documents_optional(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("OMNIWATCH_AGENT_BIN", str(tmp_path / "missing"))

    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Preflight App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])
    client.post("/exporters", json={"workspace_id": ws_id, "name": "edge-01"},
                headers=headers)

    bundle = client.get(f"/exporters/bundle/{ws_id}?number=1", headers=headers)
    assert bundle.status_code == 200, bundle.text
    zf = zipfile.ZipFile(io.BytesIO(bundle.content))
    script = zf.read("install.sh").decode()
    # Preflight is warn-not-fail: docker reachable path + heartbeat fallback.
    assert "docker: reachable" in script
    assert "heartbeat-only mode" in script
    assert "LoggingDriver" in script or "logging driver" in script
    assert "exit 1" not in script.split("heartbeat-only mode")[0].split(
        "docker: reachable")[-1]  # no fatal exit inside preflight
    # Optional docker section is commented out (backward compatible).
    yaml_text = zf.read("exporter-config.yaml").decode()
    assert "#docker:" in yaml_text and "containers:" in yaml_text
    assert "PASTE_IMPORTER_TOKEN_HERE" in yaml_text  # old keys intact
    # NOT_CONNECTED matrix is a README-section deliverable (no dashboard).
    readme = zf.read("README.txt").decode()
    assert "NOT CONNECTED" in readme
    assert "CloudWatch" in readme and "Kubernetes" in readme


def test_delete_agent_script_uninstalls_and_stays_executable(
    client: TestClient, tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("OMNIWATCH_AGENT_BIN", str(tmp_path / "missing"))

    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Uninst App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])
    client.post("/exporters", json={"workspace_id": ws_id, "name": "edge-01"},
                headers=headers)

    bundle = client.get(f"/exporters/bundle/{ws_id}?number=1", headers=headers)
    assert bundle.status_code == 200, bundle.text
    zf = zipfile.ZipFile(io.BytesIO(bundle.content))
    assert "delete_agent.sh" in zf.namelist()
    mode = (zf.getinfo("delete_agent.sh").external_attr >> 16) & 0o777
    assert mode & 0o111, f"uninstaller must stay executable, mode={oct(mode)}"
    script = zf.read("delete_agent.sh").decode()
    # Uninstaller behavior: confirm gate, stop via pidfile, remove files.
    assert "[y/N]" in script
    assert "agent.pid" in script and "kill" in script
    assert "agent.log" in script and "exporter-config.yaml" in script
    assert "omniwatch-agent" in script
    assert "--yes" in script
    assert "heartbeat" not in script  # sanity: we read the right file
    assert "UNINSTALL" in zf.read("README.txt").decode()


def test_delete_removes_exporter_and_frees_list(client: TestClient) -> None:
    pair = _admin_login(client)
    created = _create(client, pair["access_token"], "Del App")
    ws_id = created["workspace"]["workspace_id"]
    headers = _auth(pair["access_token"])
    client.post("/exporters", json={"workspace_id": ws_id, "name": "old-01"},
                headers=headers)

    deleted = client.delete(f"/exporters/{ws_id}/1", headers=headers)
    assert deleted.status_code == 200, deleted.text
    assert deleted.json() == {
        "workspace_id": ws_id, "number": 1, "deleted": True}

    listed = client.get(f"/exporters/by-workspace/{ws_id}", headers=headers)
    assert listed.status_code == 200, listed.text
    assert listed.json() == []

    # Unknown number is 403 (never 404-leak), same as unknown workspace.
    assert client.delete(f"/exporters/{ws_id}/9", headers=headers).status_code == 403
    assert client.delete("/exporters/does-not-exist/1", headers=headers).status_code == 403


def test_delete_cross_user_403(
    client: TestClient, store: MemoryStore
) -> None:
    from identity.auth import hash_password

    owner = _admin_login(client)
    created = _create(client, owner["access_token"], "Del Owned")
    ws_id = created["workspace"]["workspace_id"]
    client.post("/exporters",
                json={"workspace_id": ws_id, "name": "victim"},
                headers=_auth(owner["access_token"]))

    stranger_pw = "del-proof-55!"
    store.create_user("del-stranger@example.com", hash_password(stranger_pw))
    stranger = _login(client, "del-stranger@example.com", stranger_pw)

    denied = client.delete(f"/exporters/{ws_id}/1",
                           headers=_auth(stranger["access_token"]))
    assert denied.status_code == 403, denied.text
