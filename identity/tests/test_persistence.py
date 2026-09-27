"""OmniWatch Identity persistence tests — mirror + reload (exporter-importer-split).

FakeCH stands in for ClickHouse; the OMNIWATCH_IDENTITY_STORE flag gates
everything (memory mode is a strict no-op so the main suite stays hermetic).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from identity import persistence as persistence_module
from identity.agents import get_agent_registry, reset_agent_registry
from identity.exporters import get_exporter_registry, reset_exporter_registry
from identity.importers import get_importer_registry, reset_importer_registry
from identity.workspaces import get_registry, reset_registry


class FakeResult:
    def __init__(self, rows: list) -> None:
        self.result_rows = rows


class FakeCH:
    LAYOUT = {
        "workspaces": ["workspace_id", "user_id", "name", "slug",
                       "app_type", "cloud_provider", "endpoints",
                       "expected_volume", "retention_days", "created_at",
                       "deleted", "mirrored_at"],
        "workspace_importers": ["workspace_id", "user_id", "slug",
                                "endpoint_url", "token_hash", "created_at",
                                "mirrored_at"],
        "workspace_exporters": ["workspace_id", "user_id", "number",
                                "name", "created_at", "deleted",
                                "mirrored_at"],
        "agent_bindings": ["binding_id", "user_id", "workspace_id",
                           "workspace_slug", "agent_endpoint", "token_hash",
                           "status", "created_at", "deleted", "mirrored_at"],
    }

    def __init__(self, tables: dict) -> None:
        self.tables = tables
        self.inserts: list[tuple] = []

    def query(self, sql: str, **kwargs) -> FakeResult:
        import re

        for name, rows in self.tables.items():
            if f"FROM {name}" in sql:
                selected = re.search(r"SELECT (.*?) FROM ",
                                     sql, re.DOTALL).group(1)
                wanted = [c.strip() for c in selected.split(",")]
                layout = self.LAYOUT[name]
                projected = [tuple(r[layout.index(c)] for c in wanted)
                             for r in rows]
                # Emulate LIMIT 1 BY mirrored_at: newest write wins.
                midx = layout.index("mirrored_at")
                latest: dict = {}
                for full, row in zip(rows, projected):
                    key = full[0] if name in (
                        "workspaces", "workspace_importers",
                        "agent_bindings") else (full[0], full[2])
                    if key not in latest or full[midx] > latest[key][0]:
                        latest[key] = (full[midx], row)
                return FakeResult([row for _, row in latest.values()])
        return FakeResult([])

    def insert(self, table: str, rows: list, column_names: list) -> None:
        self.inserts.append((table, [list(r) for r in rows], list(column_names)))

    def command(self, *args, **kwargs) -> None:
        pass

    def close(self) -> None:
        pass


def _ch_env(monkeypatch, fake: FakeCH) -> None:
    monkeypatch.setenv("OMNIWATCH_IDENTITY_STORE", "clickhouse")
    monkeypatch.setattr(persistence_module, "_connect", lambda: fake)


@pytest.fixture(autouse=True)
def _clean_registries():
    reset_registry()
    reset_importer_registry()
    reset_exporter_registry()
    reset_agent_registry()
    yield
    reset_registry()
    reset_importer_registry()
    reset_exporter_registry()
    reset_agent_registry()


def test_disabled_in_memory_mode(monkeypatch) -> None:
    monkeypatch.setenv("OMNIWATCH_IDENTITY_STORE", "memory")

    def _boom():
        raise AssertionError("no client in memory mode")

    monkeypatch.setattr(persistence_module, "_connect", _boom)
    counts = persistence_module.reload_all()
    assert counts["mode"].startswith("disabled")

    from identity.workspaces import WorkspaceRecord

    assert persistence_module.mirror_workspace(
        WorkspaceRecord(workspace_id="w", user_id="u", name="n",
                        slug="s")) == "disabled"


def test_mirror_writes_expected_tables(monkeypatch) -> None:
    from identity.exporters import ExporterRecord
    from identity.importers import ImporterRecord
    from identity.workspaces import WorkspaceRecord

    fake = FakeCH({})
    _ch_env(monkeypatch, fake)
    now = datetime.now(timezone.utc)
    assert persistence_module.mirror_workspace(
        WorkspaceRecord(workspace_id="w", user_id="u", name="n",
                        slug="s", created_at=now)) == "ok"
    assert persistence_module.mirror_importer(
        ImporterRecord(workspace_id="w", user_id="u", slug="s",
                       endpoint_url="e", token_hash="h",
                       created_at=now)) == "ok"
    assert persistence_module.mirror_exporter(
        ExporterRecord(workspace_id="w", user_id="u", number=1,
                       name="e1", created_at=now)) == "ok"
    tables = [t for t, _, _ in fake.inserts]
    assert tables == ["workspaces", "workspace_importers",
                      "workspace_exporters"]
    assert "token_hash" in fake.inserts[1][2]
    assert not any("api_token" in str(columns)
                   for _, _, columns in fake.inserts), \
        "raw tokens must never reach ClickHouse"


def test_reload_latest_wins_and_deleted_skipped(monkeypatch) -> None:
    old = datetime(2026, 1, 1, tzinfo=timezone.utc)
    new = datetime(2026, 2, 1, tzinfo=timezone.utc)
    written_old = datetime(2026, 3, 1, tzinfo=timezone.utc)
    written_new = datetime(2026, 4, 1, tzinfo=timezone.utc)
    # Tie on created_at (rename/tombstone reuse it): mirrored_at decides.
    tied = datetime(2026, 5, 1, tzinfo=timezone.utc)
    fake = FakeCH({
        "workspaces": [
            ("w1", "u", "Old", "old", "", "unknown", [], "<100", 30,
             old, 0, written_old),
            ("w1", "u", "New", "old", "", "unknown", [], "<100", 30,
             new, 0, written_new),
            ("w2", "u", "Gone", "gone", "", "unknown", [], "<100", 30,
             new, 1, written_new),
            # Same created_at twice: tombstone (newer write) must win.
            ("w3", "u", "Live", "w3", "", "unknown", [], "<100", 30,
             tied, 0, written_old),
            ("w3", "u", "Live", "w3", "", "unknown", [], "<100", 30,
             tied, 1, written_new),
        ],
        "workspace_importers": [
            ("w1", "u", "old", "http://x/ingest", "hash-old", old,
             written_old),
            ("w1", "u", "old", "http://x/ingest", "hash-new", new,
             written_new),
        ],
        "workspace_exporters": [
            ("w1", "u", 1, "e1", old, 0, written_old),
            ("w1", "u", 1, "e1", new, 1, written_new),
            ("w1", "u", 2, "e2", new, 0, written_new),
        ],
        "agent_bindings": [
            ("b1", "u", "w1", "old", "http://a", "h", "connected",
             new, 0, written_new),
            ("b2", "u", "w1", "old", "http://b", "h", "connected",
             new, 1, written_new),
        ],
    })
    _ch_env(monkeypatch, fake)
    counts = persistence_module.reload_all()
    assert counts == {"workspaces": 1, "importers": 1, "exporters": 1,
                      "agents": 1}

    mine = get_registry().list_mine("u")
    assert [(w.workspace_id, w.name) for w in mine] == [("w1", "New")]
    imp = get_importer_registry().get_owned("u", "w1")
    assert imp is not None and imp.token_hash == "hash-new"
    exp = get_exporter_registry().list_owned("u", "w1")
    assert [(e.number, e.name) for e in exp] == [(2, "e2")]
    agents = get_agent_registry().list_mine("u")
    assert [a.binding_id for a in agents] == ["b1"]


def test_reload_degraded_never_raises(monkeypatch) -> None:
    monkeypatch.setenv("OMNIWATCH_IDENTITY_STORE", "clickhouse")

    def _down():
        raise ConnectionError("clickhouse down")

    monkeypatch.setattr(persistence_module, "_connect", _down)
    counts = persistence_module.reload_all()
    assert "degraded" in counts

    from identity.workspaces import WorkspaceRecord

    status = persistence_module.mirror_workspace(
        WorkspaceRecord(workspace_id="w", user_id="u", name="n", slug="s"))
    assert status.startswith("degraded")
