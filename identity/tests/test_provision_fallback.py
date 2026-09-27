"""OmniWatch provision fallback tests — table clone without storage/.

The identity image ships without storage/, so _provision_clickhouse must
clone the base DB's tables (not crash) when the 001 migration module is
unimportable. Regression guard: an empty workspace DB is what surfaced as
importer `503 storage unavailable` on live workspaces.
"""

from __future__ import annotations

import sys
import types

from identity.provision import _provision_clickhouse, naming_helpers
from identity.workspaces import WorkspaceRecord


class FakeResult:
    def __init__(self, rows: list) -> None:
        self.result_rows = rows


class FakeCH:
    def __init__(self) -> None:
        self.commands: list[str] = []

    def query(self, sql: str, **kwargs) -> FakeResult:
        assert "SHOW TABLES FROM" in sql, sql
        return FakeResult([("metrics",), ("logs",), ("traces",)])

    def command(self, sql: str, **kwargs) -> None:
        self.commands.append(sql)

    def close(self) -> None:
        pass


def test_clone_fallback_without_storage_module(monkeypatch) -> None:
    fake_module = types.ModuleType("clickhouse_connect")
    fake = FakeCH()
    fake_module.get_client = lambda **kwargs: fake
    monkeypatch.setitem(sys.modules, "clickhouse_connect", fake_module)
    # storage/ absent in the identity image -> ImportError path.
    monkeypatch.setitem(
        sys.modules,
        "storage.clickhouse.migrations.001_initial_schema", None,
    )
    record = WorkspaceRecord(
        workspace_id="ws-1", user_id="u", name="Fallback",
        slug="fallback-ws",
    )
    assert _provision_clickhouse(record, naming_helpers()) == "ok"
    assert fake.commands[0].startswith("CREATE DATABASE")
    clones = [c for c in fake.commands if c.startswith("CREATE TABLE")]
    assert len(clones) == 3, clones
    assert all("`omniwatch_ws_fallback-ws`." in c for c in clones)
    assert all("AS `omniwatch`." in c for c in clones)
