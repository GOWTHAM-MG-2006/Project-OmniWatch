"""OmniWatch — ClickHouse migrations package (Phase 5).

Contains idempotent DDL migrations. Each migration is runnable as a module:

    python -m storage.clickhouse.migrations.001_initial_schema

Migrations must be safe to run repeatedly (schema.sql uses CREATE
DATABASE/TABLE IF NOT EXISTS, so double execution never errors).

Digit-leading migration filenames (001_initial_schema.py) are not valid
Python identifiers, so a plain ``from ... import 001_initial_schema`` is a
syntax error. Each migration is therefore also aliased via a lazy PEP 562
module ``__getattr__`` (e.g. ``_001_initial_schema``) so it can be imported
programmatically: ``from storage.clickhouse.migrations import _001_initial_schema``.

The alias is lazy (not imported at package-import time) so that
``python -m storage.clickhouse.migrations.001_initial_schema`` executes the
migration cleanly without a runpy sys.modules warning.
"""

import importlib as _importlib


def __getattr__(name: str):
    if name == "_001_initial_schema":
        return _importlib.import_module(f"{__name__}.001_initial_schema")
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
