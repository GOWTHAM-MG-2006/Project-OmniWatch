"""
OmniWatch — Causal Graph Engine
Component: Cross-Cloud Entity Mapper
Phase: 7
Purpose: Normalize provider-qualified entity identifiers into canonical
         OmniWatch ids (``{provider}:{region}:{entity_type}:{name}``) so the
         causal graph can correlate the same logical entity across AWS / Azure
         / GCP / K8s.  On id conflicts the first-seen mapping wins and a
         warning is logged (plan Decision 13).
Inputs: Raw entity dicts (id, name, entity_type, cloud_provider, region) and
        canonical-id template from causal_rules.yaml (cross_cloud_mapper).
Outputs: Canonical entity ids + a persistent first-seen mapping registry.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

from storage.common import create_logger

_LOG: logging.Logger = create_logger("omniwatch.causal.cross_cloud_mapper")

_DEFAULT_RULES_PATH = Path(__file__).resolve().parent / "config" / "causal_rules.yaml"
_DEFAULT_PROVIDER = "gcp"
_DEFAULT_TEMPLATE = "{provider}:{region}:{entity_type}:{name}"
_UNKNOWN = "unknown"


class CrossCloudMapper:
    """Canonical id resolution with first-seen-wins conflict handling.

    ``to_canonical`` normalizes any entity reference into the canonical
    ``{provider}:{region}:{entity_type}:{name}`` form.  The mapping registry
    remembers the first canonical id produced for each raw id; a later
    conflict logs a warning and returns the previously seen canonical id, so
    downstream consumers (two_layer_graph, dag_traversal) always address a
    given raw id by one stable name.
    """

    def __init__(self, default_provider: str = _DEFAULT_PROVIDER, id_template: str = _DEFAULT_TEMPLATE) -> None:
        self._default_provider = default_provider
        self._template = id_template
        self._registry: dict[str, str] = {}

    @classmethod
    def from_config(cls, rules_path: Path | None = None) -> CrossCloudMapper:
        """Build a mapper from the ``cross_cloud_mapper`` rules section."""
        path = rules_path or _DEFAULT_RULES_PATH
        try:
            with open(path, "r", encoding="utf-8") as fh:
                rules: dict[str, Any] = yaml.safe_load(fh) or {}
        except OSError:
            _LOG.warning("causal_rules.yaml not found; using default cross-cloud mapping")
            return cls()
        section = rules.get("cross_cloud_mapper", {}) or {}
        return cls(
            default_provider=str(section.get("default_provider", _DEFAULT_PROVIDER)),
            id_template=str(
                section.get("canonical_id_template", _DEFAULT_TEMPLATE)
            ),
        )

    # ------------------------------------------------------------------ #
    # Canonical id resolution
    # ------------------------------------------------------------------ #
    def canonical_id(
        self,
        name: str,
        *,
        entity_type: str = _UNKNOWN,
        cloud_provider: str | None = None,
        region: str | None = None,
        raw_id: str | None = None,
    ) -> str:
        """Compute the canonical id for an entity.

        ``name`` is the bare entity name (e.g. ``postgresql-database``);
        provider/region default to ``gcp``/``unknown`` when absent.  When
        ``raw_id`` is supplied it is remembered in the first-seen registry so
        later calls with the same raw id return the same canonical id.
        """
        provider = (cloud_provider or self._default_provider).strip().lower()
        region = (region or _UNKNOWN).strip().lower()
        etype = (entity_type or _UNKNOWN).strip().upper()
        entity_name = (name or _UNKNOWN).strip().lower()
        canonical = self._template.format(
            provider=provider,
            region=region,
            entity_type=etype,
            name=entity_name,
        )
        if raw_id:
            previous = self._registry.get(raw_id)
            if previous is not None and previous != canonical:
                _LOG.warning(
                    "canonical_id_conflict raw_id=%s first=%s now=%s keeping first",
                    raw_id,
                    previous,
                    canonical,
                )
                return previous
            self._registry[raw_id] = canonical
        return canonical

    def to_canonical(self, entity: dict[str, Any]) -> str:
        """Convenience: canonical id from a raw entity dict.

        Reads ``name`` (or ``id`` when name is absent), ``entity_type``,
        ``cloud_provider`` and ``region`` from the dict.
        """
        name = str(entity.get("name") or entity.get("id") or "")
        return self.canonical_id(
            name,
            entity_type=str(entity.get("entity_type") or entity.get("type") or _UNKNOWN),
            cloud_provider=entity.get("cloud_provider"),
            region=entity.get("region"),
            raw_id=str(entity.get("id") or "") or None,
        )

    def resolve(self, raw_id: str) -> str | None:
        """Return the canonical id previously mapped for a raw id (if any)."""
        return self._registry.get(raw_id)

    # ------------------------------------------------------------------ #
    # Registry utilities
    # ------------------------------------------------------------------ #
    def registry_size(self) -> int:
        return len(self._registry)

    def to_dict(self) -> dict[str, Any]:
        """Serialize the mapping registry for diagnostics / E2E assertions."""
        return {
            "template": self._template,
            "default_provider": self._default_provider,
            "mappings": dict(self._registry),
        }
