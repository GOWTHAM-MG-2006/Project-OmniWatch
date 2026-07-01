"""
OmniWatch — Cloud Abstraction Layer
Component: Resource Normalizer
Phase: 3
Purpose: Maps cloud-specific names to unified entity types for cross-cloud analytics
Inputs: Raw telemetry events with cloud-specific service names
Outputs: Enriched events with unified entity_id, entity_type, cloud_provider
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

# ---------------------------------------------------------------------------
# Default entity mappings (fallback if YAML not loaded)
# ---------------------------------------------------------------------------
DEFAULT_MAPPINGS = {
    "api-gateway": {"entity_type": "API_NODE", "cloud_provider": "simulated-aws", "criticality": "critical", "sla_tier": "gold"},
    "auth-service": {"entity_type": "AUTH_NODE", "cloud_provider": "simulated-azure", "criticality": "critical", "sla_tier": "gold"},
    "product-service": {"entity_type": "API_NODE", "cloud_provider": "simulated-aws", "criticality": "high", "sla_tier": "silver"},
    "inventory-service": {"entity_type": "API_NODE", "cloud_provider": "simulated-aws", "criticality": "high", "sla_tier": "silver"},
    "load-balancer": {"entity_type": "LOADBALANCER_NODE", "cloud_provider": "simulated-aws", "criticality": "critical", "sla_tier": "gold"},
    "background-worker": {"entity_type": "WORKER_NODE", "cloud_provider": "simulated-gcp", "criticality": "medium", "sla_tier": "bronze"},
    "postgresql-database": {"entity_type": "DATABASE_NODE", "cloud_provider": "simulated-aws", "criticality": "critical", "sla_tier": "gold"},
    "redis-cache": {"entity_type": "CACHE_NODE", "cloud_provider": "simulated-aws", "criticality": "high", "sla_tier": "silver"},
    "user-database": {"entity_type": "DATABASE_NODE", "cloud_provider": "simulated-azure", "criticality": "critical", "sla_tier": "gold"},
    "minio-storage": {"entity_type": "STORAGE_NODE", "cloud_provider": "simulated-gcp", "criticality": "medium", "sla_tier": "bronze"},
}

DEFAULTS = {"entity_type": "UNKNOWN_NODE", "cloud_provider": "unknown", "criticality": "low", "sla_tier": "bronze"}


class ResourceNormalizer:
    """
    Maps cloud-specific service names to unified entity types.

    Usage:
        normalizer = ResourceNormalizer()
        enriched = normalizer.normalize(raw_event)
    """

    def __init__(self, mappings_path: str = None):
        """
        Initialize the normalizer with entity mappings.

        Args:
            mappings_path: Path to entity_mappings.yaml (optional)
        """
        self._mappings = dict(DEFAULT_MAPPINGS)
        self._defaults = dict(DEFAULTS)

        if mappings_path is None:
            mappings_path = Path(__file__).parent / "mappings" / "entity_mappings.yaml"

        if Path(mappings_path).exists():
            self._load_yaml(mappings_path)

    def _load_yaml(self, path: str):
        """Load mappings from YAML config file."""
        try:
            with open(path) as f:
                config = yaml.safe_load(f)

            if config and "service_mappings" in config:
                for name, attrs in config["service_mappings"].items():
                    self._mappings[name] = attrs

            if config and "defaults" in config:
                self._defaults.update(config["defaults"])

        except Exception as e:
            print(f"[resource_normalizer] WARNING: Failed to load YAML: {e}")

    def normalize(self, event: dict) -> dict:
        """
        Normalize a raw telemetry event to the unified model.

        Args:
            event: Raw telemetry event dict with at least 'entity_id' or 'service'

        Returns:
            Enriched event dict with unified fields
        """
        # Extract the original service name
        original_name = (
            event.get("entity_id")
            or event.get("service")
            or event.get("service_name")
            or "unknown"
        )

        # Look up mapping
        mapping = self._mappings.get(original_name, self._defaults)

        # Build enriched event
        enriched = {
            "entity_id": original_name,
            "entity_type": mapping.get("entity_type", self._defaults["entity_type"]),
            "normalized_name": original_name.replace("-", "_"),
            "cloud_provider": mapping.get("cloud_provider", self._defaults["cloud_provider"]),
            "original_name": original_name,
            "criticality": mapping.get("criticality", self._defaults["criticality"]),
            "sla_tier": mapping.get("sla_tier", self._defaults["sla_tier"]),
            "timestamp": event.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "source": event.get("source", "unknown"),
            "metadata": {
                k: v for k, v in event.items()
                if k not in ("entity_id", "service", "service_name", "timestamp", "source")
            },
        }

        return enriched

    def normalize_batch(self, events: list) -> list:
        """Normalize a batch of events."""
        return [self.normalize(e) for e in events]

    def get_entity_type(self, name: str) -> str:
        """Get the unified entity type for a service name."""
        return self._mappings.get(name, self._defaults)["entity_type"]

    def get_cloud_provider(self, name: str) -> str:
        """Get the cloud provider for a service name."""
        return self._mappings.get(name, self._defaults)["cloud_provider"]

    def list_entities(self) -> list:
        """Return all known entity mappings."""
        return [
            {"name": name, **attrs}
            for name, attrs in self._mappings.items()
        ]


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch Resource Normalizer")
    subparsers = parser.add_subparsers(dest="command")

    # normalize command
    norm_parser = subparsers.add_parser("normalize", help="Normalize an event")
    norm_parser.add_argument("--entity", required=True, help="Entity/service name")
    norm_parser.add_argument("--source", default="test", help="Source identifier")

    # list command
    subparsers.add_parser("list", help="List all entity mappings")

    args = parser.parse_args()
    normalizer = ResourceNormalizer()

    if args.command == "normalize":
        event = {"entity_id": args.entity, "source": args.source}
        result = normalizer.normalize(event)
        print(json.dumps(result, indent=2))

    elif args.command == "list":
        entities = normalizer.list_entities()
        print(f"Known entities ({len(entities)}):")
        for e in entities:
            print(f"  {e['name']:25s} → {e['entity_type']:20s} ({e['cloud_provider']})")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
