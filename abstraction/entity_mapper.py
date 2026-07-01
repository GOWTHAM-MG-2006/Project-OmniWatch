"""
OmniWatch — Cloud Abstraction Layer
Component: Entity Mapper
Phase: 3
Purpose: Enriches entities with business metadata from topology and config
Inputs: Normalized events from ResourceNormalizer
Outputs: Fully enriched events with business context (criticality, SLA, dependencies)
"""

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from dotenv import load_dotenv

# ---------------------------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------------------------
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

TOPOLOGY_PATH = Path(__file__).resolve().parent.parent / "simulation" / "topology.json"


class EntityMapper:
    """
    Enriches normalized events with business metadata from topology.

    Usage:
        mapper = EntityMapper()
        enriched = mapper.map(normalized_event)
    """

    def __init__(self, topology_path: str = None):
        """
        Initialize with topology data.

        Args:
            topology_path: Path to topology.json (optional)
        """
        self._topology = {}
        self._service_meta = {}
        self._relationships = []

        if topology_path is None:
            topology_path = TOPOLOGY_PATH

        if Path(topology_path).exists():
            self._load_topology(topology_path)

    def _load_topology(self, path: str):
        """Load topology from JSON file."""
        try:
            with open(path) as f:
                self._topology = json.load(f)

            for svc in self._topology.get("services", []):
                self._service_meta[svc["id"]] = svc

            self._relationships = self._topology.get("relationships", [])

        except Exception as e:
            print(f"[entity_mapper] WARNING: Failed to load topology: {e}")

    def map(self, event: dict) -> dict:
        """
        Enrich a normalized event with business metadata.

        Args:
            event: Normalized event from ResourceNormalizer

        Returns:
            Fully enriched event with business context
        """
        entity_id = event.get("entity_id", "unknown")
        meta = self._service_meta.get(entity_id, {})

        # Get dependencies
        upstream = [
            r["from"] for r in self._relationships if r["to"] == entity_id
        ]
        downstream = [
            r["to"] for r in self._relationships if r["from"] == entity_id
        ]

        # Merge metadata
        enriched = dict(event)
        enriched["business_metadata"] = {
            "display_name": meta.get("name", entity_id),
            "criticality": meta.get("criticality", event.get("criticality", "low")),
            "sla_tier": meta.get("sla_tier", event.get("sla_tier", "bronze")),
            "cloud_provider": meta.get("cloud_provider", event.get("cloud_provider", "unknown")),
            "upstream_dependencies": upstream,
            "downstream_dependencies": downstream,
            "dependency_count": len(upstream) + len(downstream),
        }

        return enriched

    def map_batch(self, events: list) -> list:
        """Map a batch of events."""
        return [self.map(e) for e in events]

    def get_service_info(self, entity_id: str) -> dict:
        """Get full service info from topology."""
        return self._service_meta.get(entity_id, {})

    def get_all_services(self) -> list:
        """Return all services from topology."""
        return list(self._service_meta.values())

    def get_dependencies(self, entity_id: str) -> dict:
        """Get upstream and downstream dependencies for an entity."""
        upstream = [r["from"] for r in self._relationships if r["to"] == entity_id]
        downstream = [r["to"] for r in self._relationships if r["from"] == entity_id]
        return {"upstream": upstream, "downstream": downstream}


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch Entity Mapper")
    subparsers = parser.add_subparsers(dest="command")

    # map command
    map_parser = subparsers.add_parser("map", help="Map an entity")
    map_parser.add_argument("--entity", required=True, help="Entity ID")

    # list command
    subparsers.add_parser("list", help="List all entities")

    # deps command
    deps_parser = subparsers.add_parser("deps", help="Get dependencies")
    deps_parser.add_argument("--entity", required=True, help="Entity ID")

    args = parser.parse_args()
    mapper = EntityMapper()

    if args.command == "map":
        info = mapper.get_service_info(args.entity)
        if info:
            print(json.dumps(info, indent=2))
        else:
            print(f"Entity '{args.entity}' not found in topology")

    elif args.command == "list":
        services = mapper.get_all_services()
        print(f"Services in topology ({len(services)}):")
        for s in services:
            print(f"  {s['id']:25s} {s['type']:20s} {s['cloud_provider']}")

    elif args.command == "deps":
        deps = mapper.get_dependencies(args.entity)
        print(f"Dependencies for {args.entity}:")
        print(f"  Upstream:   {deps['upstream'] or 'none'}")
        print(f"  Downstream: {deps['downstream'] or 'none'}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
