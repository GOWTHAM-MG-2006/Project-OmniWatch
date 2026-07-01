"""
OmniWatch — Unified Storage Layer
Component: Neo4j Topology Loader
Phase: 3
Purpose: Loads service topology from topology.json into Neo4j as a property graph
Inputs: simulation/topology.json (10 services, 9 relationships)
Outputs: Neo4j graph with :Service, :Database, :Infrastructure nodes and relationships
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
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

TOPOLOGY_PATH = Path(__file__).resolve().parent.parent.parent / "simulation" / "topology.json"

# Node type to Neo4j label mapping
NODE_TYPE_MAP = {
    "API_NODE": "Service",
    "AUTH_NODE": "Service",
    "LOADBALANCER_NODE": "Service",
    "WORKER_NODE": "Service",
    "DATABASE_NODE": "Database",
    "CACHE_NODE": "Infrastructure",
    "STORAGE_NODE": "Infrastructure",
    "UNKNOWN_NODE": "Service",
}


class Neo4jTopologyManager:
    """
    Loads and manages service topology in Neo4j.

    Usage:
        manager = Neo4jTopologyManager()
        manager.load_topology("simulation/topology.json")
        deps = manager.get_dependencies("api-gateway")
    """

    def __init__(self):
        """Initialize with Neo4j client."""
        from client import Neo4jClient
        self._client = Neo4jClient()

    def is_connected(self) -> bool:
        """Check if connected to Neo4j."""
        return self._client.is_connected()

    def load_topology(self, topology_path: str = None):
        """
        Load topology from JSON file into Neo4j.

        Args:
            topology_path: Path to topology.json
        """
        if not self._client.is_connected():
            print("[topology_loader] Not connected to Neo4j")
            return False

        if topology_path is None:
            topology_path = TOPOLOGY_PATH

        try:
            with open(topology_path) as f:
                topology = json.load(f)

            # Clear existing graph
            self._client.clear_graph()

            # Create nodes
            services = topology.get("services", [])
            for svc in services:
                self._create_node(svc)

            # Create relationships
            relationships = topology.get("relationships", [])
            for rel in relationships:
                self._create_relationship(rel)

            node_count = self._client.get_node_count()
            rel_count = self._client.get_relationship_count()
            print(f"[topology_loader] Loaded {node_count} nodes and {rel_count} relationships")
            return True

        except Exception as e:
            print(f"[topology_loader] Failed to load topology: {e}")
            return False

    def _create_node(self, service: dict):
        """Create a single node in Neo4j."""
        node_type = service.get("type", "UNKNOWN_NODE")
        label = NODE_TYPE_MAP.get(node_type, "Service")

        cypher = f"""
        CREATE (n:{label} {{
            id: $id,
            name: $name,
            type: $type,
            cloud_provider: $cloud_provider,
            criticality: $criticality,
            sla_tier: $sla_tier,
            status: 'healthy',
            anomaly_score: 0.0,
            last_seen: $timestamp
        }})
        """

        self._client.execute(cypher, {
            "id": service["id"],
            "name": service["name"],
            "type": node_type,
            "cloud_provider": service.get("cloud_provider", "unknown"),
            "criticality": service.get("criticality", "low"),
            "sla_tier": service.get("sla_tier", "bronze"),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def _create_relationship(self, rel: dict):
        """Create a single relationship in Neo4j."""
        rel_type = rel.get("type", "DEPENDS_ON")

        cypher = f"""
        MATCH (a {{id: $from_id}})
        MATCH (b {{id: $to_id}})
        CREATE (a)-[r:{rel_type} {{
            created_at: $timestamp
        }}]->(b)
        """

        self._client.execute(cypher, {
            "from_id": rel["from"],
            "to_id": rel["to"],
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def update_node_health(self, entity_id: str, status: str, anomaly_score: float):
        """
        Update health status and anomaly score on a node.

        Args:
            entity_id: Service/entity ID
            status: "healthy", "degraded", or "down"
            anomaly_score: Float 0.0 to 1.0
        """
        cypher = """
        MATCH (n {id: $entity_id})
        SET n.status = $status,
            n.anomaly_score = $anomaly_score,
            n.last_seen = $timestamp
        """

        self._client.execute(cypher, {
            "entity_id": entity_id,
            "status": status,
            "anomaly_score": anomaly_score,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def get_dependencies(self, entity_id: str) -> dict:
        """
        Get upstream and downstream dependencies for an entity.

        Args:
            entity_id: Service/entity ID

        Returns:
            Dict with 'upstream' and 'downstream' lists
        """
        # Upstream: nodes that call this entity
        upstream_cypher = """
        MATCH (a)-[r]->(b {id: $entity_id})
        RETURN a.id as id, a.name as name, a.type as type, type(r) as rel_type
        """
        upstream = self._client.query(upstream_cypher, {"entity_id": entity_id})

        # Downstream: nodes this entity calls
        downstream_cypher = """
        MATCH (a {id: $entity_id})-[r]->(b)
        RETURN b.id as id, b.name as name, b.type as type, type(r) as rel_type
        """
        downstream = self._client.query(downstream_cypher, {"entity_id": entity_id})

        return {
            "entity_id": entity_id,
            "upstream": upstream,
            "downstream": downstream,
        }

    def get_all_nodes(self) -> list:
        """Get all nodes in the graph."""
        return self._client.query("MATCH (n) RETURN n.id, n.name, n.type, n.status, n.anomaly_score")

    def get_all_relationships(self) -> list:
        """Get all relationships in the graph."""
        return self._client.query("MATCH (a)-[r]->(b) RETURN a.id as from_id, b.id as to_id, type(r) as rel_type")

    def close(self):
        """Close the Neo4j connection."""
        self._client.close()


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch Neo4j Topology Loader")
    subparsers = parser.add_subparsers(dest="command")

    # load command
    load_parser = subparsers.add_parser("load", help="Load topology into Neo4j")
    load_parser.add_argument("--file", default=None, help="Path to topology.json")

    # deps command
    deps_parser = subparsers.add_parser("deps", help="Get dependencies")
    deps_parser.add_argument("--entity", required=True, help="Entity ID")

    # status command
    subparsers.add_parser("status", help="Check graph status")

    args = parser.parse_args()
    manager = Neo4jTopologyManager()

    if args.command == "load":
        manager.load_topology(args.file)

    elif args.command == "deps":
        deps = manager.get_dependencies(args.entity)
        print(f"Dependencies for {args.entity}:")
        print(f"  Upstream:   {[d['id'] for d in deps['upstream']]}")
        print(f"  Downstream: {[d['id'] for d in deps['downstream']]}")

    elif args.command == "status":
        print(f"Connected: {manager.is_connected()}")
        if manager.is_connected():
            print(f"  Nodes: {manager._client.get_node_count()}")
            print(f"  Relationships: {manager._client.get_relationship_count()}")

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
