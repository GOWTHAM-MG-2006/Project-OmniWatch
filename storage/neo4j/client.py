"""
OmniWatch — Unified Storage Layer
Component: Neo4j Client
Phase: 3
Purpose: Neo4j query client for graph operations and topology queries
Inputs: Cypher queries from causal engine and other layers
Outputs: Query results from Neo4j graph database
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

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "omniwatch")


class Neo4jClient:
    """
    Neo4j client for OmniWatch graph operations.

    Usage:
        client = Neo4jClient()
        result = client.query("MATCH (n) RETURN n LIMIT 10")
    """

    def __init__(self):
        """Initialize Neo4j connection."""
        self._driver = None
        self._connected = False
        self._connect()

    def _connect(self):
        """Establish connection to Neo4j."""
        try:
            from neo4j import GraphDatabase
            self._driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            # Test connection
            self._driver.verify_connectivity()
            self._connected = True
            print(f"[neo4j] Connected to {NEO4J_URI}")
        except ImportError:
            print("[neo4j] WARNING: neo4j driver not installed. Install with: pip install neo4j")
            self._connected = False
        except Exception as e:
            print(f"[neo4j] WARNING: Connection failed: {e}")
            self._connected = False

    def is_connected(self) -> bool:
        """Check if connected to Neo4j."""
        return self._connected

    def query(self, cypher: str, parameters: dict = None) -> list:
        """
        Execute a Cypher query and return results.

        Args:
            cypher: Cypher query string
            parameters: Optional query parameters

        Returns:
            List of dicts representing query results
        """
        if not self._connected:
            return []

        try:
            with self._driver.session() as session:
                result = session.run(cypher, parameters or {})
                return [dict(record) for record in result]
        except Exception as e:
            print(f"[neo4j] Query failed: {e}")
            return []

    def execute(self, cypher: str, parameters: dict = None) -> bool:
        """
        Execute a Cypher query without returning results.

        Args:
            cypher: Cypher query string
            parameters: Optional query parameters

        Returns:
            True if successful
        """
        if not self._connected:
            return False

        try:
            with self._driver.session() as session:
                session.run(cypher, parameters or {})
                return True
        except Exception as e:
            print(f"[neo4j] Execute failed: {e}")
            return False

    def get_node_count(self) -> int:
        """Get total number of nodes in the graph."""
        result = self.query("MATCH (n) RETURN count(n) as count")
        return result[0]["count"] if result else 0

    def get_relationship_count(self) -> int:
        """Get total number of relationships in the graph."""
        result = self.query("MATCH ()-[r]->() RETURN count(r) as count")
        return result[0]["count"] if result else 0

    def get_node_labels(self) -> list:
        """Get all node labels in the graph."""
        result = self.query("CALL db.labels() YIELD label RETURN label")
        return [r["label"] for r in result]

    def get_relationship_types(self) -> list:
        """Get all relationship types in the graph."""
        result = self.query("CALL db.relationshipTypes() YIELD relationshipType RETURN relationshipType")
        return [r["relationshipType"] for r in result]

    def clear_graph(self):
        """Delete all nodes and relationships (use with caution)."""
        self.execute("MATCH (n) DETACH DELETE n")
        print("[neo4j] Graph cleared")

    def close(self):
        """Close the Neo4j connection."""
        if self._driver:
            self._driver.close()
            self._connected = False


# ---------------------------------------------------------------------------
# CLI interface
# ---------------------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description="OmniWatch Neo4j Client")
    subparsers = parser.add_subparsers(dest="command")

    subparsers.add_parser("status", help="Check connection and graph stats")

    query_parser = subparsers.add_parser("query", help="Run a Cypher query")
    query_parser.add_argument("--cypher", required=True, help="Cypher query")

    args = parser.parse_args()
    client = Neo4jClient()

    if args.command == "status":
        print(f"Connected: {client.is_connected()}")
        if client.is_connected():
            print(f"  Nodes: {client.get_node_count()}")
            print(f"  Relationships: {client.get_relationship_count()}")
            print(f"  Labels: {client.get_node_labels()}")
            print(f"  Relationship Types: {client.get_relationship_types()}")

    elif args.command == "query":
        results = client.query(args.cypher)
        print(json.dumps(results, indent=2, default=str))

    else:
        parser.print_help()


if __name__ == "__main__":
    main()
