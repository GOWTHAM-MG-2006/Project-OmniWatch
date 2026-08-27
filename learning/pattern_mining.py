"""
OmniWatch — Continuous Learning Layer
Component: Pattern Mining Engine
Phase: 11
Purpose: Mine recurring incident patterns from ClickHouse incidents table
         (group by root_cause_entity + severity + time_bucket), create
         Neo4j :Pattern nodes connected to affected entities via :HAS_PATTERN.
Inputs: ClickHouse omniwatch.incidents table (incidents from prioritization/)
Outputs: Neo4j :Pattern nodes with properties {id, root_cause_entity, severity,
         pattern_count, first_seen, last_seen}; :HAS_PATTERN edges to entity nodes
"""

from __future__ import annotations

import hashlib
import logging
import os
import signal
import sys
import time
from typing import Any

import clickhouse_connect
from neo4j import Driver, GraphDatabase

logger = logging.getLogger("omniwatch.learning.pattern_mining")

# ClickHouse connection (env vars match feedback_loop.py pattern)
CLICKHOUSE_HOST = os.environ.get("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_DB = os.environ.get("CLICKHOUSE_DB", "omniwatch")
CLICKHOUSE_USER = os.environ.get("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.environ.get("CLICKHOUSE_PASSWORD", "")

# Neo4j connection
NEO4J_URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
NEO4J_USER = os.environ.get("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.environ.get("NEO4J_PASSWORD", "omniwatch")

# Mining parameters
DEFAULT_INTERVAL_SECONDS = int(os.environ.get("PATTERN_MINING_INTERVAL", "900"))
DEFAULT_LOOKBACK_HOURS = int(os.environ.get("PATTERN_MINING_LOOKBACK_HOURS", "24"))
DEFAULT_MIN_OCCURRENCES = int(os.environ.get("PATTERN_MINING_MIN_OCCURRENCES", "2"))

# SQL to query recurring incident patterns from ClickHouse.
# Groups by root_cause_entity + severity + hourly time bucket.
# Filters to incidents with >= min_occurrences within the lookback window.
_PATTERN_QUERY = """
SELECT
    root_cause_entity,
    severity,
    toStartOfHour(created_at) AS time_bucket,
    count() AS pattern_count,
    min(created_at) AS first_seen,
    max(created_at) AS last_seen
FROM {database}.incidents
WHERE created_at >= now() - INTERVAL {lookback_hours} HOUR
  AND root_cause_entity != ''
GROUP BY root_cause_entity, severity, time_bucket
HAVING pattern_count >= {min_occurrences}
ORDER BY pattern_count DESC
"""


class PatternMiner:
    """Mine recurring incident patterns and materialize them as Neo4j graph nodes.

    Lifecycle:
        1. ``__init__`` — lazy ClickHouse + Neo4j client setup.
        2. ``mine_patterns()`` — one-shot mining pass (query CH → create Neo4j nodes).
        3. ``start(interval)`` — periodic mining loop (blocks until stop signal).
        4. ``stop()`` — signal the loop to exit gracefully.
    """

    def __init__(
        self,
        clickhouse_config: dict[str, Any] | None = None,
        neo4j_config: dict[str, Any] | None = None,
        lookback_hours: int = DEFAULT_LOOKBACK_HOURS,
        min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
    ) -> None:
        self._running = False
        self._lookback_hours = lookback_hours
        self._min_occurrences = min_occurrences

        # ClickHouse client (lazy init).
        ch_overrides = clickhouse_config or {}
        self._ch_host = ch_overrides.get("host", CLICKHOUSE_HOST)
        self._ch_port = int(ch_overrides.get("port", CLICKHOUSE_PORT))
        self._ch_db = ch_overrides.get("database", CLICKHOUSE_DB)
        self._ch_user = ch_overrides.get("username", CLICKHOUSE_USER)
        self._ch_password = ch_overrides.get("password", CLICKHOUSE_PASSWORD)
        self._ch_client: Any = None

        # Neo4j driver (lazy init).
        neo_overrides = neo4j_config or {}
        self._neo4j_uri = neo_overrides.get("uri", NEO4J_URI)
        self._neo4j_user = neo_overrides.get("user", NEO4J_USER)
        self._neo4j_password = neo_overrides.get("password", NEO4J_PASSWORD)
        self._neo4j_driver: Driver | None = None

    # ------------------------------------------------------------------ #
    # Connection helpers
    # ------------------------------------------------------------------ #

    def _get_ch_client(self) -> Any:
        if self._ch_client is None:
            self._ch_client = clickhouse_connect.get_client(
                host=self._ch_host,
                port=self._ch_port,
                database=self._ch_db,
                username=self._ch_user,
                password=self._ch_password,
            )
        return self._ch_client

    def _get_neo4j_driver(self) -> Driver:
        if self._neo4j_driver is None:
            self._neo4j_driver = GraphDatabase.driver(
                self._neo4j_uri,
                auth=(self._neo4j_user, self._neo4j_password),
            )
        return self._neo4j_driver

    # ------------------------------------------------------------------ #
    # Pattern mining core
    # ------------------------------------------------------------------ #

    def mine_patterns(self) -> list[dict[str, Any]]:
        """Execute one mining pass: query ClickHouse, create Neo4j Pattern nodes.

        Returns a list of created pattern dicts for testability.
        """
        patterns = self._query_patterns()
        created: list[dict[str, Any]] = []
        for pattern in patterns:
            pattern_node = self._create_pattern_node(pattern)
            if pattern_node:
                created.append(pattern_node)
        logger.info(
            "mining_pass_complete queried=%d created=%d",
            len(patterns),
            len(created),
        )
        return created

    def _query_patterns(self) -> list[dict[str, Any]]:
        """Query ClickHouse for recurring incident patterns."""
        sql = _PATTERN_QUERY.format(
            database=self._ch_db,
            lookback_hours=self._lookback_hours,
            min_occurrences=self._min_occurrences,
        )
        try:
            client = self._get_ch_client()
            result = client.query(sql)
            patterns: list[dict[str, Any]] = []
            for row in result.result_rows:
                root_cause_entity = row[0]
                severity = row[1]
                time_bucket = row[2]
                pattern_count = row[3]
                first_seen = row[4]
                last_seen = row[5]
                patterns.append({
                    "root_cause_entity": root_cause_entity,
                    "severity": severity,
                    "time_bucket": str(time_bucket),
                    "pattern_count": int(pattern_count),
                    "first_seen": str(first_seen),
                    "last_seen": str(last_seen),
                })
            logger.info("pattern_query_complete matched=%d", len(patterns))
            return patterns
        except Exception as exc:  # noqa: BLE001
            logger.error("pattern query failed: %s", exc)
            return []

    def _generate_pattern_id(self, pattern: dict[str, Any]) -> str:
        """Generate a deterministic pattern ID from entity + severity + time_bucket."""
        key = f"{pattern['root_cause_entity']}|{pattern['severity']}|{pattern['time_bucket']}"
        return "pat-" + hashlib.sha256(key.encode()).hexdigest()[:12]

    def _create_pattern_node(self, pattern: dict[str, Any]) -> dict[str, Any] | None:
        """Create a Neo4j :Pattern node and link it to the entity via :HAS_PATTERN."""
        pattern_id = self._generate_pattern_id(pattern)
        entity_id = pattern["root_cause_entity"]
        driver = self._get_neo4j_driver()

        try:
            with driver.session() as session:
                # Upsert the :Pattern node
                session.run(
                    """
                    MERGE (p:Pattern {id: $pattern_id})
                    SET p.root_cause_entity = $root_cause_entity,
                        p.severity = $severity,
                        p.pattern_count = $pattern_count,
                        p.first_seen = $first_seen,
                        p.last_seen = $last_seen,
                        p.time_bucket = $time_bucket
                    """,
                    pattern_id=pattern_id,
                    root_cause_entity=pattern["root_cause_entity"],
                    severity=pattern["severity"],
                    pattern_count=pattern["pattern_count"],
                    first_seen=pattern["first_seen"],
                    last_seen=pattern["last_seen"],
                    time_bucket=pattern["time_bucket"],
                )

                # Link Pattern to entity node if it exists
                session.run(
                    """
                    MATCH (e {id: $entity_id})
                    MATCH (p:Pattern {id: $pattern_id})
                    MERGE (e)-[:HAS_PATTERN]->(p)
                    """,
                    entity_id=entity_id,
                    pattern_id=pattern_id,
                )

            logger.info(
                "pattern_node_created pattern_id=%s entity=%s severity=%s count=%d",
                pattern_id,
                entity_id,
                pattern["severity"],
                pattern["pattern_count"],
            )
            return {
                "pattern_id": pattern_id,
                "root_cause_entity": entity_id,
                "severity": pattern["severity"],
                "pattern_count": pattern["pattern_count"],
                "first_seen": pattern["first_seen"],
                "last_seen": pattern["last_seen"],
            }
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "pattern_node_create_failed pattern_id=%s entity=%s: %s",
                pattern_id,
                entity_id,
                exc,
            )
            return None

    # ------------------------------------------------------------------ #
    # Periodic loop
    # ------------------------------------------------------------------ #

    def start(self, interval: float = DEFAULT_INTERVAL_SECONDS) -> None:
        """Block and run mining passes at ``interval`` seconds until ``stop()``.

        Parameters
        ----------
        interval:
            Seconds between mining passes (default 900 = 15 minutes).
        """
        self._running = True
        logger.info("pattern miner started, interval=%.0fs", interval)

        try:
            while self._running:
                self.mine_patterns()
                # Sleep in small increments so stop() is responsive
                for _ in range(int(interval)):
                    if not self._running:
                        break
                    time.sleep(1.0)
        except Exception as exc:  # noqa: BLE001
            logger.error("pattern miner loop error: %s", exc)
        finally:
            self.stop()
            logger.info("pattern miner stopped")

    def stop(self) -> None:
        """Signal the mining loop to exit and close connections."""
        self._running = False
        if self._neo4j_driver is not None:
            try:
                self._neo4j_driver.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self._neo4j_driver = None
        if self._ch_client is not None:
            try:
                self._ch_client.close()
            except Exception:  # noqa: BLE001, S110
                pass
            self._ch_client = None


def main() -> None:
    """Entry point for ``python -m learning.pattern_mining``."""
    miner = PatternMiner()

    def _handle_signal(signum: int, _frame: Any) -> None:
        logger.info("received signal %s, stopping", signum)
        miner.stop()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    miner.start()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
        stream=sys.stdout,
    )
    main()
