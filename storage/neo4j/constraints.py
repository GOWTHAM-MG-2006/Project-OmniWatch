"""
OmniWatch — Unified Storage Layer
Component: Neo4j Constraints
Phase: 5
Purpose: Idempotent bootstrap of Neo4j schema constraints and indexes for the
         four topology node labels (:Service, :Database, :Infrastructure,
         :K8sResource). Creates a unique constraint on `id` and a composite
         index on (entity_id, name, type) per label so topology_loader and
         the causal engine can upsert and traverse nodes efficiently.
Inputs: A connected neo4j driver (bolt://localhost:7687, neo4j/omniwatch by
        default via storage.config.StorageConfig)
Outputs: Neo4j schema objects (constraints + indexes) created idempotently;
         nothing is returned. Logs each applied statement as structured JSON.
"""

from __future__ import annotations

from typing import List, Sequence

from storage.common import StorageError, create_logger, retry_with_backoff

# The four node labels the Phase 5 topology graph supports. Neo4j constraint
# and index names are derived per label as {label_lower}_id_unique /
# {label_lower}_entity_name_type_index — stable names make re-runs a no-op.
NODE_LABELS: Sequence[str] = ("Service", "Database", "Infrastructure", "K8sResource")


def _constraint_statements(labels: Sequence[str]) -> List[str]:
    """Build the full list of idempotent DDL statements.

    Neo4j 5.x syntax only: ``CREATE CONSTRAINT ... IF NOT EXISTS`` /
    ``CREATE INDEX ... IF NOT EXISTS`` (the legacy ``CREATE INDEX ON`` form
    was removed in Neo4j 5). One unique constraint on ``id`` plus one
    composite index on (entity_id, name, type) per label — a single composite
    index covers all three lookup patterns (entity_id first). Running the
    statements twice is a no-op thanks to the IF NOT EXISTS guards.
    """
    statements: List[str] = []
    for label in labels:
        prefix = label.lower()
        statements.append(
            f"CREATE CONSTRAINT {prefix}_id_unique IF NOT EXISTS "
            f"FOR (n:{label}) REQUIRE n.id IS UNIQUE"
        )
        statements.append(
            f"CREATE INDEX {prefix}_entity_name_type IF NOT EXISTS "
            f"FOR (n:{label}) ON (n.entity_id, n.name, n.type)"
        )
    return statements


def _execute_statements(driver, statements: Sequence[str]) -> None:
    """Run every DDL statement in a single session; errors abort the batch."""
    with driver.session() as session:
        for statement in statements:
            session.run(statement).consume()


def apply_constraints(driver) -> None:
    """Idempotently create the Neo4j constraints and indexes.

    Wraps statement execution in :func:`storage.common.retry_with_backoff`
    (3x, 100ms -> 500ms -> 2s) so a transient connection drop during schema
    bootstrap is retried before raising :class:`storage.common.StorageError`.

    Args:
        driver: An open ``neo4j.GraphDatabase.driver`` instance (created by
                the caller — storage/neo4j/client.py builds one for this
                module and for topology_loader).
    """
    logger = create_logger("omniwatch.storage.neo4j.constraints")
    statements = _constraint_statements(NODE_LABELS)
    try:
        retry_with_backoff(
            _execute_statements,
            logger=logger,
            driver=driver,
            statements=statements,
        )
    except Exception as exc:  # noqa: BLE001 - re-wrap as StorageError
        raise StorageError(f"Failed to apply Neo4j constraints/indexes: {exc}") from exc
    logger.info("applied %d neo4j constraint/index statements", len(statements))


def main() -> None:
    """CLI entry point: build a driver from env config and apply constraints.

    Usable as ``python -m storage.neo4j.constraints`` — requires Neo4j to be
    up (``docker-compose up -d``) and defaults to bolt://localhost:7687.
    """
    from neo4j import GraphDatabase  # local import: driver is a runtime dep

    from storage.config import StorageConfig

    cfg = StorageConfig.from_env()
    logger = create_logger("omniwatch.storage.neo4j.constraints")
    driver = GraphDatabase.driver(
        cfg.neo4j_uri,
        auth=(cfg.neo4j_user, cfg.neo4j_password),
    )
    try:
        driver.verify_connectivity()
        apply_constraints(driver)
        logger.info("neo4j constraints applied successfully")
    finally:
        driver.close()


if __name__ == "__main__":
    main()
