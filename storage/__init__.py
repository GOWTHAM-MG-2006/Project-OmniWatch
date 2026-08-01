"""
OmniWatch — Unified Storage Layer
Component: Storage Package
Phase: 5
Purpose: Unified persistence layer for ClickHouse (metrics), Neo4j (graph),
         and MinIO (objects); re-exports shared logging/retry helpers.
Inputs: None (package bootstrap — clients are filled in by later plan tasks)
Outputs: Importable helpers: create_logger, StorageError, retry_with_backoff
"""

from __future__ import annotations

from .common import StorageError, create_logger, retry_with_backoff

__all__ = ["StorageError", "create_logger", "retry_with_backoff"]
