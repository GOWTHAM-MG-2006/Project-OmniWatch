"""
OmniWatch — Unified Storage Layer
Component: Storage Client Unit Tests
Phase: 5
Purpose: Unit tests with MOCKED connections for ClickHouse, Neo4j, and MinIO
         storage clients covering retry logic (exactly 3 retries on failure),
         health checks, and error handling.
Inputs: Mocked driver/client objects (no real network connections)
Outputs: pytest pass/fail for all storage client unit tests
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from storage.common import StorageError
from storage.config import StorageConfig


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _cfg() -> StorageConfig:
    """Return a minimal StorageConfig without reading env vars."""
    return StorageConfig(
        clickhouse_host="test-ch",
        clickhouse_port=8123,
        clickhouse_db="test_db",
        clickhouse_user="default",
        clickhouse_password="",
        neo4j_uri="bolt://test-neo4j:7687",
        neo4j_user="neo4j",
        neo4j_password="test",
        minio_endpoint="test-minio:9010",
        minio_access_key="key",
        minio_secret_key="secret",
        minio_secure=False,
    )


_METRIC_ROW = {
    "entity_id": "svc-1",
    "entity_type": "SERVICE",
    "metric_name": "cpu_usage",
    "value": 42.0,
    "tags": {"region": "us-east"},
    "source_type": "performance",
    "timestamp": "2026-08-01T00:00:00",
}


# ===================================================================
# ClickHouse Client
# ===================================================================


class TestClickHouseClient:
    """Tests for storage.clickhouse.client.ClickHouseClient."""

    # ------------------------------------------------------------------ #
    # Retry — connect
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_retries_connect_then_succeeds(
        self, mock_get_client: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """Connect fails 3 times then succeeds on the 4th attempt (1+3 retries)."""
        from storage.clickhouse.client import ClickHouseClient

        mock_ch = MagicMock()
        mock_get_client.side_effect = [
            Exception("conn 1"),
            Exception("conn 2"),
            Exception("conn 3"),
            mock_ch,
        ]

        client = ClickHouseClient(config=_cfg())
        result = client.get_client()

        assert result is mock_ch
        assert mock_get_client.call_count == 4

    @patch("storage.common.time.sleep")
    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_connect_all_fails_raises_storage_error(
        self, mock_get_client: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 connect attempts fail → StorageError raised."""
        from storage.clickhouse.client import ClickHouseClient

        mock_get_client.side_effect = ConnectionError("refused")

        client = ClickHouseClient(config=_cfg())
        with pytest.raises(StorageError, match="could not connect to ClickHouse"):
            client.get_client()

        assert mock_get_client.call_count == 4

    # ------------------------------------------------------------------ #
    # Health check
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_health_check_returns_true_when_healthy(
        self, mock_get_client: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """health_check returns True when SELECT 1 succeeds."""
        from storage.clickhouse.client import ClickHouseClient

        mock_ch = MagicMock()
        mock_ch.command.return_value = None
        mock_get_client.return_value = mock_ch

        client = ClickHouseClient(config=_cfg())
        assert client.health_check() is True

    @patch("storage.common.time.sleep")
    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_health_check_returns_false_when_unhealthy(
        self, mock_get_client: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """health_check returns False (never raises) when SELECT 1 fails."""
        from storage.clickhouse.client import ClickHouseClient

        mock_ch = MagicMock()
        mock_ch.command.side_effect = Exception("query timeout")
        mock_get_client.return_value = mock_ch

        client = ClickHouseClient(config=_cfg())
        assert client.health_check() is False
        # 1 initial + 3 retries = 4 total command() calls
        assert mock_ch.command.call_count == 4

    # ------------------------------------------------------------------ #
    # Retry — operations (insert)
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_insert_retries_then_succeeds(
        self, mock_get_client: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """insert_metrics retries insert 3 times then succeeds on 4th."""
        from storage.clickhouse.client import ClickHouseClient

        mock_ch = MagicMock()
        mock_ch.insert.side_effect = [
            Exception("transient"),
            Exception("transient"),
            Exception("transient"),
            None,  # success
        ]
        mock_get_client.return_value = mock_ch

        client = ClickHouseClient(config=_cfg())
        result = client.insert_metrics([_METRIC_ROW])

        assert result == 1
        assert mock_ch.insert.call_count == 4

    @patch("storage.common.time.sleep")
    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_insert_all_fails_raises(
        self, mock_get_client: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 insert attempts fail → exception propagates after retries."""
        from storage.clickhouse.client import ClickHouseClient

        mock_ch = MagicMock()
        mock_ch.insert.side_effect = Exception("disk full")
        mock_get_client.return_value = mock_ch

        client = ClickHouseClient(config=_cfg())
        with pytest.raises(Exception, match="disk full"):
            client.insert_metrics([_METRIC_ROW])

        assert mock_ch.insert.call_count == 4

    # ------------------------------------------------------------------ #
    # Error handling — select_by_entity validation
    # ------------------------------------------------------------------ #

    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_select_invalid_table_raises_value_error(
        self, mock_get_client: MagicMock
    ) -> None:
        """select_by_entity rejects tables not in the whitelist."""
        from storage.clickhouse.client import ClickHouseClient

        mock_get_client.return_value = MagicMock()
        client = ClickHouseClient(config=_cfg())

        with pytest.raises(ValueError, match="table must be one of"):
            client.select_by_entity("svc-1", table="nonexistent")

    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_select_invalid_order_by_raises_value_error(
        self, mock_get_client: MagicMock
    ) -> None:
        """select_by_entity rejects SQL-injection-style order_by values."""
        from storage.clickhouse.client import ClickHouseClient

        mock_get_client.return_value = MagicMock()
        client = ClickHouseClient(config=_cfg())

        with pytest.raises(ValueError, match="invalid order_by"):
            client.select_by_entity("svc-1", order_by="1; DROP TABLE")

    # ------------------------------------------------------------------ #
    # Close
    # ------------------------------------------------------------------ #

    @patch("storage.clickhouse.client.clickhouse_connect.get_client")
    def test_clickhouse_close_releases_client(
        self, mock_get_client: MagicMock
    ) -> None:
        """close() calls close() on the underlying client and clears reference."""
        from storage.clickhouse.client import ClickHouseClient

        mock_ch = MagicMock()
        mock_get_client.return_value = mock_ch

        client = ClickHouseClient(config=_cfg())
        _ = client.get_client()
        client.close()

        mock_ch.close.assert_called_once()
        assert client._client is None


# ===================================================================
# Neo4j Client
# ===================================================================


class TestNeo4jClient:
    """Tests for storage.neo4j.client.Neo4jClient."""

    # ------------------------------------------------------------------ #
    # Retry — connect
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_retries_connect_then_succeeds(
        self, mock_driver_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """verify_connectivity fails 3 times then succeeds on 4th."""
        from storage.neo4j.client import Neo4jClient

        mock_driver = MagicMock()
        mock_driver_cls.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = [
            Exception("bolt 1"),
            Exception("bolt 2"),
            Exception("bolt 3"),
            None,
        ]

        client = Neo4jClient(config=_cfg())
        client.connect()

        assert mock_driver.verify_connectivity.call_count == 4

    @patch("storage.common.time.sleep")
    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_connect_all_fails_raises_storage_error(
        self, mock_driver_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 connect attempts fail → StorageError raised."""
        from storage.neo4j.client import Neo4jClient

        mock_driver = MagicMock()
        mock_driver_cls.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = ConnectionError("bolt down")

        client = Neo4jClient(config=_cfg())
        with pytest.raises(StorageError, match="Neo4j connect failed"):
            client.connect()

        assert mock_driver.verify_connectivity.call_count == 4

    # ------------------------------------------------------------------ #
    # Health check
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_health_check_returns_true_when_healthy(
        self, mock_driver_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """health_check returns True when verify_connectivity succeeds."""
        from storage.neo4j.client import Neo4jClient

        mock_driver = MagicMock()
        mock_driver_cls.return_value = mock_driver

        client = Neo4jClient(config=_cfg())
        assert client.health_check() is True
        mock_driver.verify_connectivity.assert_called_once()

    @patch("storage.common.time.sleep")
    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_health_check_raises_on_failure(
        self, mock_driver_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """health_check RAISES StorageError when connect() fails (not returns False)."""
        from storage.neo4j.client import Neo4jClient

        mock_driver = MagicMock()
        mock_driver_cls.return_value = mock_driver
        mock_driver.verify_connectivity.side_effect = Exception("auth failed")

        client = Neo4jClient(config=_cfg())
        with pytest.raises(StorageError, match="Neo4j connect failed"):
            client.health_check()

        assert mock_driver.verify_connectivity.call_count == 4

    # ------------------------------------------------------------------ #
    # Error handling — create_node
    # ------------------------------------------------------------------ #

    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_create_node_invalid_label_raises_storage_error(
        self, mock_driver_cls: MagicMock
    ) -> None:
        """create_node rejects labels that don't match the safe regex."""
        from storage.neo4j.client import Neo4jClient

        mock_driver_cls.return_value = MagicMock()
        client = Neo4jClient(config=_cfg())

        with pytest.raises(StorageError, match="Invalid Neo4j label"):
            client.create_node("Bad Label!@#", {"id": "n1"})

    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_create_node_missing_id_raises_storage_error(
        self, mock_driver_cls: MagicMock
    ) -> None:
        """create_node requires 'id' in properties."""
        from storage.neo4j.client import Neo4jClient

        mock_driver_cls.return_value = MagicMock()
        client = Neo4jClient(config=_cfg())

        with pytest.raises(StorageError, match="requires an 'id' property"):
            client.create_node("Service", {"name": "no-id"})

    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_create_node_query_failure_raises_storage_error(
        self, mock_driver_cls: MagicMock
    ) -> None:
        """create_node wraps session.run failures in StorageError."""
        from storage.neo4j.client import Neo4jClient

        mock_driver = MagicMock()
        mock_driver_cls.return_value = mock_driver
        mock_session = MagicMock()
        mock_driver.session.return_value.__enter__ = MagicMock(return_value=mock_session)
        mock_driver.session.return_value.__exit__ = MagicMock(return_value=False)
        mock_session.run.side_effect = Exception("bolt timeout")

        client = Neo4jClient(config=_cfg())
        with pytest.raises(StorageError, match="create_node.*failed"):
            client.create_node("Service", {"id": "svc-1", "name": "svc-1"})

    # ------------------------------------------------------------------ #
    # Close
    # ------------------------------------------------------------------ #

    @patch("storage.neo4j.client.GraphDatabase.driver")
    def test_neo4j_close_releases_driver(
        self, mock_driver_cls: MagicMock
    ) -> None:
        """close() calls close() on the underlying driver."""
        from storage.neo4j.client import Neo4jClient

        mock_driver = MagicMock()
        mock_driver_cls.return_value = mock_driver

        client = Neo4jClient(config=_cfg())
        client.close()

        mock_driver.close.assert_called_once()


# ===================================================================
# MinIO Client
# ===================================================================


class TestMinioClient:
    """Tests for storage.minio.client.MinioClient."""

    # ------------------------------------------------------------------ #
    # Retry — upload
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_retries_upload_then_succeeds(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """upload_object retries put_object 3 times then succeeds on 4th."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_result = MagicMock()
        mock_client.put_object.side_effect = [
            Exception("upload 1"),
            Exception("upload 2"),
            Exception("upload 3"),
            mock_result,
        ]

        client = MinioClient(config=_cfg())
        result = client.upload_object("bucket", "obj.bin", b"data")

        assert result is mock_result
        assert mock_client.put_object.call_count == 4

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_upload_all_fails_raises_storage_error(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 upload attempts fail → StorageError raised."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_client.put_object.side_effect = Exception("s3 down")

        client = MinioClient(config=_cfg())
        with pytest.raises(StorageError, match="MinIO upload failed"):
            client.upload_object("bucket", "obj.bin", b"data")

        assert mock_client.put_object.call_count == 4

    # ------------------------------------------------------------------ #
    # Retry — download
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_retries_download_then_succeeds(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """download_object retries get_object 3 times then succeeds on 4th."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_response = MagicMock()
        mock_response.read.return_value = b"payload"

        mock_client.get_object.side_effect = [
            Exception("download 1"),
            Exception("download 2"),
            Exception("download 3"),
            mock_response,
        ]

        client = MinioClient(config=_cfg())
        data = client.download_object("bucket", "obj.bin")

        assert data == b"payload"
        assert mock_client.get_object.call_count == 4

    # ------------------------------------------------------------------ #
    # Retry — list_objects
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_retries_list_then_succeeds(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """list_objects retries 3 times then succeeds on 4th."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client

        mock_obj = MagicMock()
        mock_obj.object_name = "file.txt"

        mock_client.list_objects.side_effect = [
            Exception("list 1"),
            Exception("list 2"),
            Exception("list 3"),
            [mock_obj],
        ]

        client = MinioClient(config=_cfg())
        result = client.list_objects("bucket", prefix="file")

        assert result == ["file.txt"]
        assert mock_client.list_objects.call_count == 4

    # ------------------------------------------------------------------ #
    # Retry — delete
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_retries_delete_then_succeeds(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """delete_object retries remove_object 3 times then succeeds on 4th."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_client.remove_object.side_effect = [
            Exception("del 1"),
            Exception("del 2"),
            Exception("del 3"),
            None,
        ]

        client = MinioClient(config=_cfg())
        client.delete_object("bucket", "obj.bin")

        assert mock_client.remove_object.call_count == 4

    # ------------------------------------------------------------------ #
    # Health check
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_health_check_returns_true_when_healthy(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """health_check returns True when bucket_exists succeeds."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_client.bucket_exists.return_value = True

        client = MinioClient(config=_cfg())
        assert client.health_check() is True

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_health_check_returns_false_when_unhealthy(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """health_check returns False (never raises) when probe fails."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        # bucket_exists returns False (bucket missing), list_buckets also fails
        mock_client.bucket_exists.return_value = False
        mock_client.list_buckets.side_effect = Exception("connection refused")

        client = MinioClient(config=_cfg())
        assert client.health_check() is False
        # _probe called 4 times (1 + 3 retries); each calls bucket_exists
        assert mock_client.bucket_exists.call_count == 4

    # ------------------------------------------------------------------ #
    # Error handling — download
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_download_all_fails_raises_storage_error(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 download attempts fail → StorageError raised."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_client.get_object.side_effect = Exception("not found")

        client = MinioClient(config=_cfg())
        with pytest.raises(StorageError, match="MinIO download failed"):
            client.download_object("bucket", "missing.bin")

        assert mock_client.get_object.call_count == 4

    # ------------------------------------------------------------------ #
    # Error handling — list
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_list_all_fails_raises_storage_error(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 list attempts fail → StorageError raised."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_client.list_objects.side_effect = Exception("access denied")

        client = MinioClient(config=_cfg())
        with pytest.raises(StorageError, match="MinIO list failed"):
            client.list_objects("bucket")

        assert mock_client.list_objects.call_count == 4

    # ------------------------------------------------------------------ #
    # Error handling — delete
    # ------------------------------------------------------------------ #

    @patch("storage.common.time.sleep")
    @patch("storage.minio.client.Minio")
    def test_minio_delete_all_fails_raises_storage_error(
        self, mock_minio_cls: MagicMock, mock_sleep: MagicMock
    ) -> None:
        """All 4 delete attempts fail → StorageError raised."""
        from storage.minio.client import MinioClient

        mock_client = MagicMock()
        mock_minio_cls.return_value = mock_client
        mock_client.remove_object.side_effect = Exception("permission denied")

        client = MinioClient(config=_cfg())
        with pytest.raises(StorageError, match="MinIO delete failed"):
            client.delete_object("bucket", "obj.bin")

        assert mock_client.remove_object.call_count == 4
