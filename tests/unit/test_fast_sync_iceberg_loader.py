"""
Unit tests for Fast Sync Iceberg loader in target-redshift
"""

import pyarrow as pa
import pytest
from unittest.mock import MagicMock, patch

from target_redshift import db_sync
from target_redshift.fast_sync.loader import FastSyncS3Info
from target_redshift.fast_sync.iceberg.iceberg_loader import (
    FastSyncIcebergLoader,
)


class TestFastSyncIcebergLoader:
    """Test cases for FastSyncIcebergLoader"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = {
            "host": "test-host.redshift.amazonaws.com",
            "port": 5439,
            "user": "test_user",
            "password": "test_password",
            "dbname": "test_db",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "s3_bucket": "my-bucket",
            "default_target_schema": "test_schema",
            "iceberg_catalog_name": "glue_catalog",
            "iceberg_namespace": "my_iceberg_db",
        }
        self.stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "_sdc_batched_at": {"type": ["null", "string"], "format": "date-time"},
                }
            },
            "key_properties": ["id"],
        }
        self.s3_info = FastSyncS3Info(
            s3_bucket="my-bucket",
            s3_path="fast_sync/export/path/data.parquet",
            s3_region="us-east-1",
            files_uploaded=1,
            replication_method="FULL_TABLE",
            file_format="parquet",
            rows_uploaded=100,
        )

    def _create_db_sync(self, config=None, schema=None):
        """Create DbSync with optional overrides"""
        return db_sync.DbSync(
            config or self.config,
            schema or self.stream_schema_message,
        )

    def test_init_sets_attributes(self):
        """Test FastSyncIcebergLoader init sets catalog, namespace, table and paths"""
        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)

        assert loader.catalog_name == "glue_catalog"
        assert loader.iceberg_namespace == "my_iceberg_db"
        assert loader.iceberg_table == "test_schema_test_table"
        assert loader.iceberg_table_identifier == "my_iceberg_db.test_schema_test_table"
        assert loader.partition_column == "_sdc_batched_at"
        assert loader.s3_region == "us-east-1"
        assert loader.s3_bucket == "my-bucket"
        assert loader.source_s3_path == "my-bucket/fast_sync/export/path/data.parquet"
        assert (
            loader.iceberg_table_location
            == "s3://my-bucket/iceberg/my_iceberg_db/test_schema_test_table"
        )

    @patch(
        "target_redshift.fast_sync.iceberg.iceberg_loader.pq.read_schema",
        return_value=pa.schema([("id", pa.int64()), ("name", pa.string())]),
    )
    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_creates_table_and_adds_files(
        self, mock_load_catalog, mock_read_schema
    ):
        """Test load_from_s3 when table does not exist: create table, sync schema, add_files"""
        db = self._create_db_sync()
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = False
        mock_catalog.create_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        loader = FastSyncIcebergLoader(db, self.s3_info)
        loader.load_from_s3()

        mock_read_schema.assert_called_once()
        mock_catalog.create_namespace_if_not_exists.assert_called_once_with(
            "my_iceberg_db"
        )
        mock_catalog.table_exists.assert_called_once_with(
            "my_iceberg_db.test_schema_test_table"
        )
        mock_catalog.create_table.assert_called_once()
        call_kw = mock_catalog.create_table.call_args[1]
        assert call_kw["identifier"] == "my_iceberg_db.test_schema_test_table"
        assert call_kw["location"] == (
            "s3://my-bucket/iceberg/my_iceberg_db/test_schema_test_table"
        )

    @patch(
        "target_redshift.fast_sync.iceberg.iceberg_loader.pq.read_schema",
        return_value=pa.schema([("id", pa.int64()), ("name", pa.string())]),
    )
    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_existing_table_loads_and_syncs(
        self, mock_load_catalog, mock_read_schema
    ):
        """Test load_from_s3 when table exists: load_table, no create_table"""
        db = self._create_db_sync()
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        loader = FastSyncIcebergLoader(db, self.s3_info)
        loader.load_from_s3()

        mock_catalog.load_table.assert_called_once_with(
            "my_iceberg_db.test_schema_test_table"
        )
        mock_catalog.create_table.assert_not_called()

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.fs.S3FileSystem")
    @patch(
        "target_redshift.fast_sync.iceberg.iceberg_loader.pq.read_schema",
        return_value=pa.schema([("id", pa.int64())]),
    )
    def test_load_source_schema_caches_schema(self, mock_read_schema, mock_s3fs):
        """Test _load_source_schema is cached (read_schema called once for two invocations)"""
        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)
        s3_file_path = "my-bucket/fast_sync/export/path/data.parquet"

        schema1 = loader._load_source_schema(s3_file_path)
        schema2 = loader._load_source_schema(s3_file_path)

        assert schema1 is schema2
        mock_read_schema.assert_called_once()
        assert mock_read_schema.call_args[0][0] == s3_file_path

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_iceberg_catalog_caches_catalog(self, mock_load_catalog):
        """Test _load_iceberg_catalog is cached (load_catalog called once)"""
        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog

        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)
        loader._source_schema = pa.schema([("id", pa.int64())])  # avoid pq.read_schema
        loader._iceberg_table = MagicMock()  # avoid create_table path

        cat1 = loader._load_iceberg_catalog()
        cat2 = loader._load_iceberg_catalog()

        assert cat1 is cat2
        mock_load_catalog.assert_called_once()

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_iceberg_catalog_passes_glue_properties(self, mock_load_catalog):
        """Test _load_iceberg_catalog passes Glue type and region to load_catalog"""
        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog

        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)
        loader._source_schema = pa.schema([("id", pa.int64())])
        loader._iceberg_table = MagicMock()

        loader._load_iceberg_catalog()

        mock_load_catalog.assert_called_once()
        assert mock_load_catalog.call_args[0][0] == "glue_catalog"
        call_kw = mock_load_catalog.call_args[1]
        assert call_kw["type"] == "glue"
        assert call_kw["client.region"] == "us-east-1"

    def test_sync_iceberg_table_data_calls_add_files(self):
        """Test _sync_iceberg_table_data calls add_files with single file path and check_duplicate_files"""
        mock_table = MagicMock()
        mock_txt = MagicMock()
        mock_table.transaction.return_value.__enter__.return_value = mock_txt
        mock_table.transaction.return_value.__exit__.return_value = None

        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)
        s3_file_path = "s3://my-bucket/path/file.parquet"
        loader._sync_iceberg_table_data(mock_table, s3_file_path)

        mock_table.transaction.assert_called_once()
        mock_txt.add_files.assert_called_once_with(
            file_paths=[s3_file_path],
            check_duplicate_files=True,
        )

    def test_sync_iceberg_table_schema_union_and_delete(self):
        """Test _sync_iceberg_table_schema calls update_schema with union_by_name and delete_column"""
        mock_table = MagicMock()
        mock_txt = MagicMock()
        mock_update = MagicMock()
        mock_txt.update_schema.return_value.__enter__.return_value = mock_update
        mock_txt.update_schema.return_value.__exit__.return_value = None
        mock_table.transaction.return_value.__enter__.return_value = mock_txt
        mock_table.transaction.return_value.__exit__.return_value = None

        # Table has a,b; source has a,b,c -> union adds c; to_delete = source - table = {c}
        source_schema = pa.schema(
            [("a", pa.int64()), ("b", pa.string()), ("c", pa.int64())]
        )
        mock_table.schema.return_value.as_arrow.return_value.names = ["a", "b"]

        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)
        loader._sync_iceberg_table_schema(mock_table, source_schema)

        mock_table.transaction.assert_called_once()
        mock_txt.update_schema.assert_called_once()
        mock_update.union_by_name.assert_called_once_with(source_schema)
        mock_update.delete_column.assert_called()  # at least once for 'c' (in source not in table)

    @patch(
        "target_redshift.fast_sync.iceberg.iceberg_loader.pq.read_schema",
        return_value=pa.schema([("id", pa.int64()), ("_sdc_batched_at", pa.timestamp("us"))]),
    )
    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_sync_schema_and_data_called(
        self, mock_load_catalog, mock_read_schema
    ):
        """Test load_from_s3 calls schema sync and data sync with correct S3 path"""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, self.s3_info)

        with patch.object(
            loader, "_sync_iceberg_table_schema"
        ) as mock_sync_schema, patch.object(
            loader, "_sync_iceberg_table_data"
        ) as mock_sync_data:
            loader.load_from_s3()

        mock_sync_schema.assert_called_once()
        assert mock_sync_schema.call_args[0][0] is mock_table
        assert mock_sync_schema.call_args[0][1] == mock_read_schema.return_value

        mock_sync_data.assert_called_once()
        assert mock_sync_data.call_args[0][0] is mock_table
        # Path from _iterate_source_s3_path is bucket/path (no s3:// prefix)
        assert mock_sync_data.call_args[0][1] == "my-bucket/fast_sync/export/path/data.parquet"

    @patch(
        "target_redshift.fast_sync.iceberg.iceberg_loader.pq.read_schema",
        return_value=pa.schema([("id", pa.int64())]),
    )
    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_multiple_files_calls_sync_data_per_file(
        self, mock_load_catalog, mock_read_schema
    ):
        """Test load_from_s3 with files_uploaded=2 calls _sync_iceberg_table_data twice with part paths"""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        s3_info_multi = FastSyncS3Info(
            s3_bucket="my-bucket",
            s3_path="fast_sync/export/path/data.parquet",
            s3_region="us-east-1",
            files_uploaded=2,
            replication_method="FULL_TABLE",
            file_format="parquet",
            rows_uploaded=200,
        )
        db = self._create_db_sync()
        loader = FastSyncIcebergLoader(db, s3_info_multi)

        with patch.object(
            loader, "_sync_iceberg_table_schema"
        ) as mock_sync_schema, patch.object(
            loader, "_sync_iceberg_table_data"
        ) as mock_sync_data:
            loader.load_from_s3()

        assert mock_sync_schema.call_count == 2
        assert mock_sync_data.call_count == 2
        assert mock_sync_data.call_args_list[0][0][1] == (
            "my-bucket/fast_sync/export/path/data.parquet_part1"
        )
        assert mock_sync_data.call_args_list[1][0][1] == (
            "my-bucket/fast_sync/export/path/data.parquet_part2"
        )
