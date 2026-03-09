"""
Unit tests for Fast Sync Iceberg loader in target-redshift
"""

import pyarrow as pa
from unittest.mock import MagicMock, patch

from target_redshift.fast_sync.loader import FastSyncS3Info
from target_redshift.fast_sync.iceberg.iceberg_loader import (
    FastSyncIcebergLoader,
)


class TestFastSyncIcebergLoader:
    """Test cases for FastSyncIcebergLoader"""

    def setup_method(self):
        """Set up test fixtures"""
        self.source_schema = pa.schema(
            [("id", pa.int64()), ("name", pa.string()), ("_sdc_batched_at", pa.timestamp("us"))]
        )
        self.s3_info = FastSyncS3Info(
            s3_bucket="my-bucket",
            s3_paths=["fast_sync/export/path/data.parquet"],
            s3_region="us-east-1",
            replication_method="FULL_TABLE",
            file_format="parquet",
            rows_uploaded=100,
            pyarrow_schema=self.source_schema,
        )

    def _create_loader(self, s3_info=None):
        connection_config = {
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "iceberg_catalog_name": "glue_catalog",
            "iceberg_namespace": "my_iceberg_db",
            "iceberg_s3_prefix": "iceberg",
        }
        return FastSyncIcebergLoader(
            logger=MagicMock(),
            stream="test_schema-test_table",
            connection_config=connection_config,
            stream_s3_info=s3_info or self.s3_info,
        )

    def test_init_sets_attributes(self):
        """Test FastSyncIcebergLoader init sets catalog, namespace, table and paths"""
        loader = self._create_loader()

        assert loader.catalog_name == "glue_catalog"
        assert loader.iceberg_namespace == "my_iceberg_db"
        assert loader.iceberg_table == "test_schema_test_table"
        assert loader.iceberg_table_identifier == "my_iceberg_db.test_schema_test_table"
        assert loader.partition_column == "_sdc_batched_at"
        assert loader.s3_region == "us-east-1"
        assert loader.s3_bucket == "my-bucket"
        assert loader.source_s3_paths == ["my-bucket/fast_sync/export/path/data.parquet"]
        assert (
            loader.iceberg_table_location
            == "s3://my-bucket/iceberg/test_schema-test_table"
        )

    def test_init_uses_partition_column_from_s3_info_when_provided(self):
        """When FastSyncS3Info has partition_column set, loader uses it."""
        s3_info_with_partition = FastSyncS3Info(
            s3_bucket="my-bucket",
            s3_paths=["fast_sync/export/path/data.parquet"],
            s3_region="us-east-1",
            replication_method="FULL_TABLE",
            file_format="parquet",
            rows_uploaded=100,
            pyarrow_schema=self.source_schema,
            partition_column="custom_ts",
        )
        loader = self._create_loader(s3_info=s3_info_with_partition)
        assert loader.partition_column == "custom_ts"

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_creates_table_and_adds_files(self, mock_load_catalog):
        """Test load_from_s3 when table does not exist: create table, sync schema, add_files"""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = False
        mock_catalog.create_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        loader = self._create_loader()
        loader.load_from_s3()

        mock_catalog.create_namespace_if_not_exists.assert_called_once_with(
            "my_iceberg_db"
        )
        mock_catalog.table_exists.assert_called_once_with(
            "my_iceberg_db.test_schema_test_table"
        )
        mock_catalog.create_table.assert_called_once()
        call_kw = mock_catalog.create_table.call_args[1]
        assert call_kw["identifier"] == "my_iceberg_db.test_schema_test_table"
        assert call_kw["location"] == "s3://my-bucket/iceberg/test_schema-test_table"

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_existing_table_loads_and_syncs(self, mock_load_catalog):
        """Test load_from_s3 when table exists: load_table, no create_table"""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        loader = self._create_loader()
        loader.load_from_s3()

        mock_catalog.load_table.assert_called_once_with(
            "my_iceberg_db.test_schema_test_table"
        )
        mock_catalog.create_table.assert_not_called()

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_iceberg_catalog_caches_catalog(self, mock_load_catalog):
        """Test iceberg_catalog is cached (load_catalog called once)"""
        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog

        loader = self._create_loader()

        cat1 = loader.iceberg_catalog
        cat2 = loader.iceberg_catalog

        assert cat1 is cat2
        mock_load_catalog.assert_called_once()

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_iceberg_catalog_passes_glue_properties(self, mock_load_catalog):
        """Test iceberg_catalog passes Glue type and region to load_catalog"""
        mock_catalog = MagicMock()
        mock_load_catalog.return_value = mock_catalog

        loader = self._create_loader()
        _ = loader.iceberg_catalog

        mock_load_catalog.assert_called_once()
        assert mock_load_catalog.call_args[0][0] == "glue_catalog"
        call_kw = mock_load_catalog.call_args[1]
        assert call_kw["type"] == "glue"
        assert call_kw["client.region"] == "us-east-1"

    def test_sync_iceberg_table_calls_add_files(self):
        """Test _sync_iceberg_table calls add_files with s3:// URIs for each file path"""
        mock_table = MagicMock()
        mock_txt = MagicMock()
        mock_table.transaction.return_value.__enter__.return_value = mock_txt
        mock_table.transaction.return_value.__exit__.return_value = None
        mock_table.schema.return_value.as_arrow.return_value.names = ["id"]

        loader = self._create_loader()
        loader._sync_iceberg_table(mock_table, ["my-bucket/path/file.parquet"])

        mock_table.transaction.assert_called_once()
        mock_txt.add_files.assert_called_once_with(
            file_paths=["s3://my-bucket/path/file.parquet"],
            check_duplicate_files=True,
        )

    def test_sync_iceberg_table_schema_union_and_delete(self):
        """Test _sync_iceberg_table calls update_schema with union_by_name and deletes columns absent from source"""
        # Table has a, b, d; source has a, b, c -> union adds c; to_delete = table - source = {d}
        source_schema = pa.schema(
            [("a", pa.int64()), ("b", pa.string()), ("c", pa.int64())]
        )
        s3_info = FastSyncS3Info(
            s3_bucket="my-bucket",
            s3_paths=["fast_sync/export/path/data.parquet"],
            s3_region="us-east-1",
            replication_method="FULL_TABLE",
            file_format="parquet",
            rows_uploaded=100,
            pyarrow_schema=source_schema,
        )

        mock_table = MagicMock()
        mock_txt = MagicMock()
        mock_update = MagicMock()
        mock_txt.update_schema.return_value.__enter__.return_value = mock_update
        mock_txt.update_schema.return_value.__exit__.return_value = None
        mock_table.transaction.return_value.__enter__.return_value = mock_txt
        mock_table.transaction.return_value.__exit__.return_value = None
        mock_table.schema.return_value.as_arrow.return_value.names = ["a", "b", "d"]

        loader = self._create_loader(s3_info=s3_info)
        loader._sync_iceberg_table(mock_table, ["my-bucket/fast_sync/export/path/data.parquet"])

        mock_table.transaction.assert_called_once()
        mock_txt.update_schema.assert_called_once()
        mock_update.union_by_name.assert_called_once_with(source_schema)
        mock_update.delete_column.assert_called_once_with("d")

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_sync_schema_and_data_called(self, mock_load_catalog):
        """Test load_from_s3 calls _sync_iceberg_table once with all file paths as a list"""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        loader = self._create_loader()

        with patch.object(loader, "_sync_iceberg_table") as mock_sync:
            loader.load_from_s3()

        mock_sync.assert_called_once_with(
            mock_table, ["my-bucket/fast_sync/export/path/data.parquet"]
        )

    @patch("target_redshift.fast_sync.iceberg.iceberg_loader.load_catalog")
    def test_load_from_s3_multiple_files_calls_sync_once_with_all_paths(
        self, mock_load_catalog
    ):
        """Test load_from_s3 with multiple s3_paths calls _sync_iceberg_table once with all part paths"""
        mock_catalog = MagicMock()
        mock_table = MagicMock()
        mock_catalog.table_exists.return_value = True
        mock_catalog.load_table.return_value = mock_table
        mock_load_catalog.return_value = mock_catalog

        s3_info_multi = FastSyncS3Info(
            s3_bucket="my-bucket",
            s3_paths=[
                "fast_sync/export/path/data.parquet_part1",
                "fast_sync/export/path/data.parquet_part2",
            ],
            s3_region="us-east-1",
            replication_method="FULL_TABLE",
            file_format="parquet",
            rows_uploaded=200,
            pyarrow_schema=self.source_schema,
        )
        loader = self._create_loader(s3_info=s3_info_multi)

        with patch.object(loader, "_sync_iceberg_table") as mock_sync:
            loader.load_from_s3()

        mock_sync.assert_called_once_with(
            mock_table,
            [
                "my-bucket/fast_sync/export/path/data.parquet_part1",
                "my-bucket/fast_sync/export/path/data.parquet_part2",
            ],
        )
