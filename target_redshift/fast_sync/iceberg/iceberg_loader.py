from typing import List
from functools import cached_property

import pyarrow as pa
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table
from pyiceberg.table.sorting import NullOrder
from pyiceberg.transforms import DayTransform, IdentityTransform

from target_redshift.fast_sync.loader import FastSyncS3Info
from target_redshift.db_sync import DbSync


class FastSyncIcebergLoader:
    def __init__(
        self,
        db_sync: DbSync,
        stream_s3_info: FastSyncS3Info,
    ):

        if stream_s3_info.file_format not in ("parquet", "orc", "avro"):
            raise ValueError(
                f"Unsupported file format: {stream_s3_info.file_format}. Should be one of: parquet, orc, avro"
            )

        self.logger = db_sync.logger
        self.connection_config = db_sync.connection_config
        self.stream = db_sync.stream_schema_message["stream"]
        self.catalog_name = db_sync.connection_config.get("iceberg_catalog_name")
        self.iceberg_namespace = self.connection_config.get("iceberg_namespace")
        self.iceberg_s3_prefix = self.connection_config.get("iceberg_s3_prefix")
        self.source_schema = stream_s3_info.pyarrow_schema
        # Make it simpler for now, just one partition column
        self.partition_column = "_sdc_batched_at"
        self.s3_region = stream_s3_info.s3_region
        self.s3_bucket = stream_s3_info.s3_bucket
        self.s3_keys = stream_s3_info.s3_paths

    @property
    def iceberg_table(self) -> str:
        """Iceberg table name derived from the stream name, with hyphens normalised to underscores."""
        return self.stream.replace("-", "_")

    @property
    def iceberg_table_identifier(self) -> str:
        """Fully-qualified Iceberg table identifier: <namespace>.<table>."""
        return f"{self.iceberg_namespace}.{self.iceberg_table}"

    @property
    def iceberg_table_location(self) -> str:
        """S3 URI where the Iceberg table data is stored."""
        return f"s3://{self.s3_bucket}/{self.iceberg_s3_prefix}/{self.stream}"

    @property
    def source_s3_paths(self) -> List[str]:
        """List of all S3 paths to the source data files."""
        return [self._source_s3_path(s3_key) for s3_key in self.s3_keys]

    def _source_s3_path(self, s3_key) -> str:
        """Full S3 path to the source data files (bucket/key prefix, no s3:// scheme)."""
        return f"{self.s3_bucket}/{s3_key}"

    def _sync_iceberg_table(self, table: Table, s3_file_paths: List[str]):
        """
        Sync the iceberg table schema and data from the source S3 path
        """
        to_delete = set(table.schema().as_arrow().names) - set(self.source_schema.names)

        with table.transaction() as txt:
            with txt.update_schema() as update:
                update.union_by_name(self.source_schema)

                for column_name in to_delete:
                    update.delete_column(column_name)
            txt.add_files(
                # s3_file_path is bucket/key (from source_s3_paths); add_files expects a full s3:// URI, so we prepend the prefix.
                file_paths=[f"s3://{s3_file_path}" for s3_file_path in s3_file_paths],
                check_duplicate_files=True,
            )

    @cached_property
    def iceberg_catalog(self) -> Catalog:
        """
        Load the iceberg catalog and create a namespace if it doesn't exist
        """
        catalog_props = {
            "type": "glue",
            "client.region": self.s3_region,
            "client.access-key-id": self.connection_config.get("aws_access_key_id"),
            "client.secret-access-key": self.connection_config.get(
                "aws_secret_access_key"
            ),
            "client.session-token": self.connection_config.get("aws_session_token"),
        }
        iceberg_catalog = load_catalog(self.catalog_name, **catalog_props)
        iceberg_catalog.create_namespace_if_not_exists(self.iceberg_namespace)
        return iceberg_catalog

    def _load_iceberg_table(self, schema: pa.Schema) -> Table:
        """
        Load the iceberg table and create it if it doesn't exist
        """
        if self.iceberg_catalog.table_exists(self.iceberg_table_identifier):
            iceberg_table = self.iceberg_catalog.load_table(
                self.iceberg_table_identifier
            )
        else:
            iceberg_table = self.iceberg_catalog.create_table(
                identifier=self.iceberg_table_identifier,
                schema=schema,
                location=self.iceberg_table_location,
            )
            with iceberg_table.transaction() as txt:
                with txt.update_spec() as update:
                    update.add_field(
                        source_column_name=self.partition_column,
                        transform=DayTransform(),
                        partition_field_name=f"{self.partition_column}_day",
                    )
                with txt.update_sort_order() as update:
                    update.asc(
                        source_column_name=self.partition_column,
                        transform=IdentityTransform(),
                        null_order=NullOrder.NULLS_FIRST,
                    )
        return iceberg_table

    def load_from_s3(self):
        """
        Load the iceberg table data from the source S3 path
        """
        file_paths = self.source_s3_paths
        iceberg_table = self._load_iceberg_table(self.source_schema)
        self.logger.info("Loading iceberg table data from %s", ", ".join(file_paths))
        self._sync_iceberg_table(iceberg_table, file_paths)
