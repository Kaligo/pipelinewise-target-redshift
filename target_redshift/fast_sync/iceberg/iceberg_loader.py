from functools import cached_property, lru_cache

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as fs
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
        # Make it simpler for now, just one partition column
        self.partition_column = "_sdc_batched_at"
        self.s3_region = stream_s3_info.s3_region
        self.s3_bucket = stream_s3_info.s3_bucket
        self.s3_key = stream_s3_info.s3_path
        self.number_of_files = stream_s3_info.files_uploaded
        self._load_source_schema = lru_cache(self._load_source_schema)

    @property
    def iceberg_table(self) -> str:
        """Iceberg table name derived from the stream name, with hyphens normalised to underscores."""
        return self.stream.replace("-", "_")

    @property
    def iceberg_table_identifier(self) -> str:
        """Fully-qualified Iceberg table identifier: <namespace>.<table>."""
        return f"{self.iceberg_namespace}.{self.iceberg_table}"

    @property
    def source_s3_path(self) -> str:
        """Full S3 path to the source data files (bucket/key prefix, no s3:// scheme)."""
        return f"{self.s3_bucket}/{self.s3_key}"

    @property
    def iceberg_table_location(self) -> str:
        """S3 URI where the Iceberg table data is stored."""
        return f"s3://{self.s3_bucket}/iceberg/{self.iceberg_namespace}/{self.iceberg_table}"

    def _iterate_source_s3_path(self):
        for part_num in range(1, self.number_of_files + 1):
            if self.number_of_files > 1:
                yield f"{self.source_s3_path}_part{part_num}"
            else:
                yield self.source_s3_path

    def _sync_iceberg_table(self, table: Table, s3_file_path: str):
        """
        Sync the iceberg table schema and data from the source S3 path
        """
        source_schema = self._load_source_schema(s3_file_path)
        to_delete = set(source_schema.names) - set(table.schema().as_arrow().names)

        with table.transaction() as txt:
            with txt.update_schema() as update:
                update.union_by_name(source_schema)

                for column_name in to_delete:
                    update.delete_column(column_name)
            txt.add_files(
                # s3_file_path is bucket/key (from _iterate_source_s3_path); add_files expects a full s3:// URI, so we prepend the prefix.
                file_paths=[f"s3://{s3_file_path}"],
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

    def _load_source_schema(self, s3_file_path: str) -> pa.Schema:
        """
        Get the source schema from the source S3 path
        """
        return pq.read_schema(
            # s3_file_path must be bucket/key (no s3://); PyArrow S3FileSystem rejects URIs.
            s3_file_path,
            filesystem=fs.S3FileSystem(
                region=self.s3_region,
                access_key=self.connection_config.get("aws_access_key_id"),
                secret_key=self.connection_config.get("aws_secret_access_key"),
                session_token=self.connection_config.get("aws_session_token"),
            ),
        )

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
        file_paths = list(self._iterate_source_s3_path())
        iceberg_table = self._load_iceberg_table(
            self._load_source_schema(file_paths[0])
        )
        for s3_file_path in file_paths:
            self.logger.info("Loading iceberg table data from %s", s3_file_path)
            self._sync_iceberg_table(iceberg_table, s3_file_path)
