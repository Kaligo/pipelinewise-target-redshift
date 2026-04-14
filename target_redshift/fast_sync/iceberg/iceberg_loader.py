from typing import Any, Dict, List, Optional
from functools import cached_property
import logging

import boto3
import pyarrow as pa
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table
from pyiceberg.table.sorting import NullOrder
from pyiceberg.transforms import DayTransform, IdentityTransform

from target_redshift.fast_sync.loader import FastSyncS3Info


class FastSyncIcebergLoader:
    def __init__(
        self,
        logger: logging.Logger,
        stream: str,
        connection_config: Dict[str, Any],
        stream_s3_info: FastSyncS3Info,
        boto3_session: Optional[boto3.session.Session] = None,
    ):

        if stream_s3_info.file_format not in ("parquet", "orc", "avro"):
            raise ValueError(
                f"Unsupported file format: {stream_s3_info.file_format}. Should be one of: parquet, orc, avro"
            )

        self.logger = logger
        self.stream = stream
        self.connection_config = connection_config
        self.catalog_name = connection_config.get("iceberg_catalog_name")
        self.iceberg_namespace = connection_config.get("iceberg_namespace")
        self.iceberg_s3_prefix = connection_config.get("iceberg_s3_prefix")
        self.source_schema = stream_s3_info.pyarrow_schema
        # Make it simpler for now, just one partition column
        self.partition_column = stream_s3_info.partition_column or "_sdc_batched_at"
        self.s3_region = stream_s3_info.s3_region
        self.s3_bucket = stream_s3_info.s3_bucket
        self.s3_keys = stream_s3_info.s3_paths
        self.boto3_session = boto3_session

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

        When a boto3 Session is provided, build a Glue client with Session.client().
        That is the supported boto3 API: the client uses the session's credential
        provider for each request, so temporary creds (IRSA, assumed roles, etc.)
        refresh instead of passing frozen keys from connection_config (used for COPY).

        PyIceberg's GlueCatalog accepts this client directly; see GlueCatalog.__init__
        (client=...) in apache/iceberg-python.
        """
        catalog_props: Dict[str, Any] = {
            "type": "glue",
            "client.region": self.s3_region,
        }
        if self.boto3_session is not None:
            catalog_props["client"] = self.boto3_session.client(
                "glue", region_name=self.s3_region
            )
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
