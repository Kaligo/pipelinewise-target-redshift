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
        self.catalog_name = db_sync.connection_config.get("iceberg_catalog_name")
        self.iceberg_namespace = self.connection_config.get("iceberg_namespace")
        self.iceberg_table = db_sync.stream_schema_message["stream"].replace("-", "_")
        self.iceberg_table_identifier = f"{self.iceberg_namespace}.{self.iceberg_table}"
        # Make it simpler for now, just one partition column
        self.partition_column = "_sdc_batched_at"
        self.s3_region = stream_s3_info.s3_region
        self.s3_bucket = stream_s3_info.s3_bucket
        self.source_s3_path = f"s3://{self.s3_bucket}/{stream_s3_info.s3_path}"
        self.number_of_files = stream_s3_info.files_uploaded
        self.iceberg_table_location = f"s3://{self.s3_bucket}/iceberg/{self.iceberg_namespace}/{self.iceberg_table}"
        self._source_schema = None
        self._iceberg_catalog = None
        self._iceberg_table = None

    def _iterate_source_s3_path(self, s3_path: str, number_of_files: int):
        for part_num in range(1, number_of_files + 1):
            if number_of_files > 1:
                yield f"{s3_path}_part{part_num}"
            else:
                yield s3_path

    def _sync_iceberg_table_schema(self, table: Table, source_schema: pa.Schema):
        """
        Evolve the iceberg table schema to match the source schema
        """
        to_delete = set(source_schema.names) - set(table.schema().as_arrow().names)

        with table.transaction() as txt:
            with txt.update_schema() as update:
                update.union_by_name(source_schema)

                for column_name in to_delete:
                    update.delete_column(column_name)

    def _sync_iceberg_table_data(self, table: Table, s3_file_path: str):
        """
        Sync the iceberg table data from the source S3 path
        This based on the assumption the source data file already has static schema.
        """
        with table.transaction() as txt:
            txt.add_files(
                file_paths=[s3_file_path],
                check_duplicate_files=True,
            )

    def _load_source_schema(self, s3_file_path: str) -> pa.Schema:
        """
        Get the source schema from the source S3 path
        """
        if not self._source_schema:
            self._source_schema = pq.read_schema(
                s3_file_path,
                filesystem=fs.S3FileSystem(
                    region=self.s3_region,
                    access_key=self.connection_config.get("aws_access_key_id"),
                    secret_key=self.connection_config.get("aws_secret_access_key"),
                    session_token=self.connection_config.get("aws_session_token"),
                ),
            )
        return self._source_schema

    def _load_iceberg_catalog(self) -> Catalog:
        """
        Load the iceberg catalog and create a namespace if it doesn't exist
        """
        if not self._iceberg_catalog:
            catalog_props = {
                "type": "glue",
                "client.region": self.s3_region,
                "client.access-key-id": self.connection_config.get("aws_access_key_id"),
                "client.secret-access-key": self.connection_config.get(
                    "aws_secret_access_key"
                ),
                "client.session-token": self.connection_config.get("aws_session_token"),
            }
            self._iceberg_catalog = load_catalog(self.catalog_name, **catalog_props)
            self._iceberg_catalog.create_namespace_if_not_exists(self.iceberg_namespace)
        return self._iceberg_catalog

    def _load_iceberg_table(self, catalog: Catalog, schema: pa.Schema) -> Table:
        """
        Load the iceberg table and create it if it doesn't exist
        """
        if not self._iceberg_table:
            if catalog.table_exists(self.iceberg_table_identifier):
                self._iceberg_table = catalog.load_table(self.iceberg_table_identifier)
            else:
                self._iceberg_table = catalog.create_table(
                    identifier=self.iceberg_table_identifier,
                    schema=schema,
                    location=self.iceberg_table_location,
                )
                with self._iceberg_table.transaction() as txt:
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
        return self._iceberg_table

    def load_from_s3(self):
        """
        Load the iceberg table data from the source S3 path
        """

        for s3_file_path in self._iterate_source_s3_path(
            self.source_s3_path, self.number_of_files
        ):
            self.logger.info("Loading iceberg table data from %s", s3_file_path)
            catalog = self._load_iceberg_catalog()
            source_schema = self._load_source_schema(s3_file_path)
            table = self._load_iceberg_table(catalog, source_schema)

            # Syncing iceberg table schema and data
            self._sync_iceberg_table_schema(table, source_schema)
            self._sync_iceberg_table_data(table, s3_file_path)
