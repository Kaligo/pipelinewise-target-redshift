"""
Fast Sync Loader for target-redshift

This module handles loading data from S3 into Redshift using the fast sync strategy.
It extracts data loading logic from DbSync to provide a cleaner separation of concerns.
"""

import base64
import json
import os
import re
import shutil
import tempfile
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any, Optional
import psycopg2.extras
import boto3
import pyarrow as pa

from target_redshift.db_sync import safe_column_name, column_trans, primary_column_names

from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.partitioning import PartitionSpec

@dataclass
class FastSyncS3Info:
    """Value object representing fast sync S3 information from STATE messages."""

    s3_bucket: str
    s3_path: str
    s3_region: str
    files_uploaded: int
    replication_method: str
    rows_uploaded: int = 0
    file_format: str = "csv"
    pyarrow_schema: Optional[pa.Schema] = None

    @classmethod
    def from_message(cls, message: Dict[str, Any]) -> "FastSyncS3Info":
        """Create FastSyncS3Info from a message dictionary."""
        pyarrow_schema = None
        if "pyarrow_schema" in message:
            # Convert base64-encoded serialized PyArrow schema back to PyArrow schema
            # The schema is stored as base64-encoded serialized bytes in the message
            schema_data = message["pyarrow_schema"]
            # Decode base64 string to bytes and deserialize to PyArrow schema
            schema_bytes = base64.b64decode(schema_data.encode('utf-8'))
            pyarrow_schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))

        return cls(
            s3_bucket=message["s3_bucket"],
            s3_path=message["s3_path"],
            s3_region=message["s3_region"],
            files_uploaded=message["files_uploaded"],
            replication_method=message["replication_method"],
            rows_uploaded=message.get("rows_uploaded", 0),
            file_format=message.get("file_format", "csv"),
            pyarrow_schema=pyarrow_schema,
        )


class FastSyncLoader:  # pylint: disable=too-few-public-methods,too-many-instance-attributes
    """
    Handles fast sync data loading from S3 to Redshift
    """

    def __init__(self, db_sync):
        """
        Initialize FastSyncLoader

        Args:
            db_sync: DbSync instance to use for database operations
        """
        self.db_sync = db_sync
        self.logger = db_sync.logger
        self.connection_config = db_sync.connection_config
        self.s3_client = db_sync.s3
        self.skip_updates = db_sync.skip_updates
        self.full_refresh = db_sync.full_refresh
        self.detect_deletions = self.connection_config.get("detect_deletions", False)
        self.append_only = self.connection_config.get("append_only", False)
        self.cleanup_s3_files = self.connection_config.get("cleanup_s3_files", True)
        self.stream_schema_message = db_sync.stream_schema_message
        self.flatten_schema = db_sync.flatten_schema

    @staticmethod
    def _escape_sql_string(value: str) -> str:
        """
        Escape single quotes in SQL string literals
        """
        return value.replace("'", "''")

    @staticmethod
    def _assign_pyarrow_field_ids(pa_fields: List[pa.Field], field_id: List[int]) -> List[pa.Field]:
        """
        Assign field IDs to PyArrow schema fields.

        Based on: https://github.com/SidetrekAI/target-iceberg/blob/6152e26e587d69ea74bdf4de909571d61790dfab/target_iceberg/iceberg.py#L137-L160

        Args:
            pa_fields: List of PyArrow fields
            field_id: Mutable list containing current field ID [0] (will be incremented)

        Returns:
            List of fields with field IDs assigned
        """
        new_fields = []
        for field in pa_fields:
            if isinstance(field.type, pa.StructType):
                # Handle nested struct types recursively
                field_indices = list(range(field.type.num_fields))
                struct_fields = [field.type.field(field_i) for field_i in field_indices]
                nested_pa_fields = FastSyncLoader._assign_pyarrow_field_ids(struct_fields, field_id)
                new_fields.append(
                    pa.field(field.name, pa.struct(nested_pa_fields), nullable=field.nullable, metadata=field.metadata)
                )
            else:
                # Assign field ID to non-struct fields
                field_id[0] += 1
                # Add field ID to metadata
                metadata = field.metadata or {}
                # Convert bytes metadata to dict if needed
                if isinstance(metadata, dict):
                    metadata = {k.decode() if isinstance(k, bytes) else k:
                               v.decode() if isinstance(v, bytes) else v
                               for k, v in metadata.items()}
                else:
                    metadata = {}
                metadata["PARQUET:field_id"] = str(field_id[0])
                field_with_metadata = field.with_metadata(metadata)
                new_fields.append(field_with_metadata)

        return new_fields

    @staticmethod
    def _add_field_ids_to_pyarrow_schema(pyarrow_schema: pa.Schema) -> pa.Schema:
        """
        Add field IDs to PyArrow schema for Iceberg compatibility.

        Args:
            pyarrow_schema: PyArrow schema without field IDs

        Returns:
            PyArrow schema with field IDs in metadata
        """
        pa_fields_with_field_ids = FastSyncLoader._assign_pyarrow_field_ids(list(pyarrow_schema), [0])
        return pa.schema(pa_fields_with_field_ids)

    def _development_only_setup_glue_catalog_config(
        self, catalog_properties: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        DEVELOPMENT ONLY: Setup AWS Glue catalog configuration for development.

        Sets up:
        - Region: ap-southeast-1
        - Warehouse: s3://data-team-iceberg-playground/glue-catalog

        Note: AWS_PROFILE and HTTP_PROXY are handled by _development_only_with_aws_env wrapper.

        Args:
            catalog_properties: Existing catalog properties dictionary

        Returns:
            Updated catalog_properties dictionary
        """
        # Set development region
        catalog_properties["client.region"] = "ap-southeast-1"
        catalog_properties["warehouse"] = "s3://data-team-iceberg-playground/glue-catalog"

        self.logger.info(
            "DEVELOPMENT ONLY: Using region=%s, warehouse=%s for Glue catalog",
            catalog_properties["client.region"],
            catalog_properties["warehouse"],
        )

        return catalog_properties

    def _development_only_restore_aws_profile(
        self, original_aws_profile: Optional[str], original_http_proxy: Optional[str], original_https_proxy: Optional[str]
    ) -> None:
        """
        DEVELOPMENT ONLY: Restore original AWS_PROFILE, HTTP_PROXY, and HTTPS_PROXY environment variables.

        Args:
            original_aws_profile: Original AWS_PROFILE value to restore, or None
            original_http_proxy: Original HTTP_PROXY value to restore, or None
            original_https_proxy: Original HTTPS_PROXY value to restore, or None
        """
        if original_aws_profile is None:
            # Remove AWS_PROFILE if it wasn't set originally
            os.environ.pop("AWS_PROFILE", None)
        else:
            # Restore original value
            os.environ["AWS_PROFILE"] = original_aws_profile

        if original_http_proxy is None:
            # Remove HTTP_PROXY if it wasn't set originally
            os.environ.pop("HTTP_PROXY", None)
        else:
            # Restore original value
            os.environ["HTTP_PROXY"] = original_http_proxy

        if original_https_proxy is None:
            # Remove HTTPS_PROXY if it wasn't set originally
            os.environ.pop("HTTPS_PROXY", None)
        else:
            # Restore original value
            os.environ["HTTPS_PROXY"] = original_https_proxy

    def _development_only_with_aws_env(self, func, *args, **kwargs):
        """
        DEVELOPMENT ONLY: Wrapper method to set AWS_PROFILE and HTTP_PROXY environment variables
        before executing a function, then restore them afterwards.

        This ensures S3 operations use the correct AWS profile and proxy settings.

        Args:
            func: Function to execute with environment variables set
            *args: Positional arguments to pass to func
            **kwargs: Keyword arguments to pass to func

        Returns:
            Return value of func
        """
        # Set AWS profile for development
        aws_profile = "ascenda-sandbox"
        original_aws_profile = os.environ.get("AWS_PROFILE")
        os.environ["AWS_PROFILE"] = aws_profile

        # Set HTTP_PROXY and HTTPS_PROXY for development
        http_proxy = "http://proxy.int.kaligo.com:3128"
        original_http_proxy = os.environ.get("HTTP_PROXY")
        original_https_proxy = os.environ.get("HTTPS_PROXY")
        os.environ["HTTP_PROXY"] = http_proxy
        os.environ["HTTPS_PROXY"] = http_proxy
        # Also set NO_PROXY to empty to ensure proxy is used for all requests
        os.environ.pop("NO_PROXY", None)

        try:
            # Execute the function with environment variables set
            return func(*args, **kwargs)
        finally:
            # Restore original environment variables
            self._development_only_restore_aws_profile(
                original_aws_profile, original_http_proxy, original_https_proxy
            )

    def _create_or_load_iceberg_table(
        self,
        columns_with_trans: List[Dict[str, str]],
        target_table: str,
        s3_info: Optional[FastSyncS3Info] = None,
    ) -> Tuple[Any, str]:
        """
        Create or load an Iceberg table.

        This function:
        - Initializes the Iceberg catalog
        - Creates namespace if it doesn't exist
        - Builds Iceberg schema from columns
        - Creates the table if it doesn't exist, or loads it if it does

        Args:
            columns_with_trans: List of column definitions with transformations
            target_table: Target table name (schema.table format)
            s3_info: Optional FastSyncS3Info containing PyArrow schema from parquet file

        Returns:
            Tuple of (table object, table_identifier)
        """
        # Get catalog configuration from connection_config
        # Use AWS Glue catalog as per AWS documentation:
        # https://docs.aws.amazon.com/prescriptive-guidance/latest/apache-iceberg-on-aws/iceberg-pyiceberg.html
        # Use dbname from connection_config as catalog name
        catalog_name = self.connection_config.get("dbname", "default")
        catalog_properties = self.connection_config.get("iceberg_catalog_properties", {})

        # DEVELOPMENT ONLY: Override region and warehouse
        catalog_properties = self._development_only_setup_glue_catalog_config(
            catalog_properties
        )

        # Initialize the AWS Glue catalog
        # AWS credentials and proxy are automatically picked up from environment variables
        # set by _development_only_with_aws_env wrapper
        catalog = load_catalog(catalog_name, **catalog_properties, type="glue")

        # Normalize table_identifier: remove quotes, replace special characters, convert to lowercase
        # Handle cases like: "dev_manvuong__loyalty_engine".db/"users_safe"
        normalized_table = target_table.lower()
        # Remove quotes and other special characters
        normalized_table = normalized_table.replace('"', '').replace("'", "")
        # Replace forward slashes and other path separators with dots
        normalized_table = normalized_table.replace("/", ".").replace("\\", ".")
        # Remove any double dots that might result
        while ".." in normalized_table:
            normalized_table = normalized_table.replace("..", ".")
        # Split by dot to get schema and table
        parts = [part.strip() for part in normalized_table.split(".") if part.strip()]

        if len(parts) >= 2:
            # Use the last two parts as namespace and table
            namespace = parts[-2]
            table_name = parts[-1]
            table_identifier = f"{namespace}.{table_name}"
        elif len(parts) == 1:
            # Only table name provided, use default schema
            namespace = self.connection_config.get("default_target_schema", "public")
            table_name = parts[0]
            table_identifier = f"{namespace}.{table_name}"
        else:
            # Fallback if normalization results in empty
            namespace = self.connection_config.get("default_target_schema", "public")
            table_identifier = f"{namespace}.{target_table.lower()}"

        self.logger.info(
            "Creating or loading Iceberg table: %s", table_identifier
        )

        # Create namespace if it doesn't exist
        catalog.create_namespace_if_not_exists(namespace)

        self.logger.info("Using PyArrow schema from s3_info for Iceberg table")
        # Add field IDs to PyArrow schema before conversion
        # Based on: https://github.com/SidetrekAI/target-iceberg/blob/6152e26e587d69ea74bdf4de909571d61790dfab/target_iceberg/iceberg.py#L137-L160
        pyarrow_schema_with_ids = self._add_field_ids_to_pyarrow_schema(s3_info.pyarrow_schema)
        # Convert PyArrow schema to Iceberg schema
        schema = pyarrow_to_schema(pyarrow_schema_with_ids)

        # Load table if it exists, otherwise create it
        try:
            table = catalog.load_table(table_identifier)
            self.logger.info("Loaded existing Iceberg table: %s", table_identifier)
        except NoSuchTableError:
            properties = {
                "format-version": "3",
            }

            # Table doesn't exist, so create it
            table = catalog.create_table(
                identifier=table_identifier,
                partition_spec=PartitionSpec.identity("_sdc_batched_at"),
                schema=schema,
                properties=properties,
            )
            self.logger.info("Created new Iceberg table: %s", table_identifier)

        return table, table_identifier

    def _build_parquet_file_paths(self, s3_info: FastSyncS3Info) -> List[str]:
        """
        Build list of S3 paths for parquet files.

        Args:
            s3_info: FastSyncS3Info value object containing S3 information

        Returns:
            List of S3 paths to parquet files
        """
        # Build S3 paths for parquet files
        # Match the pattern used by aws_s3.query_export_to_s3: path, path_part2, path_part3, etc.
        s3_base_path = f"s3://{s3_info.s3_bucket}/{s3_info.s3_path}"
        parquet_files = []

        if s3_info.files_uploaded == 1:
            # Single file - use the path as-is
            parquet_files.append(s3_base_path)
        else:
            # Multiple files - main file and part files
            # The main file is s3_info.s3_path
            parquet_files.append(s3_base_path)

            # Part files follow the pattern: {s3_path}_part{part_num}
            # Remove .parquet extension if present to build base path
            base_key = s3_info.s3_path
            if base_key.endswith(".parquet"):
                base_key = base_key[:-8]

            # Add part files (part2, part3, etc.)
            for part_num in range(2, s3_info.files_uploaded + 1):
                part_key = f"{base_key}_part{part_num}.parquet"
                part_path = f"s3://{s3_info.s3_bucket}/{part_key}"
                parquet_files.append(part_path)

        return parquet_files

    def _development_only_copy_parquet_files_to_playground(
        self, parquet_files: List[str]
    ) -> List[str]:
        """
        DEVELOPMENT ONLY: Copy parquet files to data-team-iceberg-playground bucket.

        This function:
        - Downloads parquet files from original S3 location to local temp directory
        - Uploads them to data-team-iceberg-playground bucket in ap-southeast-1 region
        - Uses ascenda-sandbox AWS profile for the upload

        Args:
            parquet_files: List of original S3 paths (s3://bucket/key format)

        Returns:
            List of new S3 paths in data-team-iceberg-playground bucket
        """
        target_bucket = "data-team-iceberg-playground"
        target_region = "ap-southeast-1"

        self.logger.info(
            "DEVELOPMENT ONLY: Copying %d parquet file(s) to %s bucket",
            len(parquet_files),
            target_bucket,
        )

        # Create S3 clients with different profiles and proxy configuration
        # boto3 needs explicit proxy configuration via Config
        from botocore.config import Config

        http_proxy = os.environ.get("HTTP_PROXY", "http://proxy.int.kaligo.com:3128")
        boto3_config = Config(
            proxies={
                'http': http_proxy,
                'https': http_proxy,
            }
        )

        # Use different profiles for source and target
        source_profile = "ascenda-bnz-stg"
        target_profile = "ascenda-sandbox"

        source_s3_session = boto3.Session(profile_name=source_profile)
        source_s3_client = source_s3_session.client("s3", region_name=target_region, config=boto3_config)

        target_s3_session = boto3.Session(profile_name=target_profile)
        target_s3_client = target_s3_session.client("s3", region_name=target_region, config=boto3_config)

        new_parquet_files = []
        temp_dir = tempfile.mkdtemp()

        try:
            for s3_path in parquet_files:
                # Parse S3 path
                if not s3_path.startswith("s3://"):
                    raise ValueError(f"Invalid S3 path: {s3_path}")

                path_parts = s3_path[5:].split("/", 1)  # Remove 's3://' prefix
                source_bucket = path_parts[0]
                source_key = path_parts[1] if len(path_parts) > 1 else ""

                if not source_key:
                    raise ValueError(f"Empty S3 key in path: {s3_path}")

                # Extract filename from key (use the full key path to preserve structure)
                filename = os.path.basename(source_key)
                if not filename:
                    # Fallback: use the key itself if no basename
                    filename = source_key.replace("/", "_")

                # Use full key path for local file to avoid conflicts
                # Replace slashes with underscores for local filesystem
                safe_filename = source_key.replace("/", "_")
                local_file_path = os.path.join(temp_dir, safe_filename)

                # Download file to temp directory using profile-based S3 client
                self.logger.debug(
                    "Downloading s3://%s/%s to %s", source_bucket, source_key, local_file_path
                )
                source_s3_client.download_file(source_bucket, source_key, local_file_path)

                # Upload to target bucket using profile-based S3 client
                # Use the same key structure in the target bucket with development prefix
                target_key = f"glue-data/{source_key}"
                self.logger.debug(
                    "Uploading %s to s3://%s/%s", local_file_path, target_bucket, target_key
                )
                target_s3_client.upload_file(local_file_path, target_bucket, target_key)

                new_s3_path = f"s3://{target_bucket}/{target_key}"
                new_parquet_files.append(new_s3_path)
                self.logger.info(
                    "Copied %s -> %s", s3_path, new_s3_path
                )

        finally:
            # Clean up temp directory
            shutil.rmtree(temp_dir, ignore_errors=True)
            self.logger.debug("Cleaned up temporary directory: %s", temp_dir)

        return new_parquet_files

    def _add_files_to_iceberg_table(
        self,
        table: Any,
        table_identifier: str,
        parquet_files: List[str],
    ) -> None:
        """
        Add parquet files to an Iceberg table.

        Args:
            table: Iceberg table object
            table_identifier: Table identifier (schema.table format)
            parquet_files: List of S3 paths to parquet files to add
        """
        # DEVELOPMENT ONLY: Override region and warehouse
        # Note: AWS_PROFILE and HTTP_PROXY are handled by _development_only_with_aws_env wrapper
        catalog_properties = {}
        catalog_properties = self._development_only_setup_glue_catalog_config(
            catalog_properties
        )

        self.logger.info(
            "Adding %d parquet file(s) to Iceberg table: %s",
            len(parquet_files),
            table_identifier
        )
        table.add_files(parquet_files)
        self.logger.info(
            "Successfully added parquet files to Iceberg table: %s", table_identifier
        )


    def _load_parquet_to_iceberg(
        self,
        stream_schema_message: Dict[str, Any],
        columns_with_trans: List[Dict[str, str]],
        s3_info: FastSyncS3Info,
        target_table: str,
    ) -> None:
        """
        Load parquet file from S3 to Iceberg table using pyiceberg.

        This function orchestrates:
        - Creating or loading the Iceberg table
        - Adding the existing parquet file on S3 to the Iceberg table using add_files

        Args:
            stream_schema_message: Stream schema message containing stream information
            columns_with_trans: List of column definitions with transformations
            s3_info: FastSyncS3Info value object containing S3 information
            target_table: Target table name (schema.table format)
        """
        # DEVELOPMENT ONLY: Wrap entire operation with AWS_PROFILE and HTTP_PROXY
        def _execute_load():
            try:
                # Create or load the Iceberg table
                table, table_identifier = self._create_or_load_iceberg_table(
                    columns_with_trans,
                    target_table,
                    s3_info,
                )

                # Build parquet file paths
                parquet_files = self._build_parquet_file_paths(s3_info)

                # DEVELOPMENT ONLY: Copy files to playground bucket
                parquet_files = self._development_only_copy_parquet_files_to_playground(parquet_files)

                # Add files to Iceberg table
                self._add_files_to_iceberg_table(table, table_identifier, parquet_files)

            except Exception as exc:
                self.logger.error(
                    "Failed to load parquet file to Iceberg table: %s", str(exc), exc_info=True
                )
                raise

        # Execute with environment variables set
        self._development_only_with_aws_env(_execute_load)

    def _build_copy_credentials(self) -> str:
        if self.connection_config.get("aws_redshift_copy_role_arn"):
            role_arn = self._escape_sql_string(
                self.connection_config["aws_redshift_copy_role_arn"]
            )
            return f"IAM_ROLE '{role_arn}'"

        access_key = self._escape_sql_string(
            self.connection_config["aws_access_key_id"]
        )
        secret_key = self._escape_sql_string(
            self.connection_config["aws_secret_access_key"]
        )

        aws_session_token = ""
        if self.connection_config.get("aws_session_token"):
            session_token = self._escape_sql_string(
                self.connection_config["aws_session_token"]
            )
            aws_session_token = f"SESSION_TOKEN '{session_token}'"

        return f"""
            ACCESS_KEY_ID '{access_key}'
            SECRET_ACCESS_KEY '{secret_key}'
            {aws_session_token}
        """.strip()

    def _build_copy_options(self, s3_region: str, file_format: str) -> str:
        if file_format.lower() == "parquet":
            # Parquet COPY in Redshift doesn't support most options (including REGION when using manifests/prefix).
            return ""

        copy_options = self.connection_config.get(
            "copy_options",
            """
            EMPTYASNULL BLANKSASNULL TRIMBLANKS TRUNCATECOLUMNS
            TIMEFORMAT 'auto'
            COMPUPDATE OFF STATUPDATE OFF
        """,
        ).strip()

        # Check if REGION is already in copy_options
        if re.search(r"\bREGION\s+\'[^\']+\'", copy_options, re.IGNORECASE):
            # REGION already present, return as-is
            return copy_options

        # REGION not present, add it
        escaped_region = self._escape_sql_string(s3_region)
        return f"{copy_options} REGION '{escaped_region}'"

    @staticmethod
    def _build_s3_copy_path(s3_info: FastSyncS3Info) -> str:
        s3_copy_path = f"s3://{s3_info.s3_bucket}/{s3_info.s3_path}"
        if s3_info.files_uploaded > 1:
            # For multiple files, use prefix pattern.
            if s3_copy_path.endswith(".csv"):
                s3_copy_path = s3_copy_path[:-4]
            elif s3_copy_path.endswith(".parquet"):
                s3_copy_path = s3_copy_path[:-8]
        return s3_copy_path

    def _build_copy_sql(
        self,
        table_name: str,
        columns_with_trans: List[Dict[str, str]],
        s3_info: FastSyncS3Info,
    ) -> str:
        column_names = ", ".join(c["name"] for c in columns_with_trans)
        s3_copy_path = self._build_s3_copy_path(s3_info)
        copy_credentials = self._build_copy_credentials()
        copy_options = self._build_copy_options(s3_info.s3_region, s3_info.file_format)
        format_clause = "FORMAT AS PARQUET" if s3_info.file_format.lower() == "parquet" else "CSV"

        return f"""COPY {table_name} ({column_names})
            FROM '{s3_copy_path}'
            {copy_credentials}
            {copy_options}
            {format_clause}
        """.strip()

    def _perform_full_refresh(
        self, cur: Any, stream: str, target_table: str, stage_table: str
    ) -> None:
        """
        Perform full refresh by swapping tables
        """
        self.logger.info("Performing full refresh")
        archived_target_table = self.db_sync.table_name(
            stream, is_stage=False, is_archived=True
        )
        archived_table_name = archived_target_table.split(".")[1]
        target_table_name = target_table.split(".")[1]
        drop_archived = self.db_sync.drop_table_query(is_stage=False, is_archived=True)

        table_swap_sql = f"""BEGIN;
            ALTER TABLE {target_table} RENAME TO {archived_table_name};
            ALTER TABLE {stage_table} RENAME TO {target_table_name};
            {drop_archived};
            COMMIT;
        """
        self.logger.info("Running full-refresh query: %s", table_swap_sql)
        cur.execute(table_swap_sql)

    def _update_existing_records(
        self,
        cur: Any,
        target_table: str,
        stage_table: str,
        columns_with_trans: List[Dict[str, str]],
    ) -> int:
        """
        Update existing records in target table from stage table
        """
        if self.skip_updates:
            return 0

        self.logger.info("Performing data update")
        set_clause = ", ".join(
            f"{c['name']} = s.{c['name']}" for c in columns_with_trans
        )
        update_sql = f"""UPDATE {target_table}
            SET {set_clause}
            FROM {stage_table} s
            WHERE {self.db_sync.primary_key_merge_condition()}
        """
        self.logger.debug("Running query: %s", update_sql)
        cur.execute(update_sql)
        return cur.rowcount

    def _insert_new_records(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        cur: Any,
        target_table: str,
        stage_table: str,
        columns_with_trans: List[Dict[str, str]],
        stream_schema_message: Dict[str, Any],
    ) -> int:
        """
        Insert new records from stage table to target table
        """
        self.logger.info("Inserting new records")
        primary_key_conditions = " AND ".join(
            f"{target_table}.{c} IS NULL"
            for c in primary_column_names(stream_schema_message)
        )

        column_names = ", ".join(c["name"] for c in columns_with_trans)
        column_values = ", ".join(f"s.{c['name']}" for c in columns_with_trans)
        insert_sql = f"""INSERT INTO {target_table} ({column_names})
            SELECT {column_values}
            FROM {stage_table} s LEFT JOIN {target_table}
            ON {self.db_sync.primary_key_merge_condition()}
            WHERE {primary_key_conditions}
        """
        self.logger.debug("Running query: %s", insert_sql)
        cur.execute(insert_sql)
        return cur.rowcount

    def _detect_deletions(  # pylint: disable=too-many-arguments
        self,
        cur: Any,
        target_table: str,
        stage_table: str,
        columns_with_trans: List[Dict[str, str]],
    ) -> int:
        """
        Detect deleted records and set _SDC_DELETED_AT timestamp
        """
        has_deleted_at = any(
            "_SDC_DELETED_AT" in c["name"].strip('"') for c in columns_with_trans
        )

        if not has_deleted_at:
            return 0

        self.logger.info("Detecting deleted records")
        deleted_at_col = safe_column_name("_SDC_DELETED_AT")
        deletion_sql = f"""UPDATE {target_table}
            SET {deleted_at_col} = CURRENT_TIMESTAMP
            WHERE {deleted_at_col} IS NULL
            AND NOT EXISTS (
                SELECT 1 FROM {stage_table} s
                WHERE {self.db_sync.primary_key_merge_condition()}
            )
        """
        self.logger.debug("Running deletion detection query: %s", deletion_sql)
        cur.execute(deletion_sql)
        deletions = cur.rowcount
        if deletions > 0:
            self.logger.info(
                "Marked %s records as deleted in %s", deletions, target_table
            )
        return deletions

    def _append_all_records(
        self,
        cur: Any,
        target_table: str,
        columns_with_trans: List[Dict[str, str]],
        s3_info: FastSyncS3Info,
    ) -> None:
        """
        Append all records from S3 files directly to target table using COPY command.

        This method bypasses the stage table and directly copies data from S3 to the
        target table. In Redshift, COPY command always appends new data to the target table.

        Args:
            cur: Database cursor
            target_table: Target table name (already sanitized)
            columns_with_trans: List of column definitions
            s3_info: FastSyncS3Info value object containing S3 information
        """
        self.logger.info("Appending all records directly to target table %s", target_table)
        copy_sql = self._build_copy_sql(
            target_table,
            columns_with_trans,
            s3_info,
        )
        self.logger.debug("Running COPY query: %s", copy_sql)
        cur.execute(copy_sql)

    def _merge_data(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        cur: Any,
        stream_schema_message: Dict[str, Any],
        target_table: str,
        stage_table: str,
        columns_with_trans: List[Dict[str, str]],
        s3_info: FastSyncS3Info,
    ) -> Tuple[int, int, int]:
        """
        Merge data from stage table into target table

        Args:
            cur: Database cursor
            stream_schema_message: Stream schema message
            target_table: Target table name (already sanitized)
            stage_table: Stage table name (already sanitized)
            columns_with_trans: List of column definitions
            s3_info: FastSyncS3Info value object containing S3 information

        Returns:
            Tuple of (inserts, updates, deletions) counts
        """
        inserts = 0
        updates = 0
        deletions = 0
        has_key_properties = len(stream_schema_message["key_properties"]) > 0

        if self.full_refresh:
            self._perform_full_refresh(
                cur, stream_schema_message["stream"], target_table, stage_table
            )
            inserts = s3_info.rows_uploaded
        else:
            # Incremental update. Default mode.
            updates = self._update_existing_records(
                cur, target_table, stage_table, columns_with_trans
            )
            inserts = self._insert_new_records(
                cur,
                target_table,
                stage_table,
                columns_with_trans,
                stream_schema_message,
            )

        if (
            self.detect_deletions
            and s3_info.replication_method == "FULL_TABLE"
            and has_key_properties
            and not self.full_refresh
        ):
            # Delete detection works only if there is full data and table has primary key(s).
            deletions = self._detect_deletions(
                cur, target_table, stage_table, columns_with_trans
            )

        return inserts, updates, deletions

    def _load_via_staging(  # pylint: disable=too-many-arguments
        self,
        cur: Any,
        stream_schema_message: Dict[str, Any],
        target_table: str,
        columns_with_trans: List[Dict[str, str]],
        s3_info: FastSyncS3Info,
    ) -> Tuple[int, int, int]:
        """
        Load data from S3 via staging table and merge into target table.

        This method handles both full refresh and incremental merge operations:
        - Creates a temporary stage table
        - Loads data from S3 into stage table using COPY command
        - Merges data into target table (via _merge_data):
          * If full_refresh=True: Performs table swap (replaces entire table)
          * Otherwise: Performs incremental merge (UPDATE existing records, INSERT new records)
        - Detects deletions and sets _SDC_DELETED_AT (if enabled and conditions are met)
        - Cleans up stage table

        Args:
            cur: Database cursor
            stream_schema_message: Stream schema message
            target_table: Target table name (already sanitized)
            columns_with_trans: List of column definitions
            s3_info: FastSyncS3Info value object containing S3 information

        Returns:
            Tuple of (inserts, updates, deletions) counts
        """
        stream = stream_schema_message["stream"]
        stage_table = self.db_sync.table_name(stream, is_stage=True)

        self.logger.info(
            "Fast sync: Loading %s rows from s3://%s/%s into '%s'",
            s3_info.rows_uploaded,
            s3_info.s3_bucket,
            s3_info.s3_path,
            stage_table,
        )

        # Create stage table
        cur.execute(self.db_sync.drop_table_query(is_stage=True))
        cur.execute(self.db_sync.create_table_query(is_stage=True))

        # Build and execute COPY command to load data into stage table
        copy_sql = self._build_copy_sql(
            stage_table,
            columns_with_trans,
            s3_info,
        )
        self.logger.debug("Running COPY query: %s", copy_sql)
        cur.execute(copy_sql)

        # Merge data into target table (includes deletion detection if enabled)
        inserts, updates, deletions = self._merge_data(
            cur,
            stream_schema_message,
            target_table,
            stage_table,
            columns_with_trans,
            s3_info,
        )

        # Drop stage table
        cur.execute(self.db_sync.drop_table_query(is_stage=True))

        return inserts, updates, deletions

    def _cleanup_s3_files(
        self, s3_bucket: str, s3_path: str, files_uploaded: int
    ) -> None:
        try:
            self.s3_client.delete_object(Bucket=s3_bucket, Key=s3_path)
            self.logger.info("Deleted s3://%s/%s", s3_bucket, s3_path)

            if files_uploaded > 1:
                for part_num in range(2, files_uploaded + 1):
                    # aws_s3.query_export_to_s3 creates files as: path, path_part2, path_part3, etc.
                    # Note: The pattern is _part2, _part3, not _part02, _part03
                    if s3_path.endswith(".csv"):
                        part_path = s3_path[:-4] + f"_part{part_num}.csv"
                    else:
                        part_path = f"{s3_path}_part{part_num}.csv"
                    try:
                        self.s3_client.delete_object(Bucket=s3_bucket, Key=part_path)
                        self.logger.info("Deleted s3://%s/%s", s3_bucket, part_path)
                    except Exception as exc:
                        self.logger.warning(
                            "Failed to delete S3 file s3://%s/%s: %s",
                            s3_bucket,
                            part_path,
                            str(exc),
                        )
        except Exception as exc:
            self.logger.warning(
                "Failed to clean up S3 files from bucket %s: %s", s3_bucket, str(exc)
            )

    def load_from_s3(  # pylint: disable=too-many-locals
        self,
        s3_info: FastSyncS3Info,
    ) -> None:
        """
        Load data from S3 using fast sync strategy.

        This method handles two modes:
        1. Append-only mode (append_only=True or no primary keys):
           - Directly copies data from S3 to target table using COPY command
           - No stage table is created
           - No merge or deletion detection is performed
           - More efficient as it bypasses staging and merge operations

        2. Staging table mode (default when primary keys exist):
           - Uses _load_via_staging to load data via a temporary stage table
           - Handles both full refresh (table swap) and incremental merge (UPDATE/INSERT)
           - Detects deletions and sets _SDC_DELETED_AT (if enabled and conditions are met)
           - See _load_via_staging for detailed behavior

        Both modes clean up S3 files after successful processing (if enabled).

        Args:
            s3_info: FastSyncS3Info value object containing S3 information
        """
        stream_schema_message = self.stream_schema_message
        stream = stream_schema_message["stream"]
        target_table = self.db_sync.table_name(stream, is_stage=False)
        has_key_properties = len(stream_schema_message["key_properties"]) > 0

        self.logger.info(
            "Fast sync: Loading data from S3 into '%s'", target_table
        )

        columns_with_trans = [
            {"name": safe_column_name(name), "trans": column_trans(schema)}
            for (name, schema) in self.flatten_schema.items()
        ]

        with self.db_sync.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                inserts = updates = deletions = 0

                # Load parquet file to Iceberg table if file format is parquet
                if s3_info.file_format.lower() == "parquet":
                    stage_table = self.db_sync.table_name(stream, is_stage=True)

                    self.logger.info(
                        "Fast sync: Loading %s rows from s3://%s/%s into %s iceberg table",
                        s3_info.rows_uploaded,
                        s3_info.s3_bucket,
                        s3_info.s3_path,
                        stage_table,
                    )

                    self._load_parquet_to_iceberg(
                        stream_schema_message,
                        columns_with_trans,
                        s3_info,
                        target_table,
                    )

                    self.logger.info("Fast sync: Loaded data into iceberg table")
                else:
                    if self.append_only or not has_key_properties:
                        # Append-only mode: directly copy from S3 to target table
                        inserts = s3_info.rows_uploaded
                        self._append_all_records(
                            cur,
                            target_table,
                            columns_with_trans,
                            s3_info,
                        )
                    else:
                        # Staging table mode: load via staging table and merge
                        inserts, updates, deletions = self._load_via_staging(
                            cur,
                            stream_schema_message,
                            target_table,
                            columns_with_trans,
                            s3_info,
                        )

                if self.cleanup_s3_files:
                    # Clean up S3 files
                    self._cleanup_s3_files(s3_info.s3_bucket, s3_info.s3_path, s3_info.files_uploaded)

                self.logger.info(
                    "Fast sync completed for %s: %s",
                    target_table,
                    json.dumps(
                        {
                            "inserts": inserts,
                            "updates": updates,
                            "deletions": deletions,
                            "rows_loaded": s3_info.rows_uploaded,
                        }
                    ),
                )
