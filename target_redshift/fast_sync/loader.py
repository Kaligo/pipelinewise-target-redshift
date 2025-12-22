"""
Fast Sync Loader for target-redshift

This module handles loading data from S3 into Redshift using the fast sync strategy.
It extracts data loading logic from DbSync to provide a cleaner separation of concerns.
"""

import json
import re
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any
import psycopg2.extras

from target_redshift.db_sync import safe_column_name, column_trans, primary_column_names


@dataclass
class FastSyncS3Info:
    """Value object representing fast sync S3 information from STATE messages."""

    s3_bucket: str
    s3_path: str
    s3_region: str
    files_uploaded: int
    replication_method: str
    rows_uploaded: int = 0

    @classmethod
    def from_message(cls, message: Dict[str, Any]) -> "FastSyncS3Info":
        """Create FastSyncS3Info from a message dictionary."""
        return cls(
            s3_bucket=message["s3_bucket"],
            s3_path=message["s3_path"],
            s3_region=message["s3_region"],
            files_uploaded=message["files_uploaded"],
            replication_method=message["replication_method"],
            rows_uploaded=message.get("rows_uploaded", 0),
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

    def _build_copy_options(self, s3_region: str) -> str:
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
        if s3_info.files_uploaded > 1 and s3_copy_path.endswith(".csv"):
            # For multiple files, use prefix pattern.
            s3_copy_path = s3_copy_path[:-4]
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
        copy_options = self._build_copy_options(s3_info.s3_region)

        return f"""COPY {table_name} ({column_names})
            FROM '{s3_copy_path}'
            {copy_credentials}
            {copy_options}
            CSV
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
