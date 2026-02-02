"""
Fast Sync Loader for target-redshift

This module handles loading data from S3 into Redshift using the fast sync strategy.
It extracts data loading logic from DbSync to provide a cleaner separation of concerns.
"""

import json
from dataclasses import dataclass
from typing import Dict, List, Tuple, Any
import psycopg2.extras

from target_redshift.db_sync import safe_column_name


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
        self.s3_client = db_sync.s3
        self.full_refresh = db_sync.full_refresh
        self.detect_deletions = db_sync.connection_config.get("detect_deletions", False)
        self.cleanup_s3_files = db_sync.connection_config.get("cleanup_s3_files", True)
        self.stream_schema_message = db_sync.stream_schema_message

    @staticmethod
    def _build_s3_copy_path(s3_info: FastSyncS3Info) -> str:
        s3_copy_path = f"s3://{s3_info.s3_bucket}/{s3_info.s3_path}"
        if s3_info.files_uploaded > 1 and s3_copy_path.endswith(".csv"):
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
        copy_credentials = self.db_sync.build_copy_credentials()
        copy_options = self.db_sync.build_copy_options(s3_info.s3_region)

        return f"""COPY {table_name} ({column_names})
            FROM '{s3_copy_path}'
            {copy_credentials}
            {copy_options}
            CSV
        """.strip()

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
        deletion_sql = f"""UPDATE {target_table} t
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

    def _merge_data(
        self,
        cur: Any,
        target_table: str,
        stage_table: str,
        columns_with_trans: List[Dict[str, str]],
        s3_info: FastSyncS3Info,
    ) -> Tuple[int, int, int]:
        """
        Merge data from stage table into target table

        Args:
            cur: Database cursor
            target_table: Target table name (already sanitized)
            stage_table: Stage table name (already sanitized)
            columns_with_trans: List of column definitions
            s3_info: FastSyncS3Info value object containing S3 information

        Returns:
            Tuple of (inserts, updates, deletions) counts
        """
        inserts, updates = self.db_sync.merge_data_from_stage(
            cur,
            target_table,
            stage_table,
            columns_with_trans,
            stream=self.stream_schema_message["stream"],
            rows_uploaded=s3_info.rows_uploaded,
        )

        deletions = 0

        if (
            self.detect_deletions
            and s3_info.replication_method == "FULL_TABLE"
            and self.db_sync.has_key_properties()
            and not self.full_refresh
        ):
            deletions = self._detect_deletions(
                cur, target_table, stage_table, columns_with_trans
            )

        return inserts, updates, deletions

    def _cleanup_s3_files(self, s3_info: FastSyncS3Info) -> None:
        try:
            self.s3_client.delete_object(Bucket=s3_info.s3_bucket, Key=s3_info.s3_path)
            self.logger.info("Deleted s3://%s/%s", s3_info.s3_bucket, s3_info.s3_path)

            if s3_info.files_uploaded > 1:
                for part_num in range(2, s3_info.files_uploaded + 1):
                    if s3_info.s3_path.endswith(".csv"):
                        part_path = s3_info.s3_path[:-4] + f"_part{part_num}.csv"
                    else:
                        part_path = f"{s3_info.s3_path}_part{part_num}.csv"
                    try:
                        self.s3_client.delete_object(
                            Bucket=s3_info.s3_bucket, Key=part_path
                        )
                        self.logger.info(
                            "Deleted s3://%s/%s", s3_info.s3_bucket, part_path
                        )
                    except Exception as exc:
                        self.logger.warning(
                            "Failed to delete S3 file s3://%s/%s: %s",
                            s3_info.s3_bucket,
                            part_path,
                            str(exc),
                        )
        except Exception as exc:
            self.logger.warning(
                "Failed to clean up S3 files from bucket %s: %s",
                s3_info.s3_bucket,
                str(exc),
            )

    def load_from_s3(  # pylint: disable=too-many-locals
        self,
        s3_info: FastSyncS3Info,
    ) -> None:
        """
        Load data from S3 using fast sync strategy.

        This method:
        1. Creates a temporary table
        2. Loads data from S3 using COPY command
        3. Merges data into target table (INSERT/UPDATE/APPEND)
        4. Detects deletions and sets _SDC_DELETED_AT
        5. Cleans up temporary table and S3 files

        Args:
            s3_info: FastSyncS3Info value object containing S3 information
        """
        stream = self.stream_schema_message["stream"]
        stage_table = self.db_sync.table_name(stream, is_stage=True)
        target_table = self.db_sync.table_name(stream, is_stage=False)

        self.logger.info(
            "Fast sync: Loading %s rows from s3://%s/%s into '%s'",
            s3_info.rows_uploaded,
            s3_info.s3_bucket,
            s3_info.s3_path,
            stage_table,
        )

        columns_with_trans = self.db_sync.get_columns_with_trans()

        with self.db_sync.open_connection() as connection:
            with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute(self.db_sync.drop_table_query(is_stage=True))
                cur.execute(self.db_sync.create_table_query(is_stage=True))
                copy_sql = self._build_copy_sql(
                    stage_table,
                    columns_with_trans,
                    s3_info,
                )
                self.logger.debug("Running COPY query: %s", copy_sql)
                cur.execute(copy_sql)

                inserts, updates, deletions = self._merge_data(
                    cur,
                    target_table,
                    stage_table,
                    columns_with_trans,
                    s3_info,
                )

                cur.execute(self.db_sync.drop_table_query(is_stage=True))

                if self.cleanup_s3_files:
                    self._cleanup_s3_files(s3_info)

                result_info = {
                    "inserts": inserts,
                    "updates": updates,
                    "deletions": deletions,
                    "rows_loaded": s3_info.rows_uploaded,
                }

                self.logger.info(
                    "Fast sync completed for %s: %s",
                    self.db_sync.table_name(stream, False),
                    json.dumps(result_info),
                )
                self.db_sync.metrics.data_sync_gauge(result_info, stream)
                self.db_sync.metrics.data_sync_incremental(result_info, stream)
