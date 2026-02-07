"""
Unit tests for fast sync functionality in target-redshift
"""

from unittest.mock import MagicMock, patch

from target_redshift import db_sync
from target_redshift.fast_sync.loader import FastSyncLoader, FastSyncS3Info


class TestFastSyncLoader:
    """Test Cases for Fast Sync functionality"""

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
            "s3_bucket": "test-bucket",
            "default_target_schema": "test_schema",
            "aws_redshift_copy_role_arn": "arn:aws:iam::123456789012:role/redshift-role",
        }

        self.stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "_SDC_EXTRACTED_AT": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                    "_SDC_BATCHED_AT": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                    "_SDC_DELETED_AT": {"type": ["null", "string"]},
                }
            },
            "key_properties": ["id"],
        }

    def _create_mock_db_connection(self, rowcount=0):
        """Helper to create a mock database connection"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = rowcount
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        return mock_conn, mock_cursor

    def _create_mock_s3_client(self):
        """Helper to create a mock S3 client"""
        mock_s3_client = MagicMock()
        return mock_s3_client

    def _create_s3_info(
        self,
        s3_bucket="source-bucket",
        s3_path="test/path/data.csv",
        s3_region="us-east-1",
        rows_uploaded=100,
        files_uploaded=1,
        replication_method=None,
    ) -> FastSyncS3Info:
        """Helper to create FastSyncS3Info for tests"""
        return FastSyncS3Info(
            s3_bucket=s3_bucket,
            s3_path=s3_path,
            s3_region=s3_region,
            files_uploaded=files_uploaded,
            replication_method=replication_method or "FULL_TABLE",
            rows_uploaded=rows_uploaded,
        )

    def _create_loader(
        self, config=None, schema=None, mock_connect=None, mock_boto3=None
    ):
        """Helper to create FastSyncLoader with mocked dependencies

        Returns:
            If mocks provided: (loader, mock_cursor, mock_s3_client or None)
            If no mocks: loader
        """
        mock_s3_client = None
        if mock_boto3:
            mock_s3_client = self._create_mock_s3_client()
            mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        mock_cursor = None
        if mock_connect:
            mock_conn, mock_cursor = self._create_mock_db_connection()
            mock_connect.return_value = mock_conn

        db = db_sync.DbSync(config or self.config, schema or self.stream_schema_message)
        loader = FastSyncLoader(db)

        if mock_connect or mock_boto3:
            return loader, mock_cursor, mock_s3_client
        return loader

    def _get_sql_calls(self, mock_cursor, sql_keyword):
        """Helper to extract SQL calls containing a keyword"""
        return [
            call
            for call in mock_cursor.execute.call_args_list
            if sql_keyword in str(call)
        ]

    def _get_sql_strings(self, mock_cursor):
        """Helper to extract SQL strings from execute calls"""
        return [
            str(call[0][0]) if call[0] else ""
            for call in mock_cursor.execute.call_args_list
        ]

    def _get_deletion_detection_calls(self, mock_cursor):
        """Helper to extract deletion detection SQL calls"""
        all_calls = self._get_sql_strings(mock_cursor)
        return [
            call_str
            for call_str in all_calls
            if call_str.strip().startswith("UPDATE")
            and "_SDC_DELETED_AT" in call_str
            and "NOT EXISTS" in call_str
        ]

    def _create_s3_info(
        self,
        s3_bucket="source-bucket",
        s3_path="test/path/data.csv",
        s3_region="us-east-1",
        rows_uploaded=100,
        files_uploaded=1,
        replication_method=None,
    ) -> FastSyncS3Info:
        """Helper to create FastSyncS3Info for tests"""
        return FastSyncS3Info(
            s3_bucket=s3_bucket,
            s3_path=s3_path,
            s3_region=s3_region,
            files_uploaded=files_uploaded,
            replication_method=replication_method or "FULL_TABLE",
            rows_uploaded=rows_uploaded,
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_load_from_s3_basic(self, mock_connect, mock_boto3):
        """Test basic fast sync load from S3"""
        loader, mock_cursor, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify essential operations
        assert len(self._get_sql_calls(mock_cursor, "COPY")) > 0, (
            "COPY command should be executed"
        )
        assert len(self._get_sql_calls(mock_cursor, "CREATE TABLE")) > 0, (
            "Stage table should be created"
        )
        assert len(self._get_sql_calls(mock_cursor, "DROP TABLE")) > 0, (
            "Stage table should be dropped"
        )
        # Verify S3 cleanup (enabled by default)
        assert mock_s3_client.delete_object.call_count > 0, (
            "S3 files should be deleted by default"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_with_multiple_files(self, mock_connect, mock_boto3):
        """Test fast sync with multiple S3 files"""
        loader, mock_cursor, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=1000,
            files_uploaded=3,
        )
        loader.load_from_s3(s3_info)

        # Verify S3 cleanup was attempted for multiple files
        assert mock_s3_client.delete_object.call_count >= 3, (
            "Should delete at least 3 files"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_deletion_detection(self, mock_connect, mock_boto3):
        """Test deletion detection in fast sync with FULL_TABLE replication method"""
        config_with_metadata = self.config.copy()
        config_with_metadata["add_metadata_columns"] = True
        config_with_metadata["detect_deletions"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_with_metadata,
            mock_connect=mock_connect,
            mock_boto3=mock_boto3,
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="FULL_TABLE",
        )
        loader.load_from_s3(s3_info)

        # Verify deletion detection query was executed
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) > 0, "Deletion detection query should be executed"

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_insert_update(self, mock_connect, mock_boto3):
        """Test INSERT and UPDATE operations in fast sync"""
        loader, mock_cursor, _ = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify UPDATE and INSERT queries were executed
        all_calls = self._get_sql_strings(mock_cursor)
        update_calls = [
            call_str
            for call_str in all_calls
            if call_str.strip().startswith("UPDATE") and "FROM" in call_str
        ]
        insert_calls = [
            call_str
            for call_str in all_calls
            if "INSERT INTO" in call_str and "SELECT" in call_str
        ]

        assert len(update_calls) > 0, "UPDATE query should be executed"
        assert len(insert_calls) > 0, "INSERT query should be executed"

        # Verify UPDATE query includes native IS DISTINCT FROM condition
        # Pattern: (a IS DISTINCT FROM b)
        update_sql = " ".join(update_calls)
        assert "IS DISTINCT FROM" in update_sql, (
            "UPDATE query should use native IS DISTINCT FROM operator for distinct from condition"
        )
        assert "t." in update_sql, "UPDATE query should alias target table as 't'"

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_full_refresh(self, mock_connect, mock_boto3):
        """Test full refresh mode in fast sync"""
        config_full_refresh = self.config.copy()
        config_full_refresh["full_refresh"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_full_refresh, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify table swap query was executed
        all_calls = self._get_sql_strings(mock_cursor)
        swap_calls = [
            call_str
            for call_str in all_calls
            if "ALTER TABLE" in call_str and "RENAME" in call_str
        ]
        assert len(swap_calls) > 0, (
            "Table swap query should be executed for full refresh"
        )

        # Verify deletion detection is NOT executed for full_refresh
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) == 0, (
            "Deletion detection should NOT be executed for full_refresh"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_s3_cleanup_error_handling(self, mock_connect, mock_boto3):
        """Test that S3 cleanup errors don't fail the operation"""
        mock_s3_client = self._create_mock_s3_client()
        mock_s3_client.delete_object.side_effect = Exception("S3 delete failed")
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        loader, mock_cursor, _ = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        # Should not raise exception even if S3 cleanup fails
        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify COPY command was still executed
        assert len(self._get_sql_calls(mock_cursor, "COPY")) > 0, (
            "COPY command should be executed"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_cleanup_s3_files_disabled(self, mock_connect, mock_boto3):
        """Test that S3 files are not deleted when cleanup_s3_files is False"""
        config_no_cleanup = self.config.copy()
        config_no_cleanup["cleanup_s3_files"] = False

        loader, mock_cursor, mock_s3_client = self._create_loader(
            config=config_no_cleanup, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        assert len(self._get_sql_calls(mock_cursor, "COPY")) > 0, (
            "COPY command should be executed"
        )
        assert mock_s3_client.delete_object.call_count == 0, (
            "S3 files should not be deleted"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_no_primary_key(self, mock_connect, mock_boto3):
        """Test fast sync with table that has no primary key"""
        schema_no_pk = self.stream_schema_message.copy()
        schema_no_pk["key_properties"] = []

        loader, mock_cursor, _ = self._create_loader(
            schema=schema_no_pk, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify INSERT INTO ... SELECT was executed
        all_calls = self._get_sql_strings(mock_cursor)
        insert_calls = [
            call_str
            for call_str in all_calls
            if "INSERT INTO" in call_str and "SELECT" in call_str
        ]
        assert len(insert_calls) > 0, "INSERT INTO ... SELECT should be executed"

        # Verify no incremental UPDATE query
        incremental_update_calls = [
            call_str
            for call_str in all_calls
            if call_str.strip().startswith("UPDATE")
            and "FROM" in call_str
            and "NOT EXISTS" not in call_str
        ]
        assert len(incremental_update_calls) == 0, (
            "Incremental UPDATE should not be executed"
        )

        # Verify deletion detection is NOT executed without primary keys
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) == 0, (
            "Deletion detection should NOT be executed without primary keys"
        )

    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_copy_options_adds_region_when_missing(self, mock_connect):
        """Test that load_from_s3 adds REGION to COPY command when not in copy_options"""
        config_no_region = self.config.copy()
        config_no_region["copy_options"] = "EMPTYASNULL BLANKSASNULL TRIMBLANKS"
        config_no_region["cleanup_s3_files"] = (
            False  # Disable S3 cleanup to avoid needing S3 mocks
        )

        loader, mock_cursor, _ = self._create_loader(
            config=config_no_region, mock_connect=mock_connect, mock_boto3=None
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="ap-southeast-1",
            rows_uploaded=10,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify REGION was added to the COPY command
        copy_calls = self._get_sql_strings(mock_cursor)
        copy_sql = " ".join([call for call in copy_calls if "COPY" in call])
        assert "REGION 'ap-southeast-1'" in copy_sql, (
            "Should add REGION when not present"
        )
        assert copy_sql.count("REGION") == 1, "Should have exactly one REGION clause"
        assert "EMPTYASNULL" in copy_sql, "Should preserve existing copy_options"

    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_copy_options_preserves_existing_region(self, mock_connect):
        """Test that load_from_s3 doesn't duplicate REGION when already in copy_options"""
        config_with_region = self.config.copy()
        config_with_region["copy_options"] = (
            "REGION 'ap-southeast-1' NULL AS 'null' EMPTYASNULL BLANKSASNULL TRIMBLANKS"
        )
        config_with_region["cleanup_s3_files"] = (
            False  # Disable S3 cleanup to avoid needing S3 mocks
        )

        loader, mock_cursor, _ = self._create_loader(
            config=config_with_region, mock_connect=mock_connect, mock_boto3=None
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=10,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify existing REGION is preserved and not duplicated
        copy_calls = self._get_sql_strings(mock_cursor)
        copy_sql = " ".join([call for call in copy_calls if "COPY" in call])
        assert "REGION 'ap-southeast-1'" in copy_sql, "Should preserve existing REGION"
        assert copy_sql.count("REGION") == 1, "Should not add duplicate REGION"
        assert "REGION 'us-east-1'" not in copy_sql, (
            "Should not add new REGION when one already exists"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_deletion_detection_incremental(self, mock_connect, mock_boto3):
        """Test that deletion detection is NOT executed for incremental updates"""
        config_with_metadata = self.config.copy()
        config_with_metadata["add_metadata_columns"] = True
        config_with_metadata["detect_deletions"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_with_metadata,
            mock_connect=mock_connect,
            mock_boto3=mock_boto3,
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="INCREMENTAL",
        )
        loader.load_from_s3(s3_info)

        # Verify deletion detection query was NOT executed
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) == 0, (
            "Deletion detection should NOT be executed for incremental updates"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_deletion_detection_full_table_append_only(
        self, mock_connect, mock_boto3
    ):
        """Test deletion detection with FULL_TABLE replication and append_only mode"""
        config_append = self.config.copy()
        config_append["append_only"] = True
        config_append["add_metadata_columns"] = True
        config_append["detect_deletions"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_append, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="FULL_TABLE",
        )
        loader.load_from_s3(s3_info)

        # Verify INSERT INTO ... SELECT was executed
        all_calls = self._get_sql_strings(mock_cursor)
        insert_calls = [
            call_str
            for call_str in all_calls
            if "INSERT INTO" in call_str and "SELECT" in call_str
        ]
        assert len(insert_calls) > 0, (
            "INSERT INTO ... SELECT should be executed for append_only mode"
        )

        # Verify deletion detection query was executed
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) > 0, (
            "Deletion detection should be executed for FULL_TABLE with append_only"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_deletion_detection_no_primary_key_full_table(
        self, mock_connect, mock_boto3
    ):
        """Test that deletion detection is NOT executed without primary keys, even with FULL_TABLE"""
        schema_no_pk = self.stream_schema_message.copy()
        schema_no_pk["key_properties"] = []

        config_with_metadata = self.config.copy()
        config_with_metadata["add_metadata_columns"] = True
        config_with_metadata["detect_deletions"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_with_metadata,
            schema=schema_no_pk,
            mock_connect=mock_connect,
            mock_boto3=mock_boto3,
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="FULL_TABLE",
        )
        loader.load_from_s3(s3_info)

        # Verify deletion detection is NOT executed (requires primary keys)
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) == 0, (
            "Deletion detection should NOT be executed without primary keys"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_insert_all_records_with_primary_key(
        self, mock_connect, mock_boto3
    ):
        """Test that _insert_all_records uses NOT EXISTS to avoid duplicates when primary keys exist"""
        config_append = self.config.copy()
        config_append["append_only"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_append, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify INSERT INTO ... SELECT was executed
        all_calls = self._get_sql_strings(mock_cursor)
        insert_calls = [
            call_str
            for call_str in all_calls
            if "INSERT INTO" in call_str
            and "SELECT" in call_str
            and "NOT EXISTS" in call_str
        ]

        assert len(insert_calls) > 0, (
            "INSERT INTO ... SELECT with NOT EXISTS should be executed for append_only with primary keys"
        )

        # Verify INSERT query includes native IS DISTINCT FROM condition
        # Pattern: (a IS DISTINCT FROM b)
        insert_sql = " ".join(insert_calls)
        assert "IS DISTINCT FROM" in insert_sql, (
            "INSERT query should use native IS DISTINCT FROM operator for distinct from condition"
        )
        assert "NOT EXISTS" in insert_sql, (
            "INSERT query should use NOT EXISTS when primary keys exist"
        )
        assert "LEFT JOIN" not in insert_sql, (
            "INSERT query should NOT use LEFT JOIN to avoid duplicates"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_insert_all_records_with_primary_key_skip_unchanged_rows_false(
        self, mock_connect, mock_boto3
    ):
        """Test that _insert_all_records doesn't use distinct from condition when skip_unchanged_rows is False"""
        config_append = self.config.copy()
        config_append["append_only"] = True
        config_append["skip_unchanged_rows"] = False

        loader, mock_cursor, _ = self._create_loader(
            config=config_append, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify INSERT INTO ... SELECT was executed
        all_calls = self._get_sql_strings(mock_cursor)
        insert_calls = [
            call_str
            for call_str in all_calls
            if "INSERT INTO" in call_str
            and "SELECT" in call_str
            and "NOT EXISTS" in call_str
        ]

        assert len(insert_calls) > 0, (
            "INSERT INTO ... SELECT with NOT EXISTS should be executed for append_only with primary keys"
        )

        # Verify INSERT query does NOT include distinct from pattern when disabled
        # Pattern: NOT (a = b OR (a IS NULL AND b IS NULL))
        insert_sql = " ".join(insert_calls)
        # When skip_unchanged_rows is False, we should only have primary key condition in NOT EXISTS
        # Check that the distinct from pattern is NOT present
        has_distinct_pattern = (
            "NOT (" in insert_sql
            and "=" in insert_sql
            and "IS NULL" in insert_sql
            and "AND" in insert_sql
            and "OR" in insert_sql
        )
        assert not has_distinct_pattern, (
            "INSERT query should NOT include distinct from condition pattern when skip_unchanged_rows is False"
        )
        assert "NOT EXISTS" in insert_sql, (
            "INSERT query should still use NOT EXISTS when primary keys exist"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_insert_all_records_no_primary_key(
        self, mock_connect, mock_boto3
    ):
        """Test that _insert_all_records does not include LEFT JOIN when no primary keys exist"""
        schema_no_pk = self.stream_schema_message.copy()
        schema_no_pk["key_properties"] = []

        config_append = self.config.copy()
        config_append["append_only"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_append,
            schema=schema_no_pk,
            mock_connect=mock_connect,
            mock_boto3=mock_boto3,
        )

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify INSERT INTO ... SELECT was executed without LEFT JOIN
        all_calls = self._get_sql_strings(mock_cursor)
        insert_calls = [
            call_str
            for call_str in all_calls
            if "INSERT INTO" in call_str and "SELECT" in call_str
        ]

        assert len(insert_calls) > 0, "INSERT INTO ... SELECT should be executed"

        # Verify INSERT query does NOT include LEFT JOIN when no primary keys
        insert_sql = " ".join(insert_calls)
        assert "LEFT JOIN" not in insert_sql, (
            "INSERT query should NOT include LEFT JOIN when no primary keys exist"
        )
        # Should not have the distinct from pattern when no primary keys
        # Pattern: NOT (a = b OR (a IS NULL AND b IS NULL))
        has_distinct_pattern = (
            "NOT (" in insert_sql
            and "=" in insert_sql
            and "IS NULL" in insert_sql
            and "AND" in insert_sql
        )
        assert not has_distinct_pattern, (
            "INSERT query should NOT include distinct from condition when no primary keys exist"
        )
        # Should not have NOT EXISTS when no primary keys
        assert "NOT EXISTS" not in insert_sql, (
            "INSERT query should NOT use NOT EXISTS when no primary keys exist"
        )

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_result_logging(self, mock_connect, mock_boto3):
        """Test that result_info is logged correctly after fast sync completion"""
        loader, mock_cursor, _ = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        # Mock the logger.info method
        mock_logger_info = MagicMock()
        loader.logger.info = mock_logger_info

        # Mock rowcount for inserts, updates, deletions
        mock_cursor.rowcount = 10

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify logger.info was called with the correct format
        assert mock_logger_info.called, "logger.info should be called"

        # Get the call arguments - find the call with "Fast sync completed for"
        calls = [
            call
            for call in mock_logger_info.call_args_list
            if len(call[0]) > 0 and "Fast sync completed for" in str(call[0][0])
        ]
        assert len(calls) > 0, "Should have logged 'Fast sync completed for' message"

        call_args = calls[0][0]
        assert len(call_args) >= 3, (
            "Log call should have at least 3 arguments (format, table_name, json)"
        )

        # Check the log message format (format string with %s placeholders)
        log_message = call_args[0]
        assert "Fast sync completed for" in log_message, (
            "Log message should contain 'Fast sync completed for'"
        )
        assert "%s" in log_message, "Log message should use %s format"

        # Check that result_info is passed as JSON string (third argument)
        import json

        # The format is: "Fast sync completed for %s: %s" with table_name and json.dumps(result_info)
        # So call_args[1] is the table name, call_args[2] is the JSON string
        result_info_str = call_args[2]
        result_info = json.loads(result_info_str)

        # Verify result_info structure
        assert "inserts" in result_info, "result_info should contain 'inserts'"
        assert "updates" in result_info, "result_info should contain 'updates'"
        assert "deletions" in result_info, "result_info should contain 'deletions'"
        assert "rows_loaded" in result_info, "result_info should contain 'rows_loaded'"
        assert result_info["rows_loaded"] == 100, (
            "rows_loaded should match rows_uploaded"
        )

        # Check that unchanged_rows is included when skip_unchanged_rows is True (default)
        if loader.db_sync.skip_unchanged_rows:
            assert "unchanged_rows" in result_info, (
                "result_info should contain 'unchanged_rows' when skip_unchanged_rows is True"
            )
            assert (
                result_info["unchanged_rows"]
                == 100 - result_info["inserts"] - result_info["updates"]
            ), "unchanged_rows should be calculated correctly"

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_result_logging_skip_unchanged_rows_false(
        self, mock_connect, mock_boto3
    ):
        """Test that unchanged_rows is not included when skip_unchanged_rows is False"""
        config_no_skip = self.config.copy()
        config_no_skip["skip_unchanged_rows"] = False

        loader, mock_cursor, _ = self._create_loader(
            config=config_no_skip, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        # Mock the logger.info method
        mock_logger_info = MagicMock()
        loader.logger.info = mock_logger_info

        # Mock rowcount for inserts, updates, deletions
        mock_cursor.rowcount = 10

        s3_info = self._create_s3_info(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )
        loader.load_from_s3(s3_info)

        # Verify logger.info was called
        assert mock_logger_info.called, "logger.info should be called"

        # Get the call arguments - find the call with "Fast sync completed for"
        calls = [
            call
            for call in mock_logger_info.call_args_list
            if len(call[0]) > 0 and "Fast sync completed for" in str(call[0][0])
        ]
        assert len(calls) > 0, "Should have logged 'Fast sync completed for' message"

        call_args = calls[0][0]
        assert len(call_args) >= 3, (
            "Log call should have at least 3 arguments (format, table_name, json)"
        )
        import json

        # The format is: "Fast sync completed for %s: %s" with table_name and json.dumps(result_info)
        # So call_args[1] is the table name, call_args[2] is the JSON string
        result_info_str = call_args[2]
        result_info = json.loads(result_info_str)

        # Check that unchanged_rows is NOT included when skip_unchanged_rows is False
        assert "unchanged_rows" not in result_info, (
            "result_info should NOT contain 'unchanged_rows' when skip_unchanged_rows is False"
        )

    def test_build_s3_copy_path_single_file(self):
        """Test building S3 copy path for single file"""
        loader = self._create_loader()
        s3_info = self._create_s3_info(s3_path="test/path/data.csv", files_uploaded=1)

        path = loader._build_s3_copy_path(s3_info)

        assert path == "s3://source-bucket/test/path/data.csv"

    def test_build_s3_copy_path_multiple_files(self):
        """Test building S3 copy path for multiple files"""
        loader = self._create_loader()
        s3_info = self._create_s3_info(s3_path="test/path/data.csv", files_uploaded=3)

        path = loader._build_s3_copy_path(s3_info)

        assert path == "s3://source-bucket/test/path/data"

    def test_build_s3_copy_path_multiple_files_no_csv_extension(self):
        """Test building S3 copy path for multiple files without .csv extension"""
        loader = self._create_loader()
        s3_info = self._create_s3_info(s3_path="test/path/data", files_uploaded=3)

        path = loader._build_s3_copy_path(s3_info)

        assert path == "s3://source-bucket/test/path/data"

    def test_build_copy_sql(self):
        """Test building COPY SQL command"""
        loader = self._create_loader()
        s3_info = self._create_s3_info(
            s3_bucket="test-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
        )

        columns_with_trans = [
            {"name": '"ID"', "trans": ""},
            {"name": '"NAME"', "trans": ""},
        ]

        copy_sql = loader._build_copy_sql("test_table", columns_with_trans, s3_info)

        assert "COPY" in copy_sql
        assert "test_table" in copy_sql
        assert "ID" in copy_sql
        assert "NAME" in copy_sql
        assert "s3://test-bucket" in copy_sql
        assert "CSV" in copy_sql

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_detect_deletions_with_metadata_columns(self, mock_connect, mock_boto3):
        """Test deletion detection with metadata columns"""
        config_with_metadata = self.config.copy()
        config_with_metadata["add_metadata_columns"] = True
        config_with_metadata["detect_deletions"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_with_metadata,
            mock_connect=mock_connect,
            mock_boto3=mock_boto3,
        )

        columns_with_trans = [
            {"name": '"ID"', "trans": ""},
            {"name": '"_SDC_DELETED_AT"', "trans": ""},
        ]

        loader._detect_deletions(
            mock_cursor, "target_table", "stage_table", columns_with_trans
        )

        assert mock_cursor.execute.called
        call_args = mock_cursor.execute.call_args[0][0]
        assert "UPDATE" in call_args
        assert "_SDC_DELETED_AT" in call_args
        assert "NOT EXISTS" in call_args

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_detect_deletions_without_metadata_columns(self, mock_connect, mock_boto3):
        """Test deletion detection without metadata columns returns 0"""
        config_with_metadata = self.config.copy()
        config_with_metadata["add_metadata_columns"] = True
        config_with_metadata["detect_deletions"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_with_metadata,
            mock_connect=mock_connect,
            mock_boto3=mock_boto3,
        )

        columns_with_trans = [
            {"name": '"ID"', "trans": ""},
            {"name": '"NAME"', "trans": ""},
        ]

        deletions = loader._detect_deletions(
            mock_cursor, "target_table", "stage_table", columns_with_trans
        )

        assert deletions == 0
        assert not mock_cursor.execute.called

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_cleanup_s3_files_single_file(self, mock_connect, mock_boto3):
        """Test S3 cleanup for single file"""
        loader, _, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="test-bucket",
            s3_path="test/path/data.csv",
            files_uploaded=1,
        )

        loader._cleanup_s3_files(s3_info)

        assert mock_s3_client.delete_object.call_count == 1
        call_args = mock_s3_client.delete_object.call_args
        assert call_args[1]["Bucket"] == "test-bucket"
        assert call_args[1]["Key"] == "test/path/data.csv"

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_cleanup_s3_files_multiple_files(self, mock_connect, mock_boto3):
        """Test S3 cleanup for multiple files"""
        loader, _, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        s3_info = self._create_s3_info(
            s3_bucket="test-bucket",
            s3_path="test/path/data.csv",
            files_uploaded=3,
        )

        loader._cleanup_s3_files(s3_info)

        assert mock_s3_client.delete_object.call_count == 3
        calls = mock_s3_client.delete_object.call_args_list
        assert calls[0][1]["Key"] == "test/path/data.csv"
        assert calls[1][1]["Key"] == "test/path/data_part2.csv"
        assert calls[2][1]["Key"] == "test/path/data_part3.csv"

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_cleanup_s3_files_error_handling(self, mock_connect, mock_boto3):
        """Test S3 cleanup error handling"""
        loader, _, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        mock_s3_client.delete_object.side_effect = Exception("S3 error")

        s3_info = self._create_s3_info(
            s3_bucket="test-bucket",
            s3_path="test/path/data.csv",
            files_uploaded=1,
        )

        # Should not raise exception
        loader._cleanup_s3_files(s3_info)

        assert mock_s3_client.delete_object.called

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_cleanup_s3_files_partial_error(self, mock_connect, mock_boto3):
        """Test S3 cleanup with partial errors on multiple files"""
        loader, _, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        # First call succeeds, second fails, third succeeds
        def side_effect(*args, **kwargs):
            if mock_s3_client.delete_object.call_count == 2:
                raise Exception("S3 error")
            return None

        mock_s3_client.delete_object.side_effect = side_effect

        s3_info = self._create_s3_info(
            s3_bucket="test-bucket",
            s3_path="test/path/data.csv",
            files_uploaded=3,
        )

        # Should not raise exception even if some files fail
        loader._cleanup_s3_files(s3_info)

        assert mock_s3_client.delete_object.call_count == 3

    def test_fast_sync_s3_info_from_message(self):
        """Test creating FastSyncS3Info from message dictionary"""
        message = {
            "s3_bucket": "test-bucket",
            "s3_path": "test/path/data.csv",
            "s3_region": "us-east-1",
            "files_uploaded": 2,
            "replication_method": "INCREMENTAL",
            "rows_uploaded": 100,
        }

        s3_info = FastSyncS3Info.from_message(message)

        assert s3_info.s3_bucket == "test-bucket"
        assert s3_info.s3_path == "test/path/data.csv"
        assert s3_info.s3_region == "us-east-1"
        assert s3_info.files_uploaded == 2
        assert s3_info.replication_method == "INCREMENTAL"
        assert s3_info.rows_uploaded == 100

    def test_fast_sync_s3_info_from_message_without_rows_uploaded(self):
        """Test creating FastSyncS3Info from message without rows_uploaded"""
        message = {
            "s3_bucket": "test-bucket",
            "s3_path": "test/path/data.csv",
            "s3_region": "us-east-1",
            "files_uploaded": 1,
            "replication_method": "FULL_TABLE",
        }

        s3_info = FastSyncS3Info.from_message(message)

        assert s3_info.rows_uploaded == 0  # Default value
