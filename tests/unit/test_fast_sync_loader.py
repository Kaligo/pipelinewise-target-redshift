"""
Unit tests for fast sync functionality in target-redshift
"""

from unittest.mock import MagicMock, patch

from target_redshift import db_sync
from target_redshift.fast_sync.loader import FastSyncLoader


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

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_load_from_s3_basic(self, mock_connect, mock_boto3):
        """Test basic fast sync load from S3"""
        loader, mock_cursor, mock_s3_client = self._create_loader(
            mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=1000,
            files_uploaded=3,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="FULL_TABLE",
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )

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

    @patch("target_redshift.db_sync.boto3")
    @patch("target_redshift.db_sync.psycopg2.connect")
    def test_fast_sync_full_refresh(self, mock_connect, mock_boto3):
        """Test full refresh mode in fast sync"""
        config_full_refresh = self.config.copy()
        config_full_refresh["full_refresh"] = True

        loader, mock_cursor, _ = self._create_loader(
            config=config_full_refresh, mock_connect=mock_connect, mock_boto3=mock_boto3
        )

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )

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
        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="ap-southeast-1",
            rows_uploaded=10,
            files_uploaded=1,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",  # Different region, but should be ignored
            rows_uploaded=10,
            files_uploaded=1,
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="INCREMENTAL",
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="FULL_TABLE",
        )

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

        loader.load_from_s3(
            s3_bucket="source-bucket",
            s3_path="test/path/data.csv",
            s3_region="us-east-1",
            rows_uploaded=100,
            files_uploaded=1,
            replication_method="FULL_TABLE",
        )

        # Verify deletion detection is NOT executed (requires primary keys)
        deletion_calls = self._get_deletion_detection_calls(mock_cursor)
        assert len(deletion_calls) == 0, (
            "Deletion detection should NOT be executed without primary keys"
        )
