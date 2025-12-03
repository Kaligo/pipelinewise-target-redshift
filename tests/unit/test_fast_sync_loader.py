"""
Unit tests for fast sync functionality in target-redshift
"""
import pytest
from unittest.mock import MagicMock, patch

import target_redshift.db_sync as db_sync
from target_redshift.fast_sync.loader import FastSyncLoader


class TestFastSyncLoader:
    """Test Cases for Fast Sync functionality"""

    def setup_method(self):
        """Set up test fixtures"""
        self.config = {
            'host': 'test-host.redshift.amazonaws.com',
            'port': 5439,
            'user': 'test_user',
            'password': 'test_password',
            'dbname': 'test_db',
            'aws_access_key_id': 'test_key',
            'aws_secret_access_key': 'test_secret',
            's3_bucket': 'test-bucket',
            'default_target_schema': 'test_schema',
            'aws_redshift_copy_role_arn': 'arn:aws:iam::123456789012:role/redshift-role'
        }

        self.stream_schema_message = {
            'stream': 'test_schema-test_table',
            'schema': {
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'name': {'type': ['null', 'string']},
                    '_SDC_EXTRACTED_AT': {'type': ['null', 'string'], 'format': 'date-time'},
                    '_SDC_BATCHED_AT': {'type': ['null', 'string'], 'format': 'date-time'},
                    '_SDC_DELETED_AT': {'type': ['null', 'string']}
                }
            },
            'key_properties': ['id']
        }

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_load_from_s3_basic(self, mock_connect, mock_boto3):
        """Test basic fast sync load from S3"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # For UPDATE and INSERT operations
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1
        )

        # Verify COPY command was executed
        copy_calls = [call for call in mock_cursor.execute.call_args_list
                     if 'COPY' in str(call)]
        assert len(copy_calls) > 0, "COPY command should be executed"

        # Verify stage table was created
        create_calls = [call for call in mock_cursor.execute.call_args_list
                       if 'CREATE TABLE' in str(call)]
        assert len(create_calls) > 0, "Stage table should be created"

        # Verify stage table was dropped
        drop_calls = [call for call in mock_cursor.execute.call_args_list
                     if 'DROP TABLE' in str(call)]
        assert len(drop_calls) > 0, "Stage table should be dropped"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_with_multiple_files(self, mock_connect, mock_boto3):
        """Test fast sync with multiple S3 files"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # For UPDATE and INSERT operations
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3 with multiple files
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=1000,
            files_uploaded=3
        )

        # Verify S3 cleanup was attempted for multiple files
        delete_calls = mock_s3_client.delete_object.call_count
        assert delete_calls >= 3, "Should delete at least 3 files (main + 2 parts)"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_deletion_detection(self, mock_connect, mock_boto3):
        """Test deletion detection in fast sync with FULL_TABLE replication method and primary key"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 5  # Simulate 5 deleted rows
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance with metadata columns and detect_deletions enabled
        # Note: Deletion detection requires primary keys, so we use the default schema with primary key
        config_with_metadata = self.config.copy()
        config_with_metadata['add_metadata_columns'] = True
        config_with_metadata['detect_deletions'] = True
        db = db_sync.DbSync(config_with_metadata, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3 with FULL_TABLE replication method
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1,
            replication_method='FULL_TABLE'
        )

        # Verify deletion detection query was executed
        # Check for UPDATE query with _SDC_DELETED_AT and NOT EXISTS (deletion detection pattern)
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        deletion_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and '_SDC_DELETED_AT' in call_str and 'NOT EXISTS' in call_str
        ]
        assert len(deletion_calls) > 0, "Deletion detection query should be executed for FULL_TABLE replication with primary key"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_insert_update(self, mock_connect, mock_boto3):
        """Test INSERT and UPDATE operations in fast sync"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 10  # Simulate 10 affected rows for UPDATE
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1
        )

        # Verify UPDATE query was executed
        update_calls = [call for call in mock_cursor.execute.call_args_list
                       if 'UPDATE' in str(call) and 'FROM' in str(call)]
        assert len(update_calls) > 0, "UPDATE query should be executed"

        # Verify INSERT query was executed
        insert_calls = [call for call in mock_cursor.execute.call_args_list
                       if 'INSERT INTO' in str(call) and 'SELECT' in str(call)]
        assert len(insert_calls) > 0, "INSERT query should be executed"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_full_refresh(self, mock_connect, mock_boto3):
        """Test full refresh mode in fast sync"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance with full_refresh enabled
        config_full_refresh = self.config.copy()
        config_full_refresh['full_refresh'] = True
        db = db_sync.DbSync(config_full_refresh, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1
        )

        # Verify table swap query was executed (full refresh)
        swap_calls = [call for call in mock_cursor.execute.call_args_list
                     if 'ALTER TABLE' in str(call) and 'RENAME' in str(call)]
        assert len(swap_calls) > 0, "Table swap query should be executed for full refresh"

        # Verify deletion detection is NOT executed for full_refresh
        # Check for UPDATE query with _SDC_DELETED_AT and NOT EXISTS (deletion detection pattern)
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        deletion_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and '_SDC_DELETED_AT' in call_str and 'NOT EXISTS' in call_str
        ]
        assert len(deletion_calls) == 0, "Deletion detection should NOT be executed for full_refresh"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_s3_cleanup_error_handling(self, mock_connect, mock_boto3):
        """Test that S3 cleanup errors don't fail the operation"""
        # Mock S3 client that raises an error on delete
        mock_s3_client = MagicMock()
        mock_s3_client.delete_object.side_effect = Exception("S3 delete failed")
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # For UPDATE and INSERT operations
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3 - should not raise exception even if S3 cleanup fails
        try:
            loader.load_from_s3(
                s3_bucket='source-bucket',
                s3_path='test/path/data.csv',
                s3_region='us-east-1',
                rows_uploaded=100,
                files_uploaded=1
            )
        except Exception as e:
            pytest.fail(f"load_from_s3 should not raise exception on S3 cleanup failure: {e}")

        # Verify COPY command was still executed despite S3 cleanup failure
        copy_calls = [call for call in mock_cursor.execute.call_args_list
                     if 'COPY' in str(call)]
        assert len(copy_calls) > 0, "COPY command should be executed even if S3 cleanup fails"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_cleanup_s3_files_disabled(self, mock_connect, mock_boto3):
        """Test that S3 files are not deleted when cleanup_s3_files is False"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # For UPDATE and INSERT operations
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create config with cleanup_s3_files disabled
        config_no_cleanup = self.config.copy()
        config_no_cleanup['cleanup_s3_files'] = False

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(config_no_cleanup, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1
        )

        # Verify COPY command was executed
        copy_calls = [call for call in mock_cursor.execute.call_args_list
                     if 'COPY' in str(call)]
        assert len(copy_calls) > 0, "COPY command should be executed"

        # Verify S3 files were NOT deleted
        assert mock_s3_client.delete_object.call_count == 0, "S3 files should not be deleted when cleanup_s3_files is False"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_cleanup_s3_files_enabled(self, mock_connect, mock_boto3):
        """Test that S3 files are deleted when cleanup_s3_files is True (default)"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0  # For UPDATE and INSERT operations
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader (cleanup_s3_files defaults to True)
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1
        )

        # Verify COPY command was executed
        copy_calls = [call for call in mock_cursor.execute.call_args_list
                     if 'COPY' in str(call)]
        assert len(copy_calls) > 0, "COPY command should be executed"

        # Verify S3 files were deleted (default behavior)
        assert mock_s3_client.delete_object.call_count > 0, "S3 files should be deleted when cleanup_s3_files is True (default)"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_no_primary_key(self, mock_connect, mock_boto3):
        """Test fast sync with table that has no primary key (uses INSERT INTO ... SELECT)"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 50  # Simulate 50 inserted rows
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create schema message without primary key
        schema_no_pk = self.stream_schema_message.copy()
        schema_no_pk['key_properties'] = []

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, schema_no_pk)
        loader = FastSyncLoader(db)

        # Call load_from_s3
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1
        )

        # Verify INSERT INTO ... SELECT was executed (for tables without PK)
        insert_calls = [call for call in mock_cursor.execute.call_args_list
                       if 'INSERT INTO' in str(call) and 'SELECT' in str(call)]
        assert len(insert_calls) > 0, "INSERT INTO ... SELECT should be executed for tables without primary key"

        # Verify no UPDATE query for incremental updates was executed
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        incremental_update_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and 'FROM' in call_str and 'NOT EXISTS' not in call_str
        ]
        assert len(incremental_update_calls) == 0, "Incremental UPDATE query should not be executed for tables without primary key"

        # Verify deletion detection is NOT executed without primary keys (even with FULL_TABLE)
        # Deletion detection requires primary keys, so it should not run for tables without PK
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        deletion_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and '_SDC_DELETED_AT' in call_str and 'NOT EXISTS' in call_str
        ]
        assert len(deletion_calls) == 0, "Deletion detection should NOT be executed without primary keys"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_build_s3_copy_path_multiple_files(self, mock_connect, mock_boto3):
        """Test S3 copy path building for multiple files"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Test single file
        path = loader._build_s3_copy_path('bucket', 'path/data.csv', 1)
        assert path == 's3://bucket/path/data.csv', "Single file should not have wildcard"

        # Test multiple files
        path = loader._build_s3_copy_path('bucket', 'path/data.csv', 3)
        assert path == 's3://bucket/path/data', "Multiple files should use prefix without .csv extension (no wildcard needed)"

        # Test multiple files without .csv extension
        path = loader._build_s3_copy_path('bucket', 'path/data', 2)
        assert path == 's3://bucket/path/data', "Multiple files without extension should use prefix as-is (no wildcard needed)"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_build_copy_credentials_iam_role(self, mock_connect, mock_boto3):
        """Test COPY credentials building with IAM role"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Test IAM role credentials
        credentials = loader._build_copy_credentials()
        assert 'IAM_ROLE' in credentials, "Should use IAM role when configured"
        assert 'arn:aws:iam::123456789012:role/redshift-role' in credentials

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_build_copy_credentials_access_keys(self, mock_connect, mock_boto3):
        """Test COPY credentials building with access keys"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create config without IAM role
        config_no_role = self.config.copy()
        del config_no_role['aws_redshift_copy_role_arn']

        # Create DbSync instance and FastSyncLoader
        db = db_sync.DbSync(config_no_role, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Test access key credentials
        credentials = loader._build_copy_credentials()
        assert 'ACCESS_KEY_ID' in credentials, "Should use access keys when IAM role not configured"
        assert 'SECRET_ACCESS_KEY' in credentials
        # Note: The actual key value comes from boto3 session which is mocked, so we just verify the structure

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_deletion_detection_with_primary_key(self, mock_connect, mock_boto3):
        """Test that deletion detection is NOT executed for incremental updates (tables with primary key)"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 10  # For UPDATE and INSERT operations
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance with metadata columns enabled and primary key
        config_with_metadata = self.config.copy()
        config_with_metadata['add_metadata_columns'] = True
        config_with_metadata['detect_deletions'] = True
        db = db_sync.DbSync(config_with_metadata, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3 with INCREMENTAL replication (default for tables with PK)
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1,
            replication_method='INCREMENTAL'
        )

        # Verify deletion detection query was NOT executed (incremental update doesn't support deletion detection)
        # Check for UPDATE query with _SDC_DELETED_AT and NOT EXISTS (deletion detection pattern)
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        deletion_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and '_SDC_DELETED_AT' in call_str and 'NOT EXISTS' in call_str
        ]
        assert len(deletion_calls) == 0, "Deletion detection should NOT be executed for incremental updates"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_deletion_detection_full_table_append_only(self, mock_connect, mock_boto3):
        """Test deletion detection with FULL_TABLE replication and append_only mode"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 3  # Simulate 3 deleted rows
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create DbSync instance with append_only and detect_deletions enabled
        config_append = self.config.copy()
        config_append['append_only'] = True
        config_append['add_metadata_columns'] = True
        config_append['detect_deletions'] = True
        db = db_sync.DbSync(config_append, self.stream_schema_message)
        loader = FastSyncLoader(db)

        # Call load_from_s3 with FULL_TABLE replication method
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1,
            replication_method='FULL_TABLE'
        )

        # Verify INSERT INTO ... SELECT was executed (append mode)
        insert_calls = [call for call in mock_cursor.execute.call_args_list
                       if 'INSERT INTO' in str(call) and 'SELECT' in str(call)]
        assert len(insert_calls) > 0, "INSERT INTO ... SELECT should be executed for append_only mode"

        # Verify deletion detection query was executed (FULL_TABLE with append_only)
        # Check for UPDATE query with _SDC_DELETED_AT and NOT EXISTS (deletion detection pattern)
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        deletion_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and '_SDC_DELETED_AT' in call_str and 'NOT EXISTS' in call_str
        ]
        assert len(deletion_calls) > 0, "Deletion detection should be executed for FULL_TABLE replication with append_only"

    @patch('target_redshift.db_sync.boto3')
    @patch('target_redshift.db_sync.psycopg2.connect')
    def test_fast_sync_deletion_detection_no_primary_key_full_table(self, mock_connect, mock_boto3):
        """Test that deletion detection is NOT executed without primary keys, even with FULL_TABLE"""
        # Mock S3 client
        mock_s3_client = MagicMock()
        mock_boto3.session.Session.return_value.client.return_value = mock_s3_client

        # Mock database connection
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 50  # Simulate 50 inserted rows
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_conn.__enter__.return_value = mock_conn
        mock_connect.return_value = mock_conn

        # Create schema message without primary key
        schema_no_pk = self.stream_schema_message.copy()
        schema_no_pk['key_properties'] = []

        # Create DbSync instance with detect_deletions enabled but no primary key
        config_with_metadata = self.config.copy()
        config_with_metadata['add_metadata_columns'] = True
        config_with_metadata['detect_deletions'] = True
        db = db_sync.DbSync(config_with_metadata, schema_no_pk)
        loader = FastSyncLoader(db)

        # Call load_from_s3 with FULL_TABLE replication method but no primary key
        loader.load_from_s3(
            s3_bucket='source-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1,
            replication_method='FULL_TABLE'
        )

        # Verify deletion detection is NOT executed (requires primary keys)
        all_calls = [str(call[0][0]) if call[0] else '' for call in mock_cursor.execute.call_args_list]
        deletion_calls = [
            call_str for call_str in all_calls
            if call_str.strip().startswith('UPDATE') and '_SDC_DELETED_AT' in call_str and 'NOT EXISTS' in call_str
        ]
        assert len(deletion_calls) == 0, "Deletion detection should NOT be executed without primary keys, even with FULL_TABLE"
