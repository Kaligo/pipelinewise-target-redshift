"""
Unit tests for FastSyncHandler in target-redshift
"""
import pytest
from unittest.mock import MagicMock, patch

import target_redshift.db_sync as db_sync
from target_redshift.fast_sync.handler import FastSyncHandler


class TestFastSyncHandler:
    """Test Cases for FastSyncHandler functionality"""

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
        }

        self.stream_schema_message = {
            'stream': 'test_schema-test_table',
            'schema': {
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'name': {'type': ['null', 'string']}
                }
            },
            'key_properties': ['id']
        }

        self.schemas = {
            'test_schema-test_table': {}
        }

    def test_validate_message_success(self):
        """Test successful message validation"""
        stream_to_sync = {}
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        stream_to_sync['test_schema-test_table'] = db

        message = {
            'stream': 'test_schema-test_table',
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv'
        }

        stream, msg = FastSyncHandler.validate_message(
            message, self.schemas, stream_to_sync, 'test line'
        )

        assert stream == 'test_schema-test_table'
        assert msg == message

    def test_validate_message_missing_stream(self):
        """Test validation fails when stream is missing"""
        message = {'s3_bucket': 'test-bucket', 's3_path': 'test/path/data.csv'}

        with pytest.raises(Exception, match="missing required key 'stream'"):
            FastSyncHandler.validate_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_message_missing_schema(self):
        """Test validation fails when schema is missing"""
        message = {
            'stream': 'unknown_stream',
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv'
        }

        with pytest.raises(Exception, match="before a corresponding schema"):
            FastSyncHandler.validate_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_message_missing_stream_to_sync(self):
        """Test validation fails when stream_to_sync is missing"""
        message = {
            'stream': 'test_schema-test_table',
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv'
        }

        with pytest.raises(Exception, match="before stream was initialized"):
            FastSyncHandler.validate_message(
                message, self.schemas, {}, 'test line'
            )

    @patch('target_redshift.fast_sync.handler.FastSyncLoader')
    def test_load_batch_success(self, mock_loader_class):
        """Test successful batch loading"""
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader

        db = MagicMock()
        message = {
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv',
            's3_region': 'us-east-1',
            'rows_uploaded': 100,
            'files_uploaded': 1,
            'replication_method': 'FULL_TABLE'
        }

        FastSyncHandler.load_batch('test_stream', message, db)

        mock_loader_class.assert_called_once_with(db)
        mock_loader.load_from_s3.assert_called_once_with(
            s3_bucket='test-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=100,
            files_uploaded=1,
            replication_method='FULL_TABLE'
        )

    @patch('target_redshift.fast_sync.handler.FastSyncLoader')
    def test_load_batch_error(self, mock_loader_class):
        """Test batch loading error handling"""
        mock_loader = MagicMock()
        mock_loader.load_from_s3.side_effect = Exception("Load failed")
        mock_loader_class.return_value = mock_loader

        db = MagicMock()
        message = {
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv',
            'replication_method': 'INCREMENTAL'
        }

        with pytest.raises(Exception, match="Load failed"):
            FastSyncHandler.load_batch('test_stream', message, db)

        # Verify load_from_s3 was called with replication_method
        mock_loader.load_from_s3.assert_called_once_with(
            s3_bucket='test-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',  # Default value
            rows_uploaded=0,  # Default value
            files_uploaded=1,  # Default value
            replication_method='INCREMENTAL'
        )

    @patch('target_redshift.fast_sync.handler.FastSyncLoader')
    def test_load_batch_without_replication_method(self, mock_loader_class):
        """Test batch loading when replication_method is not in message"""
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader

        db = MagicMock()
        message = {
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv',
            's3_region': 'us-west-2',
            'rows_uploaded': 50,
            'files_uploaded': 2
            # replication_method is missing
        }

        FastSyncHandler.load_batch('test_stream', message, db)

        # Verify load_from_s3 was called with replication_method=None (default)
        mock_loader.load_from_s3.assert_called_once_with(
            s3_bucket='test-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-west-2',
            rows_uploaded=50,
            files_uploaded=2,
            replication_method=None
        )

    @patch('target_redshift.fast_sync.handler.FastSyncHandler.load_batch')
    @patch('target_redshift.fast_sync.handler.parallel_backend')
    @patch('target_redshift.fast_sync.handler.Parallel')
    def test_flush_operations_empty_queue(self, mock_parallel, mock_backend, mock_load_batch):
        """Test flush_operations with empty queue"""
        fast_sync_queue = {}
        stream_to_sync = {}

        FastSyncHandler.flush_operations(fast_sync_queue, stream_to_sync, 4, 16)

        mock_load_batch.assert_not_called()
        mock_parallel.assert_not_called()

    @patch('target_redshift.fast_sync.handler.FastSyncHandler.load_batch')
    @patch('target_redshift.fast_sync.handler.parallel_backend')
    @patch('target_redshift.fast_sync.handler.Parallel')
    def test_flush_operations_with_parallelism(self, mock_parallel_class, mock_backend, mock_load_batch):
        """Test flush_operations with explicit parallelism"""
        # Mock Parallel instance
        mock_parallel_instance = MagicMock()
        mock_parallel_class.return_value = mock_parallel_instance

        fast_sync_queue = {
            'stream1': {'s3_bucket': 'bucket1', 's3_path': 'path1'},
            'stream2': {'s3_bucket': 'bucket2', 's3_path': 'path2'}
        }
        stream_to_sync = {
            'stream1': MagicMock(),
            'stream2': MagicMock()
        }

        FastSyncHandler.flush_operations(fast_sync_queue, stream_to_sync, 2, 16)

        # Verify Parallel was instantiated and called
        mock_parallel_class.assert_called_once()
        mock_parallel_instance.assert_called_once()

    @patch('target_redshift.fast_sync.handler.FastSyncHandler.load_batch')
    @patch('target_redshift.fast_sync.handler.parallel_backend')
    @patch('target_redshift.fast_sync.handler.Parallel')
    def test_flush_operations_auto_parallelism(self, mock_parallel, mock_backend, mock_load_batch):
        """Test flush_operations with auto parallelism (parallelism=0)"""
        fast_sync_queue = {
            'stream1': {'s3_bucket': 'bucket1', 's3_path': 'path1'},
            'stream2': {'s3_bucket': 'bucket2', 's3_path': 'path2'},
            'stream3': {'s3_bucket': 'bucket3', 's3_path': 'path3'}
        }
        stream_to_sync = {
            'stream1': MagicMock(),
            'stream2': MagicMock(),
            'stream3': MagicMock()
        }

        # parallelism=0 should use min(n_streams, max_parallelism)
        FastSyncHandler.flush_operations(fast_sync_queue, stream_to_sync, 0, 2)

        # Verify parallel processing was invoked
        mock_parallel.assert_called_once()
        mock_backend.assert_called_once()
        # Verify parallel_backend was called with threading backend
        assert 'threading' in str(mock_backend.call_args[0][0])
