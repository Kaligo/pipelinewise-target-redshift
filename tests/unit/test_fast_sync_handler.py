"""
Unit tests for FastSyncHandler in target-redshift
"""
import pytest
from unittest.mock import MagicMock, patch

from target_redshift import db_sync
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

    def _create_valid_message(self, **overrides):
        """Helper to create a valid FAST_SYNC_RDS_S3_INFO message"""
        message = {
            'stream': 'test_schema-test_table',
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv',
            's3_region': 'us-east-1',
            'files_uploaded': 1,
            'replication_method': 'FULL_TABLE'
        }
        message.update(overrides)
        return message

    def _create_stream_to_sync(self):
        """Helper to create a stream_to_sync dictionary"""
        stream_to_sync = {}
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        stream_to_sync['test_schema-test_table'] = db
        return stream_to_sync

    def test_validate_and_extract_message_success(self):
        """Test successful message validation and extraction"""
        stream_to_sync = self._create_stream_to_sync()
        message = self._create_valid_message(rows_uploaded=100)

        stream, msg = FastSyncHandler.validate_and_extract_message(
            message, self.schemas, stream_to_sync, 'test line'
        )

        assert stream == 'test_schema-test_table'
        assert msg == message

    def test_validate_and_extract_message_success_without_rows_uploaded(self):
        """Test successful message validation when rows_uploaded is omitted (optional field)"""
        stream_to_sync = self._create_stream_to_sync()
        message = self._create_valid_message()

        stream, msg = FastSyncHandler.validate_and_extract_message(
            message, self.schemas, stream_to_sync, 'test line'
        )

        assert stream == 'test_schema-test_table'
        assert msg == message

    def test_validate_and_extract_message_missing_stream(self):
        """Test validation fails when stream is missing"""
        message = self._create_valid_message()
        del message['stream']

        with pytest.raises(ValueError, match="missing required fields: stream"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_schema(self):
        """Test validation fails when schema is missing"""
        message = self._create_valid_message(stream='unknown_stream')

        with pytest.raises(ValueError, match="before a corresponding schema"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_stream_to_sync(self):
        """Test validation fails when stream_to_sync is missing"""
        message = self._create_valid_message()

        with pytest.raises(ValueError, match="before stream was initialized"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_s3_bucket(self):
        """Test validation fails when s3_bucket is missing"""
        message = self._create_valid_message()
        del message['s3_bucket']

        with pytest.raises(ValueError, match="missing required fields: s3_bucket"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_s3_path(self):
        """Test validation fails when s3_path is missing"""
        message = self._create_valid_message()
        del message['s3_path']

        with pytest.raises(ValueError, match="missing required fields: s3_path"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_s3_region(self):
        """Test validation fails when s3_region is missing"""
        message = self._create_valid_message()
        del message['s3_region']

        with pytest.raises(ValueError, match="missing required fields: s3_region"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_files_uploaded(self):
        """Test validation fails when files_uploaded is missing"""
        message = self._create_valid_message()
        del message['files_uploaded']

        with pytest.raises(ValueError, match="missing required fields: files_uploaded"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_replication_method(self):
        """Test validation fails when replication_method is missing"""
        message = self._create_valid_message()
        del message['replication_method']

        with pytest.raises(ValueError, match="missing required fields: replication_method"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    def test_validate_and_extract_message_missing_multiple_fields(self):
        """Test validation fails when multiple required fields are missing"""
        message = {
            'stream': 'test_schema-test_table',
            's3_bucket': 'test-bucket'
            # Missing: s3_path, s3_region, files_uploaded, replication_method
            # Note: rows_uploaded is optional
        }

        with pytest.raises(ValueError, match="missing required fields"):
            FastSyncHandler.validate_and_extract_message(
                message, self.schemas, {}, 'test line'
            )

    @patch('target_redshift.fast_sync.handler.FastSyncLoader')
    def test_load_batch_success(self, mock_loader_class):
        """Test successful batch loading"""
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader

        db = MagicMock()
        message = self._create_valid_message(rows_uploaded=100)

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
        message = self._create_valid_message(replication_method='INCREMENTAL')

        with pytest.raises(Exception, match="Load failed"):
            FastSyncHandler.load_batch('test_stream', message, db)

        # Verify load_from_s3 was called with rows_uploaded defaulting to 0
        mock_loader.load_from_s3.assert_called_once_with(
            s3_bucket='test-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-east-1',
            rows_uploaded=0,  # Default value when not provided
            files_uploaded=1,
            replication_method='INCREMENTAL'
        )

    @patch('target_redshift.fast_sync.handler.FastSyncLoader')
    def test_load_batch_without_rows_uploaded(self, mock_loader_class):
        """Test batch loading when rows_uploaded is not provided (optional field)"""
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader

        db = MagicMock()
        message = self._create_valid_message(
            s3_region='us-west-2',
            files_uploaded=2
        )

        FastSyncHandler.load_batch('test_stream', message, db)

        # Verify load_from_s3 was called with rows_uploaded defaulting to 0
        mock_loader.load_from_s3.assert_called_once_with(
            s3_bucket='test-bucket',
            s3_path='test/path/data.csv',
            s3_region='us-west-2',
            rows_uploaded=0,  # Default value when not provided
            files_uploaded=2,
            replication_method='FULL_TABLE'
        )

    @patch('target_redshift.fast_sync.handler.FastSyncHandler.load_batch')
    @patch('target_redshift.fast_sync.handler.parallel_backend')
    @patch('target_redshift.fast_sync.handler.Parallel')
    def test_flush_operations_empty_queue(self, mock_parallel, mock_backend, mock_load_batch):
        """Test flush_operations with empty queue"""
        FastSyncHandler.flush_operations({}, {}, 4, 16)

        mock_load_batch.assert_not_called()
        mock_parallel.assert_not_called()

    @patch('target_redshift.fast_sync.handler.FastSyncHandler.load_batch')
    @patch('target_redshift.fast_sync.handler.parallel_backend')
    @patch('target_redshift.fast_sync.handler.Parallel')
    def test_flush_operations_with_parallelism(self, mock_parallel_class, mock_backend, mock_load_batch):
        """Test flush_operations with explicit parallelism"""
        mock_parallel_instance = MagicMock()
        mock_parallel_class.return_value = mock_parallel_instance

        fast_sync_queue = {
            'stream1': self._create_valid_message(rows_uploaded=100, replication_method='FULL_TABLE'),
            'stream2': self._create_valid_message(rows_uploaded=200, replication_method='INCREMENTAL')
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
            'stream1': self._create_valid_message(rows_uploaded=100),
            'stream2': self._create_valid_message(rows_uploaded=200, replication_method='INCREMENTAL'),
            'stream3': self._create_valid_message(rows_uploaded=300)
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
