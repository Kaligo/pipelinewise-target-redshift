"""
Unit tests for fast sync handler functions in target-redshift
"""

import pytest
from unittest.mock import MagicMock, patch

from target_redshift import db_sync
import target_redshift.fast_sync.handler as fast_sync_handler
from target_redshift.fast_sync.loader import FastSyncS3Info


class TestFastSyncHandler:
    """Test Cases for fast sync handler functionality"""

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
        }

        self.stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                }
            },
            "key_properties": ["id"],
        }

        self.schemas = {"test_schema-test_table": {}}

    def _create_valid_message(self, **overrides):
        """Helper to create a valid FAST_SYNC_RDS_S3_INFO message"""
        message = {
            "s3_bucket": "test-bucket",
            "s3_path": "test/path/data.csv",
            "s3_region": "us-east-1",
            "files_uploaded": 1,
            "replication_method": "FULL_TABLE",
            "file_format": "csv",
        }
        message.update(overrides)
        return message

    def _create_stream_to_sync(self):
        """Helper to create a stream_to_sync dictionary"""
        stream_to_sync = {}
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        stream_to_sync["test_schema-test_table"] = db
        return stream_to_sync

    def _test_validate_missing_field(self, field_name, expected_error_pattern):
        """Helper to test validation failure for missing required fields"""
        message = self._create_valid_message()
        del message[field_name]
        stream_id = "test_schema-test_table"
        stream_to_sync = self._create_stream_to_sync()

        with pytest.raises(ValueError, match=expected_error_pattern):
            fast_sync_handler.validate_and_extract_message(
                stream_id, message, self.schemas, stream_to_sync
            )

    def _create_mock_loader(self, mock_loader_class):
        """Helper to create and return a mock loader instance"""
        mock_loader = MagicMock()
        mock_loader_class.return_value = mock_loader
        return mock_loader

    def _create_fast_sync_queue_and_streams(self, num_streams=2):
        """Helper to create fast_sync_queue and stream_to_sync dictionaries"""
        fast_sync_queue = {}
        stream_to_sync = {}

        for i in range(1, num_streams + 1):
            stream_name = f"stream{i}"
            fast_sync_queue[stream_name] = self._create_valid_message(
                rows_uploaded=100 * i,
                replication_method="FULL_TABLE" if i == 1 else "INCREMENTAL",
            )
            stream_to_sync[stream_name] = MagicMock()

        return fast_sync_queue, stream_to_sync

    def test_validate_and_extract_message_success(self):
        """Test successful message validation and extraction"""
        stream_to_sync = self._create_stream_to_sync()
        message = self._create_valid_message()
        stream_id = "test_schema-test_table"

        msg = fast_sync_handler.validate_and_extract_message(
            stream_id, message, self.schemas, stream_to_sync
        )

        assert msg == message

    def test_validate_and_extract_message_missing_schema(self):
        """Test validation fails when schema is missing"""
        message = self._create_valid_message()
        stream_id = "unknown_stream"

        with pytest.raises(ValueError, match="before a corresponding schema"):
            fast_sync_handler.validate_and_extract_message(
                stream_id, message, self.schemas, {}
            )

    def test_validate_and_extract_message_missing_stream_to_sync(self):
        """Test validation fails when stream_to_sync is missing"""
        message = self._create_valid_message()
        stream_id = "test_schema-test_table"

        with pytest.raises(ValueError, match="before stream was initialized"):
            fast_sync_handler.validate_and_extract_message(
                stream_id, message, self.schemas, {}
            )

    def test_validate_and_extract_message_missing_s3_bucket(self):
        """Test validation fails when s3_bucket is missing"""
        self._test_validate_missing_field(
            "s3_bucket", "missing required fields: s3_bucket"
        )

    def test_validate_and_extract_message_missing_s3_path(self):
        """Test validation fails when s3_path is missing"""
        self._test_validate_missing_field("s3_path", "missing required fields: s3_path")

    def test_validate_and_extract_message_missing_s3_region(self):
        """Test validation fails when s3_region is missing"""
        self._test_validate_missing_field(
            "s3_region", "missing required fields: s3_region"
        )

    def test_validate_and_extract_message_missing_files_uploaded(self):
        """Test validation fails when files_uploaded is missing"""
        self._test_validate_missing_field(
            "files_uploaded", "missing required fields: files_uploaded"
        )

    def test_validate_and_extract_message_missing_replication_method(self):
        """Test validation fails when replication_method is missing"""
        self._test_validate_missing_field(
            "replication_method", "missing required fields: replication_method"
        )

    def test_validate_and_extract_message_missing_multiple_fields(self):
        """Test validation fails when multiple required fields are missing"""
        message = {
            "s3_bucket": "test-bucket"
            # Missing: s3_path, s3_region, files_uploaded, replication_method
            # Note: rows_uploaded is optional
        }
        stream_id = "test_schema-test_table"
        stream_to_sync = self._create_stream_to_sync()

        with pytest.raises(ValueError, match="missing required fields"):
            fast_sync_handler.validate_and_extract_message(
                stream_id, message, self.schemas, stream_to_sync
            )

    @patch("target_redshift.fast_sync.handler.FastSyncLoader")
    def test_load_from_s3_success(self, mock_loader_class):
        """Test successful loading from S3"""
        mock_loader = self._create_mock_loader(mock_loader_class)

        db = MagicMock()
        message = self._create_valid_message(rows_uploaded=100)

        fast_sync_handler.load_from_s3("test_stream", message, db)

        mock_loader_class.assert_called_once_with(db)
        # Verify load_from_s3 was called with FastSyncS3Info
        mock_loader.load_from_s3.assert_called_once()
        call_args = mock_loader.load_from_s3.call_args[0][0]
        assert isinstance(call_args, FastSyncS3Info)
        assert call_args.s3_bucket == "test-bucket"
        assert call_args.s3_path == "test/path/data.csv"
        assert call_args.s3_region == "us-east-1"
        assert call_args.rows_uploaded == 100
        assert call_args.files_uploaded == 1
        assert call_args.replication_method == "FULL_TABLE"
        assert call_args.file_format == "csv"

    @patch("target_redshift.fast_sync.handler.FastSyncLoader")
    def test_load_from_s3_error(self, mock_loader_class):
        """Test loading from S3 error handling"""
        mock_loader = self._create_mock_loader(mock_loader_class)
        mock_loader.load_from_s3.side_effect = Exception("Load failed")

        db = MagicMock()
        message = self._create_valid_message(replication_method="INCREMENTAL")

        with pytest.raises(Exception, match="Load failed"):
            fast_sync_handler.load_from_s3("test_stream", message, db)

        # Verify load_from_s3 was called
        mock_loader.load_from_s3.assert_called_once()

    @patch("target_redshift.fast_sync.handler.FastSyncLoader")
    def test_load_from_s3_without_rows_uploaded(self, mock_loader_class):
        """Test loading from S3 when rows_uploaded is not provided (optional field)"""
        mock_loader = self._create_mock_loader(mock_loader_class)

        db = MagicMock()
        message = self._create_valid_message(s3_region="us-west-2", files_uploaded=2)

        fast_sync_handler.load_from_s3("test_stream", message, db)

        # Verify load_from_s3 was called with FastSyncS3Info
        mock_loader.load_from_s3.assert_called_once()
        call_args = mock_loader.load_from_s3.call_args[0][0]
        assert isinstance(call_args, FastSyncS3Info)
        assert call_args.s3_bucket == "test-bucket"
        assert call_args.s3_path == "test/path/data.csv"
        assert call_args.s3_region == "us-west-2"
        assert call_args.rows_uploaded == 0  # Default value when not provided
        assert call_args.files_uploaded == 2
        assert call_args.replication_method == "FULL_TABLE"
        assert call_args.file_format == "csv"

    def test_flush_operations_empty_queue(self):
        """Test flush_operations with empty queue"""
        # Should return early without any processing
        fast_sync_handler.flush_operations({}, {}, 4, 16)

    @patch("target_redshift.fast_sync.handler.Parallel")
    def test_flush_operations_with_parallelism(self, mock_parallel_class):
        """Test flush_operations with explicit parallelism"""
        mock_parallel_instance = MagicMock()
        mock_parallel_class.return_value = mock_parallel_instance

        fast_sync_queue, stream_to_sync = self._create_fast_sync_queue_and_streams(2)

        fast_sync_handler.flush_operations(fast_sync_queue, stream_to_sync, 2, 16)

        # Verify Parallel was instantiated and called
        mock_parallel_class.assert_called_once()
        mock_parallel_instance.assert_called_once()

    @patch("target_redshift.fast_sync.handler.parallel_backend")
    @patch("target_redshift.fast_sync.handler.Parallel")
    def test_flush_operations_auto_parallelism(self, mock_parallel, mock_backend):
        """Test flush_operations with auto parallelism (parallelism=0)"""
        fast_sync_queue, stream_to_sync = self._create_fast_sync_queue_and_streams(3)

        # parallelism=0 should use min(n_streams, max_parallelism)
        fast_sync_handler.flush_operations(fast_sync_queue, stream_to_sync, 0, 2)

        # Verify parallel processing was invoked
        mock_parallel.assert_called_once()
        mock_backend.assert_called_once()
        # Verify parallel_backend was called with threading backend
        assert "threading" in str(mock_backend.call_args[0][0])

    def test_extract_operations_from_state_single_operation(self):
        """Test extract_operations_from_state with single fast_sync_s3_info"""
        s3_info = {
            "s3_bucket": "test-bucket",
            "s3_path": "test/path/data.csv",
            "s3_region": "us-east-1",
            "files_uploaded": 1,
            "replication_method": "FULL_TABLE",
            "rows_uploaded": 100,
        }
        state = {
            "bookmarks": {
                "test_schema-test_table": {
                    fast_sync_handler.FAST_SYNC_S3_INFO_KEY: s3_info
                }
            }
        }
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert len(operations) == 1
        assert "test_schema-test_table" in operations
        assert operations["test_schema-test_table"] == s3_info

    def test_extract_operations_from_state_empty_state(self):
        """Test extract_operations_from_state with empty state"""
        state = {}
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert operations == {}

    def test_cleanup_fast_sync_s3_info_from_state(self):
        """Test cleanup_fast_sync_s3_info_from_state removes fast_sync_s3_info"""
        state = {
            "bookmarks": {
                "test_schema-test_table": {
                    fast_sync_handler.FAST_SYNC_S3_INFO_KEY: {
                        "s3_bucket": "test-bucket"
                    },
                    "other_bookmark": "value",
                }
            }
        }

        fast_sync_handler.cleanup_fast_sync_s3_info_from_state(
            state, ["test_schema-test_table"]
        )

        assert (
            fast_sync_handler.FAST_SYNC_S3_INFO_KEY
            not in state["bookmarks"]["test_schema-test_table"]
        )
        assert "other_bookmark" in state["bookmarks"]["test_schema-test_table"]
