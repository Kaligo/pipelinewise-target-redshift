"""
Unit tests for target_redshift main module
"""
import json
import os
import pytest
from unittest.mock import patch

import target_redshift
from target_redshift import db_sync
import target_redshift.fast_sync.handler as fast_sync_handler


class TestTargetRedshift:
    """Test cases for target_redshift main module"""

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

    def _create_stream_to_sync(self):
        """Helper to create a stream_to_sync dictionary"""
        stream_to_sync = {}
        db = db_sync.DbSync(self.config, self.stream_schema_message)
        stream_to_sync['test_schema-test_table'] = db
        return stream_to_sync

    def _create_fast_sync_s3_info(self, **overrides):
        """Helper to create fast_sync_s3_info dictionary"""
        s3_info = {
            's3_bucket': 'test-bucket',
            's3_path': 'test/path/data.csv',
            's3_region': 'us-east-1',
            'files_uploaded': 1,
            'replication_method': 'FULL_TABLE',
            'rows_uploaded': 100
        }
        s3_info.update(overrides)
        return s3_info

    def _create_state_with_bookmarks(self, bookmarks):
        """Helper to create a state dictionary with bookmarks"""
        return {
            'bookmarks': bookmarks
        }

    def test_add_metadata_values_to_record(self):
        """Test add_metadata_values_to_record adds metadata columns"""
        record_message = {
            'record': {'id': 1, 'name': 'test'},
            'time_extracted': '2023-01-01T00:00:00Z'
        }

        result = target_redshift.add_metadata_values_to_record(record_message)

        assert result['id'] == 1
        assert result['name'] == 'test'
        assert result['_sdc_extracted_at'] == '2023-01-01T00:00:00Z'
        assert '_sdc_batched_at' in result
        assert result['_sdc_deleted_at'] is None

    def test_add_metadata_values_to_record_with_deleted_at(self):
        """Test add_metadata_values_to_record with _sdc_deleted_at in record"""
        record_message = {
            'record': {
                'id': 1,
                'name': 'test',
                '_sdc_deleted_at': '2023-01-01T00:00:00Z'
            },
            'time_extracted': '2023-01-01T00:00:00Z'
        }

        result = target_redshift.add_metadata_values_to_record(record_message)

        assert result['_sdc_deleted_at'] == '2023-01-01T00:00:00Z'

    def test_add_metadata_values_to_record_without_time_extracted(self):
        """Test add_metadata_values_to_record without time_extracted"""
        record_message = {
            'record': {'id': 1, 'name': 'test'}
        }

        result = target_redshift.add_metadata_values_to_record(record_message)

        assert result['_sdc_extracted_at'] is None
        assert '_sdc_batched_at' in result

    @patch('target_redshift.flush_streams')
    @patch('target_redshift.DbSync')
    def test_persist_lines_with_40_records_and_batch_size_of_20_expect_flushing_once(
            self, db_sync_mock, flush_streams_mock):
        """Test persist_lines with batch size"""
        self.config['batch_size_rows'] = 20
        self.config['flush_all_streams'] = True

        with open(f'{os.path.dirname(__file__)}/resources/logical-streams.json', 'r') as f:
            lines = f.readlines()

        instance = db_sync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        flush_streams_mock.return_value = {'currently_syncing': None}

        target_redshift.persist_lines(self.config, lines)

        flush_streams_mock.assert_called_once()

    def test_extract_fast_sync_operations_from_state_empty_state(self):
        """Test extract_operations_from_state with empty state"""
        state = {}
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert operations == {}

    def test_extract_fast_sync_operations_from_state_no_bookmarks(self):
        """Test extract_operations_from_state with state without bookmarks"""
        state = {'currently_syncing': None}
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert operations == {}

    def test_extract_fast_sync_operations_from_state_no_fast_sync_info(self):
        """Test extract_operations_from_state with bookmarks but no fast_sync_s3_info"""
        bookmarks = {
            'test_schema-test_table': {
                'initial_full_table_complete': True
            }
        }
        state = self._create_state_with_bookmarks(bookmarks)
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert operations == {}

    def test_extract_fast_sync_operations_from_state_single_operation(self):
        """Test extract_operations_from_state with single fast_sync_s3_info"""
        s3_info = self._create_fast_sync_s3_info()
        bookmarks = {
            'test_schema-test_table': {
                'fast_sync_s3_info': s3_info
            }
        }
        state = self._create_state_with_bookmarks(bookmarks)
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert len(operations) == 1
        assert 'test_schema-test_table' in operations
        operation = operations['test_schema-test_table']
        assert operation['s3_bucket'] == 'test-bucket'
        assert operation['s3_path'] == 'test/path/data.csv'
        assert operation['s3_region'] == 'us-east-1'
        assert operation['files_uploaded'] == 1
        assert operation['replication_method'] == 'FULL_TABLE'
        assert operation['rows_uploaded'] == 100

    def test_extract_fast_sync_operations_from_state_multiple_operations(self):
        """Test extract_operations_from_state with multiple fast_sync_s3_info"""
        s3_info_1 = self._create_fast_sync_s3_info(
            s3_path='test/path/data1.csv',
            rows_uploaded=100
        )
        s3_info_2 = self._create_fast_sync_s3_info(
            s3_path='test/path/data2.csv',
            rows_uploaded=200,
            replication_method='INCREMENTAL'
        )

        # Create second stream schema
        stream_schema_2 = {
            'stream': 'test_schema-test_table2',
            'schema': {
                'properties': {
                    'id': {'type': ['null', 'integer']},
                    'name': {'type': ['null', 'string']}
                }
            },
            'key_properties': ['id']
        }
        schemas = {
            'test_schema-test_table': {},
            'test_schema-test_table2': {}
        }
        stream_to_sync = self._create_stream_to_sync()
        db2 = db_sync.DbSync(self.config, stream_schema_2)
        stream_to_sync['test_schema-test_table2'] = db2

        bookmarks = {
            'test_schema-test_table': {
                'fast_sync_s3_info': s3_info_1
            },
            'test_schema-test_table2': {
                'fast_sync_s3_info': s3_info_2
            }
        }
        state = self._create_state_with_bookmarks(bookmarks)

        operations = fast_sync_handler.extract_operations_from_state(
            state, schemas, stream_to_sync
        )

        assert len(operations) == 2
        assert 'test_schema-test_table' in operations
        assert 'test_schema-test_table2' in operations
        assert operations['test_schema-test_table']['rows_uploaded'] == 100
        assert operations['test_schema-test_table2']['rows_uploaded'] == 200
        assert operations['test_schema-test_table2']['replication_method'] == 'INCREMENTAL'

    def test_extract_fast_sync_operations_from_state_mixed_bookmarks(self):
        """Test extract_operations_from_state with mixed bookmarks"""
        s3_info = self._create_fast_sync_s3_info()
        bookmarks = {
            'test_schema-test_table': {
                'fast_sync_s3_info': s3_info,
                'initial_full_table_complete': True
            },
            'test_schema-test_table2': {
                'initial_full_table_complete': True
            }
        }
        state = self._create_state_with_bookmarks(bookmarks)
        stream_to_sync = self._create_stream_to_sync()

        operations = fast_sync_handler.extract_operations_from_state(
            state, self.schemas, stream_to_sync
        )

        assert len(operations) == 1
        assert 'test_schema-test_table' in operations

    def test_extract_fast_sync_operations_from_state_invalid_message(self):
        """Test extract_operations_from_state with invalid fast_sync_s3_info"""
        s3_info = {
            's3_bucket': 'test-bucket'
            # Missing required fields
        }
        bookmarks = {
            'test_schema-test_table': {
                'fast_sync_s3_info': s3_info
            }
        }
        state = self._create_state_with_bookmarks(bookmarks)
        stream_to_sync = self._create_stream_to_sync()

        with pytest.raises(ValueError):
            fast_sync_handler.extract_operations_from_state(
                state, self.schemas, stream_to_sync
            )

    def test_extract_fast_sync_operations_from_state_missing_stream(self):
        """Test extract_operations_from_state with stream not in stream_to_sync"""
        s3_info = self._create_fast_sync_s3_info()
        bookmarks = {
            'unknown_stream': {
                'fast_sync_s3_info': s3_info
            }
        }
        state = self._create_state_with_bookmarks(bookmarks)
        stream_to_sync = {}

        with pytest.raises(ValueError):
            fast_sync_handler.extract_operations_from_state(
                state, self.schemas, stream_to_sync
            )

    def test_flush_fast_sync_queue_empty_queue(self):
        """Test flush_fast_sync_queue with empty queue"""
        fast_sync_queue = {}
        stream_to_sync = {}
        config = {}

        # Should not raise any exception
        target_redshift.flush_fast_sync_queue(fast_sync_queue, stream_to_sync, config)

        assert fast_sync_queue == {}

    @patch('target_redshift.fast_sync.handler.flush_operations')
    def test_flush_fast_sync_queue_with_operations(self, mock_flush_operations):
        """Test flush_fast_sync_queue with queued operations"""
        fast_sync_queue = {
            'test_schema-test_table': {
                's3_bucket': 'test-bucket',
                's3_path': 'test/path/data.csv',
                's3_region': 'us-east-1',
                'files_uploaded': 1,
                'replication_method': 'FULL_TABLE',
                'rows_uploaded': 100
            }
        }
        stream_to_sync = self._create_stream_to_sync()
        config = {
            'parallelism': 2,
            'max_parallelism': 16
        }

        target_redshift.flush_fast_sync_queue(fast_sync_queue, stream_to_sync, config)

        mock_flush_operations.assert_called_once_with(
            fast_sync_queue, stream_to_sync, 2, 16
        )
        assert fast_sync_queue == {}

    @patch('target_redshift.fast_sync.handler.flush_operations')
    def test_flush_fast_sync_queue_default_parallelism(self, mock_flush_operations):
        """Test flush_fast_sync_queue with default parallelism values"""
        fast_sync_queue = {
            'test_schema-test_table': {
                's3_bucket': 'test-bucket',
                's3_path': 'test/path/data.csv',
                's3_region': 'us-east-1',
                'files_uploaded': 1,
                'replication_method': 'FULL_TABLE'
            }
        }
        stream_to_sync = self._create_stream_to_sync()
        config = {}

        target_redshift.flush_fast_sync_queue(fast_sync_queue, stream_to_sync, config)

        mock_flush_operations.assert_called_once_with(
            fast_sync_queue, stream_to_sync, 0, 16
        )
        assert fast_sync_queue == {}

    def test_cleanup_fast_sync_s3_info_from_state(self):
        """Test cleanup_fast_sync_s3_info_from_state removes fast_sync_s3_info from bookmarks"""
        state = {
            'bookmarks': {
                'test_schema-test_table': {
                    'fast_sync_s3_info': {
                        's3_bucket': 'test-bucket',
                        's3_path': 'test/path/data.csv'
                    },
                    'other_bookmark': 'value'
                },
                'test_schema-test_table2': {
                    'fast_sync_s3_info': {
                        's3_bucket': 'test-bucket',
                        's3_path': 'test/path/data2.csv'
                    }
                },
                'test_schema-test_table3': {
                    'other_bookmark': 'value'
                }
            }
        }

        processed_streams = ['test_schema-test_table', 'test_schema-test_table2']

        fast_sync_handler.cleanup_fast_sync_s3_info_from_state(state, processed_streams)

        # Verify fast_sync_s3_info was removed from processed streams
        assert 'fast_sync_s3_info' not in state['bookmarks']['test_schema-test_table']
        assert 'other_bookmark' in state['bookmarks']['test_schema-test_table']
        assert 'fast_sync_s3_info' not in state['bookmarks']['test_schema-test_table2']
        # Verify unprocessed stream is unchanged
        assert 'fast_sync_s3_info' not in state['bookmarks']['test_schema-test_table3']
        assert 'other_bookmark' in state['bookmarks']['test_schema-test_table3']

    def test_cleanup_fast_sync_s3_info_from_state_empty_state(self):
        """Test cleanup_fast_sync_s3_info_from_state with empty state"""
        state = {}
        processed_streams = ['test_schema-test_table']

        # Should not raise any exception - function handles empty state gracefully
        fast_sync_handler.cleanup_fast_sync_s3_info_from_state(state, processed_streams)

    def test_cleanup_fast_sync_s3_info_from_state_no_bookmarks(self):
        """Test cleanup_fast_sync_s3_info_from_state with state without bookmarks"""
        state = {'currently_syncing': None}
        processed_streams = ['test_schema-test_table']

        # Should not raise any exception - function handles missing bookmarks gracefully
        fast_sync_handler.cleanup_fast_sync_s3_info_from_state(state, processed_streams)

    @patch('target_redshift.fast_sync.handler.flush_operations')
    @patch('target_redshift.flush_streams')
    @patch('target_redshift.DbSync')
    def test_persist_lines_with_state_containing_fast_sync_info(
            self, db_sync_mock, flush_streams_mock, mock_flush_operations):
        """Test persist_lines processes STATE messages with fast_sync_s3_info"""
        self.config['batch_size_rows'] = 100000

        instance = db_sync_mock.return_value
        instance.create_schema_if_not_exists.return_value = None
        instance.sync_table.return_value = None

        # Create lines with SCHEMA, STATE with fast_sync_s3_info, and RECORD messages
        lines = [
            json.dumps({
                'type': 'SCHEMA',
                'stream': 'test_schema-test_table',
                'schema': {
                    'properties': {
                        'id': {'type': ['null', 'integer']},
                        'name': {'type': ['null', 'string']}
                    }
                },
                'key_properties': ['id']
            }) + '\n',
            json.dumps({
                'type': 'STATE',
                'value': {
                    'bookmarks': {
                        'test_schema-test_table': {
                            'fast_sync_s3_info': {
                                's3_bucket': 'test-bucket',
                                's3_path': 'test/path/data.csv',
                                's3_region': 'us-east-1',
                                'files_uploaded': 1,
                                'replication_method': 'FULL_TABLE',
                                'rows_uploaded': 100
                            }
                        }
                    }
                }
            }) + '\n',
            json.dumps({
                'type': 'RECORD',
                'stream': 'test_schema-test_table',
                'record': {'id': 1, 'name': 'test'}
            }) + '\n'
        ]

        flush_streams_mock.return_value = {'currently_syncing': None}

        target_redshift.persist_lines(self.config, lines)

        # Verify fast_sync operations were flushed at the end
        mock_flush_operations.assert_called_once()
        flush_streams_mock.assert_called_once()
