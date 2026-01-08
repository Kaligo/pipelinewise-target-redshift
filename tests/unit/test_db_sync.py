from unittest.mock import MagicMock, patch, call

import target_redshift
from target_redshift import db_sync


class TestTargetRedshift(object):
    """
    Unit Tests for PipelineWise Target Redshift
    """
    def setup_method(self):
        self.config = {}


    def teardown_method(self):
        pass


    def test_config_validation(self):
        """Test configuration validator"""
        validator = target_redshift.db_sync.validate_config
        empty_config = {}
        minimal_config = {
            'host':                     "dummy-value",
            'port':                     5439,
            'user':                     "dummy-value",
            'password':                 "dummy-value",
            'dbname':                   "dummy-value",
            'aws_access_key_id':        "dummy-value",
            'aws_secret_access_key':    "dummy-value",
            's3_bucket':                "dummy-value",
            'default_target_schema':    "dummy-value"
        }

        # Config validator returns a list of errors
        # If the list is empty then the configuration is valid otherwise invalid

        # Empty configuration should fail - (nr_of_errors > 0)
        assert len(validator(empty_config)) > 0

        # Minimal configuratino should pass - (nr_of_errors == 0)
        assert len(validator(minimal_config)) == 0

        # Configuration without schema references - (nr_of_errors >= 0)
        config_with_no_schema = minimal_config.copy()
        config_with_no_schema.pop('default_target_schema')
        assert len(validator(config_with_no_schema)) > 0

        # Configuration with schema mapping - (nr_of_errors == 0)
        config_with_schema_mapping = minimal_config.copy()
        config_with_schema_mapping.pop('default_target_schema')
        config_with_schema_mapping['schema_mapping'] = {
            "dummy_stream": {
                "target_schema": "dummy_schema"
            }
        }
        assert len(validator(config_with_schema_mapping)) == 0


    def test_column_type_mapping(self):
        """Test JSON type to Redshift column type mappings"""
        mapper = target_redshift.db_sync.column_type

        # Incoming JSON schema types
        json_str =          {"type": ["string"]             }
        json_str_or_null =  {"type": ["string", "null"]     }
        json_dt =           {"type": ["string"]             , "format": "date-time"}
        json_dt_or_null =   {"type": ["string", "null"]     , "format": "date-time"}
        json_t =            {"type": ["string"]             , "format": "time"}
        json_t_or_null =    {"type": ["string", "null"]     , "format": "time"}
        json_num =          {"type": ["number"]             }
        json_int =          {"type": ["integer"]            }
        json_int_or_str =   {"type": ["integer", "string"]  }
        json_bool =         {"type": ["boolean"]            }
        json_obj =          {"type": ["object"]             }
        json_arr =          {"type": ["array"]              }

        # Mapping from JSON schema types ot Redshift column types
        assert mapper(json_str)          == 'character varying(10000)'
        assert mapper(json_str_or_null)  == 'character varying(10000)'
        assert mapper(json_dt)           == 'timestamp without time zone'
        assert mapper(json_dt_or_null)   == 'timestamp without time zone'
        assert mapper(json_t)            == 'character varying(256)'
        assert mapper(json_t_or_null)    == 'character varying(256)'
        assert mapper(json_num)          == 'double precision'
        assert mapper(json_int)          == 'numeric'
        assert mapper(json_int_or_str)   == 'character varying(65535)'
        assert mapper(json_bool)         == 'boolean'
        assert mapper(json_obj)          == 'character varying(65535)'
        assert mapper(json_arr)          == 'character varying(65535)'


    def test_stream_name_to_dict(self):
        """Test identifying catalog, schema and table names from fully qualified stream and table names"""
        # Singer stream name format (Default '-' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_table') == \
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_schema-my_table') == \
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Singer stream name format (Default '-' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_catalog-my_schema-my_table') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_table', separator='.') == \
            {"catalog_name": None, "schema_name": None, "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_schema.my_table', separator='.') == \
            {"catalog_name": None, "schema_name": "my_schema", "table_name": "my_table"}

        # Redshift table format (Custom '.' separator)
        assert \
            target_redshift.db_sync.stream_name_to_dict('my_catalog.my_schema.my_table', separator='.') == \
            {"catalog_name": "my_catalog", "schema_name": "my_schema", "table_name": "my_table"}


    def test_flatten_schema(self):
        """Test flattening of SCHEMA messages"""
        flatten_schema = target_redshift.db_sync.flatten_schema

        # Schema with no object properties should be empty dict
        schema_with_no_properties = {"type": "object"}
        assert flatten_schema(schema_with_no_properties) == {}

        not_nested_schema = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]}}}
        # NO FLATTENNING - Schema with simple properties should be a plain dictionary
        assert flatten_schema(not_nested_schema) == not_nested_schema['properties']

        nested_schema_with_no_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {"type": ["null", "object"]}}}
        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        assert flatten_schema(nested_schema_with_no_properties) == nested_schema_with_no_properties['properties']

        nested_schema_with_properties = {
            "type": "object",
            "properties": {
                "c_pk": {"type": ["null", "integer"]},
                "c_varchar": {"type": ["null", "string"]},
                "c_int": {"type": ["null", "integer"]},
                "c_obj": {
                    "type": ["null", "object"],
                    "properties": {
                        "nested_prop1": {"type": ["null", "string"]},
                        "nested_prop2": {"type": ["null", "string"]},
                        "nested_prop3": {
                            "type": ["null", "object"],
                            "properties": {
                                "multi_nested_prop1": {"type": ["null", "string"]},
                                "multi_nested_prop2": {"type": ["null", "string"]}
                            }
                        }
                    }
                }
            }
        }
        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        # No flattening (default)
        assert flatten_schema(nested_schema_with_properties) == nested_schema_with_properties['properties']

        # NO FLATTENNING - Schema with object type property but without further properties should be a plain dictionary
        #   max_level: 0 : No flattening (default)
        assert flatten_schema(nested_schema_with_properties, max_level=0) == nested_schema_with_properties['properties']

        # FLATTENNING - Schema with object type property but without further properties should be a dict with flattened properties
        assert \
            flatten_schema(nested_schema_with_properties, max_level=1) == \
            {
                'c_pk': {'type': ['null', 'integer']},
                'c_varchar': {'type': ['null', 'string']},
                'c_int': {'type': ['null', 'integer']},
                'c_obj__nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop2': {'type': ['null', 'string']},
                'c_obj__nested_prop3': {
                    'type': ['null', 'object'],
                    "properties": {
                        "multi_nested_prop1": {"type": ["null", "string"]},
                        "multi_nested_prop2": {"type": ["null", "string"]}
                    }
                }
        }

        # FLATTENNING - Schema with object type property but without further properties should be a dict with flattened properties
        assert \
            flatten_schema(nested_schema_with_properties, max_level=10) == \
            {
                'c_pk': {'type': ['null', 'integer']},
                'c_varchar': {'type': ['null', 'string']},
                'c_int': {'type': ['null', 'integer']},
                'c_obj__nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop2': {'type': ['null', 'string']},
                'c_obj__nested_prop3__multi_nested_prop1': {'type': ['null', 'string']},
                'c_obj__nested_prop3__multi_nested_prop2': {'type': ['null', 'string']}
            }


    def test_flatten_record(self):
        """Test flattening of RECORD messages"""
        flatten_record = target_redshift.db_sync.flatten_record

        empty_record = {}
        # Empty record should be empty dict
        assert flatten_record(empty_record) == {}

        not_nested_record = {"c_pk": 1, "c_varchar": "1", "c_int": 1}
        # NO FLATTENNING - Record with simple properties should be a plain dictionary
        assert flatten_record(not_nested_record) == not_nested_record

        nested_record = {
            "c_pk": 1,
            "c_varchar": "1",
            "c_int": 1,
            "c_obj": {
                "nested_prop1": "value_1",
                "nested_prop2": "value_2",
                "nested_prop3": {
                    "multi_nested_prop1": "multi_value_1",
                    "multi_nested_prop2": "multi_value_2",
                }}}

        # NO FLATTENNING - No flattening (default)
        assert \
            flatten_record(nested_record) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
            }

        # NO FLATTENNING
        #   max_level: 0 : No flattening (default)
        assert \
            flatten_record(nested_record, max_level=0) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj": '{"nested_prop1": "value_1", "nested_prop2": "value_2", "nested_prop3": {"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}}'
            }

        # SEMI FLATTENNING
        #   max_level: 1 : Semi-flattening (default)
        assert \
            flatten_record(nested_record, max_level=1) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj__nested_prop1": "value_1",
                "c_obj__nested_prop2": "value_2",
                "c_obj__nested_prop3": '{"multi_nested_prop1": "multi_value_1", "multi_nested_prop2": "multi_value_2"}'
            }

        # FLATTENNING
        assert \
            flatten_record(nested_record, max_level=10) == \
            {
                "c_pk": 1,
                "c_varchar": "1",
                "c_int": 1,
                "c_obj__nested_prop1": "value_1",
                "c_obj__nested_prop2": "value_2",
                "c_obj__nested_prop3__multi_nested_prop1": "multi_value_1",
                "c_obj__nested_prop3__multi_nested_prop2": "multi_value_2"
            }

    def test_flatten_record_with_flatten_schema(self):
        flatten_record = target_redshift.db_sync.flatten_record

        flatten_schema = {
            "id": {
                "type": [
                    "object",
                    "array",
                    "null"
                ]
            }
        }

        test_cases = [
            (
                True,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": "1",
                    "data": "xyz"
                }
            ),
            (
                False,
                {
                    "id": 1,
                    "data": "xyz"
                },
                {
                    "id": 1,
                    "data": "xyz"
                }
            )
        ]

        for idx, (should_use_flatten_schema, record, expected_output) in enumerate(test_cases):
            output = flatten_record(record, flatten_schema if should_use_flatten_schema else None)
            assert output == expected_output

    def _create_test_config(self):
        """Helper to create test configuration"""
        return {
            "host": "test-host",
            "port": 5439,
            "user": "test_user",
            "password": "test_password",
            "dbname": "test_db",
            "aws_access_key_id": "test_key",
            "aws_secret_access_key": "test_secret",
            "s3_bucket": "test-bucket",
            "default_target_schema": "test_schema",
        }

    def _create_mock_connection(self, mock_connect):
        """Helper to create mock database connection"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.rowcount = 0
        mock_cursor.description = None

        # Set up cursor as context manager
        mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor.__exit__ = MagicMock(return_value=False)

        # Set up connection.cursor() to return cursor context manager
        mock_cursor_context = MagicMock()
        mock_cursor_context.__enter__ = MagicMock(return_value=mock_cursor)
        mock_cursor_context.__exit__ = MagicMock(return_value=False)
        mock_conn.cursor.return_value = mock_cursor_context

        # Set up connection as context manager
        mock_conn.__enter__ = MagicMock(return_value=mock_conn)
        mock_conn.__exit__ = MagicMock(return_value=False)

        mock_connect.return_value = mock_conn
        return mock_cursor

    def _setup_boto3_mock(self, mock_boto3):
        """Helper to set up boto3 mock for DbSync initialization"""
        mock_session = MagicMock()
        mock_credentials = MagicMock()
        mock_frozen_credentials = MagicMock()
        mock_frozen_credentials.access_key = "test_key"
        mock_frozen_credentials.secret_key = "test_secret"
        mock_frozen_credentials.token = None
        mock_credentials.get_frozen_credentials.return_value = mock_frozen_credentials
        mock_session.get_credentials.return_value = mock_credentials
        mock_s3_client = MagicMock()
        mock_session.client.return_value = mock_s3_client
        mock_boto3.session.Session.return_value = mock_session

    @patch("target_redshift.db_sync.psycopg2.connect")
    @patch("target_redshift.db_sync.boto3")
    def test_alter_column_type(self, mock_boto3, mock_connect):
        """Test alter_column_type method generates correct SQL"""
        config = self._create_test_config()
        stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                }
            },
            "key_properties": ["id"],
        }

        self._setup_boto3_mock(mock_boto3)
        mock_cursor = self._create_mock_connection(mock_connect)
        db = db_sync.DbSync(config, stream_schema_message)

        db.alter_column_type('"_SDC_DELETED_AT"', "timestamp without time zone", "test_schema-test_table")

        expected_sql = 'ALTER TABLE "test_schema"."TEST_TABLE" ALTER COLUMN "_SDC_DELETED_AT" TYPE timestamp without time zone'
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0][0]
        assert call_args == expected_sql

    @patch("target_redshift.db_sync.psycopg2.connect")
    @patch("target_redshift.db_sync.boto3")
    def test_update_columns_sdc_deleted_at_type_change(self, mock_boto3, mock_connect):
        """Test that _sdc_deleted_at column type is altered directly when type changes"""
        config = self._create_test_config()
        stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "_sdc_deleted_at": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                }
            },
            "key_properties": ["id"],
        }

        self._setup_boto3_mock(mock_boto3)
        self._create_mock_connection(mock_connect)
        table_cache = [
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "id",
                "data_type": "numeric",
            },
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "_sdc_deleted_at",
                "data_type": "character varying",
            },
        ]

        db = db_sync.DbSync(config, stream_schema_message, table_cache=table_cache)

        with patch.object(db, "alter_column_type") as mock_alter, \
             patch.object(db, "version_column") as mock_version:
            db.update_columns()

            mock_alter.assert_called_once_with(
                '"_SDC_DELETED_AT"',
                "timestamp without time zone",
                "test_schema-test_table"
            )
            assert not mock_version.called

    @patch("target_redshift.db_sync.psycopg2.connect")
    @patch("target_redshift.db_sync.boto3")
    def test_update_columns_sdc_deleted_at_no_type_change(self, mock_boto3, mock_connect):
        """Test that _sdc_deleted_at is not altered when type is already correct"""
        config = self._create_test_config()
        stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "_sdc_deleted_at": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                }
            },
            "key_properties": ["id"],
        }

        self._setup_boto3_mock(mock_boto3)
        self._create_mock_connection(mock_connect)
        table_cache = [
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "id",
                "data_type": "numeric",
            },
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "_sdc_deleted_at",
                "data_type": "timestamp without time zone",
            },
        ]

        db = db_sync.DbSync(config, stream_schema_message, table_cache=table_cache)

        with patch.object(db, "alter_column_type") as mock_alter:
            db.update_columns()
            assert not mock_alter.called

    @patch("target_redshift.db_sync.psycopg2.connect")
    @patch("target_redshift.db_sync.boto3")
    def test_update_columns_sdc_deleted_at_not_in_schema(self, mock_boto3, mock_connect):
        """Test that _sdc_deleted_at is not processed when not in schema"""
        config = self._create_test_config()
        stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                }
            },
            "key_properties": ["id"],
        }

        self._setup_boto3_mock(mock_boto3)
        self._create_mock_connection(mock_connect)
        table_cache = [
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "id",
                "data_type": "numeric",
            },
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "_sdc_deleted_at",
                "data_type": "character varying",
            },
        ]

        db = db_sync.DbSync(config, stream_schema_message, table_cache=table_cache)

        with patch.object(db, "alter_column_type") as mock_alter:
            db.update_columns()
            assert not mock_alter.called

    @patch("target_redshift.db_sync.psycopg2.connect")
    @patch("target_redshift.db_sync.boto3")
    def test_update_columns_sdc_deleted_at_excluded_from_versioning(self, mock_boto3, mock_connect):
        """Test that _sdc_deleted_at is excluded from columns_to_replace after being altered"""
        config = self._create_test_config()
        stream_schema_message = {
            "stream": "test_schema-test_table",
            "schema": {
                "properties": {
                    "id": {"type": ["null", "integer"]},
                    "name": {"type": ["null", "string"]},
                    "_sdc_deleted_at": {
                        "type": ["null", "string"],
                        "format": "date-time",
                    },
                }
            },
            "key_properties": ["id"],
        }

        self._setup_boto3_mock(mock_boto3)
        self._create_mock_connection(mock_connect)
        table_cache = [
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "id",
                "data_type": "numeric",
            },
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "_sdc_deleted_at",
                "data_type": "character varying",
            },
            {
                "table_schema": "test_schema",
                "table_name": "test_table",
                "column_name": "name",
                "data_type": "character varying(100)",
            },
        ]

        db = db_sync.DbSync(config, stream_schema_message, table_cache=table_cache)

        with patch.object(db, "alter_column_type") as mock_alter, \
             patch.object(db, "version_column") as mock_version:
            db.update_columns()

            mock_alter.assert_called_once()
            assert mock_alter.call_args[0][0] == '"_SDC_DELETED_AT"'
            version_calls = [call[0][0] for call in mock_version.call_args_list]
            assert '"_SDC_DELETED_AT"' not in version_calls
