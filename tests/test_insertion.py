"""Tests for data insertion module"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from mysql.connector import Error as MySQLError
from aware_filter.insertion import insert_record, insert_records, get_device_uid, transform_and_write, apply_rate_limit


examples = {
    'table_double': [
        {
            'device_id': "device_123",
            'timestamp': 1706342400000,
            'double_value_0': 23.5,
            'double_value_1': 42.1,
            'accuracy': 10
        },
        {
            'device_id': "device_123",
            'timestamp': 1706428800000,
            'double_value_0': 25.0,
            'double_value_1': 40.3,
            'accuracy': 12
        }
    ],
    'table_text': [
        {
            'device_id': "device_456",
            'timestamp': 1706342400000,
            'text_value': "example_text_data"
        },
        {
            'device_id': "device_456",
            'timestamp': 1706428800000,
            'text_value': "another_text_entry"
        }
    ]
}

class TestInsertRecord:
    """Test cases for the insert_record function"""

    @patch('aware_filter.insertion.transform_and_write')
    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_success_with_transform(self, mock_get_conn, mock_transform, table_type, data_list):
        """Test successful data insertion when transformed table exists and succeeds"""
        mock_transform.return_value = (True, None)
        
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        success, msg = insert_record(data, table_type, stats)
        
        assert success is True
        assert 'transformed table' in msg
        mock_transform.assert_called_once()

    @patch('aware_filter.insertion.transform_and_write')
    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_success_fallback_to_original(self, mock_get_conn, mock_transform, table_type, data_list):
        """Test successful data insertion when transformed table doesn't exist, falls back to original"""
        mock_transform.return_value = (False, "Transformed table does not exist")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        success, msg = insert_record(data, table_type, stats)
        
        assert success is True
        assert stats['successful_inserts'] == 1
        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()

    @patch('aware_filter.insertion.get_connection')
    def test_insert_record_db_connection_failed(self, mock_get_conn):
        """Test insertion when database connection fails"""
        mock_get_conn.return_value = None

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = examples['table_double'][0]
        success, msg = insert_record(data, 'sensor_data', stats)
        
        assert success is False
        assert msg == "Database connection failed"
        assert stats['successful_inserts'] == 0
        assert stats['failed_inserts'] == 0

    @patch('aware_filter.insertion.transform_and_write')
    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_mysql_error_on_fallback(self, mock_get_conn, mock_transform, table_type, data_list):
        """Test insertion when MySQL error occurs on fallback to original table"""
        mock_transform.return_value = (False, "Transformed table does not exist")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Duplicate entry")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        success, msg = insert_record(data, table_type, stats)
        
        assert success is False
        assert "Duplicate entry" in msg
        assert stats['successful_inserts'] == 0
        assert stats['failed_inserts'] == 1

    @patch('aware_filter.insertion.transform_and_write')
    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_builds_correct_query(self, mock_get_conn, mock_transform, table_type, data_list):
        """Test that the INSERT query is built correctly for fallback to original table"""
        mock_transform.return_value = (False, "Transformed table does not exist")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        insert_record(data, table_type, stats)
        
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert f'INSERT INTO `{table_type}`' in query
        assert '`device_id`' in query
        assert '`timestamp`' in query
        
        # Verify table-specific columns
        if table_type == 'sensor_data':
            assert '`double_value_0`' in query
        else:
            assert '`text_value`' in query
        
        assert data['device_id'] in params



class TestInsertRecords:
    """Test cases for the insert_records function"""

    @patch('aware_filter.insertion.insert_record')
    def test_insert_records_no_data(self, mock_insert_record):
        """Test with no data"""
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        success, response = insert_records(None, 'sensor_data', stats)
        
        assert success is False
        assert response['error'] == 'no data'

    @patch('aware_filter.insertion.insert_record')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_records_single_record_success(self, mock_insert_record, table_type, data_list):
        """Test inserting a single record successfully for both data types"""
        mock_insert_record.return_value = (True, "Data inserted successfully")
        
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        success, response = insert_records(data, table_type, stats)
        
        assert success is True
        assert response['status'] == 'ok'

    @patch('aware_filter.insertion.insert_record')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_records_single_record_failure(self, mock_insert_record, table_type, data_list):
        """Test inserting a single record that fails for both data types"""
        mock_insert_record.return_value = (False, "Duplicate entry")
        
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        success, response = insert_records(data, table_type, stats)
        
        assert success is False
        assert response['error'] == 'Duplicate entry'

    @patch('aware_filter.insertion.insert_record')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_records_multiple_records(self, mock_insert_record, table_type, data_list):
        """Test inserting multiple records for both data types"""
        mock_insert_record.return_value = (True, "Data inserted successfully")
        
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list  # Use all examples for this table type
        success, response = insert_records(data, table_type, stats)
        
        assert success is True
        assert response['status'] == 'ok'
        assert response['inserted'] == len(data)
        assert response['errors'] == 0
        assert mock_insert_record.call_count == len(data)

    @patch('aware_filter.insertion.insert_record')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_records_partial_failure(self, mock_insert_record, table_type, data_list):
        """Test inserting multiple records with some failures for both data types"""
        mock_insert_record.side_effect = [(True, "Success"), (False, "Duplicate entry")]
        
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list  # Use all examples for this table type
        success, response = insert_records(data, table_type, stats)
        
        assert success is True
        assert response['status'] == 'ok'
        assert response['inserted'] == 1
        assert response['errors'] == 1


class TestGetDeviceUid:
    """Test cases for the get_device_uid function"""

    @patch('aware_filter.insertion.get_connection')
    def test_get_device_uid_success(self, mock_get_conn):
        """Test successful device_uid lookup"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {'id': 'uid_12345'}
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, device_uid, error_msg = get_device_uid('device_123')
        
        assert success is True
        assert device_uid == 'uid_12345'
        assert error_msg is None
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()

    @patch('aware_filter.insertion.get_connection')
    def test_get_device_uid_not_found(self, mock_get_conn):
        """Test device_uid lookup when device not found"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, device_uid, error_msg = get_device_uid('device_nonexistent')
        
        assert success is False
        assert device_uid is None
        assert 'not found' in error_msg

    @patch('aware_filter.insertion.get_connection')
    def test_get_device_uid_connection_failed(self, mock_get_conn):
        """Test device_uid lookup when database connection fails"""
        mock_get_conn.return_value = None

        success, device_uid, error_msg = get_device_uid('device_123')
        
        assert success is False
        assert device_uid is None
        assert 'connection failed' in error_msg

    @patch('aware_filter.insertion.get_connection')
    def test_get_device_uid_database_error(self, mock_get_conn):
        """Test device_uid lookup when database error occurs"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Connection lost")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, device_uid, error_msg = get_device_uid('device_123')
        
        assert success is False
        assert device_uid is None
        assert 'Connection lost' in error_msg


class TestTransformAndWrite:
    """Test cases for the transform_and_write function"""

    @patch('aware_filter.insertion.get_device_uid')
    @patch('aware_filter.insertion.get_connection')
    def test_transform_and_write_success(self, mock_get_conn, mock_get_device_uid):
        """Test successful transformation and write to transformed table"""
        mock_get_device_uid.return_value = (True, 'uid_12345', None)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # First cursor for checking table existence, second for insert
        mock_conn.cursor.side_effect = [mock_cursor, mock_cursor]
        mock_get_conn.return_value = mock_conn

        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        stats = {'successful_transforms': 0, 'transformation_failures': 0}
        success, error_msg = transform_and_write(record, 'sensor_data', stats)
        
        assert success is True
        assert error_msg is None
        assert stats['successful_transforms'] == 1
        # Verify device_uid was looked up
        mock_get_device_uid.assert_called_once_with('device_123')

    @patch('aware_filter.insertion.get_connection')
    def test_transform_and_write_no_device_id(self, mock_get_conn):
        """Test transformation skipped when record has no device_id"""
        record = {
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        stats = {}
        success, error_msg = transform_and_write(record, 'sensor_data', stats)
        
        assert success is True
        assert error_msg is None
        mock_get_conn.assert_not_called()

    @patch('aware_filter.insertion.get_connection')
    def test_transform_and_write_transformed_table_not_exists(self, mock_get_conn):
        """Test transformation returns False when transformed table doesn't exist"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Table doesn't exist")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        stats = {}
        success, error_msg = transform_and_write(record, 'sensor_data', stats)
        
        assert success is False
        assert error_msg == "Transformed table does not exist"

    @patch('aware_filter.insertion.get_device_uid')
    @patch('aware_filter.insertion.get_connection')
    def test_transform_and_write_device_lookup_fails(self, mock_get_conn, mock_get_device_uid):
        """Test transformation when device_uid lookup fails"""
        mock_get_device_uid.return_value = (False, None, "Device not found")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        record = {
            'device_id': 'device_nonexistent',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        stats = {'transformation_failures': 0}
        success, error_msg = transform_and_write(record, 'sensor_data', stats)
        
        assert success is False
        assert error_msg == "Device not found"
        assert stats['transformation_failures'] == 1

    @patch('aware_filter.insertion.get_device_uid')
    @patch('aware_filter.insertion.get_connection')
    def test_transform_and_write_insert_fails(self, mock_get_conn, mock_get_device_uid):
        """Test transformation when insert to transformed table fails"""
        mock_get_device_uid.return_value = (True, 'uid_12345', None)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        # First for checking table, second for insert
        mock_cursor.execute.side_effect = [None, MySQLError("Duplicate entry")]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        stats = {'transformation_failures': 0}
        success, error_msg = transform_and_write(record, 'sensor_data', stats)
        
        assert success is False
        assert 'Duplicate entry' in error_msg
        assert stats['transformation_failures'] == 1

    @patch('aware_filter.insertion.get_device_uid')
    @patch('aware_filter.insertion.get_connection')
    def test_transform_and_write_record_transformation_preserves_fields(self, mock_get_conn, mock_get_device_uid):
        """Test that transformation preserves all fields except device_id and adds device_uid"""
        mock_get_device_uid.return_value = (True, 'uid_12345', None)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5,
            'double_value_1': 42.1,
            'accuracy': 10
        }
        
        stats = {}
        success, error_msg = transform_and_write(record, 'sensor_data', stats)
        
        assert success is True
        
        # Verify the insert query contains all expected fields
        call_args = mock_cursor.execute.call_args_list[1]  # Second call is the insert
        query = call_args[0][0]
        params = call_args[0][1]
        
        # Should have all original fields except device_id, plus device_uid
        assert '`device_uid`' in query
        assert '`device_id`' not in query
        assert '`timestamp`' in query
        assert '`double_value_0`' in query
        assert '`double_value_1`' in query
        assert '`accuracy`' in query
        
        # Verify device_uid is in params
        assert 'uid_12345' in params
        # Verify original device_id is NOT in params
        assert 'device_123' not in params


class TestInsertRecordWithTransformation:
    """Integration tests for insert_record with transformation"""

    @patch('aware_filter.insertion.transform_and_write')
    @patch('aware_filter.insertion.get_connection')
    def test_insert_record_uses_transformed_table_if_available(self, mock_get_conn, mock_transform):
        """Test that insert_record writes to transformed table if it exists"""
        mock_transform.return_value = (True, None)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        success, msg = insert_record(record, 'sensor_data', stats)
        
        assert success is True
        assert 'transformed table' in msg
        mock_transform.assert_called_once_with(record, 'sensor_data', stats)

    @patch('aware_filter.insertion.get_connection')
    @patch('aware_filter.insertion.transform_and_write')
    def test_insert_record_falls_back_to_original_on_transform_failure(self, mock_transform, mock_get_conn):
        """Test that insert_record falls back to original table when transform fails"""
        mock_transform.return_value = (False, "Transformed table does not exist")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        success, msg = insert_record(record, 'sensor_data', stats)
        
        assert success is True
        assert stats['successful_inserts'] == 1
        mock_transform.assert_called_once_with(record, 'sensor_data', stats)
        # Verify insert into original table was called
        mock_cursor.execute.assert_called_once()

    @patch('aware_filter.insertion.get_connection')
    @patch('aware_filter.insertion.transform_and_write')
    def test_insert_record_original_table_fails(self, mock_transform, mock_get_conn):
        """Test insertion failure when both transformed and original table writes fail"""
        mock_transform.return_value = (False, "Transformed table does not exist")
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Duplicate entry")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        success, msg = insert_record(record, 'sensor_data', stats)
        
        assert success is False
        assert stats['failed_inserts'] == 1
        assert 'Duplicate entry' in msg

    @patch('aware_filter.insertion.get_connection')
    @patch('aware_filter.insertion.transform_and_write')
    def test_insert_record_does_not_insert_original_if_transform_succeeds(self, mock_transform, mock_get_conn):
        """Test that original table is NOT written if transformed table write succeeds"""
        mock_transform.return_value = (True, None)
        
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        record = {
            'device_id': 'device_123',
            'timestamp': 1706342400000,
            'double_value_0': 23.5
        }
        
        success, msg = insert_record(record, 'sensor_data', stats)
        
        assert success is True
        # Original table insert should not be called when transform succeeds
        mock_cursor.execute.assert_not_called()
        # Stats should reflect that insert didn't happen on original table
        assert stats['successful_inserts'] == 0


class TestApplyRateLimit:
    """Test cases for the apply_rate_limit function"""

    def test_apply_rate_limit_empty_data(self):
        """Test rate limiting with empty data list"""
        data = []
        
        result = apply_rate_limit(data, 'sensor_data')
        
        assert result == []
        assert len(result) == 0

    def test_apply_rate_limit_single_record(self):
        """Test rate limiting with a single record"""
        data = [
            {'device_id': 'device_123', 'timestamp': 1706342400000, 'value': 23.5}
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        assert len(result) == 1
        assert result[0] == data[0]

    def test_apply_rate_limit_general_limit_applied(self):
        """Test that general rate limit (200000 microseconds / 5 Hz) is applied correctly"""
        # General rate limit is 200000 microseconds (5 Hz)
        base_time = 1706342400000
        data = [
            {'device_id': 'device_123', 'timestamp': base_time, 'value': 23.5},
            {'device_id': 'device_123', 'timestamp': base_time + 100000, 'value': 25.0},  # Below limit - filtered
            {'device_id': 'device_123', 'timestamp': base_time + 200000, 'value': 26.0},  # At limit - kept
            {'device_id': 'device_123', 'timestamp': base_time + 300000, 'value': 27.0},  # Below limit from last kept - filtered
        ]
        
        result = apply_rate_limit(data, 'text_events')  # Use table without specific limit
        
        # Should keep records at index 0 and 2 (timestamps at 0 and 200000)
        assert len(result) == 2
        assert result[0]['value'] == 23.5
        assert result[1]['value'] == 26.0

    def test_apply_rate_limit_specific_table_limit(self):
        """Test that table-specific rate limit overrides general limit"""
        # accelerometer has specific rate limit of 200000 microseconds
        base_time = 1706342400000
        data = [
            {'device_id': 'device_123', 'timestamp': base_time, 'value': 23.5},
            {'device_id': 'device_123', 'timestamp': base_time + 100000, 'value': 25.0},  # Below limit - filtered
            {'device_id': 'device_123', 'timestamp': base_time + 200000, 'value': 26.0},  # At limit - kept
            {'device_id': 'device_123', 'timestamp': base_time + 400000, 'value': 27.0},  # Above limit - kept
        ]
        
        result = apply_rate_limit(data, 'accelerometer')
        
        assert len(result) == 3
        assert result[0]['value'] == 23.5
        assert result[1]['value'] == 26.0
        assert result[2]['value'] == 27.0

    def test_apply_rate_limit_high_frequency_data_filtered(self):
        """Test that high-frequency data below rate limit is filtered out"""
        # Generate data at 100 Hz (100000 microseconds per sample)
        # With 200000 microsecond limit, should keep every 2nd+ sample
        base_time = 1706342400000
        data = [
            {'device_id': 'device_123', 'timestamp': base_time + i * 100000, 'value': float(i)}
            for i in range(10)
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        # Should keep every 2nd or more sample (at 200000 microsecond intervals or more)
        # Timestamps: 0, 100k, 200k, 300k, 400k, 500k, 600k, 700k, 800k, 900k
        # Keep: 0, 200k (i=2), 400k (i=4), 600k (i=6), 800k (i=8) = 5 records
        assert len(result) == 5
        assert result[0]['value'] == 0.0
        assert result[1]['value'] == 2.0
        assert result[2]['value'] == 4.0
        assert result[3]['value'] == 6.0
        assert result[4]['value'] == 8.0

    def test_apply_rate_limit_preserves_record_structure(self):
        """Test that rate limiting preserves all fields in records"""
        base_time = 1706342400000
        data = [
            {
                'device_id': 'device_123',
                'timestamp': base_time,
                'value': 23.5,
                'accuracy': 10,
                'extra_field': 'preserved'
            },
            {
                'device_id': 'device_123',
                'timestamp': base_time + 200000,
                'value': 26.0,
                'accuracy': 12,
                'extra_field': 'also_preserved'
            }
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        assert len(result) == 2
        assert result[0]['accuracy'] == 10
        assert result[0]['extra_field'] == 'preserved'
        assert result[1]['accuracy'] == 12
        assert result[1]['extra_field'] == 'also_preserved'

    def test_apply_rate_limit_mixed_intervals(self):
        """Test rate limiting with mixed time intervals"""
        base_time = 1706342400000
        data = [
            {'device_id': 'device_123', 'timestamp': base_time, 'value': 1.0},
            {'device_id': 'device_123', 'timestamp': base_time + 50000, 'value': 2.0},  # Below limit - filtered
            {'device_id': 'device_123', 'timestamp': base_time + 150000, 'value': 3.0},  # Below limit - filtered
            {'device_id': 'device_123', 'timestamp': base_time + 350000, 'value': 4.0},  # At limit (200k from start) - kept
            {'device_id': 'device_123', 'timestamp': base_time + 400000, 'value': 5.0},  # Below limit (50k from last kept) - filtered
            {'device_id': 'device_123', 'timestamp': base_time + 600000, 'value': 6.0},  # Above limit (250k from last kept) - kept
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        # Should keep records at indices 0, 3, 5
        assert len(result) == 3
        assert result[0]['value'] == 1.0
        assert result[1]['value'] == 4.0
        assert result[2]['value'] == 6.0

    def test_apply_rate_limit_no_timestamp_in_single_record(self):
        """Test rate limiting when records are missing timestamp"""
        data = [
            {'device_id': 'device_123', 'value': 23.5}
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        # Should return data as-is when timestamp is missing
        assert result == data

    def test_apply_rate_limit_large_dataset(self):
        """Test rate limiting with a large dataset"""
        base_time = 1706342400000
        # Create 1000 records at 10kHz (100 microsecond intervals)
        data = [
            {'device_id': 'device_123', 'timestamp': base_time + i * 100, 'value': float(i)}
            for i in range(1000)
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        # With 200000 microsecond limit and 100 microsecond intervals,
        # should keep approximately every 2000th record (200000/100)
        # Starting record + every 2000th after = first, then 2000, then would be 4000 (out of range)
        assert len(result) == 1  # Only first record at 0 and none within the data due to the math
        assert result[0]['value'] == 0.0

    def test_apply_rate_limit_duplicate_timestamps(self):
        """Test rate limiting when records have the same or very close timestamps"""
        base_time = 1706342400000
        data = [
            {'device_id': 'device_123', 'timestamp': base_time, 'value': 1.0},
            {'device_id': 'device_123', 'timestamp': base_time, 'value': 2.0},  # Same timestamp - filtered
            {'device_id': 'device_123', 'timestamp': base_time + 200000, 'value': 3.0},  # At limit - kept
            {'device_id': 'device_123', 'timestamp': base_time + 200000, 'value': 4.0},  # Same as last - filtered
        ]
        
        result = apply_rate_limit(data, 'sensor_data')
        
        # Should keep records 0 and 2 (first, then one at 200k)
        assert len(result) == 2
        assert result[0]['value'] == 1.0
        assert result[1]['value'] == 3.0
