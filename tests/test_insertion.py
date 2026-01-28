"""Tests for data insertion module"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from mysql.connector import Error as MySQLError
from aware_filter.insertion import insert_record, insert_records


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

    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_success(self, mock_get_conn, table_type, data_list):
        """Test successful data insertion for both double values and text data"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]  # Use first example from each table type
        success, msg = insert_record(data, table_type, stats)
        
        assert success is True
        assert msg == "Data inserted successfully"
        assert stats['successful_inserts'] == 1
        assert stats['failed_inserts'] == 0

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()

    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_db_connection_failed(self, mock_get_conn, table_type, data_list):
        """Test insertion when database connection fails for both data types"""
        mock_get_conn.return_value = None

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = data_list[0]
        success, msg = insert_record(data, table_type, stats)
        
        assert success is False
        assert msg == "Database connection failed"
        assert stats['successful_inserts'] == 0
        assert stats['failed_inserts'] == 0

    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_mysql_error(self, mock_get_conn, table_type, data_list):
        """Test insertion when MySQL error occurs for both data types"""
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
        mock_cursor.close.assert_called_once()

    @patch('aware_filter.insertion.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_insert_record_builds_correct_query(self, mock_get_conn, table_type, data_list):
        """Test that the INSERT query is built correctly for both data types"""
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

