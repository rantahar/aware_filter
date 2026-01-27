"""Tests for data retrieval module"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from mysql.connector import Error as MySQLError
from aware_filter.retrieval import query_data


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


class TestQueryData:
    """Test cases for the query_data function"""

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_data_with_device_id(self, mock_get_db, table_type, data_list):
        """Test retrieving data with device_id filter for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        device_id = data_list[0]['device_id']
        success, response, status = query_data(table_type, device_id, None, None, None)

        assert success is True
        assert status == 200
        assert response['count'] == len(data_list)
        assert response['data'][0]['device_id'] == device_id
        assert response['total_count'] == len(data_list)  # Check new pagination field
        assert 'limit' in response  # Check pagination metadata
        assert 'offset' in response
        assert 'has_more' in response
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_data_with_device_uid(self, mock_get_db, table_type, data_list):
        """Test retrieving data with device_uid filter for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        # Use device_id as a UID for testing
        uid = data_list[0]['device_id']
        success, response, status = query_data(table_type, None, uid, None, None)

        assert success is True
        assert status == 200
        assert response['count'] == len(data_list)
        assert response['total_count'] == len(data_list)  # Check new pagination field
        assert 'limit' in response  # Check pagination metadata
        assert 'offset' in response
        assert 'has_more' in response

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_missing_table(self, mock_get_db):
        """Test missing table parameter"""
        success, response, status = query_data(None, '123', None, None, None)

        assert success is False
        assert status == 400
        assert 'missing table parameter' in response['error']

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_missing_device_id_and_uid(self, mock_get_db):
        """Test missing both device_id and device_uid"""
        success, response, status = query_data('sensor_data', None, None, None, None)

        assert success is False
        assert status == 400
        assert 'missing device_id or device_uid' in response['error']

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_data_with_time_filters(self, mock_get_db, table_type, data_list):
        """Test retrieving data with time range filters for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        device_id = data_list[0]['device_id']
        start_time = data_list[0]['timestamp']
        end_time = data_list[-1]['timestamp']
        
        success, response, status = query_data(
            table_type, device_id, None, start_time, end_time
        )
        
        assert success is True
        assert status == 200
        assert response['total_count'] == len(data_list)  # Check new pagination field
        
        # Verify the query was constructed with time filters
        # Note: now there are two execute calls (count + main query)
        call_args_list = mock_cursor.execute.call_args_list
        assert len(call_args_list) >= 1  # At least one call for the main query
        
        # Check the main query (should be the second call)
        if len(call_args_list) >= 2:
            main_query_call = call_args_list[1]  # Second call is main query
        else:
            main_query_call = call_args_list[0]  # Fallback to first call
            
        query = main_query_call[0][0]
        params = main_query_call[0][1]
        
        assert '`timestamp` >=' in query
        assert '`timestamp` <=' in query
        assert start_time in params
        assert end_time in params

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type", ['sensor_data', 'text_events'])
    def test_query_data_db_connection_failed(self, mock_get_db, table_type):
        """Test handling of database connection failure for both data types"""
        mock_get_db.return_value = None

        success, response, status = query_data(table_type, 'device_123', None, None, None)
        
        assert success is False
        assert status == 503
        assert 'database connection failed' in response['error']

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type", ['sensor_data', 'text_events'])
    def test_query_data_mysql_error(self, mock_get_db, table_type):
        """Test handling of MySQL errors for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Table not found")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data(table_type, 'device_123', None, None, None)
        
        assert success is False
        assert status == 500
        assert 'Table not found' in response['error']
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type", ['sensor_data', 'text_events'])
    def test_query_data_empty_result(self, mock_get_db, table_type):
        """Test retrieving data when no records match for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {'total': 0}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data(table_type, 'device_999', None, None, None)
        
        assert success is True
        assert status == 200
        assert response['count'] == 0§
        assert response['data'] == []
        assert response['total_count'] == 0
        assert 'has_more' in response

    @patch('aware_filter.retrieval.get_db_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_data_multiple_results(self, mock_get_db, table_type, data_list):
        """Test retrieving multiple records from both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        device_id = data_list[0]['device_id']
        success, response, status = query_data(table_type, device_id, None, None, None)

        assert success is True
        assert status == 200
        assert response['count'] == len(data_list)
        assert len(response['data']) == len(data_list)
        assert response['total_count'] == len(data_list)
        assert 'has_more' in response

