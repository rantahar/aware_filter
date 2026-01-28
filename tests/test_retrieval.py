"""Tests for data retrieval module"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from mysql.connector import Error as MySQLError
from aware_filter.retrieval import query_table, table_has_data, query_data, get_tables_for_devices


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


class TestQueryTable:
    """Test cases for the query_table function"""

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_table_with_device_id(self, mock_get_conn, table_type, data_list):
        """Test retrieving data with device_id filter for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        device_id = data_list[0]['device_id']
        conditions = ['`device_id` = %s']
        params = [device_id]
        success, response, status = query_table(table_type, conditions, params)

        assert success is True
        assert status == 200
        assert response['count'] == len(data_list)
        assert response['data'][0]['device_id'] == device_id
        assert response['total_count'] == len(data_list)  # Check new pagination field
        assert 'limit' in response  # Check pagination metadata
        assert 'offset' in response
        assert 'has_more' in response
        mock_cursor.close.assert_called_once()

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_table_no_conditions(self, mock_get_conn, table_type, data_list):
        """Test retrieving all data without conditions for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, response, status = query_table(table_type)

        assert success is True
        assert status == 200
        assert response['count'] == len(data_list)
        assert response['total_count'] == len(data_list)  # Check new pagination field
        assert 'limit' in response  # Check pagination metadata
        assert 'offset' in response
        assert 'has_more' in response

    @patch('aware_filter.retrieval.get_connection')
    def test_query_table_missing_table(self, mock_get_conn):
        """Test missing table parameter"""
        success, response, status = query_table(None)

        assert success is False
        assert status == 400
        assert 'missing table name' in response['error']

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_table_with_time_filters(self, mock_get_conn, table_type, data_list):
        """Test retrieving data with time range filters for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        start_time = data_list[0]['timestamp']
        end_time = data_list[-1]['timestamp']
        conditions = ['`timestamp` >= %s', '`timestamp` <= %s']
        params = [start_time, end_time]
        
        success, response, status = query_table(table_type, conditions, params)
        
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
        params_used = main_query_call[0][1]
        
        assert '`timestamp` >=' in query
        assert '`timestamp` <=' in query
        assert start_time in params_used
        assert end_time in params_used

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type", ['sensor_data', 'text_events'])
    def test_query_table_db_connection_failed(self, mock_get_conn, table_type):
        """Test handling of database connection failure for both data types"""
        mock_get_conn.return_value = None

        success, response, status = query_table(table_type)
        
        assert success is False
        assert status == 503
        assert 'database connection failed' in response['error']

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type", ['sensor_data', 'text_events'])
    def test_query_table_mysql_error(self, mock_get_conn, table_type):
        """Test handling of MySQL errors for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Table not found")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, response, status = query_table(table_type)
        
        assert success is False
        assert status == 500
        assert 'Table not found' in response['error']
        mock_cursor.close.assert_called_once()

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type", ['sensor_data', 'text_events'])
    def test_query_table_empty_result(self, mock_get_conn, table_type):
        """Test retrieving data when no records match for both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_cursor.fetchone.return_value = {'total': 0}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, response, status = query_table(table_type)
        
        assert success is True
        assert status == 200
        assert response['count'] == 0
        assert response['data'] == []
        assert response['total_count'] == 0
        assert 'has_more' in response

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_table_multiple_results(self, mock_get_conn, table_type, data_list):
        """Test retrieving multiple records from both data types"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = data_list
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, response, status = query_table(table_type)

        assert success is True
        assert status == 200
        assert response['count'] == len(data_list)
        assert len(response['data']) == len(data_list)
        assert response['total_count'] == len(data_list)
        assert 'has_more' in response

    @patch('aware_filter.retrieval.get_connection')
    @pytest.mark.parametrize("table_type,data_list", [
        ('sensor_data', examples['table_double']),
        ('text_events', examples['table_text'])
    ])
    def test_query_table_with_pagination(self, mock_get_conn, table_type, data_list):
        """Test retrieving data with limit and offset pagination"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [data_list[0]]
        mock_cursor.fetchone.return_value = {'total': len(data_list)}  # Mock the count query
        mock_conn.cursor.return_value = mock_cursor
        mock_get_conn.return_value = mock_conn

        success, response, status = query_table(table_type, limit=1, offset=0)

        assert success is True
        assert status == 200
        assert response['count'] == 1
        assert response['limit'] == 1
        assert response['offset'] == 0
        assert response['total_count'] == len(data_list)
        assert response['has_more'] is True  # Since total is 2 but we only returned 1

    @patch('aware_filter.retrieval.get_connection')
    def test_query_table_limit_exceeds_max(self, mock_get_conn):
        """Test that limit exceeding MAX_LIMIT is rejected"""
        success, response, status = query_table('sensor_data', limit=100000)

        assert success is False
        assert status == 400
        assert 'limit cannot exceed' in response['error']


class TestTableHasData:
    """Test cases for the table_has_data function"""

    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_with_existing_data(self, mock_get_connection):
        """Test table_has_data returns True when data exists"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)  # Row exists
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table', ['`device_id` = %s'], ['123'])
        
        # Assert
        assert success is True
        assert has_data is True
        assert status_code == 200
        mock_cursor.execute.assert_called_once()
        mock_cursor.close.assert_called_once()
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_with_no_data(self, mock_get_connection):
        """Test table_has_data returns False when no data exists"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = None  # No rows
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table', ['`device_id` = %s'], ['123'])
        
        # Assert
        assert success is True
        assert has_data is False
        assert status_code == 200
        mock_cursor.execute.assert_called_once()
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_without_conditions(self, mock_get_connection):
        """Test table_has_data without WHERE conditions"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table')
        
        # Assert
        assert success is True
        assert has_data is True
        assert status_code == 200
        # Check that query was executed without WHERE clause
        call_args = mock_cursor.execute.call_args
        assert 'WHERE' not in call_args[0][0]
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_with_multiple_conditions(self, mock_get_connection):
        """Test table_has_data with multiple WHERE conditions"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data(
            'test_table',
            ['`device_id` = %s', '`timestamp` >= %s'],
            ['123', '2024-01-01']
        )
        
        # Assert
        assert success is True
        assert has_data is True
        # Verify both parameters were passed
        mock_cursor.execute.assert_called_once()
        call_args = mock_cursor.execute.call_args[0]
        assert call_args[1] == ['123', '2024-01-01']
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_with_invalid_table_name(self, mock_get_connection):
        """Test table_has_data with empty table name"""
        # Test
        success, has_data, status_code = table_has_data('')
        
        # Assert
        assert success is False
        assert has_data is False
        assert status_code == 400
        mock_get_connection.assert_not_called()
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_with_no_connection(self, mock_get_connection):
        """Test table_has_data when database connection fails"""
        # Setup mock
        mock_get_connection.return_value = None
        
        # Test
        success, has_data, status_code = table_has_data('test_table')
        
        # Assert
        assert success is False
        assert has_data is False
        assert status_code == 503
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_with_database_error(self, mock_get_connection):
        """Test table_has_data when database query fails"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Query error")
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table', ['`device_id` = %s'], ['123'])
        
        # Assert
        assert success is False
        assert has_data is False
        assert status_code == 500
        mock_cursor.close.assert_called_once()
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_closes_cursor_on_success(self, mock_get_connection):
        """Test that cursor is closed even on success"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table')
        
        # Assert
        mock_cursor.close.assert_called_once()
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_closes_cursor_on_error(self, mock_get_connection):
        """Test that cursor is closed even on error"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Query error")
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table')
        
        # Assert
        mock_cursor.close.assert_called_once()
    
    @patch('aware_filter.retrieval.get_connection')
    def test_table_has_data_uses_select_1_for_efficiency(self, mock_get_connection):
        """Test that table_has_data uses SELECT 1 instead of SELECT *"""
        # Setup mock
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (1,)
        mock_connection = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_get_connection.return_value = mock_connection
        
        # Test
        success, has_data, status_code = table_has_data('test_table', ['`device_id` = %s'], ['123'])
        
        # Assert
        call_args = mock_cursor.execute.call_args[0][0]
        assert 'SELECT 1' in call_args
        assert 'SELECT *' not in call_args

class TestQueryData:
    """Test cases for the query_data function"""

    @patch('aware_filter.retrieval.query_table')
    def test_query_data_single_device(self, mock_query_table):
        """Test query_data with a single device_id"""
        # Mock the device_lookup query
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        # Mock the main table query
        main_table_response = {
            'data': [
                {'device_id': 'device_123', 'timestamp': 1706342400000, 'value': 23.5},
                {'device_id': 'device_123', 'timestamp': 1706428800000, 'value': 25.0}
            ],
            'count': 2,
            'total_count': 2,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        # Set up mock to return different values based on table name
        def query_side_effect(table_name, conditions, params, limit=None, offset=None):
            if table_name == 'device_lookup':
                return True, device_lookup_response, 200
            elif table_name == 'sensor_data':
                return True, main_table_response, 200
            return False, {}, 404
        
        mock_query_table.side_effect = query_side_effect
        
        # Create mock request args
        mock_request_args = {
            'table': 'sensor_data',
            'device_id': 'device_123'
        }
        
        # Test
        success, response, status = query_data('sensor_data', mock_request_args)
        
        # Assert
        assert success is True
        assert status == 200
        assert response['count'] == 2
        assert response['total_count'] == 2
        assert response['data'][0]['device_id'] == 'device_123'

    @patch('aware_filter.retrieval.query_table')
    def test_query_data_multiple_devices(self, mock_query_table):
        """Test query_data with multiple device_ids"""
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        main_table_response = {
            'data': [
                {'device_id': 'device_123', 'timestamp': 1706342400000, 'value': 23.5},
                {'device_id': 'device_456', 'timestamp': 1706342400000, 'value': 30.0}
            ],
            'count': 2,
            'total_count': 2,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        def query_side_effect(table_name, conditions, params, limit=None, offset=None):
            if table_name == 'device_lookup':
                return True, device_lookup_response, 200
            elif table_name == 'sensor_data':
                return True, main_table_response, 200
            return False, {}, 404
        
        mock_query_table.side_effect = query_side_effect
        
        mock_request_args = {
            'table': 'sensor_data',
            'device_id': 'device_123,device_456'
        }
        
        # Test
        success, response, status = query_data('sensor_data', mock_request_args)
        
        # Assert
        assert success is True
        assert status == 200
        assert response['count'] == 2

    @patch('aware_filter.retrieval.query_table')
    def test_query_data_with_time_filters(self, mock_query_table):
        """Test query_data with time range filters"""
        main_table_response = {
            'data': [
                {'device_id': 'device_123', 'timestamp': 1706342400000, 'value': 23.5},
                {'device_id': 'device_123', 'timestamp': 1706428800000, 'value': 25.0}
            ],
            'count': 2,
            'total_count': 2,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        mock_query_table.return_value = (True, main_table_response, 200)
        
        mock_request_args = {
            'table': 'sensor_data',
            'start_time': '1706342400000',
            'end_time': '1706428800000'
        }
        
        # Test
        success, response, status = query_data('sensor_data', mock_request_args)
        
        # Assert
        assert success is True
        assert status == 200
        assert response['count'] == 2
        
        # Verify query_table was called with time conditions
        calls = mock_query_table.call_args_list
        assert any('`timestamp` >=' in str(call) for call in calls)

    @patch('aware_filter.retrieval.query_table')
    def test_query_data_invalid_limit(self, mock_query_table):
        """Test query_data with invalid limit parameter"""
        mock_request_args = {
            'table': 'sensor_data',
            'limit': 'invalid'
        }
        
        # Test
        success, response, status = query_data('sensor_data', mock_request_args)
        
        # Assert
        assert success is False
        assert status == 400
        assert 'valid integer' in response['error']

    @patch('aware_filter.retrieval.query_table')
    def test_query_data_with_pagination(self, mock_query_table):
        """Test query_data with limit and offset parameters"""
        main_table_response = {
            'data': [
                {'device_id': 'device_123', 'timestamp': 1706342400000, 'value': 23.5}
            ],
            'count': 1,
            'total_count': 10,
            'limit': 1,
            'offset': 0,
            'has_more': True
        }
        
        mock_query_table.return_value = (True, main_table_response, 200)
        
        mock_request_args = {
            'table': 'sensor_data',
            'limit': '1',
            'offset': '0'
        }
        
        # Test
        success, response, status = query_data('sensor_data', mock_request_args)
        
        # Assert
        assert success is True
        assert status == 200
        assert response['count'] == 1
        assert response['limit'] == 1
        assert response['offset'] == 0

    @patch('aware_filter.retrieval.query_table')
    def test_query_data_transformed_table_with_device_uid(self, mock_query_table):
        """Test query_data queries both original and transformed tables"""
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        original_response = {
            'data': [{'device_id': 'device_123', 'timestamp': 1706342400000, 'value': 23.5}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        transformed_response = {
            'data': [{'device_uid': 'uuid_123', 'timestamp': 1706428800000, 'value': 25.0}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        
        def query_side_effect(table_name, conditions, params, limit=None, offset=None):
            if table_name == 'device_lookup':
                return True, device_lookup_response, 200
            elif table_name == 'sensor_data':
                return True, original_response, 200
            elif table_name == 'sensor_data_transformed':
                return True, transformed_response, 200
            return False, {}, 404
        
        mock_query_table.side_effect = query_side_effect
        
        mock_request_args = {
            'table': 'sensor_data',
            'device_id': 'device_123'
        }
        
        # Test
        success, response, status = query_data('sensor_data', mock_request_args)
        
        # Assert
        assert success is True
        assert status == 200
        # Should have combined data from both tables
        assert response['count'] == 2
        assert response['total_count'] == 2


class TestGetTablesForDevices:
    """Test cases for the get_tables_for_devices function"""

    @patch('aware_filter.retrieval.table_has_data')
    @patch('aware_filter.retrieval.get_all_tables')
    @patch('aware_filter.retrieval.query_table')
    def test_get_tables_for_single_device(self, mock_query_table, mock_get_all_tables, mock_table_has_data):
        """Test get_tables_for_devices with a single device"""
        # Mock device_lookup response
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        mock_query_table.return_value = (True, device_lookup_response, 200)
        
        # Mock all tables
        mock_get_all_tables.return_value = (True, ['device_lookup', 'sensor_data', 'gps_data'], 200)
        
        # Mock table_has_data to return data found
        mock_table_has_data.return_value = (True, True, 200)
        
        # Test
        requested_ids = ['device_123']
        success, response, status = get_tables_for_devices(requested_ids)
        
        # Assert
        assert success is True
        assert status == 200
        assert 'device_ids' in response
        assert 'device_uid_map' in response
        assert 'tables_with_data' in response
        assert response['device_ids'] == requested_ids
        assert 'device_123' in response['device_uid_map']
        assert response['device_uid_map']['device_123'] == 'uuid_123'

    @patch('aware_filter.retrieval.table_has_data')
    @patch('aware_filter.retrieval.get_all_tables')
    @patch('aware_filter.retrieval.query_table')
    def test_get_tables_for_multiple_devices(self, mock_query_table, mock_get_all_tables, mock_table_has_data):
        """Test get_tables_for_devices with multiple devices"""
        def device_lookup_side_effect(table_name, conditions, params):
            device_map = {
                'device_123': 'uuid_123',
                'device_456': 'uuid_456'
            }
            device_id = params[0]
            if device_id in device_map:
                return True, {
                    'data': [{'id': device_map[device_id]}],
                    'count': 1,
                    'total_count': 1,
                    'limit': 10000,
                    'offset': 0,
                    'has_more': False
                }, 200
            return False, {'data': []}, 404
        
        mock_query_table.side_effect = device_lookup_side_effect
        mock_get_all_tables.return_value = (True, ['device_lookup', 'sensor_data'], 200)
        mock_table_has_data.return_value = (True, True, 200)
        
        # Test
        requested_ids = ['device_123', 'device_456']
        success, response, status = get_tables_for_devices(requested_ids)
        
        # Assert
        assert success is True
        assert status == 200
        assert len(response['device_uid_map']) == 2
        assert response['device_uid_map']['device_123'] == 'uuid_123'
        assert response['device_uid_map']['device_456'] == 'uuid_456'

    @patch('aware_filter.retrieval.query_table')
    def test_get_tables_for_devices_not_found(self, mock_query_table):
        """Test get_tables_for_devices when device not found in lookup"""
        mock_query_table.return_value = (True, {'data': []}, 200)
        
        # Test
        requested_ids = ['nonexistent_device']
        success, response, status = get_tables_for_devices(requested_ids)
        
        # Assert
        assert success is False
        assert status == 404
        assert 'device_ids not found' in response['error']
        assert response['found_count'] == 0

    @patch('aware_filter.retrieval.table_has_data')
    @patch('aware_filter.retrieval.get_all_tables')
    @patch('aware_filter.retrieval.query_table')
    def test_get_tables_for_devices_skips_system_tables(self, mock_query_table, mock_get_all_tables, mock_table_has_data):
        """Test that get_tables_for_devices skips system tables"""
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        mock_query_table.return_value = (True, device_lookup_response, 200)
        
        # Include system tables that should be skipped
        all_tables = [
            'device_lookup', 'aware_device', 'aware_log', 'mqtt_history',
            'sensor_data', 'gps_data'
        ]
        mock_get_all_tables.return_value = (True, all_tables, 200)
        mock_table_has_data.return_value = (True, True, 200)
        
        # Test
        requested_ids = ['device_123']
        success, response, status = get_tables_for_devices(requested_ids)
        
        # Assert
        assert success is True
        # Verify that table_has_data was called only for non-system tables
        calls = mock_table_has_data.call_args_list
        called_tables = [call[0][0] for call in calls]
        
        # Should not be called for system tables
        assert 'device_lookup' not in called_tables
        assert 'aware_device' not in called_tables
        assert 'aware_log' not in called_tables

    @patch('aware_filter.retrieval.table_has_data')
    @patch('aware_filter.retrieval.get_all_tables')
    @patch('aware_filter.retrieval.query_table')
    def test_get_tables_for_devices_matches_by_type(self, mock_query_table, mock_get_all_tables, mock_table_has_data):
        """Test that get_tables_for_devices tracks match type (device_id vs device_uid)"""
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        mock_query_table.return_value = (True, device_lookup_response, 200)
        
        all_tables = ['device_lookup', 'sensor_data', 'sensor_data_transformed']
        mock_get_all_tables.return_value = (True, all_tables, 200)
        mock_table_has_data.return_value = (True, True, 200)
        
        # Test
        requested_ids = ['device_123']
        success, response, status = get_tables_for_devices(requested_ids)
        
        # Assert
        assert success is True
        tables_with_data = response['tables_with_data']
        
        # Verify tables are returned with matched_by field
        for table_entry in tables_with_data:
            assert 'table' in table_entry
            assert 'matched_by' in table_entry
            assert 'device_ids_matched' in table_entry

    @patch('aware_filter.retrieval.table_has_data')
    @patch('aware_filter.retrieval.get_all_tables')
    @patch('aware_filter.retrieval.query_table')
    def test_get_tables_for_devices_removes_transformed_suffix(self, mock_query_table, mock_get_all_tables, mock_table_has_data):
        """Test that get_tables_for_devices removes _transformed suffix from table names"""
        device_lookup_response = {
            'data': [{'id': 'uuid_123'}],
            'count': 1,
            'total_count': 1,
            'limit': 10000,
            'offset': 0,
            'has_more': False
        }
        mock_query_table.return_value = (True, device_lookup_response, 200)
        
        all_tables = ['device_lookup', 'sensor_data_transformed']
        mock_get_all_tables.return_value = (True, all_tables, 200)
        mock_table_has_data.return_value = (True, True, 200)
        
        # Test
        requested_ids = ['device_123']
        success, response, status = get_tables_for_devices(requested_ids)
        
        # Assert
        assert success is True
        tables_with_data = response['tables_with_data']
        
        # Verify that _transformed suffix is removed from display
        for table_entry in tables_with_data:
            assert not table_entry['table'].endswith('_transformed')
            if 'sensor' in table_entry['table']:
                assert table_entry['table'] == 'sensor_data'