"""Tests for data retrieval module"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from mysql.connector import Error as MySQLError
from aware_filter.retrieval import query_data


class TestQueryData:
    """Test cases for the query_data function"""

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_with_device_id(self, mock_get_db):
        """Test retrieving data with device_id filter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'device_id': '123', 'timestamp': '2024-01-27', 'value': 42}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data('sensor_data', '123', None, None, None)

        assert success is True
        assert status == 200
        assert response['count'] == 1
        assert response['data'][0]['device_id'] == '123'
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_with_device_uid(self, mock_get_db):
        """Test retrieving data with device_uid filter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'device_uid': 'uid-123', 'timestamp': '2024-01-27', 'value': 42}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data('sensor_data', None, 'uid-123', None, None)

        assert success is True
        assert status == 200
        assert response['count'] == 1

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
    def test_query_data_with_time_filters(self, mock_get_db):
        """Test retrieving data with time range filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'device_id': '123', 'timestamp': '2024-01-27', 'value': 42}
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data(
            'sensor_data', '123', None, '2024-01-01', '2024-01-31'
        )

        assert success is True
        assert status == 200
        
        # Verify the query was constructed with time filters
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]
        
        assert '`timestamp` >=' in query
        assert '`timestamp` <=' in query
        assert '2024-01-01' in params
        assert '2024-01-31' in params

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_db_connection_failed(self, mock_get_db):
        """Test handling of database connection failure"""
        mock_get_db.return_value = None

        success, response, status = query_data('sensor_data', '123', None, None, None)

        assert success is False
        assert status == 503
        assert 'database connection failed' in response['error']

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_mysql_error(self, mock_get_db):
        """Test handling of MySQL errors"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Table not found")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data('sensor_data', '123', None, None, None)

        assert success is False
        assert status == 500
        assert 'Table not found' in response['error']
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_empty_result(self, mock_get_db):
        """Test retrieving data when no records match"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data('sensor_data', '999', None, None, None)

        assert success is True
        assert status == 200
        assert response['count'] == 0
        assert response['data'] == []

    @patch('aware_filter.retrieval.get_db_connection')
    def test_query_data_multiple_results(self, mock_get_db):
        """Test retrieving multiple records"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {'device_id': '123', 'timestamp': '2024-01-27', 'value': 42},
            {'device_id': '123', 'timestamp': '2024-01-28', 'value': 43},
            {'device_id': '123', 'timestamp': '2024-01-29', 'value': 44},
        ]
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        success, response, status = query_data('sensor_data', '123', None, None, None)

        assert success is True
        assert status == 200
        assert response['count'] == 3
        assert len(response['data']) == 3

