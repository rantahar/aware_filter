"""Tests for data insertion module"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
from mysql.connector import Error as MySQLError
from aware_filter.insertion import insert_record, insert_records


class TestInsertRecord:
    """Test cases for the insert_record function"""

    @patch('aware_filter.insertion.get_db_connection')
    def test_insert_record_success(self, mock_get_db):
        """Test successful data insertion"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = {'device_id': '123', 'timestamp': '2024-01-27', 'value': 42}

        success, msg = insert_record(data, 'sensor_data', stats)

        assert success is True
        assert msg == "Data inserted successfully"
        assert stats['successful_inserts'] == 1
        assert stats['failed_inserts'] == 0

        mock_cursor.execute.assert_called_once()
        mock_conn.commit.assert_called_once()
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('aware_filter.insertion.get_db_connection')
    def test_insert_record_db_connection_failed(self, mock_get_db):
        """Test insertion when database connection fails"""
        mock_get_db.return_value = None

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = {'device_id': '123', 'timestamp': '2024-01-27'}

        success, msg = insert_record(data, 'sensor_data', stats)

        assert success is False
        assert msg == "Database connection failed"
        assert stats['successful_inserts'] == 0
        assert stats['failed_inserts'] == 0

    @patch('aware_filter.insertion.get_db_connection')
    def test_insert_record_mysql_error(self, mock_get_db):
        """Test insertion when MySQL error occurs"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.execute.side_effect = MySQLError("Duplicate entry")
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = {'device_id': '123', 'timestamp': '2024-01-27'}

        success, msg = insert_record(data, 'sensor_data', stats)

        assert success is False
        assert "Duplicate entry" in msg
        assert stats['successful_inserts'] == 0
        assert stats['failed_inserts'] == 1
        mock_cursor.close.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch('aware_filter.insertion.get_db_connection')
    def test_insert_record_builds_correct_query(self, mock_get_db):
        """Test that the INSERT query is built correctly"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_get_db.return_value = mock_conn

        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = {'device_id': '123', 'timestamp': '2024-01-27', 'value': 42}

        insert_record(data, 'sensor_data', stats)

        # Verify the query was executed with correct parameters
        call_args = mock_cursor.execute.call_args
        query = call_args[0][0]
        params = call_args[0][1]

        assert 'INSERT INTO `sensor_data`' in query
        assert '`device_id`' in query
        assert '`timestamp`' in query
        assert '`value`' in query
        assert params == ['123', '2024-01-27', 42]


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
    def test_insert_records_single_record_success(self, mock_insert_record):
        """Test inserting a single record successfully"""
        mock_insert_record.return_value = (True, "Data inserted successfully")
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = {'device_id': '123', 'value': 42}

        success, response = insert_records(data, 'sensor_data', stats)

        assert success is True
        assert response['status'] == 'ok'
        mock_insert_record.assert_called_once_with(data, 'sensor_data', stats)

    @patch('aware_filter.insertion.insert_record')
    def test_insert_records_single_record_failure(self, mock_insert_record):
        """Test inserting a single record that fails"""
        mock_insert_record.return_value = (False, "Duplicate entry")
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = {'device_id': '123', 'value': 42}

        success, response = insert_records(data, 'sensor_data', stats)

        assert success is False
        assert response['error'] == 'Duplicate entry'

    @patch('aware_filter.insertion.insert_record')
    def test_insert_records_multiple_records(self, mock_insert_record):
        """Test inserting multiple records"""
        mock_insert_record.return_value = (True, "Data inserted successfully")
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = [
            {'device_id': '123', 'value': 42},
            {'device_id': '124', 'value': 43},
        ]

        success, response = insert_records(data, 'sensor_data', stats)

        assert success is True
        assert response['status'] == 'ok'
        assert response['inserted'] == 2
        assert response['errors'] == 0
        assert mock_insert_record.call_count == 2

    @patch('aware_filter.insertion.insert_record')
    def test_insert_records_partial_failure(self, mock_insert_record):
        """Test inserting multiple records with some failures"""
        mock_insert_record.side_effect = [(True, "Success"), (False, "Duplicate entry")]
        stats = {'successful_inserts': 0, 'failed_inserts': 0}
        data = [
            {'device_id': '123', 'value': 42},
            {'device_id': '124', 'value': 43},
        ]

        success, response = insert_records(data, 'sensor_data', stats)

        assert success is True
        assert response['status'] == 'ok'
        assert response['inserted'] == 1
        assert response['errors'] == 1

