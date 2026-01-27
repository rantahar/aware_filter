"""Data retrieval module for AWARE Webservice Receiver"""

import mysql.connector
from mysql.connector import Error
import logging
import os

logger = logging.getLogger(__name__)

DB_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', 3306)),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', 'aware_database'),
}


def get_db_connection():
    """Establish a database connection."""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        return connection
    except Error as e:
        logger.error(f"Error connecting to database: {e}")
        return None


def query_data(table_name, device_id, device_uid, start_time, end_time):
    """
    Query the database for sensor data without Flask dependencies.
    
    Args:
        table_name: Name of the table to query
        device_id: Device ID filter (optional)
        device_uid: Device UID filter (optional)
        start_time: Start timestamp filter (optional)
        end_time: End timestamp filter (optional)
    
    Returns:
        tuple: (success: bool, response_dict: dict, status_code: int)
    """
    if not table_name:
        return False, {'error': 'missing table parameter'}, 400
    
    if not device_id and not device_uid:
        return False, {'error': 'missing device_id or device_uid'}, 400
    
    conn = get_db_connection()
    if conn is None:
        return False, {'error': 'database connection failed'}, 503
    
    try:
        cursor = conn.cursor(dictionary=True)
        
        # Build WHERE clause
        where_conditions = []
        params = []
        
        if device_id:
            where_conditions.append('`device_id` = %s')
            params.append(device_id)
        elif device_uid:
            where_conditions.append('`device_uid` = %s')
            params.append(device_uid)
        
        if start_time:
            where_conditions.append('`timestamp` >= %s')
            params.append(start_time)
        
        if end_time:
            where_conditions.append('`timestamp` <= %s')
            params.append(end_time)
        
        where_clause = ' AND '.join(where_conditions)
        query = f"SELECT * FROM `{table_name}` WHERE {where_clause}"
        
        cursor.execute(query, params)
        results = cursor.fetchall()
        
        logger.info(f"Retrieved {len(results)} records from {table_name}")
        return True, {'data': results, 'count': len(results)}, 200
    
    except Error as e:
        logger.error(f"Error retrieving data: {e}")
        return False, {'error': str(e)}, 500
    finally:
        cursor.close()
        conn.close()
