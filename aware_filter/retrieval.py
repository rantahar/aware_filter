"""Data retrieval module for AWARE Webservice Receiver"""

from mysql.connector import Error
import logging
import time
from .connection import get_connection

logger = logging.getLogger(__name__)


def get_all_tables():
    """
    Get list of all tables in the database.
    
    Returns:
        tuple: (success: bool, tables: list, status_code: int)
    """
    conn = get_connection()
    if conn is None:
        return False, [], 503
    
    try:
        cursor = conn.cursor()
        query_start = time.time()
        
        cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = DATABASE()")
        tables = [row[0] for row in cursor.fetchall()]
        
        query_time = (time.time() - query_start) * 1000
        logger.info(f"Retrieved {len(tables)} tables from database | Query: {query_time:.1f}ms")
        
        return True, tables, 200
    
    except Error as e:
        logger.error(f"Error retrieving tables: {e}")
        return False, [], 500
    finally:
        cursor.close()


def query_table(table_name, conditions=None, params=None, limit=None, offset=None):
    """
    Generic table query function with pagination support.
    
    Args:
        table_name: Name of the table to query
        conditions: List of WHERE conditions (e.g., ['`field` = %s', '`timestamp` >= %s'])
        params: List of parameter values corresponding to conditions
        limit: Maximum number of records to return (default: 10000)
        offset: Number of records to skip (default: 0)
    
    Returns:
        tuple: (success: bool, response_dict: dict, status_code: int)
    """
    if not table_name:
        return False, {'error': 'missing table name'}, 400
    
    # Set default and maximum limits to prevent memory exhaustion
    DEFAULT_LIMIT = 10000
    MAX_LIMIT = 50000
    
    if limit is None:
        limit = DEFAULT_LIMIT
    elif limit > MAX_LIMIT:
        return False, {'error': f'limit cannot exceed {MAX_LIMIT} records'}, 400
    
    if offset is None:
        offset = 0
    
    conn = get_connection()
    if conn is None:
        return False, {'error': 'database connection failed'}, 503
    
    try:
        operation_start = time.time()
        cursor = conn.cursor(dictionary=True)
        
        # Get total count for pagination info
        count_start = time.time()
        if conditions and params:
            where_clause = ' AND '.join(conditions)
            count_query = f"SELECT COUNT(*) as total FROM `{table_name}` WHERE {where_clause}"
            cursor.execute(count_query, params)
        else:
            count_query = f"SELECT COUNT(*) as total FROM `{table_name}`"
            cursor.execute(count_query)
        
        count_result = cursor.fetchone()
        total_count = count_result['total'] if count_result and 'total' in count_result else 0
        count_time = time.time() - count_start
        
        query_start = time.time()
        
        # Build main query with pagination
        if conditions and params:
            where_clause = ' AND '.join(conditions)
            query = f"SELECT * FROM `{table_name}` WHERE {where_clause} LIMIT {limit} OFFSET {offset}"
            cursor.execute(query, params)
        else:
            query = f"SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {offset}"
            cursor.execute(query)
        
        query_execute_time = time.time() - query_start
        
        fetch_start = time.time()
        results = cursor.fetchall()
        fetch_time = time.time() - fetch_start
        
        serialize_start = time.time()
        response_data = {
            'data': results, 
            'count': len(results),
            'total_count': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + len(results)) < total_count
        }
        serialize_time = time.time() - serialize_start
        total_time = time.time() - operation_start
        
        logger.info(f"Retrieved {len(results)} records from {table_name} (total: {total_count}) | Count: {count_time*1000:.1f}ms | Query: {query_execute_time*1000:.1f}ms | Fetch: {fetch_time*1000:.1f}ms | Serialize: {serialize_time*1000:.2f}ms | Total: {total_time*1000:.1f}ms")
        
        return True, response_data, 200
    
    except Error as e:
        total_time = time.time() - operation_start
        logger.error(f"Error querying table {table_name}: {e} | Total time: {total_time*1000:.1f}ms")
        return False, {'error': str(e)}, 500
    finally:
        cursor.close()
