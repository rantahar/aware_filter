"""Data retrieval module for AWARE Webservice Receiver"""

from mysql.connector import Error
import logging
import time
from .connection import get_connection

logger = logging.getLogger(__name__)


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
            'limit': limit,
            'offset': offset
        }
        serialize_time = time.time() - serialize_start
        total_time = time.time() - operation_start
        
        logger.info(f"Retrieved {len(results)} records from {table_name} | Query: {query_execute_time*1000:.1f}ms | Fetch: {fetch_time*1000:.1f}ms | Serialize: {serialize_time*1000:.2f}ms | Total: {total_time*1000:.1f}ms")
        
        return True, response_data, 200
    
    except Error as e:
        total_time = time.time() - operation_start
        logger.error(f"Error querying table {table_name}: {e} | Total time: {total_time*1000:.1f}ms")
        return False, {'error': str(e)}, 500
    finally:
        cursor.close()
