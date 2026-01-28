"""Data retrieval module for AWARE Webservice Receiver"""

from mysql.connector import Error
import logging
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
        cursor = conn.cursor(dictionary=True)
        
        # Build main query with pagination
        if conditions and params:
            where_clause = ' AND '.join(conditions)
            query = f"SELECT * FROM `{table_name}` WHERE {where_clause} LIMIT {limit} OFFSET {offset}"
            cursor.execute(query, params)
        else:
            query = f"SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {offset}"
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        logger.info(f"Retrieved {len(results)} records from {table_name}")
        
        response_data = {
            'data': results, 
            'count': len(results),
            'limit': limit,
            'offset': offset
        }
        
        return True, response_data, 200
    
    except Error as e:
        logger.error(f"Error querying table {table_name}: {e}")
        return False, {'error': str(e)}, 500
    finally:
        cursor.close()
