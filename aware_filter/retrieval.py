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
        
        # First, get total count for pagination info
        if conditions and params:
            where_clause = ' AND '.join(conditions)
            count_query = f"SELECT COUNT(*) as total FROM `{table_name}` WHERE {where_clause}"
            cursor.execute(count_query, params)
        else:
            count_query = f"SELECT COUNT(*) as total FROM `{table_name}`"
            cursor.execute(count_query)
        
        count_result = cursor.fetchone()
        total_count = count_result['total'] if count_result and 'total' in count_result else 0
        
        # Check if the total count is reasonable for processing
        try:
            if isinstance(total_count, int) and total_count > 1000000 and limit > 10000:
                logger.warning(f"Large dataset detected ({total_count} records). Consider using smaller limit and pagination.")
        except (TypeError, ValueError):
            # Handle case where total_count might be a mock or invalid value during testing
            pass
        
        # Build main query with pagination
        if conditions and params:
            where_clause = ' AND '.join(conditions)
            query = f"SELECT * FROM `{table_name}` WHERE {where_clause} LIMIT {limit} OFFSET {offset}"
            cursor.execute(query, params)
        else:
            query = f"SELECT * FROM `{table_name}` LIMIT {limit} OFFSET {offset}"
            cursor.execute(query)
        
        results = cursor.fetchall()
        
        logger.info(f"Retrieved {len(results)} records from {table_name} (total: {total_count})")
        
        response_data = {
            'data': results, 
            'count': len(results),
            'total_count': total_count if isinstance(total_count, int) else len(results),
            'limit': limit,
            'offset': offset,
            'has_more': (offset + len(results)) < total_count if isinstance(total_count, int) else False
        }
        
        return True, response_data, 200
    
    except Error as e:
        logger.error(f"Error querying table {table_name}: {e}")
        return False, {'error': str(e)}, 500
    finally:
        cursor.close()
