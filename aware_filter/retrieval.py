"""Data retrieval module for AWARE Webservice Receiver"""

from mysql.connector import Error
import logging
import time
import base64
from .connection import get_connection

logger = logging.getLogger(__name__)



def serialize_for_json(data):
    """
    Convert database records to JSON-serializable format.
    Handles bytes objects by converting to base64-encoded strings.
    
    Args:
        data: List of dictionaries from database cursor
    
    Returns:
        List of dictionaries with all values JSON-serializable
    """
    if not data:
        return data
    
    serialized = []
    for record in data:
        if isinstance(record, dict):
            new_record = {}
            for key, value in record.items():
                if isinstance(value, bytes):
                    # Encode bytes as base64 string
                    new_record[key] = base64.b64encode(value).decode('utf-8')
                else:
                    new_record[key] = value
            serialized.append(new_record)
        else:
            serialized.append(record)
    
    return serialized


def table_has_data(table_name, conditions=None, params=None):
    """
    Check if a table has any data matching the given conditions.
    Fast check without COUNT(*) - only returns True/False.
    
    Args:
        table_name: Name of the table to check
        conditions: List of WHERE conditions (e.g., ['`field` = %s'])
        params: List of parameter values corresponding to conditions
    
    Returns:
        tuple: (success: bool, has_data: bool, status_code: int)
    """
    if not table_name:
        return False, False, 400
    
    conn = get_connection()
    if conn is None:
        return False, False, 503
    
    try:
        cursor = conn.cursor()
        query_start = time.time()
        
        # Build query to check existence
        if conditions and params:
            where_clause = ' AND '.join(conditions)
            query = f"SELECT 1 FROM `{table_name}` WHERE {where_clause} LIMIT 1"
            cursor.execute(query, params)
        else:
            query = f"SELECT 1 FROM `{table_name}` LIMIT 1"
            cursor.execute(query)
        
        result = cursor.fetchone()
        has_data = result is not None
        
        query_time = (time.time() - query_start) * 1000
        logger.debug(f"Checked existence in {table_name}: {has_data} | Query: {query_time:.1f}ms")
        
        return True, has_data, 200
    
    except Error as e:
        query_time = (time.time() - query_start) * 1000
        logger.error(f"Error checking table {table_name}: {e} | Query time: {query_time:.1f}ms")
        return False, False, 500
    finally:
        cursor.close()


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
        # Convert bytes to base64 strings for JSON serialization
        serialized_results = serialize_for_json(results)
        
        response_data = {
            'data': serialized_results, 
            'count': len(serialized_results),
            'total_count': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + len(serialized_results)) < total_count
        }
        serialize_time = time.time() - serialize_start
        total_time = time.time() - operation_start
        
        logger.info(f"Retrieved {len(serialized_results)} records from {table_name} (total: {total_count}) | Count: {count_time*1000:.1f}ms | Query: {query_execute_time*1000:.1f}ms | Fetch: {fetch_time*1000:.1f}ms | Serialize: {serialize_time*1000:.2f}ms | Total: {total_time*1000:.1f}ms")
        
        return True, response_data, 200
    
    except Error as e:
        total_time = time.time() - operation_start
        logger.error(f"Error querying table {table_name}: {e} | Total time: {total_time*1000:.1f}ms")
        return False, {'error': str(e)}, 500
    finally:
        cursor.close()

def query_data(table_name, request_args):
    """
    Build and execute a complex query with pagination, filtering, and device UID lookups.
    Handles both original tables (with device_id) and transformed tables (with device_uid).
    
    Args:
        table_name: Name of the table to query
        request_args: Flask request.args object containing query parameters
    
    Returns:
        tuple: (success: bool, response_dict: dict, status_code: int)
    """
    try:
        # Build WHERE conditions from query parameters
        conditions = []
        params = []
        limit = None
        offset = None
        device_id_index = None  # Track which index device_id condition is at
        device_id_param_count = 0  # Track how many device_id params
        
        # Check if device_id is provided and needs to be converted to device_uid for transformed tables
        device_id_param = request_args.get('device_id')
        device_uids = None
        
        if device_id_param:
            # Parse device_ids
            device_ids = [d.strip() for d in device_id_param.split(',') if d.strip()]
            
            # Look up device_uids for the provided device_ids (for transformed table queries)
            device_uids = []
            for device_id in device_ids:
                success, device_lookup, _ = query_table('device_lookup', ['`device_uuid` = %s'], [device_id])
                if success and device_lookup.get('data') and len(device_lookup['data']) > 0:
                    device_uid = device_lookup['data'][0].get('id')
                    device_uids.append(device_uid)
        
        for key, value in request_args.items():
            if key == 'table':  # Skip the table parameter
                continue
            elif key == 'device_id':  # Handle device_id specially
                if device_id_param:
                    device_ids = [d.strip() for d in device_id_param.split(',') if d.strip()]
                    device_id_index = len(conditions)  # Record where this condition is
                    device_id_param_count = len(device_ids)  # Record how many params
                    if len(device_ids) > 1:
                        placeholders = ', '.join(['%s'] * len(device_ids))
                        conditions.append(f'`device_id` IN ({placeholders})')
                        params.extend(device_ids)
                    else:
                        conditions.append('`device_id` = %s')
                        params.append(device_ids[0])
            elif key == 'start_time':
                conditions.append('`timestamp` >= %s')
                params.append(value)
            elif key == 'end_time':
                conditions.append('`timestamp` <= %s')
                params.append(value)
            elif key == 'limit':
                try:
                    limit = int(value)
                    if limit <= 0:
                        return False, {'error': 'limit must be positive'}, 400
                except ValueError:
                    return False, {'error': 'limit must be a valid integer'}, 400
            elif key == 'offset':
                try:
                    offset = int(value)
                    if offset < 0:
                        return False, {'error': 'offset must be non-negative'}, 400
                except ValueError:
                    return False, {'error': 'offset must be a valid integer'}, 400
            else:
                # Check if value contains comma-separated list for IN conditions
                if ',' in value:
                    values = [v.strip() for v in value.split(',') if v.strip()]
                    if not values:
                        return False, {'error': f'invalid comma-separated list for {key}'}, 400
                    placeholders = ', '.join(['%s'] * len(values))
                    conditions.append(f'`{key}` IN ({placeholders})')
                    params.extend(values)
                else:
                    conditions.append(f'`{key}` = %s')
                    params.append(value)
        
        # Query both original and transformed tables
        all_data = []
        
        # Query original table with device_id
        success, response_dict, status_code = query_table(table_name, conditions, params, limit=None, offset=None)
        if success and response_dict.get('data'):
            all_data.extend(response_dict['data'])
        
        # Query transformed table with device_uid if device_ids were provided and device_uids exist
        if device_uids:
            transformed_table_name = f"{table_name}_transformed"
            # Build transformed conditions by excluding device_id condition and replacing with device_uid
            transformed_conditions = []
            transformed_params = []
            
            param_offset = 0  # Track how many params we've consumed
            
            for i, condition in enumerate(conditions):
                param_count = condition.count('%s')  # How many params this condition uses
                
                if i == device_id_index:
                    # Skip device_id condition and its params
                    param_offset += param_count
                    continue
                
                # Add this condition to transformed conditions
                transformed_conditions.append(condition)
                # Add the corresponding params
                transformed_params.extend(params[param_offset:param_offset + param_count])
                param_offset += param_count
            
            # Add device_uid condition
            if len(device_uids) > 1:
                placeholders = ', '.join(['%s'] * len(device_uids))
                transformed_conditions.append(f'`device_uid` IN ({placeholders})')
                transformed_params.extend(device_uids)
            else:
                transformed_conditions.append('`device_uid` = %s')
                transformed_params.append(device_uids[0])
            
            success_t, response_dict_t, status_code_t = query_table(transformed_table_name, transformed_conditions, transformed_params, limit=None, offset=None)
            if success_t and response_dict_t.get('data'):
                all_data.extend(response_dict_t['data'])
        
        # Sort all data by timestamp if available
        if all_data and 'timestamp' in all_data[0]:
            all_data.sort(key=lambda x: x.get('timestamp', 0))
        
        # Apply limit and offset to combined results
        total_count = len(all_data)
        if offset is None:
            offset = 0
        if limit is None:
            limit = 10000
        
        paginated_data = all_data[offset:offset + limit]
        
        response_dict = {
            'data': paginated_data,
            'count': len(paginated_data),
            'total_count': total_count,
            'limit': limit,
            'offset': offset,
            'has_more': (offset + len(paginated_data)) < total_count
        }
        
        return True, response_dict, 200
    
    except Exception as e:
        logger.error(f"Error in query_data: {e}")
        return False, {'error': str(e)}, 500


def get_tables_for_devices(requested_device_ids):
    """
    Find all tables that have data for one or more device_ids.
    
    Args:
        requested_device_ids: List of device IDs to search for
    
    Returns:
        tuple: (success: bool, response_dict: dict, status_code: int)
    """
    try:
        if not requested_device_ids:
            return False, {'error': 'invalid device_id format'}, 400
        
        # Build device_uid map by looking up each device_id
        device_uid_map = {}
        for device_id in requested_device_ids:
            success, device_lookup, _ = query_table('device_lookup', ['`device_uuid` = %s'], [device_id])
            if success and device_lookup.get('data') and len(device_lookup['data']) > 0:
                device_uid = device_lookup['data'][0].get('id')
                device_uid_map[device_id] = device_uid
        
        if not device_uid_map:
            logger.warning(f"None of the devices {requested_device_ids} found in device_lookup table")
            return False, {
                'error': 'device_ids not found',
                'device_ids': requested_device_ids,
                'found_count': 0
            }, 404
        
        # Get list of all tables
        success, all_tables, status_code = get_all_tables()
        if not success:
            return False, {'error': 'failed to retrieve table list'}, status_code
        
        tables_with_data = []
        
        # Check each table for data matching any device_id or device_uid
        for table_name in all_tables:
            if table_name in ['device_lookup', 'aware_device', 'aware_log', 'mqtt_history', 'mqtt_history_transformed', 'encryption_skip_list', 'device_index']:
                continue
            
            matched_by_list = set()
            matched_device_ids_for_table = []
            
            # Check non-transformed tables for device_id matches using IN clause
            if not table_name.endswith('_transformed'):
                placeholders = ', '.join(['%s'] * len(requested_device_ids))
                success, result, _ = table_has_data(table_name, [f'`device_id` IN ({placeholders})'], requested_device_ids)
                if success and result:
                    matched_device_ids_for_table = requested_device_ids
                    matched_by_list.add('device_id')
            
            # Check transformed tables for device_uid matches using IN clause
            if table_name.endswith('_transformed'):
                device_uids = list(device_uid_map.values())
                if device_uids:
                    placeholders = ', '.join(['%s'] * len(device_uids))
                    success, result, _ = table_has_data(table_name, [f'`device_uid` IN ({placeholders})'], device_uids)
                    if success and result:
                        # Map back to original device_ids
                        matched_device_ids_for_table = [did for did, duid in device_uid_map.items() if duid in device_uids]
                        matched_by_list.add('device_uid')
            
            # If this table has data for any of our devices, add it to results
            if matched_device_ids_for_table:
                # Remove "_transformed" suffix if present for display
                display_table_name = table_name
                if table_name.endswith('_transformed'):
                    display_table_name = table_name[:-len('_transformed')]
                
                tables_with_data.append({
                    'table': display_table_name,
                    'matched_by': ','.join(sorted(matched_by_list)),
                    'device_ids_matched': sorted(matched_device_ids_for_table)
                })
        
        response_data = {
            'device_ids': requested_device_ids,
            'device_uid_map': device_uid_map,
            'tables_with_data': tables_with_data,
            'count': len(tables_with_data)
        }
        
        logger.info(f"Found {len(tables_with_data)} tables with data for {len(requested_device_ids)} devices")
        return True, response_data, 200
    
    except Exception as e:
        logger.error(f"Error in get_tables_for_devices: {e}")
        return False, {'error': 'Internal server error'}, 500