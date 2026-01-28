#!/usr/bin/env python3
"""
AWARE Webservice Receiver
Receives JSON POST data from AWARE clients and writes to MySQL database.
No filtering yet - just direct passthrough.
"""

from flask import Flask, jsonify, request
import logging
import psutil
import gc
import time
from datetime import datetime
from dotenv import load_dotenv
import os
from .auth import login, check_token
from .insertion import insert_records, STUDY_PASSWORD
from .retrieval import query_table, get_all_tables, table_has_data
from .connection import get_connection

load_dotenv()

app = Flask(__name__)

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,  # Changed to DEBUG
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def check_memory_usage():
    """Check current memory usage and log warnings if high"""
    process = psutil.Process()
    memory_info = process.memory_info()
    memory_mb = memory_info.rss / 1024 / 1024
    
    if memory_mb > 400:  # Warning if over 400MB
        logger.warning(f"High memory usage: {memory_mb:.1f} MB")
        if memory_mb > 500:  # Force garbage collection if over 500MB
            gc.collect()
            logger.info("Forced garbage collection due to high memory usage")
    
    return memory_mb

stats = {
    'total_requests': 0,
    'successful_inserts': 0,
    'failed_inserts': 0,
    'unauthorized_attempts': 0
}

@app.route('/webservice/index/<study_id>/<password>/<table_name>', methods=['POST'])
def webservice_table_route(study_id, password, table_name):
    """
    Table-specific endpoint - receives data for specific table
    
    Path Parameters:
        study_id (str): Study identifier
        password (str): Study password for authentication
        table_name (str): Name of the table to insert data into
    
    Request Body:
        JSON array or object containing records to insert
    
    Returns:
        200: Successful insertion with record count
        401: Invalid password
        500: Database error or insertion failure
    """
    route_start_time = time.time()
    if password != STUDY_PASSWORD:
        logger.warning(f"Unauthorized attempt: study_id={study_id}, table={table_name}")
        stats['unauthorized_attempts'] += 1
        return jsonify({'error': 'unauthorized'}), 401
    
    stats['total_requests'] += 1
    
    try:
        data = request.get_json()
        success, response_dict = insert_records(data, table_name, stats)
        
        if success:
            elapsed = time.time() - route_start_time
            logger.info(f"webservice_table_route completed in {elapsed:.3f}s")
            return jsonify(response_dict), 200
        else:
            elapsed = time.time() - route_start_time
            logger.info(f"webservice_table_route completed in {elapsed:.3f}s (failure)")
            return jsonify(response_dict), 500
        
    except Exception as e:
        elapsed = time.time() - route_start_time
        logger.error(f"Error processing request after {elapsed:.3f}s: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/health', methods=['GET'])
def health():
    """
    Health check endpoint
    
    Returns:
        200: Service is healthy and database is connected
        503: Database connection failed or service is unhealthy
    """
    route_start_time = time.time()
    conn = get_connection()
    elapsed = time.time() - route_start_time
    if conn:
        logger.debug(f"health endpoint completed in {elapsed:.3f}s")
        return jsonify({'status': 'healthy', 'database': 'connected'}), 200
    else:
        logger.debug(f"health endpoint completed in {elapsed:.3f}s (unhealthy)")
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 503


@app.route('/stats', methods=['GET'])
def get_stats():
    """
    Stats endpoint for monitoring
    
    Returns:
        Service name, current timestamp, and statistics including:
        - total_requests: Total requests processed
        - successful_inserts: Successful data insertions
        - failed_inserts: Failed data insertions
        - unauthorized_attempts: Failed authentication attempts
    """
    route_start_time = time.time()
    result = jsonify({
        'service': 'AWARE Webservice Receiver',
        'timestamp': datetime.utcnow().isoformat(),
        'stats': stats,
        'endpoints': [
            '/webservice/index/<study_id>/<password>',
            '/webservice/index/<study_id>/<password>/<table_name>'
        ]
    })
    elapsed = time.time() - route_start_time
    logger.debug(f"stats endpoint completed in {elapsed:.3f}s")
    return result, 200

@app.route('/login', methods=['POST'])
def login_route():
    """
    Authenticate and receive JWT token
    
    Request Body:
        JSON object with authentication credentials
    
    Returns:
        200: Authentication successful, returns JWT token
        401: Authentication failed
    """
    route_start_time = time.time()
    result = login(stats)
    elapsed = time.time() - route_start_time
    logger.info(f"login endpoint completed in {elapsed:.3f}s")
    return result


@app.route('/data', methods=['GET'])
def query_route():
    """
    Generic query endpoint - query any table with any conditions
    
    Query Parameters:
        table (str, required): Name of the table to query
        device_id (str, optional): Filter by device_id column (supports comma-separated values for multiple IDs)
        device_uid (str, optional): Filter by device_uid column (supports comma-separated values)
        timestamp (str, optional): Filter by exact timestamp
        start_time (str, optional): Filter records with timestamp >= start_time
        end_time (str, optional): Filter records with timestamp <= end_time
        limit (int, optional): Maximum records to return (default: 10000, max: 50000)
        offset (int, optional): Number of records to skip for pagination (default: 0)
        <any_column> (str, optional): Filter by any table column using equality (supports comma-separated values)
    
    Examples:
        Single device_id: /data?table=screen_sensor&device_id=device1
        Multiple device_ids: /data?table=screen_sensor&device_id=device1,device2,device3
    
    Returns:
        200: Query successful with data, count, total_count, limit, offset, has_more
        400: Invalid parameters or missing table name
        404: Table not found
        500: Database error
    """
    request_start_time = datetime.utcnow()
    try:
        # Check memory usage before processing request
        memory_mb = check_memory_usage()
        logger.debug(f"Processing data query request. Current memory: {memory_mb:.1f} MB")
        
        # Validate token
        token_error = check_token()
        if token_error:
            return token_error
        
        table_name = request.args.get('table')
        if not table_name:
            return jsonify({'error': 'missing table parameter'}), 400
        
        # Build WHERE conditions from query parameters
        conditions = []
        params = []
        limit = None
        offset = None
        
        # Check if device_id is provided and needs to be converted to device_uid for transformed tables
        device_id_param = request.args.get('device_id')
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
        
        for key, value in request.args.items():
            if key == 'table':  # Skip the table parameter
                continue
            elif key == 'device_id':  # Handle device_id specially
                if device_id_param:
                    device_ids = [d.strip() for d in device_id_param.split(',') if d.strip()]
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
                        return jsonify({'error': 'limit must be positive'}), 400
                except ValueError:
                    return jsonify({'error': 'limit must be a valid integer'}), 400
            elif key == 'offset':
                try:
                    offset = int(value)
                    if offset < 0:
                        return jsonify({'error': 'offset must be non-negative'}), 400
                except ValueError:
                    return jsonify({'error': 'offset must be a valid integer'}), 400
            else:
                # Check if value contains comma-separated list for IN conditions
                if ',' in value:
                    values = [v.strip() for v in value.split(',') if v.strip()]
                    if not values:
                        return jsonify({'error': f'invalid comma-separated list for {key}'}), 400
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
            transformed_conditions = [c for c in conditions if 'device_id' not in c]
            transformed_params = [p for i, p in enumerate(params) if i not in [i for i, c in enumerate(conditions) if 'device_id' in c]]
            
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
        
        # Calculate request duration
        request_duration = (datetime.utcnow() - request_start_time).total_seconds()
        
        # Check memory usage after query but before JSON serialization
        memory_after_query = check_memory_usage()
        logger.debug(f"After database query. Memory: {memory_after_query:.1f} MB, Duration: {request_duration:.1f}s")
        
        # Add warnings to response
        warnings = []
        if response_dict['total_count'] > 100000:
            warnings.append(f"Large dataset ({response_dict['total_count']} total records). Consider using pagination with limit and offset parameters.")
        
        if request_duration > 60:  # Warn if query takes more than 1 minute
            warnings.append(f"Long-running query ({request_duration:.1f}s). Consider adding more specific filters or pagination.")
            logger.warning(f"Long query duration: {request_duration:.1f}s for table {table_name}")
        
        if warnings:
            response_dict['warnings'] = warnings
        
        response_dict['query_duration_seconds'] = round(request_duration, 2)
        
        return jsonify(response_dict), 200
    
    except Exception as e:
        request_duration = (datetime.utcnow() - request_start_time).total_seconds()
        logger.error(f"Unexpected error in query route after {request_duration:.1f}s: {e}")
        
        # Check if this might be a timeout-related error
        if request_duration > 240:  # Close to our 300s timeout
            logger.error(f"Query likely timed out after {request_duration:.1f}s. Consider using pagination or more specific filters.")
            return jsonify({
                'error': 'Query timeout - request took too long to process',
                'suggestion': 'Use limit/offset parameters or more specific filters to reduce dataset size',
                'duration_seconds': round(request_duration, 2)
            }), 408  # Request Timeout
        
        # Force garbage collection on error
        gc.collect()
        return jsonify({'error': 'Internal server error'}), 500


@app.route('/tables-for-device', methods=['GET'])
def tables_for_device_route():
    """
    Find all tables that have data for one or more device_ids
    
    Query Parameters:
        device_id (str, required): Single device ID or comma-separated list of device IDs
    
    Returns:
        200: List of tables with data for the devices, including:
             - device_ids: Array of requested device IDs
             - device_uid_map: Dict mapping each device_id to its device_uid
             - tables_with_data: Array of tables containing data for these devices
               - table: Table name
               - matched_by: 'device_id', 'device_uid', or 'both'
               - device_ids_matched: Array of device_ids that matched this table
             - count: Number of tables with data
        400: Missing device_id parameter or invalid format
        404: None of the device_ids found in device_lookup table
        500: Database error
    
    Examples:
        Single device: /tables-for-device?device_id=device1
        Multiple devices: /tables-for-device?device_id=device1,device2,device3
    """
    route_start_time = time.time()
    try:
        device_id_param = request.args.get('device_id')
        if not device_id_param:
            return jsonify({'error': 'missing device_id parameter'}), 400
        
        token_error = check_token()
        if token_error:
            return token_error
        
        # Parse comma-separated device_ids
        requested_device_ids = [d.strip() for d in device_id_param.split(',') if d.strip()]
        if not requested_device_ids:
            return jsonify({'error': 'invalid device_id format'}), 400
        
        # Build device_uid map by looking up each device_id
        device_uid_map = {}
        for device_id in requested_device_ids:
            success, device_lookup, _ = query_table('device_lookup', ['`device_uuid` = %s'], [device_id])
            if success and device_lookup.get('data') and len(device_lookup['data']) > 0:
                device_uid = device_lookup['data'][0].get('id')
                device_uid_map[device_id] = device_uid
        
        if not device_uid_map:
            elapsed = time.time() - route_start_time
            logger.warning(f"None of the devices {requested_device_ids} found in device_lookup table (took {elapsed:.3f}s)")
            return jsonify({
                'error': 'device_ids not found',
                'device_ids': requested_device_ids,
                'found_count': 0
            }), 404
        
        # Get list of all tables
        success, all_tables, status_code = get_all_tables()
        if not success:
            return jsonify({'error': 'failed to retrieve table list'}), status_code
        
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
        
        elapsed = time.time() - route_start_time
        logger.info(f"Found {len(tables_with_data)} tables with data for {len(requested_device_ids)} devices in {elapsed:.3f}s")
        return jsonify(response_data), 200
    
    except Exception as e:
        elapsed = time.time() - route_start_time
        logger.error(f"Error in tables_for_device_route after {elapsed:.3f}s: {e}")
        return jsonify({'error': 'Internal server error'}), 500


def main():
    """Entry point for the aware-filter command"""
    logger.info("Starting AWARE Webservice Receiver")
    
    app.run(
        host='0.0.0.0',
        port=3446,
        ssl_context='adhoc',
        debug=False
    )


if __name__ == '__main__':
    main()

